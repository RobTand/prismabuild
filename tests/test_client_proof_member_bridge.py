"""Fresh installed updater readers own transport, not broker-loaded proof code."""
import json

import pytest

from client_proof_bridge_fixture import (
    PREBRIDGE, PREBRIDGE_UPDATER_SHA256, ROOT, _fresh, _sha, fixed_source_artifact, publication,
)


def test_old_installed_reader_adopts_bridge_before_fresh_reader_introduces_proof(publication):
    generation, install, _, _, _, _ = publication
    receipt = (generation / "RUNTIME_VERSION.json").read_bytes()
    proof = (generation / "src/prismabuild/park_proof.py").read_bytes()
    first = _fresh(publication)
    assert first["exception"] is None, first
    assert first["result"]["state"] == "updated", first
    assert "park_proof.py" not in first["result"]["desired"]["files"]
    assert (install / "upgrade_client.py").read_bytes() == (ROOT / "tools/fleet/upgrade_client.py").read_bytes()
    assert not (install / "park_proof.py").exists()
    assert (generation / "RUNTIME_VERSION.json").read_bytes() == receipt
    assert (generation / "src/prismabuild/park_proof.py").read_bytes() == proof

    second = _fresh(publication)
    assert second["exception"] is None, second
    assert second["executed_sha256"] == _sha((ROOT / "tools/fleet/upgrade_client.py").read_bytes())
    assert second["result"]["state"] == "updated", second
    assert second["result"]["desired"]["files"]["park_proof.py"] == _sha(proof)
    assert (install / "park_proof.py").read_bytes() == proof
    assert (generation / "RUNTIME_VERSION.json").read_bytes() == receipt


@pytest.mark.parametrize("fault,expected", [("none", True), ("proof-loaded", False),
                                           ("missing-gpu", False)])
def test_broker_loaded_identity_excludes_only_updater_owned_members(publication, fault, expected):
    _, install, _, _, backend_path, _ = publication
    (install / "upgrade_client.py").write_bytes((ROOT / "tools/fleet/upgrade_client.py").read_bytes())
    (install / "park_proof.py").write_bytes((ROOT / "src/prismabuild/park_proof.py").read_bytes())
    backend = json.loads(backend_path.read_text())
    if fault == "proof-loaded":
        backend["loaded"]["park_proof.py"] = _sha((install / "park_proof.py").read_bytes())
    elif fault == "missing-gpu":
        backend["loaded"].pop("gpu_capacity.py")
    backend_path.write_text(json.dumps(backend))
    observed = _fresh(publication, "loaded")
    assert observed["exception"] is None, observed
    assert observed["result"]["loaded_matches"] is expected, observed


@pytest.mark.parametrize("incapable", ["previous", "candidate"])
def test_proof_transaction_requires_both_actual_reader_capabilities_before_mutation(publication, incapable):
    generation, install, state, _, backend_path, _ = publication
    candidate = (ROOT / "tools/fleet/upgrade_client.py").read_bytes()
    if incapable == "candidate":
        (install / "upgrade_client.py").write_bytes(candidate)
        candidate = fixed_source_artifact(
            "prebridge-upgrader.py", PREBRIDGE, "tools/fleet/upgrade_client.py", PREBRIDGE_UPDATER_SHA256)
        assert _sha(candidate) == PREBRIDGE_UPDATER_SHA256
        target = generation / "tools/upgrade_client.py"
        target.write_bytes(candidate)
        assert target.read_bytes() == candidate
        receipt_path = generation / "RUNTIME_VERSION.json"
        receipt = json.loads(receipt_path.read_text())
        receipt["files"]["tools/upgrade_client.py"] = _sha(candidate)
        receipt_path.write_text(json.dumps(receipt))
    before = {path.name: path.read_bytes() for path in install.iterdir()}
    observed = _fresh(publication, "direct" if incapable == "previous" else "run")
    assert observed["exception"] == "ValueError", observed
    assert "proof" in observed["error"].lower() and "protocol" in observed["error"].lower(), observed
    assert observed["journal_exists"] is False
    assert not (state / "transaction.json").exists()
    assert {path.name: path.read_bytes() for path in install.iterdir()} == before
    operations = json.loads(backend_path.read_text())["operations"]
    assert not set(operations) & {"maintenance_begin", "stop", "start", "maintenance_end"}
