"""Installed-root physical proof must know its rule owner and drain identity."""
import json
import os
from pathlib import Path

import pytest

import resource_broker

from client_proof_bridge_fixture import (
    PREBRIDGE, ROOT, _fresh, _sha, fixed_source_artifact, make_publication,
    member_bytes, reader_process,
)
from test_client_proof_prior_trust import (
    PRIOR_FILES, STRICT_PROOF_SHA256, _file_facts, _retain_proof, tmp_path,
)
from test_park_proof_real_writer import parked_role


LEGACY_PROOF_SHA256 = "7b951734fe4471ab012fe715cda238be1dd4c7d83c8f15c39447e1b75009bfe7"


class EmptyKernel:
    def healthy(self):
        return True

    def inventory(self):
        return {}


@pytest.fixture
def host_publication(tmp_path):
    # The original writer owns tmp_path/runtime and tmp_path/installed. Keep
    # the independent transaction's files in a separate private subtree.
    root = tmp_path / "upgrade"
    root.mkdir(mode=0o755)
    publication = make_publication(root)
    generation, install, _, config_path, _, _ = publication
    for path in (root, *root.rglob("*")):
        if path.is_dir():
            path.chmod(0o755)
    config = json.loads(config_path.read_text())
    config.update(maintenance_gate=str(tmp_path / "maintenance.json"),
                  socket=str(root / "run/resources.sock"), rollout_root=str(root / "rollout"))
    assert all(Path(value).is_relative_to(tmp_path) for key, value in config.items()
               if key != "reader_uid")
    config_path.write_text(json.dumps(config))
    installed = install / "upgrade_client.py"
    installed.write_bytes((ROOT / "tools/fleet/upgrade_client.py").read_bytes())
    installed.chmod(0o644)
    assert installed.read_bytes() == (generation / "tools/upgrade_client.py").read_bytes()
    assert installed.read_bytes() == (ROOT / "tools/fleet/upgrade_client.py").read_bytes()
    return publication


def _close_real_gate(publication):
    _, _, _, config_path, _, _ = publication
    config = json.loads(config_path.read_text())
    gate = Path(config["maintenance_gate"])
    # Bootstrap only an open volatile mirror. The real Authority writes both
    # the durable closure and the worker-visible closed gate used by the test.
    gate.write_text(json.dumps({"schema": resource_broker.MAINTENANCE_SCHEMA,
                                "draining": False}))
    gate.chmod(0o644)
    durable = gate.parent / "durable" / "maintenance.json"
    authority = resource_broker.Authority(
        gate.parent / "jobs", os.getuid(), EmptyKernel(),
        max_memory_bytes=1024**3, maintenance_state=durable)
    assert authority.maintenance_path == gate
    assert authority.maintenance_state_path == durable
    assert authority.maintenance_error is None, authority.maintenance_error
    initial = authority.admin(0, {"op": "maintenance_status"})
    assert initial["health"] is True and initial["draining"] is False, initial
    status = authority.admin(0, {"op": "maintenance_begin", "owner": "client-upgrade",
                                 "reason": "observe installed physical proof"})
    assert status["health"] is True and status["draining"] is True, status
    assert type(status["active_scopes"]) is int and status["active_scopes"] == 0, status
    assert status["active_scope_ids"] == [], status
    closed = json.loads(gate.read_text())
    assert closed == json.loads(durable.read_text())
    assert type(closed["changed_unix"]) is float and closed["changed_unix"] > 0
    status_path = config_path.parent / "host-status.json"
    status_path.write_text(json.dumps(status))
    config["host_proof_status"] = str(status_path)
    config_path.write_text(json.dumps(config))
    return authority, closed


def _observe(publication, authority, selected):
    _, install, _, _, backend_path, _ = publication
    before = member_bytes(install)
    durable = authority.maintenance_state_path.read_bytes()
    mirror = authority.maintenance_path.read_bytes()
    operations = json.loads(backend_path.read_text())["operations"]
    observed = _fresh(publication, "host-proof")
    assert observed["exception"] is None, observed
    assert observed["executed_sha256"] == _sha((ROOT / "tools/fleet/upgrade_client.py").read_bytes())
    assert observed["census_before"] == selected, observed
    assert observed["census_after"] == selected, observed
    assert observed["result"]["active_scopes"] == 0, observed
    assert member_bytes(install) == before
    assert authority.maintenance_state_path.read_bytes() == durable
    assert authority.maintenance_path.read_bytes() == mirror
    assert json.loads(backend_path.read_text())["operations"] == operations
    assert observed["journal_exists"] is False
    return observed


def _post_real_marker(role, closed):
    pid, start, _ = role.census[0]
    name = role.post()
    assert name == f"{pid}-{start}-{closed['changed_unix']}"
    marker = role.parked / name
    assert marker.read_bytes() == b""
    return marker, [[pid, start]]


def _retain_current_proof(publication, kind):
    leaf, _ = _retain_proof(publication, kind)
    data = (ROOT / "src/prismabuild/park_proof.py").read_bytes()
    leaf.write_bytes(data)
    assert leaf.read_bytes() == data
    assert _file_facts(leaf) == PRIOR_FILES[kind]
    return leaf, data


@pytest.mark.parametrize("leaf_kind", [
    "missing", "worker-owned", "worker-writable", "legacy-invalid", "trusted",
])
def test_installed_rule_owner_is_required_with_a_retained_real_marker(
        host_publication, parked_role, leaf_kind):
    _, install, _, _, _, _ = host_publication
    kind = leaf_kind if leaf_kind in PRIOR_FILES else "trusted"
    leaf, strict_bytes = _retain_current_proof(host_publication, kind)
    with parked_role() as role:
        authority, closed = _close_real_gate(host_publication)
        marker, selected = _post_real_marker(role, closed)
        if leaf_kind == "missing":
            leaf.unlink()
        elif leaf_kind == "legacy-invalid":
            legacy = fixed_source_artifact(
                "legacy-proof.py", PREBRIDGE, "src/prismabuild/park_proof.py", LEGACY_PROOF_SHA256)
            assert legacy != strict_bytes
            leaf.write_bytes(legacy)
            assert _sha(leaf.read_bytes()) == LEGACY_PROOF_SHA256
        before = None if not leaf.exists() else (leaf.read_bytes(), _file_facts(leaf))
        observed = _observe(host_publication, authority, selected)
        assert marker.read_bytes() == b""
        assert (None if not leaf.exists() else (leaf.read_bytes(), _file_facts(leaf))) == before
        assert (install / "upgrade_client.py").read_bytes() == (
            ROOT / "tools/fleet/upgrade_client.py").read_bytes()
        assert observed["result"]["drained"] is (leaf_kind == "trusted"), {
            "invariant": "a valid marker cannot authorize an unknown installed rule owner",
            "leaf": leaf_kind, "facts": None if before is None else before[1],
            "observed": observed}


@pytest.mark.parametrize("kind", ["worker-owned", "worker-writable", "trusted"])
def test_fresh_consumer_rejects_unsafe_rule_owner_after_real_failed_transaction_rollback(
        host_publication, parked_role, kind):
    _, install, state, _, backend_path, _ = host_publication
    leaf, retained = _retain_proof(host_publication, kind)
    previous_reader = (install / "upgrade_client.py").read_bytes()
    backend = json.loads(backend_path.read_text())
    backend["fail_new_health"] = True
    backend_path.write_text(json.dumps(backend))
    restored_metadata = state / "restored-proof.json"
    with reader_process(host_publication, cut="restore-updater-after-replace",
                        restore_observation=str(restored_metadata)) as reader:
        assert reader.message() == {"bridge_cut": "restore-updater-after-replace", "phase": "recover"}
        transaction = json.loads((state / "transaction.json").read_text())
        assert "broker did not become healthy while drained" in transaction["error"]
        assert transaction["previous"]["park_proof.py"] == STRICT_PROOF_SHA256
        uid, gid, mode = PRIOR_FILES[kind]
        assert transaction["previous_proof_file"] == {"uid": uid, "gid": gid, "mode": mode}
        assert (state / "previous/park_proof.py").read_bytes() == retained
        assert leaf.read_bytes() == retained and _file_facts(leaf) == PRIOR_FILES[kind]
        restored = json.loads(restored_metadata.read_text())
        assert (restored["uid"], restored["gid"], restored["mode"]) == PRIOR_FILES[kind]
        assert restored["sha256"] == STRICT_PROOF_SHA256
        assert (install / "upgrade_client.py").read_bytes() == previous_reader
        reader.kill()
    recovered = _fresh(host_publication)
    assert recovered["exception"] is None, recovered
    assert recovered["result"]["state"] == "rolled_back", recovered
    assert recovered["executed_sha256"] == _sha(previous_reader)
    assert not (state / "transaction.json").exists()
    assert leaf.read_bytes() == retained and _file_facts(leaf) == PRIOR_FILES[kind]
    backend = json.loads(backend_path.read_text())
    assert backend["holder"] is None and backend["running"] is True and backend["active"] == 0
    assert "stop" in backend["operations"] and "start" in backend["operations"]
    with parked_role() as role:
        authority, closed = _close_real_gate(host_publication)
        marker, selected = _post_real_marker(role, closed)
        observed = _observe(host_publication, authority, selected)
        assert marker.read_bytes() == b""
        assert leaf.read_bytes() == retained and _file_facts(leaf) == PRIOR_FILES[kind]
        assert observed["result"]["drained"] is False, {
            "invariant": "restoring historical bytes does not establish a current physical-proof owner",
            "kind": kind, "facts": _file_facts(leaf), "observed": observed}
        assert observed["result"]["evidence_errors"]


@pytest.mark.parametrize("fault", [
    "missing-leaf", "bool-gate", "infinite-gate", "missing-gate-key", "known-gate",
])
def test_empty_selected_real_census_still_requires_known_rule_owner_and_gate(
        host_publication, fault):
    leaf, retained = _retain_current_proof(host_publication, "trusted")
    authority, closed = _close_real_gate(host_publication)
    gate = authority.maintenance_path
    if fault == "missing-leaf":
        leaf.unlink()
    elif fault != "known-gate":
        damaged = dict(closed)
        if fault == "missing-gate-key":
            damaged.pop("changed_unix")
        else:
            damaged["changed_unix"] = True if fault == "bool-gate" else float("inf")
        gate.write_text(json.dumps(damaged))
    assert not (gate.parent / "rollout/parked").exists()
    assert json.loads(authority.maintenance_state_path.read_text()) == closed
    observed = _observe(host_publication, authority, [])
    if fault == "missing-leaf":
        assert not leaf.exists()
    else:
        assert leaf.read_bytes() == retained and _file_facts(leaf) == PRIOR_FILES["trusted"]
    assert observed["result"]["drained"] is (fault == "known-gate"), {
        "invariant": "an empty selected census cannot certify unknown proof facts",
        "fault": fault, "observed": observed}
