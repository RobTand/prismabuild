"""Actual root rollback must preserve a retained proof file's prior trust."""
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
from tempfile import TemporaryDirectory

import pytest

from client_proof_bridge_fixture import (
    PROTOCOL_ONE_UPDATER, PROTOCOL_ONE_UPDATER_SHA256, ROOT, _fresh, _sha,
    fixed_source_artifact, member_bytes, publication, reader_process,
)


STRICT_PROOF = "52b803d3bf637c6e3d45a87abacf13020c15cf22"
STRICT_PROOF_SHA256 = "772fa28b24b3d0cc6ad2edca803b63ae5965c0ee85f30c56cc113e214aabf68d"
PRIOR_FILES = {
    "worker-writable": (0, 65533, 0o660),
    "worker-owned": (65534, 65533, 0o640),
    "trusted": (0, 0, 0o644),
}


@pytest.fixture
def tmp_path():
    assert os.geteuid() == 0 and os.getegid() == 0, "real UID/GID 0 container required"
    with TemporaryDirectory(prefix="pb-proof-trust-", dir="/var/lib") as directory:
        root = Path(directory)
        root.chmod(0o755)
        yield root


@pytest.fixture
def root_publication(publication):
    _, install, _, config_path, _, _ = publication
    root = install.parent
    assert root.parent == Path("/var/lib") and root.name.startswith("pb-proof-trust-")
    for path in (root, *root.rglob("*")):
        if path.is_dir():
            path.chmod(0o755)
    config = json.loads(config_path.read_text())
    config.update(socket=str(root / "run/resources.sock"), rollout_root=str(root / "rollout"))
    assert all(Path(value).is_relative_to(root) for key, value in config.items()
               if key != "reader_uid")
    config_path.write_text(json.dumps(config))
    # Use the actual current installed reader as the positive. Protocol-one
    # bytes remain a fixed negative when metadata-preserving protocol two lands.
    (install / "upgrade_client.py").write_bytes((ROOT / "tools/fleet/upgrade_client.py").read_bytes())
    return publication


def _file_facts(path):
    info = path.stat()
    return info.st_uid, info.st_gid, stat.S_IMODE(info.st_mode)


def _retain_proof(publication, kind):
    _, install, _, _, _, _ = publication
    data = fixed_source_artifact(
        "strict-proof.py", STRICT_PROOF, "src/prismabuild/park_proof.py", STRICT_PROOF_SHA256)
    target = install / "park_proof.py"
    target.write_bytes(data)
    uid, gid, mode = PRIOR_FILES[kind]
    os.chown(target, uid, gid)
    target.chmod(mode)
    if kind != "trusted":
        writer = subprocess.run(
            [sys.executable, "-I", "-c",
             "import os, pathlib, sys; assert (os.getuid(), os.getgid()) == (65534, 65533); "
             "pathlib.Path(sys.argv[1]).write_bytes(sys.stdin.buffer.read())", str(target)],
            input=data, capture_output=True, timeout=15,
            user=65534, group=65533, extra_groups=[])
        assert writer.returncode == 0, writer.stderr
    assert target.read_bytes() == data and _sha(data) == STRICT_PROOF_SHA256
    assert _file_facts(target) == PRIOR_FILES[kind]
    observed = _fresh(publication, "trust")
    assert observed["exception"] is None, observed
    assert observed["result"]["trusted"] is (kind == "trusted"), observed
    return target, data


@pytest.mark.parametrize("kind", ["worker-writable", "worker-owned", "trusted"])
def test_failed_transaction_and_fresh_rollback_preserve_previous_proof_trust(root_publication, kind):
    _, install, state, _, backend_path, _ = root_publication
    leaf, retained = _retain_proof(root_publication, kind)
    previous_reader = (install / "upgrade_client.py").read_bytes()
    backend = json.loads(backend_path.read_text())
    backend["fail_new_health"] = True
    backend_path.write_text(json.dumps(backend))
    # Force a real failed forward transaction, then interrupt its actual rollback
    # after restoring the updater. Recovery must now use that installed reader.
    with reader_process(root_publication, cut="restore-updater-after-replace") as reader:
        assert reader.message() == {"bridge_cut": "restore-updater-after-replace", "phase": "recover"}
        transaction = json.loads((state / "transaction.json").read_text())
        assert "broker did not become healthy while drained" in transaction["error"]
        assert transaction["previous"]["park_proof.py"] == STRICT_PROOF_SHA256
        assert leaf.read_bytes() == retained
        assert (install / "upgrade_client.py").read_bytes() == previous_reader
        reader.kill()
    restored = _fresh(root_publication)
    assert restored["exception"] is None, restored
    assert restored["result"]["state"] == "rolled_back", restored
    assert restored["executed_sha256"] == _sha(previous_reader)
    assert not (state / "transaction.json").exists()
    assert leaf.read_bytes() == retained
    observed = _fresh(root_publication, "trust")
    assert observed["exception"] is None, observed
    assert observed["result"]["trusted"] is (kind == "trusted"), (
        "rollback must not turn worker-controlled retained bytes into trusted root code", observed)
    assert _file_facts(leaf) == PRIOR_FILES[kind]
    backend = json.loads(backend_path.read_text())
    assert backend["holder"] is None and backend["running"] is True and backend["active"] == 0


@pytest.mark.parametrize("kind", ["worker-writable", "worker-owned"])
def test_owned_zero_scope_repair_replaces_unknown_proof_without_receipt_match_or_park_markers(root_publication, kind):
    generation, _, _, config_path, backend_path, _ = root_publication
    leaf, _ = _retain_proof(root_publication, kind)
    config = json.loads(config_path.read_text())
    Path(config["maintenance_gate"]).write_text("unreadable prior gate identity")
    assert not (Path(config["maintenance_gate"]).parent / "rollout/parked").exists()
    repaired = _fresh(root_publication)
    assert repaired["exception"] is None, repaired
    assert repaired["result"]["state"] == "updated", repaired
    assert leaf.read_bytes() == (generation / "src/prismabuild/park_proof.py").read_bytes()
    assert _file_facts(leaf) == (0, 0, 0o644)
    assert _fresh(root_publication, "trust")["result"]["trusted"] is True
    backend = json.loads(backend_path.read_text())
    assert backend["holder"] is None and backend["running"] is True and backend["active"] == 0


def test_legacy_journal_without_previous_proof_metadata_refuses_before_service_or_file_mutation(root_publication):
    _, install, state, _, backend_path, _ = root_publication
    leaf, _ = _retain_proof(root_publication, "worker-owned")
    with reader_process(root_publication, cut="journal-durable") as reader:
        assert reader.message() == {"bridge_cut": "journal-durable", "phase": "forward"}
        reader.kill()
    journal = state / "transaction.json"
    transaction = json.loads(journal.read_text())
    assert transaction["previous"]["park_proof.py"] == STRICT_PROOF_SHA256
    transaction.pop("previous_proof_file", None)
    journal.write_text(json.dumps(transaction))
    before = member_bytes(install), _file_facts(leaf)
    operations = json.loads(backend_path.read_text())["operations"]
    observed = _fresh(root_publication)
    assert observed["exception"] in ("ValueError", "RuntimeError"), observed
    assert "proof" in observed["error"].lower(), observed
    assert (member_bytes(install), _file_facts(leaf)) == before
    assert json.loads(backend_path.read_text())["operations"] == operations
    assert journal.exists()


@pytest.mark.parametrize("incapable", ["previous", "candidate"])
def test_unpublished_protocol_one_cannot_participate_in_proof_metadata_transaction(root_publication, incapable):
    generation, install, state, _, backend_path, _ = root_publication
    fixed = fixed_source_artifact(
        "protocol-one-upgrader.py", PROTOCOL_ONE_UPDATER, "tools/fleet/upgrade_client.py",
        PROTOCOL_ONE_UPDATER_SHA256)
    assert _sha(fixed) == "be33c1ad42df95da03322aa73ccd48d0ee086ed24829a9975871662cf0acdb1e"
    if incapable == "previous":
        (install / "upgrade_client.py").write_bytes(fixed)
    else:
        (generation / "tools/upgrade_client.py").write_bytes(fixed)
        receipt_path = generation / "RUNTIME_VERSION.json"
        receipt = json.loads(receipt_path.read_text())
        receipt["files"]["tools/upgrade_client.py"] = PROTOCOL_ONE_UPDATER_SHA256
        receipt_path.write_text(json.dumps(receipt))
    target = install / "upgrade_client.py" if incapable == "previous" else generation / "tools/upgrade_client.py"
    assert target.read_bytes() == fixed
    before = member_bytes(install)
    observed = _fresh(root_publication, "direct" if incapable == "previous" else "run")
    assert observed["exception"] == "ValueError", observed
    assert "proof" in observed["error"].lower() and "protocol" in observed["error"].lower(), observed
    assert not (state / "transaction.json").exists() and observed["journal_exists"] is False
    assert member_bytes(install) == before
    operations = json.loads(backend_path.read_text())["operations"]
    assert not set(operations) & {"maintenance_begin", "stop", "start", "maintenance_end"}
