"""Real proof filesystem and journal boundaries under the admitted root fixture."""
import json
import os
from pathlib import Path
import runpy

import pytest

from client_proof_bridge_fixture import ROOT, _fresh, member_bytes, publication, reader_process
from test_client_proof_prior_trust import (
    PRIOR_FILES, STRICT_PROOF_SHA256, _file_facts, _retain_proof, root_publication, tmp_path,
)


@pytest.mark.parametrize("kind", ["worker-writable", "worker-owned"])
def test_path_replacement_cannot_pair_previous_bytes_with_replacement_metadata(root_publication, kind, monkeypatch):
    _, install, _, _, _, _ = root_publication
    leaf, retained = _retain_proof(root_publication, kind)
    replacement = install / "proof-replacement"
    replacement.write_bytes((ROOT / "tools/fleet/upgrade_client.py").read_bytes())
    replacement.chmod(0o644)
    program = runpy.run_path(str(install / "upgrade_client.py"), run_name="metadata_reader")
    real_open = os.open
    def opened(path, flags, *args, **kwargs):
        fd = real_open(path, flags, *args, **kwargs)
        if Path(path) == leaf:
            os.replace(replacement, leaf)
        return fd
    with monkeypatch.context() as scope:
        scope.setattr(os, "open", opened)
        captured = program["read_proof_file"](leaf)
    assert captured.data == retained
    assert tuple(captured.metadata) == PRIOR_FILES[kind]
    assert leaf.read_bytes() != retained and _file_facts(leaf) == (0, 0, 0o644)
    assert _fresh(root_publication, "trust")["result"]["trusted"] is True


@pytest.mark.parametrize("kind", ["symlink", "dangling-symlink", "directory", "fifo", "oversized"])
def test_unsafe_or_unbounded_proof_refuses_before_drain_journal_or_service(root_publication, kind):
    _, install, state, _, backend_path, _ = root_publication
    leaf = install / "park_proof.py"
    if kind == "symlink":
        leaf.symlink_to(ROOT / "src/prismabuild/park_proof.py")
    elif kind == "dangling-symlink":
        leaf.symlink_to(install / "missing-proof")
    elif kind == "directory":
        leaf.mkdir()
    elif kind == "fifo":
        os.mkfifo(leaf)
    else:
        leaf.write_bytes(b"x" * (4 * 1024 * 1024 + 1))
    before = leaf.lstat()
    observed = _fresh(root_publication)
    assert observed["exception"] in ("ValueError", "OSError"), observed
    after = leaf.lstat()
    assert all(getattr(after, name) == getattr(before, name) for name in (
        "st_dev", "st_ino", "st_uid", "st_gid", "st_mode", "st_size", "st_mtime_ns", "st_ctime_ns"))
    assert not (state / "transaction.json").exists()
    operations = json.loads(backend_path.read_text())["operations"]
    assert not set(operations) & {"maintenance_begin", "stop", "start", "maintenance_end"}


@pytest.mark.parametrize("damage", [
    "null-present", "uid-bool", "gid-bool", "uid-negative", "gid-sentinel",
    "uid-overflow", "mode-bool", "mode-negative", "mode-type-bits", "missing-mode",
    "extra-field", "metadata-for-absence", "missing-previous-member",
])
def test_malformed_previous_metadata_refuses_before_recovery_io(root_publication, damage):
    _, install, state, _, backend_path, _ = root_publication
    if damage != "metadata-for-absence":
        _retain_proof(root_publication, "worker-owned")
    with reader_process(root_publication, cut="journal-durable") as reader:
        assert reader.message() == {"bridge_cut": "journal-durable", "phase": "forward"}
        reader.kill()
    journal = state / "transaction.json"
    transaction = json.loads(journal.read_text())
    changes = {
        "uid-bool": ("uid", True), "gid-bool": ("gid", False),
        "uid-negative": ("uid", -1), "gid-sentinel": ("gid", 2**32 - 1),
        "uid-overflow": ("uid", 2**100), "mode-bool": ("mode", True),
        "mode-negative": ("mode", -1), "mode-type-bits": ("mode", 0o10000),
    }
    if damage in changes:
        name, value = changes[damage]
        transaction["previous_proof_file"][name] = value
    elif damage == "null-present":
        transaction["previous_proof_file"] = None
    elif damage == "missing-mode":
        transaction["previous_proof_file"].pop("mode")
    elif damage == "extra-field":
        transaction["previous_proof_file"]["trusted"] = True
    elif damage == "missing-previous-member":
        transaction["previous"].pop("park_proof.py")
        transaction.pop("previous_proof_file")
    else:
        assert transaction["previous"]["park_proof.py"] is None
        transaction["previous_proof_file"] = {"uid": 0, "gid": 0, "mode": 0o644}
    journal.write_text(json.dumps(transaction))
    before = member_bytes(install)
    leaf = install / "park_proof.py"
    facts = _file_facts(leaf) if leaf.exists() else None
    operations = json.loads(backend_path.read_text())["operations"]
    observed = _fresh(root_publication)
    assert observed["exception"] == "ValueError", observed
    assert "proof" in observed["error"].lower(), observed
    assert member_bytes(install) == before
    assert (_file_facts(leaf) if leaf.exists() else None) == facts
    assert json.loads(backend_path.read_text())["operations"] == operations
    assert journal.exists()


def test_known_previous_absence_is_explicit_and_recovers_through_fresh_reader(root_publication):
    _, install, state, _, _, _ = root_publication
    assert not (install / "park_proof.py").exists()
    with reader_process(root_publication, cut="journal-durable") as reader:
        assert reader.message() == {"bridge_cut": "journal-durable", "phase": "forward"}
        reader.kill()
    transaction = json.loads((state / "transaction.json").read_text())
    assert transaction["previous"]["park_proof.py"] is None
    assert "previous_proof_file" in transaction and transaction["previous_proof_file"] is None
    observed = _fresh(root_publication)
    assert observed["exception"] is None, observed
    assert observed["result"]["state"] == "rolled_back", observed
    assert not (install / "park_proof.py").exists() and not (state / "transaction.json").exists()


def test_rollback_restores_all_permission_bits_on_temp_before_replace(root_publication):
    _, install, state, _, backend_path, _ = root_publication
    leaf, retained = _retain_proof(root_publication, "worker-owned")
    leaf.chmod(0o7640)
    backend = json.loads(backend_path.read_text())
    backend["fail_new_health"] = True
    backend_path.write_text(json.dumps(backend))
    observation = state / "restore-proof-temp.json"
    with reader_process(root_publication, cut="restore-leaf-before-replace",
                        restore_observation=str(observation)) as reader:
        assert reader.message() == {"bridge_cut": "restore-leaf-before-replace", "phase": "recover"}
        actual = json.loads(observation.read_text())
        temporary = Path(actual.pop("source"))
        assert actual == {"uid": 65534, "gid": 65533, "mode": 0o7640,
                          "sha256": STRICT_PROOF_SHA256}
        assert temporary.parent == install and temporary != leaf
        assert _file_facts(temporary) == (65534, 65533, 0o7640)
        assert _file_facts(leaf) == (0, 0, 0o644)
        reader.kill()
    restored = _fresh(root_publication)
    assert restored["exception"] is None, restored
    assert restored["result"]["state"] == "rolled_back", restored
    assert leaf.read_bytes() == retained and _file_facts(leaf) == (65534, 65533, 0o7640)
    assert _fresh(root_publication, "trust")["result"]["trusted"] is False
