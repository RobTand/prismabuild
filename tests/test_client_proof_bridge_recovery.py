"""Byte-bound snapshots and fresh installed readers at durable copy cuts."""
import json
import subprocess

import pytest

from client_proof_bridge_fixture import (
    BROKER_LOADED, PREBRIDGE, PREBRIDGE_UPDATER_SHA256,
    ROOT, _fresh, _sha, capable_publication, capable_reader_artifact, member_bytes,
    publication, reader_process,
)


def _restored(publication, before, observed):
    _, install, state, _, backend_path, _ = publication
    assert observed["exception"] is None, observed
    assert observed["result"]["state"] == "rolled_back", observed
    assert member_bytes(install) == before
    assert (install / "park_proof.py").exists() is (before["park_proof.py"] is not None)
    assert not (state / "transaction.json").exists()
    backend = json.loads(backend_path.read_text())
    assert backend["running"] is True and backend["holder"] is None
    assert backend["loaded"] == {name: _sha(before[name]) for name in BROKER_LOADED}


def _interrupt(publication, cut):
    with reader_process(publication, cut=cut) as reader:
        assert reader.message() == {"bridge_cut": cut, "phase": "forward"}
        reader.kill()


@pytest.mark.parametrize("changed", ["previous", "candidate"])
def test_capability_digest_and_backup_bind_the_same_captured_bytes(
        capable_publication, capable_reader_artifact, changed):
    generation, install, state, _, backend_path, _ = capable_publication
    previous_sha256 = capable_reader_artifact[2]
    before = member_bytes(install)
    assert _sha(before["upgrade_client.py"]) == previous_sha256
    candidate = (generation / "tools/upgrade_client.py").read_bytes()
    incapable = subprocess.check_output(
        ["git", "show", f"{PREBRIDGE}:tools/fleet/upgrade_client.py"], cwd=ROOT)
    assert _sha(incapable) == PREBRIDGE_UPDATER_SHA256
    replacement = state / "incapable-reader.py"
    replacement.write_bytes(incapable)
    observation = state / "read-observation.json"
    controls = {"cut": "journal-durable", "observation": str(observation),
                f"swap_{changed}": str(replacement)}
    with reader_process(capable_publication, **controls) as reader:
        boundary = reader.message()
        if "test_outcome" in boundary:
            refused = boundary["test_outcome"]
            assert refused["exception"] in ("ValueError", "RuntimeError"), refused
            assert not (state / "transaction.json").exists()
            expected = {**before, "upgrade_client.py": incapable} if changed == "previous" else before
            assert member_bytes(install) == expected
            operations = json.loads(backend_path.read_text())["operations"]
            assert not set(operations) & {"maintenance_begin", "stop", "start", "maintenance_end"}
            return
        assert boundary == {"bridge_cut": "journal-durable", "phase": "forward"}
        journal = json.loads((state / "transaction.json").read_text())
        assert journal["previous"]["park_proof.py"] is None
        assert not (state / "previous/park_proof.py").exists()
        assert journal["previous"]["upgrade_client.py"] == previous_sha256
        assert (state / "previous/upgrade_client.py").read_bytes() == before["upgrade_client.py"]
        assert journal["desired"]["files"]["upgrade_client.py"] == _sha(candidate)
        assert (state / "staged/upgrade_client.py").read_bytes() == candidate
        if changed == "previous":
            assert json.loads(observation.read_text()) == {
                "captured_sha256": previous_sha256,
                "replacement_sha256": PREBRIDGE_UPDATER_SHA256}
            assert (install / "upgrade_client.py").read_bytes() == incapable
        else:
            assert (generation / "tools/upgrade_client.py").read_bytes() == incapable
        reader.resume()
        completed = reader.finish()
    assert completed["exception"] is None, completed
    assert completed["result"]["state"] == "updated", completed
    assert (install / "upgrade_client.py").read_bytes() == candidate


@pytest.mark.parametrize("cut", [
    "journal-durable", "leaf-before-replace", "leaf-after-replace",
    "updater-before-replace", "updater-after-replace", "install-after-sync",
])
def test_first_introduction_recovers_through_the_actual_installed_reader(capable_publication, cut):
    _, install, state, _, _, _ = capable_publication
    before = member_bytes(install)
    assert before["park_proof.py"] is None
    _interrupt(capable_publication, cut)
    journal = json.loads((state / "transaction.json").read_text())
    assert journal["previous"]["park_proof.py"] is None
    assert "park_proof.py" in journal["desired"]["files"]
    _restored(capable_publication, before, _fresh(capable_publication))


@pytest.mark.parametrize("cut", ["leaf-after-replace", "updater-after-replace"])
def test_a_capable_bridge_restores_exact_previous_proof_presence(capable_publication, cut):
    _, install, _, _, _, _ = capable_publication
    previous = subprocess.check_output(
        ["git", "show", f"{PREBRIDGE}:src/prismabuild/park_proof.py"], cwd=ROOT)
    assert _sha(previous) == "7b951734fe4471ab012fe715cda238be1dd4c7d83c8f15c39447e1b75009bfe7"
    (install / "park_proof.py").write_bytes(previous)
    before = member_bytes(install)
    _interrupt(capable_publication, cut)
    assert (install / "park_proof.py").read_bytes() != previous
    _restored(capable_publication, before, _fresh(capable_publication))


@pytest.mark.parametrize("cut", [
    "restore-leaf-before-absence", "restore-leaf-after-absence",
    "restore-updater-before-replace", "restore-updater-after-replace",
])
def test_interrupted_recovery_restarts_from_the_reader_actually_left_installed(capable_publication, cut):
    _, install, state, _, _, _ = capable_publication
    before = member_bytes(install)
    _interrupt(capable_publication, "updater-after-replace")
    assert (install / "park_proof.py").exists()
    with reader_process(capable_publication, cut=cut) as reader:
        assert reader.message() == {"bridge_cut": cut, "phase": "recover"}
        if cut == "restore-leaf-after-absence":
            assert not (install / "park_proof.py").exists()
        assert (state / "transaction.json").exists()
        reader.kill()
    _restored(capable_publication, before, _fresh(capable_publication))


def test_corrupt_previous_bytes_refuse_before_stopping_the_service(capable_publication):
    _, install, state, _, backend_path, _ = capable_publication
    before = member_bytes(install)
    _interrupt(capable_publication, "journal-durable")
    backup = state / "previous/upgrade_client.py"
    assert backup.read_bytes() == before["upgrade_client.py"]
    backup.write_bytes(b"corrupt previous updater")
    prior_operations = json.loads(backend_path.read_text())["operations"]
    refused = _fresh(capable_publication)
    assert refused["exception"] == "RuntimeError", refused
    assert "previous client hash mismatch" in refused["error"]
    assert (state / "transaction.json").exists()
    assert json.loads(backend_path.read_text())["operations"] == prior_operations
    backup.write_bytes(before["upgrade_client.py"])
    _restored(capable_publication, before, _fresh(capable_publication))


def test_reopened_work_prevents_a_fresh_recovery_from_stopping_the_service(capable_publication):
    _, install, state, _, backend_path, _ = capable_publication
    before = member_bytes(install)
    with reader_process(capable_publication, cut="journal-before-clear") as reader:
        assert reader.message() == {"bridge_cut": "journal-before-clear", "phase": "forward"}
        backend = json.loads(backend_path.read_text())
        assert backend["running"] is True and backend["holder"] is None
        backend["active"] = 1
        prior_count = len(backend["operations"])
        backend_path.write_text(json.dumps(backend))
        reader.kill()
    refused = _fresh(capable_publication)
    assert refused["exception"] == "RuntimeError", refused
    assert "still owns active work" in refused["error"]
    backend = json.loads(backend_path.read_text())
    assert backend["running"] is True and backend["active"] == 1
    assert "stop" not in backend["operations"][prior_count:]
    assert (state / "transaction.json").exists()
    backend["active"] = 0
    backend_path.write_text(json.dumps(backend))
    _restored(capable_publication, before, _fresh(capable_publication))
