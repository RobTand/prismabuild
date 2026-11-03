"""Byte-bound snapshots and fresh installed readers at durable copy cuts."""
import json

import pytest

from client_proof_bridge_fixture import (
    BROKER_LOADED, PREBRIDGE, PREBRIDGE_UPDATER_SHA256,
    _fresh, _sha, capable_publication, capable_reader_artifact, fixed_source_artifact,
    member_bytes, publication, reader_process,
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
    with pytest.MonkeyPatch.context() as patches:
        _protocol_diagnostics(publication[2].parent, patches)
    with reader_process(publication, cut=cut) as reader:
        assert reader.message() == {"bridge_cut": cut, "phase": "forward"}
        reader.kill()


def _protocol_diagnostics(tmp_path, monkeypatch):
    """Exercise the transport with real children, without adding collected cases.

    The first child cannot dispatch a cut until the receiver is installed and
    the parent accepts readiness. Advancing the legacy select clock to its
    watchdog is deterministic: no sleep or scheduler-speed assertion is used.
    The historical-reader checks below still run separately and unmodified.
    """
    import select

    state = tmp_path / "protocol-state"
    state.mkdir(exist_ok=True)
    harness = tmp_path / "protocol-child.py"
    stub = (None, None, state, tmp_path / "config", tmp_path / "backend", harness)
    prelude = '''import json
from pathlib import Path
import sys
control = Path(sys.argv[4])
controls = json.loads(control.read_text())
print(json.dumps({"bridge_ready": True}), flush=True)
assert sys.stdin.readline().strip() == "start"
assert Path(str(control) + ".receiver").is_file()
'''
    cut = 'print(json.dumps({"bridge_cut": "updater-before-replace", "phase": "forward"}), flush=True)\n'
    outcome = {"exception": None, "result": {"state": "updated"}}
    harness.write_text(prelude + cut + '''assert sys.stdin.readline().strip() == "continue"
print(json.dumps({"bridge_resumed": "updater-before-replace", "phase": "forward"}), flush=True)
print(json.dumps({"test_outcome": ''' + repr(outcome) + '''}), flush=True)
''')
    with monkeypatch.context() as clock:
        # Expire only the old per-call select watchdog. The new queue protocol
        # does not use select; its receiver is installed before dispatch.
        clock.setattr(select, "select", lambda *args: ([], [], []))
        with reader_process(stub, cut="updater-before-replace") as reader:
            assert reader.message() == {"bridge_cut": "updater-before-replace", "phase": "forward"}
            with pytest.raises(AssertionError, match="needs resume at acknowledged cut"):
                reader.finish()
            reader.resume()
            assert reader.finish() == outcome

    # EOF before readiness and EOF before a requested cut both propagate the
    # real exit status and stderr, rather than waiting for a boundary watchdog.
    for startup in ("", prelude):
        harness.write_text('import sys\n' + startup
                           + 'print("early-exit diagnostic", file=sys.stderr, flush=True)\nsys.exit(23)\n')
        with pytest.raises(AssertionError) as failure:
            with reader_process(stub, cut="updater-before-replace") as reader:
                reader.message()
        assert "23" in str(failure.value) and "early-exit diagnostic" in str(failure.value)
        assert "updater-before-replace" in str(failure.value)

    for actual in ({"bridge_cut": "wrong-cut", "phase": "forward"},
                   {"bridge_cut": "updater-before-replace", "phase": "recover"}):
        harness.write_text(prelude + 'print(json.dumps(' + repr(actual) + '), flush=True)\nsys.stdin.readline()\n')
        with pytest.raises(AssertionError) as failure:
            with reader_process(stub, cut="updater-before-replace") as reader:
                reader.message()
        assert actual["bridge_cut"] in str(failure.value)
        assert actual["phase"] in str(failure.value)

    harness.write_text(prelude + cut + '''assert sys.stdin.readline().strip() == "continue"
print("resume diagnostic", file=sys.stderr, flush=True)
sys.exit(24)
''')
    with pytest.raises(AssertionError) as failure:
        with reader_process(stub, cut="updater-before-replace") as reader:
            reader.message()
            reader.resume()
            reader.finish()
    assert "24" in str(failure.value) and "resume diagnostic" in str(failure.value)

    unexpected = {"test_outcome": {"exception": "RuntimeError", "error": "resume refused"}}
    harness.write_text(prelude + cut + '''assert sys.stdin.readline().strip() == "continue"
print(json.dumps(''' + repr(unexpected) + '''), flush=True)
''')
    with pytest.raises(AssertionError) as failure:
        with reader_process(stub, cut="updater-before-replace") as reader:
            reader.message()
            reader.resume()
    assert "RuntimeError" in str(failure.value) and "resume refused" in str(failure.value)

    refused = {"exception": "RuntimeError", "error": "restore diagnostic", "result": None}
    harness.write_text(prelude + 'print(json.dumps({"test_outcome": ' + repr(refused) + '}), flush=True)\n')
    with reader_process(stub, cut="restore-updater-before-replace") as reader:
        observed = reader.finish()
    with pytest.raises(AssertionError) as failure:
        _restored(stub, {}, observed)
    assert "RuntimeError" in str(failure.value) and "restore diagnostic" in str(failure.value)


@pytest.mark.parametrize("changed", ["previous", "candidate"])
def test_capability_digest_and_backup_bind_the_same_captured_bytes(
        capable_publication, capable_reader_artifact, changed):
    generation, install, state, _, backend_path, _ = capable_publication
    previous_sha256 = capable_reader_artifact[2]
    before = member_bytes(install)
    assert _sha(before["upgrade_client.py"]) == previous_sha256
    candidate = (generation / "tools/upgrade_client.py").read_bytes()
    incapable = fixed_source_artifact(
        "prebridge-upgrader.py", PREBRIDGE, "tools/fleet/upgrade_client.py", PREBRIDGE_UPDATER_SHA256)
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
    previous = fixed_source_artifact(
        "legacy-proof.py", PREBRIDGE, "src/prismabuild/park_proof.py",
        "7b951734fe4471ab012fe715cda238be1dd4c7d83c8f15c39447e1b75009bfe7")
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
