"""Private process markers read by the host and membership proof observers."""
from __future__ import annotations

from contextlib import contextmanager
import json
import os
from pathlib import Path
import select
import socket
import subprocess
import sys
from types import SimpleNamespace

import pytest

import fleet_membership
import supervise
import upgrade_client
from prismabuild import park_proof

from test_client_proof_prior_trust import tmp_path
from test_installed_park_proof_loader import installed_leaf
from test_prewarm_rollout import load, role

ROOT = Path(__file__).resolve().parents[1]
WRITER = """\
import json
from pathlib import Path
import sys
from prismabuild import role_session

role_session.GENERATION_VERSION = Path(sys.argv[1])
role_session.RUNTIME_VERSION = Path(sys.argv[2])
print('ready', flush=True)
for command in sys.stdin:
    assert command.strip() == 'post'
    marker = role_session.post_park_marker(role_session.read_maintenance_gate())
    print(json.dumps(None if marker is None else marker.name), flush=True)
"""


def _line(process, log):
    readable, _, _ = select.select([process.stdout], [], [], 5)
    assert readable, f"writer did not answer: {log.read_text()}"
    line = process.stdout.readline()
    assert line, f"writer exited {process.poll()}: {log.read_text()}"
    return line.rstrip("\n")


def _census_entry(pid):
    directory = Path(f"/proc/{pid}")
    before = (directory / "stat").read_text().rpartition(")")[2].split()[19]
    argv = [part.decode() for part in (directory / "cmdline").read_bytes().split(b"\0")
            if part]
    after = (directory / "stat").read_text().rpartition(")")[2].split()[19]
    assert before == after
    return pid, before, argv


@pytest.fixture
def parked_role(tmp_path):
    runtime = tmp_path / "runtime"
    runtime.mkdir()
    gate = tmp_path / "maintenance.json"
    parked = tmp_path / "rollout" / "parked"
    missing_receipt = tmp_path / "missing-loaded.json"
    malformed_receipt = tmp_path / "malformed-published.json"
    malformed_receipt.write_text("{not JSON")
    install = tmp_path / "installed"
    install.mkdir(mode=0o755)
    install.chmod(0o755)
    proof = install / "park_proof.py"
    proof_bytes = (ROOT / "src/prismabuild/park_proof.py").read_bytes()
    proof.write_bytes(proof_bytes)
    proof.chmod(0o644)
    info = proof.lstat()
    assert (info.st_uid, info.st_gid, info.st_mode & 0o7777) == (0, 0, 0o644)
    assert upgrade_client.trusted(proof) == proof

    def set_gate(stamp):
        gate.write_text(json.dumps({"draining": True, "changed_unix": stamp}))

    @contextmanager
    def spawn(script="worker_loop.py", *, writable=True):
        path = runtime / script
        path.write_text(WRITER)
        log = tmp_path / "writer.log"
        if writable:
            parked.mkdir(parents=True, exist_ok=True)
        with log.open("w") as errors:
            process = subprocess.Popen(
                [sys.executable, str(path), str(missing_receipt), str(malformed_receipt)],
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=errors,
                text=True, start_new_session=True,
                env={**os.environ, "PYTHONPATH": str(ROOT / "src"),
                     "PRISMABUILD_MAINTENANCE_GATE": str(gate),
                     supervise.OWNERSHIP_ENV: socket.gethostname()})
            try:
                assert _line(process, log) == "ready"

                def post():
                    process.stdin.write("post\n")
                    process.stdin.flush()
                    return json.loads(_line(process, log))

                def stop():
                    process.stdin.close()
                    process.wait(timeout=5)
                    assert process.returncode == 0, log.read_text()

                census = [_census_entry(process.pid)]
                client = upgrade_client.Upgrader(
                    {"install_dir": str(tmp_path / "installed"),
                     "state_dir": str(tmp_path / "state"),
                     "maintenance_gate": str(gate)}, procs=lambda: census)
                yield SimpleNamespace(post=post, stop=stop, census=census, client=client,
                                      gate=gate, parked=parked, runtime=runtime,
                                      set_gate=set_gate)
            finally:
                process.stdin.close()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
                process.stdout.close()
        assert process.returncode == 0, log.read_text()

    set_gate(101.5)
    return spawn


def _observations(role):
    host = role.client.drain_evidence({"draining": True, "active_scopes": 0})
    gate = fleet_membership.read_gate(role.gate)
    census = [(pid, start) for pid, start, _ in role.census]
    expected = fleet_membership.expected_markers(census, gate)
    actual = {path.name for path in fleet_membership.parked_markers(role.parked, gate)}
    return host, expected <= actual


@pytest.mark.parametrize("stamp", [101, 101.5])
def test_real_writer_exact_bytes_are_idempotent_and_accepted_by_both_readers(
        parked_role, stamp):
    with parked_role() as role:
        role.set_gate(stamp)
        pid, start, _ = role.census[0]
        expected = f"{pid}-{start}-{stamp}"
        assert role.post() == expected
        marker = role.parked / expected
        assert marker.read_bytes() == b""
        first = marker.stat()
        assert role.post() == expected
        second = marker.stat()
        assert (second.st_ino, second.st_mtime_ns) == (first.st_ino, first.st_mtime_ns)
        assert list(role.parked.iterdir()) == [marker]
        assert _observations(role) == (
            {"drained": True, "unparked": [], "active_scopes": 0}, True)


def test_previous_drain_marker_cannot_acknowledge_the_current_drain(parked_role):
    with parked_role() as role:
        old = role.post()
        role.set_gate(202.5)
        assert _observations(role) == (
            {"drained": False, "unparked": [role.census[0][0]], "active_scopes": 0},
            False)
        assert role.post() != old
        assert _observations(role) == (
            {"drained": True, "unparked": [], "active_scopes": 0}, True)


def test_marker_does_not_acknowledge_a_different_start_tick(parked_role):
    with parked_role() as role:
        role.post()
        pid, start, argv = role.census[0]
        role.census[:] = [(pid, str(int(start) + 1), argv)]
        assert _observations(role) == (
            {"drained": False, "unparked": [pid], "active_scopes": 0}, False)


def test_failed_marker_write_cannot_acknowledge_until_the_directory_is_repaired(
        parked_role):
    with parked_role(writable=False) as role:
        assert role.post() is None
        assert _observations(role) == (
            {"drained": False, "unparked": [role.census[0][0]], "active_scopes": 0},
            False)
        role.parked.mkdir(parents=True)
        assert role.post() is not None
        assert _observations(role) == (
            {"drained": True, "unparked": [], "active_scopes": 0}, True)


@pytest.mark.parametrize("script,required", [
    ("worker_loop.py", True),
    ("worker.py", True),
    ("prewarm_loop.py", True),
    ("tier_loop.py", True),
    ("local_tier_loop.py", True),
    ("pbmetrics.py", False),
])
def test_work_storage_roles_require_actual_park_or_exit_for_both_proofs(
        parked_role, monkeypatch, script, required):
    with parked_role(script) as role:
        pid, start, _ = role.census[0]
        role.client.procs = None
        census, error = fleet_membership.live_loop_census()
        assert error is None
        assert ((pid, start) in census) is required
        assert role.client.drain_evidence({"draining": True, "active_scopes": 0}) == {
            "drained": not required,
            "unparked": [pid] if required else [],
            "active_scopes": 0,
        }
        gate = fleet_membership.read_gate(role.gate)
        markers = {p.name for p in fleet_membership.parked_markers(role.parked, gate)}
        before = park_proof.acknowledgements(
            processes=census, markers=markers, gate_before=gate, gate_after=gate)
        assert before.complete is (not required)
        assert before.unparked == ((pid,) if required else ())
        if script == "worker.py":
            assert markers == set()
            role.stop()
            assert not Path(f"/proc/{pid}").exists()
        elif required:
            assert role.post() is not None
        census, error = fleet_membership.live_loop_census()
        assert error is None
        after_gate = fleet_membership.read_gate(role.gate)
        markers = {p.name for p in fleet_membership.parked_markers(role.parked, after_gate)}
        after = park_proof.acknowledgements(
            processes=census, markers=markers, gate_before=gate, gate_after=after_gate)
        assert after.complete
        assert role.client.drain_evidence({"draining": True, "active_scopes": 0}) == {
            "drained": True, "unparked": [], "active_scopes": 0,
        }


def test_unrelated_missing_and_malformed_receipts_do_not_block_physical_park_proof(
        parked_role):
    with parked_role() as role:
        assert not (role.gate.parent / "missing-loaded.json").exists()
        assert (role.gate.parent / "malformed-published.json").read_text() == "{not JSON"
        role.post()
        assert _observations(role) == (
            {"drained": True, "unparked": [], "active_scopes": 0}, True)


def test_work_already_running_holds_the_drain_open(parked_role):
    with parked_role() as role:
        role.post()
        assert role.client.drain_evidence({"draining": True, "active_scopes": 1}) == {
            "drained": False, "unparked": [], "active_scopes": 1}


def test_actual_writer_acknowledges_only_the_exact_census_and_current_gate(parked_role):
    with parked_role() as role:
        name = role.post()
        assert name is not None
        gate = park_proof.read_gate(role.gate)
        pid, start, _ = role.census[0]
        def observe(processes, *, after=gate):
            return park_proof.acknowledgements(
                processes=processes, markers={name},
                gate_before=gate, gate_after=after)

        assert observe([(pid, start)]).complete is True
        replaced = observe([(pid, str(int(start) + 1))])
        assert replaced.complete is False and replaced.unparked == (pid,)
        foreign = observe([(pid + 1, start)])
        assert foreign.complete is False and foreign.unparked == (pid + 1,)
        unknown = observe([(pid, "unknown")])
        assert unknown.complete is False and unknown.unknown
        assert observe([(True, start)]).complete is False
        assert observe([(pid, start)], after=None).complete is False


@pytest.mark.parametrize("pid,start", [("True", "1"), ("0", "1"), ("01", "1"),
                                       ("1", "unknown"), ("1", "١")])
def test_member_reader_rejects_malformed_retained_process_names(parked_role, pid, start):
    with parked_role() as role:
        name = role.post()
        assert name is not None
        marker = role.parked / name
        inode = marker.stat().st_ino
        retained = role.parked / f"{pid}-{start}-101.5"
        marker.rename(retained)
        assert retained.stat().st_ino == inode
        assert park_proof.parked_markers(role.parked, park_proof.read_gate(role.gate)) == []


@pytest.mark.usefixtures("installed_leaf")
def test_storage_boundary_marker_satisfies_the_updaters_drain_census(role):
    loop, gate, parked, current, calls, argv, stop = role
    upgrade = load('upgrade_client')
    gate.write_text(json.dumps({'draining': True, 'changed_unix': 458.0}))
    # Read the process identity independently of the marker writer.
    start = Path('/proc/self/stat').read_text().rpartition(')')[2].split()[19]
    client = upgrade.Upgrader({
        'install_dir': str(gate.parent / 'installed'),
        'state_dir': str(gate.parent / 'state'),
        'maintenance_gate': str(gate),
    }, procs=lambda: [(os.getpid(), start, ['python3', str(ROOT / 'tools/fleet/prewarm_loop.py')])])
    status = {'draining': True, 'active_scopes': 0}
    assert client.drain_evidence(status)['unparked'] == [os.getpid()]
    assert loop.main(argv + ['--once']) == 75
    assert calls == []
    assert client.drain_evidence(status) == {
        'drained': True, 'unparked': [], 'active_scopes': 0}
