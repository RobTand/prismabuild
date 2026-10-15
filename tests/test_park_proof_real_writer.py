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

                census = [_census_entry(process.pid)]
                client = upgrade_client.Upgrader(
                    {"install_dir": str(tmp_path / "installed"),
                     "state_dir": str(tmp_path / "state"),
                     "maintenance_gate": str(gate)}, procs=lambda: census)
                yield SimpleNamespace(post=post, census=census, client=client,
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


@pytest.mark.parametrize("script,host_requires,member_requires", [
    ("worker_loop.py", True, True),
    ("worker.py", True, False),
    ("prewarm_loop.py", True, False),
    ("tier_loop.py", False, False),
    ("local_tier_loop.py", False, False),
    ("pbmetrics.py", False, False),
])
def test_existing_host_and_member_profiles_select_different_roles(
        parked_role, script, host_requires, member_requires):
    with parked_role(script) as role:
        pid, start, _ = role.census[0]
        assert upgrade_client.serving(role.census) == (
            [(pid, start)] if host_requires else [])
        assert supervise._is_fleet_loop(pid, [role.runtime]) is member_requires


def test_unrelated_missing_and_malformed_receipts_do_not_block_physical_park_proof(
        parked_role):
    with parked_role() as role:
        assert not (role.gate.parent / "missing-loaded.json").exists()
        assert (role.gate.parent / "malformed-published.json").read_text() == "{not JSON"
        role.post()
        assert _observations(role) == (
            {"drained": True, "unparked": [], "active_scopes": 0}, True)
