"""A stopped-but-alive role is named, not silent (2026-09-19, #709).

The wedge-night forensics found sparky role pids under the supervisor's
parent partially SIGSTOPped from earlier diagnosis.  A stopped loop answers
``pgrep``, carries a readable ``cmdline`` and appears in every census as
present, so the supervisor counted it toward the role and spawned no
replacement while the storage service was silently down.  In the process
table a ``T`` process and a quiet one look alike; it took manual ``ps``
forensics to notice.  So the supervisor now reads the one field the kernel
keeps for exactly this, ``/proc/<pid>/stat``'s scheduler state, and names
every owned role whose reported health changes:

* ``T``/``t`` (job-control and tracing stops, what ``SIGSTOP`` leaves) is
  reported with pid, role and state on the transition, and a later healthy
  tick reports the state cleared;
* a state that cannot be read is "unreadable" -- never "stopped"; missing
  evidence is not a diagnosis;
* only loops that pass the full ownership proof are named, so nothing
  unrelated or foreign is ever inspected for health or signalled.

The stopped process here is a real child of this test that SIGSTOPs itself;
no live fleet process is inspected or signalled.
"""

from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
import time

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools" / "fleet"))

import supervise  # noqa: E402

HOST = "dl380g10"
#: Far outside any pid this box could hand out, so a mistake cannot land on a
#: real process even if a patch were to slip.
UNREADABLE = 1000000007
STOPPED_PID = 4242


def _wait_for(predicate, timeout: float = 10.0) -> bool:
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() >= deadline:
            return False
        time.sleep(0.02)
    return True


_SCRIPTS = {"storage": "prewarm_loop.py", "tiers": "tier_loop.py"}


def _stopped_entry(pid: int = STOPPED_PID, role: str = "storage") -> dict:
    return {"pid": pid, "role": role, "script": _SCRIPTS[role],
            "state": "T", "state_name": "stopped", "stopped": True}


def test_a_stopped_owned_role_is_named_with_pid_role_and_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The incident's own shape: alive, present, serving nothing.

    The child stops itself with SIGKILL-safe ``SIGSTOP``, so the state read
    is the kernel's and the only pid involved is this test's own.
    """

    child = subprocess.Popen(
        [sys.executable, "-c",
         "import os, signal, time; os.kill(os.getpid(), signal.SIGSTOP); "
         "time.sleep(60)"])
    try:
        assert _wait_for(lambda: supervise._proc_state(child.pid) == "T"), (
            "the test child never reached a stopped state")
        monkeypatch.setattr(
            supervise, "_live_role_loops",
            lambda script, proc_root=None: (
                [child.pid] if script == "prewarm_loop.py" else []))
        health = supervise.role_health([("storage", ["--readers", "4"])])

        assert len(health) == 1, health
        entry = health[0]
        assert (entry["pid"], entry["role"], entry["script"]) == (
            child.pid, "storage", "prewarm_loop.py")
        assert entry["state"] == "T"
        assert entry["state_name"] == "stopped"
        assert entry["stopped"] is True

        seen: dict[int, str] = {}
        lines = supervise.role_health_lines(HOST, health, seen)
        assert len(lines) == 1, lines
        assert f"role storage pid {child.pid}" in lines[0], lines[0]
        assert "stopped" in lines[0], lines[0]
        assert supervise.role_health_lines(HOST, health, seen) == [], (
            "a steady stopped state must not reprint every tick")
    finally:
        # SIGKILL is delivered to a stopped process without a continue.
        child.kill()
        child.wait(timeout=5)


def test_an_unreadable_state_is_unknown_never_stopped(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Missing evidence is not a diagnosis: no process, no state to read."""

    monkeypatch.setattr(supervise, "PROC", tmp_path / "proc")
    monkeypatch.setattr(
        supervise, "_live_role_loops",
        lambda script, proc_root=None: (
            [UNREADABLE] if script == "tier_loop.py" else []))
    health = supervise.role_health([("tiers", [])])

    assert len(health) == 1, health
    entry = health[0]
    assert entry["state"] is None
    assert entry["state_name"] == "unknown"
    assert entry["stopped"] is None, "unreadable is not stopped"

    lines = supervise.role_health_lines(HOST, health, {})
    assert len(lines) == 1 and "unreadable" in lines[0], lines
    assert f"role tiers pid {UNREADABLE}" in lines[0], lines[0]


def test_only_proven_owned_roles_enter_the_health_census(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ownership is the same proof every restart path makes, so a foreign
    ``prewarm_loop.py`` is never named as this box's role."""

    mirror = tmp_path / "fleet"
    generation = mirror / "runtime-generations" / "gen-live"
    (generation / "tools").mkdir(parents=True)
    (generation / "tools" / "prewarm_loop.py").write_text("# a loop\n")
    (generation / "RUNTIME_VERSION.json").write_text(
        json.dumps({"commit": "a" * 40, "generation": "gen-live"}))
    (mirror / "repo").symlink_to(generation)
    proc = tmp_path / "proc"
    proc.mkdir()
    monkeypatch.setattr(supervise, "MIRROR", mirror)
    monkeypatch.setattr(supervise, "PROC", proc)

    def _process(pid: int, argv: list[str], *, mark: str | None) -> int:
        directory = proc / str(pid)
        directory.mkdir()
        (directory / "cmdline").write_bytes(
            b"".join(part.encode() + b"\0" for part in argv))
        entries = ["HOME=/home/rob"]
        if mark is not None:
            entries.append(f"{supervise.OWNERSHIP_ENV}={mark}")
        (directory / "environ").write_bytes(
            b"".join(entry.encode() + b"\0" for entry in entries))
        (directory / "stat").write_bytes(
            f"{pid} (prewarm_loop.py) S 1 1 1".encode())
        return pid

    owned = _process(7001, [
        "/usr/bin/python3", str(generation / "tools" / "prewarm_loop.py"),
        "--readers", "4"], mark=HOST)
    foreign = _process(7002, [
        "/usr/bin/python3", "/another-project/prewarm_loop.py"], mark=HOST)
    _process(7003, [
        "/usr/bin/python3", str(generation / "tools" / "prewarm_loop.py")],
        mark=None)

    def fake_run(argv, *_args, **_kwargs):
        assert argv[0] == "pgrep", argv
        return subprocess.CompletedProcess(argv, 0, f"{owned}\n{foreign}\n7003", "")

    monkeypatch.setattr(supervise.subprocess, "run", fake_run)

    health = supervise.role_health([("storage", ["--readers", "4"])])

    assert [entry["pid"] for entry in health] == [owned]
    assert health[0]["state"] == "S"
    assert health[0]["stopped"] is False


def test_health_lines_report_the_transition_not_every_tick() -> None:
    """One line into the stopped/unknown state, one line out of it."""

    stopped = _stopped_entry()
    running = {**stopped, "state": "S", "state_name": "sleeping",
               "stopped": False}
    seen: dict[int, str] = {}

    assert len(supervise.role_health_lines(HOST, [stopped], seen)) == 1
    assert supervise.role_health_lines(HOST, [stopped], seen) == []
    cleared = supervise.role_health_lines(HOST, [running], seen)
    assert len(cleared) == 1 and "cleared" in cleared[0], cleared
    assert supervise.role_health_lines(HOST, [running], seen) == []


def test_a_zombie_or_unreadable_state_never_clears_an_alarm() -> None:
    """Only a state outside the alarm set is a return to health.

    A ``Z`` role is dead and awaiting reap, not serving, and a state the
    reader cannot get is unknown: neither may print the "cleared" line that
    says the role came back.
    """

    stopped = _stopped_entry()
    zombie = {**stopped, "state": "Z", "state_name": "zombie",
              "stopped": False}
    unreadable = {**stopped, "state": None, "state_name": "unknown",
                  "stopped": None}

    seen: dict[int, str] = {}
    assert len(supervise.role_health_lines(HOST, [stopped], seen)) == 1
    lines = supervise.role_health_lines(HOST, [zombie], seen)
    assert len(lines) == 1 and "zombie" in lines[0], lines
    assert "cleared" not in lines[0], lines
    lines = supervise.role_health_lines(HOST, [unreadable], seen)
    assert len(lines) == 1 and "unreadable" in lines[0], lines
    assert "cleared" not in lines[0], lines


def test_the_supervisor_tick_writes_a_stopped_role_into_its_log(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys,
) -> None:
    """The signal lands in the supervisor's own status output -- the log the
    installed unit appends to -- not in a place an operator must guess."""

    config = tmp_path / "fleet_boxes.json"
    config.write_text(json.dumps({"boxes": {HOST: {
        "loops": 1, "args": ["--class", "x86"],
        "roles": {"storage": ["--readers", "4"]}}}}))
    monkeypatch.setattr(supervise, "CONFIG", config)
    monkeypatch.setattr(supervise, "MIRROR", tmp_path / "absent")
    monkeypatch.setattr(supervise, "CLAIM", tmp_path / "supervisor.claim")
    monkeypatch.setattr(supervise, "LOG_DIR", tmp_path / "logs")
    monkeypatch.setattr(supervise.socket, "gethostname", lambda: HOST)
    monkeypatch.setattr(supervise, "_systemd_managed", lambda: False)
    monkeypatch.setattr(supervise, "_loaded_published_generation", lambda: None)
    monkeypatch.setattr(supervise, "_reap_children", lambda: 0)
    monkeypatch.setattr(supervise, "declared_shape", lambda *a, **k: (1, []))
    monkeypatch.setattr(supervise, "declared_roles",
                        lambda host: [("storage", ["--readers", "4"])])
    monkeypatch.setattr(
        supervise, "role_health",
        lambda roles, proc_root=None, census=None: [_stopped_entry()])
    monkeypatch.setattr(supervise, "ensure_roles", lambda *a, **k: [])
    monkeypatch.setattr(supervise, "_live_role_loops", lambda *a, **k: [])
    monkeypatch.setattr(supervise, "_live_loops", lambda: [])
    monkeypatch.setattr(supervise, "_claim_holders", lambda: frozenset())
    monkeypatch.setattr(supervise, "_ready_backlog", lambda: False)
    monkeypatch.setattr(supervise, "_spawn",
                        lambda args, index: 9000 + index)
    monkeypatch.setattr(sys, "argv", ["supervise", "--interval-s", "30"])

    class CycleComplete(Exception):
        pass

    def sleep(_seconds):
        raise CycleComplete

    monkeypatch.setattr(supervise.time, "sleep", sleep)

    with pytest.raises(CycleComplete):
        supervise.main()

    out = capsys.readouterr().out
    assert f"role storage pid {STOPPED_PID}" in out, out
    assert "stopped" in out, out


def test_a_shutdown_held_open_by_a_stopped_role_says_which_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A SIGTERM sent to a T process is queued, not delivered: the pending
    line must say so instead of looking like a slow drain."""

    monkeypatch.setattr(
        supervise, "_proc_state",
        lambda pid, proc_root=None: "T" if pid == STOPPED_PID else "S")

    note = supervise._stopped_pending_note(
        {7: STOPPED_PID, 8: 5151}, [(STOPPED_PID, "prewarm_loop.py"),
                                    (5151, "worker_loop.py")])

    assert f"storage pid {STOPPED_PID}" in note, note
    assert "state T" in note, note
    assert "5151" not in note, "a running target is not a stopped one"
