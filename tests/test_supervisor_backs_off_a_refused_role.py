"""A role that refused must not be respawned every supervisor tick (#1046).

After #1036 the ``metrics`` role exits ``ROLE_SINGLETON_HELD_EXIT`` (3) with
one refusal record when its port is already bound -- for instance by a
``prismabuild-metrics.service`` unit whose ``PrivateTmp`` hides its lock from
the role.  The lock probe in ``ensure_roles`` cannot see that holder, and the
supervisor discarded every child's exit status, so it spawned the role, the
role refused and exited, and the next 5 s tick spawned it again: one refusal
record per tick, forever.

The contract here: a role child reaped with the refusal status is backed off
-- not spawned again until its backoff elapses, the interval doubling with
each consecutive refusal up to ``ROLE_REFUSAL_BACKOFF_MAX_S`` -- and the
supervisor names the refusal and the next attempt once, in its own log.  A
role that is later seen live clears the backoff and says so.  Nothing here
touches a live process: ``waitpid``, ``pgrep``, ``/proc``, ``Popen`` and the
clock are all the test's own.
"""

from __future__ import annotations

import os
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
TESTS = ROOT / "tests"
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools" / "fleet"))
sys.path.insert(0, str(TESTS))

import supervise  # noqa: E402
import worker_loop  # noqa: E402
from test_a_second_role_refuses_the_host_role_lock import (  # noqa: E402
    HOST, STORAGE, _role_box, _role_process, _spawn_recorder)


def _exit_status(code: int) -> int:
    """The raw ``waitpid`` status of a process that called ``exit(code)``."""

    return (code & 0xFF) << 8


@pytest.fixture
def box(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    mirror, proc, offered = _role_box(tmp_path, monkeypatch)
    offered[STORAGE] = []
    monkeypatch.setattr(supervise, "declared_roles",
                        lambda host: [("storage", ["--readers", "4"])])
    monkeypatch.setattr(supervise, "_published_receipt", lambda: {})
    monkeypatch.setattr(worker_loop, "ROLE_LOCK_ROOT", tmp_path / "role-locks")
    monkeypatch.setattr(supervise, "_ROLE_CHILDREN", {})
    monkeypatch.setattr(supervise, "_ROLE_REFUSALS", {})
    clock = [1000.0]
    monkeypatch.setattr(supervise, "_monotonic", lambda: clock[0])
    exits: list[tuple[int, int]] = []

    def waitpid(pid, options):
        assert (pid, options) == (-1, os.WNOHANG)
        if exits:
            return exits.pop(0)
        return (0, 0)

    monkeypatch.setattr(supervise.os, "waitpid", waitpid)
    return mirror, proc, offered, clock, exits


def test_a_refused_role_is_not_respawned_every_tick(box, monkeypatch, capsys):
    _mirror, _proc, _offered, clock, exits = box
    spawned = _spawn_recorder(monkeypatch, pid=9001)

    # Tick 1: nothing runs, the lock is free, the role is spawned.
    assert supervise.ensure_roles(HOST) == [("storage", 9001)]
    # It refuses (exit 3) before the next tick, which reaps it.
    exits.append((9001, _exit_status(worker_loop.ROLE_SINGLETON_HELD_EXIT)))
    clock[0] += supervise.BUSY_INTERVAL_S
    assert supervise._reap_children() == 1
    capsys.readouterr()

    # Tick 2, one busy interval later: before #1046 this spawned it again.
    assert supervise.ensure_roles(HOST) == []
    assert len(spawned) == 1, "a refused role was respawned on the next tick"
    out = capsys.readouterr().out
    assert "role storage refused" in out, out
    assert f"exit {worker_loop.ROLE_SINGLETON_HELD_EXIT}" in out, out
    # The refusal is named once, not once per tick of the backoff.
    assert supervise.ensure_roles(HOST) == []
    assert "role storage refused" not in capsys.readouterr().out

    # The first backoff elapses: one retry.
    clock[0] += supervise.BUSY_INTERVAL_S
    assert supervise.ensure_roles(HOST) == [("storage", 9001)]
    assert len(spawned) == 2


def test_consecutive_refusals_double_the_backoff_to_a_ceiling(box, monkeypatch):
    _mirror, _proc, _offered, clock, exits = box
    _spawn_recorder(monkeypatch, pid=9001)
    delays = []
    for _ in range(12):
        assert supervise.ensure_roles(HOST) == [("storage", 9001)]
        exits.append((9001, _exit_status(worker_loop.ROLE_SINGLETON_HELD_EXIT)))
        assert supervise._reap_children() == 1
        refusal = supervise._ROLE_REFUSALS["storage"]
        delays.append(refusal["retry_at"] - clock[0])
        # Just before the retry is due nothing starts; at it, one does.
        clock[0] = refusal["retry_at"] - 0.001
        assert supervise.ensure_roles(HOST) == []
        clock[0] = refusal["retry_at"]
    base = supervise.BUSY_INTERVAL_S
    ceiling = supervise.ROLE_REFUSAL_BACKOFF_MAX_S
    assert delays == [min(base * 2 ** n, ceiling) for n in range(12)]
    assert delays[-1] == ceiling


def test_a_role_seen_live_clears_the_backoff(box, monkeypatch, capsys):
    _mirror, proc, offered, clock, exits = box
    _spawn_recorder(monkeypatch, pid=9001)
    assert supervise.ensure_roles(HOST) == [("storage", 9001)]
    exits.append((9001, _exit_status(worker_loop.ROLE_SINGLETON_HELD_EXIT)))
    supervise._reap_children()
    clock[0] = supervise._ROLE_REFUSALS["storage"]["retry_at"]
    assert supervise.ensure_roles(HOST) == [("storage", 9001)]
    capsys.readouterr()

    # The retry stays up: the census proves it, and the refusal clears.
    live = _role_process(proc, 9001, [
        "/usr/bin/python3",
        str(supervise.MIRROR / "runtime-generations" / "gen-live" / "tools"
            / STORAGE), "--readers", "4"])
    offered[STORAGE] = [live]
    assert supervise.ensure_roles(HOST) == []
    assert "storage" not in supervise._ROLE_REFUSALS
    assert "role storage refusal cleared" in capsys.readouterr().out


@pytest.mark.parametrize("status", [_exit_status(0), _exit_status(1),
                                    _exit_status(75), 9])  # 9: SIGKILL
def test_other_exits_keep_the_prompt_respawn(box, monkeypatch, status):
    """Only the refusal status backs off; a crash is still respawned at once."""

    _mirror, _proc, _offered, clock, exits = box
    spawned = _spawn_recorder(monkeypatch, pid=9001)
    assert supervise.ensure_roles(HOST) == [("storage", 9001)]
    exits.append((9001, status))
    assert supervise._reap_children() == 1
    assert supervise.ensure_roles(HOST) == [("storage", 9001)]
    assert len(spawned) == 2


def test_a_worker_exit_three_is_not_a_role_refusal(box, monkeypatch):
    """The status is only read for a pid this supervisor spawned as a role."""

    _mirror, _proc, _offered, _clock, exits = box
    _spawn_recorder(monkeypatch, pid=9001)
    exits.append((7777, _exit_status(worker_loop.ROLE_SINGLETON_HELD_EXIT)))
    assert supervise._reap_children() == 1
    assert supervise._ROLE_REFUSALS == {}
    assert supervise.ensure_roles(HOST) == [("storage", 9001)]
