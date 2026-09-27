"""#1214: a discovery fork inherits argv/env, but not session leadership.

Only private proc fixtures and intercepted signals: never inspect or signal a
live fleet process. The production supervisor sent SIGTERM to these children
as excess idle workers, causing SystemExit(143) inside bounded discovery.
"""
from pathlib import Path

import pytest

from test_supervisor_owns_the_loops_it_signals import (
    LIVE, _process, candidates, fleet, signals, supervise,
)

CHILD = 1000000020
ROLES = ("worker_loop.py", "tier_loop.py", "prewarm_loop.py", "metrics_loop.py")


def _stat(proc: Path, pid: int, *, parent: int = 1,
          group: int | None = None, session: int | None = None) -> None:
    # comm may contain spaces and closing parentheses. Field 3 is the state,
    # 4 PPid, 5 process group, 6 session; field 22 is starttime.
    group = pid if group is None else group
    session = pid if session is None else session
    (proc / str(pid) / "stat").write_text(
        f"{pid} (python reader) name) S {parent} {group} {session} "
        + "0 " * 15 + "12345\n"
    )


@pytest.mark.parametrize("role", ROLES)
def test_forked_reader_is_not_owned_or_signalled(fleet, signals, role):
    mirror, store, proc = fleet
    script = store / "gen-live" / "tools" / role
    script.write_text("# private role fixture\n")
    argv = ["/usr/bin/python3", str(script)]
    _process(proc, LIVE, argv)
    _stat(proc, LIVE)
    _process(proc, CHILD, argv)  # inherited ownership env and identical argv
    _stat(proc, CHILD, parent=LIVE, group=LIVE, session=LIVE)
    roots = supervise._proven_roots()

    assert supervise._is_fleet_loop(LIVE, roots, proc, role)
    assert not supervise._is_fleet_loop(CHILD, roots, proc, role), (
        "a forked discovery reader was mistaken for a supervisor-owned role"
    )
    assert supervise._stop_idle_loops(
        [CHILD], proc_root=proc, script_name=role, holders=set()
    ) == []
    assert signals == [], "the supervisor must never SIGTERM the reader"


def test_fork_does_not_inflate_worker_census(fleet, candidates):
    mirror, _store, proc = fleet
    argv = ["/usr/bin/python3", str(mirror / "repo" / "tools" / "worker_loop.py")]
    _process(proc, LIVE, argv)
    _stat(proc, LIVE)
    _process(proc, CHILD, argv)
    _stat(proc, CHILD, parent=LIVE, group=LIVE, session=LIVE)
    candidates.extend([LIVE, CHILD])
    assert supervise._live_loops() == [LIVE]


@pytest.mark.parametrize("bad_stat", [None, "", "garbage", "1 (x) S", "1 (x) S 0 x y", "1 (x) S 0 1 1"])
def test_unproven_kernel_identity_is_not_signal_authority(fleet, signals, bad_stat):
    mirror, _store, proc = fleet
    _process(proc, LIVE, ["/usr/bin/python3", str(mirror / "repo" / "tools" / "worker_loop.py")])
    if bad_stat is None:
        (proc / str(LIVE) / "stat").unlink()
    else:
        (proc / str(LIVE) / "stat").write_text(bad_stat)
    assert not supervise._is_fleet_loop(LIVE, supervise._proven_roots(), proc)
    assert supervise._stop_idle_loops([LIVE], proc_root=proc, holders=set()) == []
    assert signals == []


@pytest.mark.parametrize("group,session", [(LIVE, CHILD), (CHILD, LIVE)])
def test_partial_leadership_does_not_prove_launch(fleet, group, session):
    mirror, _store, proc = fleet
    _process(proc, CHILD, ["/usr/bin/python3", str(mirror / "repo" / "tools" / "worker_loop.py")])
    _stat(proc, CHILD, group=group, session=session)
    assert not supervise._is_fleet_loop(CHILD, supervise._proven_roots(), proc)


@pytest.mark.parametrize("parent", [1, 777])
def test_real_session_leader_survives_supervisor_reexec_or_adoption(fleet, parent):
    mirror, _store, proc = fleet
    _process(proc, LIVE, ["/usr/bin/python3", str(mirror / "repo" / "tools" / "worker_loop.py")])
    _stat(proc, LIVE, parent=parent)
    assert supervise._is_fleet_loop(LIVE, supervise._proven_roots(), proc)
