"""The wrapper bounds the execution; against a real kernel, it must not move it.

``capped_launch_argv``'s docstring is the contract -- "the wrapper must not
change *what* is executed, only what bounds it" -- and ``worker_argv``'s says
what is at stake: "an action executed here and the same action executed under
SLURM must be the same execution, or the CAS receipt is comparing two different
things."  Members of that context were dropped silently on the way in, and argv
assertions could not have caught it, because argv was not where they went
missing: a transient unit is forked by the **user manager**, so nothing of the
launcher's execution reaches the work except by being named.  Measured before
the fix, on two boxes: affinity 5-9 became 0-19 on a GB10 and 0-1 became 0-79
on the Xeon; soft ``RLIMIT_NOFILE`` 500000 became systemd's
``DefaultLimitNOFILE`` soft of 1024.

So these tests compare the *child's own* view against the launcher's, wrapped
and unwrapped, rather than the argv.  That is what catches the two cases argv
cannot: a property systemd silently ignores, and ``setrlimit_closest`` clamping
a limit the user manager will not grant.

**The launcher perturbs every dimension it is about to compare**, not just the
two the objection named, and that is the whole method rather than a detail.  A
dimension left at the box default matches on both sides whether the wrapper
carries it or not, so a comparison that leaves it alone is green against a
wrapper that carries nothing.  The first version of this file restricted
affinity and ``RLIMIT_NOFILE`` and read "the other 14 rlimits: identical" --
which was true and proved nothing.  Perturbed, sparky 2026-09-04 answers
differently: ``CORE``, ``MSGQUEUE``, ``NPROC``, ``SIGPENDING`` and ``STACK``
move too, and so do the umask (``0o077`` to the manager's ``0o002``) and the
nice level (5 to 0).
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import resource
import subprocess
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from prismabuild import pool  # noqa: E402

_SUPPORTED, _WHY = pool.memory_capping_supported()
pytestmark = pytest.mark.skipif(
    not _SUPPORTED, reason=f"this box cannot start a capped user unit: {_WHY}"
)

#: Neither systemd's soft default (1024) nor any box's hard limit here, so
#: "kept the launcher's" and "got the default" cannot be mistaken for each
#: other.
PROBE_SOFT = 314159

#: Soft limits the launcher moves to a value that is neither the box's default
#: nor systemd's, so "kept the launcher's" and "got the default" cannot be
#: mistaken for each other.  Every one of these is restorable in-process, which
#: ``nice`` is not -- see ``test_..._keeps_the_launchers_nice_level``.
PROBE_LIMITS = {
    "RLIMIT_NOFILE": PROBE_SOFT,
    "RLIMIT_CORE": 1,
    "RLIMIT_STACK": 9 * 1024 * 1024,
    "RLIMIT_MSGQUEUE": 819100,
    "RLIMIT_NPROC": 100000,
    "RLIMIT_SIGPENDING": 100000,
}

#: Nothing here is the box's default either.
PROBE_UMASK = 0o077

CHILD = (
    "import json, os, resource\n"
    "N = sorted(n for n in dir(resource) if n.startswith('RLIMIT_')"
    " and n != 'RLIMIT_OFILE')\n"
    "m = os.umask(0o022); os.umask(m)\n"
    "print(json.dumps({'affinity': sorted(os.sched_getaffinity(0)),\n"
    "                  'umask': m, 'nice': os.nice(0),\n"
    "                  'nofile': list(resource.getrlimit(resource.RLIMIT_NOFILE)),\n"
    "                  'rlimits': {n: list(resource.getrlimit(getattr(resource, n)))\n"
    "                              for n in N}}))\n"
)

#: Runs one arm from a launcher that has raised its own nice level.  ``os.nice``
#: only goes up without privilege, so perturbing it in the suite's own process
#: would slow every test after this one and could not be undone.
NICE_LAUNCHER = (
    "import json, os, subprocess, sys\n"
    "sys.path.insert(0, sys.argv[1])\n"
    "from prismabuild import pool\n"
    "os.nice(int(sys.argv[3]))\n"
    "argv = pool.capped_launch_argv(\n"
    "    [sys.executable, sys.argv[2]], cap_gb=4,\n"
    "    unit='pbtest-nice-' + os.urandom(4).hex(),\n"
    "    cwd=os.getcwd(), env=os.environ, **pool.launcher_exec_context())\n"
    "done = subprocess.run(argv, capture_output=True, text=True,\n"
    "                      stdin=subprocess.DEVNULL, env=pool._bus_ready_env())\n"
    "print(json.dumps({'rc': done.returncode, 'out': done.stdout,\n"
    "                  'err': done.stderr[-400:], 'launcher_nice': os.nice(0)}))\n"
)


@pytest.fixture()
def restricted_launcher(tmp_path):
    """Perturb *this* process on every axis it can restore, then put it back.

    Returns the child script plus what the launcher now is, which is what both
    arms are measured against.  Anything not perturbed here is a dimension this
    file cannot speak about, because a match on it is not evidence.
    """

    was_affinity = os.sched_getaffinity(0)
    was_umask = os.umask(PROBE_UMASK)
    was_limits = {}
    perturbed = {}
    hard = resource.getrlimit(resource.RLIMIT_NOFILE)[1]
    if hard != resource.RLIM_INFINITY and hard <= 1024:
        os.umask(was_umask)
        pytest.skip(f"hard RLIMIT_NOFILE is {hard}; no soft value above the "
                    f"systemd default is reachable to tell the two apart")
    if len(was_affinity) < 2:
        os.umask(was_umask)
        pytest.skip("launcher is already pinned to one CPU; a narrower mask "
                    "would not distinguish carried from inherited")
    child = tmp_path / "exec_context_child.py"
    child.write_text(CHILD)
    want = set(sorted(was_affinity)[:2])
    try:
        os.sched_setaffinity(0, want)
        for name, target in PROBE_LIMITS.items():
            number = getattr(resource, name, None)
            if number is None:
                continue
            soft, limit = resource.getrlimit(number)
            was_limits[number] = (soft, limit)
            # A soft above the hard raises rather than skipping, and an ERROR
            # on a box whose hard limit merely sits between the two is a broken
            # test, not a finding.  Any value the launcher can actually hold
            # distinguishes carried from defaulted.
            if limit != resource.RLIM_INFINITY and target > limit:
                target = limit
            if target == soft:
                # Equal to what the unit would get anyway proves nothing, so
                # this axis is simply not claimed rather than claimed falsely.
                continue
            resource.setrlimit(number, (target, limit))
            perturbed[name] = [target, limit]
        yield {"script": child, "affinity": sorted(want),
               "umask": PROBE_UMASK, "perturbed": perturbed,
               "nofile": list(resource.getrlimit(resource.RLIMIT_NOFILE))}
    finally:
        for number, pair in was_limits.items():
            resource.setrlimit(number, pair)
        os.sched_setaffinity(0, was_affinity)
        os.umask(was_umask)


def _observed(argv) -> dict:
    done = subprocess.run(argv, capture_output=True, text=True,
                          stdin=subprocess.DEVNULL,
                          env=pool._bus_ready_env())
    assert done.returncode == 0, (done.returncode, done.stderr[-400:])
    return json.loads(done.stdout)


def _wrapped(script: Path, unit: str) -> list[str]:
    return pool.capped_launch_argv(
        [sys.executable, str(script)], cap_gb=4, unit=unit,
        cwd=os.getcwd(), env=os.environ, **pool.launcher_exec_context())


def test_the_wrapped_child_sees_the_same_context_as_the_unwrapped_one(
    restricted_launcher,
) -> None:
    """The whole objection in one assertion: same execution, different bound."""

    script = restricted_launcher["script"]
    unwrapped = _observed([sys.executable, str(script)])
    wrapped = _observed(_wrapped(script, "pbtest-ctx-" + os.urandom(4).hex()))
    assert wrapped == unwrapped, {"unwrapped": unwrapped, "wrapped": wrapped}


def test_the_child_of_a_capped_launch_keeps_the_launchers_pin(
    restricted_launcher,
) -> None:
    """``cpu_topology.pin_to_preferred`` is inheritance-by-fork; a unit is not
    forked by this process, so the mask is carried or it is lost."""

    observed = _observed(
        _wrapped(restricted_launcher["script"], "pbtest-cpu-" + os.urandom(4).hex()))
    assert observed["affinity"] == restricted_launcher["affinity"]


def test_the_child_of_a_capped_launch_keeps_the_launchers_fd_ceiling(
    restricted_launcher,
) -> None:
    """Compared against the launcher, not against the argv: systemd clamps a
    limit above what the user manager will grant, and does it silently."""

    observed = _observed(
        _wrapped(restricted_launcher["script"], "pbtest-fd-" + os.urandom(4).hex()))
    assert observed["nofile"] == restricted_launcher["nofile"]
    assert observed["nofile"][0] != 1024, (
        "1024 is systemd's DefaultLimitNOFILE soft: the launcher's value did "
        "not reach the unit")


def test_the_child_of_a_capped_launch_keeps_the_launchers_umask(
    restricted_launcher,
) -> None:
    """The mask decides the mode of every byte an action writes to the CAS.

    Left uncarried the child gets the user manager's own mask, which on sparky
    is ``0o002`` -- group-writable where the launcher asked for owner-only.
    """

    observed = _observed(
        _wrapped(restricted_launcher["script"],
                 "pbtest-umask-" + os.urandom(4).hex()))
    assert observed["umask"] == restricted_launcher["umask"], (
        f"launcher umask {restricted_launcher['umask']:04o}, child "
        f"{observed['umask']:04o}")


def test_the_child_of_a_capped_launch_keeps_every_limit_systemd_can_carry(
    restricted_launcher,
) -> None:
    """The rule, not the roster.

    The objection named ``RLIMIT_NOFILE`` because that is the one that moves on
    today's fleet.  Against a launcher that is perturbed on every axis, five
    more move -- and a wrapper that carried only the named one would be green
    on the fleet and wrong on the mechanism.
    """

    observed = _observed(
        _wrapped(restricted_launcher["script"],
                 "pbtest-limits-" + os.urandom(4).hex()))
    assert restricted_launcher["perturbed"], (
        "no limit was perturbable on this box, so this test proves nothing")
    for name, expected in restricted_launcher["perturbed"].items():
        assert observed["rlimits"][name] == expected, (
            f"{name}: launcher {expected}, child {observed['rlimits'][name]}")


def test_the_child_of_a_capped_launch_keeps_the_launchers_nice_level(
    tmp_path,
) -> None:
    """The CPU half of the same escape ``CPUAffinity`` closes.

    Run from a launcher of its own because ``os.nice`` only goes up without
    privilege: perturbing the suite's process would slow every test after this
    one and could not be undone.  Compared against what that launcher actually
    reached rather than against 5, because a suite that is itself niced -- a
    capped pbrun action, a ``nice pytest`` -- starts higher and would fail this
    for a reason that is not the wrapper's.  Both scripts live under
    ``tmp_path``: this checkout is shared, and two suite runs writing one
    fixed name into ``tests/`` would clobber each other.
    """

    root = Path(__file__).resolve().parents[1]
    launcher = tmp_path / "nice_launcher.py"
    child = tmp_path / "nice_child.py"
    launcher.write_text(NICE_LAUNCHER)
    child.write_text(CHILD)
    done = subprocess.run(
        [sys.executable, str(launcher), str(root / "src"), str(child), "5"],
        capture_output=True, text=True, stdin=subprocess.DEVNULL, cwd=root)
    assert done.returncode == 0, done.stderr[-400:]
    arm = json.loads(done.stdout)
    assert arm["rc"] == 0, arm
    # The manager's own level is 0, so anything from 5 up still tells the two
    # apart; what it must not be is "whatever the manager was".
    assert arm["launcher_nice"] >= 5, arm
    assert json.loads(arm["out"])["nice"] == arm["launcher_nice"], (
        "the unit ran at the user manager's nice level, not the "
        f"launcher's: {arm}")
