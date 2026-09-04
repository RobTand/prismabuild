"""The wrapper bounds the execution; against a real kernel, it must not move it.

``capped_launch_argv``'s docstring is the contract -- "the wrapper must not
change *what* is executed, only what bounds it" -- and ``worker_argv``'s says
what is at stake: "an action executed here and the same action executed under
SLURM must be the same execution, or the CAS receipt is comparing two different
things."  Two members of that context were dropped silently on the way in, and
argv assertions could not have caught it, because argv was not where they went
missing: a transient unit is forked by the **user manager**, so the launcher's
CPU affinity and its soft ``RLIMIT_NOFILE`` reached the work only by being
named, and neither was.  Measured before the fix, on two boxes: affinity 5-9
became 0-19 on a GB10 and 0-1 became 0-79 on the Xeon; soft ``RLIMIT_NOFILE``
500000 became systemd's ``DefaultLimitNOFILE`` soft of 1024.

So these tests compare the *child's own* view against the launcher's, wrapped
and unwrapped, rather than the argv.  That is what catches the two cases argv
cannot: a property systemd silently ignores, and ``setrlimit_closest`` clamping
a limit the user manager will not grant.

The launcher restricts itself first, to values that are neither the box's
defaults nor systemd's.  Without that the test would be vacuous exactly where
it matters -- a suite that happens to be running *inside* a capped unit already
has soft 1024 and the full affinity mask, so "child matches launcher" would
pass with the wrapper carrying nothing at all.
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

CHILD = (
    "import json, os, resource\n"
    "print(json.dumps({'affinity': sorted(os.sched_getaffinity(0)),\n"
    "                  'nofile': list(resource.getrlimit(resource.RLIMIT_NOFILE))}))\n"
)


@pytest.fixture()
def restricted_launcher(tmp_path):
    """Pin and throttle *this* process, and put it back afterwards.

    Returns the child script plus what the launcher now is, which is what both
    arms are measured against.
    """

    was_affinity = os.sched_getaffinity(0)
    was_nofile = resource.getrlimit(resource.RLIMIT_NOFILE)
    hard = was_nofile[1]
    if hard != resource.RLIM_INFINITY and hard <= 1024:
        pytest.skip(f"hard RLIMIT_NOFILE is {hard}; no soft value above the "
                    f"systemd default is reachable to tell the two apart")
    # A soft above the hard raises rather than skipping, and an ERROR on a box
    # whose hard limit merely sits between the two is a broken test, not a
    # finding.  Any value above 1024 distinguishes carried from defaulted.
    soft = (PROBE_SOFT if hard == resource.RLIM_INFINITY or PROBE_SOFT <= hard
            else hard)
    if len(was_affinity) < 2:
        pytest.skip("launcher is already pinned to one CPU; a narrower mask "
                    "would not distinguish carried from inherited")
    child = tmp_path / "exec_context_child.py"
    child.write_text(CHILD)
    want = set(sorted(was_affinity)[:2])
    try:
        os.sched_setaffinity(0, want)
        resource.setrlimit(resource.RLIMIT_NOFILE, (soft, hard))
        yield {"script": child, "affinity": sorted(want),
               "nofile": [soft, hard]}
    finally:
        resource.setrlimit(resource.RLIMIT_NOFILE, was_nofile)
        os.sched_setaffinity(0, was_affinity)


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
