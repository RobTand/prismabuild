"""A test that hangs is a recorded failure naming the test, not a lost slot.

Action ``766d7ae5...`` held a 63-file shard for the full 3600 s ceiling
because one test hung in a futex wait: pytest printed 185 dots and then
nothing, the shard's whole-action deadline was the only thing that ended it,
and the record could not say which test was running when the output stopped.
The suite needs its own per-test bound so the hang is attributed *inside*
pytest -- a failed test with a message -- instead of being inferred from
where the dots stopped.

These cases run a real pytest in a child process, because the thing under
test is what a shard's failure record looks like, and a shard is a pytest
process. The child writes one test that sleeps past a small timeout; the
assertions read the child's own report. An unbounded child would hang this
suite the same way the shard hung, so every child here runs under a
subprocess timeout that is deliberately larger than the timeout it is
exercising.

``PRISMABUILD_TEST_TIMEOUT_S`` is the knob the bound reads, spelled the same
way here, in the plugin and in the shard ``pbtest.py`` builds, so the contract
under test is one name.
"""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

#: The plugin under test, by the name both the shard's own session and a
#: child suite load it under. One name, so a wiring that drifts is visible.
PLUGIN = "prismabuild.pytest_test_bound"

#: This checkout's importable root, absolute because the child runs from its
#: own directory.
REPO_SRC = Path(__file__).resolve().parents[1] / "src"

#: The timeout the child suite enforces, small enough that the case is fast
#: and large enough that a slow box starting pytest is not mistaken for a
#: hang in the test.
CHILD_TIMEOUT_S = "2"

#: The hang the child's test performs, longer than the timeout but shorter
#: than anything a busy box would take to run two passing tests.
CHILD_SLEEP_S = "30"

#: The sleeping test, as source. The name is what the failure record must
#: carry; without it the shard knows only that *something* hung.
_HANGING_TEST = textwrap.dedent(f"""
    def test_the_hanging_test():
        import time
        time.sleep({CHILD_SLEEP_S})
""")


def _run_child(child_root: Path, files: dict[str, str], *,
               timeout_env: str | None,
               subprocess_timeout_s: float,
               workers: int = 0) -> subprocess.CompletedProcess:
    """Run a private pytest child and return its completed process.

    The child runs the real bound -- the same plugin module the rootdir
    ``conftest.py`` loads for a fleet shard -- over a suite of its own, so
    nothing of this repository's tests is collected or run.
    """

    for rel, source in files.items():
        path = child_root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(source)
    env = dict(os.environ)
    # A shard may export its own value; these cases must be exact about the
    # bound the child runs under, inherited or set or cleared.
    env.pop("PRISMABUILD_TEST_TIMEOUT_S", None)
    if timeout_env is not None:
        env["PRISMABUILD_TEST_TIMEOUT_S"] = timeout_env
    # The child is rooted at its own directory, so it loads none of this
    # repository's conftests -- deliberately: ``tests/conftest.py`` censuses
    # the fleet's live store at session start, and a child suite must not.
    # It therefore needs the plugin named on its argv and an absolute
    # ``PYTHONPATH``: the relative ``src`` a shard exports does not resolve
    # from the child's own working directory.
    env["PYTHONPATH"] = os.pathsep.join(
        part for part in (str(REPO_SRC), env.get("PYTHONPATH", "")) if part)
    # A fleet shard may be fanned out with xdist, and xdist runs each item in
    # its worker process's main thread -- which is where a signal alarm can be
    # armed at all. Exercised rather than assumed.
    distribute = ["-n", str(workers)] if workers else []
    return subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "--no-header",
         "-p", "no:cacheprovider", "-p", PLUGIN, *distribute, str(child_root)],
        capture_output=True, text=True, timeout=subprocess_timeout_s,
        env=env, cwd=child_root,
    )


@pytest.mark.parametrize("workers", [0, 2], ids=["single", "xdist"])
def test_a_test_sleeping_past_the_bound_fails_naming_the_test(
        tmp_path: Path, workers: int):
    """The red case: without a per-test bound this child passes the hanging
    test's sleep, so the session is green and the record says nothing. With
    the bound, the same child must fail, and must say which test.

    Run both ways, because a fleet shard may be fanned out: ``-n 2`` puts the
    item in an xdist worker process, and an alarm armed anywhere but a main
    thread does not arm at all.
    """
    proc = _run_child(
        tmp_path / f"named-{workers}",
        {"test_hang.py": _HANGING_TEST},
        timeout_env=CHILD_TIMEOUT_S,
        # The child must finish well inside the outer bound: startup plus
        # the per-test timeout, with room for a loaded box.
        subprocess_timeout_s=120,
        workers=workers,
    )
    assert proc.returncode == 1, (
        "the hanging test must fail the child session, not pass it; "
        f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
    )
    assert "test_the_hanging_test" in proc.stdout, (
        "the failure record must name the test that hung; "
        f"stdout:\n{proc.stdout}"
    )


def test_the_name_reaches_stderr_before_the_session_can_be_killed(
        tmp_path: Path):
    """The summary is printed at the end of a session that may never reach it.

    A bound does not promise the remaining tests fit in the lease, so the
    shard can still be killed at its deadline with no summary written. The
    handler writes the node id and the limit to stderr and flushes *before*
    raising, and the pool counts those bytes into the lease's execution
    observation while the action is alive -- which is how the name survives a
    session that ends the way ``766d7ae5...`` ended.
    """
    proc = _run_child(
        tmp_path / "streamed",
        {"test_hang.py": _HANGING_TEST},
        timeout_env=CHILD_TIMEOUT_S,
        subprocess_timeout_s=120,
    )
    assert "test_the_hanging_test" in proc.stderr, (
        "the node id must reach stderr as the alarm fires, not only the "
        f"end-of-session summary; stderr:\n{proc.stderr}"
    )
    assert CHILD_TIMEOUT_S in proc.stderr, (
        f"and with the limit that fired; stderr:\n{proc.stderr}")


def test_the_bound_names_the_limit_in_the_failure(tmp_path: Path):
    """A timeout failure that does not say it was a timeout reads as a test
    bug. The record must distinguish "this test hung" from "this test broke"
    -- the timeout limit and the test share the failure message."""
    proc = _run_child(
        tmp_path / "reason",
        {"test_hang.py": _HANGING_TEST},
        timeout_env=CHILD_TIMEOUT_S,
        subprocess_timeout_s=120,
    )
    assert proc.returncode == 1, (
        f"the hanging test must fail; stdout:\n{proc.stdout}"
    )
    assert CHILD_TIMEOUT_S in proc.stdout, (
        "the failure must say the per-test limit that fired, so the reader "
        f"can tell a hang from a break; stdout:\n{proc.stdout}"
    )


def test_a_passing_test_is_unaffected_by_the_bound(tmp_path: Path):
    """The bound is an alarm, not a test-changer: a test that finishes
    inside it passes and the session is green."""
    proc = _run_child(
        tmp_path / "passes",
        {"test_ok.py": "def test_ok():\n    assert True\n"},
        timeout_env=CHILD_TIMEOUT_S,
        subprocess_timeout_s=120,
    )
    assert proc.returncode == 0, (
        "a passing test under the bound must still pass; "
        f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
    )


def test_this_suites_own_session_runs_under_the_bound_plugin(
        pytestconfig: pytest.Config):
    """A bound only a child suite loads would leave the shard unbounded.

    The cases above name the plugin on the child's argv, which proves the
    mechanism and nothing about the wiring. This one reads the session that is
    running *these* tests -- the same session shape a fleet shard runs -- and
    asks whether the plugin is registered in it. It is the assertion that
    fails if the rootdir conftest stops loading the plugin.
    """

    assert pytestconfig.pluginmanager.hasplugin(PLUGIN), (
        "the suite's own session must load the per-test bound; a shard whose "
        "session lacks it holds its slot to the execution ceiling with "
        "nothing naming the test that hung (#600)"
    )
    assert hasattr(pytestconfig, "_prismabuild_test_bound"), (
        "the loaded plugin must have configured this session's bound"
    )
