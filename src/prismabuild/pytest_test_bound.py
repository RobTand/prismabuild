"""Bound every test the suite runs, so a hang is a named failure.

Action ``766d7ae5e0382b755a1189d4c1c3a42407d90fdb3cea56898b02b26855908489``
held a 63-file shard on dl380g10 for the whole 3600 s execution ceiling.  One
test hung in a futex wait: pytest printed 185 dots and then nothing, the only
thing that could end the lease was the pool's deadline, and a deadline names
no test.  The record said "timeout" and the reader was left counting dots to
guess how far collection had got.  The suite needs a bound of its own, because
only the suite knows which test is running.

``pytest-timeout`` is not installed in the shard interpreter
(``/home/rob/venvs/pb-cpu``: pytest 9.1.1, pytest-xdist 3.8.0, no
pytest-timeout), and adding a dependency to a venv other projects share is not
this repository's call.  A ``SIGALRM`` alarm is the standard-library spelling
of the same mechanism -- it is what ``pytest-timeout``'s own ``signal`` method
does -- so the bound is written here rather than depended on.

Two properties are the point, and both come from where the alarm is armed:

*   **It names the test.**  The alarm is armed inside each of the three
    per-item phases, so the ``Failed`` it raises lands in the ``CallInfo`` that
    phase is already wrapped in and is reported against the item.  A fixture
    that hangs is named the same way a test body is; arming around the test
    call alone would have left a hung fixture anonymous.
*   **The name survives a session that never finishes.**  A bound is not a
    guarantee that the remaining tests fit in the lease, so the end-of-session
    summary may never print.  The handler writes the node id and the bound to
    stderr *before* raising -- with pytest's own capture suspended for the
    write, because a captured write lands in a buffer printed with the report
    and the report is the thing that may never come.  The pool's execution
    observation counts those bytes, so the record carries the name even when
    the shard is killed later for an unrelated reason.

The bound is off unless ``PRISMABUILD_TEST_TIMEOUT_S`` says otherwise: an
interactive ``pytest`` under a debugger must not be shot by an alarm, and a
value this repository cannot derive is not one it should invent.  ``pbtest.py``
derives it per shard from that shard's own execution ceiling.  A malformed
value is refused rather than ignored, because silently running unbounded is
the state this module exists to end.
"""

from __future__ import annotations

import os
import signal
import sys
import threading
import time

import pytest

from . import test_bound_contract

#: Held back from the budget so the handler's stderr, which names the test,
#: reaches the lease's execution observation while the action is still alive.
BUDGET_MARGIN_S = 30.0

#: The shortest alarm armed for a test that starts with no budget left.
MIN_ARMED_S = 1.0

#: Whether this platform can arm a wall-clock alarm at all.  Absent on
#: Windows; the bound is then reported as unavailable rather than pretended.
SUPPORTED = hasattr(signal, "SIGALRM") and hasattr(signal, "setitimer")


def configured_bound(environ: "os._Environ[str] | dict[str, str] | None" = None) -> float:
    """Read the per-test bound in seconds; ``0.0`` means no bound.

    An empty or absent value disarms.  A non-positive value disarms too and
    says so by returning zero, which is the spelling an interactive run uses.
    Anything that is not a number is a configuration error: a shard that meant
    to bound its tests and typo'd the number must not run unbounded and green.
    """

    raw = (os.environ if environ is None else environ).get(test_bound_contract.TIMEOUT_ENV)
    if raw is None or not raw.strip():
        return 0.0
    try:
        seconds = float(raw)
    except ValueError:
        raise pytest.UsageError(
            f"{test_bound_contract.TIMEOUT_ENV}={raw!r} is not a number of seconds; a per-test "
            "bound that cannot be read would leave this session unbounded"
        ) from None
    if seconds != seconds or seconds in (float("inf"), float("-inf")):
        raise pytest.UsageError(
            f"{test_bound_contract.TIMEOUT_ENV}={raw!r} is not a finite number of seconds")
    return seconds if seconds > 0 else 0.0


def configured_budget(
        environ: "os._Environ[str] | dict[str, str] | None" = None) -> float:
    """Read the shard's sealed budget in seconds; ``0.0`` means none."""

    raw = (os.environ if environ is None else environ).get(test_bound_contract.SHARD_BUDGET_ENV)
    if raw is None or not raw.strip():
        return 0.0
    try:
        seconds = float(raw)
    except ValueError:
        raise pytest.UsageError(
            f"{test_bound_contract.SHARD_BUDGET_ENV}={raw!r} is not a number of seconds") from None
    if seconds != seconds or seconds in (float("inf"), float("-inf")):
        raise pytest.UsageError(
            f"{test_bound_contract.SHARD_BUDGET_ENV}={raw!r} is not a finite number of seconds")
    return seconds if seconds > 0 else 0.0


def remaining_bound(*, bound_s: float, budget_s: float, elapsed_s: float) -> float:
    """The alarm for a test starting ``elapsed_s`` into a ``budget_s`` shard.

    The per-test bound, or the time left less ``BUDGET_MARGIN_S`` when that is
    shorter, and never below ``MIN_ARMED_S``.
    """

    if not bound_s or not budget_s:
        return bound_s
    remaining = budget_s - elapsed_s - BUDGET_MARGIN_S
    if remaining <= MIN_ARMED_S:
        return MIN_ARMED_S
    return min(bound_s, remaining)


class _Alarm:
    """One armed bound, disarmable without a late signal reaching the next.

    ``setitimer(0)`` cancels a timer that has not fired; it cannot recall one
    whose ``SIGALRM`` is already queued.  Without the ``armed`` flag that late
    signal would raise ``Failed`` inside whatever ran next -- the report
    writer, the following test's setup -- and blame the wrong test.  The flag
    is what makes disarming total.
    """

    def __init__(self, nodeid: str, phase: str, seconds: float,
                 capman=None, budgeted: bool = False) -> None:
        self.nodeid = nodeid
        self.phase = phase
        self.seconds = seconds
        self.capman = capman
        self.budgeted = budgeted
        self.armed = False

    def message(self) -> str:
        text = (
            f"{self.nodeid} exceeded the per-test bound of {self.seconds:g}s "
            f"during {self.phase} ({test_bound_contract.TIMEOUT_ENV}). A test that runs longer "
            "than its shard can afford is a hang until measured otherwise: "
            "this fails the test, not the shard's slot (#600)."
        )
        if self.budgeted:
            text += (
                " The bound was tightened to the time this test had left in "
                f"its shard ({test_bound_contract.SHARD_BUDGET_ENV}): it started too late to run "
                "its full bound before the shard's end.")
        return text

    def __call__(self, signum: int, frame: object) -> None:
        if not self.armed:
            return
        self.armed = False
        # Before raising, not after: an exception ends this frame, and the
        # summary that would otherwise carry the name is printed at the end of
        # a session that may be killed first.
        self._announce()
        pytest.fail(self.message(), pytrace=False)

    def _announce(self) -> None:
        """Put the node id on the session's real stderr, now.

        Not simply ``sys.stderr.write``: pytest captures file descriptors 1
        and 2 for the whole session, so a plain write lands in a buffer that
        is printed with the report -- at the end of a session that a shard
        deadline may never let finish.  Suspending capture for the write is
        what puts the bytes where the pool's execution observation counts
        them while the action is still alive.  If capture cannot be
        suspended, the buffered write is still better than nothing.
        """

        line = f"PrismaBuild per-test bound: {self.message()}\n"
        try:
            if self.capman is not None:
                with self.capman.global_and_fixture_disabled():
                    sys.stderr.write(line)
                    sys.stderr.flush()
                return
        except Exception:
            pass
        try:
            sys.stderr.write(line)
            sys.stderr.flush()
        except Exception:  # pragma: no cover - a broken stderr is not the story
            pass


def _bounded(item: pytest.Item, phase: str):
    """Run one phase of ``item`` under the alarm, restoring what was there."""

    seconds = getattr(item.config, "_prismabuild_test_bound", 0.0)
    if (not seconds or not SUPPORTED
            or threading.current_thread() is not threading.main_thread()):
        # xdist workers run their items in the worker process's main thread,
        # so this is not the xdist case; it is the "someone called us from a
        # thread" case, where ``signal.signal`` raises. Declining to arm is
        # the honest answer -- and it is the answer the record can read,
        # because an unbounded phase simply has no bound failure in it.
        return None
    budget = getattr(item.config, "_prismabuild_shard_budget", 0.0)
    started = getattr(item.config, "_prismabuild_session_start", None)
    budgeted = False
    if budget and started is not None:
        tightened = remaining_bound(
            bound_s=seconds, budget_s=budget,
            elapsed_s=time.monotonic() - started)
        budgeted = tightened != seconds
        seconds = tightened
    alarm = _Alarm(item.nodeid, phase, seconds,
                   item.config.pluginmanager.get_plugin("capturemanager"),
                   budgeted=budgeted)
    previous = signal.signal(signal.SIGALRM, alarm)
    alarm.armed = True
    signal.setitimer(signal.ITIMER_REAL, seconds)
    return alarm, previous


def _disarm(state) -> None:
    if state is None:
        return
    alarm, previous = state
    alarm.armed = False
    signal.setitimer(signal.ITIMER_REAL, 0.0)
    signal.signal(signal.SIGALRM, previous)


def pytest_configure(config: pytest.Config) -> None:
    # Read once per process -- including once per xdist worker, which inherits
    # the environment -- so a malformed value is refused at startup rather
    # than at the first test, and every item in the session shares one bound.
    config._prismabuild_test_bound = configured_bound()
    config._prismabuild_shard_budget = configured_budget()
    config._prismabuild_session_start = time.monotonic()


def pytest_report_header(config: pytest.Config) -> str | None:
    seconds = getattr(config, "_prismabuild_test_bound", 0.0)
    if not seconds:
        return None
    if not SUPPORTED:
        return (f"per-test bound {seconds:g}s requested but this platform has "
                "no SIGALRM: tests run unbounded")
    return f"per-test bound {seconds:g}s per phase ({test_bound_contract.TIMEOUT_ENV})"


@pytest.hookimpl(wrapper=True)
def pytest_runtest_setup(item: pytest.Item):
    state = _bounded(item, "setup")
    try:
        return (yield)
    finally:
        _disarm(state)


@pytest.hookimpl(wrapper=True)
def pytest_runtest_call(item: pytest.Item):
    state = _bounded(item, "call")
    try:
        return (yield)
    finally:
        _disarm(state)


@pytest.hookimpl(wrapper=True)
def pytest_runtest_teardown(item: pytest.Item, nextitem: pytest.Item | None):
    state = _bounded(item, "teardown")
    try:
        return (yield)
    finally:
        _disarm(state)
