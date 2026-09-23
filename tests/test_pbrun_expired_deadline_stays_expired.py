"""A wait whose deadline has passed starts no read (#938).

``await_outcome`` used to fall back to the full ``OUTCOME_READ_TIMEOUT_S``
when a positive ``wait_s`` had already run out before the first read.  A
caller past its deadline then got a longer read than any caller still inside
it, whose read gets ``min(OUTCOME_READ_TIMEOUT_S, remaining)``.  The time a
wait could take was not monotonic in the time it was given.

``POLL_S`` and ``OUTCOME_READ_TIMEOUT_S`` are both 5 s, so a test that only
checks "returns within one poll interval" cannot tell the old code from the
new: the old fallback took exactly one read budget.  These tests check the
budget each read is given, on a fake clock, and that an expired wait returns
before the clock moves at all.
"""
from __future__ import annotations

from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import pbrun  # noqa: E402


KEY = "c" * 64
PENDING = "d" * 64


class FakeClock:
    """``time.monotonic`` and ``time.sleep`` for pbrun, with scripted jumps.

    ``jumps`` maps the index of a ``monotonic()`` call to seconds added just
    before that call returns, which is how a test says "the deadline passed
    between computing it and the first read".
    """

    def __init__(self, jumps: dict[int, float] | None = None) -> None:
        self.now = 1000.0
        self.calls = 0
        self.jumps = dict(jumps or {})
        self.slept: list[float] = []

    def monotonic(self) -> float:
        self.now += self.jumps.pop(self.calls, 0.0)
        self.calls += 1
        return self.now

    def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
        self.now += seconds


def _observe(monkeypatch, clock: FakeClock) -> list[float]:
    """Record each read's budget; a read takes its whole budget, and finds nothing."""

    budgets: list[float] = []

    def observation(_q, _key, generation, *, budget_s, use_delivered_snapshot=False):
        budgets.append(budget_s)
        clock.now += budget_s
        return None, generation

    monkeypatch.setattr(pbrun, "time", clock)
    monkeypatch.setattr(pbrun, "bounded_outcome_observation", observation)
    return budgets


def test_a_wait_whose_deadline_passed_before_the_first_read_starts_none(
        monkeypatch, capsys) -> None:
    # Call 0 computes the deadline; call 1 is the first read's check, which
    # the fake clock places 2 s later -- past a 1 s wait.
    clock = FakeClock(jumps={1: 2.0})
    budgets = _observe(monkeypatch, clock)

    code = pbrun.await_outcome(object(), KEY, wait_s=1.0)
    expired_at = 1002.0

    assert code == pbrun.GAVE_UP_EXIT
    assert budgets == [], "an expired wait started a read"
    assert clock.now - expired_at < pbrun.POLL_S
    assert clock.now == expired_at, "an expired wait spent time"
    assert "gave up waiting" in capsys.readouterr().err


@pytest.mark.parametrize(("wait_s", "late_s", "expected"), [
    pytest.param(0.0, 0.0, [pbrun.OUTCOME_READ_TIMEOUT_S], id="wait-0-observes-once"),
    pytest.param(0.0, 9.0, [pbrun.OUTCOME_READ_TIMEOUT_S], id="wait-0-late-still-observes"),
    pytest.param(1.0, 0.0, [1.0], id="inside-deadline-reads-what-is-left"),
    pytest.param(1.0, 0.75, [0.25], id="late-inside-deadline-reads-less"),
    pytest.param(1.0, 1.0, [], id="deadline-exactly-passed-reads-nothing"),
    pytest.param(1.0, 2.0, [], id="deadline-passed-reads-nothing"),
])
def test_no_read_gets_more_than_the_caller_has_left(
        monkeypatch, wait_s, late_s, expected) -> None:
    """``wait_s=0`` keeps its one-observation contract; a positive wait never
    reads past its deadline, so a later start never means a longer read."""

    clock = FakeClock(jumps={1: late_s})
    budgets = _observe(monkeypatch, clock)

    assert pbrun.await_outcome(object(), KEY, wait_s=wait_s) == pbrun.GAVE_UP_EXIT
    assert budgets == pytest.approx(expected)


def _released(monkeypatch, clock: FakeClock) -> list[dict]:
    """``await_release`` finds its release at once; record what it asks next."""

    waits: list[dict] = []

    def await_outcome(_q, key, *, wait_s, generation=None):
        waits.append({"key": key, "wait_s": wait_s})
        return 0

    monkeypatch.setattr(pbrun, "time", clock)
    monkeypatch.setattr(pbrun.action_edges, "read_published", lambda _root, _pending: {
        "action_key": KEY, "published_unix": 1.0})
    monkeypatch.setattr(pbrun, "await_outcome", await_outcome)
    return waits


class _Queue:
    root = Path("/nonexistent-queue")


def test_a_release_read_after_the_deadline_is_not_turned_into_a_probe(
        monkeypatch, capsys) -> None:
    """``max(0.0, remaining)`` used to hand an expired wait the ``wait_s=0``
    contract: one more read with the full budget."""

    clock = FakeClock(jumps={1: 3.0})
    waits = _released(monkeypatch, clock)

    assert pbrun.await_release(_Queue(), PENDING, wait_s=2.0) == pbrun.GAVE_UP_EXIT
    assert waits == []
    err = capsys.readouterr().err
    assert f"was released as {KEY[:12]}" in err
    assert f"pbwait.py {KEY[:12]}" in err


@pytest.mark.parametrize(("wait_s", "late_s", "expected"), [
    pytest.param(0.0, 0.0, 0.0, id="wait-0-still-observes-once"),
    pytest.param(2.0, 0.5, 1.5, id="inside-deadline-passes-what-is-left"),
])
def test_a_release_inside_the_deadline_waits_for_what_is_left(
        monkeypatch, wait_s, late_s, expected) -> None:
    clock = FakeClock(jumps={1: late_s})
    waits = _released(monkeypatch, clock)

    assert pbrun.await_release(_Queue(), PENDING, wait_s=wait_s) == 0
    assert waits == [{"key": KEY, "wait_s": pytest.approx(expected)}]
