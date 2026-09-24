"""The terms of the tier loop's liveness rule and cycle budget (#1072).

`tier_loop.Liveness` re-announces the minted tier records at a checkpoint
when ``age + U + W >= H`` (``H = L - P``), and lets a unit of a long step
start while ``spent + est(kind) <= b`` (``b = H - W - I - T``).  Each term is
driven here on a fake clock and a fake queue that records every write, so
each rule is checked at its boundary.
"""
from __future__ import annotations

from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools" / "fleet"))

from prismabuild import pool  # noqa: E402
import tier_loop  # noqa: E402

L = pool.OFFER_TIMEOUT_S
P = pool.HEARTBEAT_S
H = L - P


class _Clock:
    def __init__(self) -> None:
        self.now = 1_000_000.0

    def __call__(self) -> float:
        return self.now


class _Queue:
    """Records every announcement; the clock moves ``write_s`` per write."""

    def __init__(self, clock: _Clock, write_s: float = 0.0) -> None:
        self.clock = clock
        self.write_s = write_s
        self.writes: list[dict] = []
        self.fail = False

    def announce_tier(self, record, *, now=None):
        if self.fail:
            raise OSError("the queue is unreachable")
        self.clock.now += self.write_s
        self.writes.append(dict(record, announced_unix=now))


@pytest.fixture
def clock(monkeypatch) -> _Clock:
    clock = _Clock()
    monkeypatch.setattr(pool, "_now", clock)
    return clock


def _record(tier: str = "stage:box") -> dict:
    return {"tier_id": tier, "mountpoint": "/stage", "epoch": 7}


def test_the_horizon_is_the_bound_less_the_slowest_poll(clock):
    liveness = tier_loop.Liveness(interval_s=5.0)
    assert (liveness.bound_s, liveness.poll_s, liveness.horizon_s) == (L, P, H)


def test_a_checkpoint_refreshes_only_once_the_next_stretch_could_pass_h(clock):
    queue = _Queue(clock)
    liveness = tier_loop.Liveness(interval_s=5.0)
    liveness.begin_cycle()
    liveness.announce(queue, _record())
    # Stretches of 20 s: U = 20.
    for after in ("a", "b", "c"):
        clock.now += 20.0
        liveness.checkpoint(after)
    # age 69 + U 20 = 89 < H: no write.
    clock.now += 9.0
    liveness.checkpoint("d")
    assert len(queue.writes) == 1
    # age 70 + U 20 = 90 >= H: written.
    clock.now += 1.0
    liveness.checkpoint("e")
    assert len(queue.writes) == 2
    refresh = queue.writes[-1]
    assert refresh["announced_unix"] == clock.now
    assert refresh["liveness_refresh"] == {
        "after": "e", "minted_unix": queue.writes[0]["announced_unix"],
        "refreshes": 1}
    assert {k: v for k, v in refresh.items()
            if k not in ("announced_unix", "liveness_refresh")} == _record()


def test_the_write_latency_counts_against_the_horizon(clock):
    queue = _Queue(clock, write_s=4.0)
    liveness = tier_loop.Liveness(interval_s=5.0)
    liveness.begin_cycle()
    liveness.announce(queue, _record())      # W = 4
    clock.now += 10.0
    liveness.checkpoint("a")                 # U = 14, the write's 4 s included
    for after in ("b", "c", "d", "e"):
        clock.now += 14.0
        liveness.checkpoint(after)
    # age 70 + U 14 + W 4 = 88 < H.
    assert len(queue.writes) == 1
    clock.now += 3.0
    liveness.checkpoint("f")
    # age 73 + U 14 = 87 would not write; with W 4 it is 91 >= H.
    assert len(queue.writes) == 2


def test_the_budget_is_the_horizon_less_write_interval_and_the_rest(clock):
    queue = _Queue(clock, write_s=2.0)
    liveness = tier_loop.Liveness(interval_s=5.0)
    liveness.begin_cycle()
    liveness.announce(queue, _record())
    # 3 s outside any unit so far (2 s of it the write): T = 3, W = 2.
    clock.now += 1.0
    assert liveness.budget_s() == pytest.approx(H - 2.0 - 5.0 - 3.0)


def test_each_kind_gets_one_unit_and_then_what_the_budget_allows(clock):
    liveness = tier_loop.Liveness(interval_s=5.0, bound_s=60.0, poll_s=30.0)
    liveness.begin_cycle()
    # H = 30, b = 30 - 0 - 5 - 0 = 25.
    assert liveness.start("census", "a")
    clock.now += 40.0                 # one unit alone is over the budget
    liveness.done("census")
    assert not liveness.start("census", "b")
    assert liveness.start("retirement", "x"), "every kind makes progress"
    clock.now += 1.0
    liveness.done("retirement")
    assert not liveness.start("retirement", "y")
    stats = liveness.end_cycle(completed=True)
    assert stats["units"] == {"census": 1, "retirement": 1}
    assert stats["deferred"] == {"census": 1, "retirement": 1}


def test_units_run_while_the_estimate_fits(clock):
    liveness = tier_loop.Liveness(interval_s=5.0)
    liveness.begin_cycle()
    budget = liveness.budget_s()
    assert budget == pytest.approx(H - 5.0)
    started = 0
    while liveness.start("census", f"c{started}"):
        started += 1
        clock.now += 10.0
        liveness.done("census")
    # spent + est <= b admits floor(b / 10) units of 10 s.
    assert started == int(budget // 10.0)


def test_the_next_cycle_starts_from_the_first_unit_refused(clock):
    liveness = tier_loop.Liveness(interval_s=5.0, bound_s=60.0, poll_s=30.0)
    liveness.begin_cycle()
    keys = ["a", "b", "c", "d"]
    assert liveness.order("census", keys) == keys
    for key in keys:
        if liveness.start("census", key):
            clock.now += 20.0
            liveness.done("census")
    liveness.end_cycle(completed=True)
    liveness.begin_cycle()
    assert liveness.order("census", keys) == ["b", "c", "d", "a"]
    assert liveness.order(("other", "census"), keys) == ["b", "c", "d", "a"]
    assert liveness.order("other", keys) == keys


def test_the_cycle_end_counts_the_sleep_ahead(clock):
    queue = _Queue(clock)
    liveness = tier_loop.Liveness(interval_s=30.0)
    liveness.begin_cycle()
    liveness.announce(queue, _record())
    clock.now += 50.0
    liveness.checkpoint("a")          # age 50 + U 50 >= H: written here
    assert len(queue.writes) == 2
    clock.now += 1.0
    liveness.end_cycle(completed=True)
    # age 1 + I 30 + U 50 < H: no write at the end.
    assert len(queue.writes) == 2
    liveness.begin_cycle()
    liveness.announce(queue, _record())
    clock.now += 45.0
    liveness.end_cycle(completed=True)
    # age 45 + I 30 + U 50 >= H: the sleep would take it past H.
    assert len(queue.writes) == 4
    assert queue.writes[-1]["liveness_refresh"]["after"] == "cycle-end"


def test_a_tier_the_cycle_did_not_announce_is_never_written_again(clock):
    queue = _Queue(clock)
    liveness = tier_loop.Liveness(interval_s=5.0)
    liveness.begin_cycle()
    liveness.announce(queue, _record("stage:box"))
    liveness.announce(queue, _record("ram:box"))
    liveness.end_cycle(completed=True)
    liveness.begin_cycle()
    liveness.announce(queue, _record("stage:box"))
    liveness.end_cycle(completed=True)
    clock.now += H
    liveness.begin_cycle()
    liveness.checkpoint("a")
    written = [write["tier_id"] for write in queue.writes
               if "liveness_refresh" in write]
    assert written == ["stage:box"]


def test_a_failed_refresh_is_a_refresh_that_did_not_happen(clock):
    queue = _Queue(clock)
    liveness = tier_loop.Liveness(interval_s=5.0)
    liveness.begin_cycle()
    liveness.announce(queue, _record())
    queue.fail = True
    clock.now += H
    liveness.checkpoint("a")
    stats = liveness.end_cycle(completed=True)
    assert stats["refreshes"] == 0
    assert stats["refresh_errors"]
    assert set(stats["refresh_errors"]) == {"stage:box: the queue is unreachable"}
