"""A tier loop that is making progress never reads as dead (#1072).

PQ's ``landing_verdict`` refuses a staged wait once the tier record is older
than ``tier_loop_liveness_s`` (``pool.OFFER_TIMEOUT_S``, 120 s), and PB's
``PoolQueue._tier_loop_alive`` applies the same bound.  Before #1072 the loop
announced each tier once per cycle, near its start, so a long cycle read as a
dead loop.  The live first cycle after the 09-24 publish of ``02b27a8804d3``
took 222.7 s: 137 dead-producer batch retirements in 129.7 s and a cold
stale-mention census of 50 owners in 87.9 s.  The tier record went 221 s
without an update, and any consumer blocked in a staged wait in that window
would have been refused.

The queue here is the dead R13 instance (`tests/r13_1053_replay.py`, ten
unretired batches) and two pairs of #1056's co-owned dead owners (four
census units), on `bench_tier_cycle`'s queue at a handful of rows each.  The
clock is a fake one, and it moves only inside the two long steps' units:
each retirement's egress and each owner's census is charged its share of the
live first cycle's phase time, so the ten retirements cost the live 129.7 s
and the four censuses the live 87.9 s.  Everything else takes no time.  The
tier record's age is read the way PQ reads it, from the file, after every
charge.

What must hold:

* the record never reads as dead while units complete, in the first cycle
  or any later one, and the backlog still drains;
* a record written again between steps carries the content the cycle minted,
  and only the announcement time moves;
* a loop that stops making progress reads as dead once the bound has passed,
  because nothing but progress writes the record.

Nothing here touches the live queue, a real stage root or a real device.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import socket
import sys
import time

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools" / "fleet"))
sys.path.insert(0, str(ROOT / "tests"))

from prismabuild import pool  # noqa: E402
import prismabuild.produced_output as po  # noqa: E402
import bench_tier_cycle as bench  # noqa: E402
import stage_release  # noqa: E402
import tier_loop  # noqa: E402

import r13_1053_replay as r13  # noqa: E402

#: The bound both readers apply to the tier record's age.
BOUND_S = pool.OFFER_TIMEOUT_S
#: The live first cycle's two long steps (``tier-cycle`` at unix 1790218904
#: in ``/home/rob/tmp/pb-role-tiers.log`` on dl380g10).
LIVE_RETIREMENT_S = 129.7
LIVE_CENSUS_S = 87.9
#: Pairs of co-owned dead owners; each owner is one census unit.
DEAD_OWNER_PAIRS = 2
UNRETIRED = 10
RETIREMENT_S = LIVE_RETIREMENT_S / UNRETIRED
CENSUS_S = LIVE_CENSUS_S / (2 * DEAD_OWNER_PAIRS)


class FakeClock:
    """``pool._now`` for the test: moves only when told to."""

    def __init__(self) -> None:
        self.now = time.time()

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def _shape() -> argparse.Namespace:
    return argparse.Namespace(
        done=5, failed=2, withdrawn=2, receipts=10, passes=3,
        empty_dirs=2, small_dirs=1, big_fragments="20",
        produced_fragment_dirs=2, live_consumers=2, phases=3,
        files_per_range=4, ready_noise=2, claimed_noise=2,
        dead_owner_pairs=DEAD_OWNER_PAIRS, dead_entries=8)


class _Loop:
    """The replayed backlog, the fake clock, and every age a reader saw."""

    def __init__(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        # The tier record names this box, as the live one names the tier
        # host, so the retirement takes the in-process egress.
        monkeypatch.setattr(bench, "HOST", socket.gethostname())
        self.clock = FakeClock()
        monkeypatch.setattr(pool, "_now", self.clock)
        self.queue = pool.PoolQueue(tmp_path / "pb-queue")
        self.queue.ensure_layout()
        self.stage = tmp_path / "stage"
        self.stage.mkdir()
        self.counts = bench.build_queue(self.queue, self.stage, _shape())
        assert self.counts.get("dead_owners") == 2 * DEAD_OWNER_PAIRS
        self.replay = r13.install(self.queue,
                                  prefix=tmp_path / "origin" / "adjoint",
                                  stage=self.stage, origin_files=False)
        assert len(self.replay.unretired) == UNRETIRED
        self.receipts = tier_loop.ReceiptCache()
        self.liveness = (tier_loop.Liveness()
                         if callable(getattr(tier_loop, "Liveness", None))
                         else None)
        #: ``(fake unix, age)`` read off the file after every charge.
        self.ages: list[tuple[float, float]] = []
        self.retirements = 0
        self.censuses = 0
        #: Every tier record written, as written.
        self.writes: list[dict[str, object]] = []
        self._charge(monkeypatch)
        real_announce = self.queue.announce_tier

        def announce(record, **kwargs):
            path = real_announce(record, **kwargs)
            if record.get("tier_id") == bench.TIER:
                self.writes.append(json.loads(path.read_text()))
            return path

        monkeypatch.setattr(self.queue, "announce_tier", announce)
        # The loop that was replaced announced the tier once.
        real_announce(self.record())

    def record(self) -> dict[str, object]:
        return bench._tier_record(self.stage)

    def _charge(self, monkeypatch: pytest.MonkeyPatch) -> None:
        output_root = po.OUTPUT_FRAGMENTS_SUBDIR
        real_evict = stage_release.evict
        real_prune = stage_release.prune_stale_mentions

        def evict(queue, mover, *args, **kwargs):
            retiring = Path(str(kwargs.get("residency_root") or "")).name == output_root
            if retiring:
                self.retirements += 1
                self.clock.advance(RETIREMENT_S)
            try:
                return real_evict(queue, mover, *args, **kwargs)
            finally:
                if retiring:
                    self.sample()

        def prune(*args, **kwargs):
            self.censuses += 1
            self.clock.advance(CENSUS_S)
            try:
                return real_prune(*args, **kwargs)
            finally:
                self.sample()

        monkeypatch.setattr(stage_release, "evict", evict)
        monkeypatch.setattr(stage_release, "prune_stale_mentions", prune)

    def age(self) -> float:
        record = json.loads(self.queue.tier_record_path(bench.TIER).read_text())
        return self.clock() - float(record["announced_unix"])

    def sample(self) -> None:
        self.ages.append((self.clock(), self.age()))

    def cycle(self) -> None:
        kwargs = {} if self.liveness is None else {"liveness": self.liveness}
        tier_loop.cycle(self.queue, host=bench.HOST, source_pool="storage_pool",
                        receipts=self.receipts,
                        discover=lambda **_kw: {bench.TIER: self.record()},
                        **kwargs)
        self.sample()

    def unretired(self) -> list[str]:
        return [batch_id for batch_id in self.replay.unretired
                if not po._batch_stage_retired(
                    self.replay.entry(self.queue.root, batch_id))]


@pytest.fixture(autouse=True)
def _isolated(monkeypatch):
    for name in ("PRISMABUILD_ACTION_NONCE", "PRISMABUILD_ACTION_SCOPE",
                 "PRISMABUILD_READER_HELPER_ROOT", "PRISMABUILD_ACTION_KEY"):
        monkeypatch.delenv(name, raising=False)
    stage_release.reset_holder_reports()
    po._UNFILED_REPORTS.clear()
    yield
    stage_release.reset_holder_reports()
    po._UNFILED_REPORTS.clear()


def _oldest(ages: list[tuple[float, float]]) -> float:
    return max(age for _unix, age in ages)


def test_a_first_cycle_with_a_backlog_never_reads_dead(tmp_path, monkeypatch):
    loop = _Loop(tmp_path, monkeypatch)
    loop.cycle()
    # The cycle did charge its units: at least one of each kind ran.
    assert loop.retirements >= 1 and loop.censuses >= 1, (
        loop.retirements, loop.censuses)
    oldest = _oldest(loop.ages)
    assert oldest < BOUND_S, (
        f"tier record read dead: oldest age {oldest:.1f} s against the "
        f"{BOUND_S:g} s bound, over {loop.retirements} retirements and "
        f"{loop.censuses} censuses in one cycle")
    assert loop.queue._tier_loop_alive(bench.TIER, now=loop.clock())[0]


def test_the_backlog_drains_and_no_cycle_reads_dead(tmp_path, monkeypatch):
    loop = _Loop(tmp_path, monkeypatch)
    units = UNRETIRED + 2 * DEAD_OWNER_PAIRS
    # Each cycle completes at least one unit, so the backlog is gone within
    # as many cycles as it has units; one more shows the steady state.
    for _ in range(units + 1):
        loop.cycle()
        if not loop.unretired() and loop.censuses >= 2 * DEAD_OWNER_PAIRS:
            break
    assert loop.unretired() == []
    assert loop.censuses >= 2 * DEAD_OWNER_PAIRS
    oldest = _oldest(loop.ages)
    assert oldest < BOUND_S, (
        f"tier record read dead: oldest age {oldest:.1f} s against the "
        f"{BOUND_S:g} s bound")


def test_a_record_written_between_steps_carries_the_minted_content(
        tmp_path, monkeypatch):
    loop = _Loop(tmp_path, monkeypatch)
    loop.writes.clear()
    loop.cycle()
    assert len(loop.writes) >= 2, (
        "the cycle wrote the tier record only at its mint; a long cycle "
        "must write it again between steps")
    moving = {"announced_unix", "liveness_refresh"}
    minted = {key: value for key, value in loop.writes[0].items()
              if key not in moving}
    for later in loop.writes[1:]:
        assert {key: value for key, value in later.items()
                if key not in moving} == minted
        assert later["announced_unix"] > loop.writes[0]["announced_unix"]
        refresh = later["liveness_refresh"]
        assert refresh["minted_unix"] == loop.writes[0]["announced_unix"]
        assert isinstance(refresh["after"], str) and refresh["after"]


def test_a_loop_that_stops_making_progress_reads_dead(tmp_path, monkeypatch):
    loop = _Loop(tmp_path, monkeypatch)
    seen: dict[str, object] = {}
    real_prune = stage_release.prune_stale_mentions

    def stalled(*args, **kwargs):
        # The first census unit hangs past the bound: no step completes, so
        # nothing may write the record, and a reader must see a dead loop.
        if "dead_at" not in seen:
            stamp = json.loads(loop.queue.tier_record_path(
                bench.TIER).read_text())["announced_unix"]
            while loop.clock() - stamp <= BOUND_S:
                assert loop.queue._tier_loop_alive(bench.TIER,
                                                   now=loop.clock())[0]
                loop.clock.advance(1.0)
            seen["dead_at"] = loop.clock() - stamp
            seen["alive"] = loop.queue._tier_loop_alive(bench.TIER,
                                                        now=loop.clock())[0]
            seen["rewritten"] = json.loads(loop.queue.tier_record_path(
                bench.TIER).read_text())["announced_unix"] != stamp
        return real_prune(*args, **kwargs)

    monkeypatch.setattr(stage_release, "prune_stale_mentions", stalled)
    loop.cycle()
    assert seen.get("alive") is False, seen
    assert seen["rewritten"] is False, seen
    assert BOUND_S < float(seen["dead_at"]) <= BOUND_S + 1.0, seen
