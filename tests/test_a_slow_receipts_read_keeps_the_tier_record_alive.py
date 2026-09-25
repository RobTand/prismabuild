"""A slow receipts read re-announces the tier record from inside it (#1148).

Live shape (dl380g10, 2026-09-25 07:56:42Z): one tier cycle took 153.6 s,
113.7 s of it in the ``receipts`` step, while stage movers loaded the HDD
pool that ``pb-queue`` lives on.  ``tier_loop.Liveness`` (#1072)
re-announces a record only at a checkpoint, and the step had none inside
it: ``ReceiptCache.read`` lists the prewarm and movement receipt
directories and stats and parses every entry of a changed one as a single
stretch.  The stage record aged past the 120 s bound, and a Stage B row
died with ``StagedRangeNotLanded``.

The step runs before the cycle's ``mint_announce``, so the only record it
can re-announce is the one the previous cycle minted.

Here the first cycle is fast and mints the tier record.  Then 60 receipts
are filed, half prewarm and half movement, and the second cycle's read of
them costs ``RECEIPT_S`` each on a fake clock (``pool._now``, the clock the
records are stamped with): 150 s in all, past the bound.  The cost is
charged either while the directory is listed (``select``) or while each
entry is read (``parse``), and only inside ``ReceiptCache.read``.  The
record's age is read the way a reader reads it, from the file.

What must hold:

* the record never reads older than ``H`` during the slow read;
* what is re-announced during the read is the previous cycle's minted
  record, byte for byte but for ``announced_unix`` and ``liveness_refresh``;
* a read that hangs, a stretch longer than every one measured, gets no
  write, so a reader sees a dead loop within ``L + P``.

Every fixture is a temp queue and a temp stage root; nothing touches a real
``/stage``, ``/ram`` or the live queue.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys
import time

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools" / "fleet"))
sys.path.insert(0, str(ROOT / "tests"))

from prismabuild import pool  # noqa: E402
import stage_release  # noqa: E402
import tier_loop  # noqa: E402

import test_a_consumer_stages_only_to_its_refill_horizon as hz  # noqa: E402

TIER = hz.TIER
STAGE_GIB = 5
#: The live terms: ``L`` 120 s, ``P`` 30 s, so ``H`` = 90 s; ``I`` 5 s.
BOUND_S = float(pool.OFFER_TIMEOUT_S)
POLL_S = float(pool.HEARTBEAT_S)
HORIZON_S = BOUND_S - POLL_S
INTERVAL_S = 5.0
#: Receipts filed between the two cycles, and what reading each one costs.
#: Together they take 150 s, past ``L``, like the 153.6 s live cycle.
SLOW_RECEIPTS = 60
RECEIPT_S = 2.5
SLOW_PREFIX = "slow-receipt-"
#: The charge the hang test hangs in: 112.5 s into the read, after the
#: read has had to re-announce the record at least once.
HANG_AT = 45
#: Where the cost is charged: while the directory is listed, or while each
#: entry is stat-ed and parsed.
MODES = ("listing", "reading")


class FakeClock:
    """``pool._now`` for the test: moves only when a receipt is charged."""

    def __init__(self) -> None:
        self.now = time.time()

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class _Loop:
    """The queue, the fake clock, the loop's `Liveness`, and what a reader saw."""

    def __init__(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
                 mode: str) -> None:
        self.clock = FakeClock()
        monkeypatch.setattr(pool, "_now", self.clock)
        self.queue, self.stage = hz._fixture_queue(tmp_path, STAGE_GIB)
        self.receipts = tier_loop.ReceiptCache()
        self.liveness = tier_loop.Liveness(interval_s=INTERVAL_S)
        #: True while ``ReceiptCache.read`` runs: the ``receipts`` step.
        self.reading = False
        self.charges = 0
        #: ``age`` of the tier record, read off the file after every charge.
        self.ages: list[float] = []
        #: Every tier record written, as written, and whether the receipts
        #: read was running when it was.
        self.writes: list[tuple[dict[str, object], bool]] = []
        #: Called on the ``HANG_AT``-th charge, when set.
        self.hang = None

        real_read = self.receipts.read

        def read(*args, **kwargs):
            self.reading = True
            try:
                return real_read(*args, **kwargs)
            finally:
                self.reading = False

        self.receipts.read = read  # type: ignore[method-assign]

        real_announce = self.queue.announce_tier

        def announce(record, **kwargs):
            path = real_announce(record, **kwargs)
            if record.get("tier_id") == TIER:
                self.writes.append((json.loads(path.read_text()), self.reading))
            return path

        monkeypatch.setattr(self.queue, "announce_tier", announce)

        if mode == "listing":
            real_select = tier_loop._receipt_name

            def select(entry):
                if self.reading and entry.name.startswith(SLOW_PREFIX):
                    self.charge()
                return real_select(entry)

            monkeypatch.setattr(tier_loop, "_receipt_name", select)
        else:
            # The fake clock charges each read in full, one after another:
            # a model of a serial read.  Readers run concurrently (#1153),
            # and a clock charged from several reader threads at once sums
            # reads that overlapped in time, so here the read is serial.
            # The parallel read's checkpoints are tested on a real clock in
            # ``test_a_fresh_tier_loop_announces_before_its_cold_read``.
            monkeypatch.setattr(tier_loop, "RECEIPT_READERS", 1)
            real_parse = pool._read_json

            def parse(path, *args, **kwargs):
                if self.reading and Path(path).name.startswith(SLOW_PREFIX):
                    self.charge()
                return real_parse(path, *args, **kwargs)

            monkeypatch.setattr(pool, "_read_json", parse)

    def stamp(self) -> float:
        record = json.loads(self.queue.tier_record_path(TIER).read_text())
        return float(record["announced_unix"])

    def age(self) -> float:
        return self.clock() - self.stamp()

    def charge(self) -> None:
        self.charges += 1
        self.clock.advance(RECEIPT_S)
        self.ages.append(self.age())
        if self.hang is not None and self.charges == HANG_AT:
            self.hang()

    def file_slow_receipts(self) -> None:
        """Half prewarm receipts, half movement receipts, none of them folded."""

        for index in range(SLOW_RECEIPTS):
            directory = self.queue.root / (
                pool.PREWARM if index % 2 else tier_loop.MOVER_RECEIPTS)
            directory.mkdir(parents=True, exist_ok=True)
            (directory / f"{SLOW_PREFIX}{index:04d}.json").write_text(
                json.dumps({"schema": "pb.test-1148-noise.v1",
                            "unix": self.clock()}))

    def cycle(self) -> dict[str, object]:
        tier_loop.cycle(self.queue, host="dl380g10", source_pool="storage_pool",
                        receipts=self.receipts,
                        discover=lambda **_kw: {
                            TIER: hz._tier_record(self.stage, gib=STAGE_GIB)},
                        liveness=self.liveness)
        return dict(tier_loop.LAST_CYCLE.get("liveness") or {})


@pytest.fixture(autouse=True)
def _fresh_process_state():
    stage_release.reset_skip_checkpoints()
    stage_release.reset_holder_reports()
    yield
    stage_release.reset_skip_checkpoints()
    stage_release.reset_holder_reports()


def _minted(loop: _Loop) -> dict[str, object]:
    """The first cycle's mint: its first write that is not a refresh."""

    for record, _reading in loop.writes:
        if "liveness_refresh" not in record:
            return record
    raise AssertionError(f"the first cycle minted no tier record: {loop.writes}")


def _content(record: dict[str, object]) -> dict[str, object]:
    return {key: value for key, value in record.items()
            if key not in ("announced_unix", "liveness_refresh")}


@pytest.mark.parametrize("mode", MODES)
def test_a_slow_receipts_read_never_lets_the_record_age_past_h(
        tmp_path, monkeypatch, mode):
    loop = _Loop(tmp_path, monkeypatch, mode)
    loop.cycle()
    minted = _minted(loop)
    loop.file_slow_receipts()
    first = len(loop.writes)

    line = loop.cycle()

    print("liveness-evidence " + json.dumps({
        "test": f"slow-{mode}", "charges": loop.charges,
        "oldest_age_s": round(max(loop.ages, default=0.0), 3),
        "writes_during_read": sum(1 for _r, reading in loop.writes[first:]
                                  if reading),
        "liveness": line}, sort_keys=True))
    assert loop.charges == SLOW_RECEIPTS, loop.charges
    oldest = max(loop.ages)
    assert oldest < HORIZON_S, (
        f"a {SLOW_RECEIPTS * RECEIPT_S:g} s receipts read ({mode}) left the "
        f"tier record {oldest:.1f} s old, past the {HORIZON_S:g} s horizon "
        f"(dead at {BOUND_S:g} s): nothing re-announced it inside the read. "
        f"{line}")
    assert line["oldest_age_s"] < HORIZON_S, line
    assert line["overran"] is False, line
    refreshed = [record for record, reading in loop.writes[first:] if reading]
    assert refreshed, "no tier record was written during the receipts read"
    # The read runs before this cycle's mint: what it re-announces is the
    # previous cycle's minted record, byte for byte but the two moving fields.
    for record in refreshed:
        assert _content(record) == _content(minted), record
        refresh = record["liveness_refresh"]
        assert refresh["minted_unix"] == minted["announced_unix"], record
        assert str(refresh["after"]).startswith("receipts"), record
        assert record["announced_unix"] > minted["announced_unix"], record


@pytest.mark.parametrize("mode", MODES)
def test_a_receipts_read_that_hangs_reads_dead_within_the_bound(
        tmp_path, monkeypatch, mode):
    loop = _Loop(tmp_path, monkeypatch, mode)
    loop.cycle()
    loop.file_slow_receipts()
    first = len(loop.writes)
    seen: dict[str, object] = {}

    def hang() -> None:
        # One entry's read stalls, far longer than any stretch measured: no
        # step completes, so nothing may write the record, and a reader must
        # see a dead loop once the bound has passed.
        stamp = loop.stamp()
        writes = len(loop.writes)
        seen["refreshed_before"] = sum(
            1 for _r, reading in loop.writes[first:] if reading)
        while loop.clock() - stamp <= BOUND_S:
            assert loop.queue._tier_loop_alive(TIER, now=loop.clock())[0]
            loop.clock.advance(1.0)
        seen["dead_at"] = loop.clock() - stamp
        seen["alive"] = loop.queue._tier_loop_alive(TIER, now=loop.clock())[0]
        seen["rewritten"] = loop.stamp() != stamp
        seen["writes"] = len(loop.writes) - writes

    loop.hang = hang
    loop.cycle()

    print("liveness-evidence " + json.dumps({
        "test": f"hung-{mode}", "bound_s": BOUND_S, "poll_s": POLL_S,
        **seen}, sort_keys=True))
    # The hang came after the read had re-announced the record, so it is
    # the in-read checkpoints that must stay silent through it.
    assert int(seen.get("refreshed_before") or 0) >= 1, seen
    assert seen.get("alive") is False, seen
    assert seen["rewritten"] is False, seen
    assert seen["writes"] == 0, seen
    assert BOUND_S < float(seen["dead_at"]) <= BOUND_S + 1.0, seen
    # A reader polling every ``P`` sees the dead loop within ``L + P``.
    assert float(seen["dead_at"]) <= BOUND_S + POLL_S, seen
