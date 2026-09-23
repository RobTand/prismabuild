"""Claimed consumers drain an over-committed stage tier in admission order (#1011).

The #907 joint commitment gates newcomers only.  A consumer that is already
claimed is never re-checked, yet its commitment can still outgrow the tier:
its refill horizon lengthens when the landing rate falls, and the tier's room
shrinks under adoption conflicts or foreign files.  Once no claimed
consumer's next range fits and nothing past any horizon is left to evict,
each waits on the others' reading and none can read.  Before #1009 the
reader's constant wait or the ``no_progress`` rung killed one of them, and a
kill throws away a GPU consumer's work.

The fixture is R12's plan and receipts (``tests/fixtures/r12_stage_20260922.json``),
once per consumer, each under its own manifest.  Every consumer:

* was claimed ``CLAIM_GAP_S`` (5 s) after the one before it, and reports
  ``chain-043`` the same 3919 s after its claim as R12 did, so every one
  reads at R12's 20.7 MB/s and has R12's horizon;
* has read ``head`` to ``chain-044``, whose egress rows are in ``done/``;
* has no copy of ``chain-043``, the range it is reading: none landed and
  none published;
* holds its next ranges, landed at the fixture's own copy rates:
  ``chain-042`` to ``chain-034`` (198 GiB) with two consumers, ``chain-042``
  to ``chain-040`` (66 GiB) with eight.

By ``test_r12_and_the_capture_replay_under_the_refill_horizon``'s arithmetic
each consumer can be reading anything that starts before 274.5 GB, which is
``chain-042`` to ``chain-034``; ``chain-033`` completes the horizon and
``chain-032`` is the advance.  So every range a consumer holds is inside its
horizon, and nothing on the tier is past one.

The tier then shrinks to what the consumers hold plus 21 GiB.  Every
``chain-043`` is a 22 GiB range, so none fits, and the joint commitment --
each consumer's 242 GiB horizon -- is far past the tier.

The bound.  A range the window publishes lands within one heartbeat, one
cycle and the slowest copy of the consumer's plan: ``HEARTBEAT_S +
CYCLE_INTERVAL_S + landing_s``, the latency ``residency_plan.refill_horizon``
prices a horizon with.  Here that is 30 + 60 + 169.6 s with two consumers
(the slowest receipt they hold is ``chain-037``'s 138.1 MB/s, and the largest
range still to read is 23.42 GB), and 30 + 60 + 150.2 s with eight
(``chain-040``, 155.9 MB/s).  The oldest consumer's ``chain-043`` must be
published within that many seconds of cycles: ``ceil(259.6 / 60)`` and
``ceil(240.2 / 60)``, 5 cycles either way.  On main the state is a fixpoint
of ``cycle()``: nothing publishes, nothing is evicted, and no term depends
on the time.  So the bound is a ceiling, not a race.

After the fix the tier ranks its claimed consumers (``window_credit.
claim_order``): blocked on the range it is reading first, then by when each
became blocked (#1022 review), the claim time breaking ties.  Here every
consumer blocked at its last report, which the fixture staggers in claim
order.  The longest-blocked consumer is the head; the tier
evicts what is ranked after it, farthest first, to make its room, and every
consumer behind it is held back with a record naming the consumer ahead of
it, the GiB it waits for and the priced landing.  One head is served a
cycle.

Everything runs on ``tmp_path`` queues and stage roots (#628).
"""
from __future__ import annotations

import json
import math
from pathlib import Path
import sys
import time

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from prismabuild import (  # noqa: E402
    pool, progress as pb_progress, residency_map, residency_plan,
    storage_tiers, window_credit)
import stage_release  # noqa: E402
import tier_loop  # noqa: E402
from test_a_consumer_stages_only_to_its_refill_horizon import (  # noqa: E402
    _cycle, _fixture_queue, _land)
from test_a_consumer_stages_only_to_its_refill_horizon import (  # noqa: E402
    _plan as _small_plan, _publish_consumer)
from test_a_resident_range_is_adopted_rather_than_recopied import (  # noqa: E402
    PHASE_GIB, TIER, _hexkey, _tier_record)
from test_r12_and_the_capture_replay_under_the_refill_horizon import (  # noqa: E402
    DATA, FILL, SAMPLE_UNIX, _claim, _consumer, _plan)
from test_a_resident_range_is_adopted_rather_than_recopied import _row  # noqa: E402

CLAIM_GAP_S = 5.0
READING = "chain-043"
PASSED = ("head", "forward-044", "chain-044")
PHASES = {str(phase["name"]): phase for phase in DATA["r12"]["phases"]}
RATES = {entry["phase"]: entry["bytes_staged"] / entry["seconds"]
         for entry in DATA["r12"]["landed"]}
TWO_HOLD = tuple(f"chain-{n:03d}" for n in range(42, 33, -1))
EIGHT_HOLD = tuple(f"chain-{n:03d}" for n in range(42, 39, -1))


def _key(n: int) -> str:
    return _hexkey(f"order{n}")


def _manifest(n: int) -> str:
    # Distinct in its first eight characters: ``_land`` files a range under
    # ``manifest[:8]``, and two consumers must not share a staged path here.
    return _hexkey(f"{n}m-order")


def _mover(n: int, phase: str) -> str:
    return _hexkey(f"order{n}mover{phase}")


def _blocked(queue: pool.PoolQueue, stage: Path, n: int, *, shift: float,
             holding: tuple[str, ...]) -> dict[str, object]:
    """Consumer ``n``: claimed, reading ``chain-043``, with ``holding`` landed."""

    r12 = DATA["r12"]
    key, manifest = _key(n), _manifest(n)
    plan = _plan(queue, key, label=f"order{n}", manifest=manifest,
                 phases=r12["phases"], fill=r12["sealed_fill_mb_s"],
                 stage_root=str(stage))
    _consumer(queue, key, plan, manifest=manifest,
              mem_gb=r12["resources"]["mem_gb"])
    for name in holding:
        start, end = int(PHASES[name]["start_bytes"]), int(PHASES[name]["end_bytes"])
        _land(queue, stage, consumer=key, manifest=manifest,
              mover=_mover(n, name), name=name, start=start, end=end,
              seconds=(end - start) / RATES[name])
    # The ranges it has read are given back: their egress rows ran.
    by_name = {str(phase["name"]): phase for phase in plan["phases"]}  # type: ignore[union-attr]
    for name in PASSED:
        egress = dict(by_name[name]["egress_row"])
        path = queue.item_path(pool.DONE, str(egress["action_key"]))
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({**egress, "status": "done"}))
    offset = n * CLAIM_GAP_S
    _claim(queue, key, phase=READING,
           claimed_unix=r12["claimed_unix"] + shift + offset,
           reported_unix=r12["accepted"]["reported_unix"] + shift + offset,
           gpu_budget=r12["gpu_memory_budget_bytes"])
    return plan


def _fixture(tmp_path: Path, count: int, holding: tuple[str, ...]):
    shift = time.time() - SAMPLE_UNIX
    queue, stage = _fixture_queue(tmp_path, DATA["tier"]["capacity_gib"])
    plans = [_blocked(queue, stage, n, shift=shift, holding=holding)
             for n in range(count)]
    held = count * sum(int(PHASES[name]["stage_gib"]) for name in holding)
    shrunk = held + int(PHASES[READING]["stage_gib"]) - 1
    return queue, stage, plans, shrunk


def _bound(queue: pool.PoolQueue, stage: Path, plan, *, gib: int
           ) -> tuple[int, dict[str, object]]:
    """The module docstring's bound, in cycles, and the horizon it came from."""

    key = str(plan["consumer_action_key"])
    consumer = next(entry for entry in tier_loop.live_consumers(queue)
                    if entry["action_key"] == key)
    horizon = tier_loop._stage_horizon(queue, consumer, plan,
                                       _tier_record(stage, gib=gib))
    assert horizon is not None
    latency = (pool.HEARTBEAT_S + tier_loop.CYCLE_INTERVAL_S
               + float(horizon["landing_s"]))              # type: ignore[arg-type]
    return math.ceil(latency / tier_loop.CYCLE_INTERVAL_S), horizon


def _published(queue: pool.PoolQueue, mover: str) -> bool:
    return (queue.item_path(pool.READY, mover).exists()
            or queue.item_path(pool.CLAIMED, mover).exists()
            or bool(queue.tier_ledger(TIER).holder_tokens(mover)))


def _events(capsys: pytest.CaptureFixture[str]) -> list[dict[str, object]]:
    return [json.loads(line) for line in capsys.readouterr().out.splitlines()
            if line.startswith("{")]


_TOLD = ("window-gated", "window-stalled", "window-unknown", "window-unfunded",
         "tier-over-committed", "beyond-horizon-evicted",
         "beyond-horizon-eviction-futile", "beyond-horizon-eviction-declined",
         "claim-order-evicted", "claim-order-eviction-declined",
         "mover-published", "advance-deferred-unknown-evidence")


def _diagnosis(queue: pool.PoolQueue, events, count: int) -> str:
    """What the cycles said, trimmed to the events that decide publication."""

    told = []
    for event in events:
        if event.get("event") not in _TOLD:
            continue
        told.append({field: value for field, value in event.items()
                     if field not in ("terms", "commitment", "unix", "host")})
    record = queue.tier_commitment(TIER) or {}
    ledger = queue.tier_ledger(TIER)
    return json.dumps({
        "events": told[-40:],
        "commitment": {field: record.get(field) for field in (
            "capacity_gib", "committed_gib", "over_committed_gib",
            "held_gib", "evictable_gib", "claim_order")},
        "free_gib": ledger.available().get(storage_tiers.STAGE_CAPACITY_KIND),
        "reading_published": {n: _published(queue, _mover(n, READING))
                              for n in range(count)},
    }, indent=1, default=str)


@pytest.mark.parametrize("count,holding", [(2, TWO_HOLD), (8, EIGHT_HOLD)],
                         ids=["two", "eight"])
def test_the_oldest_claimed_consumers_range_publishes_within_the_landing_bound(
        tmp_path: Path, capsys: pytest.CaptureFixture[str], count: int,
        holding: tuple[str, ...]) -> None:
    """The acceptance fixture: ``count`` claimed R12s on a shrunk stage.

    On main no consumer's ``chain-043`` is published within the bound, or at
    all: every consumer waits on the others' reading, and only a kill would
    end the wait.  After the fix the oldest consumer's range is published
    within the bound, by evicting what the youngest holds farthest ahead.
    """

    queue, stage, plans, shrunk = _fixture(tmp_path, count, holding)
    bound, horizon = _bound(queue, stage, plans[0], gib=shrunk)
    # Every held range is inside the horizon, so nothing is past one.
    by_start = {int(phase["start_bytes"]): str(phase["name"])
                for phase in plans[0]["phases"]}          # type: ignore[union-attr]
    assert by_start[int(horizon["horizon_end_bytes"])] == "chain-032"  # type: ignore[arg-type]
    assert not {leg["phase"] for leg in horizon["beyond"]} & set(holding)  # type: ignore[union-attr]
    assert bound == 5

    published_at = None
    for cycle in range(1, bound + 1):
        _cycle(queue, stage, gib=shrunk)
        if _published(queue, _mover(0, READING)):
            published_at = cycle
            break
    events = _events(capsys)

    assert published_at is not None, _diagnosis(queue, events, count)
    # The first cycle serves the oldest consumer: one head a cycle.
    assert published_at == 1, _diagnosis(queue, events, count)
    _assert_held_back_records(queue, count, head=0, served=())


def _landing(queue: pool.PoolQueue, key: str) -> dict[str, object]:
    return residency_map.read_landing(residency_map.landing_path(
        queue.residency_fragment_root(), key))


def _assert_held_back_records(queue: pool.PoolQueue, count: int, *, head: int,
                              served: tuple[int, ...]) -> None:
    """Every consumer the order holds back says what holds it, three ways.

    The tier's commitment record carries the rank; each held-back consumer's
    event file carries this cycle's ``window-gated`` verdict naming the one
    ahead and the head; its landing record lists its ``chain-043`` with the
    one ahead, the head, the GiB and a priced landing (in a state the
    reader knows: :func:`test_every_landing_state_the_order_writes_is_one_the_reader_reads`).
    """

    record = queue.tier_commitment(TIER)
    assert record is not None
    order = record["claim_order"]
    assert isinstance(order, dict), record
    entries = {entry["consumer"]: entry for entry in order["entries"]}
    assert order["head"] == _key(head), order
    for n in range(count):
        if n == head or n in served:
            continue
        key = _key(n)
        entry = entries[key]
        assert entry["standing"] == window_credit.CLAIM_HELD_BACK, entry
        # Blocked consumers rank by when they became blocked.  Here each
        # blocked at its last report, which the fixture staggers in claim
        # order, so the one ahead of a held-back consumer is the next older
        # blocked one.
        older = [m for m in range(n) if m not in served]
        assert entry["ahead"] == _key(older[-1]), entry
        assert entry["waiting_on"] == _key(head), entry
        assert entry["need_gib"] == int(PHASES[READING]["stage_gib"])
        assert isinstance(entry["expected_landing_unix"], float), entry
        gated = [event for event in queue.consumer_events(key)
                 if event.get("event") == "window-gated"]
        assert gated, key
        last = gated[-1]
        assert last["reason"] == window_credit.REASON_CLAIM_ORDER, last
        assert last["ahead"] == _key(older[-1]), last
        assert last["waiting_on"]["consumer"] == _key(head), last
        assert last["expected_landing_unix"] == entry["expected_landing_unix"]
        rows = {row["phase"]: row for row in _landing(queue, key)["ranges"]}
        row = rows[READING]
        assert row["held_back_by"] == _key(older[-1]), row
        assert row["waiting_on"] == _key(head), row
        assert row["waiting_gib"] == int(PHASES[READING]["stage_gib"]), row
        assert row["expected_landing_unix"] == entry["expected_landing_unix"]
        assert "claim order" in str(row["waiting_for"]), row


# ------------------------------------------------ the drain, and its cost


def _run_movers(queue: pool.PoolQueue, stage: Path, count: int, *,
                unfit: list[str] | None = None) -> list[str]:
    """Every queued copy of the fixture's consumers that can claim lands.

    One cycle's copies.  The mover's row leaves ``ready/`` for ``done/`` and
    its range is filed as ``stage_move`` files one (``_land``), at the
    slowest fixture rate for a range with no receipt of its own.  A claim
    takes its tokens from free, counting a fence the window put on this very
    leg (#907: held under the consumer's grant for the leg, or already moved
    onto the mover by ``_settle_protected``); that fence is handed back and
    ``_land`` takes the same tokens again.  A mover whose tokens are not
    there stays in ``ready/``, as the claim pass leaves it
    (``tier_reservation_unavailable``), and is named in ``unfit``.
    """

    landed: list[str] = []
    slowest = min(RATES.values())
    ledger = queue.tier_ledger(TIER)
    for n in range(count):
        key, manifest = _key(n), _manifest(n)
        plan = residency_plan.read(queue, key)
        for phase in plan["phases"]:                        # type: ignore[union-attr]
            name = str(phase["name"])
            # A chunked phase's legs are its chunks (#675), one mover each.
            legs = ([(phase, None)] if isinstance(phase.get("mover_row"), dict)
                    else [(chunk, int(chunk["chunk_index"]))
                          for chunk in phase.get("stage_chunks") or ()
                          if isinstance(chunk.get("mover_row"), dict)])
            for leg, chunk in legs:
                label = name if chunk is None else f"{name}c{chunk}"
                mover = str(leg["mover_row"]["action_key"])
                ready = queue.item_path(pool.READY, mover)
                if not ready.exists():
                    continue
                start, end = int(leg["start_bytes"]), int(leg["end_bytes"])
                need = storage_tiers.stage_tokens_for_bytes(end - start)
                own = (mover, window_credit.grant_key(
                    key, TIER, "mover_row", name, chunk))
                funded = sum(int(ledger.holder_tokens(holder).get("stage_gib", 0))
                             for holder in own)
                if int(ledger.available().get("stage_gib", 0)) + funded < need:
                    if unfit is not None:
                        unfit.append(f"{n}:{label}")
                    continue
                item = json.loads(ready.read_text())
                ready.unlink()
                for holder in own:
                    if ledger.holder_tokens(holder):
                        ledger.release(holder)
                done = queue.item_path(pool.DONE, mover)
                done.parent.mkdir(parents=True, exist_ok=True)
                done.write_text(json.dumps({**item, "status": "done"}))
                _land(queue, stage, consumer=key, manifest=manifest, mover=mover,
                      name=label, start=start, end=end,
                      seconds=(end - start) / RATES.get(name, slowest))
                landed.append(f"{n}:{label}")
    return landed


class _Timer:
    """Wall seconds and calls per wrapped tier-loop step, per cycle."""

    STEPS = ("window_pressure", "evict_beyond_horizon", "residency_window",
             "publish_landing_expectations", "_commitment_census",
             "_rank_claims", "_claim_order_candidates", "_report_commitments")

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.cycle: dict[str, list[float]] = {}
        for name in self.STEPS:
            original = getattr(tier_loop, name)
            monkeypatch.setattr(tier_loop, name, self._wrap(name, original))

    def _wrap(self, name, original):
        def timed(*args, **kwargs):
            began = time.perf_counter()
            try:
                return original(*args, **kwargs)
            finally:
                self.cycle.setdefault(name, []).append(time.perf_counter() - began)
        return timed

    def take(self) -> dict[str, dict[str, float]]:
        out = {name: {"calls": len(spent), "s": round(sum(spent), 4)}
               for name, spent in sorted(self.cycle.items())}
        self.cycle = {}
        return out


def test_eight_claimed_consumers_drain_the_tier_in_claim_order(
        tmp_path: Path, capsys: pytest.CaptureFixture[str],
        monkeypatch: pytest.MonkeyPatch) -> None:
    """Eight blocked R12s on the shrunk tier are served one a cycle, oldest first.

    Each cycle's copies land before the next (``_run_movers``).  Consumer
    ``n``'s ``chain-043`` is published on cycle ``n + 1``: every older
    consumer is served first, and each served consumer's read-ahead is what
    the next blocked one's room comes from.  Every consumer still waiting
    carries its held-back record, every cycle.  Once no consumer is blocked,
    the tier stops evicting: a leg another reader needs no later than the
    head's own is not given back for it, so no read-ahead is traded.

    The scale half: per-cycle wall seconds and calls of every step the
    order adds, printed, with the calls bounded -- one rank and at most one
    candidate walk per cycle, and at most the two commitment censuses the
    cycle took before (#1011 adds one; the report reuses it).
    """

    count = 8
    queue, stage, _plans, shrunk = _fixture(tmp_path, count, EIGHT_HOLD)
    timer = _Timer(monkeypatch)
    served_at: dict[int, int] = {}
    per_cycle: list[dict[str, object]] = []
    unfit: list[str] = []
    began = time.perf_counter()
    for cycle in range(1, count + 3):
        # The rank this cycle takes: consumers served on earlier cycles have
        # their range and are no longer blocked.
        served = tuple(sorted(served_at))
        head = min((n for n in range(count) if n not in served), default=None)
        start = time.perf_counter()
        _cycle(queue, stage, gib=shrunk)
        wall = time.perf_counter() - start
        steps = timer.take()
        events = _events(capsys)
        now_served = [n for n in range(count) if n not in served_at
                      and _published(queue, _mover(n, READING))]
        for n in now_served:
            served_at[n] = cycle
        if head is not None:
            _assert_held_back_records(queue, count, head=head, served=served)
        evicted = [event for event in events
                   if event.get("event") == "claim-order-evicted"]
        record = queue.tier_commitment(TIER) or {}
        order = record.get("claim_order")
        per_cycle.append({"cycle": cycle, "wall_s": round(wall, 4),
                          "relief": (order.get("relief")
                                     if isinstance(order, dict) else None),
                          "served": now_served, "evicted": len(evicted),
                          "landed": _run_movers(queue, stage, count, unfit=unfit),
                          "steps": steps})
        calls = {name: steps.get(name, {"calls": 0})["calls"]
                 for name in _Timer.STEPS}
        assert calls["_rank_claims"] <= 1, steps
        assert calls["_claim_order_candidates"] <= 1, steps
        assert calls["_commitment_census"] <= 2, steps
    total = time.perf_counter() - began
    with capsys.disabled():
        print(json.dumps({"test": "eight-consumer drain", "wall_s": round(total, 3),
                          "cycles": per_cycle}, default=str))

    assert served_at == {n: n + 1 for n in range(count)}, per_cycle
    # Every published range could claim: the order grants what fits.
    assert unfit == [], (unfit, per_cycle)
    # Served: nobody is blocked, and no read-ahead is traded for read-ahead.
    for entry in per_cycle[count:]:
        assert entry["evicted"] == 0, per_cycle


def test_a_younger_blocked_consumer_ranks_before_an_older_read_ahead() -> None:
    """Blocked first, then admission order: where the rank leaves claim time.

    An older consumer that asks for read-ahead waits behind a younger one
    blocked on the range it is reading.  Among the blocked consumers, and
    among the rest, the older claim still ranks first.  Every acceptance
    case above has only blocked consumers, so this is the one test where
    strict claim order and this rank disagree.
    """
    order = window_credit.claim_order([
        {"consumer": "older-read-ahead", "claimed_unix": 100.0,
         "need_gib": 30, "blocked": False},
        {"consumer": "younger-blocked", "claimed_unix": 200.0,
         "need_gib": 30, "blocked": True},
        {"consumer": "youngest-blocked", "claimed_unix": 300.0,
         "need_gib": 30, "blocked": True},
        {"consumer": "oldest-read-ahead", "claimed_unix": 50.0,
         "need_gib": 30, "blocked": False}], free_gib=40)
    assert [(entry["consumer"], entry["standing"], entry["ahead"])
            for entry in order["entries"]] == [
        ("younger-blocked", window_credit.CLAIM_GRANTED, None),
        ("youngest-blocked", window_credit.CLAIM_HEAD, "younger-blocked"),
        ("oldest-read-ahead", window_credit.CLAIM_HELD_BACK,
         "youngest-blocked"),
        ("older-read-ahead", window_credit.CLAIM_HELD_BACK,
         "oldest-read-ahead")]
    assert order["head"] == "youngest-blocked"
    # The granted need plus the head's: the room relief must make.
    assert order["target_free_gib"] == 60


# ------------------------------------------------ N consumers of one range


def test_a_range_two_claimed_consumers_read_is_charged_to_each_of_them(
        tmp_path: Path) -> None:
    """#1011 point 3: how joint commitment charges a range two consumers read.

    Two claimed consumers read the same manifest, so their ``phase-0`` legs
    name the same bytes (``tier_loop._descriptor``) and, by
    ``stage_move.stage_relative``, the same staged name: the second copy
    adopts the first's file.  The ledger is not told.  Each consumer's mover
    is its own action key (the key hashes ``--consumer-action-key``), and the
    second mover's claim takes the range's full tokens from free
    (``PoolQueue._begin_tier_acquire``).  The census then counts the range
    once per consumer.  This test records that per-consumer charge; it is
    evidence for the gap the PR reports, not a behaviour to keep.
    """

    queue, stage = _fixture_queue(tmp_path, 20)
    manifest = _hexkey("7shared")
    first, second = _hexkey("sharedfirst"), _hexkey("sharedsecond")
    plans = {}
    for key, label in ((first, "sharedfirst"), (second, "sharedsecond")):
        plans[key] = _small_plan(queue, key, label=label, manifest=manifest,
                                 phases=4)
        _publish_consumer(queue, key, plans[key], manifest=manifest)
    lead = plans[first]["phases"][0]                       # type: ignore[index]
    first_mover = str(lead["mover_row"]["action_key"])
    _land(queue, stage, consumer=first, manifest=manifest, mover=first_mover,
          name="phase-0", start=int(lead["start_bytes"]),
          end=int(lead["end_bytes"]))
    now = time.time()
    for key in (first, second):
        _claim(queue, key, phase="phase-0", claimed_unix=now - 1000.0,
               reported_unix=now - 10.0)
    row = dict(plans[second]["phases"][0]["mover_row"])    # type: ignore[index]
    second_mover = str(row["action_key"])
    queue.publish(**row)
    received = queue.staged_range_of(first_mover)
    assert received is not None
    residency = row["residency"]
    assert tier_loop._descriptor(
        str(received["manifest_sha256"]), TIER,
        int(received["range_start_bytes"]), int(received["range_end_bytes"])
    ) == tier_loop._descriptor(
        str(residency["manifest_sha256"]), TIER,
        int(residency["range_start_bytes"]), int(residency["range_end_bytes"]))
    assert first_mover != second_mover
    ledger = queue.tier_ledger(TIER)
    before = int(ledger.held().get("stage_gib", 0))

    handles: dict[str, str] = {}
    funded: dict[str, dict[str, object]] = {}
    shortage = queue._begin_tier_acquire(
        second_mover, {TIER: {"stage_gib": PHASE_GIB}}, handles, funded)

    assert shortage is None
    # The claim wins its rename and files the tokens under the mover.
    assert queue._commit_tier_acquire(second_mover, handles) == PHASE_GIB
    # One range of PHASE_GIB bytes, charged twice.
    assert int(ledger.held().get("stage_gib", 0)) == before + PHASE_GIB
    assert before == PHASE_GIB
    tiers = {TIER: _tier_record(stage, gib=20)}
    unknown: list[dict[str, object]] = []
    consumers = tier_loop._planned_consumers(queue, tiers, unknown=unknown)
    census = tier_loop._commitment_census(queue, tiers, consumers=consumers,
                                          unknown=unknown, remember=False)[TIER]
    charged = {entry["consumer"]: entry["gib"] for entry in census["holders"]
               if entry["key"] in (first_mover, second_mover)}
    assert charged == {first: PHASE_GIB, second: PHASE_GIB}, census["holders"]


# ------------------------------------------------ the no_progress verdict


from test_a_staged_wait_is_not_no_progress import (  # noqa: E402
    TOKEN, _finish, _judge, _verdict, _verdict_fixture)
from test_a_resident_range_is_adopted_rather_than_recopied import STAGE_KIND  # noqa: E402

AHEAD = _hexkey("verdictahead")
GRACE_S = 600.0


def _file_order(queue: pool.PoolQueue, key: str, *, standing: str,
                ahead: str | None = AHEAD, relief: str | None = None) -> None:
    """The tier's commitment record, 161 GiB over, ranking ``key``."""

    entries = [{"consumer": AHEAD, "rank": 0, "standing": window_credit.CLAIM_HEAD,
                "ahead": None, "need_gib": 22, "blocked": True}]
    if key != AHEAD:
        entries.append({"consumer": key, "rank": 1, "standing": standing,
                        "ahead": ahead, "need_gib": 22, "blocked": True,
                        "expected_landing_unix": time.time() + 400.0,
                        **({"waiting_on": AHEAD}
                           if standing == window_credit.CLAIM_HELD_BACK else {})})
    queue.file_tier_commitment({
        "tier_id": TIER, "capacity_gib": 565, "committed_gib": 726,
        "over_committed_gib": 161,
        "claim_order": {"head": AHEAD, "entries": entries,
                        **({"relief": relief} if relief else {})}})


def _ahead_running(queue: pool.PoolQueue, *, quiet_s: float | None,
                   heartbeat_age_s: float = 0.0,
                   accepted_count: int | None = None,
                   waiting_exempt: bool | None = None) -> None:
    """The consumer ranked ahead: claimed, with the lease its worker writes.

    ``quiet_s`` is what its progress watch last reported (``None``: the
    lease carries no progress observation).  ``accepted_count`` and
    ``waiting_exempt`` are the watch's accepted reports and its latest
    staged-wait verdict (``ProgressWatch.as_record``).  Written directly:
    ``write_lease`` is the claiming worker's, and checks it is that worker.
    """

    claimed = time.time() - 3600.0
    item = {"action_key": AHEAD, "claimed_unix": claimed,
            "claimed_by": "verdict-fixture", "published_unix": claimed - 10.0}
    path = queue.item_path(pool.CLAIMED, AHEAD)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(item))
    lease: dict[str, object] = {
        "schema": pool.POOL_LEASE_SCHEMA_V1, "action_key": AHEAD,
        "owner": "verdict-fixture", "heartbeat_unix": time.time() - heartbeat_age_s,
        "claimed_unix": claimed, "published_unix": claimed - 10.0}
    if quiet_s is not None:
        observation: dict[str, object] = {
            "source": "action-progress", "quiet_s": quiet_s, "grace_s": GRACE_S}
        if accepted_count is not None:
            observation.update({"accepted_count": accepted_count,
                                "staged_wait_exempt_s": 0.0})
        if waiting_exempt is not None:
            observation["staged_wait"] = {"exempt": waiting_exempt}
        lease["progress_observation"] = observation
    queue.lease_path(AHEAD).parent.mkdir(parents=True, exist_ok=True)
    queue.lease_path(AHEAD).write_text(json.dumps(lease))


def test_a_held_back_consumer_is_exempt_while_the_one_ahead_advances(
        tmp_path: Path) -> None:
    """Over-committed, unpublished, held back behind a consumer whose lease
    carries its counts: the first look is the baseline, the wait is the
    order's, and it is exempt.  The verdict names what holds it back and
    what that consumer last said."""

    queue, item, _row, progress_path = _verdict_fixture(tmp_path, over_committed_gib=161)
    key = str(item["action_key"])
    _file_order(queue, key, standing=window_credit.CLAIM_HELD_BACK)
    _ahead_running(queue, quiet_s=60.0, accepted_count=1)

    verdict = _verdict(queue, item, progress_path)

    assert verdict["movers"][0]["state"] == "unpublished"
    assert verdict["exempt"] is True, verdict
    assert verdict["held_back_by"] == AHEAD
    assert verdict["ahead_grace_s"] == GRACE_S
    assert 60.0 <= float(verdict["ahead_quiet_s"]) < 70.0   # type: ignore[arg-type]
    assert verdict["claim_order"]["standing"] == window_credit.CLAIM_HELD_BACK


def test_a_held_back_consumer_stays_exempt_once_the_one_ahead_has_ended(
        tmp_path: Path) -> None:
    """The consumer ahead finished (or was ended): it holds nothing the order
    waits on, so the one behind it is not blamed for its absence."""

    queue, item, _row, progress_path = _verdict_fixture(tmp_path, over_committed_gib=161)
    _file_order(queue, str(item["action_key"]), standing=window_credit.CLAIM_HELD_BACK)

    verdict = _verdict(queue, item, progress_path)

    assert verdict["exempt"] is True, verdict
    assert verdict["held_back_by"] == AHEAD


def test_a_held_back_consumer_is_not_exempt_behind_one_that_proves_nothing(
        tmp_path: Path) -> None:
    """A lease with no progress observation carries no evidence, and there
    is no earlier evidence to carry: the wait behind it is not the order's,
    and the ``no_progress`` rung may end it, as before #1011.  A consumer
    ahead that shows nothing for a whole window is read the same way
    (``test_a_held_back_consumer_is_not_exempt_once_the_one_ahead_shows_nothing``)."""

    queue, item, _row, progress_path = _verdict_fixture(tmp_path, over_committed_gib=161)
    _file_order(queue, str(item["action_key"]), standing=window_credit.CLAIM_HELD_BACK)
    _ahead_running(queue, quiet_s=None)

    verdict = _verdict(queue, item, progress_path)

    assert verdict["exempt"] is False, verdict
    assert verdict["held_back_by"] == AHEAD
    assert verdict["ahead_evidence"]["evidence"] == "unread", verdict


@pytest.mark.parametrize("standing", [window_credit.CLAIM_HEAD,
                                      window_credit.CLAIM_GRANTED])
def test_the_head_and_a_granted_consumer_are_exempt_on_an_over_committed_tier(
        tmp_path: Path, standing: str) -> None:
    """The order is serving them: the head's room is being made, a granted
    consumer's is already counted.  Exempt while the tier loop is alive."""

    queue, item, _row, progress_path = _verdict_fixture(tmp_path, over_committed_gib=161)
    _file_order(queue, str(item["action_key"]), standing=standing, ahead=AHEAD)

    verdict = _verdict(queue, item, progress_path)

    assert verdict["exempt"] is True, verdict
    assert verdict["claim_order"]["standing"] == standing
    assert "held_back_by" not in verdict


def test_a_consumer_the_order_does_not_rank_keeps_the_old_rule(
        tmp_path: Path) -> None:
    """Over-committed and absent from the rank (a claim time that did not
    read, or a loop from before #1011): not exempt."""

    queue, item, _row, progress_path = _verdict_fixture(tmp_path, over_committed_gib=161)
    _file_order(queue, AHEAD, standing=window_credit.CLAIM_HEAD)

    verdict = _verdict(queue, item, progress_path)

    assert verdict["exempt"] is False, verdict
    assert "claim_order" not in verdict


def test_a_stalled_consumer_ahead_whose_range_landed_is_still_killed(
        tmp_path: Path) -> None:
    """The consumer ahead is the one stalled: its range is resident (its mover
    is in ``done/`` and holds its tokens), so its own verdict is not exempt
    whatever its rank says, and its ``no_progress`` rung ends it.  The order
    shelters a consumer waiting for room, never one that has its bytes."""

    queue, item, row, progress_path = _verdict_fixture(tmp_path, over_committed_gib=161)
    key = _finish(queue, row, pool.DONE)
    queue.mint_tier_capacity(TIER, {STAGE_KIND: 21})
    assert queue.tier_ledger(TIER).acquire(key, {STAGE_KIND: 21})
    _file_order(queue, str(item["action_key"]), standing=window_credit.CLAIM_HEAD)

    verdict = _verdict(queue, item, progress_path)

    assert verdict["movers"] == [{"key": key, "state": "done"}]
    assert verdict["exempt"] is False, verdict


@pytest.mark.parametrize("standing", [window_credit.CLAIM_HEAD,
                                      window_credit.CLAIM_HELD_BACK])
def test_a_stuck_order_ends_the_wait_as_before(tmp_path: Path, standing: str) -> None:
    """Relief was futile, nobody is granted and every ranked consumer is
    blocked: nobody reads, so no egress will make the head's room either.
    The order cannot leave that state by itself, so the stuck rule ends the
    lowest-ranked consumer, this one, and ``no_progress`` ends its wait as
    it did before #1011 -- never a wait without end."""

    queue, item, _row, progress_path = _verdict_fixture(tmp_path, over_committed_gib=161)
    _file_order(queue, str(item["action_key"]), standing=standing, relief="futile")
    _ahead_running(queue, quiet_s=60.0, accepted_count=1)

    verdict = _verdict(queue, item, progress_path)

    assert verdict["exempt"] is False, verdict
    assert verdict["claim_order"]["relief"] == "futile"
    assert verdict["claim_order"]["stuck_victim"] == str(item["action_key"])


# ================================================ the #1022 review (items 2-6)
#
# Each test below names the review item it pins.  Every one fails on the
# first head of #1022 (14d6c96d3aab) on a behavioural assertion.


# ------------------------------------------------ item 2: no starvation


def _lease_reported(queue: pool.PoolQueue, n: int) -> float:
    lease = json.loads(queue.lease_path(_key(n)).read_text())
    return float(lease["progress_observation"]["last_accepted"]["reported_unix"])


def _waiting(queue: pool.PoolQueue, n: int, *, phase: str, since: float) -> None:
    """Consumer ``n``'s reader declares its staged wait on ``phase``'s range,
    the record ``progress.declare_staged_wait`` writes beside its report."""

    path = Path(pb_progress.staged_wait_path(str(queue.action_progress_path(_key(n)))))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({
        "schema": pb_progress.STAGED_WAIT_SCHEMA_V1, "token": "w" * 32,
        "since_unix": float(since), "movers": [_mover(n, phase)]}))


def _read_and_pass(queue: pool.PoolQueue, stage: Path, n: int, *, phase: str,
                   following: str, now: float) -> None:
    """Consumer ``n`` read ``phase``: it reports ``following``, gives
    ``phase`` back (its egress ran) and blocks on ``following``."""

    key = _key(n)
    # The lease its worker refreshes, written in place: ``write_lease`` is
    # the claiming worker's, and after its first write the lease names this
    # box, which the fixture's claim does not.
    path = queue.lease_path(key)
    lease = json.loads(path.read_text())
    lease.update({"heartbeat_unix": now, "progress_observation": {
        "source": "action-progress",
        "last_accepted": {"phase": following, "units_completed": 1,
                          "reported_unix": now}}})
    path.write_text(json.dumps(lease))
    receipt = stage_release.evict(queue, _mover(n, phase), consumer_action_key=key,
                                  stage_root=str(stage), reason="egress")
    assert receipt.get("tokens_released") or receipt.get("complete"), receipt
    plan = residency_plan.read(queue, key)
    egress = next(dict(entry["egress_row"]) for entry in plan["phases"]  # type: ignore[union-attr]
                  if entry["name"] == phase)
    path = queue.item_path(pool.DONE, str(egress["action_key"]))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({**egress, "status": "done"}))
    _waiting(queue, n, phase=following, since=now)


def test_among_blocked_consumers_the_one_blocked_longest_ranks_first() -> None:
    """Item 2's rank: blocked first, then by when each became blocked.

    The older claim blocked later, so it waits behind the younger claim
    that has been blocked longer.  Claim time only breaks a tie."""

    order = window_credit.claim_order([
        {"consumer": "older-claim", "claimed_unix": 100.0,
         "blocked_since_unix": 900.0, "need_gib": 22, "blocked": True},
        {"consumer": "younger-claim", "claimed_unix": 200.0,
         "blocked_since_unix": 300.0, "need_gib": 22, "blocked": True}],
        free_gib=30)

    assert [(entry["consumer"], entry["standing"]) for entry in order["entries"]] == [
        ("younger-claim", window_credit.CLAIM_GRANTED),
        ("older-claim", window_credit.CLAIM_HEAD)]


def test_the_second_consumer_is_served_before_the_first_blocks_again(
        tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """Item 2's starvation fixture: two R12s, one 22 GiB range each, 30 GiB.

    Unchunked, R = 22 GiB, C_eff = 30 GiB (``_fixture(count=2,
    holding=())``, ``_cycle(gib=30)``).  One consumer's range fits at a
    time.  A, blocked longer, is granted on cycle 1.  Between cycles every
    copy that can claim lands (``_run_movers``), and a consumer whose range
    landed reads it, gives it back and blocks on the next one
    (``_read_and_pass``): one T_land + T_phase per cycle here.  On the first
    head A is the older claim again every cycle and is granted again: B's
    GPU idles and B is never killed.  The bound: with k = 2 consumers, B's
    range lands within (k - 1) x (T_phase + T_land), so on cycle 2.

    Served means landed, not published: a head row published before its
    room exists sits unfit in ``ready/``, and if the claim pass reaches it
    first it takes the room the walk gave a granted leg.  So no row may be
    left unfit (``stuck``): the head publishes only once relief made its
    room, and a granted leg's room is not spent by a fence.
    """

    count, room = 2, 30
    queue, stage, _plans, _shrunk = _fixture(tmp_path, count, ())
    for n in range(count):
        _waiting(queue, n, phase=READING, since=_lease_reported(queue, n))
    names = [str(phase["name"]) for phase in DATA["r12"]["phases"]]
    reading = {n: READING for n in range(count)}
    served: list[tuple[int, int, str]] = []
    stuck: list[str] = []
    for cycle in range(1, 5):
        _cycle(queue, stage, gib=room)
        unfit: list[str] = []
        landed = _run_movers(queue, stage, count, unfit=unfit)
        stuck.extend(f"cycle {cycle} {name}" for name in unfit)
        for n in [n for n in range(count) if f"{n}:{reading[n]}" in landed]:
            served.append((cycle, n, reading[n]))
            following = names[names.index(reading[n]) + 1]
            _read_and_pass(queue, stage, n, phase=reading[n],
                           following=following, now=time.time())
            reading[n] = following
    events = _events(capsys)

    b_served = [cycle for cycle, n, _phase in served if n == 1]
    assert b_served and b_served[0] <= 2, (served, stuck,
                                           _diagnosis(queue, events, count))
    assert stuck == [], (stuck, served)


# ------------------------------------------------ item 3: Belady for a blocked head


def test_a_granted_readers_next_range_is_kept_for_a_head_that_lands_later(
        tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """Item 3: A is granted and holds ``chain-042``, the range it reads the
    moment its ``chain-043`` is read (``seconds_until_needed`` 0).  B, the
    blocked head, cannot have its range for ``need_bytes / rate`` seconds.
    Evicting A's next range for B trades a range needed now for one that
    lands later, and A's reader already saw it hit.  It stays."""

    shift = time.time() - SAMPLE_UNIX
    queue, stage = _fixture_queue(tmp_path, DATA["tier"]["capacity_gib"])
    _blocked(queue, stage, 0, shift=shift, holding=("chain-042",))
    _blocked(queue, stage, 1, shift=shift, holding=())
    kept = _mover(0, "chain-042")
    room = int(PHASES["chain-042"]["stage_gib"]) + 30

    _cycle(queue, stage, gib=room)
    events = _events(capsys)

    record = queue.tier_commitment(TIER) or {}
    order = record.get("claim_order") or {}
    standing = {entry["consumer"]: entry["standing"]
                for entry in order.get("entries") or ()}
    assert standing == {_key(0): window_credit.CLAIM_GRANTED,
                        _key(1): window_credit.CLAIM_HEAD}, order
    assert queue.tier_ledger(TIER).holder_tokens(kept), _diagnosis(queue, events, 2)
    assert not [event for event in events
                if event.get("event") == "claim-order-evicted"
                and event.get("mover") == kept], events


# ------------------------------------------------ item 6: futile, on transition


def test_a_futile_relief_is_told_once_while_it_stays_futile(
        tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """Item 6: three cycles in one futile state file one event, not three.
    Each is a line in every consumer's 256-line event file."""

    queue, stage, _plans, _shrunk = _fixture(tmp_path, 2, ())
    for _cycle_number in range(3):
        _cycle(queue, stage, gib=30)
    events = _events(capsys)

    told = [event for event in events
            if event.get("event") == "claim-order-eviction-futile"]
    filed = [event for event in queue.consumer_events(_key(1))
             if event.get("event") == "claim-order-eviction-futile"]
    assert len(told) == 1, told
    assert len(filed) == 1, filed


# ------------------------------------------------ item 4: chunked plans

CHUNKS = 2


def _chunked_plan(queue: pool.PoolQueue, key: str, *, label: str,
                  manifest: str, stage_root: str) -> dict[str, object]:
    """R12's plan with every phase sealed as ``CHUNKS`` stage chunks (#675)."""

    r12 = DATA["r12"]
    total = int(r12["phases"][-1]["end_bytes"])
    built = []
    for phase in r12["phases"]:
        name = str(phase["name"])
        start, end = int(phase["start_bytes"]), int(phase["end_bytes"])
        cuts = [start + (end - start) * index // CHUNKS
                for index in range(CHUNKS)] + [end]
        chunks = []
        for index in range(CHUNKS):
            low, high = cuts[index], cuts[index + 1]
            gib = storage_tiers.stage_tokens_for_bytes(high - low)
            chunks.append({
                "chunk_index": index, "start_bytes": low, "end_bytes": high,
                "stage_gib": gib,
                "mover_row": {
                    **_row(queue, _hexkey(f"{label}mover{name}c{index}"),
                           {STAGE_KIND: gib, FILL: r12["sealed_fill_mb_s"],
                            "cpu": 2, "mem_gb": 1}),
                    "residency": {
                        "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                        "manifest_sha256": manifest, "manifest_bytes": total,
                        "range_start_bytes": low, "range_end_bytes": high}},
                "egress_row": _row(queue, _hexkey(f"{label}egress{name}c{index}"),
                                   {"mem_gb": 1})})
        built.append({"name": name, "start_bytes": start, "end_bytes": end,
                      "stage_gib": sum(int(chunk["stage_gib"]) for chunk in chunks),
                      "stage_chunks": chunks})
    return residency_plan.build_plan(
        consumer_action_key=key, tier_id=TIER, stage_root=stage_root,
        manifest_sha256=manifest, manifest_bytes=total, phases=built)


def _chunk_of(plan, name: str, index: int) -> dict[str, object]:
    phase = next(entry for entry in plan["phases"] if entry["name"] == name)
    return phase["stage_chunks"][index]


def _chunked_blocked(queue: pool.PoolQueue, stage: Path, n: int, *, shift: float,
                     holding: tuple[tuple[str, int], ...]) -> dict[str, object]:
    """``_blocked`` on the chunked plan: ``holding`` names landed chunks."""

    r12 = DATA["r12"]
    key, manifest = _key(n), _manifest(n)
    plan = _chunked_plan(queue, key, label=f"order{n}", manifest=manifest,
                         stage_root=str(stage))
    _consumer(queue, key, plan, manifest=manifest,
              mem_gb=r12["resources"]["mem_gb"])
    for name, index in holding:
        chunk = _chunk_of(plan, name, index)
        start, end = int(chunk["start_bytes"]), int(chunk["end_bytes"])
        _land(queue, stage, consumer=key, manifest=manifest,
              mover=str(chunk["mover_row"]["action_key"]),  # type: ignore[index]
              name=f"{name}c{index}", start=start, end=end,
              seconds=(end - start) / RATES[name])
    for name in PASSED:
        for index in range(CHUNKS):
            egress = dict(_chunk_of(plan, name, index)["egress_row"])  # type: ignore[arg-type]
            path = queue.item_path(pool.DONE, str(egress["action_key"]))
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps({**egress, "status": "done"}))
    offset = n * CLAIM_GAP_S
    _claim(queue, key, phase=READING,
           claimed_unix=r12["claimed_unix"] + shift + offset,
           reported_unix=r12["accepted"]["reported_unix"] + shift + offset,
           gpu_budget=r12["gpu_memory_budget_bytes"])
    return plan


def _chunked_cycle(queue: pool.PoolQueue, stage: Path, *, gib: int,
                   chunk_gib: int) -> None:
    """One cycle on a stage tier that announces its chunking."""

    record = {**_tier_record(stage, gib=gib), "promotion_chunk_gib": chunk_gib,
              "window_gib": CHUNKS * chunk_gib}
    tier_loop.cycle(queue, host="dl380g10", source_pool="storage_pool",
                    receipts=tier_loop.ReceiptCache(),
                    discover=lambda **_kwargs: {TIER: record})


def test_chunked_consumers_holding_part_of_their_reading_phase_do_not_deadlock(
        tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """Item 4's hold-and-wait: C_eff < sum(held chunks) + chunk_head.

    Two chunked R12s each hold chunk 0 of ``chain-043``, the phase they are
    reading, and each is blocked on its chunk 1.  The tier has room for
    neither chunk 1 and nothing is ranked past a horizon, so relief finds no
    read-ahead to evict.  On the first head relief is futile, nothing is
    published and the stuck rule ends both.  Now the lowest-ranked blocked
    consumer's reading-phase chunk is preempted -- evicted whole, copied
    again when its turn comes -- and the head's chunk is published on the
    first cycle, with no kill."""

    shift = time.time() - SAMPLE_UNIX
    queue, stage = _fixture_queue(tmp_path, DATA["tier"]["capacity_gib"])
    plans = [_chunked_blocked(queue, stage, n, shift=shift, holding=((READING, 0),))
             for n in range(2)]
    chunk_gib = int(_chunk_of(plans[0], READING, 1)["stage_gib"])  # type: ignore[arg-type]
    held = sum(int(_chunk_of(plan, READING, 0)["stage_gib"])  # type: ignore[arg-type]
               for plan in plans)
    room = held + chunk_gib - 1
    head_chunk = str(_chunk_of(plans[0], READING, 1)["mover_row"]["action_key"])  # type: ignore[index]
    preempted = str(_chunk_of(plans[1], READING, 0)["mover_row"]["action_key"])  # type: ignore[index]

    _chunked_cycle(queue, stage, gib=room, chunk_gib=chunk_gib)
    events = _events(capsys)

    assert _published(queue, head_chunk), _diagnosis(queue, events, 2)
    assert not queue.tier_ledger(TIER).holder_tokens(preempted), events
    evicted = [event for event in events
               if event.get("event") == "claim-order-evicted"]
    assert [(event["mover"], event.get("basis")) for event in evicted] == [
        (preempted, "preempt-reading-phase")], evicted
    order = (queue.tier_commitment(TIER) or {}).get("claim_order") or {}
    assert order.get("relief") != "futile", order
    assert order.get("stuck_victim") is None, order


def test_a_reader_waiting_again_for_a_chunk_it_lost_keeps_the_one_it_holds(
        tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """Round 2, item 2: a gather that spans a chunk boundary.

    Consumer 1's gather needs chunks 0 and 1 of ``chain-043``.  Chunk 0 had
    landed and was preempted while it waited for chunk 1; chunk 1 landed,
    the gather raised ``StagedRangeNotLanded``, and the reader's one retry
    now waits for chunk 0 again while it holds chunk 1.  A reader reads in
    byte order, so a blocked chunk before a landed one is that retry (or an
    out-of-order landing).  On the first head relief preempted chunk 1 as
    well, and the retry's gather lost it again: the second loss ends the
    run.  Now its reading phase is protected, relief is futile, and the
    stuck rule names it -- the lowest ranked -- for a recorded kill."""

    shift = time.time() - SAMPLE_UNIX
    queue, stage = _fixture_queue(tmp_path, DATA["tier"]["capacity_gib"])
    head = _chunked_blocked(queue, stage, 0, shift=shift, holding=((READING, 0),))
    retry = _chunked_blocked(queue, stage, 1, shift=shift, holding=((READING, 1),))
    held = (int(_chunk_of(head, READING, 0)["stage_gib"])  # type: ignore[arg-type]
            + int(_chunk_of(retry, READING, 1)["stage_gib"]))  # type: ignore[arg-type]
    needs = (int(_chunk_of(head, READING, 1)["stage_gib"]),  # type: ignore[arg-type]
             int(_chunk_of(retry, READING, 0)["stage_gib"]))  # type: ignore[arg-type]
    kept = str(_chunk_of(retry, READING, 1)["mover_row"]["action_key"])  # type: ignore[index]

    _chunked_cycle(queue, stage, gib=held + min(needs) - 1, chunk_gib=needs[0])
    events = _events(capsys)

    evicted = [event for event in events
               if event.get("event") == "claim-order-evicted"]
    assert evicted == [], evicted
    assert queue.tier_ledger(TIER).holder_tokens(kept), events
    order = (queue.tier_commitment(TIER) or {}).get("claim_order") or {}
    assert order.get("head") == _key(0), order
    assert order.get("relief") == "futile", order
    assert order.get("stuck_victim") == _key(1), order
    assert order.get("preempt_protected") == [_key(1)], order
    futile = [event for event in events
              if event.get("event") == "claim-order-eviction-futile"]
    assert [event.get("preempt_protected") for event in futile] == [[_key(1)]], futile


#: The range states the reader accepts: PQ ``prismaquant/residency_map.py``
#: ``LANDING_STATES`` at PQ ``f0fa27f7e7e``.  Its ``_read_landing`` returns
#: ``None`` for a whole record that lists any other state, and the reader
#: then falls back to its bounded wait (``STAGED_RANGE_WAIT_S``) and declares
#: no staged wait.  Pinned here because the test may not import the reader.
READER_LANDING_STATES = ("ready", "claimed", "unpublished", "evicted",
                         "done-not-resident", "terminal-no-receipt")


def test_every_landing_state_the_order_writes_is_one_the_reader_reads(
        tmp_path: Path) -> None:
    """Round 2: the first head wrote the range a held-back consumer waits for
    as ``held-by-claim-order``, a state the reader does not know.  The reader
    drops the whole record on it, so the consumer the order holds back --
    the case #1011 is for -- lost its landing record and waited on the
    reader's bounded clock instead.  The range is ``unpublished``, which it
    is, and says the rest in fields of its own."""

    queue, stage, _plans, shrunk = _fixture(tmp_path, 2, TWO_HOLD)

    _cycle(queue, stage, gib=shrunk)

    states = {}
    for n in range(2):
        for row in _landing(queue, _key(n))["ranges"]:  # type: ignore[union-attr]
            states[(n, row["phase"])] = row["state"]
            assert row["state"] in READER_LANDING_STATES, (n, row)
    held = {row["phase"]: row for row in _landing(queue, _key(1))["ranges"]}  # type: ignore[union-attr]
    assert held[READING]["state"] == "unpublished", held[READING]
    assert held[READING]["held_back_by"] == _key(0), held[READING]


LAST = _hexkey("verdictlast")


def _file_stuck_order(queue: pool.PoolQueue, key: str, *, rank: int) -> None:
    """Three blocked consumers, none granted, relief futile: the order is
    stuck.  ``key`` is filed at ``rank``; the others are ``AHEAD`` and ``LAST``."""

    names = [AHEAD, LAST]
    names.insert(rank, key)
    entries = []
    for position, name in enumerate(names):
        entry = {"consumer": name, "rank": position, "need_gib": 22,
                 "blocked": True,
                 "standing": (window_credit.CLAIM_HEAD if position == 0
                              else window_credit.CLAIM_HELD_BACK),
                 "ahead": names[position - 1] if position else None}
        if position:
            entry.update({"waiting_on": names[0],
                          "expected_landing_unix": time.time() + 400.0})
        entries.append(entry)
    queue.file_tier_commitment({
        "tier_id": TIER, "capacity_gib": 565, "committed_gib": 726,
        "over_committed_gib": 161,
        "claim_order": {"head": names[0], "entries": entries, "relief": "futile"}})


@pytest.mark.parametrize("rank", [0, 1], ids=["head", "middle"])
def test_a_stuck_order_ends_only_its_lowest_ranked_consumer(
        tmp_path: Path, rank: int) -> None:
    """Item 4: every rung reads the same record, so a stuck rule every
    consumer applies to itself ends all of them in one cycle.  It names one
    consumer, the lowest ranked; the rest stay exempt."""

    queue, item, _row, progress_path = _verdict_fixture(tmp_path, over_committed_gib=161)
    _file_stuck_order(queue, str(item["action_key"]), rank=rank)
    _ahead_running(queue, quiet_s=60.0, accepted_count=3)

    verdict = _verdict(queue, item, progress_path)

    assert verdict["exempt"] is True, verdict
    assert verdict["claim_order"].get("stuck_victim") == LAST, verdict


def test_an_order_with_a_granted_consumer_is_not_stuck(tmp_path: Path) -> None:
    """Item 4: relief is futile and both are blocked, but the older one is
    granted: its range lands, it reads it, and its egress returns the room.
    That order is moving, so its head is exempt."""

    queue, item, _row, progress_path = _verdict_fixture(tmp_path, over_committed_gib=161)
    key = str(item["action_key"])
    queue.file_tier_commitment({
        "tier_id": TIER, "capacity_gib": 30, "committed_gib": 44,
        "over_committed_gib": 14,
        "claim_order": {"head": key, "relief": "futile", "entries": [
            {"consumer": AHEAD, "rank": 0, "standing": window_credit.CLAIM_GRANTED,
             "ahead": None, "need_gib": 22, "blocked": True},
            {"consumer": key, "rank": 1, "standing": window_credit.CLAIM_HEAD,
             "ahead": AHEAD, "need_gib": 22, "blocked": True}]}})

    verdict = _verdict(queue, item, progress_path)

    assert verdict["exempt"] is True, verdict


# ------------------------------------------------ item 5: evidence, not a clock


def test_a_late_lease_on_the_ahead_box_does_not_strip_the_exemption(
        tmp_path: Path) -> None:
    """Item 5: the consumer ahead advanced within this consumer's evidence
    window, and its box's loop is 90 s late with the lease.  A late loop on
    another box is not a stall; the exemption holds."""

    queue, item, _row, progress_path = _verdict_fixture(tmp_path, over_committed_gib=161)
    _file_order(queue, str(item["action_key"]), standing=window_credit.CLAIM_HELD_BACK)
    now = time.time()
    _ahead_running(queue, quiet_s=GRACE_S - 10.0, heartbeat_age_s=90.0,
                   accepted_count=5)

    first = _judge(queue, item, progress_path, prior=None, window_s=GRACE_S,
                   now=now - 100.0)
    late = _judge(queue, item, progress_path, prior=first, window_s=GRACE_S,
                  now=now)

    assert late["exempt"] is True, late
    assert late.get("ahead_evidence", {}).get("evidence") == "carried", late


def test_a_held_back_consumer_is_not_exempt_once_the_one_ahead_shows_nothing(
        tmp_path: Path) -> None:
    """Item 5: a whole evidence window with no accepted report, no credited
    wait and no exempt verdict from the consumer ahead: it is not advancing,
    whatever its last quiet said, and the wait behind it is not the order's."""

    queue, item, _row, progress_path = _verdict_fixture(tmp_path, over_committed_gib=161)
    _file_order(queue, str(item["action_key"]), standing=window_credit.CLAIM_HELD_BACK)
    now = time.time()
    _ahead_running(queue, quiet_s=60.0, accepted_count=5)

    first = _judge(queue, item, progress_path, prior=None, window_s=GRACE_S,
                   now=now - GRACE_S - 1.0)
    idle = _judge(queue, item, progress_path, prior=first, window_s=GRACE_S,
                  now=now)

    assert idle["exempt"] is False, idle
    assert idle.get("ahead_evidence", {}).get("evidence") == "none", idle


@pytest.mark.parametrize("change,evidence", [
    ({"quiet_s": 5.0, "accepted_count": 6}, "ahead-advanced"),
    # Waiting itself, on a verdict its own worker judged exempt, from a box
    # whose loop is late with the lease.
    ({"quiet_s": GRACE_S - 5.0, "heartbeat_age_s": 90.0, "accepted_count": 5,
      "waiting_exempt": True}, "ahead-waiting"),
], ids=["advanced", "waiting"])
def test_a_held_back_consumer_is_exempt_on_the_ahead_ones_evidence(
        tmp_path: Path, change: dict[str, object], evidence: str) -> None:
    """Item 5: growth of the ahead consumer's ``accepted_count``, or its own
    exempt staged wait, renews the exemption past a whole window."""

    queue, item, _row, progress_path = _verdict_fixture(tmp_path, over_committed_gib=161)
    _file_order(queue, str(item["action_key"]), standing=window_credit.CLAIM_HELD_BACK)
    now = time.time()
    _ahead_running(queue, quiet_s=60.0, accepted_count=5)
    first = _judge(queue, item, progress_path, prior=None, window_s=GRACE_S,
                   now=now - GRACE_S - 1.0)
    _ahead_running(queue, **change)  # type: ignore[arg-type]

    renewed = _judge(queue, item, progress_path, prior=first, window_s=GRACE_S,
                     now=now)

    assert renewed["exempt"] is True, renewed
    assert renewed.get("ahead_evidence", {}).get("evidence") == evidence, renewed


# ------------------------------------------------ item 7: a measurement

@pytest.mark.parametrize("room_chunks", [3, 4, 6])
def test_measure_the_legs_a_granted_chunked_window_publishes_a_cycle(
        tmp_path: Path, capsys: pytest.CaptureFixture[str], room_chunks: int) -> None:
    """Item 7, a measurement and not a red test: it asserts only that the
    cycles ran, and prints what they published.

    Two chunked R12s, nothing landed, on a stage tier of ``room_chunks``
    chunks.  Three cycles, each followed by every copy that can claim
    landing (``_run_movers``).  Per cycle it records the tier's
    over-commitment, each consumer's standing and how many legs each
    window published that cycle, and it prices the most any granted window
    published in a cycle against the tier's landing rate: the fraction of
    the landing rate one granted window can use when the cycle, not the
    copy, is what paces it.
    """

    shift = time.time() - SAMPLE_UNIX
    queue, stage = _fixture_queue(tmp_path, DATA["tier"]["capacity_gib"])
    plans = [_chunked_blocked(queue, stage, n, shift=shift, holding=())
             for n in range(2)]
    first = _chunk_of(plans[0], READING, 0)
    chunk_gib = int(first["stage_gib"])                     # type: ignore[arg-type]
    chunk_bytes = int(first["end_bytes"]) - int(first["start_bytes"])  # type: ignore[arg-type]
    movers = [[str(chunk["mover_row"]["action_key"])
               for phase in plan["phases"] for chunk in phase["stage_chunks"]]  # type: ignore[index]
              for plan in plans]
    seen: set[str] = set()
    cycles = []
    for cycle in range(1, 4):
        _chunked_cycle(queue, stage, gib=room_chunks * chunk_gib, chunk_gib=chunk_gib)
        record = queue.tier_commitment(TIER) or {}
        order = record.get("claim_order") or {}
        standing = {entry["consumer"]: entry["standing"]
                    for entry in order.get("entries") or ()}
        published = {}
        for n in range(2):
            new = [mover for mover in movers[n]
                   if mover not in seen and _published(queue, mover)]
            seen.update(new)
            published[n] = len(new)
        cycles.append({
            "cycle": cycle, "over_committed_gib": record.get("over_committed_gib"),
            "relief": order.get("relief"),
            "standing": {n: standing.get(_key(n)) for n in range(2)},
            "legs_published": published,
            "landed": _run_movers(queue, stage, 2)})
    capsys.readouterr()
    granted = [entry["legs_published"][n] for entry in cycles for n in range(2)
               if entry["standing"][n] == window_credit.CLAIM_GRANTED]
    landing = min(RATES.values())
    most = max(granted, default=0)
    window_rate = most * chunk_bytes / tier_loop.CYCLE_INTERVAL_S
    with capsys.disabled():
        print(json.dumps({
            "test": "item-7 legs per granted window per cycle",
            "room_chunks": room_chunks, "chunk_gib": chunk_gib,
            "chunk_bytes": chunk_bytes,
            "cycle_interval_s": tier_loop.CYCLE_INTERVAL_S,
            "slowest_landing_bytes_per_s": round(landing, 1),
            "granted_legs_per_cycle": granted,
            "granted_window_bytes_per_s_at_most": round(window_rate, 1),
            "fraction_of_landing_rate": (round(min(1.0, window_rate / landing), 4)
                                         if landing > 0 else None),
            "cycles": cycles}, default=str))
    assert len(cycles) == 3

