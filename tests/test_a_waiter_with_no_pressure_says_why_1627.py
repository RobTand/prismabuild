"""A waiter that produces no pressure says why (#1627).

On 2026-10-08 the stage tier held 887 GiB in 178 holders of ended consumers
while a strict consumer waited on native residency, and the loop evicted
nothing for over an hour.  Fourteen private-queue cases on main show the sweep
evicting correctly when a waiter produces pressure.  So the difference is in
what produces it: ``window_pressure`` decides, in about twenty places, that a
waiter asks for no room, and none of them left a record.  An operator saw only
a full tier and a waiter.

Everything runs on a ``tmp_path`` queue through the real tier cycle.  Nothing
touches a live queue.
"""
from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from prismabuild import pool, residency_plan  # noqa: E402
import stage_release  # noqa: E402
import tier_loop  # noqa: E402
from test_a_resident_range_is_adopted_rather_than_recopied import (  # noqa: E402
    PHASE_GIB, TIER, _cycle, _stage_range, _tier_record)
from test_prelaunch_group_reconcile_1594 import _hexkey, _queue  # noqa: E402
from test_prelaunch_tier_gate_1594 import _live  # noqa: E402
from test_prelaunch_tier_module_1594 import _declared_plan  # noqa: E402
from test_prelaunch_waiter_frees_orphans_1594 import (  # noqa: E402
    CAPACITY, _held_orphans, _orphans)
from test_a_consumer_stages_only_to_its_refill_horizon import (  # noqa: E402
    _fixture_queue)
from test_admission_charges_refill_horizons_jointly import (  # noqa: E402
    NEWCOMER_FOOTPRINT, READER_FOOTPRINT, Consumer, _reader, _tiers)

EVENT = "window-pressure-skipped"


def _tier(tmp_path: Path):
    queue = _queue(tmp_path, stage_gib=CAPACITY)
    stage = tmp_path / "stage"
    stage.mkdir()
    stage_release.register_stage_root(queue, tier_id=TIER, stage_root=stage)
    return queue, stage


def _pressure(queue, stage, **kwargs):
    return tier_loop.window_pressure(
        queue, tiers={TIER: _tier_record(stage, gib=CAPACITY)}, **kwargs)


def _skips(queue, stage, **kwargs):
    skipped: list[dict[str, object]] = []
    result = _pressure(queue, stage, skipped=skipped, **kwargs)
    return result, skipped


def _events(queue, key: str) -> list[dict[str, object]]:
    return [event for event in queue.consumer_events(key)
            if event.get("event") == EVENT]


def _too_big(queue, stage):
    """A declared waiter whose peak (26) is above the tier (20)."""
    movers = _orphans(queue, stage, 7)
    waiter = _hexkey("too-big")
    plan = _declared_plan(queue, waiter,
                          [("p0", 25, True, 1), ("p1", 1, False, 1)], tag="b")
    _live(queue, plan, waiter)
    return waiter, movers


def _beside_a_live_prefix(queue, stage):
    """A waiter that fits only if a live prefix were evictable (the PR 1615 case)."""
    _orphans(queue, stage, 2)
    live = _hexkey("live-declared")
    live_plan = _declared_plan(queue, live, [("a0", PHASE_GIB, True, 1),
                                             ("a1", 1, False, 1)], tag="a")
    _stage_range(queue, mover=residency_plan.leads_for(live_plan)[0],
                 consumer=live, stage=stage, ordinal=5, manifest="d" * 64)
    _live(queue, live_plan, live)
    waiter = _hexkey("waiter-too-big-for-the-rest")
    _live(queue, _declared_plan(queue, waiter, [("b0", 19, True, 1)], tag="b"),
          waiter)
    return waiter


def test_a_waiter_above_the_tier_says_the_gate_refused_it(tmp_path: Path) -> None:
    queue, stage = _tier(tmp_path)
    waiter, _movers = _too_big(queue, stage)
    pressure, skipped = _skips(queue, stage)
    assert pressure == {}
    [skip] = [row for row in skipped if row["consumer"] == waiter]
    assert skip["scope"] == "waiter" and skip["tier_id"] == TIER
    assert skip["reason"] == "gate-refused", skip
    assert skip["capacity_gib"] == CAPACITY and skip["cur_min_gib"] == 26
    assert skip["evictable_gib"] == 14, "seven orphans of two GiB"


def test_a_shortfall_beyond_everything_evictable_says_so(tmp_path: Path) -> None:
    queue, stage = _tier(tmp_path)
    waiter = _beside_a_live_prefix(queue, stage)
    pressure, skipped = _skips(queue, stage)
    # The live reader's own next phase (1 GiB) is the only pressure, and free
    # (14) covers it: nothing is evicted for the waiter.
    assert pressure.get(TIER, 0) <= 1
    [skip] = [row for row in skipped if row["consumer"] == waiter]
    assert skip["reason"] == "shortfall-exceeds-evictable", skip
    assert skip["evictable_gib"] == 4 and skip["shortfall_gib"] > 4
    assert skip["free_gib"] == 14


def test_a_waiter_the_commitment_refuses_says_which_decision(tmp_path: Path) -> None:
    """The real decision, not a copy of its schema (review of 5c0f5997c2).

    10 GiB held by a holder with no receipt and no live owner, a reader at its
    12 GiB footprint and a newcomer at 14, on a stage of 30: 10 + 12 + 14 = 36.
    The fixture is the one ``test_joint_commitment_stalls_are_reported`` uses,
    so the refusal is ``_commitment_decision``'s own.
    """
    queue, stage = _fixture_queue(tmp_path, 30)
    assert queue.tier_ledger(TIER).acquire(_hexkey("static"), {"stage_gib": 10})
    _reader(queue, stage)
    newcomer = Consumer(queue, stage, "n")
    skipped: list[dict[str, object]] = []
    tier_loop.window_pressure(queue, tiers=_tiers(stage), skipped=skipped)
    [skip] = [row for row in skipped if row["consumer"] == newcomer.key]
    assert skip["reason"] == "commitment-refused", skip
    decision = skip["decision"]
    assert decision["capacity_gib"] == 30, decision
    assert decision["committed_gib"] == 10 + READER_FOOTPRINT
    assert decision["footprint_gib"] == NEWCOMER_FOOTPRINT
    assert decision["shortfall_gib"] == 10 + READER_FOOTPRINT + NEWCOMER_FOOTPRINT - 30
    assert decision["terms_total"] >= 2
    static = [term for term in decision["terms"] if term.get("holder") == _hexkey("static")]
    assert static and static[0]["basis"] == "receipt-less" and static[0]["evictable"] is False


def test_a_commitment_that_could_not_be_read_keeps_its_error(
        tmp_path: Path, monkeypatch) -> None:
    queue, stage = _tier(tmp_path)
    _orphans(queue, stage, 7)
    waiter = _hexkey("unreadable-census-waiter")
    plan = _declared_plan(queue, waiter,
                          [("p0", 10, True, 1), ("p1", 1, False, 1)], tag="u")
    _live(queue, plan, waiter)
    monkeypatch.setattr(tier_loop, "_commitment_census",
                        lambda *a, **k: {TIER: {"error": "ledger torn mid-read"}})
    _pressure, skipped = _skips(queue, stage)
    [skip] = [row for row in skipped if row["consumer"] == waiter]
    assert skip["reason"] == "commitment-refused"
    assert skip["decision"]["error"] == "ledger torn mid-read"


def test_a_waiter_with_no_evictable_bytes_says_how_the_tier_is_held(
        tmp_path: Path) -> None:
    """Nothing to give back, and the record says what the tier holds instead."""
    queue, stage = _tier(tmp_path)
    live = _hexkey("only-live-holder")
    live_plan = _declared_plan(
        queue, live, [(f"a{i}", PHASE_GIB, True, 1) for i in range(7)]
        + [("tail", 1, False, 1)], tag="n")
    for ordinal, lead in enumerate(residency_plan.leads_for(live_plan)):
        _stage_range(queue, mover=lead, consumer=live, stage=stage,
                     ordinal=ordinal, manifest="d" * 64)
    _live(queue, live_plan, live)                # 14 of 20 held, all protected
    waiter = _hexkey("waiter-with-no-room")
    _live(queue, _declared_plan(queue, waiter, [("b0", 10, True, 1)], tag="m"),
          waiter)
    pressure, skipped = _skips(queue, stage)
    assert pressure.get(TIER, 0) <= 1
    [skip] = [row for row in skipped if row.get("scope") == "tier"]
    assert skip["reason"] == "no-evictable" and skip["tier_id"] == TIER
    assert skip["held_gib"] == 14 and skip["free_gib"] == 6
    assert skip["holders"] == 7 and skip["live_holders"] + skip["prelaunch_holders"] == 7
    assert skip["receiptless_holders"] == 0 and skip["orphan_gib"] == 0
    # Both are waiting newcomers here: the live reader is a declared unit that
    # is not admitted either.  The row names every one the tier cannot serve.
    assert waiter in skip["waiters"] and live in skip["waiters"]


def test_a_feasible_waiter_is_not_a_skip(tmp_path: Path) -> None:
    """The control: relief that is asked for is pressure, not a skipped waiter."""
    queue, stage = _tier(tmp_path)
    _orphans(queue, stage, 7)
    waiter = _hexkey("feasible-waiter")
    plan = _declared_plan(queue, waiter,
                          [("p0", 10, True, 1), ("p1", 1, False, 1)], tag="f")
    _live(queue, plan, waiter)
    pressure, skipped = _skips(queue, stage)
    assert pressure.get(TIER, 0) > 0
    assert [row for row in skipped if row["consumer"] == waiter] == []


def test_asking_why_does_not_change_the_pressure(tmp_path: Path) -> None:
    """Reporting is a side channel: the answer is identical with and without it."""
    for build in (_too_big, _beside_a_live_prefix):
        (tmp_path / build.__name__).mkdir()
        queue, stage = _tier(tmp_path / build.__name__)
        build(queue, stage)
        bare = _pressure(queue, stage)
        asked, _skipped = _skips(queue, stage)
        assert asked == bare


def test_the_cycle_reports_a_skipped_waiter_once_per_change(tmp_path: Path) -> None:
    queue, stage = _tier(tmp_path)
    waiter, movers = _too_big(queue, stage)
    for _ in range(4):
        _cycle(queue, stage, gib=CAPACITY)
    events = _events(queue, waiter)
    assert len(events) == 1, [event.get("reason") for event in events]
    assert events[0]["consumer"] == waiter and events[0]["reason"] == "gate-refused"
    assert _held_orphans(queue, movers) == 7, "no futile eviction"


def test_a_waiter_that_stops_and_returns_is_reported_again(tmp_path: Path) -> None:
    queue, stage = _tier(tmp_path)
    waiter, _movers = _too_big(queue, stage)
    _cycle(queue, stage, gib=CAPACITY)
    queue.item_path(pool.READY, waiter).rename(tmp_path / "parked.json")
    _cycle(queue, stage, gib=CAPACITY)
    (tmp_path / "parked.json").rename(queue.item_path(pool.READY, waiter))
    _cycle(queue, stage, gib=CAPACITY)
    assert len(_events(queue, waiter)) == 2


def test_a_tier_with_no_waiter_reports_nothing(tmp_path: Path) -> None:
    queue, stage = _tier(tmp_path)
    _orphans(queue, stage, 7)
    _, skipped = _skips(queue, stage)
    assert skipped == []
    _cycle(queue, stage, gib=CAPACITY)
    assert [event for event in queue.host_events()
            if event.get("event") == EVENT] == []


def _claimant(queue, stage, label: str, demand_gib: int) -> str:
    """A ready consumer whose lead holds its tokens and whose claim asks the
    stage for ``demand_gib``: the third term of ``window_pressure`` (#901)."""
    consumer = _hexkey(label)
    plan = _declared_plan(queue, consumer, [("c0", 1, False, 1)], tag=label[:2])
    lead = residency_plan.leads_for(plan)[0]
    _stage_range(queue, mover=lead, consumer=consumer, stage=stage,
                 ordinal=hash(label) % 90 + 10, manifest="c" * 64)
    residency_plan.freeze(queue, plan)
    queue.publish(
        action_key=consumer, cas_root=str(queue.root / "cas"),
        checkout_root=str(queue.root / "co"),
        worker_script=str(queue.root / "worker.py"),
        resources={"cpu": 1, "mem_gb": 1, "stage_gib@" + TIER: demand_gib},
        residency={"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                   "manifest_sha256": "c" * 64, "manifest_bytes": 1 << 30,
                   "leads": [lead]})
    return consumer


def test_two_claims_with_different_reasons_each_name_their_consumer(
        tmp_path: Path) -> None:
    """A claim is somebody's: the review found them filed with no consumer, so
    two unrelated waits merged into whichever came first."""
    queue, stage = _tier(tmp_path)
    _orphans(queue, stage, 7)
    big = _claimant(queue, stage, "claim-above-the-tier", 25)
    stuck = _claimant(queue, stage, "claim-beyond-evictable", 19)
    _pressure, skipped = _skips(queue, stage)
    claims = {row["consumer"]: row for row in skipped if row["scope"] == "claim"}
    assert set(claims) == {big, stuck}, skipped
    assert claims[big]["reason"] == "gate-refused" and claims[big]["cur_min_gib"] == 25
    assert claims[stuck]["reason"] == "shortfall-exceeds-evictable"
    assert claims[stuck]["cur_min_gib"] == 19


def test_two_verdicts_for_one_tier_are_each_reported_once(tmp_path: Path) -> None:
    """Keyed without the reason, the memory kept the last of two verdicts and
    reported the other again every cycle."""
    queue, stage = _tier(tmp_path)
    _orphans(queue, stage, 7)
    big = _claimant(queue, stage, "claim-above-the-tier", 25)
    stuck = _claimant(queue, stage, "claim-beyond-evictable", 19)
    for _ in range(4):
        _cycle(queue, stage, gib=CAPACITY)
    for key, reason in ((big, "gate-refused"), (stuck, "shortfall-exceeds-evictable")):
        events = _events(queue, key)
        assert [event["reason"] for event in events] == [reason], events


def test_a_changed_reason_is_reported_and_an_unchanged_one_is_not() -> None:
    """The memory itself, on synthetic rows: two keys, one tier, then a change."""
    class _Queue:
        root = Path("/synthetic/queue")

    def rows(reason_b: str):
        return [
            {"scope": "claim", "consumer": "a" * 64, "tier_id": TIER,
             "reason": "gate-refused"},
            {"scope": "claim", "consumer": "b" * 64, "tier_id": TIER,
             "reason": reason_b}]

    queue = _Queue()
    first = tier_loop._pressure_skip_events(queue, rows("shortfall-exceeds-evictable"))
    assert len(first) == 2
    for _ in range(3):
        assert tier_loop._pressure_skip_events(
            queue, rows("shortfall-exceeds-evictable")) == []
    changed = tier_loop._pressure_skip_events(queue, rows("no-shortfall"))
    assert [event["reason"] for event in changed] == ["no-shortfall"]
    assert tier_loop._pressure_skip_events(queue, []) == []
    assert len(tier_loop._pressure_skip_events(
        queue, rows("no-shortfall"))) == 2, "a waiter that returns is reported again"
