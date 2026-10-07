"""The prelaunch gate wiring in the tier loop (#1594).

Neighbour style: the queue, tier and plan helpers follow
test_prelaunch_tier_module_1594.py, which takes its base from
test_prelaunch_group_reconcile_1594.py. Every number asserted comes
from a plan the test builds. The integrator runs them; nothing here
runs anything itself.
"""
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from prismabuild import pool, residency_plan, storage_tiers  # noqa: E402
from prismabuild import window_credit  # noqa: E402
import prelaunch_tier as pt  # noqa: E402
import tier_loop  # noqa: E402
from test_prelaunch_group_reconcile_1594 import (  # noqa: E402
    GIB, MANIFEST, TIER, _hexkey, _queue)
from test_prelaunch_tier_module_1594 import (  # noqa: E402
    _consumer, _declared_plan, _mover_of, _publish_leg)

KIND = storage_tiers.STAGE_CAPACITY_KIND


def _tiers(stage):
    """One announced stage tier record."""
    return {TIER: {"tier_id": TIER, "tier": "stage",
                   "mountpoint": str(stage)}}


def _live(queue, plan, consumer):
    """Freeze one plan and publish its consumer with its leads."""
    residency_plan.freeze(queue, plan)
    span = sum(int(phase["end_bytes"]) - int(phase["start_bytes"])
               for phase in plan["phases"])
    queue.publish(
        action_key=consumer, cas_root=str(queue.root / "cas"),
        checkout_root=str(queue.root / "co"),
        worker_script=str(queue.root / "worker.py"),
        resources={"cpu": 1, "mem_gb": 1},
        residency={"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                   "manifest_sha256": MANIFEST, "manifest_bytes": span,
                   "leads": residency_plan.leads_for(plan)})


def _protect(queue, tiers):
    """The stage advance protection pass over one queue."""
    return tier_loop._protect_tier_advances(
        queue, tiers, mover_role="mover_row",
        tier_of=lambda plan: plan.get("tier_id"),
        state_of=tier_loop._mover_state, horizon_of=None,
        claim_order=None)


def _held(queue):
    """Ledger holdings by holder, the shape obligations reads."""
    ledger = queue.tier_ledger(TIER)
    return {str(holder): dict(ledger.holder_tokens(holder))
            for holder in ledger.held_keys()}


def _reserve(queue, units):
    """Reserve every unit's group over three passes."""
    for _ in range(3):
        pt.reserve_pass(queue, TIER, units, admitted=lambda unit: True)


def _units(queue, *keys):
    """The declared units for live consumer keys on one tier."""
    return pt.declared_units(queue, {TIER: {"tier": "stage"}},
                             [_consumer(key) for key in keys])


# ------------------------------------------- two declared peaks, one tier


def _two_consumer_queue(tmp_path):
    """A 210-token tier with two T=90 suffix-1-1-40-40 plans."""
    queue = _queue(tmp_path, stage_gib=210)
    stage = tmp_path / "stage"
    stage.mkdir()
    specs = [("p0", 90, True, 1), ("p1", 1, False, 1),
             ("p2", 1, False, 1), ("p3", 40, False, 1),
             ("p4", 40, False, 1)]
    first = _hexkey("gate-first")
    second = _hexkey("gate-second")
    plan_first = _declared_plan(queue, first, specs, tag="first")
    plan_second = _declared_plan(queue, second, specs, tag="second")
    _live(queue, plan_first, first)
    _live(queue, plan_second, second)
    return queue, _tiers(stage), first, second, plan_first, plan_second


def test_fresh_declared_newcomers_admit_one_then_stall(tmp_path) -> None:
    """Two fresh peaks fit singly on 210, never jointly."""
    queue, tiers, first, second, plan_first, _ = _two_consumer_queue(tmp_path)
    bound = residency_plan.prelaunch_bound(plan_first)
    assert bound is not None
    assert (bound["retained_gib"], bound["peak_gib"]) == (90, 170)
    result = _protect(queue, tiers)
    gated = result["gated"]
    assert len(gated) == 1
    (key, _), entry = next(iter(gated.items()))
    assert key in (first, second)
    assert entry["reason"] == "joint-fit-stall"
    assert entry["permanent"] is False
    assert entry["need_gib"] == 170
    other = second if key == first else first
    assert (other, TIER) in result["permitted"]


def test_second_declared_newcomer_waits_on_commitment(tmp_path) -> None:
    """A held prefix beside a queued lead leaves no room for a peak."""
    queue, tiers, first, second, plan_first, _ = _two_consumer_queue(
        tmp_path)
    units = _units(queue, first, second)
    by_key = {unit.key: unit for unit in units}
    _reserve(queue, [by_key[first]])
    ledger = queue.tier_ledger(TIER)
    assert ledger.holder_tokens(by_key[first].holder).get(
        "stage_gib", 0) == 90
    totals, detail = pt.obligations(units, _held(queue), KIND)
    assert totals == {TIER: 80}
    assert detail[first]["obligation_gib"] == 80
    assert second not in detail
    assert 90 + 80 + 170 > 210
    _publish_leg(queue, plan_first, _mover_of(plan_first, "p0"))
    result = _protect(queue, tiers)
    assert (first, TIER) not in result["gated"]
    entry = result["gated"][(second, TIER)]
    assert entry["reason"] == "joint-commitment-stall"
    assert entry["permanent"] is False
    assert entry["need_gib"] == 170


# ------------------------------------------------- peak past capacity


def test_declared_peak_past_capacity_refuses_for_good(tmp_path) -> None:
    """A peak no retirement can fit refuses permanently."""
    queue = _queue(tmp_path, stage_gib=210)
    stage = tmp_path / "stage"
    stage.mkdir()
    consumer = _hexkey("gate-oversize")
    plan = _declared_plan(queue, consumer, [("p0", 150, True, 1),
                                            ("p1", 40, False, 1),
                                            ("p2", 40, False, 1)],
                          tag="oversize")
    _live(queue, plan, consumer)
    bound = residency_plan.prelaunch_bound(plan)
    assert bound is not None
    assert (bound["retained_gib"], bound["peak_gib"]) == (150, 230)
    units = _units(queue, consumer)
    assert units[0].peak_gib == 230
    suffix = _mover_of(plan, "p2")
    assert queue.tier_ledger(TIER).acquire(
        suffix, {"stage_gib": 25}) is True
    held = _held(queue)
    owned = residency_plan.prelaunch_owned_gib(
        plan, held, units[0].holder, KIND)
    assert owned == 25
    assert window_credit.prelaunch_footprint_gib(230, owned) == 205
    result = _protect(queue, {TIER: {"tier_id": TIER, "tier": "stage",
                                     "mountpoint": str(stage)}})
    entry = result["gated"][(consumer, TIER)]
    assert entry["reason"] == "joint-fit-oversize"
    assert entry["permanent"] is True
    assert entry["need_gib"] == 205
    totals, _ = pt.obligations(units, held, KIND)
    assert totals == {}
    gib, enforced, _, error = tier_loop.output_obligation(queue, TIER)
    assert not error
    direct = window_credit.gate_newcomer(
        held_gib=25, ready_gib=0, output_gib=gib,
        output_enforced=enforced, capacity_gib=210,
        cur_min_gib=205, next_min_gib=None, existing_min_next_gib=0)
    assert direct["reason"] == "joint-fit-stall"
    assert direct["permanent"] is False


# ---------------------------------------- undeclared beside an obligation


def test_undeclared_newcomer_waits_then_proceeds(tmp_path) -> None:
    """A declared obligation holds a plain lead; its end frees it."""
    queue = _queue(tmp_path, stage_gib=210)
    stage = tmp_path / "stage"
    stage.mkdir()
    tiers = _tiers(stage)
    declared = _hexkey("gate-declared")
    plain = _hexkey("gate-plain")
    plan_declared = _declared_plan(queue, declared, [("p0", 90, True, 1),
                                                     ("p1", 40, False, 1),
                                                     ("p2", 40, False, 1)],
                                   tag="waiter")
    plan_plain = _declared_plan(queue, plain, [("u0", 40, False, 1),
                                               ("u1", 40, False, 1)],
                                tag="plain")
    _live(queue, plan_declared, declared)
    _live(queue, plan_plain, plain)
    units = _units(queue, declared)
    _reserve(queue, units)
    first = _protect(queue, tiers)
    entry = first["gated"][(plain, TIER)]
    assert entry["reason"] == "joint-fit-stall"
    assert entry["permanent"] is False
    assert (declared, TIER) not in first["gated"]
    # The declared consumer ends.  Its leads are not resident, so a claim is
    # refused by design; a withdrawal is how a waiting window ends.
    queue.withdraw(declared, reason="test: the declared consumer ended")
    released = pt.release_terminal(queue, TIER, units, shared_owned=[])
    assert any(event["event"] == "prelaunch-group-released"
               for event in released)
    grant = first["grants"].get((declared, TIER))
    if grant is not None:
        assert queue.cancel_tier_fence(TIER, grant)["released"] == 40
    again = _protect(queue, tiers)
    assert (plain, TIER) not in again["gated"]
    assert (plain, TIER) in again["permitted"]
    assert queue.tier_ledger(TIER).available().get("stage_gib") == 170
    live = {consumer["action_key"]
            for consumer in tier_loop.live_consumers(queue)}
    assert declared not in live


# ----------------------------------------------- no declaration, no term


def test_undeclared_queue_gates_like_a_direct_call(tmp_path) -> None:
    """With no declared unit the term is zero and numbers match."""
    queue = _queue(tmp_path, stage_gib=80)
    stage = tmp_path / "stage"
    stage.mkdir()
    assert queue.tier_ledger(TIER).acquire(
        "squatter-zero", {"stage_gib": 10}) is True
    consumer = _hexkey("gate-direct")
    plan = _declared_plan(queue, consumer, [("v0", 40, False, 1),
                                            ("v1", 40, False, 1)],
                          tag="direct")
    _live(queue, plan, consumer)
    assert pt.declared_units(queue, {TIER: {"tier": "stage"}},
                             [_consumer(consumer)]) == []
    assert pt.obligations([], _held(queue), KIND) == ({}, {})
    needs = residency_plan.advance_needs(plan, None, published=[], rowed=[],
                                         staged=[])
    assert (needs["current_min_gib"], needs["next_min_gib"]) == (40, 40)
    result = _protect(queue, _tiers(stage))
    entry = result["gated"][(consumer, TIER)]
    gib, enforced, _, error = tier_loop.output_obligation(queue, TIER)
    assert not error
    direct = window_credit.gate_newcomer(
        held_gib=10, ready_gib=0, output_gib=gib,
        output_enforced=enforced, capacity_gib=80,
        cur_min_gib=40, next_min_gib=40, existing_min_next_gib=0)
    assert entry["reason"] == direct["reason"]
    assert entry["permanent"] == direct["permanent"]
    assert all(not str(event.get("event", "")).startswith("prelaunch")
               for event in result["events"])
    assert all(permit.get("advance") != "prelaunch"
               for permit in result["permitted"].values())


# ------------------------------------------------- holdings admit


def test_declared_holder_is_no_newcomer(tmp_path) -> None:
    """A group holder passes the gate its suffix could never pass fresh."""
    queue = _queue(tmp_path, stage_gib=120)
    stage = tmp_path / "stage"
    stage.mkdir()
    assert queue.tier_ledger(TIER).acquire(
        "squatter-held", {"stage_gib": 25}) is True
    consumer = _hexkey("gate-held")
    plan = _declared_plan(queue, consumer, [("p0", 90, True, 1),
                                            ("p1", 5, False, 1),
                                            ("p2", 5, False, 1)],
                          tag="held")
    _live(queue, plan, consumer)
    bound = residency_plan.prelaunch_bound(plan)
    assert bound is not None
    assert (bound["retained_gib"], bound["peak_gib"]) == (90, 100)
    fresh = _protect(queue, _tiers(stage))
    assert fresh["gated"][(consumer, TIER)]["reason"] == "joint-fit-stall"
    assert fresh["gated"][(consumer, TIER)]["need_gib"] == 100
    units = _units(queue, consumer)
    _reserve(queue, units)
    held = _held(queue)
    already, _ = tier_loop._mover_state(queue, plan, TIER)
    assert pt.is_admitted(units[0], held, KIND, already) is True
    totals, detail = pt.obligations(units, held, KIND)
    assert totals == {TIER: 10}
    assert detail[consumer]["obligation_gib"] == 10
    result = _protect(queue, _tiers(stage))
    assert (consumer, TIER) not in result["gated"]
    assert (consumer, TIER) in result["permitted"]
    assert result["permitted"][(consumer, TIER)]["advance"] != "prelaunch"


# ------------------------------------------------- retention


def _retention_queue(tmp_path):
    """A 12-token tier with a landed declared prefix and suffix."""
    queue = _queue(tmp_path, stage_gib=12)
    stage = tmp_path / "stage"
    stage.mkdir()
    tiers = _tiers(stage)
    head = _hexkey("retain-head")
    reader = _hexkey("retain-reader")
    plan_head = _declared_plan(queue, head, [("h0", 4, False, 1),
                                             ("h1", 4, False, 1)],
                               tag="head")
    plan_reader = _declared_plan(queue, reader, [("p0", 4, True, 1),
                                                 ("p1", 4, False, 1)],
                                 tag="reader")
    residency_plan.freeze(queue, plan_reader)
    prefix = _mover_of(plan_reader, "p0")
    suffix = _mover_of(plan_reader, "p1")
    ledger = queue.tier_ledger(TIER)
    assert ledger.acquire(prefix, {"stage_gib": 4}) is True
    assert ledger.acquire(suffix, {"stage_gib": 4}) is True
    for mover in (prefix, suffix):
        queue.record_move(mover, {"consumer_action_key": reader,
                                  "tier_id": TIER,
                                  "stage_root": "/stage/prewarm",
                                  "complete": True,
                                  "bytes_staged": 4 * GIB, "seconds": 10})
    keys = pt.declared_leg_keys(_units(queue, reader))
    assert keys == frozenset({prefix})
    consumers = [(head, {"action_key": head, "accepted_phase": None},
                  plan_head, TIER),
                 (reader, {"action_key": reader, "accepted_phase": None},
                  plan_reader, TIER)]
    order = {"head": head,
             "entries": [{"consumer": head, "rank": 0, "blocked": True,
                          "need_start_bytes": 0,
                          "need_end_bytes": 4 * GIB,
                          "landing_bytes_per_s": 1e8},
                         {"consumer": reader, "rank": 1, "blocked": True,
                          "need_start_bytes": 0}],
             "target_free_gib": 4, "landing_bytes_per_s": 1e8}
    return queue, tiers, consumers, order, keys, prefix, suffix


def test_claim_order_pass_spares_a_declared_prefix(tmp_path) -> None:
    """The claim-order walk keeps a live prefix, takes the suffix."""
    queue, tiers, consumers, order, keys, prefix, suffix = _retention_queue(
        tmp_path)
    plain = tier_loop._claim_order_candidates(
        queue, tier_id=TIER, tier_record=tiers[TIER], consumers=consumers,
        order=order, taken=set())
    assert {row["mover_action_key"] for row in plain} == {prefix, suffix}
    kept = tier_loop._claim_order_candidates(
        queue, tier_id=TIER, tier_record=tiers[TIER], consumers=consumers,
        order=order, taken=set(), declared_keys=keys)
    assert {row["mover_action_key"] for row in kept} == {suffix}
    defaulted = tier_loop._claim_order_candidates(
        queue, tier_id=TIER, tier_record=tiers[TIER], consumers=consumers,
        order=order, taken=set())
    assert [row["mover_action_key"] for row in defaulted] == [
        row["mover_action_key"] for row in plain]


def test_horizon_pass_keeps_a_declared_prefix(tmp_path) -> None:
    """The horizon pass names no declared mover and evicts nothing."""
    queue, tiers, consumers, _, keys, prefix, _ = _retention_queue(tmp_path)
    out = tier_loop._beyond_horizon_candidates(
        queue, tiers, consumers, frozenset())
    again = tier_loop._beyond_horizon_candidates(
        queue, tiers, consumers, frozenset(), declared_keys=keys)
    assert out == again
    assert all(not pt.is_prelaunch_leg(keys, str(row["mover_action_key"]))
               for rows in out.values() for row in rows)
    assert tier_loop.evict_beyond_horizon(
        queue, tiers, consumers=consumers, pressure={TIER: 5}) == []
    assert queue.tier_ledger(TIER).holder_tokens(prefix).get(
        "stage_gib", 0) == 4


# ------------------------------------------------- dangling census


def _orphan(queue, tag):
    """A committed group whose consumer never goes live."""
    consumer = _hexkey(f"{tag}-orphan")
    plan = _declared_plan(queue, consumer, [("p0", 2, True, 1)], tag=tag)
    residency_plan.freeze(queue, plan)
    units = _units(queue, consumer)
    _reserve(queue, units)
    holder = units[0].holder
    assert queue.tier_ledger(TIER).holder_tokens(holder).get(
        "stage_gib", 0) == 2
    return holder


def _plain_live(queue, tag):
    """Publish one small undeclared consumer to run the tier pass."""
    consumer = _hexkey(f"{tag}-live")
    plan = _declared_plan(queue, consumer, [("l0", 1, False, 1)], tag=tag)
    _live(queue, plan, consumer)
    return consumer


def test_complete_census_releases_an_orphan_holder(tmp_path) -> None:
    """A complete pass frees a holder no live unit owns."""
    queue = _queue(tmp_path, stage_gib=8)
    stage = tmp_path / "stage"
    stage.mkdir()
    holder = _orphan(queue, "gone")
    _plain_live(queue, "gone")
    result = _protect(queue, _tiers(stage))
    assert [event["event"] for event in result["events"]
            if event.get("holder") == holder] == ["prelaunch-dangling-released"]
    assert queue.tier_ledger(TIER).holder_tokens(holder).get(
        "stage_gib", 0) == 0
    assert queue.tier_ledger(TIER).available().get("stage_gib") == 8


def test_incomplete_census_withholds_the_release(tmp_path) -> None:
    """An unreadable consumer keeps every orphan where it is."""
    queue = _queue(tmp_path, stage_gib=8)
    stage = tmp_path / "stage"
    stage.mkdir()
    holder = _orphan(queue, "half")
    _plain_live(queue, "half")
    stranger = _hexkey("half-stranger")
    Path(queue.residency_plan_path(stranger)).write_text("{not json")
    queue.publish(
        action_key=stranger, cas_root=str(queue.root / "cas"),
        checkout_root=str(queue.root / "co"),
        worker_script=str(queue.root / "worker.py"),
        resources={"cpu": 1, "mem_gb": 1},
        residency={"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                   "manifest_sha256": MANIFEST, "manifest_bytes": 1,
                   "leads": ["0" * 64]})
    result = _protect(queue, _tiers(stage))
    assert queue.tier_ledger(TIER).holder_tokens(holder).get(
        "stage_gib", 0) == 2
    assert all(event.get("event") != "prelaunch-dangling-released"
               for event in result["events"])
    assert any(event.get("event") == "advance-deferred-unknown-evidence"
               for event in result["events"])


def test_live_holders_survive_the_dangling_pass(tmp_path) -> None:
    """The orphan goes; a live unit's holder stays untouched."""
    queue = _queue(tmp_path, stage_gib=8)
    stage = tmp_path / "stage"
    stage.mkdir()
    holder = _orphan(queue, "kept")
    consumer = _hexkey("kept-consumer")
    plan = _declared_plan(queue, consumer, [("c0", 2, True, 1)], tag="kept")
    _live(queue, plan, consumer)
    units = _units(queue, consumer)
    _reserve(queue, units)
    live_holder = units[0].holder
    result = _protect(queue, _tiers(stage))
    assert [event.get("holder") for event in result["events"]
            if event.get("event") == "prelaunch-dangling-released"] == [holder]
    assert queue.tier_ledger(TIER).holder_tokens(live_holder).get(
        "stage_gib", 0) == 2
