"""#1772/#2061: a bounded footprint is not a proof of plan feasibility.

These are synthetic, CPU-only scheduler tests, NOT a replay of the incomplete
October 11 admission snapshot. W1's 20/5/18/3 example remains a protected wait.
Here an unstarted leader has a later impossible movement minimum; draining the
incumbent cannot make that frozen plan executable on this tier. No fleet work,
priorities, observer budget, or sealed execution units are changed.
"""
from __future__ import annotations

from unittest.mock import patch

import pytest

import test_window_multiconsumer_liveness as base
from test_a_consumer_stages_only_to_its_refill_horizon import (
    _cycle, _fixture_queue, _plan,
)
from test_a_resident_range_is_adopted_rather_than_recopied import (
    _claim_with_progress, _stage_range,
)

INCUMBENT = "a" * 64
LEADER = "b" * 64
CANARY = "c" * 64


def _publish_window(queue, stage, key, sizes, *, priority, manifest):
    plan = _plan(queue, key, label=f"future-{key[:4]}", manifest=manifest,
                 sizes=list(sizes))
    plan["stage_root"] = str(stage)
    for phase in plan["phases"]:
        phase["mover_row"]["priority"] = priority
    base.residency_plan.freeze(queue, plan)
    queue.publish(
        action_key=key, cas_root=queue.root / "cas", checkout_root=queue.root / "co",
        worker_script=queue.root / "worker.py", priority=priority,
        resources={"cpu": 1, "mem_gb": 1},
        residency={"schema": base.pool.RESIDENCY_SCHEMA_V1, "tier_id": base.TIER,
                   "manifest_sha256": manifest, "manifest_bytes": plan["manifest_bytes"],
                   "leads": base.residency_plan.leads_for(plan)},
    )
    return plan


def _setup(tmp_path, sizes, *, include_leader=True):
    queue, stage = _fixture_queue(tmp_path, 5)
    incumbent = _publish_window(
        queue, stage, INCUMBENT, [2], priority=0, manifest=base.MANIFEST)
    pinned = incumbent["phases"][0]["mover_row"]["action_key"]
    _stage_range(queue, mover=pinned, consumer=INCUMBENT, stage=stage)
    _claim_with_progress(queue, INCUMBENT, phase="phase-0")
    leader = None
    if include_leader:
        leader = _publish_window(
            queue, stage, LEADER, sizes, priority=0, manifest="7" * 64)
    canary = _publish_window(
        queue, stage, CANARY, [1], priority=-10, manifest="8" * 64)
    return queue, stage, pinned, leader, canary


def _ready(queue, key):
    return queue.records.item_path(base.pool.record_store.READY, key).exists()


def _mover(plan, index=0):
    return plan["phases"][index]["mover_row"]["action_key"]


def _events(queue, stage, capacity=5):
    events = []
    original = base.tier_loop._emit

    def capture_cycle_event(*args, **kwargs):
        events.append(dict(args[2]))
        return original(*args, **kwargs)

    with patch.object(base.tier_loop, "_emit", side_effect=capture_cycle_event):
        _cycle(queue, stage, gib=capacity)
    return events


def _assert_canary_advances(queue, stage, pinned, leader, canary):
    digest = base.residency_plan.plan_sha256(leader)
    for _ in range(2):
        events = _events(queue, stage)
        assert _ready(queue, _mover(canary)), (
            "A provably infeasible remaining leader plan must not establish "
            "the newcomer priority barrier", events)
        assert not _ready(queue, _mover(leader)), (
            "Do not admit an infeasible leader into a later dead end", events)
        assert _ready(queue, LEADER) and _ready(queue, CANARY)
        assert queue.tier(base.TIER).holdings(pinned) == {"stage_gib": 2}
        assert base.residency_plan.plan_sha256(
            base.residency_plan.read(queue, LEADER)) == digest
    return digest


@pytest.mark.parametrize("sizes", [(1, 1, 4, 6), (1, 1, 4, 4)],
                         ids=["future-indivisible-oversize", "future-overlap-oversize"])
def test_infeasible_remaining_plan_does_not_block_viable_window(tmp_path, sizes):
    # The leader's initial minimum is 1+1, which fits alongside the incumbent.
    # Its future minimum does not fit even on an otherwise empty 5-GiB tier.
    queue, stage, pinned, leader, canary = _setup(tmp_path / "leader", sizes)
    # A no-leader control proves that the same full ordinary gate permits it.
    control, root, held, _, small = _setup(
        tmp_path / "control", sizes, include_leader=False)
    _events(control, root)
    assert _ready(control, _mover(small))
    assert control.tier(base.TIER).holdings(held) == {"stage_gib": 2}
    _assert_canary_advances(queue, stage, pinned, leader, canary)

    # Use the normal claim path: publication alone is not a resource grant.
    row = base.pool.record_store.value_or_raise(
        queue.records.item(base.pool.record_store.READY, _mover(canary)))
    claimed = queue.claim(ready=[row], tags=["dl380g10"], owner="future-probe",
                          capacity={"cpu": 1, "mem_gb": 1})
    assert claimed and claimed["action_key"] == _mover(canary)
    ledger = queue.tier(base.TIER)
    assert ledger.holdings(_mover(canary)) == {"stage_gib": 1}
    assert ledger.holdings(pinned) == {"stage_gib": 2}
    assert ledger.free_supply()["stage_gib"] == 2


@pytest.mark.parametrize("transition", ["capacity", "withdrawal"])
def test_backfill_reconsiders_leader_without_sticky_or_identity_changes(tmp_path, transition):
    queue, stage, pinned, leader, canary = _setup(tmp_path, (1, 1, 4, 6))
    digest = _assert_canary_advances(queue, stage, pinned, leader, canary)
    small_row = base.pool.record_store.value_or_raise(
        queue.records.item(base.pool.record_store.READY, _mover(canary)))
    if transition == "withdrawal":
        queue.withdraw(LEADER, reason="synthetic owner withdrawal", by="future-probe")
        _events(queue, stage)
        assert not _ready(queue, _mover(leader))
        assert _ready(queue, _mover(canary))
        assert queue.tier(base.TIER).holdings(pinned) == {"stage_gib": 2}
        return

    # Authoritative supply changes, not priority or the frozen plan. At 13 GiB
    # even the 4+6 future overlap fits beside incumbent2 and canary1.
    queue.tier(base.TIER).mint({"stage_gib": 13})
    _events(queue, stage, capacity=13)
    assert _ready(queue, _mover(leader))
    assert _ready(queue, _mover(leader, 1))
    assert base.residency_plan.plan_sha256(
        base.residency_plan.read(queue, LEADER)) == digest
    record = queue.funding.read_window(_mover(leader, 1), base.TIER)
    assert record is not None
    assert record["consumer_action_key"] == LEADER
    assert record["plan_sha256"] == digest
    assert record["range_start_bytes"] == base.GIB
    assert record["range_end_bytes"] == 2 * base.GIB

    # A live first mover must remain accounted, without resurrecting a READY
    # newcomer barrier or stealing the earlier lower-priority publication.
    assert queue.tier(base.TIER).take(_mover(leader), {"stage_gib": 1})
    _events(queue, stage, capacity=13)
    assert queue.tier(base.TIER).holdings(_mover(leader)) == {"stage_gib": 1}
    assert queue.tier(base.TIER).holdings(pinned) == {"stage_gib": 2}
    assert base.pool.record_store.value_or_raise(
        queue.records.item(base.pool.record_store.READY, _mover(canary))) == small_row


def test_feasible_future_still_protects_priority_after_backfill_case(tmp_path):
    # Keep this paired with a red case: a generic "skip commitment stalls"
    # fix would pass the first half and fail the feasible draining control.
    queue, stage, pinned, leader, canary = _setup(tmp_path / "impossible", (1, 1, 4, 6))
    _assert_canary_advances(queue, stage, pinned, leader, canary)
    queue, stage, pinned, leader, canary = _setup(tmp_path / "feasible", (1, 1, 4))
    for _ in range(2):
        events = _events(queue, stage)
        assert not _ready(queue, _mover(leader))
        assert not _ready(queue, _mover(canary))
        assert any(e.get("consumer") == CANARY
                   and e.get("reason") == "higher-priority-window-waiting"
                   for e in events), events
        assert queue.tier(base.TIER).holdings(pinned) == {"stage_gib": 2}
