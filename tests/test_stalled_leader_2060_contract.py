"""W1 (#2060): evidence boundary and CPU-only priority-barrier probe.

The controlled 20/5/18/3 stage-token example is NOT recovered fleet state.
It intentionally describes a feasible protected wait, not a demonstrated safe
backfill opportunity. GPU/CPU/RAM declarations are metadata, never allocations.
The missing portable evidence deliverable is the W1 acceptance failure; these
tests do not authorize a production change or assert that #1772 is repaired.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from prismabuild.tier_holdings import Unspent
import test_window_multiconsumer_liveness as base
from test_a_consumer_stages_only_to_its_refill_horizon import (
    _cycle as run_tier_cycle,
    _fixture_queue as make_queue,
)

LEADER = "2bce5567a050816e9af0564c4b6f775564d668fefad4365f303aaa348ea6bd28"
CANARY = "cafd40ce45d818b7d894e8a6bd6cee0eff2a1ced6f496c257855cd7f05378507"
STATIC = "e" * 64
EVIDENCE = Path(__file__).parent / "fixtures" / "stalled_leader_2060.json"


def publish_probe_consumer(queue, stage, key, *, stage_gib, priority, resources):
    plan = base._plan(queue, key, phases=1, gib_per_phase=stage_gib, seed="w1probe")
    plan["stage_root"] = str(stage)
    mover = plan["phases"][0]["mover_row"]
    mover["priority"] = priority
    mover["resources"]["cpu"] = 1
    base.residency_plan.freeze(queue, plan)
    queue.publish(
        action_key=key, cas_root=queue.root / "cas", checkout_root=queue.root / "co",
        worker_script=queue.root / "worker.py", priority=priority,
        tags=["sparky"], resources=resources,
        residency={"schema": base.pool.RESIDENCY_SCHEMA_V1, "tier_id": base.TIER,
                   "manifest_sha256": base.MANIFEST, "manifest_bytes": 1 << 30,
                   "leads": base.residency_plan.leads_for(plan)},
    )
    return mover["action_key"]


def probe_queue(tmp_path, *, include_leader=True):
    queue, stage = make_queue(tmp_path, 20)
    assert queue.tier(base.TIER).take(STATIC, {"stage_gib": 5})
    leader = None
    if include_leader:
        leader = publish_probe_consumer(
            queue, stage, LEADER, stage_gib=18, priority=0,
            resources={"gpu": 1, "cpu": 4, "mem_gb": 56})
    canary = publish_probe_consumer(
        queue, stage, CANARY, stage_gib=3, priority=-10,
        resources={"cpu": 1, "mem_gb": 4})
    return queue, stage, leader, canary


def ready(queue, key):
    return queue.records.item_path(base.pool.record_store.READY, key).exists()


def test_named_barrier_precedes_canarys_own_viable_gate(tmp_path, monkeypatch):
    queue, stage, leader, canary = probe_queue(tmp_path)
    calls = []
    original = base.tier_loop.window_credit.gate_newcomer

    def observe_gate(**inputs):
        calls.append(dict(inputs))
        return original(**inputs)

    # Observation only: the real gate and the complete tier cycle still run.
    monkeypatch.setattr(base.tier_loop.window_credit, "gate_newcomer", observe_gate)
    run_tier_cycle(queue, stage, gib=20)
    assert ready(queue, LEADER) and ready(queue, CANARY)
    assert not ready(queue, leader) and not ready(queue, canary)
    assert queue.tier(base.TIER).holdings(STATIC) == {"stage_gib": 5}
    events = [json.loads(line) for line in (
        queue.root / "residency-events" / CANARY / "dl380g10.jsonl"
    ).read_text().splitlines()]
    blocked = [e for e in events if e.get("event") == "window-gated"]
    assert any(e["reason"] == "higher-priority-window-waiting"
               and e["waiting_on"]["consumer"] == LEADER
               and e["waiting_on"]["reason"] == "joint-fit-stall"
               for e in blocked), blocked
    assert any(c["cur_min_gib"] == 18 for c in calls), calls
    assert not any(c["cur_min_gib"] == 3 for c in calls), calls
    # Independent arithmetic is a counterfactual, not an admission bypass.
    assert original(held_gib=5, ready_gib=0, output_gib=0,
                    capacity_gib=20, cur_min_gib=3, next_min_gib=None,
                    existing_min_next_gib=0)["admit"]


def test_without_leader_same_canary_and_obligations_publish(tmp_path):
    queue, stage, _, canary = probe_queue(tmp_path, include_leader=False)
    run_tier_cycle(queue, stage, gib=20)
    assert ready(queue, canary)
    assert ready(queue, CANARY)  # no worker, mover or foreign replay is executed
    assert queue.tier(base.TIER).holdings(STATIC) == {"stage_gib": 5}


def test_protected_feasible_wait_progresses_when_existing_holding_drains(tmp_path):
    queue, stage, leader, canary = probe_queue(tmp_path)
    run_tier_cycle(queue, stage, gib=20)
    assert not ready(queue, leader) and not ready(queue, canary)
    # This test holder never started and wrote no payload; release only its
    # own unspent grant, not an arbitrary receipt-less fleet holding.
    queue.tier(base.TIER).release_unspent(STATIC, authority=Unspent.NEVER_STARTED)
    run_tier_cycle(queue, stage, gib=20)
    assert ready(queue, leader)
    assert not ready(queue, canary)  # 18 + 3 > 20; protect the new promise too
    assert ready(queue, LEADER) and ready(queue, CANARY)


@pytest.mark.parametrize("unknown", [None])
def test_unknown_capacity_is_not_backfill_authority(unknown):
    verdict = base.tier_loop.window_credit.gate_newcomer(
        held_gib=0, ready_gib=0, output_gib=0, capacity_gib=unknown,
        cur_min_gib=3, next_min_gib=None, existing_min_next_gib=0)
    assert verdict["admit"] is False
    assert verdict["reason"] == "advance-deferred-unknown-evidence"


def test_portable_historical_evidence_and_contract_are_delivered():
    """Missing W1 evidence is a deliverable failure, not a production regression.

    Fixture format: declarations, historical (selected event + provenance +
    missing-input inventory), synthetic_probe, and contract. Narrative judgments
    and proposed W2 predicate still require review; JSON alone cannot prove them.
    """
    assert EVIDENCE.is_file(), (
        "#2060 needs a committed portable evidence/contract fixture at "
        "tests/fixtures/stalled_leader_2060.json; do not fill historical gaps "
        "from capacity138/held40/growth21 or modify production to pass W1"
    )
    evidence = json.loads(EVIDENCE.read_text())
    assert evidence["declarations"] == {
        LEADER: {"priority": 0, "resources": {"gpu": 1, "cpu": 4, "mem_gb": 56}},
        CANARY: {"priority": -10, "resources": {"cpu": 1, "mem_gb": 4}},
    }
    historical = evidence["historical"]
    event = historical["barrier_event"]
    assert event["consumer"] == CANARY
    assert event["reason"] == "higher-priority-window-waiting"
    assert event["waiting_on"] == {"consumer": LEADER, "reason": "joint-fit-stall"}
    assert event["unix"] == 1791748669.3183753
    assert event["need_gib"] == 1  # historical stage demand is NOT CPU memory
    provenance = historical["barrier_source"]
    assert provenance["queue_relative_path"] == f"residency-events/{CANARY}/dl380g10.jsonl"
    assert provenance["line"] == 137
    assert provenance["sha256"] == "744fa6646ac07f8a641cf67adeb299234b9c64ac59edf11749bd5f2bd4c25542"
    assert historical["full_admission_snapshot_recovered"] is False
    assert historical["audit_status"] == "previously_permission_denied_not_independently_verified"
    for term in ("held", "queued", "output", "protected_next", "prelaunch",
                 "sharing", "commitment", "leader_plan"):
        assert isinstance(historical["unresolved"][term], str)
        assert historical["unresolved"][term].strip(), term
    assert evidence["synthetic_probe"] == {
        "historical_replay": False, "capacity_gib": 20, "static_held_gib": 5,
        "leader_stage_gib": 18, "canary_stage_gib": 3,
    }
    contract = evidence["contract"]
    assert contract["historical_safe_backfill_proved"] is False
    assert contract["preserve_feasible_wait"] is True
    assert contract["unknown_state"] == "refuse"
    assert contract["preserve_future_higher_priority_rights"] is True
    assert contract["production_change_in_w1"] is False
    assert contract["repair_owner"] == "#2061"
    assert contract["tracking_parent"] == "#1772"
    assert contract["priority"] == "P1"
