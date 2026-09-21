"""Membership retry continues the frozen window (R12).

Withdrawing a consumer retired its frozen plan filing, and any live
withdrawal marker on a plan mover did the same at the next window tick --
even when the withdrawal was a membership handoff whose successor revives
the exact decision. After phase 0 a real consumer needs its later movers;
with the plan retired the window sets ``publishable=[]`` forever and the
successor stalls behind bytes nobody will stage.

The supported lifecycle: a membership handoff (membership supervisor
owner shape, the exact identity the pool checks) preserves the sealed
plan and its attempt-bound retry authorization through settlement, while
an actual operator cancellation still retires it. Proven on a TWO-PHASE
real plan: membership drain/withdraw, a normal tier-loop window tick
while the withdrawal is still visible before requeue, settlement,
requeue, new-worker claim, and the later mover's completion after
settlement -- the corrected joint-fit policy (#745) queues the lead and
its protected run-ahead together, so the later mover is already
published when the handoff starts and must survive it. An operator
cancellation of the same shape still stops publication.
"""

from __future__ import annotations

import json
import socket
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

from prismabuild import (  # noqa: E402
    adaptive_cpu,
    pool,
    residency_map,
    residency_plan,
    storage_tiers,
)
import tier_loop  # noqa: E402

TIER = "prismabuild-stage:dl380g10"
STAGE_KIND = f"stage_gib@{TIER}"
MANIFEST = "cd" * 32
GIB = 1 << 30
SPAN = 2 * GIB


def _hexkey(seed: str) -> str:
    return (seed.encode().hex() * 64)[:64]


CONSUMER = _hexkey("r12consumer")
MOVER0 = _hexkey("r12mover0")
MOVER1 = _hexkey("r12mover1")
EGRESS0 = _hexkey("r12egress0")
EGRESS1 = _hexkey("r12egress1")


@pytest.fixture()
def queue(tmp_path: Path) -> pool.PoolQueue:
    q = pool.PoolQueue(tmp_path / "pb-queue")
    q.ensure_layout()
    return q


def _sealed_shape(monkeypatch) -> None:
    monkeypatch.setattr(adaptive_cpu, "action_identity",
                        lambda item: ("shape", False))


def _ready_for(queue: pool.PoolQueue, key: str) -> list[dict[str, object]]:
    """The ready snapshot naming one key, in the queue's own record shape.

    The corrected joint-fit policy queues the lead and its protected run-ahead
    together, so a bare ``claim`` may take either.  A test about one row asks
    for that row the way a worker's prefetched snapshot does.
    """

    return [item for item in queue.ready_items()
            if str(item.get("action_key")) == key]


def _row(key: str, resources: dict[str, int],
         queue: pool.PoolQueue) -> dict[str, object]:
    return {"action_key": key, "cas_root": str(queue.root / "cas"),
            "checkout_root": str(queue.root / "co"),
            "worker_script": str(queue.root / "worker.py"),
            "tags": ["dl380g10"], "resources": resources}


def _mover_row(key: str, start: int, end: int,
               queue: pool.PoolQueue) -> dict[str, object]:
    return {
        **_row(key, {STAGE_KIND: 2, "mem_gb": 1}, queue),
        "retry_safe": True, "max_attempts": 5,
        "residency": {"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                      "manifest_sha256": MANIFEST, "manifest_bytes": SPAN,
                      "range_start_bytes": start, "range_end_bytes": end}}


def _plan(queue: pool.PoolQueue) -> dict[str, object]:
    phases = []
    for ordinal, mover, egress in ((0, MOVER0, EGRESS0), (1, MOVER1, EGRESS1)):
        start, end = ordinal * SPAN, (ordinal + 1) * SPAN
        phases.append({
            "name": f"phase-{ordinal}", "start_bytes": start,
            "end_bytes": end, "stage_gib": 2,
            "mover_row": _mover_row(mover, start, end, queue),
            "egress_row": _row(egress, {"mem_gb": 1}, queue)})
    return residency_plan.build_plan(
        consumer_action_key=CONSUMER, tier_id=TIER,
        stage_root="/stage/prewarm", manifest_sha256=MANIFEST,
        manifest_bytes=2 * SPAN, phases=phases)


def _tiers(tmp_path: Path) -> dict[str, dict[str, object]]:
    return {TIER: {"tier_id": TIER, "tier": "stage",
                   "mountpoint": str(tmp_path / "stage")}}


def _land(queue: pool.PoolQueue, tmp_path: Path, mover: str,
          host: str) -> None:
    """Stage one mover's range through the real records: claim, complete
    receipt, fragment, executed finish (tokens kept as the pin)."""
    snap = queue.claim(tags=["dl380g10"], owner=f"{host}:1:stage",
                       capacity={"cpu": 4, "mem_gb": 16})
    assert snap is not None and snap["action_key"] == mover
    start = 0 if mover == MOVER0 else SPAN
    queue.record_move(mover, {
        "tier_id": TIER, "manifest_sha256": MANIFEST,
        "range_start_bytes": start, "range_end_bytes": start + SPAN,
        "bytes_staged": SPAN, "complete": True, "errors": []})
    residency_map.write_fragment(queue.residency_fragment_root(), {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": CONSUMER, "mover_action_key": mover,
        "tier_id": TIER, "stage_root": str(tmp_path / "stage"),
        "manifest_sha256": MANIFEST,
        "entries": {residency_map.residency_map_key("/pool/a.bin", 0): {
            "stage_path": str(tmp_path / "stage" / "a.bin"),
            "bytes": 4096, "offset": 0, "sha256": "a" * 64}}})
    queue.finish(mover, status="executed", detail={}, claim_snapshot=snap)
    assert queue.tier_ledger(TIER).holder_tokens(mover) == {"stage_gib": 2}


def test_consumer_handoff_keeps_later_phases_publishable(
    queue: pool.PoolQueue, tmp_path: Path, monkeypatch,
) -> None:
    """Withdrawing the running consumer must not retire its plan: the
    window keeps staging later phases across the handoff interval, and
    the later mover publishes and completes after settlement."""
    _sealed_shape(monkeypatch)
    host = socket.gethostname()
    owner = f"{host}:supervisor-9:8"
    queue.mint_tier_capacity(TIER, {"stage_gib": 4})
    plan = _plan(queue)
    residency_plan.freeze(queue, plan)
    queue.publish(action_key=CONSUMER, cas_root=queue.root / "cas",
                  checkout_root=queue.root / "co",
                  worker_script=queue.root / "worker.py",
                  resources={"cpu": 1, "mem_gb": 1},
                  max_attempts=5, retry_safe=True, tags=["x86"],
                  residency={"schema": pool.RESIDENCY_SCHEMA_V1,
                             "tier_id": TIER, "manifest_sha256": MANIFEST,
                             "manifest_bytes": 2 * SPAN,
                             "leads": residency_plan.leads_for(plan)})
    assert residency_plan.leads_for(plan) == [MOVER0]
    tick0 = tier_loop.residency_window(queue, tiers=_tiers(tmp_path))
    assert [e["action_key"] for e in tick0
            if e["event"] == "mover-published"] == [MOVER0, MOVER1], (
        "the corrected joint-fit policy queues the lead and its protected "
        "run-ahead together")

    _land(queue, tmp_path, MOVER0, host)
    assert tier_loop.compose_map(queue, CONSUMER) == queue.residency_map_path(
        CONSUMER)
    snap_a = queue.claim(tags=["x86"], owner=f"{host}:1:ca",
                         capacity={"cpu": 4, "mem_gb": 16})
    assert snap_a is not None and snap_a["action_key"] == CONSUMER

    # The ordinary resign sequence, with a real window tick inside the
    # interval while the withdrawal is still visible before requeue. The
    # withdraw carries the handoff plan so the queue can prove (not
    # assume) the handoff and persist its identity in the decision.
    requeue = queue.plan_requeue(dict(snap_a))
    withdrawn = queue.withdraw(CONSUMER, reason=f"resign {owner}: test",
                               by=owner,
                               membership_handoff=requeue["snapshot"])
    assert withdrawn["residency_plan_superseded"] is False
    interval = tier_loop.residency_window(queue, tiers=_tiers(tmp_path))
    assert residency_plan.superseded(queue, plan) is None
    assert not [e for e in interval
                if e["event"] == "residency-plan-superseded"]
    queue.finish(CONSUMER, status="failed",
                 detail={"termination_reason": "resign-test"},
                 claim_snapshot=snap_a)
    queue.publish(**requeue["arguments"],
                  preempted_claim=requeue["snapshot"], handoff_by=owner)

    # Fund and tick: the later mover the run-ahead queued is still there --
    # the handoff retires no plan -- and it runs to completion, handing the
    # consumer back.
    queue.mint_tier_capacity(TIER, {"stage_gib": 4})
    tick1 = tier_loop.residency_window(queue, tiers=_tiers(tmp_path))
    assert not [e for e in tick1 if e["event"] == "mover-published"], (
        "the window republished a mover that is already queued")
    ready1 = json.loads(queue.item_path(pool.READY, MOVER1).read_text())
    assert ready1["residency"]["range_start_bytes"] == SPAN
    assert ready1["recompute"] is True
    snap_m1 = queue.claim(tags=["dl380g10"], owner=f"{host}:1:m1",
                          capacity={"cpu": 4, "mem_gb": 16})
    assert snap_m1 is not None and snap_m1["action_key"] == MOVER1
    queue.record_move(MOVER1, {
        "tier_id": TIER, "manifest_sha256": MANIFEST,
        "range_start_bytes": SPAN, "range_end_bytes": 2 * SPAN,
        "bytes_staged": SPAN, "complete": True, "errors": []})
    queue.finish(MOVER1, status="executed", detail={},
                 claim_snapshot=snap_m1)
    assert json.loads(
        queue.item_path(pool.DONE, MOVER1).read_text())["status"] == "executed"
    snap_b = queue.claim(tags=["x86"], owner=f"{host}:1:cb",
                         capacity={"cpu": 4, "mem_gb": 16})
    assert snap_b is not None and int(snap_b["attempts"]) == 1


def test_mover_handoff_is_not_read_as_operator_cancellation(
    queue: pool.PoolQueue, tmp_path: Path, monkeypatch,
) -> None:
    """A membership-withdrawn mover visible at a window tick must not
    retire the plan; its requeued successor publishes and claims."""
    _sealed_shape(monkeypatch)
    host = socket.gethostname()
    owner = f"{host}:supervisor-9:8"
    queue.mint_tier_capacity(TIER, {"stage_gib": 4})
    plan = _plan(queue)
    residency_plan.freeze(queue, plan)
    queue.publish(action_key=CONSUMER, cas_root=queue.root / "cas",
                  checkout_root=queue.root / "co",
                  worker_script=queue.root / "worker.py",
                  resources={"cpu": 1, "mem_gb": 1},
                  max_attempts=5, retry_safe=True, tags=["x86"],
                  residency={"schema": pool.RESIDENCY_SCHEMA_V1,
                             "tier_id": TIER, "manifest_sha256": MANIFEST,
                             "manifest_bytes": 2 * SPAN,
                             "leads": residency_plan.leads_for(plan)})
    tick0 = tier_loop.residency_window(queue, tiers=_tiers(tmp_path))
    assert [e["action_key"] for e in tick0
            if e["event"] == "mover-published"] == [MOVER0, MOVER1]

    snap_m = queue.claim(tags=["dl380g10"], owner=f"{host}:1:mm",
                         capacity={"cpu": 4, "mem_gb": 16})
    assert snap_m is not None and snap_m["action_key"] == MOVER0
    requeue = queue.plan_requeue(dict(snap_m))
    queue.withdraw(MOVER0, reason=f"resign {owner}: test", by=owner,
                   membership_handoff=requeue["snapshot"])
    interval = tier_loop.residency_window(queue, tiers=_tiers(tmp_path))
    assert residency_plan.superseded(queue, plan) is None
    assert not [e for e in interval
                if e["event"] == "residency-plan-superseded"]
    queue.finish(MOVER0, status="failed",
                 detail={"termination_reason": "resign-test"},
                 claim_snapshot=snap_m)
    queue.publish(**requeue["arguments"],
                  preempted_claim=requeue["snapshot"], handoff_by=owner)
    ready = json.loads(queue.item_path(pool.READY, MOVER0).read_text())
    assert ready["residency"]["range_end_bytes"] == SPAN
    assert ready["recompute"] is True
    snap_b = queue.claim(tags=["dl380g10"], owner=f"{host}:1:mb",
                         capacity={"cpu": 4, "mem_gb": 16},
                         ready=_ready_for(queue, MOVER0))
    assert snap_b is not None and int(snap_b["attempts"]) == 1
    assert residency_plan.superseded(queue, plan) is None


def test_operator_cancellation_still_stops_publication(
    queue: pool.PoolQueue, tmp_path: Path, monkeypatch,
) -> None:
    """The control: an actual operator cancellation retires the window
    and later phases never publish."""
    _sealed_shape(monkeypatch)
    queue.mint_tier_capacity(TIER, {"stage_gib": 4})
    plan = _plan(queue)
    residency_plan.freeze(queue, plan)
    queue.publish(action_key=CONSUMER, cas_root=queue.root / "cas",
                  checkout_root=queue.root / "co",
                  worker_script=queue.root / "worker.py",
                  resources={"cpu": 1, "mem_gb": 1},
                  max_attempts=5, retry_safe=True, tags=["x86"],
                  residency={"schema": pool.RESIDENCY_SCHEMA_V1,
                             "tier_id": TIER, "manifest_sha256": MANIFEST,
                             "manifest_bytes": 2 * SPAN,
                             "leads": residency_plan.leads_for(plan)})
    tick0 = tier_loop.residency_window(queue, tiers=_tiers(tmp_path))
    assert [e["action_key"] for e in tick0
            if e["event"] == "mover-published"] == [MOVER0, MOVER1]
    queue.withdraw(MOVER0, reason="operator asked", by="operator:test")
    tick1 = tier_loop.residency_window(queue, tiers=_tiers(tmp_path))
    assert [e for e in tick1
            if e["event"] == "residency-plan-superseded"]
    assert residency_plan.superseded(queue, plan) is not None
    queue.mint_tier_capacity(TIER, {"stage_gib": 4})
    tick2 = tier_loop.residency_window(queue, tiers=_tiers(tmp_path))
    # The run-ahead queued before the cancellation is not republished and
    # nothing new publishes: the retired window stages no more.
    assert not [e for e in tick2 if e["event"] == "mover-published"]
