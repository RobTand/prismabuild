"""Membership retry preserves the staged action (R11).

``PoolQueue._requeue_arguments`` projected every ``publish``-supported
field except the staged bindings: a requeued consumer lost its leads
block (its successor claimed as ordinary work, silently bypassing lead
readiness), and a requeued mover kept its tier demand with no residency
block (``publish`` refuses tier demand without one, after the work was
already stopped). The projection now carries the sealed residency block
(``publish`` re-validates the same sealed arithmetic against the same
demand) and ``recompute``, so a retry re-enters the gates the original
passed.

Both scenarios run the ordinary resign sequence -- plan BEFORE
withdraw, membership withdraw, holder concludes, publish the successor
with the exact revival linkage -- against rows sealed through the real
``residency_plan`` + ``tier_loop.residency_window`` path, and finish
with another worker's claim.
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
MANIFEST = "ab" * 32
GIB = 1 << 30
SPAN = 2 * GIB


def _hexkey(seed: str) -> str:
    return (seed.encode().hex() * 64)[:64]


CONSUMER = _hexkey("r11consumer")
MOVER = _hexkey("r11mover0")
EGRESS = _hexkey("r11egress0")


@pytest.fixture()
def queue(tmp_path: Path) -> pool.PoolQueue:
    q = pool.PoolQueue(tmp_path / "pb-queue")
    q.ensure_layout()
    q.mint_tier_capacity(TIER, {"stage_gib": 8})
    return q


def _sealed_shape(monkeypatch) -> None:
    """Stub only the CAS-sealed identity lookup: direct/window test rows
    carry no sealed CAS action, and shape lookup is not what these tests
    prove (established pattern from the preemption-handoff tests)."""
    monkeypatch.setattr(adaptive_cpu, "action_identity",
                        lambda item: ("shape", False))


def _row(key: str, resources: dict[str, int],
         queue: pool.PoolQueue) -> dict[str, object]:
    return {"action_key": key, "cas_root": str(queue.root / "cas"),
            "checkout_root": str(queue.root / "co"),
            "worker_script": str(queue.root / "worker.py"),
            "tags": ["dl380g10"], "resources": resources}


def _plan(queue: pool.PoolQueue) -> dict[str, object]:
    return residency_plan.build_plan(
        consumer_action_key=CONSUMER, tier_id=TIER,
        stage_root="/stage/prewarm", manifest_sha256=MANIFEST,
        manifest_bytes=SPAN, phases=[{
            "name": "phase-0000", "start_bytes": 0, "end_bytes": SPAN,
            "stage_gib": 2,
            "mover_row": {
                **_row(MOVER, {STAGE_KIND: 2, "mem_gb": 1}, queue),
                "retry_safe": True, "max_attempts": 3,
                "residency": {
                    "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                    "manifest_sha256": MANIFEST, "manifest_bytes": SPAN,
                    "range_start_bytes": 0, "range_end_bytes": SPAN}},
            "egress_row": _row(EGRESS, {"mem_gb": 1}, queue),
        }])


def _stage(queue: pool.PoolQueue, tmp_path: Path) -> dict[str, object]:
    """Seal, freeze, publish the consumer, and run the real window."""
    plan = _plan(queue)
    residency_plan.freeze(queue, plan)
    queue.publish(action_key=CONSUMER, cas_root=queue.root / "cas",
                  checkout_root=queue.root / "co",
                  worker_script=queue.root / "worker.py",
                  resources={"cpu": 1, "mem_gb": 1},
                  max_attempts=3, retry_safe=True, tags=["x86"],
                  residency={"schema": pool.RESIDENCY_SCHEMA_V1,
                             "tier_id": TIER, "manifest_sha256": MANIFEST,
                             "manifest_bytes": SPAN,
                             "leads": residency_plan.leads_for(plan)})
    events = tier_loop.residency_window(
        queue, tiers={TIER: {"tier_id": TIER, "tier": "stage",
                             "mountpoint": str(tmp_path / "stage")}})
    published = [e["action_key"] for e in events
                 if e["event"] == "mover-published"]
    assert MOVER in published, events
    return plan


def _resign_handoff(queue: pool.PoolQueue, key: str, snap: dict,
                    owner: str) -> tuple[dict, dict]:
    """The ordinary resign sequence through the real queue transitions."""
    plan = queue.plan_requeue(dict(snap))
    withdrawn = queue.withdraw(key, reason=f"resign {owner}: test", by=owner)
    queue.finish(key, status="failed",
                 detail={"termination_reason": "resign-test"},
                 claim_snapshot=snap)
    queue.publish(**plan["arguments"], preempted_claim=plan["snapshot"],
                  handoff_by=owner)
    ready = json.loads(queue.item_path(pool.READY, key).read_text())
    return withdrawn, ready


def test_mover_retry_preserves_range_tier_recompute_and_claim(
    queue: pool.PoolQueue, tmp_path: Path, monkeypatch,
) -> None:
    """A retryable staged mover requeues with its range, tier demand, and
    movement-node contract intact, and another worker can claim it."""
    _sealed_shape(monkeypatch)
    host = socket.gethostname()
    owner = f"{host}:supervisor-9:8"
    _stage(queue, tmp_path)
    original = json.loads(queue.item_path(pool.READY, MOVER).read_text())
    assert original["residency"]["range_start_bytes"] == 0
    assert original["residency"]["range_end_bytes"] == SPAN
    assert original["recompute"] is True

    snap_a = queue.claim(tags=["dl380g10"], owner=f"{host}:1:ma",
                         capacity={"cpu": 4, "mem_gb": 16})
    assert snap_a is not None and snap_a["action_key"] == MOVER
    withdrawn, ready = _resign_handoff(queue, MOVER, snap_a, owner)
    # A mover names no plan: the window (not the withdrawal) owns mover
    # republication, so the consumer's frozen plan is untouched here.
    assert withdrawn["residency_plan_superseded"] is False
    assert ready["residency"] == original["residency"]
    assert ready["recompute"] is True
    assert ready["resources"] == original["resources"]
    assert ready["attempts"] == 1
    assert ready["attempt_history_missing_before"] == 1
    assert ready["max_attempts"] == original["max_attempts"]
    assert ready["resigned_by"] == owner
    assert ready["supersedes_withdrawal"]["withdrawn_by"] == owner

    snap_b = queue.claim(tags=["dl380g10"], owner=f"{host}:1:mb",
                         capacity={"cpu": 4, "mem_gb": 16})
    assert snap_b is not None and int(snap_b["attempts"]) == 1
    assert "tier_reservations" in snap_b
    assert queue.tier_ledger(TIER).holder_tokens(MOVER) == {"stage_gib": 2}


def test_consumer_retry_preserves_leads_and_lead_gate(
    queue: pool.PoolQueue, tmp_path: Path, monkeypatch,
) -> None:
    """A requeued consumer keeps its leads block: the gate that refused it
    while its mover was pending still guards the successor, and the
    successor is claimable once the bytes are resident."""
    _sealed_shape(monkeypatch)
    host = socket.gethostname()
    owner = f"{host}:supervisor-9:8"
    _stage(queue, tmp_path)
    original = json.loads(queue.item_path(pool.READY, CONSUMER).read_text())

    # The gate holds before the bytes land: ordinary work would claim.
    assert queue.claim(tags=["x86"], owner=f"{host}:1:probe",
                       capacity={"cpu": 4, "mem_gb": 16}) is None
    assert queue.residency_verdict(original)["state"] == "lead_not_resident"

    # Land the lead through the real staging records.
    snap_m = queue.claim(tags=["dl380g10"], owner=f"{host}:1:mm",
                         capacity={"cpu": 4, "mem_gb": 16})
    assert snap_m is not None and snap_m["action_key"] == MOVER
    queue.record_move(MOVER, {
        "tier_id": TIER, "manifest_sha256": MANIFEST,
        "range_start_bytes": 0, "range_end_bytes": SPAN,
        "bytes_staged": SPAN, "complete": True, "errors": []})
    residency_map.write_fragment(queue.residency_fragment_root(), {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": CONSUMER, "mover_action_key": MOVER,
        "tier_id": TIER, "stage_root": str(tmp_path / "stage"),
        "manifest_sha256": MANIFEST,
        "entries": {residency_map.residency_map_key("/pool/a.bin", 0): {
            "stage_path": str(tmp_path / "stage" / "a.bin"),
            "bytes": 4096, "offset": 0, "sha256": "a" * 64}}})
    queue.finish(MOVER, status="executed", detail={}, claim_snapshot=snap_m)
    # The pin holds: tokens stand for bytes still on the stage.
    assert queue.tier_ledger(TIER).holder_tokens(MOVER) == {"stage_gib": 2}
    assert tier_loop.compose_map(queue, CONSUMER) == queue.residency_map_path(
        CONSUMER)

    snap_a = queue.claim(tags=["x86"], owner=f"{host}:1:ca",
                         capacity={"cpu": 4, "mem_gb": 16})
    assert snap_a is not None and snap_a["action_key"] == CONSUMER
    withdrawn, ready = _resign_handoff(queue, CONSUMER, snap_a, owner)
    # A membership handoff preserves its sealed plan: the same work
    # continues under a new generation, so the filing is NOT retired the
    # way an operator cancellation retires it (R12 corrected the R11
    # claim here) -- and the retry is therefore not stranded by it.
    assert withdrawn["residency_plan_superseded"] is False
    assert ready["residency"] == original["residency"]
    assert ready["attempts"] == 1
    assert ready["resigned_by"] == owner

    snap_b = queue.claim(tags=["x86"], owner=f"{host}:1:cb",
                         capacity={"cpu": 4, "mem_gb": 16})
    assert snap_b is not None and int(snap_b["attempts"]) == 1
