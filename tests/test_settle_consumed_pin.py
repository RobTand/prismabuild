"""WIP: settle never frees consumed physical bytes (liveness lane, UNACCEPTED).

Terminal-branch proof for the funded-claim primitive, against the
UNCOMMITTED ``tier_loop`` wiring -- this file stays WIP until the broad
wiring lands and is NOT part of the minimal primitive commit:

* claim -> copy receipt -> DONE pins the mover's fence tokens as physical
  landed bytes (``finish`` keeps them on purpose);
* ``_settle_protected`` with a terminal mover and a ``consumed`` record
  releases nothing from the mover (no ``mover-terminal-fused`` event,
  tokens still held, record still ``consumed``);
* only the owner path returns them: a real ``stage_release.evict`` deletes
  the staged file, releases the tokens, and drops the fragment.

Small logical GiB quotas, real ledgers, real claim/settle/evict calls.
"""
from __future__ import annotations

from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

from prismabuild import pool, residency_map, residency_plan, storage_tiers  # noqa: E402
from prismabuild import window_credit  # noqa: E402
import stage_release  # noqa: E402
import tier_loop  # noqa: E402

TIER = "prismabuild-stage:dl380g10"
STAGE_KIND = f"stage_gib@{TIER}"
MANIFEST = "8" * 64
GIB = storage_tiers.GIB
SPAN = 1 << 20


def _hexkey(seed: str) -> str:
    return (seed.encode().hex() * 64)[:64]


def _row(key: str, resources: dict[str, int], queue: pool.PoolQueue) -> dict[str, object]:
    return {"action_key": key, "cas_root": str(queue.root / "cas"),
            "checkout_root": str(queue.root / "co"),
            "worker_script": str(queue.root / "worker.py"),
            "tags": ["dl380g10"], "resources": resources}


def _plan(queue: pool.PoolQueue, consumer: str, mover: str) -> dict[str, object]:
    start, end = 0, SPAN
    return residency_plan.build_plan(
        consumer_action_key=consumer, tier_id=TIER, stage_root="/stage/prewarm",
        manifest_sha256=MANIFEST, manifest_bytes=end, phases=[{
            "name": "phase-0",
            "start_bytes": start, "end_bytes": end, "stage_gib": 1,
            "mover_row": {
                **_row(mover, {STAGE_KIND: 1}, queue),
                "residency": {
                    "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                    "manifest_sha256": MANIFEST, "manifest_bytes": end,
                    "range_start_bytes": start, "range_end_bytes": end},
            },
            "egress_row": _row(_hexkey("settle-egress"), {"mem_gb": 1}, queue),
        }])


def test_settle_keeps_consumed_bytes_until_egress(tmp_path: Path) -> None:
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    queue.mint_tier_capacity(TIER, {"stage_gib": 5})
    ledger = queue.tier_ledger(TIER)
    stage = tmp_path / "stage"
    stage.mkdir()
    assert stage_release.register_stage_root(
        queue, tier_id=TIER, stage_root=str(stage)) == "registered"

    mover, consumer = _hexkey("settle-mover"), _hexkey("settle-consumer")
    plan = _plan(queue, mover=mover, consumer=consumer)
    phases = plan["phases"]
    assert isinstance(phases, list)
    mover_row = dict(phases[0]["mover_row"])  # type: ignore[index]
    residency_plan.freeze(queue, plan)
    queue.publish(
        action_key=mover, cas_root=mover_row["cas_root"],
        checkout_root=mover_row["checkout_root"],
        worker_script=mover_row["worker_script"],
        tags=["dl380g10"], resources=mover_row["resources"],
        residency=mover_row["residency"])
    row = pool.read_queue_record(queue.item_path(pool.READY, mover))
    assert isinstance(row, dict)

    grant = window_credit.grant_key(consumer, TIER, "mover_row", "phase-0")
    fields = {"consumer_action_key": consumer,
              "plan_sha256": residency_plan.plan_sha256(plan),
              "mover_action_key": mover,
              "range_start_bytes": 0, "range_end_bytes": SPAN,
              "kind": "stage_gib",
              "published_unix": float(row["published_unix"])}  # type: ignore[arg-type]
    assert queue.reserve_fence(TIER, grant, fields, 1) is True
    record = queue.read_funding(mover, TIER)
    assert record is not None
    generation = str(record["generation"])
    assert queue.transfer_fence(TIER, grant, mover) == 1
    assert queue.advance_funding_state(
        mover, TIER, expect="reserved", advance_to="transferring",
        generation=generation) is True

    got = queue.claim(tags=["dl380g10"], owner="w-settle")
    assert got is not None and got["action_key"] == mover
    # The actual copy, staged before the receipt measures it.
    staged = stage / "pin.bin"
    staged.write_bytes(b"s" * SPAN)
    residency_map.write_fragment(queue.residency_fragment_root(), {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": consumer, "mover_action_key": mover,
        "tier_id": TIER, "stage_root": str(stage), "manifest_sha256": MANIFEST,
        "entries": {residency_map.residency_map_key("/pool/pin.bin", 0): {
            "stage_path": str(staged), "bytes": SPAN,
            "offset": 0, "sha256": "a" * 64}}})
    queue.record_move(mover, {
        "consumer_action_key": consumer, "tier_id": TIER,
        "stage_root": str(stage), "complete": True, "bytes_staged": SPAN})
    queue.finish(mover, status="executed")
    # Landed and pinned: DONE filed, 1 GiB still charged to the mover.
    assert int(ledger.holder_tokens(mover).get("stage_gib", 0)) == 1
    assert queue.read_funding(mover, TIER)["state"] == "consumed"

    protection = {"protected": {(consumer, TIER): {
        "grant": grant, "mover": mover, "need_gib": 1, "phase": "phase-0",
        "tier_id": TIER, "kind": "stage_gib", "leg": "mover_row"}}}
    events = tier_loop._settle_protected(queue, protection)
    # Credit reconciliation frees nothing from the mover: no fused release,
    # tokens still held, record still consumed.
    assert [e for e in events
            if e.get("reason") == "mover-terminal-fused"] == []
    assert int(ledger.holder_tokens(mover).get("stage_gib", 0)) == 1
    assert ledger.available().get("stage_gib") == 4
    record = queue.read_funding(mover, TIER)
    assert record is not None and record["state"] == "consumed"
    assert str(record["generation"]) == generation

    # Only the owner path returns landed bytes: a real egress deletes the
    # staged file, releases the charge, and drops the fragment.
    receipt = stage_release.evict(
        queue, mover, consumer_action_key=consumer, stage_root=str(stage))
    assert not staged.exists()
    assert int(receipt.get("tokens_released", 0)) == 1
    assert int(ledger.holder_tokens(mover).get("stage_gib", 0)) == 0
    assert ledger.available().get("stage_gib") == 5


def _funded_pair(queue, plan, mover, row, *, kind, gib, grant):
    fields = {"consumer_action_key": str(plan["consumer_action_key"]),
              "plan_sha256": residency_plan.plan_sha256(plan),
              "mover_action_key": mover,
              "range_start_bytes": 0, "range_end_bytes": SPAN,
              "kind": kind,
              "published_unix": float(row["published_unix"])}
    tier = TIER
    assert queue.reserve_fence(tier, grant, fields, gib) is True
    record = queue.read_funding(mover, tier)
    assert record is not None
    generation = str(record["generation"])
    assert queue.transfer_fence(tier, grant, mover) == gib
    assert queue.advance_funding_state(
        mover, tier, expect="reserved", advance_to="transferring",
        generation=generation) is True
    return generation


def _protection(consumer, mover, grant, *, gib):
    return {"protected": {(consumer, TIER): {
        "grant": grant, "mover": mover, "need_gib": gib, "phase": "phase-0",
        "tier_id": TIER, "kind": "stage_gib", "leg": "mover_row"}}}


def test_settle_honors_terminal_proof_for_transferring(tmp_path: Path) -> None:
    """DONE + transferring + matching proof: physical, not free credit.

    The crash-tombstone composition: the claim persisted its ``tier_funding``
    proof, died before marking, and a finisher filed DONE from it.  The
    record says ``transferring`` but the terminal attempt binds the exact
    generation and token set -- settle must not free it.  Without the proof
    (legacy shape) the exact-name release still applies.
    """
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    queue.mint_tier_capacity(TIER, {"stage_gib": 5})
    ledger = queue.tier_ledger(TIER)
    mover, consumer = _hexkey("proof-mover"), _hexkey("proof-consumer")
    plan = _plan(queue, mover=mover, consumer=consumer)
    phases = plan["phases"]
    assert isinstance(phases, list)
    mover_row = dict(phases[0]["mover_row"])  # type: ignore[index]
    residency_plan.freeze(queue, plan)
    queue.publish(
        action_key=mover, cas_root=mover_row["cas_root"],
        checkout_root=mover_row["checkout_root"],
        worker_script=mover_row["worker_script"],
        tags=["dl380g10"], resources=mover_row["resources"],
        residency=mover_row["residency"])
    row = pool.read_queue_record(queue.item_path(pool.READY, mover))
    assert isinstance(row, dict)
    grant = window_credit.grant_key(consumer, TIER, "mover_row", "phase-proof")
    generation = _funded_pair(queue, plan, mover, row, kind="stage_gib",
                              gib=1, grant=grant)
    record = queue.read_funding(mover, TIER)
    assert record is not None
    names = [str(name) for name in record["tokens"]]

    done_path = queue.item_path(pool.DONE, mover)
    pool._write_json_atomic(done_path, {
        "schema": pool.POOL_OUTCOME_SCHEMA_V1,
        "action_key": mover, "status": "executed",
        "tier_funding": {TIER: {"generation": generation,
                                "kinds": {"stage_gib": 1},
                                "tokens": names}},
    })
    events = tier_loop._settle_protected(
        queue, _protection(consumer, mover, grant, gib=1))
    assert [e for e in events
            if e.get("reason") == "mover-terminal-fused"] == []
    assert int(ledger.holder_tokens(mover).get("stage_gib", 0)) == 1
    assert ledger.available().get("stage_gib") == 4

    # Control: the same terminal state with no proof releases exactly.
    pool._write_json_atomic(done_path, {
        "schema": pool.POOL_OUTCOME_SCHEMA_V1,
        "action_key": mover, "status": "executed",
    })
    events = tier_loop._settle_protected(
        queue, _protection(consumer, mover, grant, gib=1))
    assert [e for e in events
            if e.get("reason") == "mover-terminal-fused"] != []
    assert int(ledger.holder_tokens(mover).get("stage_gib", 0)) == 0
    assert ledger.available().get("stage_gib") == 5
