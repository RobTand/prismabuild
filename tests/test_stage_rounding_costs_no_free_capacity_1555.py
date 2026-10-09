"""Issue 1555: landed rounding costs no free capacity.

A fixture with N = 1, 10 and 100 landed sub-GiB movers keeps the
free token count at writable minus in-flight tokens. Held tokens
stay at the sum of the per-range ceil. No seal or identity wall.
An in-flight mover reports its sealed plan range, never zero
bytes. A holder no plan names reports under in_flight_unknown_gib.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

from prismabuild import pool, residency_plan, storage_tiers  # noqa: E402
import tier_loop  # noqa: E402

TIER = "prismabuild-stage:dl380g10"
GIB = storage_tiers.GIB
KIND = storage_tiers.STAGE_CAPACITY_KIND
STAGE_KIND = f"stage_gib@{TIER}"
MANIFEST = "9" * 64


def _key(seed: str) -> str:
    return (seed.encode().hex() * 64)[:64]


def _row(queue, key: str, resources: dict[str, int]) -> dict[str, object]:
    return {"action_key": key, "cas_root": str(queue.root / "cas"),
            "checkout_root": str(queue.root / "co"),
            "worker_script": str(queue.root / "worker.py"),
            "tags": ["dl380g10"], "resources": resources}


def _plan(queue, consumer: str, movers: list[tuple[str, int, int]]) -> dict:
    built = []
    for ordinal, (mover, start, end) in enumerate(movers):
        gib = storage_tiers.stage_tokens_for_bytes(end - start)
        built.append({
            "name": f"phase-{ordinal}",
            "start_bytes": start, "end_bytes": end, "stage_gib": gib,
            "mover_row": {
                **_row(queue, mover, {STAGE_KIND: gib, "cpu": 1,
                                      "mem_gb": 1}),
                "residency": {
                    "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                    "manifest_sha256": MANIFEST,
                    "manifest_bytes": movers[-1][2],
                    "range_start_bytes": start, "range_end_bytes": end},
            },
            "egress_row": _row(queue, _key(f"1555-egress-{ordinal}"),
                               {"mem_gb": 1}),
        })
    return residency_plan.build_plan(
        consumer_action_key=consumer, tier_id=TIER,
        stage_root="/stage/prewarm", manifest_sha256=MANIFEST,
        manifest_bytes=movers[-1][2], phases=built)


def _cycle(queue, tmp_path, writable_bytes):
    def discover(**_kwargs):
        return {TIER: {"schema": storage_tiers.TIER_RECORD_SCHEMA_V1,
                       "tier_id": TIER, "host": "dl380g10", "tier": "stage",
                       "mountpoint": str(tmp_path / "stage"),
                       "capacity_bytes": writable_bytes,
                       "capacity_source": storage_tiers.WRITABLE_CAPACITY_SOURCE}}
    announced = tier_loop.cycle(queue, host="dl380g10", source_pool="storage_pool",
                                receipts=tier_loop.ReceiptCache(), discover=discover)
    assert len(announced) == 1
    return announced[0]


@pytest.mark.parametrize("count", [1, 10, 100])
def test_landed_sub_gib_movers_keep_free_at_writable_minus_in_flight(tmp_path, count):
    """Free tokens equal writable minus in-flight, bounded below by zero."""
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    sub = GIB // 4
    writable_gib = count + 50
    _cycle(queue, tmp_path, writable_gib * GIB)
    ledger = queue.tier_ledger(TIER)
    total_bytes = 0
    for index in range(count):
        mover = _key(f"1555-landed-{index:04d}")
        assert ledger.acquire(mover, {KIND: 1})
        queue.record_move(mover, {"tier_id": TIER, "complete": True,
                                  "bytes_staged": sub,
                                  "range_start_bytes": 0,
                                  "range_end_bytes": sub,
                                  "manifest_sha256": "9" * 64})
        total_bytes += sub
    record = _cycle(queue, tmp_path, writable_gib * GIB)
    assert record["landed_gib"] == count
    assert record["in_flight_gib"] == 0
    assert record["landed_bytes"] == total_bytes
    assert record["landed_rounding_gib"] == count - total_bytes // GIB
    assert record["in_flight_rounding_gib"] == 0
    assert record["in_flight_unknown_gib"] == 0
    free = ledger.available().get(KIND, 0)
    assert free == max(0, writable_gib - 0)
    assert ledger.capacity()[KIND] == writable_gib + count


@pytest.mark.parametrize("writable_gib,flight_tokens", [(50, 3), (2, 5)])
def test_free_holds_in_flight_deduction_with_zero_floor(tmp_path, writable_gib,
                                                        flight_tokens):
    """Free equals writable minus in-flight, bounded below by zero."""
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    _cycle(queue, tmp_path, max(writable_gib, flight_tokens) * GIB)
    ledger = queue.tier_ledger(TIER)
    mover = _key(f"1555-flight-{writable_gib}-{flight_tokens}")
    assert ledger.acquire(mover, {KIND: flight_tokens})
    record = _cycle(queue, tmp_path, writable_gib * GIB)
    assert record["landed_gib"] == 0
    assert record["in_flight_gib"] == flight_tokens
    assert record["in_flight_unknown_gib"] == flight_tokens
    assert record["in_flight_rounding_gib"] == 0
    # The retire keeps held tokens: the total falls only as holders
    # finish, while free already reads the shrunken supply.
    assert ledger.capacity()[KIND] == max(writable_gib, flight_tokens)
    assert ledger.available().get(KIND, 0) == max(0, writable_gib - flight_tokens)


def test_empty_tier_reports_zero_rounding(tmp_path):
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    record = _cycle(queue, tmp_path, 100 * GIB)
    assert record["landed_gib"] == 0
    assert record["in_flight_gib"] == 0
    assert record["landed_bytes"] == 0
    assert record["in_flight_bytes"] == 0
    assert record["in_flight_unknown_gib"] == 0
    assert record["landed_rounding_gib"] == 0
    assert record["in_flight_rounding_gib"] == 0


def test_in_flight_mover_reports_declared_range_not_zero(tmp_path):
    """One in-flight mover with ceil(2.4 GiB) = 3 tokens reports 0-1 waste."""
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    consumer = _key("1555-consumer-flight")
    mover = _key("1555-mover-flight")
    span = int(2.4 * GIB)
    plan = _plan(queue, consumer, [(mover, 0, span)])
    residency_plan.freeze(queue, plan)
    _cycle(queue, tmp_path, 100 * GIB)
    ledger = queue.tier_ledger(TIER)
    assert ledger.acquire(mover, {KIND: 3})
    record = _cycle(queue, tmp_path, 100 * GIB)
    assert record["landed_gib"] == 0
    assert record["in_flight_gib"] == 3
    assert record["in_flight_bytes"] == span
    assert record["in_flight_unknown_gib"] == 0
    assert record["in_flight_rounding_gib"] in (0, 1)
    assert record["in_flight_rounding_gib"] == 3 - span // GIB


def test_in_flight_holder_without_plan_leg_reports_unknown(tmp_path):
    """A holder no filed plan names lands in in_flight_unknown_gib."""
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    _cycle(queue, tmp_path, 100 * GIB)
    ledger = queue.tier_ledger(TIER)
    mover = _key("1555-mover-unknown")
    assert ledger.acquire(mover, {KIND: 2})
    record = _cycle(queue, tmp_path, 100 * GIB)
    assert record["in_flight_gib"] == 2
    assert record["in_flight_bytes"] == 0
    assert record["in_flight_unknown_gib"] == 2
    assert record["in_flight_rounding_gib"] == 0


def test_mixed_known_and_unknown_excludes_unknown_from_waste(tmp_path):
    """Known rounding counts; unknown tokens stay in the deduction."""
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    consumer = _key("1555-consumer-mixed")
    known = _key("1555-mover-mixed-known")
    stranger = _key("1555-mover-mixed-unknown")
    span = int(2.4 * GIB)
    plan = _plan(queue, consumer, [(known, 0, span)])
    residency_plan.freeze(queue, plan)
    _cycle(queue, tmp_path, 100 * GIB)
    ledger = queue.tier_ledger(TIER)
    assert ledger.acquire(known, {KIND: 3})
    assert ledger.acquire(stranger, {KIND: 2})
    record = _cycle(queue, tmp_path, 100 * GIB)
    assert record["in_flight_gib"] == 5
    assert record["in_flight_bytes"] == span
    assert record["in_flight_unknown_gib"] == 2
    assert record["in_flight_rounding_gib"] == 3 - span // GIB
