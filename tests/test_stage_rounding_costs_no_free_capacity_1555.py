"""Issue 1555: landed rounding costs no free capacity.

A fixture with N = 1, 10 and 100 landed sub-GiB movers keeps the
free token count at writable minus in-flight tokens. Held tokens
stay at the sum of the per-range ceil. No seal or identity wall.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

from prismabuild import pool, storage_tiers  # noqa: E402
import tier_loop  # noqa: E402

TIER = "prismabuild-stage:dl380g10"
GIB = storage_tiers.GIB
KIND = storage_tiers.STAGE_CAPACITY_KIND


def _key(seed: str) -> str:
    return (seed.encode().hex() * 64)[:64]


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
    free = ledger.available().get(KIND, 0)
    assert free == max(0, writable_gib - 0)
    assert ledger.capacity()[KIND] == writable_gib + count


def test_empty_tier_reports_zero_rounding(tmp_path):
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    record = _cycle(queue, tmp_path, 100 * GIB)
    assert record["landed_gib"] == 0
    assert record["in_flight_gib"] == 0
    assert record["landed_bytes"] == 0
    assert record["in_flight_bytes"] == 0
    assert record["landed_rounding_gib"] == 0
    assert record["in_flight_rounding_gib"] == 0
