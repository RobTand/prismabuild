"""A stage tier's capacity is what may still be written PLUS what is staged.

``capacity_bytes`` on a stage tier is the dataset's ``available``: what ZFS
will still let a writer write, net of the bytes already on the dataset.  Those
bytes are exactly the ``stage_gib`` tokens the movers that staged them still
hold (retained == held).  Minting the ledger's capacity from ``available``
alone therefore counted every staged GiB twice: the ledger retired free tokens
as the dataset filled, free became ``available - held``, and the window
starved once staged bytes reached the pool's remaining free space -- half the
pool.  2026-09-18, ``prismabuild-stage:dl380g10``: 433 GiB held, 275 GiB
writable, zero free ``stage_gib`` tokens, and the consumer's 11 GiB head phase
was never republished after an 82 GiB egress (#621).

The ledger's capacity must be ``available + held``, so that the free supply
tracks what ZFS says is writable, and the record must say so.
"""
from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

from prismabuild import pool, storage_tiers  # noqa: E402
import tier_loop  # noqa: E402

TIER = "prismabuild-stage:dl380g10"
GIB = storage_tiers.GIB
KIND = storage_tiers.STAGE_CAPACITY_KIND
MOVER = "a" * 64


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


def test_staged_bytes_are_capacity_not_a_deduction(tmp_path):
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    first = _cycle(queue, tmp_path, 600 * GIB)
    ledger = queue.tier_ledger(TIER)
    assert ledger.capacity()[KIND] == 600
    assert first["tokens"][KIND] == 600
    assert first["held_gib"] == 0
    assert first["writable_gib"] == 600

    # A mover stages 200 GiB and keeps its tokens; ZFS now reports 400 GiB
    # writable.  The pool still has 600 GiB of stage in it.
    assert ledger.acquire(MOVER, {KIND: 200})
    second = _cycle(queue, tmp_path, 400 * GIB)
    assert ledger.capacity()[KIND] == 600, ledger.capacity()
    assert ledger.available()[KIND] == 400, ledger.available()
    assert second["tokens"][KIND] == 600
    assert second["held_gib"] == 200
    assert second["writable_gib"] == 400
    assert second["capacity_basis"] == "zfs available + held"


def test_a_release_returns_the_supply_to_what_zfs_reports(tmp_path):
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    _cycle(queue, tmp_path, 600 * GIB)
    ledger = queue.tier_ledger(TIER)
    assert ledger.acquire(MOVER, {KIND: 200})
    _cycle(queue, tmp_path, 400 * GIB)
    ledger.release(MOVER)                      # the egress deleted the bytes
    record = _cycle(queue, tmp_path, 600 * GIB)
    assert ledger.capacity()[KIND] == 600
    assert ledger.available()[KIND] == 600
    assert record["held_gib"] == 0


def test_a_record_without_a_writable_source_is_minted_as_before(tmp_path):
    """A fake or legacy record that does not say ``zfs available`` is not
    reinterpreted: its capacity is taken as the whole supply, as it was."""
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()

    def discover(**_kwargs):
        return {TIER: {"schema": storage_tiers.TIER_RECORD_SCHEMA_V1,
                       "tier_id": TIER, "host": "dl380g10", "tier": "stage",
                       "mountpoint": str(tmp_path / "stage"),
                       "capacity_bytes": 600 * GIB}}
    tier_loop.cycle(queue, host="dl380g10", source_pool="storage_pool",
                    receipts=tier_loop.ReceiptCache(), discover=discover)
    ledger = queue.tier_ledger(TIER)
    assert ledger.acquire(MOVER, {KIND: 200})
    record = tier_loop.cycle(queue, host="dl380g10", source_pool="storage_pool",
                             receipts=tier_loop.ReceiptCache(), discover=discover)[0]
    assert ledger.capacity()[KIND] == 600
    assert "capacity_basis" not in record
