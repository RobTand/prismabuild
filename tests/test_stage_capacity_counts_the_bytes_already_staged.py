"""A stage tier's capacity is what may still be written PLUS what has landed.

``capacity_bytes`` on a stage tier is the dataset's ``available``: what ZFS
will still let a writer write, net of the bytes already on the dataset.  A
mover takes its ``stage_gib`` tokens at claim and keeps them past ``finish``
only once its receipt says the whole range landed, so held tokens are two
things: bytes on the dataset (**landed**, already subtracted from
``available``) and bytes still coming (**in flight**, not yet subtracted).

Two wrong formulas, one day apart, 2026-09-18 on ``prismabuild-stage:dl380g10``:

* ``available`` alone counted every landed GiB twice -- free fell as
  ``available - held`` and the window starved at half the pool: 433 GiB held,
  275 GiB writable, zero free tokens, an 11 GiB head phase never republished
  (#621).
* ``available + held`` counted a claimed mover's unlanded bytes as free and
  admitted one more window every cycle while the first was still copying: ten
  82 GiB movers against 275 GiB writable, all ten ENOSPC (#623).

The supply is ``available + landed``; in-flight tokens stay a deduction until
their bytes are on the dataset, and the record says which is which.
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


def _landed(queue, mover, gib, *, complete=True):
    """File the receipt a mover writes when its copy ends."""
    queue.record_move(mover, {"tier_id": TIER, "complete": complete,
                              "bytes_staged": gib * GIB if complete else GIB // 2,
                              "range_start_bytes": 0, "range_end_bytes": gib * GIB,
                              "manifest_sha256": "9" * 64})


def test_staged_bytes_are_capacity_not_a_deduction(tmp_path):
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    first = _cycle(queue, tmp_path, 600 * GIB)
    ledger = queue.tier_ledger(TIER)
    assert ledger.capacity()[KIND] == 600
    assert first["tokens"][KIND] == 600
    assert first["held_gib"] == 0
    assert first["writable_gib"] == 600

    # A mover staged 200 GiB, said so, and keeps its tokens; ZFS now reports
    # 400 GiB writable.  The pool still has 600 GiB of stage in it.
    assert ledger.acquire(MOVER, {KIND: 200})
    _landed(queue, MOVER, 200)
    second = _cycle(queue, tmp_path, 400 * GIB)
    assert ledger.capacity()[KIND] == 600, ledger.capacity()
    assert ledger.available()[KIND] == 400, ledger.available()
    assert second["tokens"][KIND] == 600
    assert second["held_gib"] == 200
    assert second["landed_gib"] == 200
    assert second["in_flight_gib"] == 0
    assert second["writable_gib"] == 400
    assert second["capacity_basis"] == "zfs available + landed"


def test_an_in_flight_reservation_is_not_free_supply(tmp_path):
    """The mover has its tokens; its bytes are not on the dataset yet.

    ZFS still reports the whole pool writable, so ``available + held`` would
    mint 800 and offer the 200 GiB this copy is about to write to a second
    window (#623).  The supply is 600 and 200 of it is spoken for.
    """
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    _cycle(queue, tmp_path, 600 * GIB)
    ledger = queue.tier_ledger(TIER)
    assert ledger.acquire(MOVER, {KIND: 200})          # claimed, copying, no receipt

    record = _cycle(queue, tmp_path, 600 * GIB)
    assert ledger.capacity()[KIND] == 600, ledger.capacity()
    assert ledger.available()[KIND] == 400, ledger.available()
    assert record["in_flight_gib"] == 200
    assert record["landed_gib"] == 0
    assert record["held_gib"] == 200

    # Half way: 100 GiB landed, ZFS says 500, the receipt is not filed yet.
    # The loop cannot know how much this copy will still write, so the whole
    # reservation stays a deduction: 300 free, never 500.
    record = _cycle(queue, tmp_path, 500 * GIB)
    assert ledger.capacity()[KIND] == 500
    assert ledger.available()[KIND] == 300, ledger.available()

    # Landed: the receipt is complete, ZFS says 400, the pool has 600 in it.
    _landed(queue, MOVER, 200)
    record = _cycle(queue, tmp_path, 400 * GIB)
    assert ledger.capacity()[KIND] == 600
    assert ledger.available()[KIND] == 400
    assert record["landed_gib"] == 200 and record["in_flight_gib"] == 0


def test_a_copy_that_fell_short_is_still_in_flight(tmp_path):
    """An incomplete receipt pins nothing; until its tokens go, they deduct."""
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    _cycle(queue, tmp_path, 600 * GIB)
    ledger = queue.tier_ledger(TIER)
    assert ledger.acquire(MOVER, {KIND: 200})
    _landed(queue, MOVER, 200, complete=False)
    record = _cycle(queue, tmp_path, 580 * GIB)
    assert record["in_flight_gib"] == 200 and record["landed_gib"] == 0
    assert ledger.capacity()[KIND] == 580
    assert ledger.available()[KIND] == 380


def test_writable_below_the_in_flight_reservation_mints_without_raising(tmp_path):
    """Late in a copy ZFS reports less than the copy still holds; free is 0, not an error."""
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    _cycle(queue, tmp_path, 600 * GIB)
    ledger = queue.tier_ledger(TIER)
    assert ledger.acquire(MOVER, {KIND: 200})
    record = _cycle(queue, tmp_path, 50 * GIB)
    assert record["in_flight_gib"] == 200
    assert ledger.available().get(KIND, 0) <= 0, ledger.available()


def test_a_release_returns_the_supply_to_what_zfs_reports(tmp_path):
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    _cycle(queue, tmp_path, 600 * GIB)
    ledger = queue.tier_ledger(TIER)
    assert ledger.acquire(MOVER, {KIND: 200})
    _landed(queue, MOVER, 200)
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
