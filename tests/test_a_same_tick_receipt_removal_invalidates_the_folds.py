"""Removing an unkept receipt must invalidate folds even with receipts left."""
import time

from prismabuild import pool, tier_cycle
from record_store_trust import ObservedTrust
import test_a_terminal_receipt_retires_behind_a_covered_fold as history


def test_same_tick_receipt_removal_invalidates_both_folds(tmp_path):
    queue = pool.PoolQueue(tmp_path / 'queue')
    queue.records.ensure_layout()
    consumer = history._key('same-tick-consumer')
    retained = history._move(queue, 'retained', consumer=consumer,
                             unix=history.T0, delivered=200.0, file_side=150.0)
    # Separate the file stamps so the fence keeps the older receipt while
    # refusing the new one. This waits for a later filesystem clock tick.
    time.sleep(0.02)
    removed = history._move(queue, 'same-tick', consumer=consumer,
                            unix=history.T0 + 1, delivered=900.0, file_side=850.0)
    path = queue.records.move_path(removed)
    assert queue.records.move_path(retained).stat().st_ctime_ns < path.stat().st_ctime_ns
    trust = ObservedTrust()
    trust.fence = path.stat().st_ctime_ns
    cache = tier_cycle.ReceiptCache(trust=trust)
    directories = [path.parent]
    assert len(cache.read(directories, queue=queue)) == 2
    before = (cache.fill_supply(history.IDENTITY_A),
              cache.mover_cap(history.TIER, history.IDENTITY_A))
    generation = cache.records.generation(path.parent)
    path.unlink()
    trust.fence = 2**63 - 1
    remaining = cache.read(directories, queue=queue)
    assert [record['action_key'] for record in remaining] == [retained]
    # The removed receipt was never kept, so the kept-set generation holds.
    assert cache.records.generation(path.parent) == generation
    cold = tier_cycle.ReceiptCache(trust=ObservedTrust())
    assert cold.read(directories, queue=queue) == remaining
    expected = (cold.fill_supply(history.IDENTITY_A),
                cold.mover_cap(history.TIER, history.IDENTITY_A))
    assert all(old != fresh for old, fresh in zip(before, expected)), (
        'the removed receipt must affect both folds')
    actual = (cache.fill_supply(history.IDENTITY_A),
              cache.mover_cap(history.TIER, history.IDENTITY_A))
    assert actual == expected, 'removing an unkept receipt left supply or mover cap stale'
