"""Removing an unkept receipt must invalidate folds even with receipts left."""
import os
from types import SimpleNamespace

from prismabuild import pool, tier_cycle
from record_store_trust import ObservedTrust
import test_a_terminal_receipt_retires_behind_a_covered_fold as history


def test_same_tick_receipt_removal_invalidates_both_folds(tmp_path, monkeypatch):
    queue = pool.PoolQueue(tmp_path / 'queue')
    queue.records.ensure_layout()
    consumer = history._key('same-tick-consumer')
    retained = history._move(queue, 'retained', consumer=consumer,
                             unix=history.T0, delivered=200.0, file_side=150.0)
    removed = history._move(queue, 'same-tick', consumer=consumer,
                            unix=history.T0 + 1, delivered=900.0, file_side=850.0)
    path = queue.records.move_path(removed)
    fence = 1_000_000
    metadata = {}
    for key, tick in ((retained, fence - 1), (removed, fence)):
        receipt = queue.records.move_path(key)
        info = receipt.stat()
        pinned = SimpleNamespace(**{
            name: getattr(info, name) for name in dir(info) if name.startswith('st_')})
        pinned.st_mtime_ns = pinned.st_ctime_ns = tick
        pinned.st_mtime = pinned.st_ctime = tick / 10**9
        metadata[str(receipt)] = pinned
    real_stat, real_fstat = os.stat, os.fstat

    def stat(target, *args, **kwargs):
        info = real_stat(target, *args, **kwargs)
        return info if isinstance(target, int) else metadata.get(os.fspath(target), info)

    def fstat(descriptor):
        info = real_fstat(descriptor)
        try:
            name = os.readlink(f'/proc/self/fd/{descriptor}')
        except OSError:
            name = None
        return metadata.get(name, info)

    monkeypatch.setattr(os, 'stat', stat)
    monkeypatch.setattr(os, 'fstat', fstat)
    trust = ObservedTrust()
    trust.fence = fence
    cache = tier_cycle.ReceiptCache(trust=trust)
    directories = [path.parent]
    assert len(cache.read(directories, queue=queue)) == 2
    before = (cache.fill_supply(history.IDENTITY_A),
              cache.mover_cap(history.TIER, history.IDENTITY_A))
    generation = cache.records.generation(path.parent)
    path.unlink()
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
