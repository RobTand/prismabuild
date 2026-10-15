"""Holder queries retain physical reservations and their record error policies."""
from pathlib import Path
import errno
import time

import pytest

from prismabuild import adaptive_cpu, adaptive_gpu, pool
from test_adaptive_gpu import gpu_rig

TIERS = {'preferred': [8], 'fallback': [10]}
CAPACITY = {'cpu': 2, 'mem_gb': 8, 'gpu': 1}
KEY = 'a' * 64


def _queue(tmp_path):
    queue = pool.PoolQueue(tmp_path / 'queue')
    ledger = queue.ledger()
    ledger.configure_cpu_tiers(TIERS)
    ledger.ensure_capacity(CAPACITY)
    return queue, ledger


def _idle_cpu(monkeypatch):
    monkeypatch.setattr(adaptive_cpu.Controller, 'sample', lambda self: {
        'sampled_unix': time.time(), 'cpu_count': 2, 'interval_s': 1.,
        'busy_cpus': 0., 'psi_some': 0.})


def _reserved_cpu(tmp_path, monkeypatch):
    queue, ledger = _queue(tmp_path)
    _idle_cpu(monkeypatch)
    controller = adaptive_cpu.Controller(ledger, TIERS)
    with controller.locked():
        metadata = controller.decision({'action_key': KEY}, {'cpu': 1},
                                       identity=('shape', False))
        assert metadata is not None
        handle = ledger.begin_acquire(KEY, {'cpu': 1}, adaptive=metadata, cpu_tiers=TIERS)
    assert handle is not None
    assert ledger.commit_acquire(KEY, handle) == 1
    return queue, ledger


def test_private_and_mirror_tokens_remain_real_through_commit_and_release(tmp_path):
    queue, ledger = _queue(tmp_path)
    census = ledger.holder_census()
    assert census.token_totals() == {}
    assert queue.hold_tier_host_memory(ledger.host, 'fill', 3) == ('taken', '')
    handle = ledger.begin_acquire(KEY, {'cpu': 1, 'mem_gb': 2})
    assert handle is not None
    assert ledger.held_keys() == ['ram-host:fill']
    assert ledger.held() == census.token_totals() == {'cpu': 1, 'mem_gb': 5}

    assert ledger.commit_acquire(KEY, handle) == 3
    assert ledger.held_keys() == [KEY, 'ram-host:fill']
    assert ledger.holder_tokens(KEY) == {'cpu': 1, 'mem_gb': 2}
    assert ledger.release(KEY) == 3
    assert queue.release_tier_host_memory(ledger.host, 'fill') == 3
    assert census.token_totals() == ledger.held() == {}
    assert ledger.available() == CAPACITY


@pytest.mark.parametrize('damage', ['missing', 'empty_bytes', 'empty_object'])
def test_legacy_cpu_allocation_survives_absent_or_empty_metadata(tmp_path, monkeypatch, damage):
    queue, ledger = _reserved_cpu(tmp_path, monkeypatch)
    marker = ledger.held_dir / KEY / adaptive_cpu.METADATA
    if damage == 'missing':
        marker.unlink()
    else:
        marker.write_bytes(b'' if damage == 'empty_bytes' else b'{}')
    assert ledger.cpu_allocation(KEY, TIERS) == {'preferred': [8], 'fallback': []}
    assert ledger.held() == {'cpu': 1}
    assert ledger.release(KEY) == 1
    assert ledger.available() == CAPACITY


@pytest.mark.parametrize('damage', ['invalid_json', 'non_object', 'io_error'])
def test_cpu_allocation_errors_stay_strict_but_supplied_empty_metadata_skips_them(
        tmp_path, monkeypatch, damage):
    queue, ledger = _reserved_cpu(tmp_path, monkeypatch)
    marker = ledger.held_dir / KEY / adaptive_cpu.METADATA
    if damage == 'io_error':
        original = Path.read_bytes
        def unavailable(path):
            if path == marker:
                raise OSError(errno.EIO, 'metadata observation unavailable')
            return original(path)
        monkeypatch.setattr(Path, 'read_bytes', unavailable)
        error = OSError
    else:
        marker.write_bytes(b'{' if damage == 'invalid_json' else b'[]')
        error = pool.PoolContractError
    with pytest.raises(error):
        ledger.cpu_allocation(KEY, TIERS)
    assert ledger.cpu_allocation(KEY, TIERS, metadata={}) == {'preferred': [8], 'fallback': []}
    assert ledger.held() == {'cpu': 1}
    assert ledger.release(KEY) == 1
    assert ledger.available() == CAPACITY


@pytest.mark.parametrize('damage', ['missing', 'empty_bytes', 'empty_object'])
def test_claim_commit_distinguishes_absent_metadata_from_a_present_empty_record(
        tmp_path, monkeypatch, damage):
    queue, ledger = _queue(tmp_path)
    _idle_cpu(monkeypatch)
    queue.publish(action_key=KEY, cas_root=str(tmp_path / 'cas'),
                  checkout_root=str(tmp_path), worker_script='worker.py', resources={'cpu': 1})
    original = pool.ResourceLedger.commit_acquire
    def damage_committed_record(self, key, handle):
        filed = original(self, key, handle)
        if key == KEY:
            marker = self.held_dir / key / adaptive_cpu.METADATA
            if damage == 'missing':
                marker.unlink()
            else:
                marker.write_bytes(b'' if damage == 'empty_bytes' else b'{}')
        return filed
    monkeypatch.setattr(pool.ResourceLedger, 'commit_acquire', damage_committed_record)
    claimed = queue.claim(capacity=CAPACITY, cpu_tiers=TIERS, adaptive_cpu=True)
    if damage == 'empty_object':
        assert claimed is not None
        assert claimed['cpu_allocation'] == {'preferred': [8], 'fallback': []}
        assert ledger.held() == {'cpu': 1}
        queue.finish(KEY, status='executed', detail={})
    else:
        assert claimed is None
        assert queue.item_path(pool.READY, KEY).exists()
        records = adaptive_cpu.read_json(
            adaptive_cpu.local_state_base(ledger.base) / pool.CLAIM_DENIALS)['records']
        assert any(record['action_key'] == KEY
                   and record['reason'] == 'committed_reservation_incomplete'
                   for record in records.values())
    assert not ledger.held()
    assert ledger.available() == CAPACITY


def test_gpu_query_retains_metadata_only_ownership_after_the_physical_holder_releases(gpu_rig):
    queue, clock, sample, capacity, publish, tick, claim = gpu_rig
    publish(1)
    physical = claim()
    assert physical is not None
    publish(2)
    tick()
    borrower = claim()
    assert borrower is not None
    ledger = queue.ledger()
    assert ledger.holder_tokens(borrower['action_key']).get('gpu', 0) == 0
    queue.finish(physical['action_key'], status='executed', detail={})
    assert ledger.available()['gpu'] == 1
    assert ledger.holds_gpu(borrower['action_key']) is True
    queue.finish(borrower['action_key'], status='executed', detail={})
    assert ledger.holds_gpu(borrower['action_key']) is False
    assert ledger.available() == capacity


def test_gpu_query_keeps_unreadable_metadata_conservative_without_losing_other_tokens(tmp_path):
    queue, ledger = _queue(tmp_path)
    assert ledger.acquire(KEY, {'cpu': 1})
    marker = ledger.held_dir / KEY / adaptive_gpu.METADATA
    marker.write_bytes(b'{')
    assert ledger.holds_gpu(KEY) is False
    assert ledger.held() == {'cpu': 1}
    assert ledger.release(KEY) == 1
    assert ledger.available() == CAPACITY
