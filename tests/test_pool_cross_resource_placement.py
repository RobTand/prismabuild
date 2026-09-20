"""The cross-resource placement preference: it bites, and it always yields.

A soft preference, not a constraint. It may make a claimant wait; it may never
refuse work, strand it, or place it somewhere it could not run.
"""
import socket
import time

import pytest

from prismabuild import pool


@pytest.fixture(params=[False, True], ids=['cpu-on-gpu-busy', 'gpu-on-cpu-busy'])
def placement(tmp_path, request):
    queue = pool.PoolQueue(tmp_path / 'queue')
    capacity = {'cpu': 4, 'gpu': 1, 'mem_gb': 4}
    tiers = {'preferred': [0, 1, 2, 3], 'fallback': []}
    gpu = request.param

    def announce(host, *, busy=False, tags=(), stale=False):
        ledger = queue.ledger(host)
        ledger.configure_cpu_tiers(tiers)
        ledger.ensure_capacity(capacity)
        age = 10 if stale else 0
        queue.announce(host=host, tags=tags, has_gpu=True, capacity=capacity,
                       observed_capacity=capacity, cpu_tiers=tiers,
                       observed_detail={'observed_unix': time.time() - age,
                                        'load1': 3. if busy else 0.,
                                        'gpu_power_fraction': .75 if busy else .1,
                                        'gpu_power_sampled_unix': time.time() - age})

    # This box is working the resource the item does not want; 'other' is not.
    announce(socket.gethostname(), busy=True, tags=['local'])
    announce('other')
    key = 'a' * 64
    queue.publish(action_key=key, cas_root=str(tmp_path / 'cas'),
                  checkout_root=str(tmp_path), worker_script='worker.py',
                  resources={'cpu': 1, 'mem_gb': 1, **({'gpu': 1} if gpu else {})})

    def claim():
        return queue.claim(tags=['local'], has_gpu=True, capacity=capacity, cpu_tiers=tiers)

    return queue, announce, claim, key


def test_busy_opposite_resource_defers_to_a_freer_worker(placement, monkeypatch):
    """It bites -- and the bite is bounded, never a new resource veto."""

    queue, announce, claim, key = placement
    monkeypatch.setattr(pool.time, 'monotonic', lambda: 10.)
    assert claim() is None
    assert not queue.ledger().held_keys()
    monkeypatch.setattr(pool.time, 'monotonic', lambda: 31.)
    assert claim()


@pytest.mark.parametrize('reason', ['busy', 'memory', 'stale', 'incompatible'])
def test_without_a_viable_alternative_the_work_is_placed_anyway(placement, reason):
    """It yields: no alternative, no preference, and the work runs here."""

    queue, announce, claim, key = placement
    if reason == 'busy':
        announce('other', busy=True)
    elif reason == 'memory':
        assert queue.ledger('other').acquire('b' * 64, {'mem_gb': 4})
    elif reason == 'stale':
        announce('other', stale=True)
    else:
        item = pool._read_json(queue.item_path(pool.READY, key))
        item['tags'] = ['local']
        pool._write_json_atomic(queue.item_path(pool.READY, key), item)
    assert claim()


def test_a_stale_local_reading_is_not_a_busy_box(placement):
    queue, announce, claim, key = placement
    announce(socket.gethostname(), busy=True, tags=['local'], stale=True)
    assert claim()


def _offer(*, measured=None, legacy=None, age=0.0, has_gpu=True):
    detail = {'observed_unix': time.time() - age,
              'load1': 0.,
              'gpu_power_sampled_unix': time.time() - age}
    if measured is not None:
        detail['gpu_power_measured_fraction'] = measured
    if legacy is not None:
        detail['gpu_power_fraction'] = legacy
    return {'observed_detail': detail, 'has_gpu': has_gpu,
            'cpu_tiers': {'preferred': [0, 1, 2, 3], 'fallback': []}}


def test_placement_prefers_measured_fraction_over_legacy_proxy():
    """Idle SW-capped box: proxy 1.0 must not defer CPU work when raw is 0.03."""
    idle_capped = _offer(measured=4.32 / 140.0, legacy=1.0)
    assert pool.PoolQueue._opposite_resource_load(idle_capped, gpu_job=False) == pytest.approx(4.32 / 140.0)
    busy = _offer(measured=0.75, legacy=1.0)
    assert pool.PoolQueue._opposite_resource_load(busy, gpu_job=False) == pytest.approx(0.75)


def test_placement_falls_back_to_legacy_proxy_for_old_offers():
    legacy_only = _offer(legacy=0.75)
    assert pool.PoolQueue._opposite_resource_load(legacy_only, gpu_job=False) == pytest.approx(0.75)
    assert pool.PoolQueue._opposite_resource_load(_offer(), gpu_job=False) is None
