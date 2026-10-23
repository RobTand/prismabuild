"""Spark-first is a bounded preference over positive idle-GPU evidence (#1929)."""
from __future__ import annotations

import time

import pytest

from prismabuild import box_capacity, pool
from test_gpu_evicts_cpu_guests import guest_fleet


def _guest_offer(fleet, **changes):
    path = fleet.queue.root / pool.WORKERS / 'sparky.json'
    offer = pool.record_store.read_document(path)
    offer['observed_detail'].update(changes)
    pool.immutable_files.write_json_atomic(path, offer)


def _cpu_available(fleet):
    fleet.queue.ledger('cpuhost').release('f' * 64, certificate=fleet.queue.ledger('cpuhost').certify_release('f' * 64))


def _placement_denial(fleet, key, host):
    return next(row for row in fleet.queue.latest_denials({key})[key] if row['host'] == host)


def test_idle_gpu_host_takes_restartable_portable_cpu_before_cpu_host(guest_fleet):
    fleet = guest_fleet
    _cpu_available(fleet)
    key = fleet.publish('a')
    assert fleet.claim('cpuhost') is None
    assert _placement_denial(fleet, key, 'cpuhost')['reason'] == 'deferred_for_idle_gpu_host'
    row = fleet.claim()
    assert row['action_key'] == key and row['cpu_guest'] is True
    assert not fleet.queue.records.passes_path(key).exists()


def test_cpu_host_claims_at_twenty_seconds_even_if_idle_gpu_peer_does_nothing(guest_fleet, monkeypatch):
    fleet = guest_fleet
    _cpu_available(fleet)
    clock = [10.]
    monkeypatch.setattr(pool.time, 'monotonic', lambda: clock[0])
    key = fleet.publish('a')
    assert fleet.claim('cpuhost') is None
    clock[0] = 29.999
    assert fleet.claim('cpuhost') is None
    clock[0] = 30.
    row = fleet.claim('cpuhost')
    assert row['action_key'] == key and not row.get('cpu_guest')


@pytest.mark.parametrize('fault', ['foreign', 'vllm', 'stale', 'future', 'untrusted', 'missing', 'reserved'])
def test_gpu_in_use_or_unknown_keeps_cpu_host_placement(guest_fleet, fault):
    fleet = guest_fleet
    _cpu_available(fleet)
    if fault in ('foreign', 'vllm'):
        from test_gpu_sample_facts_profiles import _reporting_sample
        sample = _reporting_sample()
        sample.update(sampled_unix=time.time(), foreign_processes=[{
            'pid': 1234, 'cmdline': 'vllm serve model' if fault == 'vllm' else 'foreign CUDA'}])
        observed = box_capacity.observe(fleet.capacity, {}, gpu_sample=sample,
                                        mem_gb=16, load1=0)
        _guest_offer(fleet, **observed.detail)
        assert observed.detail['foreign_gpu_processes'] == 1
    elif fault == 'reserved':
        assert fleet.queue.ledger('sparky').acquire('e' * 64, {'gpu': 1})
    elif fault == 'stale':
        _guest_offer(fleet, gpu_sampled_unix=time.time() - 100)
    elif fault == 'future':
        _guest_offer(fleet, gpu_sampled_unix=time.time() + 60)
    elif fault == 'untrusted':
        _guest_offer(fleet, gpu_capacity_trusted=False)
    else:
        _guest_offer(fleet, gpu_sampled_unix=None)
    key = fleet.publish('a')
    assert fleet.claim() is None
    assert _placement_denial(fleet, key, 'sparky')['reason'] == 'deferred_for_cpu_only_host'
    assert fleet.claim('cpuhost')['action_key'] == key


def test_ready_gpu_row_and_class_pin_keep_existing_placement(guest_fleet):
    fleet = guest_fleet
    _cpu_available(fleet)
    gpu = fleet.publish('b', gpu=True, cpu=8, tags=['sparky'], priority=-10)
    cpu = fleet.publish('a', priority=10)
    assert fleet.claim('cpuhost')['action_key'] == cpu
    assert fleet.claim()['action_key'] == gpu


@pytest.mark.parametrize('fault', ['full', 'mismatch', 'cpu-peer', 'stale-offer', 'unknown-gpu'])
def test_cpu_host_only_prefers_a_matching_fitting_live_gpu_host(guest_fleet, fault):
    fleet = guest_fleet
    _cpu_available(fleet)
    if fault == 'full':
        assert fleet.queue.ledger('sparky').acquire('e' * 64, {'cpu': 8})
    path = fleet.queue.root / pool.WORKERS / 'sparky.json'
    offer = pool.record_store.read_document(path)
    if fault == 'cpu-peer':
        offer['has_gpu'] = False
    elif fault == 'stale-offer':
        offer['announced_unix'] -= pool.OFFER_TIMEOUT_S + 1
    elif fault == 'unknown-gpu':
        offer.pop('has_gpu')
    pool.immutable_files.write_json_atomic(path, offer)
    key = fleet.publish('a', **({'tags': ['x86']} if fault == 'mismatch' else {}))
    assert fleet.claim('cpuhost')['action_key'] == key


def test_an_idle_preference_does_not_mean_the_cpu_host_passed_on_the_row(guest_fleet):
    fleet = guest_fleet
    _cpu_available(fleet)
    key = fleet.publish('a')
    assert fleet.claim('cpuhost') is None
    assert fleet.queue.ledger('sparky').acquire('e' * 64, {'gpu': 1})
    assert fleet.claim() is None
    assert _placement_denial(fleet, key, 'sparky')['reason'] == 'deferred_for_cpu_only_host'
    assert fleet.claim('cpuhost')['action_key'] == key


@pytest.mark.parametrize('restartable', [False, True])
def test_nonrestartable_portable_work_keeps_cpu_first(guest_fleet, restartable):
    fleet = guest_fleet
    _cpu_available(fleet)
    key = fleet.publish('a', retry_safe=restartable, max_attempts=1)
    assert fleet.claim() is None
    assert fleet.claim('cpuhost')['action_key'] == key


def test_deferring_rows_charge_the_idle_peer_once_per_scan(guest_fleet):
    fleet = guest_fleet
    _cpu_available(fleet)
    first = fleet.publish('a', cpu=6)
    second = fleet.publish('b', cpu=6)
    row = fleet.claim('cpuhost')
    assert row['action_key'] == second
    assert _placement_denial(fleet, first, 'cpuhost')['reason'] == 'deferred_for_idle_gpu_host'


def test_busy_gpu_cpu_host_deferral_skips_restartability_io(guest_fleet, monkeypatch):
    fleet = guest_fleet
    _cpu_available(fleet)
    assert fleet.queue.ledger('sparky').acquire('e' * 64, {'gpu': 1})
    key = fleet.publish('a')
    def unexpected(row):
        pytest.fail('busy-host CPU deferral read sealed guest restartability')
    monkeypatch.setattr(fleet.queue, '_guest_restartable', unexpected)
    assert fleet.claim() is None
    assert _placement_denial(fleet, key, 'sparky')['reason'] == 'deferred_for_cpu_only_host'
