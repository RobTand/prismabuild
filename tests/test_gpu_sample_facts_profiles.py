"""Consumer policy differences on synthetic observations and real queue holds."""
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy

import pytest

from test_adaptive_gpu import gpu_rig
from prismabuild import adaptive_cpu, adaptive_gpu, box_capacity, pool


def _denial(queue, key):
    path = adaptive_cpu.local_state_base(queue.ledger().base) / pool.CLAIM_DENIALS
    records = adaptive_cpu.read_json(path)['records']
    return next(record for record in records.values() if record['action_key'] == key)


def _publish_cpu(queue):
    key = 'f' * 64
    queue.publish(action_key=key, cas_root='/cas', checkout_root='/co', worker_script='/w.py',
                  resources={'cpu': 8, 'mem_gb': 4}, priority=-10,
                  tags=[pool.socket.gethostname()])
    return key


def _claim_here(queue, capacity):
    return queue.claim(tags=[pool.socket.gethostname()], capacity=capacity,
                       cpu_tiers={'preferred': list(range(8)), 'fallback': []},
                       adaptive_cpu=True, has_gpu=True)


def _offer(sample, capacity):
    return box_capacity.observe(capacity, {}, gpu_sample=sample, mem_gb=100, load1=0)


@pytest.mark.parametrize('profile,admitted,offered', [
    ('missing_cpu_full', True, False),
    ('coerced_host_memory', False, True),
    ('multiple_devices', False, True),
    ('nonempty_device_identity', True, False),
])
def test_offer_evidence_and_admission_keep_their_own_profiles(gpu_rig, profile, admitted, offered):
    queue, clock, sample, capacity, publish, tick, claim = gpu_rig
    sample['cpu_pressure_full'] = 0.
    if profile == 'missing_cpu_full':
        del sample['cpu_pressure_full']
    elif profile == 'coerced_host_memory':
        sample['host_total_bytes'] = str(sample['host_total_bytes'])
        sample['host_available_bytes'] = str(sample['host_available_bytes'])
    elif profile == 'multiple_devices':
        second = deepcopy(sample['devices'][0])
        second['uuid'] = 'GPU-2'
        sample['devices'].append(second)
    else:
        sample['devices'][0]['uuid'] = 'device-1'
    assert bool(_offer(sample, capacity).capacity['gpu']) is offered
    key = publish(1)
    claimed = claim()
    assert (claimed is not None) is admitted
    if admitted:
        assert claimed['action_key'] == key
        assert queue.ledger().holder_tokens(key) == {'cpu': 1, 'mem_gb': 1, 'gpu': 1}
        queue.finish(key, status='executed', detail={})
    else:
        assert _denial(queue, key)['evidence']['decision']['reason'] == 'sample_invalid_or_stale'
        assert queue.item_path(pool.READY, key).exists()
    assert not queue.ledger().held_keys()
    assert queue.ledger().available() == capacity


def test_a_busy_row_keeps_room_on_evidence_that_cannot_admit_it(gpu_rig):
    queue, clock, sample, capacity, publish, tick, claim = gpu_rig
    sample['complete'] = False
    gpu = publish(1)
    cpu = _publish_cpu(queue)
    with queue._transition_locked(gpu), ThreadPoolExecutor(max_workers=1) as executor:
        assert executor.submit(_claim_here, queue, capacity).result(timeout=5) is None
    busy = _denial(queue, gpu)
    assert busy['reason'] == 'transition_busy'
    assert busy['evidence']['gpu_room_kept'] == {'cpu': 1, 'mem_gb': 1, 'gpu': 1}
    assert busy['evidence']['gpu_room_binds_all'] is True
    deferred = _denial(queue, cpu)
    assert deferred['reason'] == 'deferred_for_ready_gpu_row'
    assert deferred['evidence']['gpu_row'] == gpu[:12]
    assert not queue.ledger().held_keys()
    assert queue.ledger().available() == capacity

    claimed = _claim_here(queue, capacity)
    assert claimed['action_key'] == cpu
    assert _denial(queue, gpu)['evidence']['decision']['reason'] == 'sample_invalid_or_stale'
    assert queue.ledger().holder_tokens(cpu) == {'cpu': 8, 'mem_gb': 4}
    assert queue.item_path(pool.READY, gpu).exists()
    queue.finish(cpu, status='executed', detail={})
    sample['complete'] = True
    assert _claim_here(queue, capacity)['action_key'] == gpu
    queue.finish(gpu, status='executed', detail={})
    assert queue.ledger().available() == capacity


class _TimestampSubclass(float):
    pass


@pytest.mark.parametrize('fault', ['stale', 'future', 'boolean', 'subclass',
                                 'unknown_foreign', 'foreign'])
def test_unknown_room_evidence_allows_the_later_cpu_row(gpu_rig, fault):
    queue, clock, sample, capacity, publish, tick, claim = gpu_rig
    gpu = publish(1)
    cpu = _publish_cpu(queue)
    if fault == 'stale':
        sample['sampled_unix'] = clock[0] - adaptive_gpu.MAX_SAMPLE_AGE_S - 1
    elif fault == 'future':
        sample['sampled_unix'] = clock[0] + 1
    elif fault == 'boolean':
        sample['sampled_unix'] = True
    elif fault == 'subclass':
        sample['sampled_unix'] = _TimestampSubclass(clock[0])
    elif fault == 'unknown_foreign':
        sample['foreign_processes'] = None
    else:
        sample['foreign_processes'] = [{'pid': 42}]
    with queue._transition_locked(gpu), ThreadPoolExecutor(max_workers=1) as executor:
        claimed = executor.submit(_claim_here, queue, capacity).result(timeout=5)
    busy = _denial(queue, gpu)
    assert busy['reason'] == 'transition_busy'
    assert 'gpu_room_kept' not in (busy.get('evidence') or {})
    assert claimed['action_key'] == cpu
    assert queue.item_path(pool.READY, gpu).exists()
    assert queue.ledger().held_keys() == [cpu]
    queue.finish(cpu, status='executed', detail={})
    assert queue.ledger().available() == capacity


@pytest.mark.parametrize('unknown,reason', [
    (False, 'gpu_memory_budget'),
    (True, 'gpu_memory_sample_invalid'),
])
def test_zero_memory_and_unknown_memory_have_distinct_refusals(gpu_rig, unknown, reason):
    queue, clock, sample, capacity, publish, tick, claim = gpu_rig
    device = sample['devices'][0]
    device.update(telemetry_class='memory_only', memory_domain='discrete',
                  memory_total_bytes=None if unknown else 0,
                  memory_free_bytes=0, memory_used_bytes=0)
    key = publish(1)
    assert claim() is None
    assert _denial(queue, key)['evidence']['decision']['reason'] == reason
    assert not queue.ledger().held_keys()
    assert queue.ledger().available() == capacity
    device.update(memory_total_bytes=4 * adaptive_gpu.GIB, memory_free_bytes=4 * adaptive_gpu.GIB)
    assert claim()['action_key'] == key
    assert queue.ledger().holder_tokens(key) == {'cpu': 1, 'mem_gb': 1, 'gpu': 1}
    queue.finish(key, status='executed', detail={})
    assert queue.ledger().available() == capacity


def test_an_invalid_header_does_not_evaluate_later_numeric_evidence(gpu_rig):
    queue, clock, sample, capacity, publish, tick, claim = gpu_rig
    sample.update(complete=False, host_total_bytes=10**1000)
    key = publish(1)
    assert claim() is None
    assert _denial(queue, key)['evidence']['decision']['reason'] == 'sample_invalid_or_stale'
    assert not queue.ledger().held_keys()
    assert queue.ledger().available() == capacity
