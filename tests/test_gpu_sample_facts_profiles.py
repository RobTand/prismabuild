"""Consumer policy differences on synthetic observations and real queue holds."""
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy

import pytest

from test_adaptive_gpu import _cpu_sample, gpu_rig
from test_memory_only_framebuffer_window import KEY, NONCE, SCOPE, _sample as _window_sample
from prismabuild import adaptive_cpu, adaptive_gpu, box_capacity, box_window, pool


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


def _claim_here(queue, capacity, *, cpu_sampler, gpu_sampler):
    return queue.claim(tags=[pool.socket.gethostname()], capacity=capacity,
                       cpu_tiers={'preferred': list(range(8)), 'fallback': []},
                       adaptive_cpu=True, has_gpu=True, cpu_sampler=cpu_sampler, gpu_sampler=gpu_sampler)


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
        assert executor.submit(_claim_here, queue, capacity,
            cpu_sampler=lambda: _cpu_sample(clock),
            gpu_sampler=sample.copy).result(timeout=5) is None
    busy = _denial(queue, gpu)
    assert busy['reason'] == 'transition_busy'
    assert busy['evidence']['gpu_room_kept'] == {'cpu': 1, 'mem_gb': 1, 'gpu': 1}
    assert busy['evidence']['gpu_room_binds_all'] is True
    deferred = _denial(queue, cpu)
    assert deferred['reason'] == 'deferred_for_ready_gpu_row'
    assert deferred['evidence']['gpu_row'] == gpu[:12]
    assert not queue.ledger().held_keys()
    assert queue.ledger().available() == capacity

    claimed = _claim_here(queue, capacity, cpu_sampler=lambda: _cpu_sample(clock), gpu_sampler=sample.copy)
    assert claimed['action_key'] == cpu
    assert _denial(queue, gpu)['evidence']['decision']['reason'] == 'sample_invalid_or_stale'
    assert queue.ledger().holder_tokens(cpu) == {'cpu': 8, 'mem_gb': 4}
    assert queue.item_path(pool.READY, gpu).exists()
    queue.finish(cpu, status='executed', detail={})
    sample['complete'] = True
    assert _claim_here(queue, capacity,
        cpu_sampler=lambda: _cpu_sample(clock),
        gpu_sampler=sample.copy)['action_key'] == gpu
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
        claimed = executor.submit(_claim_here, queue, capacity,
            cpu_sampler=lambda: _cpu_sample(clock),
            gpu_sampler=sample.copy).result(timeout=5)
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


def _reporting_sample(**kwargs):
    sample = _window_sample(**kwargs)
    sample.update(host_total_bytes=128 * adaptive_gpu.GIB,
                  host_available_bytes=100 * adaptive_gpu.GIB,
                  memory_pressure_some=0., memory_pressure_full=0.,
                  cpu_pressure_some=0., cpu_pressure_full=0., foreign_processes=[])
    return sample


def _reporting_offer(sample, *, now=101.):
    return box_capacity.observe({'gpu': 2}, {}, gpu_sample=sample, now=now,
                                mem_gb=100, load1=0)


@pytest.mark.parametrize('faults,reason', [
    ({'schema': 'unknown', 'sampled_unix': 'unknown', 'devices': []},
     'incomplete or unattributed GPU capacity snapshot'),
    ({'sampled_unix': 'unknown', 'devices': [], 'host_total_bytes': -1},
     'stale or future GPU capacity snapshot'),
    ({'devices': [], 'host_total_bytes': -1, 'memory_pressure_some': -1},
     'malformed GPU device or attribution inventory'),
    ({'host_total_bytes': -1, 'memory_pressure_some': -1, 'devices': [None]},
     'invalid host memory bounds in GPU snapshot'),
    ({'memory_pressure_some': -1, 'cpu_pressure_full': -1, 'devices': [None]},
     'invalid memory_pressure_some in GPU snapshot'),
])
def test_offer_reports_the_first_failure_when_multiple_evidence_stages_are_unknown(faults, reason):
    sample = _reporting_sample(sampled_unix=100.)
    sample.update(faults)
    observed = _reporting_offer(sample)
    assert observed.capacity['gpu'] == 0
    assert observed.detail['gpu_capacity_trusted'] is False
    assert observed.detail['gpu_capacity_error'] == reason
    assert box_capacity.physical_gpu_count(sample, now=101.) == 0


def test_offer_age_uses_the_later_gpu_clock_after_the_public_observation_clock(monkeypatch):
    sample = _reporting_sample(sampled_unix=100.)
    readings = iter([101., 107.])
    monkeypatch.setattr(box_capacity.time, 'time', lambda: next(readings))
    observed = _reporting_offer(sample, now=None)
    assert observed.detail['observed_unix'] == 101.
    assert observed.capacity['gpu'] == 0
    assert observed.detail['gpu_capacity_error'] == 'stale or future GPU capacity snapshot'
    assert box_capacity.physical_gpu_count(sample, now=101.) == 1


def test_physical_count_observes_its_clock_after_schema_but_even_with_an_invalid_timestamp():
    sample = _reporting_sample(sampled_unix=100.)
    sample.update(schema='unknown', sampled_unix='unknown')
    assert box_capacity.physical_gpu_count(sample, now='unknown') == 0
    sample['schema'] = box_capacity.GPU_CAPACITY_SCHEMA
    with pytest.raises(ValueError):
        box_capacity.physical_gpu_count(sample, now='unknown')


def test_a_multi_device_offer_does_not_turn_a_window_into_a_multi_device_measurement():
    sample = _reporting_sample(sampled_unix=100.)
    second = deepcopy(sample['devices'][0])
    second['uuid'] = 'GPU-second'
    sample['devices'].append(second)
    assert box_capacity.physical_gpu_count(sample, now=101.) == 2
    observed = _reporting_offer(sample)
    assert observed.capacity['gpu'] == 2
    assert observed.detail['gpu_memory_total_bytes'] == 32 * adaptive_gpu.GIB
    window = box_window.DiscreteFramebufferWindow(KEY, NONCE, SCOPE)
    assert not window.observe(sample, now=101.)
    assert window.group() is None


class _ByteSubclass(int):
    pass


@pytest.mark.parametrize('change', ['timestamp_string', 'bytes_string', 'bytes_float',
                                  'bytes_subclass', 'device_clock_bool', 'counter_gap'])
def test_offer_coercion_does_not_replace_the_window_profile_or_spend_a_rejected_sample(change):
    window = box_window.DiscreteFramebufferWindow(KEY, NONCE, SCOPE)
    assert window.observe(_reporting_sample(sampled_unix=100.), now=100.)
    before = window.group()
    sample = _reporting_sample(sample_id='later', sampled_unix=101., used=6 * adaptive_gpu.GIB)
    device = sample['devices'][0]
    if change == 'timestamp_string':
        sample['sampled_unix'] = '101.0'
    elif change == 'bytes_string':
        device['memory_used_bytes'] = str(device['memory_used_bytes'])
    elif change == 'bytes_float':
        device['memory_used_bytes'] = float(device['memory_used_bytes'])
    elif change == 'bytes_subclass':
        device['memory_used_bytes'] = _ByteSubclass(device['memory_used_bytes'])
    elif change == 'device_clock_bool':
        device['sampled_unix'] = True
    else:
        device['memory_free_bytes'] -= 1
    assert box_capacity.physical_gpu_count(sample, now=101.) == 1
    assert _reporting_offer(sample).capacity['gpu'] == 1
    assert not window.observe(sample, now=101.)
    assert window.group() == before
    corrected = _reporting_sample(sample_id='later', sampled_unix=101., used=6 * adaptive_gpu.GIB)
    assert window.observe(corrected, now=101.)
    assert window.group()['samples'] == 2
    assert window.group()['framebuffer_used_bytes_peak'] == 6 * adaptive_gpu.GIB


def test_whole_device_reporting_keeps_incomplete_global_attribution_out_of_offer_authority():
    sample = _reporting_sample(sampled_unix=100.)
    sample.update(complete=False, attributed=False, foreign_processes=[{'pid': 42}])
    window = box_window.DiscreteFramebufferWindow(KEY, NONCE, SCOPE)
    assert window.observe(sample, now=101.)
    assert window.group()['framebuffer_used_bytes_peak'] == 4 * adaptive_gpu.GIB
    observed = _reporting_offer(sample)
    assert observed.capacity['gpu'] == 0
    assert observed.detail['gpu_capacity_error'] == 'incomplete or unattributed GPU capacity snapshot'
    sample.update(complete=True, attributed=True)
    assert _reporting_offer(sample).foreign == {'gpu': 2}
    sample['foreign_processes'] = []
    assert _reporting_offer(sample).capacity['gpu'] == 1
