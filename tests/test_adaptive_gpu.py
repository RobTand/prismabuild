"""GPU concurrency follows trusted device evidence inside fixed memory limits."""
from pathlib import Path
import sys
import time

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from prismabuild import adaptive_cpu, pool


@pytest.mark.parametrize('historical_slots', [2, 3])
def test_cold_start_is_one_job_independent_of_historical_slots(tmp_path, monkeypatch, historical_slots):
    from prismabuild import adaptive_gpu
    monkeypatch.setattr(adaptive_gpu.Controller, 'sample', lambda self: {
        'schema':'prismabuild.gpu_capacity.v1', 'sample_id':'cold',
        'sampled_unix':time.time(), 'complete':True, 'attributed':True,
        'devices':[{'uuid':'GPU-1','power_w':5,'power_limit_w':140,
                    'memory_domain':'shared_system','limited':False}],
        'host_total_bytes':128*1024**3,'host_available_bytes':100*1024**3,
        'memory_pressure_some':0,'memory_pressure_full':0,'cpu_pressure_some':0,
        'foreign_processes':[],'jobs':[]})
    monkeypatch.setattr(adaptive_cpu, 'action_identity', lambda item: ('shape', False))
    monkeypatch.setattr(adaptive_cpu.Controller, 'sample', lambda self: {
        'sampled_unix': time.time(), 'busy_cpus': 0., 'psi_some': 0.,
        'cpu_count': 4, 'interval_s': 1.})
    queue = pool.PoolQueue(tmp_path / 'queue')
    for index in range(2):
        queue.publish(action_key=f'{index:064x}', cas_root=str(tmp_path / 'cas'),
                      checkout_root=str(tmp_path), worker_script='worker.py',
                      resources={'cpu': 1, 'mem_gb': 1, 'gpu': 1}, needs_gpu=True)
    args = dict(capacity={'cpu': 4, 'mem_gb': 4, 'gpu': historical_slots},
                cpu_tiers={'preferred': [0, 1, 2, 3], 'fallback': []},
                adaptive_cpu=True, has_gpu=True)
    assert queue.claim(**args)
    assert queue.claim(**args) is None
    assert queue.ledger().held()['mem_gb'] == 1


@pytest.fixture
def gpu_rig(tmp_path, monkeypatch):
    from prismabuild import adaptive_gpu
    clock = [100.]
    monkeypatch.setattr(time, 'time', lambda: clock[0])
    monkeypatch.setattr(adaptive_cpu, 'action_identity', lambda item: ('shape', False))
    monkeypatch.setattr(adaptive_gpu, 'action_contract', lambda item, demand:
                        ('shape', False, False, demand['mem_gb'] * adaptive_gpu.GIB))
    monkeypatch.setattr(adaptive_cpu.Controller, 'sample', lambda self: {
        'sampled_unix': clock[0], 'busy_cpus': 0., 'psi_some': 0.,
        'cpu_count': 8, 'interval_s': 1.})
    sample = {'schema': 'prismabuild.gpu_capacity.v1', 'sample_id': '100',
              'sampled_unix': 100., 'complete': True, 'attributed': True,
              'devices': [{'uuid': 'GPU-1', 'power_w': 15., 'power_limit_w': None,
                           'power_reference_w': 140., 'power_reference_scope': 'soc_tdp',
                           'memory_domain': 'shared_system', 'limited': False}],
              'host_total_bytes': 128 * adaptive_gpu.GIB,
              'host_available_bytes': 100 * adaptive_gpu.GIB,
              'memory_pressure_some': 0., 'memory_pressure_full': 0.,
              'cpu_pressure_some': 0., 'foreign_processes': [], 'jobs': []}
    monkeypatch.setattr(adaptive_gpu.Controller, 'sample', lambda self: sample.copy())
    queue = pool.PoolQueue(tmp_path / 'queue')
    capacity = {'cpu': 8, 'mem_gb': 4, 'gpu': 1}
    args = dict(capacity=capacity, cpu_tiers={'preferred': list(range(8)), 'fallback': []},
                adaptive_cpu=True, has_gpu=True)
    def publish(index, memory=1):
        key = f'{index:064x}'
        queue.publish(action_key=key, cas_root=str(tmp_path / 'cas'),
                      checkout_root=str(tmp_path), worker_script='worker.py',
                      resources={'cpu': 1, 'mem_gb': memory, 'gpu': 1}, needs_gpu=True)
        return key
    def tick(seconds=2):
        clock[0] += seconds
        sample.update(sampled_unix=clock[0], sample_id=str(clock[0]))
        sample['jobs'] = []
        for key in queue.ledger().held_keys():
            record = {'action_key': key, 'nonce': key + '-attempt', 'scope_unit': key + '-scope',
                      'sampled_unix': clock[0], 'cpu_seconds': .01 * (clock[0] - 100),
                      'wall_seconds': clock[0] - 100, 'complete': True}
            adaptive_cpu.write_json(queue.ledger().base / 'telemetry' / f'{key}.json', record)
            sample['jobs'].append({'action_key': key, 'nonce': record['nonce'],
                                   'scope_id': record['scope_unit'], 'complete': True})
    def claim():
        return queue.claim(**args)
    return queue, clock, sample, capacity, publish, tick, claim


def test_probes_exceed_old_slots_without_minting_or_discounts(gpu_rig):
    queue, clock, sample, capacity, publish, tick, claim = gpu_rig
    for index in range(5):
        publish(index)
    admitted = [claim()]
    assert admitted[0]
    for index in range(3):
        if index:
            sample['devices'][0]['power_w'] += 5
            for _ in range(2):
                tick()
                assert claim() is None  # collect the probe response first
        tick()
        admitted.append(claim())
        assert admitted[-1]
        assert claim() is None  # a sample funds at most one probe
    tick()
    assert claim() is None  # the unchanged four-GiB ledger is full
    assert queue.ledger().held()['mem_gb'] == 4
    assert queue.ledger().capacity() == capacity
    assert admitted[-1]['gpu_admission']['borrowed_gpu'] == 1
    for item in admitted:
        queue.finish(item['action_key'], status='executed', detail={})
    assert queue.ledger().available() == capacity
    assert not queue.ledger().held_keys()
    assert not (queue.ledger().free_dir / '.gpu.json').exists()


@pytest.mark.parametrize('fault', ['stale', 'incomplete', 'attribution', 'foreign', 'power',
                                  'thermal', 'memory_some', 'memory_full', 'cpu_pressure',
                                  'memory_low', 'nonce', 'scope', 'job_missing', 'job_incomplete',
                                  'domain_unknown', 'multi_device'])
def test_pressure_and_unknown_evidence_stop_new_work_without_touching_holders(gpu_rig, fault):
    queue, clock, sample, capacity, publish, tick, claim = gpu_rig
    key = publish(1)
    assert claim()
    publish(2)
    tick()
    if fault == 'stale': sample['sampled_unix'] -= 10
    elif fault == 'incomplete': sample['complete'] = False
    elif fault == 'attribution': sample['attributed'] = False
    elif fault == 'foreign': sample['foreign_processes'] = [{'pid': 9000}]
    elif fault == 'power': sample['devices'][0]['power_w'] = 125
    elif fault == 'thermal': sample['devices'][0]['limited'] = True
    elif fault == 'memory_some': sample['memory_pressure_some'] = 1
    elif fault == 'memory_full': sample['memory_pressure_full'] = .1
    elif fault == 'cpu_pressure': sample['cpu_pressure_some'] = 10
    elif fault == 'memory_low': sample['host_available_bytes'] = 2 * 1024**3
    elif fault == 'nonce': sample['jobs'][0]['nonce'] = 'old-attempt'
    elif fault == 'scope': sample['jobs'][0]['scope_id'] = 'other-scope'
    elif fault == 'job_missing': sample['jobs'] = []
    elif fault == 'job_incomplete': sample['jobs'][0]['complete'] = False
    elif fault == 'domain_unknown': sample['devices'][0]['memory_domain'] = 'unknown'
    elif fault == 'multi_device': sample['devices'] *= 2
    assert claim() is None
    assert queue.item_path(pool.CLAIMED, key).exists()
    assert queue.ledger().held()['mem_gb'] == 1


def test_power_recovery_needs_two_low_samples(gpu_rig):
    queue, clock, sample, capacity, publish, tick, claim = gpu_rig
    publish(1); assert claim()
    publish(2); tick()
    sample['devices'][0]['power_w'] = 130
    assert claim() is None
    sample['devices'][0]['power_w'] = 15
    tick(); assert claim() is None
    tick(); assert claim()


def test_idle_gpu_utilization_percentage_is_ignored(gpu_rig):
    queue, clock, sample, capacity, publish, tick, claim = gpu_rig
    publish(1); assert claim()
    publish(2); tick()
    sample['devices'][0]['utilization_gpu_percent'] = 100
    assert claim()


def test_probe_startup_cannot_be_hidden_by_a_new_sample(gpu_rig):
    queue, clock, sample, capacity, publish, tick, claim = gpu_rig
    for index in range(3): publish(index)
    assert claim(); tick(); assert claim()
    tick(.5)
    assert claim() is None
    sample['devices'][0]['power_w'] += 5
    tick(2); assert claim() is None
    tick(1); assert claim() is None
    tick(1); assert claim()


def test_probe_credit_persists_across_controller_restart_and_release(gpu_rig):
    queue, clock, sample, capacity, publish, tick, claim = gpu_rig
    for index in range(3): publish(index)
    assert claim(); tick(); second = claim(); assert second
    queue.finish(second['action_key'], status='executed', detail={})
    assert claim() is None
    tick(); assert claim()


@pytest.mark.parametrize('fault', ['memory_total_bytes', 'memory_free_bytes', 'reserved', 'free', 'ram'])
def test_discrete_vram_and_ram_are_independent_hard_admission_budgets(gpu_rig, fault):
    queue, clock, sample, capacity, publish, tick, claim = gpu_rig
    device = sample['devices'][0]
    device.update(memory_domain='discrete', memory_total_bytes=2*1024**3,
                  memory_free_bytes=2*1024**3, memory_used_bytes=0)
    publish(1); assert claim(); publish(2); tick()
    if fault in ('memory_total_bytes', 'memory_free_bytes'): device[fault] = None
    elif fault == 'reserved': device['memory_total_bytes'] = int(1.5*1024**3)
    elif fault == 'free': device.update(memory_free_bytes=0, memory_used_bytes=2*1024**3)
    elif fault == 'ram': sample['host_available_bytes'] = 2*1024**3
    assert claim() is None
    assert queue.ledger().held()['mem_gb'] == 1


def test_discrete_vram_reservations_release_with_attempt(gpu_rig):
    queue, clock, sample, capacity, publish, tick, claim = gpu_rig
    sample['devices'][0].update(memory_domain='discrete', memory_total_bytes=2*1024**3,
                                memory_free_bytes=2*1024**3, memory_used_bytes=0)
    for index in range(3): publish(index)
    first=claim(); assert first; tick(); assert claim(); tick()
    assert claim() is None
    queue.finish(first['action_key'], status='executed', detail={})
    tick(); assert claim()


@pytest.mark.parametrize('measurement,exclusive', [(True, False), (False, True)])
def test_measurement_and_exclusive_requests_do_not_overlap(gpu_rig, monkeypatch, measurement, exclusive):
    from prismabuild import adaptive_gpu
    queue, clock, sample, capacity, publish, tick, claim = gpu_rig
    publish(1); assert claim(); publish(2); tick()
    monkeypatch.setattr(adaptive_gpu, 'action_contract', lambda item, demand:
                        ('shape', measurement, exclusive, 1024**3))
    assert claim() is None
    queue.finish(f'{1:064x}', status='executed', detail={})
    tick(); isolated = claim(); assert isolated
    assert isolated['gpu_admission']['measurement'] == measurement
    publish(3); tick()
    monkeypatch.setattr(adaptive_gpu, 'action_contract', lambda item, demand:
                        ('shape', False, False, 1024**3))
    assert claim() is None


def test_legacy_multi_slot_demand_reserves_one_physical_device_exclusively(gpu_rig, monkeypatch):
    from prismabuild import adaptive_gpu
    queue, clock, sample, capacity, publish, tick, claim = gpu_rig
    key=publish(1)
    path=queue.item_path(pool.READY, key)
    item=adaptive_cpu.read_json(path)
    item['resources']['gpu']=3
    adaptive_cpu.write_json(path, item)
    monkeypatch.setattr(adaptive_gpu, 'action_contract', lambda item, demand:
                        ('shape', False, demand['gpu'] > 1, 1024**3))
    first=claim(); assert first
    assert first['resources']['gpu'] == 3
    assert first['gpu_admission']['declared_gpu'] == 3
    assert first['gpu_admission']['exclusive'] is True
    assert queue.ledger().held()['gpu'] == 1
    publish(2); tick(); assert claim() is None
    queue.finish(key, status='executed', detail={})
    assert queue.ledger().available() == capacity


def test_abandoned_probe_does_not_return_virtual_tokens(gpu_rig):
    from prismabuild import adaptive_gpu
    queue, clock, sample, capacity, publish, tick, claim = gpu_rig
    publish(1); assert claim(); tick()
    controller=adaptive_gpu.Controller(queue.ledger())
    item={'action_key': 'f'*64, 'cas_root': '/unused'}
    decision=controller.decision(item, {'gpu':1,'mem_gb':1})
    assert decision
    controller.reserve_probe(decision)
    handle=queue.ledger().begin_acquire('f'*64, {'gpu':1,'mem_gb':1}, adaptive_gpu=decision)
    assert handle
    queue.ledger().abandon_acquire(handle)
    assert queue.ledger().capacity() == capacity
    assert not (queue.ledger().free_dir / '.gpu.json').exists()
    assert adaptive_gpu.Controller(queue.ledger()).decision(item, {'gpu':1,'mem_gb':1}) is None


def test_action_contract_seals_exclusivity_and_memory_budget(tmp_path):
    from prismabuild import adaptive_gpu, core
    from test_core import _body
    (tmp_path/'task_code.py').write_text('print(1)\n')
    body=_body(tmp_path)
    demand={'gpu':1,'mem_gb':4,'cpu':1}
    body['params']={'demand':demand}
    cas=core.PrismaBuildCAS(tmp_path/'cas')
    def contract():
        action=core.seal_action(body)
        cas.publish_action_request(action)
        return adaptive_gpu.action_contract({'action_key':action['action_key'],
                                             'cas_root':str(cas.root),'resources':demand}, demand)
    assert contract()[2:] == (True, 4*1024**3) # legacy ambiguity is exclusive
    body['params']['gpu_exclusive']=False
    assert contract()[2:] == (False, 4*1024**3)
    body['params']['gpu_memory_gb']=.5
    assert contract()[2:] == (False, 512*1024**2)
    body['params']['gpu_exclusive']=True
    assert contract()[2:] == (True, 512*1024**2)


@pytest.mark.parametrize('fault', ['absent','stale','unknown','unified_alias','incomplete','discrete_missing'])
def test_even_cold_start_needs_trusted_memory_and_device_evidence(gpu_rig, fault):
    queue, clock, sample, capacity, publish, tick, claim = gpu_rig
    publish(1)
    if fault == 'absent': sample.clear()
    elif fault == 'stale': sample['sampled_unix'] -= 10
    elif fault == 'unknown': sample['devices'][0]['memory_domain'] = 'unknown'
    elif fault == 'unified_alias': sample['devices'][0]['memory_domain'] = 'unified'
    elif fault == 'incomplete': sample['complete'] = False
    elif fault == 'discrete_missing': sample['devices'][0]['memory_domain'] = 'discrete'
    assert claim() is None
    assert not queue.ledger().held()


def test_concurrent_claimants_share_one_probe_credit(gpu_rig):
    from concurrent.futures import ThreadPoolExecutor
    queue, clock, sample, capacity, publish, tick, claim = gpu_rig
    for index in range(4): publish(index)
    assert claim(); tick()
    with ThreadPoolExecutor(max_workers=4) as threads:
        claims=list(threads.map(lambda _: claim(), range(4)))
    assert sum(item is not None for item in claims) == 1
    assert queue.ledger().held()['mem_gb'] == 2


def test_power_plateau_closes_below_soc_fraction_and_survives_departure(gpu_rig):
    """Extra contexts at an 81 W plateau must not keep spending a 140 W TDP."""
    from prismabuild import adaptive_gpu
    queue, clock, sample, capacity, publish, tick, claim = gpu_rig
    for index in range(4): publish(index)
    sample['devices'][0]['power_w'] = 81.
    first = claim(); assert first
    tick(); second = claim(); assert second
    for watts in (80.5, 81., 81.5):
        sample['devices'][0]['power_w'] = watts
        tick(); assert claim() is None
    state = adaptive_cpu.read_json(queue.ledger().base / 'adaptive/gpu-state.json')
    assert state['power_feedback']['status'] == 'plateau'
    # A fresh Controller instance and a holder exit must not forget saturation.
    queue.finish(first['action_key'], status='executed', detail={})
    sample['devices'][0]['power_w'] = 81.
    for _ in range(3):
        tick(); assert claim() is None
    assert len(queue.ledger().held_keys()) == 1
    # The remaining workload's sustained activity drop permits exploration.
    sample['devices'][0]['power_w'] = 40.
    resumed = None
    for _ in range(6):
        tick()
        resumed = claim()
        if resumed: break
    assert resumed
    assert resumed['gpu_admission']['probe'] is True


@pytest.mark.parametrize('holder_exits', [False, True])
def test_startup_plateau_is_invalidated_by_sustained_activity_rise(gpu_rig, holder_exits):
    """A startup plateau cannot govern a later active phase indefinitely."""
    queue, clock, sample, capacity, publish, tick, claim = gpu_rig
    for index in range(4): publish(index)
    sample['devices'][0]['power_w'] = 8.
    first = claim(); assert first
    tick(); second = claim(); assert second
    for _ in range(3):
        tick(); assert claim() is None
    state = adaptive_cpu.read_json(queue.ledger().base / 'adaptive/gpu-state.json')
    assert state['power_feedback']['status'] == 'plateau'
    if holder_exits:
        queue.finish(second['action_key'], status='executed', detail={})
    sample['devices'][0]['power_w'] = 60.
    tick(); assert claim() is None  # One changed sample is not a phase.
    resumed = None
    for _ in range(5):
        tick()
        resumed = claim()
        if resumed: break
    assert resumed, 'sustained activity rise must invalidate the startup plateau'
    assert resumed['gpu_admission']['probe'] is True


def test_power_plateau_is_cleared_when_the_gpu_busy_period_ends(gpu_rig):
    queue, clock, sample, capacity, publish, tick, claim = gpu_rig
    for index in range(3): publish(index)
    first=claim(); assert first; tick(); second=claim(); assert second
    for _ in range(3):
        tick(); assert claim() is None
    queue.finish(first['action_key'], status='executed', detail={})
    queue.finish(second['action_key'], status='executed', detail={})
    tick()
    cold=claim(); assert cold
    assert cold['gpu_admission']['probe'] is False


def test_noisy_power_response_does_not_authorize_an_unbounded_probe(gpu_rig):
    queue, clock, sample, capacity, publish, tick, claim = gpu_rig
    for index in range(3): publish(index)
    sample['devices'][0]['power_w']=50
    assert claim(); tick(); assert claim()
    # Mean power rises slightly, but noise is much larger than the response.
    for watts in (42, 60, 55):
        sample['devices'][0]['power_w']=watts
        tick(); assert claim() is None
    state=adaptive_cpu.read_json(queue.ledger().base/'adaptive/gpu-state.json')
    assert state['power_feedback']['status']=='plateau'


def test_a_telemetry_gap_cannot_reuse_old_plateau_recovery_samples(gpu_rig):
    queue, clock, sample, capacity, publish, tick, claim = gpu_rig
    for index in range(3): publish(index)
    sample['devices'][0]['power_w']=80
    assert claim(); tick(); assert claim()
    for _ in range(3):
        tick(); assert claim() is None
    sample['devices'][0]['power_w']=40
    tick(10); assert claim() is None
    tick(1); assert claim() is None
    tick(1); assert claim()


@pytest.mark.parametrize('budget', [1e300, 1e-12, 2**33])
def test_invalid_sealed_gpu_budget_cannot_overflow_or_enable_sharing(tmp_path, budget):
    from prismabuild import adaptive_gpu, core
    from test_core import _body
    (tmp_path / 'task_code.py').write_text('print(1)\n')
    body = _body(tmp_path)
    demand = {'gpu': 1, 'mem_gb': 4, 'cpu': 1}
    body['params'] = {'demand': demand, 'gpu_exclusive': False, 'gpu_memory_gb': budget}
    action = core.seal_action(body)
    cas = core.PrismaBuildCAS(tmp_path / 'cas')
    cas.publish_action_request(action)
    contract = adaptive_gpu.action_contract(
        {'action_key': action['action_key'], 'cas_root': str(cas.root), 'resources': demand}, demand)
    assert contract[0] is None
    assert contract[2:] == (True, 4 * 1024**3)


@pytest.mark.parametrize('budget,expected', [(2**-30, 1), (.5, 512*1024**2),
                                           (2**33 - 2**-20, 2**63 - 1024)])
def test_gpu_budget_byte_boundaries_remain_representable(budget, expected):
    from prismabuild import adaptive_gpu
    assert adaptive_gpu.memory_budget_bytes(budget) == expected
