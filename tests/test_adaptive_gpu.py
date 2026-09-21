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
              'devices': [{'uuid': 'GPU-1', 'name': 'NVIDIA GB10', 'power_w': 15.,
                           'power_limit_w': None,
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
    def publish(index, memory=1, cpu=1):
        key = f'{index:064x}'
        queue.publish(action_key=key, cas_root=str(tmp_path / 'cas'),
                      checkout_root=str(tmp_path), worker_script='worker.py',
                      resources={'cpu': cpu, 'mem_gb': memory, 'gpu': 1}, needs_gpu=True)
        return key
    def tick(seconds=2):
        clock[0] += seconds
        sample.update(sampled_unix=clock[0], sample_id=str(clock[0]))
        sample['jobs'] = []
        for key in queue.ledger().held_keys():
            record = {'action_key': key, 'nonce': key + '-attempt', 'scope_unit': key + '-scope',
                      'sampled_unix': clock[0], 'cpu_seconds': .01 * (clock[0] - 100),
                      'wall_seconds': clock[0] - 100, 'complete': True}
            adaptive_cpu.write_json(adaptive_cpu.local_telemetry_path(queue.ledger().base, key), record)
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


def test_memory_refusal_leaves_gpu_sample_for_smaller_candidate(gpu_rig):
    queue, clock, sample, capacity, publish, tick, claim = gpu_rig
    holder = publish(1)
    assert claim()['action_key'] == holder
    too_large = publish(2, memory=4)  # Only three GiB remain reserved-free.
    smaller = publish(3)
    tick()
    admitted = claim()
    assert admitted is not None, 'memory-refused candidate spent the GPU sample'
    assert admitted['action_key'] == smaller
    assert queue.item_path(pool.READY, too_large).exists()
    assert queue.ledger().held()['mem_gb'] == 2
    assert set(queue.ledger().held_keys()) == {holder, smaller}


def test_fallback_deferral_leaves_gpu_sample_for_preferred_candidate(gpu_rig):
    queue, clock, sample, capacity, publish, tick, _ = gpu_rig
    tiers = {'preferred': [0, 1], 'fallback': list(range(2, 8))}
    def claim():
        return queue.claim(capacity=capacity, cpu_tiers=tiers,
                           adaptive_cpu=True, has_gpu=True)
    holder = publish(1)
    assert claim()['action_key'] == holder
    remote_tiers = {'preferred': list(range(8)), 'fallback': []}
    remote = queue.ledger('another-host')
    remote.configure_cpu_tiers(remote_tiers)
    remote.ensure_capacity(capacity)
    queue.announce(host='another-host', tags=[], has_gpu=True, capacity=capacity,
                   cpu_tiers=remote_tiers)
    deferred = publish(2, cpu=2)
    smaller = publish(3)
    tick()
    admitted = claim()
    assert queue._cpu_deferrals, 'the real fallback deferral must run'
    assert admitted is not None, 'fallback deferral spent the next candidate GPU sample'
    assert admitted['action_key'] == smaller
    assert admitted['cpu_allocation'] == {'preferred': [1], 'fallback': []}
    assert queue.item_path(pool.READY, deferred).exists()
    assert set(queue.ledger().held_keys()) == {holder, smaller}
    assert queue.ledger().held()['mem_gb'] == 2
    assert claim() is None  # Successful admission still spends this sample.


@pytest.mark.parametrize('fault', ['rename_lost', 'demand_changed', 'reservation_swept'])
def test_ordinary_abandonment_leaves_gpu_sample_for_next_candidate(gpu_rig, monkeypatch, fault):
    from prismabuild import adaptive_gpu
    queue, clock, sample, capacity, publish, tick, claim = gpu_rig
    holder = publish(1)
    first = claim()
    abandoned = publish(2)
    successor = publish(3)
    tick()
    reached = []
    intent = queue._write_claim_intent
    commit = pool.ResourceLedger.commit_acquire

    def change_before_rename(key, **kwargs):
        intent(key, **kwargs)
        if key != abandoned:
            return
        reached.append(fault)
        path = queue.item_path(pool.READY, key)
        if fault == 'rename_lost':
            path.unlink()
        elif fault == 'demand_changed':
            item = adaptive_cpu.read_json(path)
            item['resources']['cpu'] = 2
            adaptive_cpu.write_json(path, item)

    def sweep_metadata(ledger, key, handle):
        filed = commit(ledger, key, handle)
        if key == abandoned:
            reached.append(fault)
            (ledger.held_dir / key / adaptive_gpu.METADATA).unlink()
        return filed

    if fault == 'reservation_swept':
        monkeypatch.setattr(pool.ResourceLedger, 'commit_acquire', sweep_metadata)
    else:
        monkeypatch.setattr(queue, '_write_claim_intent', change_before_rename)
    admitted = claim()
    assert reached == [fault]
    assert admitted is not None, f'{fault} spent the next candidate GPU sample'
    assert admitted['action_key'] == successor
    assert set(queue.ledger().held_keys()) == {holder, successor}
    assert queue.ledger().held()['mem_gb'] == 2
    assert queue.ledger().capacity() == capacity
    assert adaptive_cpu.read_json(queue.item_path(pool.CLAIMED, holder)) == first
    assert not queue.item_path(pool.CLAIMED, abandoned).exists()


@pytest.fixture
def unlaunched_probe(gpu_rig):
    from prismabuild import adaptive_gpu
    queue, clock, sample, capacity, publish, tick, claim = gpu_rig
    publish(1)
    assert claim()
    candidate = publish(2)
    tick()
    cpu = adaptive_cpu.Controller(queue.ledger(),
                                  {'preferred': list(range(8)), 'fallback': []})
    gpu = adaptive_gpu.Controller(queue.ledger(), publisher=cpu)

    def reserve():
        # Model the interval after ordinary reservation abandonment: no payload
        # launched, the existing holder is untouched, only sample credit remains.
        gpu._sample = None
        with cpu.locked():
            metadata = gpu.decision(adaptive_cpu.read_json(queue.item_path(pool.READY, candidate)),
                                    {'gpu': 1, 'cpu': 1, 'mem_gb': 1})
            assert metadata
            handle = queue.ledger().begin_acquire(candidate, {'gpu': 1, 'mem_gb': 1},
                                                  adaptive_gpu=metadata)
            assert handle
            ticket = gpu.reserve_probe(metadata)
        queue.ledger().abandon_acquire(handle)
        return ticket

    return cpu, gpu, reserve, claim, tick


@pytest.mark.parametrize('same_sample', [False, True])
def test_gpu_refund_cannot_return_an_intervening_probe(unlaunched_probe, same_sample):
    from copy import deepcopy
    cpu, gpu, reserve, claim, tick = unlaunched_probe
    first = reserve()
    if same_sample:
        # Retain a stale copy too: nonce identity must protect same-sample reuse,
        # independently of the in-process ticket being consumed only once.
        stale = deepcopy(first)
        with cpu.locked():
            gpu.return_probe(first)
        first = stale
    else:
        tick()
    second = reserve()
    assert second['probe_id'] != first['probe_id']
    before = adaptive_cpu.read_json(gpu.base / 'gpu-state.json')
    with cpu.locked():
        gpu.return_probe(first)
    assert adaptive_cpu.read_json(gpu.base / 'gpu-state.json') == before
    assert claim() is None, 'an older refund returned the intervening probe'


def test_gpu_refund_preserves_intervening_observation(unlaunched_probe):
    cpu, gpu, reserve, claim, tick = unlaunched_probe
    ticket = reserve()
    tick()
    gpu._sample = None
    with cpu.locked():
        # A later decision updates power/low-load history and invalidates the
        # abandoned membership's feedback without consuming another probe.
        assert gpu.decision({'action_key': 'f' * 64}, {'gpu': 1, 'mem_gb': 1})
        observed = adaptive_cpu.read_json(gpu.base / 'gpu-state.json')
        assert 'power_feedback' not in observed
        gpu.return_probe(ticket)
    after = adaptive_cpu.read_json(gpu.base / 'gpu-state.json')
    for key in ('sample_id', 'sampled_unix', 'low_samples', 'power_window', 'power_members'):
        assert after[key] == observed[key]
    assert 'power_feedback' not in after
    assert claim()


@pytest.mark.parametrize('written', [False, True])
def test_gpu_refund_write_error_cannot_be_replayed(unlaunched_probe, monkeypatch, written):
    cpu, gpu, reserve, claim, tick = unlaunched_probe
    ticket = reserve()
    write = gpu._write_state

    def fail(state):
        if written:
            write(state)
        raise OSError('refund persistence unavailable')

    with monkeypatch.context() as patch:
        patch.setattr(gpu, '_write_state', fail)
        with cpu.locked(), pytest.raises(OSError, match='refund persistence unavailable'):
            gpu.return_probe(ticket)
    if written:
        newer = reserve()  # Reuses the returned sample.
        assert newer
    before = adaptive_cpu.read_json(gpu.base / 'gpu-state.json')
    with cpu.locked():
        gpu.return_probe(ticket)
    assert adaptive_cpu.read_json(gpu.base / 'gpu-state.json') == before
    assert claim() is None


def test_gpu_refund_busy_admission_keeps_sample_spent(unlaunched_probe, monkeypatch):
    from contextlib import contextmanager
    cpu, gpu, reserve, claim, tick = unlaunched_probe
    ticket = reserve()

    @contextmanager
    def busy():
        raise adaptive_cpu.AdmissionBusy('test contention')
        yield

    with monkeypatch.context() as patch:
        patch.setattr(cpu, 'locked', busy)
        pool.PoolQueue._return_gpu_probe(cpu, gpu, ticket)
    assert claim() is None


@pytest.mark.parametrize('written', [False, True])
def test_gpu_sample_write_failure_releases_provisional_reservation(gpu_rig, monkeypatch, written):
    from prismabuild import adaptive_gpu
    queue, clock, sample, capacity, publish, tick, claim = gpu_rig
    holder = publish(1)
    first = claim()
    candidate = publish(2)
    before = queue.ledger().available()
    tick()
    reserve = adaptive_gpu.Controller.reserve_probe

    def fail(controller, metadata):
        if written:
            reserve(controller, metadata)
        raise OSError('sample persistence unavailable')

    with monkeypatch.context() as patch:
        patch.setattr(adaptive_gpu.Controller, 'reserve_probe', fail)
        with pytest.raises(OSError, match='sample persistence unavailable'):
            claim()
    assert queue.ledger().available() == before
    assert set(queue.ledger().held_keys()) == {holder}
    assert queue.item_path(pool.READY, candidate).exists()
    assert not queue.item_path(pool.CLAIMED, candidate).exists()
    assert adaptive_cpu.read_json(queue.item_path(pool.CLAIMED, holder)) == first
    if written:
        assert claim() is None  # Uncertain writes never refund a spent sample.
    else:
        assert claim()['action_key'] == candidate


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
    """Extra contexts at a 60 W plateau must not keep spending the reference."""
    from prismabuild import adaptive_gpu
    queue, clock, sample, capacity, publish, tick, claim = gpu_rig
    for index in range(4): publish(index)
    sample['devices'][0]['power_w'] = 60.
    first = claim(); assert first
    tick(); second = claim(); assert second
    for watts in (59.5, 60., 60.5):
        sample['devices'][0]['power_w'] = watts
        tick(); assert claim() is None
    state = adaptive_cpu.read_json(adaptive_cpu.local_state_base(queue.ledger().base) / 'gpu-state.json')
    assert state['power_feedback']['status'] == 'plateau'
    # A fresh Controller instance and a holder exit must not forget saturation.
    queue.finish(first['action_key'], status='executed', detail={})
    sample['devices'][0]['power_w'] = 60.
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
    state = adaptive_cpu.read_json(adaptive_cpu.local_state_base(queue.ledger().base) / 'gpu-state.json')
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
    state=adaptive_cpu.read_json(adaptive_cpu.local_state_base(queue.ledger().base)/'gpu-state.json')
    assert state['power_feedback']['status']=='plateau'


def test_a_telemetry_gap_cannot_reuse_old_plateau_recovery_samples(gpu_rig):
    queue, clock, sample, capacity, publish, tick, claim = gpu_rig
    for index in range(3): publish(index)
    sample['devices'][0]['power_w']=60
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


def _memory_only(sample):
    """Turn the rig's GB10 sample into the AMD/WSL2 contract: no power at all."""
    sample['devices'][0] = {
        'uuid': 'GPU-9c30c352a59e5b7a', 'vendor': 'amd',
        'telemetry_class': 'memory_only', 'memory_domain': 'discrete',
        'memory_total_bytes': 16 * 1024**3, 'memory_free_bytes': 15 * 1024**3,
        'memory_used_bytes': 1024**3, 'power_w': None, 'power_limit_w': None,
        'power_reference_w': None, 'power_reference_scope': None,
        'sm_clock_mhz': None, 'max_sm_clock_mhz': 2400., 'limited': None,
    }
    return sample


def test_memory_only_device_runs_one_job_and_never_shares(gpu_rig):
    queue, clock, sample, capacity, publish, tick, claim = gpu_rig
    _memory_only(sample)
    for index in range(3):
        publish(index)

    first = claim()
    assert first
    assert first['gpu_admission']['memory_domain'] == 'discrete'
    # A power series is what authorizes a second concurrent job. Without one
    # there is no plateau to observe, so the device stays at one job however
    # long it is watched -- the ledger, not an inferred headroom, is the limit.
    for _ in range(4):
        tick()
        assert claim() is None

    queue.finish(first['action_key'], status='executed', detail={})
    tick()
    assert claim()


def test_memory_only_device_refuses_while_a_foreign_holder_is_present(gpu_rig):
    queue, clock, sample, capacity, publish, tick, claim = gpu_rig
    _memory_only(sample)
    publish(0)
    sample['foreign_processes'] = [{'pid': 4242, 'gpu_uuid': 'GPU-9c30c352a59e5b7a',
                                    'used_bytes': None}]
    tick()
    sample['foreign_processes'] = [{'pid': 4242, 'gpu_uuid': 'GPU-9c30c352a59e5b7a',
                                    'used_bytes': None}]
    assert claim() is None


def test_memory_only_device_refuses_when_its_free_vram_cannot_hold_the_budget(gpu_rig):
    queue, clock, sample, capacity, publish, tick, claim = gpu_rig
    _memory_only(sample)
    sample['devices'][0]['memory_free_bytes'] = 512 * 1024**2
    sample['devices'][0]['memory_used_bytes'] = 16 * 1024**3 - 512 * 1024**2
    publish(0, memory=1)
    tick()
    assert claim() is None


def test_absent_power_without_the_declaration_still_refuses(gpu_rig):
    queue, clock, sample, capacity, publish, tick, claim = gpu_rig
    _memory_only(sample)
    # The narrowing is keyed on the device's own declaration. A sample that
    # merely lost its power counters is the old unreadable-telemetry case and
    # must keep failing closed.
    del sample['devices'][0]['telemetry_class']
    publish(0)
    tick()
    assert claim() is None


def _sw_cap_idle_device(**overrides):
    """Sparklina Sep-20 capture: GB10 idle at 4.32 W, SW-cap mask 0x4."""
    device = {
        'uuid': 'GPU-b1eceeea-fec7-371e-2cf3-cd10f2e7b705',
        'name': 'NVIDIA GB10',
        'power_w': 4.32,
        'power_limit_w': None,
        'power_reference_w': 140.0,
        'power_reference_scope': 'soc_tdp',
        'memory_domain': 'shared_system',
        'limited': True,
        'sm_clock_mhz': 208.0,
        'max_sm_clock_mhz': 3003.0,
        'throttle_active_mask': 0x4,
        'throttle_reasons': {
            'gpu_idle': False,
            'hw_power_brake_slowdown': False,
            'hw_slowdown': False,
            'hw_thermal_slowdown': False,
            'sw_power_cap': True,
            'sw_thermal_slowdown': False,
            'sync_boost': False,
        },
    }
    device.update(overrides)
    return device


def _arm_sw_cap(sample, tick, **overrides):
    sample['devices'][0] = _sw_cap_idle_device(**overrides)
    tick()


def test_sw_cap_idle_admits_first_job_with_exception_recorded(gpu_rig):
    queue, clock, sample, capacity, publish, tick, claim = gpu_rig
    publish(0)
    _arm_sw_cap(sample, tick)
    admitted = claim()
    assert admitted, 'idle SW-capped GB10 must admit its first generation job'
    exception = admitted['gpu_admission'].get('sw_cap_idle_exception')
    assert exception and exception['exception_reason'] == 'sw_cap_idle_first_job'
    assert exception['clock_threshold_fraction'] == 0.10
    assert exception['power_gate_fraction'] == 0.65
    assert exception['clock_ratio'] == 208.0 / 3003.0
    # The SoC TDP stays on the device for display; the gate divides by the
    # declared GPU capacity fact.
    assert exception['power_ratio'] == 4.32 / 110.0
    assert exception['admission_reference_scope'] == 'declared_fallback'


def test_sw_cap_idle_second_job_still_needs_free_samples(gpu_rig):
    queue, clock, sample, capacity, publish, tick, claim = gpu_rig
    publish(0)
    _arm_sw_cap(sample, tick)
    assert claim()
    publish(1)
    tick()
    # Holders present: no sharing exception; the SW cap still congests.
    assert claim() is None


def test_sw_cap_idle_never_excepts_measurement(gpu_rig, monkeypatch):
    from prismabuild import adaptive_gpu
    queue, clock, sample, capacity, publish, tick, claim = gpu_rig
    monkeypatch.setattr(adaptive_gpu, 'action_contract', lambda item, demand:
                        ('shape', True, False, demand['mem_gb'] * adaptive_gpu.GIB))
    publish(0)
    _arm_sw_cap(sample, tick)
    assert claim() is None


def test_sw_cap_idle_no_exception_with_broker_jobs_or_foreign(gpu_rig):
    queue, clock, sample, capacity, publish, tick, claim = gpu_rig
    publish(0)
    _arm_sw_cap(sample, tick)
    sample['jobs'] = [{'action_key': 'f' * 64, 'nonce': 'g' * 32,
                       'scope_id': 's', 'complete': True}]
    assert claim() is None
    sample['jobs'] = []
    sample['foreign_processes'] = [{'pid': 4242}]
    tick()
    assert claim() is None


@pytest.mark.parametrize('field,value', [
    ('power_w', 100.0),
    ('sm_clock_mhz', 1500.0),
    ('sm_clock_mhz', None),
    ('max_sm_clock_mhz', None),
    ('throttle_reasons', None),
    ('throttle_active_mask', None),
    ('throttle_active_mask', 0x8),
    ('limited', False),
    ('name', 'NVIDIA H100'),
    ('memory_domain', 'discrete'),
    ('power_reference_scope', 'gpu_power_limit'),
])
def test_sw_cap_idle_negative_guards_refuse_first_job(gpu_rig, field, value):
    queue, clock, sample, capacity, publish, tick, claim = gpu_rig
    publish(0)
    override = {field: value}
    if field == 'limited' and value is False:
        # A free device admits via the normal path, not the exception; force
        # congestion through power so the exception gate is what is tested.
        override = {'limited': False, 'power_w': 130.0}
    _arm_sw_cap(sample, tick, **override)
    assert claim() is None


@pytest.mark.parametrize('reason', [
    'hw_slowdown', 'hw_thermal_slowdown', 'hw_power_brake_slowdown',
    'sw_thermal_slowdown', 'sync_boost',
])
def test_sw_cap_idle_other_limiters_refuse(gpu_rig, reason):
    queue, clock, sample, capacity, publish, tick, claim = gpu_rig
    publish(0)
    reasons = dict(_sw_cap_idle_device()['throttle_reasons'])
    reasons[reason] = True
    _arm_sw_cap(sample, tick, throttle_reasons=reasons, throttle_active_mask=0xC)
    assert claim() is None


def test_sw_cap_idle_mask_mismatch_refuses(gpu_rig):
    queue, clock, sample, capacity, publish, tick, claim = gpu_rig
    publish(0)
    reasons = dict(_sw_cap_idle_device()['throttle_reasons'])
    reasons['gpu_idle'] = True
    # Mask still 0x4 while reasons claim idle: inconsistent, refuse.
    _arm_sw_cap(sample, tick, throttle_reasons=reasons, throttle_active_mask=0x4)
    assert claim() is None


def test_sw_cap_idle_pressure_refuses(gpu_rig):
    queue, clock, sample, capacity, publish, tick, claim = gpu_rig
    publish(0)
    _arm_sw_cap(sample, tick)
    sample['cpu_pressure_some'] = 10.0
    tick()
    assert claim() is None


def _gpu_state_path(queue):
    return adaptive_cpu.local_state_base(queue.ledger().base) / 'gpu-state.json'


def test_admission_reference_is_a_gpu_capacity_fact_never_the_soc_tdp():
    """The denominator of a GPU-only reading is a GPU-only number (#806)."""
    from prismabuild import adaptive_gpu
    device = _sw_cap_idle_device(power_w=100.0)
    reference, scope, source = adaptive_gpu.admission_power_reference(device, {})
    assert reference == adaptive_gpu.DECLARED_GPU_POWER_REFERENCE_W['NVIDIA GB10']
    assert reference != device['power_reference_w']
    assert scope == 'declared_fallback'
    assert source == adaptive_gpu.DECLARED_GPU_POWER_REFERENCE_SOURCE
    # A measurement above the declared floor raises it; one below never lowers
    # it, so an idle history cannot authorize anything.
    state = {'power_peaks': {device['uuid']: 114.0}}
    assert adaptive_gpu.admission_power_reference(device, state)[:2] == (114.0, 'measured_peak')
    state = {'power_peaks': {device['uuid']: 9.0}}
    assert adaptive_gpu.admission_power_reference(device, state)[1] == 'declared_fallback'
    # A driver-published GPU-only limit stays admission grade and is preferred.
    limited = _sw_cap_idle_device(power_limit_w=450.0, power_reference_scope='gpu_power_limit')
    assert adaptive_gpu.admission_power_reference(limited, {})[:2] == (450.0, 'gpu_power_limit')
    # No driver limit and no declared entry is no reference at all.
    unknown = _sw_cap_idle_device(name='NVIDIA GB99')
    assert adaptive_gpu.admission_power_reference(unknown, {}) == (None, None, None)


def test_power_peak_ratchets_but_never_past_the_published_envelope():
    """One implausible sample must not raise the reference permanently."""
    from prismabuild import adaptive_gpu
    state = {}
    adaptive_gpu.record_power_peak(state, _sw_cap_idle_device(power_w=97.0))
    adaptive_gpu.record_power_peak(state, _sw_cap_idle_device(power_w=40.0))
    assert state['power_peaks'] == {_sw_cap_idle_device()['uuid']: 97.0}
    adaptive_gpu.record_power_peak(state, _sw_cap_idle_device(power_w=999.0))
    assert state['power_peaks'] == {_sw_cap_idle_device()['uuid']: 97.0}


def test_first_job_at_the_real_gpu_ceiling_is_congested(gpu_rig):
    """100 W is 0.71 of the SoC TDP and 0.91 of the GPU's own capacity."""
    queue, clock, sample, capacity, publish, tick, claim = gpu_rig
    publish(0)
    sample['devices'][0]['power_w'] = 100.
    tick()
    assert claim() is None, 'a GPU near its measured ceiling must not admit work'


def test_measurement_idleness_is_judged_against_the_gpu_capacity_fact(gpu_rig, monkeypatch):
    """75 W is below 0.65 x 140 W but above 0.65 x the GPU's own capacity."""
    from prismabuild import adaptive_gpu
    queue, clock, sample, capacity, publish, tick, claim = gpu_rig
    monkeypatch.setattr(adaptive_gpu, 'action_contract', lambda item, demand:
                        ('shape', True, False, demand['mem_gb'] * adaptive_gpu.GIB))
    publish(0)
    sample['devices'][0]['power_w'] = 75.
    tick()
    assert claim() is None, 'a measurement admitted a GPU drawing 75 W as idle'


def test_a_measured_peak_raises_the_reference_above_the_declared_floor(gpu_rig):
    """The floor is a floor: a box that has drawn more admits on what it drew."""
    queue, clock, sample, capacity, publish, tick, claim = gpu_rig
    publish(0)
    sample['devices'][0]['power_w'] = 90.
    tick()
    assert claim() is None, '90 W is congested against the declared 110 W floor'
    adaptive_cpu.write_json(_gpu_state_path(queue), {'power_peaks': {'GPU-1': 114.0}})
    tick()
    admitted = claim()
    assert admitted, '90 W is not congested against a measured 114 W peak'


def test_sampled_power_is_recorded_as_the_device_peak(gpu_rig):
    queue, clock, sample, capacity, publish, tick, claim = gpu_rig
    publish(0)
    sample['devices'][0]['power_w'] = 62.
    tick()
    assert claim()
    assert adaptive_cpu.read_json(_gpu_state_path(queue))['power_peaks'] == {'GPU-1': 62.}
