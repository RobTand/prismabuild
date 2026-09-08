"""Admission follows measured CPU pressure, without inventing memory tokens."""
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import sys
import threading
import time

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from prismabuild import pool


def test_host_cpu_pressure_stops_even_physically_free_admissions(tmp_path, monkeypatch):
    # There is a CPU token free, but unrelated work occupies that core.
    from prismabuild import adaptive_cpu
    monkeypatch.setattr(adaptive_cpu.Controller, 'sample', lambda self: {
        'sampled_unix': time.time(), 'busy_cpus': 2., 'psi_some': 0.8,
        'cpu_count': 2, 'interval_s': 1.})
    queue = pool.PoolQueue(tmp_path / 'queue')
    queue.publish(action_key='a' * 64, cas_root=str(tmp_path / 'cas'),
                  checkout_root=str(tmp_path), worker_script='worker.py',
                  resources={'cpu': 1, 'mem_gb': 1})
    assert queue.claim(capacity={'cpu': 2, 'mem_gb': 2},
                       cpu_tiers={'preferred': [0, 1], 'fallback': []},
                       adaptive_cpu=True) is None
    assert not queue.ledger().held()


@pytest.fixture
def rig(tmp_path, monkeypatch):
    from prismabuild import adaptive_cpu
    clock = [100.]
    monkeypatch.setattr(adaptive_cpu.time, 'time', lambda: clock[0])
    state = {'busy_cpus': .1, 'psi_some': 0.}
    monkeypatch.setattr(adaptive_cpu.Controller, 'sample', lambda self: dict(
        sampled_unix=clock[0], cpu_count=2, interval_s=1., **state))
    monkeypatch.setattr(adaptive_cpu, 'action_identity', lambda item: ('shape', False))
    queue = pool.PoolQueue(tmp_path / 'queue')
    tiers = {'preferred': [0], 'fallback': [1]}
    capacity = {'cpu': 2, 'mem_gb': 4, 'gpu': 1}
    def publish(index, cpu=1, memory=1, gpu=0):
        key = f'{index:064x}'
        queue.publish(action_key=key, cas_root=str(tmp_path / 'cas'),
                      checkout_root=str(tmp_path), worker_script='worker.py',
                      resources={'cpu': cpu, 'mem_gb': memory, 'gpu': gpu})
        return key
    def claim():
        return queue.claim(capacity=capacity, cpu_tiers=tiers, adaptive_cpu=True)
    def telemetry(key, *, cpu=.1, complete=True):
        adaptive_cpu.write_json(adaptive_cpu.local_telemetry_path(queue.ledger().base, key), {
            'action_key': key, 'sampled_unix': clock[0], 'cpu_seconds': cpu,
            'wall_seconds': clock[0] - 100, 'memory_current_bytes': 100,
            'memory_peak_bytes': 100, 'complete': complete})
    # Idle initial host accepts its entire capacity; subsequent readings show
    # this nominally wide job consumes a small fraction of one core.
    state['busy_cpus'] = 0.
    key = publish(1, cpu=2)
    first = claim()
    assert first
    state['busy_cpus'] = .1
    publish(2)
    clock[0] += 1
    telemetry(key)
    assert claim() is None  # one cumulative reading cannot establish a rate
    clock[0] += 1
    telemetry(key, cpu=.2)
    return queue, clock, state, key, first, publish, claim, telemetry


def test_low_use_borrows_preferred_cpu_and_release_cannot_mint_tokens(rig):
    queue, clock, state, key, first, publish, claim, telemetry = rig
    second = claim()
    assert second and second['cpu_allocation'] == {'preferred': [0], 'fallback': []}
    assert queue.ledger().held()['cpu'] == 2
    assert queue.ledger().capacity()['cpu'] == 2
    queue.finish(second['action_key'], status='executed', detail={})
    assert queue.ledger().capacity() == {'cpu': 2, 'mem_gb': 4, 'gpu': 1}
    assert not (queue.ledger().free_dir / '.adaptive.json').exists()
    queue.finish(first['action_key'], status='executed', detail={})
    assert queue.ledger().available() == {'cpu': 2, 'mem_gb': 4, 'gpu': 1}
    assert not queue.ledger().held_keys()


@pytest.mark.parametrize('state_update', [{'busy_cpus': 1.95}, {'psi_some': .11}])
def test_greedier_jobs_stop_new_admission_immediately(rig, state_update):
    queue, clock, state, key, first, publish, claim, telemetry = rig
    second = claim()
    assert second
    publish(3)
    clock[0] += 1
    state.update(state_update)
    assert claim() is None
    assert queue.item_path(pool.CLAIMED, first['action_key']).exists()
    assert queue.item_path(pool.CLAIMED, second['action_key']).exists()


@pytest.mark.parametrize('fault', ['missing', 'stale', 'incomplete', 'previous_attempt'])
def test_untrustworthy_job_attribution_never_grants_borrowed_capacity(rig, fault):
    from prismabuild import adaptive_cpu
    queue, clock, state, key, first, publish, claim, telemetry = rig
    path = adaptive_cpu.local_telemetry_path(queue.ledger().base, key)
    if fault == 'missing':
        path.unlink()
    elif fault == 'stale':
        clock[0] += 6
    elif fault == 'incomplete':
        telemetry(key, complete=False)
    else:
        record = adaptive_cpu.read_json(path)
        record['sampled_unix'] = 99
        adaptive_cpu.write_json(path, record)
    assert claim() is None
    assert queue.ledger().held()['cpu'] == 2


def test_unavailable_host_measurement_never_borrows(rig, monkeypatch):
    from prismabuild import adaptive_cpu
    queue, clock, state, key, first, publish, claim, telemetry = rig
    monkeypatch.setattr(adaptive_cpu.Controller, 'sample', lambda self: {})
    assert claim() is None


def test_memory_and_gpu_are_never_discounted_by_cpu_headroom(rig):
    queue, clock, state, key, first, publish, claim, telemetry = rig
    # Fill the remaining memory and GPU with a real token reservation.
    assert queue.ledger().acquire('b' * 64, {'mem_gb': 3, 'gpu': 1})
    assert claim() is None
    queue.ledger().release('b' * 64)
    second = claim()
    assert second
    assert queue.ledger().held()['mem_gb'] == 2


def test_concurrent_loops_cannot_spend_one_sample_repeatedly(rig):
    from prismabuild import adaptive_cpu
    queue, clock, state, key, first, publish, claim, telemetry = rig
    adaptive_cpu.write_json(adaptive_cpu.local_state_base(queue.ledger().base) / 'profiles.json', {
        'shape': {'samples': 10, 'cpu': .1, 'sampled_unix': clock[0]}})
    for index in range(3, 10):
        publish(index)
    barrier = threading.Barrier(8)
    def run(_):
        barrier.wait()
        return claim()
    with ThreadPoolExecutor(max_workers=8) as workers:
        admitted = [x for x in workers.map(run, range(8)) if x]
    assert len(admitted) == 1
    assert queue.ledger().capacity()['cpu'] == 2
    assert len(queue.ledger().held_keys()) == 2


def test_measurement_neither_lends_nor_borrows(rig, monkeypatch):
    from prismabuild import adaptive_cpu
    queue, clock, state, key, first, publish, claim, telemetry = rig
    monkeypatch.setattr(adaptive_cpu, 'action_identity', lambda item: ('measurement', True))
    assert claim() is None
    queue.finish(first['action_key'], status='executed', detail={})
    measurement = claim()
    assert measurement
    monkeypatch.setattr(adaptive_cpu, 'action_identity', lambda item: ('shape', False))
    publish(3)
    assert claim() is None  # even the physically free CPU stays isolated


def test_cpu_samples_measure_busy_time_in_allowed_affinity_and_psi(tmp_path, monkeypatch):
    from prismabuild import adaptive_cpu
    queue = pool.PoolQueue(tmp_path / 'queue')
    controller = adaptive_cpu.Controller(queue.ledger(), {'preferred': [4], 'fallback': [8]})
    samples = iter([
        {'sampled_unix': 10., 'cpus': {'4': [10, 100], '8': [20, 100]}, 'psi_total': 0},
        {'sampled_unix': 20., 'cpus': {'4': [20, 200], '8': [70, 200]}, 'psi_total': 2000000},
    ])
    monkeypatch.setattr(adaptive_cpu, 'counters', lambda cpus: next(samples))
    assert controller.sample() == {}
    seen = controller.sample()
    assert seen['busy_cpus'] == pytest.approx(.6)
    assert seen['psi_some'] == pytest.approx(.2)
    assert seen['interval_s'] == 10


def test_lost_claim_returns_only_own_tokens_and_never_mints_borrowed_cpu(rig, monkeypatch):
    queue, clock, state, key, first, publish, claim, telemetry = rig
    real_rename = os.rename
    def lose(source, target):
        if Path(source).parent == queue.dir(pool.READY):
            raise FileNotFoundError(source)
        return real_rename(source, target)
    monkeypatch.setattr(pool.os, 'rename', lose)
    assert claim() is None
    assert queue.ledger().held_keys() == [key]
    assert queue.ledger().held() == {'cpu': 2, 'mem_gb': 1}
    assert queue.ledger().capacity() == {'cpu': 2, 'mem_gb': 4, 'gpu': 1}
    monkeypatch.setattr(pool.os, 'rename', real_rename)
    assert claim()


def test_cpu_owned_by_unmeasured_borrower_is_not_lent_again(rig):
    from prismabuild import adaptive_cpu
    queue, clock, state, key, first, publish, claim, telemetry = rig
    second = claim()
    assert second['cpu_allocation']['preferred'] == [0]
    clock[0] += 1
    telemetry(key, cpu=.3)
    adaptive_cpu.write_json(adaptive_cpu.local_state_base(queue.ledger().base) / 'profiles.json', {
        'shape': {'samples': 10, 'cpu': .1, 'sampled_unix': clock[0]}})
    publish(3)
    third = claim()
    assert third and third['cpu_allocation'] == {'preferred': [], 'fallback': [1]}


def test_shape_learning_grows_immediately_when_same_shape_gets_greedier(rig):
    from prismabuild import adaptive_cpu
    queue, clock, state, key, first, publish, claim, telemetry = rig
    assert claim()
    clock[0] += 1
    telemetry(key, cpu=2.)
    # Host reading can briefly lag a phase transition: the job-specific delta
    # already invalidates its earlier cheap profile and protects its CPU IDs.
    publish(3)
    assert claim() is None
    learned = adaptive_cpu.read_json(adaptive_cpu.local_state_base(queue.ledger().base) / 'profiles.json')
    assert learned['shape']['cpu'] >= 2.


def test_proc_stat_excludes_guest_double_count_and_iowait(tmp_path, monkeypatch):
    from prismabuild import adaptive_cpu
    original = Path.read_text
    def read(path, *args, **kwargs):
        if str(path) == '/proc/stat':
            return 'cpu 100 0 100 100 0 0 0 0\ncpu4 10 0 10 70 10 0 0 0 10 0\ncpu8 900 0 100 0 0 0 0 0\n'
        if str(path) == '/proc/pressure/cpu':
            return 'some avg10=0.00 avg60=0.00 avg300=0.00 total=123\n'
        return original(path, *args, **kwargs)
    monkeypatch.setattr(Path, 'read_text', read)
    sample = adaptive_cpu.counters({4})
    assert sample['cpus'] == {'4': [20, 100]}
    assert sample['psi_total'] == 123


def test_shape_is_bound_to_code_inputs_environment_and_command(tmp_path):
    from prismabuild import adaptive_cpu, core
    from test_core import _body
    (tmp_path / 'task_code.py').write_text('print(1)\n')
    body = _body(tmp_path)
    cas = core.PrismaBuildCAS(tmp_path / 'cas')
    def identity(value):
        action = core.seal_action(value)
        cas.publish_action_request(action)
        return adaptive_cpu.action_identity({'action_key': action['action_key'],
                                             'cas_root': str(cas.root), 'resources': {'cpu': 2}})
    original = identity(body)
    changed = json.loads(json.dumps(body))
    changed['task']['result_path'] = 'other.txt'
    assert identity(changed) == original
    changed['task']['argv'].append('larger-work')
    assert identity(changed) != original
    changed = json.loads(json.dumps(body))
    changed['inputs'][0]['bytes'] += 1
    assert identity(changed) != original
    changed = json.loads(json.dumps(body))
    changed['environment']['variables']['DECLARED'] = 'different'
    assert identity(changed) != original


def test_short_completions_learn_cpu_and_memory_without_reserving_more_memory(rig):
    from prismabuild import adaptive_cpu
    queue, clock, state, key, first, publish, claim, telemetry = rig
    for index in range(3):
        sample = {'action_key': key, 'nonce': str(index), 'complete': True,
                  'sampled_unix': clock[0], 'cpu_seconds': .01,
                  'wall_seconds': .1, 'memory_peak_bytes': 1234}
        assert adaptive_cpu.record_completion(queue.ledger(), first, sample)
        assert not adaptive_cpu.record_completion(queue.ledger(), first, sample)
    learned = adaptive_cpu.read_json(adaptive_cpu.local_state_base(queue.ledger().base) / 'profiles.json')['shape']
    assert learned['cpu'] == pytest.approx(.125)
    assert learned['samples'] == 3
    assert learned['memory_peak_bytes'] == 1234
    assert queue.ledger().capacity()['mem_gb'] == 4
    sample['complete'] = False
    sample['nonce'] = 'partial'
    assert not adaptive_cpu.record_completion(queue.ledger(), first, sample)


def test_pbrun_shape_ignores_only_generated_bookkeeping(tmp_path, monkeypatch):
    from prismabuild import adaptive_cpu, core
    from test_core import _body
    (tmp_path / 'task_code.py').write_text('print(1)\n')
    body = _body(tmp_path)
    body['task']['definition_id'] = 'fleet/pbrun'
    body['params'] = {'command': ['python', 'worker.py'], 'cwd': '/project',
                      'demand': {'cpu': 2}, 'checkout_snapshot': {
                          'schema': 'prismaquant.prismabuild.pbrun_checkout_snapshot.v2',
                          'commit': 'a' * 40}}
    body['code_closure']['files'][0]['path'] = '.pbrun-closure.aaa.json'
    body['environment']['variables']['PRISMABUILD_CONTAINER_OWNER'] = 'a' * 64
    body['environment']['variables']['PRISMABUILD_CONTAINER_MARKER'] = '/markers/a'
    body['action_key'] = 'a' * 64
    # Shape normalization consumes an already validated action; the independent
    # real contract test above validates ordinary sealed requests end to end.
    monkeypatch.setattr(core, 'validate_action', lambda raw: raw)
    monkeypatch.setattr(adaptive_cpu, 'read_json', lambda path: body)
    item = {'action_key': 'a' * 64, 'cas_root': str(tmp_path), 'resources': {'cpu': 2}}
    original = adaptive_cpu.shape_key(item)
    body['task']['argv'] = ['bash', '-c', 'command | tee another.generated.result']
    body['task']['result_path'] = 'another.generated.result'
    body['code_closure']['files'][0]['path'] = '.pbrun-closure.bbb.json'
    body['params']['checkout_snapshot']['commit'] = 'b' * 40
    body['environment']['variables']['PRISMABUILD_CONTAINER_OWNER'] = 'b' * 64
    body['environment']['variables']['PRISMABUILD_CONTAINER_MARKER'] = '/markers/b'
    assert adaptive_cpu.shape_key(item) == original
    body['params']['command'].append('--bigger')
    assert adaptive_cpu.shape_key(item) != original
    body['params']['command'].pop()
    body['code_closure']['files'][0]['sha256'] = 'f' * 64
    assert adaptive_cpu.shape_key(item) != original


def test_proven_idle_preferred_is_borrowed_before_free_fallback(tmp_path, monkeypatch):
    from prismabuild import adaptive_cpu
    queue = pool.PoolQueue(tmp_path / 'queue')
    clock = [100.]
    monkeypatch.setattr(adaptive_cpu.time, 'time', lambda: clock[0])
    monkeypatch.setattr(adaptive_cpu, 'action_identity', lambda item: ('shape', False))
    monkeypatch.setattr(adaptive_cpu.Controller, 'sample', lambda self: {
        'sampled_unix': clock[0], 'cpu_count': 2, 'interval_s': 1.,
        'busy_cpus': .1, 'psi_some': 0.})
    tiers = {'preferred': [0], 'fallback': [1]}
    def claim():
        return queue.claim(capacity={'cpu': 2, 'mem_gb': 4}, cpu_tiers=tiers,
                           adaptive_cpu=True)
    key = 'a' * 64
    queue.publish(action_key=key, cas_root=str(tmp_path / 'cas'),
                  checkout_root=str(tmp_path), worker_script='worker.py',
                  resources={'cpu': 1, 'mem_gb': 1})
    assert claim()['cpu_allocation'] == {'preferred': [0], 'fallback': []}
    queue.publish(action_key='b' * 64, cas_root=str(tmp_path / 'cas'),
                  checkout_root=str(tmp_path), worker_script='worker.py',
                  resources={'cpu': 1, 'mem_gb': 1})
    for elapsed in (1, 2):
        clock[0] = 100 + elapsed
        adaptive_cpu.write_json(adaptive_cpu.local_telemetry_path(queue.ledger().base, key), {
            'action_key': key, 'complete': True, 'sampled_unix': clock[0],
            'cpu_seconds': elapsed * .1, 'wall_seconds': elapsed})
        if elapsed == 1:
            # Prime attribution without admitting a second action; the
            # controller is the same one the queue invokes under its lock.
            controller = adaptive_cpu.Controller(queue.ledger(), tiers)
            with controller.locked():
                controller.decision({'action_key': 'b' * 64, 'cas_root': str(tmp_path / 'cas')},
                                    {'cpu': 2})
    second = claim()
    assert second['cpu_allocation'] == {'preferred': [0], 'fallback': []}
    assert queue.ledger().free_preferred(tiers) == 0
    assert queue.ledger().available()['cpu'] == 1  # SMT remains unused


@pytest.mark.parametrize('measurement', [False, True])
def test_legacy_gpu_only_holder_cannot_overlap_cpu_work_or_measurement(tmp_path, monkeypatch, measurement):
    from prismabuild import adaptive_cpu
    queue = pool.PoolQueue(tmp_path / 'queue')
    tiers = {'preferred': [0], 'fallback': [1]}
    queue.ledger().configure_cpu_tiers(tiers)
    queue.ledger().ensure_capacity({'cpu': 2, 'gpu': 2})
    assert queue.ledger().acquire('a' * 64, {'gpu': 1})
    monkeypatch.setattr(adaptive_cpu, 'action_identity', lambda item: ('shape', measurement))
    monkeypatch.setattr(adaptive_cpu.Controller, 'sample', lambda self: {
        'sampled_unix': time.time(), 'cpu_count': 2, 'interval_s': 1.,
        'busy_cpus': 0., 'psi_some': 0.})
    controller = adaptive_cpu.Controller(queue.ledger(), tiers)
    with controller.locked():
        assert controller.decision({'action_key': 'b' * 64, 'cas_root': str(tmp_path)},
                                   {'cpu': 1}) is None


def test_legacy_incoming_cpu_zero_is_charged_as_unknown_whole_host(tmp_path, monkeypatch):
    from prismabuild import adaptive_cpu
    queue = pool.PoolQueue(tmp_path / 'queue')
    tiers = {'preferred': [0], 'fallback': [1]}
    queue.ledger().configure_cpu_tiers(tiers)
    queue.ledger().ensure_capacity({'cpu': 2, 'gpu': 2})
    monkeypatch.setattr(adaptive_cpu, 'action_identity', lambda item: ('shape', False))
    monkeypatch.setattr(adaptive_cpu.Controller, 'sample', lambda self: {
        'sampled_unix': time.time(), 'cpu_count': 2, 'interval_s': 1.,
        'busy_cpus': .05, 'psi_some': 0.})
    controller = adaptive_cpu.Controller(queue.ledger(), tiers)
    with controller.locked():
        decision = controller.decision({'action_key': 'b' * 64, 'cas_root': str(tmp_path)},
                                       {'gpu': 1})
    assert decision['unbounded_cpu'] is True
    assert decision['cost'] == 2
    assert not decision['borrowing']


@pytest.mark.parametrize('measurement', [False, True])
def test_incoming_legacy_cpu_zero_cannot_overlap_claimed_cpu_work(tmp_path, monkeypatch, measurement):
    from prismabuild import adaptive_cpu
    queue = pool.PoolQueue(tmp_path / 'queue')
    tiers = {'preferred': [0], 'fallback': [1]}
    monkeypatch.setattr(adaptive_cpu, 'action_identity', lambda item: ('shape', measurement))
    monkeypatch.setattr(adaptive_cpu.Controller, 'sample', lambda self: {
        'sampled_unix': time.time(), 'cpu_count': 2, 'interval_s': 1.,
        'busy_cpus': 0., 'psi_some': 0.})
    queue.publish(action_key='a' * 64, cas_root=str(tmp_path / 'cas'),
                  checkout_root=str(tmp_path), worker_script='worker.py',
                  resources={'cpu': 1})
    capacity = {'cpu': 2, 'gpu': 1}
    first = queue.claim(capacity=capacity, cpu_tiers=tiers, adaptive_cpu=True)
    assert first
    monkeypatch.setattr(adaptive_cpu, 'action_identity', lambda item: ('shape', False))
    queue.publish(action_key='b' * 64, cas_root=str(tmp_path / 'cas'),
                  checkout_root=str(tmp_path), worker_script='worker.py',
                  resources={'gpu': 1})
    assert queue.claim(capacity=capacity, cpu_tiers=tiers, adaptive_cpu=True) is None
    assert queue.item_path(pool.CLAIMED, first['action_key']).exists()


def test_adaptive_empty_demand_is_refused_while_static_legacy_remains_supported(tmp_path):
    queue = pool.PoolQueue(tmp_path / 'queue')
    queue.publish(action_key='a' * 64, cas_root=str(tmp_path / 'cas'),
                  checkout_root=str(tmp_path), worker_script='worker.py')
    tiers = {'preferred': [0], 'fallback': []}
    assert queue.claim(capacity={'cpu': 1}, cpu_tiers=tiers, adaptive_cpu=True) is None
    assert not queue.ledger().held_keys()
    assert queue.claim(capacity={'cpu': 1}, cpu_tiers=tiers)


@pytest.mark.parametrize('measurement', [False, True])
def test_full_width_job_starts_on_empty_host_with_incidental_idle_activity(tmp_path, monkeypatch, measurement):
    from prismabuild import adaptive_cpu
    queue = pool.PoolQueue(tmp_path / 'queue')
    monkeypatch.setattr(adaptive_cpu, 'action_identity', lambda item: ('shape', measurement))
    monkeypatch.setattr(adaptive_cpu.Controller, 'sample', lambda self: {
        'sampled_unix': time.time(), 'cpu_count': 2, 'interval_s': 1.,
        'busy_cpus': .05, 'psi_some': 0.})
    queue.publish(action_key='a' * 64, cas_root=str(tmp_path / 'cas'),
                  checkout_root=str(tmp_path), worker_script='worker.py',
                  resources={'cpu': 2})
    item = queue.claim(capacity={'cpu': 2}, cpu_tiers={'preferred': [0, 1], 'fallback': []},
                       adaptive_cpu=True)
    assert item and item['cpu_allocation']['preferred'] == [0, 1]


@pytest.mark.parametrize('busy,psi', [(.2, 0.), (.05, .10)])
def test_full_width_exception_does_not_ignore_foreign_work_or_pressure(tmp_path, monkeypatch, busy, psi):
    from prismabuild import adaptive_cpu
    queue = pool.PoolQueue(tmp_path / 'queue')
    monkeypatch.setattr(adaptive_cpu, 'action_identity', lambda item: ('shape', False))
    monkeypatch.setattr(adaptive_cpu.Controller, 'sample', lambda self: {
        'sampled_unix': time.time(), 'cpu_count': 2, 'interval_s': 1.,
        'busy_cpus': busy, 'psi_some': psi})
    queue.publish(action_key='a' * 64, cas_root=str(tmp_path / 'cas'),
                  checkout_root=str(tmp_path), worker_script='worker.py',
                  resources={'cpu': 2})
    assert queue.claim(capacity={'cpu': 2}, cpu_tiers={'preferred': [0, 1], 'fallback': []},
                       adaptive_cpu=True) is None
