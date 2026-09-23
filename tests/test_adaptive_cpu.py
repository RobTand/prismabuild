"""Admission follows measured CPU pressure, without inventing memory tokens."""
from concurrent.futures import ThreadPoolExecutor
import fcntl
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
        'sampled_unix': time.time(), 'busy_cpus': 2., 'psi_some': 0.802,
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
    # The rig's .1 busy CPUs was its holder's load: once the holder is gone
    # and a sample no longer reaches back into it, the host reads idle as it
    # did before the holder ran (#997 judges a measurement on that history).
    clock[0] += 2
    state['busy_cpus'] = 0.
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
        {'sampled_unix': 10., 'cpus': {'4': [10, 100], '8': [20, 100]},
         'psi_total': 0, },
        {'sampled_unix': 20., 'cpus': {'4': [20, 200], '8': [70, 200]},
         'psi_total': 2000000},
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
            return ('some avg10=0.00 avg60=0.00 avg300=0.00 total=123\n'
                    'full avg10=0.00 avg60=0.00 avg300=0.00 total=0\n')
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


def test_a_spent_sample_admits_on_free_tokens_instead_of_refusing(tmp_path, monkeypatch):
    """A preferred borrow is a preference; free tokens that cover the demand win.

    Sparky, 2026-09-23 (#924): CPU-only actions were refused
    ``borrow_evidence_unavailable`` for about 30 minutes with fresh evidence and
    ten free CPU tokens against a demand of one or two.  The only failing
    condition was that the sample's single borrow had already been spent, so
    each item waited one sample interval before it ran on tokens it could have
    taken at once.  The freshness rule forbids a second *borrow* against one
    sample; taking free tokens is not a borrow.
    """
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
    controller = adaptive_cpu.Controller(queue.ledger(), tiers)
    for elapsed in (1, 2):
        clock[0] = 100 + elapsed
        adaptive_cpu.write_json(adaptive_cpu.local_telemetry_path(queue.ledger().base, key), {
            'action_key': key, 'complete': True, 'sampled_unix': clock[0],
            'cpu_seconds': elapsed * .1, 'wall_seconds': elapsed})
        with controller.locked():
            decision = controller.decision(
                {'action_key': 'b' * 64, 'cas_root': str(tmp_path / 'cas')}, {'cpu': 1})
            controller._host_sample = None
    # The holder lends its preferred CPU, so this sample would authorize a
    # borrow ...
    assert decision['borrowing'] and decision['preferred_borrow'] == 1
    # ... had another claim not already spent it.
    controller.write_state('last-borrow.json', {'sampled_unix': clock[0], 'borrow_id': 'peer'})
    with controller.locked():
        spent = controller.decision(
            {'action_key': 'b' * 64, 'cas_root': str(tmp_path / 'cas')}, {'cpu': 1})
    assert spent is not None, controller.last_decision
    assert spent['borrowing'] is False and spent['preferred_borrow'] == 0

    second = claim()
    assert second is not None and second['action_key'] == 'b' * 64
    assert second['cpu_allocation'] == {'preferred': [], 'fallback': [1]}
    # The peer's borrow record is untouched: nothing was borrowed here.
    assert adaptive_cpu.read_json(controller.base / 'last-borrow.json') == {
        'sampled_unix': clock[0], 'borrow_id': 'peer'}


def test_a_spent_sample_still_refuses_a_borrow_the_demand_needs(tmp_path, monkeypatch):
    """The relaxation above is bounded by free tokens, never by the borrow."""
    from prismabuild import adaptive_cpu
    queue = pool.PoolQueue(tmp_path / 'queue')
    clock = [100.]
    monkeypatch.setattr(adaptive_cpu.time, 'time', lambda: clock[0])
    monkeypatch.setattr(adaptive_cpu, 'action_identity', lambda item: ('shape', False))
    monkeypatch.setattr(adaptive_cpu.Controller, 'sample', lambda self: {
        'sampled_unix': clock[0], 'cpu_count': 2, 'interval_s': 1.,
        'busy_cpus': .1, 'psi_some': 0.})
    tiers = {'preferred': [0], 'fallback': [1]}
    key = 'a' * 64
    queue.publish(action_key=key, cas_root=str(tmp_path / 'cas'),
                  checkout_root=str(tmp_path), worker_script='worker.py',
                  resources={'cpu': 2, 'mem_gb': 1})
    assert queue.claim(capacity={'cpu': 2, 'mem_gb': 4}, cpu_tiers=tiers,
                       adaptive_cpu=True) is not None
    controller = adaptive_cpu.Controller(queue.ledger(), tiers)
    for elapsed in (1, 2):
        clock[0] = 100 + elapsed
        adaptive_cpu.write_json(adaptive_cpu.local_telemetry_path(queue.ledger().base, key), {
            'action_key': key, 'complete': True, 'sampled_unix': clock[0],
            'cpu_seconds': elapsed * .1, 'wall_seconds': elapsed})
        with controller.locked():
            controller.decision({'action_key': 'b' * 64, 'cas_root': str(tmp_path / 'cas')},
                                {'cpu': 1})
            controller._host_sample = None
    controller.write_state('last-borrow.json', {'sampled_unix': clock[0], 'borrow_id': 'peer'})
    with controller.locked():
        assert controller.decision({'action_key': 'b' * 64, 'cas_root': str(tmp_path / 'cas')},
                                   {'cpu': 1}) is None
    assert controller.last_decision['reason'] == 'borrow_evidence_unavailable'
    assert controller.last_decision['available_cpu'] == 0


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


def test_a_peer_taking_admission_after_the_lease_cannot_reuse_the_borrowed_sample(
        rig, monkeypatch):
    """The borrow is spent at the decision, so nothing after it can lose it.

    Written against the narrowed admission lock: the claim renames, writes its
    lease and commits its tokens outside the lock, so a sibling loop can hold
    the box's FLOCK the instant the lease lands.  If the borrow record were
    written after that, it could not be written at all, and the same host
    sample would authorize a second borrow -- an unbounded burst against one
    measurement, which ``docs/design.md`` forbids.

    Contributed as a review regression by the maintenance reviewer on
    PrismaBuild #403; kept here with the rest of the borrow contract.
    """

    from prismabuild import adaptive_cpu

    queue, clock, state, key, first, publish, claim, telemetry = rig
    original = queue.write_lease
    directory, digest = adaptive_cpu.box_state(queue.ledger().base)
    descriptor = os.open(Path(directory) / f'{digest}.lock', os.O_RDWR)

    def peer_enters_after_lease(action_key, **kwargs):
        result = original(action_key, **kwargs)
        # Non-blocking, so this records whether the peer got in rather than
        # waiting for it.  Under the narrowed lock it does.
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            pass
        return result

    monkeypatch.setattr(queue, 'write_lease', peer_enters_after_lease)
    try:
        second = claim()
        assert second is not None
        assert second['cpu_allocation']['preferred'] == [0]
    finally:
        os.close(descriptor)
        monkeypatch.setattr(queue, 'write_lease', original)

    # A short borrowing action finishes before the host sample changes.
    queue.finish(second['action_key'], status='executed', detail={})
    publish(3)
    third = claim()
    assert third is None, 'one host sample authorized a second borrow'


def test_returning_a_lost_claim_s_borrow_never_overwrites_a_newer_one(rig):
    """The restore is this decision's own record or nothing.

    A claimant whose rename is lost gives its borrow back.  If it gave back a
    record another loop had since written, that newer sample would be free to
    authorize a second borrow -- the burst the freshness rule forbids, arrived
    at from the other direction.
    """

    from prismabuild import adaptive_cpu

    queue, clock, state, key, first, publish, claim, telemetry = rig
    controller = adaptive_cpu.Controller(queue.ledger(), {'preferred': [0], 'fallback': [1]})
    spent = {'borrowing': True, 'sampled_unix': clock[0]}
    previous = controller.admitted(spent)
    assert previous == {}

    # Another loop borrows against a later sample before the lost claimant
    # gets around to returning its own.
    newer = {'borrowing': True, 'sampled_unix': clock[0] + 1}
    controller.admitted(newer)

    controller.withdrew(spent, previous)
    assert adaptive_cpu.read_json(controller.base / 'last-borrow.json') == {
        'sampled_unix': newer['sampled_unix'],
        'borrow_id': newer['borrow_id']}, 'an older restore took the newer borrow'

    controller.withdrew(newer, {'sampled_unix': spent['sampled_unix']})
    assert adaptive_cpu.read_json(controller.base / 'last-borrow.json') == {
        'sampled_unix': spent['sampled_unix']}, 'a record still its own was not restored'

def _host_sample(cpus, busy_by_cpu, psi_some, *, interval=10.):
    """A fresh host sample with per-CPU occupancy, as the real sampler files it.

    CPU PSI has no system-wide FULL state (kernel/sched/psi.c: "the FULL state
    doesn't exist for the CPU resource at the system level", and the system
    root's state mask omits PSI_CPU_FULL), so every sample here is the shape the
    kernel can actually produce: some only, beside per-CPU busy fractions.
    """
    busy = sum(busy_by_cpu.get(cpu, 0.) for cpu in cpus)
    return {'sampled_unix': time.time(), 'cpu_count': len(cpus), 'interval_s': interval,
            'busy_cpus': busy, 'psi_some': psi_some,
            'per_cpu_busy': {str(cpu): busy_by_cpu.get(cpu, 0.) for cpu in cpus}}


@pytest.mark.parametrize('label,busy_by_cpu,psi,expect_admission', [
    # A pinned neighbour: four of eighty cores at ~1.0, the rest idle.  PSI
    # some is high because those four threads contend with each other, not
    # because the host has nowhere left to run.
    ('pinned_neighbour',
     {cpu: (1. if cpu in (8, 9, 10, 11) else 0.) for cpu in range(80)}, .633, True),
    # Real saturation: every core busy, PSI some high, still no FULL anywhere.
    ('saturated', {cpu: 1. for cpu in range(80)}, .633, False),
])
def test_pressure_is_corroborated_by_fresh_per_cpu_idle_capacity(
        tmp_path, monkeypatch, label, busy_by_cpu, psi, expect_admission):
    from prismabuild import adaptive_cpu
    cpus = list(range(80))
    queue = pool.PoolQueue(tmp_path / 'queue')
    monkeypatch.setattr(adaptive_cpu, 'action_identity', lambda item: ('shape', False))
    monkeypatch.setattr(adaptive_cpu.Controller, 'sample',
                        lambda self: _host_sample(cpus, busy_by_cpu, psi))
    queue.publish(action_key='a' * 64, cas_root=str(tmp_path / 'cas'),
                  checkout_root=str(tmp_path), worker_script='worker.py',
                  resources={'cpu': 4, 'mem_gb': 4})
    item = queue.claim(capacity={'cpu': 80, 'mem_gb': 64},
                       cpu_tiers={'preferred': cpus, 'fallback': []},
                       adaptive_cpu=True)
    assert (item is not None) is expect_admission, label


def test_pressure_without_per_cpu_telemetry_refuses_as_unknown(tmp_path, monkeypatch):
    """Some-pressure with no per-CPU evidence is unknown, and unknown refuses."""
    from prismabuild import adaptive_cpu
    queue = pool.PoolQueue(tmp_path / 'queue')
    monkeypatch.setattr(adaptive_cpu, 'action_identity', lambda item: ('shape', False))
    monkeypatch.setattr(adaptive_cpu.Controller, 'sample', lambda self: {
        'sampled_unix': time.time(), 'cpu_count': 2, 'interval_s': 10.,
        'busy_cpus': .1, 'psi_some': .2})
    queue.publish(action_key='a' * 64, cas_root=str(tmp_path / 'cas'),
                  checkout_root=str(tmp_path), worker_script='worker.py',
                  resources={'cpu': 1, 'mem_gb': 1})
    assert queue.claim(capacity={'cpu': 2, 'mem_gb': 2},
                       cpu_tiers={'preferred': [0], 'fallback': [1]},
                       adaptive_cpu=True) is None


def _seed_queue(queue, tiers, *, held_ordinals=(), allocation=None):
    """A configured box with the given tokens already held by one action.

    Tokens are ``cpu-<ordinal>``; the ordinal is a token index into
    ``preferred + fallback``, never a CPU ID (pool.py: begin_acquire takes the
    first free tokens in sorted order, cpu_allocation maps the ordinal through
    the tiers).
    """
    from prismabuild import adaptive_cpu
    ledger = queue.ledger()
    # Bind tokens to CPUs before any reservation exists: the map is immutable
    # once a holder is present (pool.py configure_cpu_tiers).
    ledger.configure_cpu_tiers(tiers)
    ledger.ensure_capacity({'cpu': len(tiers['preferred']) + len(tiers['fallback']), 'mem_gb': 1})
    if held_ordinals:
        holder = ledger.held_dir / 'held-action'
        holder.mkdir(parents=True, exist_ok=True)
        for ordinal in held_ordinals:
            name = next(path.name for path in sorted(ledger.free_dir.glob('cpu-*'))
                        if int(path.name.split('-')[-1]) == ordinal)
            (ledger.free_dir / name).rename(holder / name)
        metadata = {'declared_cpu': len(held_ordinals), 'admitted_unix': 0}
        if allocation is not None:
            metadata['allocation'] = allocation
        (holder / adaptive_cpu.METADATA).write_text(json.dumps(metadata))


NONCONTIGUOUS = {'preferred': [8, 10], 'fallback': [2, 4]}


def test_disjoint_proof_maps_token_ordinals_through_the_tiers(tmp_path, monkeypatch):
    """Held token ordinal 1 is CPU 10, not CPU 1; the claim would take 8 and 10."""
    from prismabuild import adaptive_cpu
    tiers = dict(NONCONTIGUOUS)
    queue = pool.PoolQueue(tmp_path / 'queue')
    _seed_queue(queue, tiers, held_ordinals=(1,))
    monkeypatch.setattr(adaptive_cpu, 'action_identity', lambda item: ('shape', False))
    busy = {cpu: (1. if cpu == 10 else 0.) for cpu in (8, 10, 2, 4)}
    monkeypatch.setattr(adaptive_cpu.Controller, 'sample',
                        lambda self: _host_sample([8, 10, 2, 4], busy, .633))
    queue.publish(action_key='a' * 64, cas_root=str(tmp_path / 'cas'),
                  checkout_root=str(tmp_path), worker_script='worker.py',
                  resources={'cpu': 2, 'mem_gb': 1})
    # demand 2 would take tokens 0 and 2 -> CPUs 8 and 2, both idle: admitted.
    assert queue.claim(capacity={'cpu': 4, 'mem_gb': 2}, cpu_tiers=tiers,
                       adaptive_cpu=True) is not None

    queue2 = pool.PoolQueue(tmp_path / 'queue2')
    _seed_queue(queue2, tiers, held_ordinals=(0,))
    monkeypatch.setattr(adaptive_cpu.Controller, 'sample',
                        lambda self: _host_sample([8, 10, 2, 4], busy, .633))
    queue2.publish(action_key='b' * 64, cas_root=str(tmp_path / 'cas'),
                   checkout_root=str(tmp_path), worker_script='worker.py',
                   resources={'cpu': 1, 'mem_gb': 1})
    # The first free token is ordinal 1 -> CPU 10, which the held action owns.
    assert queue2.claim(capacity={'cpu': 4, 'mem_gb': 2}, cpu_tiers=tiers,
                        adaptive_cpu=True) is None


def test_disjoint_proof_honours_a_borrowed_allocation_in_metadata(tmp_path, monkeypatch):
    """A borrowed CPU lives in the holder's metadata, not in its token ordinal."""
    from prismabuild import adaptive_cpu
    tiers = dict(NONCONTIGUOUS)
    queue = pool.PoolQueue(tmp_path / 'queue')
    _seed_queue(queue, tiers, held_ordinals=(2,),
                allocation={'preferred': [8], 'fallback': []})
    monkeypatch.setattr(adaptive_cpu, 'action_identity', lambda item: ('shape', False))
    busy = {cpu: 0. for cpu in (8, 10, 2, 4)}
    monkeypatch.setattr(adaptive_cpu.Controller, 'sample',
                        lambda self: _host_sample([8, 10, 2, 4], busy, .633))
    queue.publish(action_key='c' * 64, cas_root=str(tmp_path / 'cas'),
                  checkout_root=str(tmp_path), worker_script='worker.py',
                  resources={'cpu': 1, 'mem_gb': 1})
    # Token ordinal 2 is CPU 2, but the metadata says the holder holds CPU 8 --
    # and 8 is the CPU this claim's first free token would be given.
    assert queue.claim(capacity={'cpu': 4, 'mem_gb': 2}, cpu_tiers=tiers,
                       adaptive_cpu=True) is None


def test_pressure_override_never_borrows_a_busy_lender(tmp_path, monkeypatch):
    """A high "some" believed because *this claim's* cores are idle must not
    then hand it a held core that the same sample shows busy.

    Ordinary admission lends a proven-cheap holder's preferred CPU to a claim
    whose demand exceeds the free *preferred* tokens; that CPU is idle by the
    holder's telemetry, not by the fresh sample.  Under the pressure override
    the claim was admitted on the strength of the CPUs its own free tokens map
    to, so borrowing must stay closed for that decision -- while the free
    fallback tokens still admit it.
    """
    from prismabuild import adaptive_cpu
    clock = [100.]
    monkeypatch.setattr(adaptive_cpu.time, 'time', lambda: clock[0])
    monkeypatch.setattr(adaptive_cpu, 'action_identity', lambda item: ('shape', False))
    state = {'psi': 0., 'busy': {'0': 0., '1': 0., '2': 0.}}
    monkeypatch.setattr(adaptive_cpu.Controller, 'sample',
                        lambda self: _host_sample([0, 1, 2], state['busy'], state['psi']))
    queue = pool.PoolQueue(tmp_path / 'queue')
    tiers = {'preferred': [0], 'fallback': [1, 2]}
    capacity = {'cpu': 3, 'mem_gb': 4}

    def publish(key):
        queue.publish(action_key=key, cas_root=str(tmp_path / 'cas'),
                      checkout_root=str(tmp_path), worker_script='worker.py',
                      resources={'cpu': 1, 'mem_gb': 1})

    publish('a' * 64)
    holder = queue.claim(capacity=capacity, cpu_tiers=tiers, adaptive_cpu=True)
    assert holder and holder['cpu_allocation'] == {'preferred': [0], 'fallback': []}

    # The holder proves cheap -- a fraction of a core over a two-second
    # interval, against the reading a previous decision recorded -- which is
    # exactly what makes its preferred CPU lendable.  One cumulative reading
    # cannot establish a rate, so the earlier record is part of the setup.
    base = adaptive_cpu.local_state_base(queue.ledger().base)
    holder_name = next(path.name for path in queue.ledger().held_dir.iterdir()
                       if path.is_dir())
    key = holder['action_key']
    adaptive_cpu.write_json(adaptive_cpu.local_telemetry_path(queue.ledger().base, key), {
        'action_key': key, 'sampled_unix': clock[0] + 1, 'cpu_seconds': .1,
        'wall_seconds': 2., 'memory_current_bytes': 100, 'memory_peak_bytes': 100,
        'complete': True})
    adaptive_cpu.write_json(base / 'jobs.json', {
        holder_name: {'sampled_unix': clock[0], 'wall_seconds': 1., 'cpu_seconds': 0.}})
    clock[0] += 1

    # The fresh sample: the holder's preferred core is at 1.0 and psi "some"
    # is over the gate, while both free fallback cores are idle.
    state['psi'] = .633
    state['busy'] = {'0': 1., '1': 0., '2': 0.}
    publish('b' * 64)
    second = queue.claim(capacity=capacity, cpu_tiers=tiers, adaptive_cpu=True)
    assert second, 'the idle free fallback token still admits under the override'
    assert second['cpu_allocation'] == {'preferred': [], 'fallback': [1]}, \
        'the busy borrowed preferred core must not be selected'


@pytest.mark.parametrize('bad', [float('nan'), -0.1, 1.5, None])
def test_impossible_per_cpu_evidence_refuses_instead_of_being_ignored(tmp_path, monkeypatch, bad):
    """One unknown CPU is unknown evidence, never a CPU that is quietly idle."""
    from prismabuild import adaptive_cpu
    tiers = dict(NONCONTIGUOUS)
    queue = pool.PoolQueue(tmp_path / 'queue')
    _seed_queue(queue, tiers)
    monkeypatch.setattr(adaptive_cpu, 'action_identity', lambda item: ('shape', False))

    def sample(self):
        value = _host_sample([8, 10, 2, 4], {cpu: 0. for cpu in (8, 10, 2, 4)}, .633)
        value['per_cpu_busy']['4'] = bad
        return value

    monkeypatch.setattr(adaptive_cpu.Controller, 'sample', sample)
    queue.publish(action_key='d' * 64, cas_root=str(tmp_path / 'cas'),
                  checkout_root=str(tmp_path), worker_script='worker.py',
                  resources={'cpu': 1, 'mem_gb': 1})
    assert queue.claim(capacity={'cpu': 4, 'mem_gb': 2}, cpu_tiers=tiers,
                       adaptive_cpu=True) is None
