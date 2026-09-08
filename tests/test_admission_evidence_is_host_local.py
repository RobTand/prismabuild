"""Admission reads holder telemetry and GPU probe state from the host, not the mount.

Issue #266, second half. PR #355 moved the CPU sample, profiles and borrowing
state under host-local authority. Two host-private records were still read --
and one still written by rename -- on the shared mount inside the admission
critical section: the per-holder telemetry the executing box samples for its
own actions, and the GPU probe state (``gpu-state.json``). Both move under the
same ``PRISMABUILD_BOX_STATE_ROOT`` directory and are published to the old
shared paths by the same independent publisher, so remote readers keep their
schema and their paths.

Tokens do not move. ``resolve_claim_holder`` treats another host's committed
``held/<key>`` reservation as the exact claim owner and the reaper releases a
dead holder's tokens from whatever box sweeps; that is cross-host evidence and
stays on the mount. The ownership tests for it are elsewhere
(``test_pool_ambiguous_holder_recovery``, ``test_pool_holder_contradiction``).
"""
import json
from pathlib import Path
import socket
import sys
import time

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tools/fleet'))
from prismabuild import adaptive_cpu, adaptive_gpu, adaptive_snapshot, pool


@pytest.fixture(autouse=True)
def reap_publishers():
    yield
    for child in adaptive_snapshot._children:
        assert child.wait(timeout=10) == 0
    adaptive_snapshot._children.clear()


def _await_publishers():
    for child in list(adaptive_snapshot._children):
        assert child.wait(timeout=10) == 0
    adaptive_snapshot._children.clear()


@pytest.fixture
def cpu_rig(tmp_path, monkeypatch):
    """A two-CPU box whose first holder reserved both CPUs and uses a fraction."""
    clock = [100.]
    monkeypatch.setattr(adaptive_cpu.time, 'time', lambda: clock[0])
    state = {'busy_cpus': 0., 'psi_some': 0.}
    monkeypatch.setattr(adaptive_cpu.Controller, 'sample', lambda self: dict(
        sampled_unix=clock[0], cpu_count=2, interval_s=1., **state))
    monkeypatch.setattr(adaptive_cpu, 'action_identity', lambda item: ('shape', False))
    queue = pool.PoolQueue(tmp_path / 'queue')
    tiers = {'preferred': [0], 'fallback': [1]}

    def publish(index, cpu=1):
        key = f'{index:064x}'
        queue.publish(action_key=key, cas_root=str(tmp_path / 'cas'),
                      checkout_root=str(tmp_path), worker_script='worker.py',
                      resources={'cpu': cpu, 'mem_gb': 1})
        return key

    def claim():
        return queue.claim(capacity={'cpu': 2, 'mem_gb': 4, 'gpu': 1},
                           cpu_tiers=tiers, adaptive_cpu=True)

    def record(key, cpu):
        return {'action_key': key, 'sampled_unix': clock[0], 'cpu_seconds': cpu,
                'wall_seconds': clock[0] - 100, 'memory_current_bytes': 100,
                'memory_peak_bytes': 100, 'complete': True}

    key = publish(1, cpu=2)
    first = claim()
    assert first
    state['busy_cpus'] = .1
    publish(2)
    return queue, clock, key, first, claim, record


def test_cpu_admission_credits_only_host_local_holder_telemetry(cpu_rig):
    queue, clock, key, first, claim, record = cpu_rig
    local = adaptive_cpu.local_telemetry_path(queue.ledger().base, key)
    assert local.is_relative_to(adaptive_cpu.local_state_base(queue.ledger().base))
    assert not local.is_relative_to(queue.root)
    clock[0] += 1
    adaptive_cpu.write_json(local, record(key, .1))
    assert claim() is None          # one cumulative reading is not a rate
    clock[0] += 1
    adaptive_cpu.write_json(local, record(key, .2))
    second = claim()
    assert second and second['cpu_allocation'] == {'preferred': [0], 'fallback': []}
    # The shared path was never written: nothing published telemetry, and the
    # decision did not need it.
    assert not (queue.ledger().base / 'telemetry' / f'{key}.json').exists()


def test_a_shared_telemetry_copy_cannot_grant_borrowed_capacity(cpu_rig):
    queue, clock, key, first, claim, record = cpu_rig
    shared = queue.ledger().base / 'telemetry' / f'{key}.json'
    for cpu in (.1, .2):
        clock[0] += 1
        adaptive_cpu.write_json(shared, record(key, cpu))
        assert claim() is None
    # Only a host-local record is attribution. The same readings written where
    # the executing box's sampler writes them lend the CPU: a rate takes two
    # decisions over two records, so the first local reading is still refused.
    local = adaptive_cpu.local_telemetry_path(queue.ledger().base, key)
    clock[0] += 1
    adaptive_cpu.write_json(local, record(key, .3))
    assert claim() is None
    clock[0] += 1
    adaptive_cpu.write_json(local, record(key, .4))
    assert claim()


@pytest.fixture
def gpu_rig(tmp_path, monkeypatch):
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

    def publish(index):
        key = f'{index:064x}'
        queue.publish(action_key=key, cas_root=str(tmp_path / 'cas'),
                      checkout_root=str(tmp_path), worker_script='worker.py',
                      resources={'cpu': 1, 'mem_gb': 1, 'gpu': 1}, needs_gpu=True)
        return key

    def tick(seconds=2):
        """Advance the broker sample and write each holder's telemetry locally."""
        clock[0] += seconds
        sample.update(sampled_unix=clock[0], sample_id=str(clock[0]))
        sample['jobs'] = []
        for key in queue.ledger().held_keys():
            record = {'action_key': key, 'nonce': key + '-attempt', 'scope_unit': key + '-scope',
                      'sampled_unix': clock[0], 'cpu_seconds': .01 * (clock[0] - 100),
                      'wall_seconds': clock[0] - 100, 'complete': True}
            adaptive_cpu.write_json(
                adaptive_cpu.local_telemetry_path(queue.ledger().base, key), record)
            sample['jobs'].append({'action_key': key, 'nonce': record['nonce'],
                                   'scope_id': record['scope_unit'], 'complete': True})

    def claim():
        return queue.claim(**args)

    return queue, clock, sample, capacity, publish, tick, claim


def test_gpu_probe_state_is_host_local_and_published_after_admission_is_released(gpu_rig):
    queue, clock, sample, capacity, publish, tick, claim = gpu_rig
    publish(1)
    assert claim()
    local = adaptive_cpu.local_state_base(queue.ledger().base) / 'gpu-state.json'
    state = json.loads(local.read_text())
    assert state['sample_id'] == '100' and state['low_samples'] == 1
    assert state['sampled_unix'] == 100.
    _await_publishers()
    published = json.loads((queue.ledger().base / 'adaptive' / 'gpu-state.json').read_text())
    assert published.pop('_snapshot')['source'] == 'host-local'
    assert published == state


def test_no_shared_adaptive_or_telemetry_io_inside_gpu_admission(gpu_rig, monkeypatch):
    queue, clock, sample, capacity, publish, tick, claim = gpu_rig
    shared = queue.ledger().base
    forbidden = (shared / 'adaptive', shared / 'telemetry')
    read, write = adaptive_cpu.read_json, adaptive_cpu.write_json

    def guard(function):
        def guarded(path, *rest):
            if any(Path(path).is_relative_to(root) for root in forbidden):
                pytest.fail(f'shared admission bookkeeping touched under the lock: {path}')
            return function(path, *rest)
        return guarded

    monkeypatch.setattr(adaptive_cpu, 'read_json', guard(read))
    monkeypatch.setattr(adaptive_cpu, 'write_json', guard(write))
    for index in range(3):
        publish(index)
    assert claim()
    tick()
    assert claim()                  # the probe path reads every holder's telemetry
    tick()
    claim()                         # a refusal here is a probe decision, not an I/O one


def test_a_cold_host_imports_nothing_from_the_shared_gpu_snapshot(gpu_rig):
    """A restarted box relearns from its own samples; the diagnostic copy is not authority.

    A worker-loop restart keeps the box-state directory, so this is the harder
    case: the local state is gone (a reboot cleared the root) while the shared
    snapshot still claims probe credit. Nothing is minted or released while the
    controller is refusing, and the second fresh sample re-earns admission.
    """
    queue, clock, sample, capacity, publish, tick, claim = gpu_rig
    for index in range(3):
        publish(index)
    first = claim()
    assert first
    _await_publishers()
    ledger = queue.ledger()
    local = adaptive_cpu.local_state_base(ledger.base) / 'gpu-state.json'
    local.unlink()
    shared = ledger.base / 'adaptive' / 'gpu-state.json'
    snapshot = json.loads(shared.read_text())
    snapshot.update(low_samples=3, sampled_unix=clock[0], sample_id=str(clock[0]))
    shared.write_text(json.dumps(snapshot))
    tokens = (ledger.held(), sorted(p.name for p in ledger.free_dir.iterdir()),
              ledger.capacity())
    tick()
    assert claim() is None
    assert (ledger.held(), sorted(p.name for p in ledger.free_dir.iterdir()),
            ledger.capacity()) == tokens
    assert json.loads(local.read_text())['low_samples'] == 1
    tick()
    assert claim()
    assert ledger.held()['mem_gb'] == 2


def test_pbstatus_reads_the_published_gpu_snapshot_from_the_shared_path(gpu_rig, monkeypatch):
    import pbstatus
    queue, clock, sample, capacity, publish, tick, claim = gpu_rig
    host = socket.gethostname()
    queue.announce(host=host, tags=['x86'], has_gpu=True, capacity=capacity,
                   observed_capacity=capacity)
    publish(1)
    assert claim()
    _await_publishers()
    result = pbstatus.read_pool(queue.root)
    node = next(n for n in result['nodes'] if n['node'] == host)
    assert node['state'] == 'live'
    gpu = node['admission']['gpu']
    assert gpu['state'] == 'fresh'
    assert gpu['record']['sample_id'] == '100'
    assert gpu['record']['_snapshot']['source'] == 'host-local'
