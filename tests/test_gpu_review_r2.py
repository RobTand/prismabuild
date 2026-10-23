"""Regressions from Opus r1, with the PB lead's three-outcome ruling."""
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

from gpu_evidence_fixtures import frozen_gpu, gpu_reply, worker_peer

from prismabuild import client, core as pb, gpu_memory as gm, pool, resource_scope
from test_core import _body
from test_resource_broker import authority, monitored  # noqa: F401
from test_pool_resource_scope import scoped  # noqa: F401

MIB, GIB = 1024 ** 2, 1024 ** 3
ZERO = {'gpu_devices_used': 0, 'gpu_uuids': [], 'gpu_evidence': {
    'sample_failed': False, 'live_sample': True, 'final_succeeded': True}}


# -- Finding 1: generic producers (demand only in pool resources) -------------

@pytest.fixture
def generic_gpu(tmp_path, monkeypatch):
    """A producer that declares GPU demand through publish, not action params.

    pool.py _start_resource_scope accepts this "trusted producer contract":
    params carry no demand and the item's resources say gpu=1.
    """
    checkout = tmp_path / 'checkout'
    checkout.mkdir()
    body = _body(checkout)
    body['inputs'] = []
    assert 'demand' not in body['params']
    action = pb.seal_action(body)
    cas = pb.PrismaBuildCAS(tmp_path / 'cas')
    cas.publish_action_request(action)
    queue = pool.PoolQueue(tmp_path / 'queue')
    demand = {'cpu': 1, 'mem_gb': 2, 'gpu': 1}
    queue.publish(action_key=action['action_key'], cas_root=cas.root, checkout_root=checkout,
                  worker_script='/worker.py', resources=demand, max_attempts=2, retry_safe=True)
    item = queue.claim(has_gpu=True, capacity=demand)
    assert item is not None and queue.demand_of(item).get('gpu') == 1
    monkeypatch.setattr(pool.cpu_admission, 'record_completion', lambda *a: None)
    monkeypatch.setattr(pool.cpu_admission.adaptive_snapshot, 'publish', lambda *a, **k: None)
    # The environment resource_exec gives every contained worker.
    nonce = 'a' * 32
    monkeypatch.setenv(pb.ACTION_NONCE_ENV, nonce)
    monkeypatch.setenv(pb.ACTION_SCOPE_ENV, 'prismabuild-job' + pb.raw_sha256(
        (action['action_key'] + nonce).encode())[:32] + '.slice')
    monkeypatch.setenv('PRISMABUILD_READER_HELPER_ROOT', str(Path(pb.__file__).resolve().parents[2]))
    monkeypatch.delenv(pb.ACTION_STATUS_PATH_ENV, raising=False)
    broker_calls = []

    def no_broker(*args):
        broker_calls.append(args)
        raise OSError('broker contacted')

    monkeypatch.setattr(pb.socket, 'socket', no_broker)
    monkeypatch.setattr(pb, 'GPU_OBSERVATION_TIMEOUT_S', .01)
    return queue, item, action, cas, checkout


def _broker_final(monkeypatch, queue, telemetry):
    item = json.loads(next(queue.records.dir(pool.record_store.CLAIMED).glob('*.json')).read_text())
    telemetry = {'gpu_record': frozen_gpu(telemetry, action_key=item['action_key'],
        nonce=(item.get('resource_scope') or {}).get('nonce'))}
    monkeypatch.setattr(queue, 'cleanup_action_containers', lambda *a, **k: {
        'complete': True, 'resource_scope': {'telemetry': dict(telemetry)}})


def test_generic_gpu_zero_use_leaves_no_success_receipt(generic_gpu, monkeypatch):
    queue, item, action, cas, checkout = generic_gpu
    monkeypatch.setattr(pb, '_gpu_broker_request', lambda *args, **fields: gpu_reply(action, ZERO, **fields))
    with pytest.raises(pb.LocalActionError, match='zero distinct GPU UUIDs'):
        pb.run_local_action(action, cas_root=cas.root, checkout_root=checkout,
                            resource_demand=queue.demand_of(item))
    _broker_final(monkeypatch, queue, ZERO)
    detail = {'status': 'executed', 'returncode': 0, 'stdout': '', 'stderr': '',
              'resource_telemetry': dict(ZERO)}
    path = queue.finish(item['action_key'], status='executed', detail=detail, claim_snapshot=item)
    terminal = json.loads(path.read_text())
    assert terminal['status'] == 'failed'          # Pool.finish refuses zero use...
    assert cas.lookup(action) is None


def test_generic_gpu_cache_hit_is_not_failed_by_idle_replay_scope(generic_gpu, monkeypatch, tmp_path):
    queue, item, action, cas, checkout = generic_gpu
    pb.run_local_action(action, cas_root=cas.root, checkout_root=checkout,
                        resource_demand=queue.demand_of(item))
    status_file = tmp_path / 'status.json'
    monkeypatch.setenv(pb.ACTION_STATUS_PATH_ENV, str(status_file))
    again = pb.run_local_action(action, cas_root=cas.root, checkout_root=checkout,
                                resource_demand=queue.demand_of(item))
    assert again['status'] == 'cache_hit'
    # pool.py _execute_in_checkout: lift the sidecar, then (no cached_execution
    # was written for this action) merge the replay scope's own broker sample.
    outcome = pool.PoolQueue._merge_action_status(
        {'status': 'cache_hit', 'returncode': 0, 'stdout': '', 'stderr': ''}, status_file)
    if not outcome.get('cached_execution'):
        outcome['resource_telemetry'] = {**ZERO, **outcome.get('resource_telemetry', {})}
    _broker_final(monkeypatch, queue, ZERO)
    path = queue.finish(item['action_key'], status='cache_hit', detail=outcome, claim_snapshot=item)
    terminal = json.loads(path.read_text())
    assert terminal['status'] == 'cache_hit', (
        f"valid cache hit filed {terminal['status']}: "
        f"{terminal['detail'].get('termination_reason')}")


def test_payload_written_cached_execution_cannot_override_broker_zero(generic_gpu, monkeypatch, tmp_path):
    queue, item, action, cas, checkout = generic_gpu
    _broker_final(monkeypatch, queue, ZERO)
    forged = {'status': 'executed', 'returncode': 0, 'stdout': '', 'stderr': '',
              'cached_execution': True,
              'resource_telemetry': {'gpu_devices_used': 1, 'gpu_uuids': ['GPU-forged']}}
    sidecar = tmp_path / 'status.json'
    sidecar.write_text(json.dumps(forged))
    merged = queue._merge_action_status({'returncode': 0}, sidecar)
    assert 'cached_execution' not in merged and 'resource_telemetry' not in merged
    path = queue.finish(item['action_key'], status='executed', detail=forged, claim_snapshot=item)
    terminal = json.loads(path.read_text())
    assert terminal['status'] == 'failed', (
        f"broker final census saw zero GPUs, sidecar fields made it {terminal['status']} "
        f"with {terminal['detail'].get('resource_telemetry')}")


def test_same_uid_payload_can_read_worker_environment(tmp_path):
    # How a payload learns PRISMABUILD_ACTION_STATUS_PATH (the sidecar it can
    # then rewrite): its parent's /proc environ is readable by the same UID.
    marker = str(tmp_path / 'sidecar.json')
    child = ('import os;'
             'env=open(f"/proc/{os.getppid()}/environ","rb").read().split(b"\\0");'
             f'print(any(e==b"{pb.ACTION_STATUS_PATH_ENV}="+{marker!r}.encode() for e in env))')
    # The grandchild reads its parent's (the wrapper's) environ, as a payload
    # reads the worker's.
    wrapper = [sys.executable, '-c',
               f'import subprocess,sys;subprocess.run([sys.executable,"-c",{child!r}],check=True)']
    out = subprocess.run(wrapper, capture_output=True, text=True,
                         env={**os.environ, pb.ACTION_STATUS_PATH_ENV: marker}, check=True)
    assert out.stdout.strip() == 'True'


# -- Finding 2: the payload-triggered census feeds the kill Guard ------------

class _RealGuardGpu:
    """gpu_memory with the production Guard; collect returns fixed jobs."""
    Scope = gm.Scope

    def __init__(self, jobs):
        self.jobs, self.samples = jobs, []

    def Guard(self):
        return gm.Guard()

    def collect(self, scopes, *, timeout_s, **kw):
        self.samples.append(time.monotonic())
        return gm.Snapshot(time.monotonic(), time.time(), 0.001, 64 * GIB, 32 * GIB, 0.0, 0.0,
                           tuple(self.jobs()), True)


def _job(record, path, *, gpu_bytes):
    info = path.stat()
    rows = ({'pid': 4242, 'start_ticks': 1, 'cgroup': '/prismabuild.slice/' + record['scope_id'],
             'gpu_uuid': 'GPU-b', 'used_bytes': gpu_bytes, 'memory_domain': 'discrete'},) if gpu_bytes else ()
    return gm.JobSample(
        scope_id=record['scope_id'], cgroup_identity=(info.st_dev, info.st_ino),
        budget_bytes=64 * MIB, host_bytes=MIB, gpu_reported_bytes=gpu_bytes,
        gpu_lower_bound_bytes=gpu_bytes, lower_bound_bytes=MIB, upper_bound_bytes=MIB + gpu_bytes,
        complete=True, processes=rows, gpu_budget_bytes=64 * MIB, memory_domain='discrete',
        shared_gpu_lower_bound_bytes=0, system_lower_bound_bytes=MIB)


def test_gpu_observation_rpc_does_not_shorten_guard_debounce(monitored, monkeypatch):
    a, backend, rows, _, monitor = monitored
    (req_a, rec_a, path_a), (req_b, rec_b, path_b) = rows
    gpu = _RealGuardGpu(lambda: [_job(rec_a, path_a, gpu_bytes=0),
                                 _job(rec_b, path_b, gpu_bytes=128 * MIB)])  # B over its 64 MiB
    live = type(monitor)(a, gpu)
    live.poll_once()                       # one periodic tick sees B over budget once
    assert backend.stopped == []
    peer = Path('/proc/123456789/cgroup')
    original = Path.read_text
    monkeypatch.setattr(Path, 'read_text', lambda f, *x, **k: (
        '0::/prismabuild.slice/' + rec_a['scope_id'] + '/payload\n') if f == peer else original(f, *x, **k))
    # Core in attempt A asks for its final observation, as every GPU action does.
    worker_peer(a, rec_a, monkeypatch)
    a.handle(a.uid, 123456789, {'op': 'gpu_observation', 'action_key': req_a['action_key'],
                                'nonce': req_a['nonce'], 'deadline_monotonic': time.monotonic() + 10})
    gap = gpu.samples[-1] - gpu.samples[-2]
    assert backend.stopped == [], (
        f"attempt B stopped by the Guard on two samples {gap * 1000:.1f} ms apart; "
        f"the second was attempt A's gpu_observation census, not the {live.interval_s}s cadence")


# -- Finding 3: GPU use between censuses is a terminal zero-use failure ------

def test_unsampled_short_gpu_use_is_not_a_terminal_zero(generic_gpu, monitored, monkeypatch):
    a, backend, rows, _, monitor = monitored
    req, rec, path = rows[0]
    # Census 1 covers the worker before the argv child starts.
    gpu = _RealGuardGpu(lambda: [_job(rec, path, gpu_bytes=0), _job(rows[1][1], rows[1][2], gpu_bytes=0)])
    live = type(monitor)(a, gpu)
    clock = [0.]
    context = lambda: .2 <= clock[0] < .7
    original_jobs = gpu.jobs
    gpu.jobs = lambda: [
        _job(rec, path, gpu_bytes=MIB if context() else 0), original_jobs()[1]]
    live.poll_once()
    # A fake child holds a CUDA context for 0.5 s, entirely between samples.
    # No process sample covers that child's lifetime.
    live_child = {'pid': 424242, 'parent': 123456789, 'starttime': '42'}
    clock[0] = .1
    a.records[rec['scope_id']]['gpu_payload'] = live_child
    monkeypatch.setattr(a, '_live_process', lambda pid: live_child if .1 <= clock[0] < .8 else None)
    clock[0] = .5
    assert context() and live._payload_live(a.records[rec['scope_id']])
    clock[0] = 1.
    assert not context() and not live._payload_live(a.records[rec['scope_id']])
    peer = Path('/proc/123456789/cgroup')
    original = Path.read_text
    monkeypatch.setattr(Path, 'read_text', lambda f, *x, **k: (
        '0::/prismabuild.slice/' + rec['scope_id'] + '/payload\n') if f == peer else original(f, *x, **k))
    worker_peer(a, rec, monkeypatch)
    final = a.handle(a.uid, 123456789, {'op': 'gpu_observation', 'action_key': req['action_key'],
                                        'nonce': req['nonce'], 'deadline_monotonic': time.monotonic() + 10})['gpu_record']
    queue, item, action, cas, checkout = generic_gpu
    monkeypatch.setattr(pb, '_gpu_broker_request', lambda *args, **fields: gpu_reply(action, final['resource_telemetry'], **fields))
    result = pb.run_local_action(action, cas_root=cas.root, checkout_root=checkout,
                                 resource_demand=queue.demand_of(item))
    assert result['receipt']['resource_telemetry']['gpu_use'] == 'unobserved'
    assert cas.lookup(action) is not None
    _broker_final(monkeypatch, queue, final['resource_telemetry'])
    path_t = queue.finish(item['action_key'], status='executed',
                          detail={'status': 'executed', 'returncode': 0, 'stdout': '', 'stderr': '',
                                  'resource_telemetry': dict(final)},
                          claim_snapshot=item)
    terminal = json.loads(path_t.read_text())
    assert pb.gpu_execution_judgment(final)['gpu_use'] == 'unobserved'
    assert terminal['detail']['gpu_use'] == 'unobserved'
    assert terminal['detail']['gpu_use_reason'] == 'no_sample_during_lifetime'
    assert path_t.parent.name != pool.record_store.FAILED


def test_guard_ignores_samples_only_1_8_ms_apart(monitored):
    from dataclasses import replace
    a, backend, rows, _, monitor = monitored
    job = _job(rows[0][1], rows[0][2], gpu_bytes=128 * MIB)
    snapshot = gm.Snapshot(1., 1., .001, 64 * GIB, 32 * GIB, 0., 0., (job,), True)
    guard = gm.Guard()
    assert guard.observe(snapshot) == []
    assert guard.observe(replace(snapshot, sampled_monotonic=1.0018)) == []
    assert guard.previous is snapshot
    assert len(guard.observe(replace(snapshot, sampled_monotonic=2.))) == 1


@pytest.mark.parametrize('outcome', ['used', 'zero', 'unobserved'])
def test_broker_core_and_pool_agree_on_all_three_outcomes(generic_gpu, monitored, monkeypatch, outcome):
    queue, item, action, cas, checkout = generic_gpu
    a, backend, rows, _, monitor = monitored
    req, rec, path = rows[0]
    gpu = _RealGuardGpu(lambda: [_job(rec, path, gpu_bytes=MIB if outcome == 'used' else 0)])
    live = type(monitor)(a, gpu)
    monkeypatch.setattr(live, '_payload_live', lambda record: True)
    if outcome == 'unobserved':
        def timeout(*a, **kw):
            raise subprocess.TimeoutExpired('nvidia-smi', 1.)
        monkeypatch.setattr(gpu, 'collect', timeout)
    live.poll_once()
    live.observe_gpu(rec['scope_id'], __import__('time').monotonic() + 10)
    observation = a.records[rec['scope_id']]['gpu_record']
    assert pb.gpu_execution_judgment(observation)['gpu_use'] == outcome
    monkeypatch.setattr(pb, '_gpu_broker_request', lambda *args, **fields: gpu_reply(action, observation['resource_telemetry'], **fields))
    if outcome == 'zero':
        with pytest.raises(pb.LocalActionError, match='zero distinct GPU UUIDs'):
            pb.run_local_action(action, cas_root=cas.root, checkout_root=checkout,
                                resource_demand=queue.demand_of(item))
        assert cas.lookup(action) is None
    else:
        result = pb.run_local_action(action, cas_root=cas.root, checkout_root=checkout,
                                     resource_demand=queue.demand_of(item))
        assert client.cas_receipt_self_check(result['receipt']) is None
        assert cas.lookup(action)['resource_telemetry']['gpu_use'] == outcome
    _broker_final(monkeypatch, queue, observation['resource_telemetry'])
    path_t = queue.finish(item['action_key'], status='executed', detail={'returncode': 0}, claim_snapshot=item)
    terminal = json.loads(path_t.read_text())
    assert terminal['status'] == ('failed' if outcome == 'zero' else 'executed')
    assert terminal['detail']['gpu_use'] == outcome
    if outcome == 'unobserved':
        assert terminal['detail']['gpu_use_reason'] == 'final_observation_failed'


def test_sealed_cpu_demand_overrides_gpu_fallback(generic_gpu, monkeypatch):
    queue, item, action, cas, checkout = generic_gpu
    body = {key: value for key, value in action.items() if key != 'action_key'}
    body['params'] = {**body['params'], 'demand': {'cpu': 1, 'mem_gb': 2}}
    sealed_cpu = pb.seal_action(body)
    assert pb.action_resource_demand(sealed_cpu, queue.demand_of(item)) == {'cpu': 1, 'mem_gb': 2}
    assert pb._action_needs_gpu(sealed_cpu, queue.demand_of(item)) is False


def test_payload_cannot_register_gpu_lifetime_for_worker(monitored, monkeypatch):
    a, backend, rows, _, monitor = monitored
    req, rec, path = rows[0]
    worker = {'pid': 11, 'parent': 1, 'starttime': '1'}
    payload = {'pid': 22, 'parent': 11, 'starttime': '2'}
    a.records[rec['scope_id']]['worker_process'] = worker
    monkeypatch.setattr(a, '_live_process', lambda pid: {11: worker, 22: payload}.get(pid))
    fields = {key: req[key] for key in ('action_key', 'nonce')}
    with pytest.raises(PermissionError, match='launched worker'):
        a.handle(a.uid, 22, {**fields, 'op': 'gpu_payload', 'payload_pid': 22})
    original = Path.read_text
    monkeypatch.setattr(Path, 'read_text', lambda file, *args, **kw: (
        '0::/prismabuild.slice/' + rec['scope_id'] + '/payload\n'
        if file in {Path('/proc/11/cgroup'), Path('/proc/22/cgroup')} else original(file, *args, **kw)))
    reply = a.handle(a.uid, 11, {**fields, 'op': 'gpu_payload', 'payload_pid': 22})
    assert reply['ok'] and a.records[rec['scope_id']]['gpu_payload'] == payload


def test_non_executable_cpu_history_has_no_cas_dependency(generic_gpu):
    from prismabuild import lifecycle
    queue, item, action, cas, checkout = generic_gpu
    status, detail, zero = lifecycle._gpu_finish_verdict(
        queue, {'action_key': item['action_key'], 'resources': {'cpu': 1}},
        'executed', {'returncode': 0}, ZERO)
    assert status == 'executed' and detail == {'returncode': 0} and not zero


def test_generic_gpu_worker_receives_admitted_fallback(generic_gpu):
    queue, item, action, cas, checkout = generic_gpu
    argv = pool.worker_argv(worker_script=item['worker_script'], action_key=item['action_key'],
                            cas_root=cas.root, checkout_root=checkout,
                            resource_demand=queue.demand_of(item))
    arguments = pb._build_parser().parse_args(argv[1:])
    assert arguments.resource_demand == queue.demand_of(item)
    assert pb.action_resource_demand(action, arguments.resource_demand)['gpu'] == 1


def test_broker_rpc_outage_cannot_leave_success_receipt_with_failed_terminal(generic_gpu, monkeypatch):
    queue, item, action, cas, checkout = generic_gpu
    # A periodic live-payload sample saw zero, but the final observation RPC
    # fails. Core may publish unobserved under the lead's outage ruling.
    result = pb.run_local_action(action, cas_root=cas.root, checkout_root=checkout,
                                 resource_demand=queue.demand_of(item))
    assert result['receipt']['resource_telemetry']['gpu_use'] == 'unobserved'
    _broker_final(monkeypatch, queue, ZERO)
    scope = {'nonce': os.environ[pb.ACTION_NONCE_ENV], 'scope_id': os.environ[pb.ACTION_SCOPE_ENV]}
    # The broker/claim scope is worker authority, independent of the sidecar.
    claimed = queue.records.item_path(pool.record_store.CLAIMED, item['action_key'])
    record = json.loads(claimed.read_text())
    record['resource_scope'] = scope
    queue.records.replace_record_at(claimed, record)
    path = queue.finish(item['action_key'], status='executed', detail={'returncode': 0}, claim_snapshot=record)
    terminal = json.loads(path.read_text())
    assert terminal['status'] == 'executed'
    assert terminal['detail']['gpu_use'] == 'unobserved'
    assert cas.lookup(action) is not None
