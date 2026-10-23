"""Opus review r2 probes for prismabuild-1744-v2 (c381024172).

Each test asserts the behaviour the lead's three-outcome ruling or the r1
fixes require; a failing test is a finding. All GPU evidence is simulated.
"""
import json
import os
import subprocess
import threading
import time
from pathlib import Path

import pytest

from prismabuild import core as pb, gpu_memory as gm, lifecycle, pool, resource_scope
from test_core import _body
from test_resource_broker import authority, monitored  # noqa: F401
from test_pool_resource_scope import scoped  # noqa: F401

MIB, GIB = 1024 ** 2, 1024 ** 3
PEER = 123456789


def ev(failed, live, final):
    return {'sample_failed': failed, 'live_sample': live, 'final_succeeded': final}


ZERO = {'gpu_devices_used': 0, 'gpu_uuids': [], 'gpu_evidence': ev(False, True, True)}
FAILED_RPC = (OSError, ValueError)


@pytest.fixture
def generic_gpu(tmp_path, monkeypatch):
    """Generic producer: GPU demand only in pool resources (r1 B1 fixture)."""
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
    assert item is not None
    monkeypatch.setattr(pool.cpu_admission, 'record_completion', lambda *a: None)
    monkeypatch.setattr(pool.cpu_admission.adaptive_snapshot, 'publish', lambda *a, **k: None)
    monkeypatch.setenv('PRISMABUILD_READER_HELPER_ROOT', str(Path(pb.__file__).resolve().parents[2]))
    monkeypatch.setenv('PRISMABUILD_RESOURCE_BROKER_SOCKET', str(tmp_path / 'run' / 'resources.sock'))
    monkeypatch.delenv(pb.ACTION_STATUS_PATH_ENV, raising=False)
    monkeypatch.setattr(pb.socket, 'socket', lambda *a: (_ for _ in ()).throw(OSError('no broker')))
    return queue, item, action, cas, checkout


def _scope_for(action_key, nonce):
    return 'prismabuild-job' + pb.raw_sha256((action_key + nonce).encode())[:32] + '.slice'


def _bind_env(monkeypatch, nonce, scope):
    monkeypatch.setenv(pb.ACTION_NONCE_ENV, nonce)
    monkeypatch.setenv(pb.ACTION_SCOPE_ENV, scope)


def _bind_claim(queue, item, nonce, scope):
    claimed = queue.records.item_path(pool.record_store.CLAIMED, item['action_key'])
    record = json.loads(claimed.read_text())
    record['resource_scope'] = {'nonce': nonce, 'scope_id': scope}
    queue.records.replace_record_at(claimed, record)
    return record


def _broker_final(monkeypatch, queue, telemetry):
    monkeypatch.setattr(queue, 'cleanup_action_containers', lambda *a, **k: {
        'complete': True, 'resource_scope': {'telemetry': dict(telemetry)}})


def _job(record, path, *, gpu_bytes, uuid='GPU-b'):
    info = path.stat()
    rows = ({'pid': 4242, 'start_ticks': 1, 'cgroup': '/prismabuild.slice/' + record['scope_id'],
             'gpu_uuid': uuid, 'used_bytes': gpu_bytes, 'memory_domain': 'discrete'},) if gpu_bytes else ()
    return gm.JobSample(
        scope_id=record['scope_id'], cgroup_identity=(info.st_dev, info.st_ino),
        budget_bytes=64 * MIB, host_bytes=MIB, gpu_reported_bytes=gpu_bytes,
        gpu_lower_bound_bytes=gpu_bytes, lower_bound_bytes=MIB, upper_bound_bytes=MIB + gpu_bytes,
        complete=True, processes=rows, gpu_budget_bytes=64 * MIB, memory_domain='discrete',
        shared_gpu_lower_bound_bytes=0, system_lower_bound_bytes=MIB)


class _ScriptedGpu:
    """gpu_memory with the production Guard; each collect follows a script."""
    Scope = gm.Scope

    def __init__(self, rows):
        self.rows, self.script, self.delay, self.samples = rows, [], 0.0, []

    def Guard(self):
        return gm.Guard()

    def collect(self, scopes, *, timeout_s, **kw):
        if self.delay:
            time.sleep(self.delay)
        self.samples.append(time.monotonic())
        step = self.script.pop(0) if self.script else 'zero'
        if step == 'fail':
            raise subprocess.TimeoutExpired('nvidia-smi', timeout_s)
        jobs = []
        for index, (req, rec, path) in enumerate(self.rows):
            gpu = 128 * MIB if (step == 'used' and index == 0) or (step == 'over_b' and index == 1) else 0
            jobs.append(_job(rec, path, gpu_bytes=gpu))
        return gm.Snapshot(time.monotonic(), time.time(), 0.001, 64 * GIB, 32 * GIB, 0.0, 0.0,
                           tuple(jobs), True)


def _peer_in(monkeypatch, scope):
    peer = Path(f'/proc/{PEER}/cgroup')
    original = Path.read_text
    monkeypatch.setattr(Path, 'read_text', lambda f, *x, **k: (
        '0::/prismabuild.slice/' + scope + '/payload\n') if f == peer else original(f, *x, **k))


# -- r1 B1: one demand source; sidecar carries no authority -----------------

def test_r1_b1_generic_zero_use_leaves_no_receipt_and_files_terminal_zero(generic_gpu, monkeypatch):
    queue, item, action, cas, checkout = generic_gpu
    nonce = 'a' * 32
    scope = _scope_for(action['action_key'], nonce)
    _bind_env(monkeypatch, nonce, scope)
    monkeypatch.setattr(pb, '_gpu_broker_request', lambda a, op, **f: {
        'ok': True, 'resource_telemetry': ZERO, 'nonce': nonce, 'scope_id': scope})
    with pytest.raises(pb.LocalActionError, match='zero distinct GPU UUIDs'):
        pb.run_local_action(action, cas_root=cas.root, checkout_root=checkout,
                            resource_demand=queue.demand_of(item))
    assert cas.lookup(action) is None
    record = _bind_claim(queue, item, nonce, scope)
    _broker_final(monkeypatch, queue, ZERO)
    path = queue.finish(item['action_key'], status='failed',
                        detail={'status': 'failed', 'returncode': 1, 'stdout': '', 'stderr': ''},
                        claim_snapshot=record)
    terminal = json.loads(path.read_text())
    assert path.parent.name == pool.record_store.FAILED
    assert terminal['detail']['gpu_use'] == 'zero'
    assert terminal['detail']['termination_reason'] == 'zero_gpu_devices_used'


def test_r1_b1_forged_sidecar_and_detail_cannot_select_evidence(generic_gpu, monkeypatch, tmp_path):
    queue, item, action, cas, checkout = generic_gpu
    forged = {'action_returncode': 0, 'cached_execution': True, 'gpu_use': 'used',
              'gpu_use_reason': 'attributed_gpu_uuid',
              'resource_telemetry': {'gpu_devices_used': 1, 'gpu_uuids': ['GPU-forged'],
                                     'gpu_use': 'used', 'gpu_evidence': ev(False, True, True)}}
    sidecar = tmp_path / 'status.json'
    sidecar.write_text(json.dumps(forged))
    merged = pool.PoolQueue._merge_action_status(
        {'status': 'executed', 'returncode': 0, 'stdout': '', 'stderr': ''}, sidecar)
    assert not ({'cached_execution', 'resource_telemetry', 'gpu_use'} & set(merged))
    _broker_final(monkeypatch, queue, ZERO)
    # Even a forged detail that reached finish directly selects no evidence.
    path = queue.finish(item['action_key'], status='executed', detail={**merged, **forged},
                        claim_snapshot=item)
    terminal = json.loads(path.read_text())
    assert terminal['status'] == 'failed' and terminal['detail']['gpu_use'] == 'zero'


def test_r1_b1_cache_hit_status_without_receipt_is_not_success(generic_gpu, monkeypatch):
    queue, item, action, cas, checkout = generic_gpu
    _broker_final(monkeypatch, queue, ZERO)
    path = queue.finish(item['action_key'], status='cache_hit',
                        detail={'status': 'cache_hit', 'returncode': 0, 'stdout': '', 'stderr': ''},
                        claim_snapshot=item)
    assert json.loads(path.read_text())['status'] == 'failed'


def test_r1_b1_valid_cache_hit_survives_idle_replay_broker_zero(generic_gpu, monkeypatch):
    queue, item, action, cas, checkout = generic_gpu
    nonce = 'a' * 32
    scope = _scope_for(action['action_key'], nonce)
    _bind_env(monkeypatch, nonce, scope)
    used = {'gpu_devices_used': 1, 'gpu_uuids': ['GPU-real'], 'gpu_evidence': ev(False, True, True)}
    monkeypatch.setattr(pb, '_gpu_broker_request', lambda a, op, **f: {
        'ok': True, 'resource_telemetry': used, 'nonce': nonce, 'scope_id': scope})
    first = pb.run_local_action(action, cas_root=cas.root, checkout_root=checkout,
                                resource_demand=queue.demand_of(item))
    assert first['receipt']['resource_telemetry']['gpu_use'] == 'used'
    again = pb.run_local_action(action, cas_root=cas.root, checkout_root=checkout,
                                resource_demand=queue.demand_of(item))
    assert again['status'] == 'cache_hit'
    _broker_final(monkeypatch, queue, ZERO)       # the replay scope's own idle broker zero
    path = queue.finish(item['action_key'], status='cache_hit',
                        detail={'status': 'cache_hit', 'returncode': 0, 'stdout': '', 'stderr': ''},
                        claim_snapshot=item)
    terminal = json.loads(path.read_text())
    assert terminal['status'] == 'cache_hit' and terminal['detail']['gpu_use'] == 'used'


def test_r1_b1_core_cache_hit_race_is_not_filed_zero(generic_gpu, monkeypatch):
    """Pool's pre-launch lookup missed; Core then hit the cache (no payload)."""
    queue, item, action, cas, checkout = generic_gpu
    nonce = 'a' * 32
    scope = _scope_for(action['action_key'], nonce)
    other_nonce = 'f' * 32
    other_scope = _scope_for(action['action_key'], other_nonce)
    used = {'gpu_devices_used': 1, 'gpu_uuids': ['GPU-real'], 'gpu_evidence': ev(False, True, True)}
    monkeypatch.setattr(pb, '_gpu_broker_request', lambda a, op, **f: {
        'ok': True, 'resource_telemetry': used, 'nonce': other_nonce, 'scope_id': other_scope})
    _bind_env(monkeypatch, other_nonce, other_scope)
    pb.run_local_action(action, cas_root=cas.root, checkout_root=checkout,
                        resource_demand=queue.demand_of(item))   # another attempt published
    record = _bind_claim(queue, item, nonce, scope)
    _broker_final(monkeypatch, queue, {'gpu_devices_used': 0, 'gpu_uuids': [],
                                       'gpu_evidence': ev(False, False, None)})
    path = queue.finish(item['action_key'], status='executed',
                        detail={'status': 'executed', 'returncode': 0, 'stdout': '', 'stderr': ''},
                        claim_snapshot=record)
    assert json.loads(path.read_text())['status'] == 'executed'


# -- Q2: planted recovery evidence cannot fake used --------------------------

@pytest.mark.parametrize('plant', ['same_uid_file', 'symlinked_dir'])
def test_q2_same_uid_planted_recovery_evidence_cannot_fake_used(generic_gpu, monkeypatch, tmp_path, plant):
    queue, item, action, cas, checkout = generic_gpu
    nonce = 'a' * 32
    scope = _scope_for(action['action_key'], nonce)
    _bind_env(monkeypatch, nonce, scope)
    forged = {'action_key': action['action_key'], 'nonce': nonce, 'scope_id': scope,
              'resource_telemetry': {'gpu_devices_used': 1, 'gpu_uuids': ['GPU-forged'],
                                     'gpu_evidence': ev(False, True, True)}}
    run = tmp_path / 'run'
    target = tmp_path / 'elsewhere' if plant == 'symlinked_dir' else run / 'gpu-observations'
    target.mkdir(parents=True)
    (target / (scope + '.json')).write_text(json.dumps(forged))
    os.chmod(target / (scope + '.json'), 0o444)
    if plant == 'symlinked_dir':
        run.mkdir(exist_ok=True)
        (run / 'gpu-observations').symlink_to(target)

    def outage(*a, **k):
        raise OSError('broker RPC failed')

    monkeypatch.setattr(pb, '_gpu_broker_request', outage)
    result = pb.run_local_action(action, cas_root=cas.root, checkout_root=checkout,
                                 resource_demand=queue.demand_of(item))
    telemetry = result['receipt']['resource_telemetry']
    assert telemetry['gpu_use'] == 'unobserved' and telemetry['gpu_uuids'] == []


# -- r1 B2: the RPC and the Guard ------------------------------------------

def test_r1_b2_final_observation_does_not_shorten_guard_debounce(monitored, monkeypatch):
    a, backend, rows, _, monitor = monitored
    (req_a, rec_a, _), (req_b, rec_b, _) = rows
    gpu = _ScriptedGpu(rows)
    gpu.script = ['over_b', 'over_b']
    live = type(monitor)(a, gpu)
    live.poll_once()
    assert backend.stopped == []
    _peer_in(monkeypatch, rec_a['scope_id'])
    a.handle(a.uid, PEER, {'op': 'gpu_observation', 'action_key': req_a['action_key'],
                           'nonce': req_a['nonce']})
    assert backend.stopped == []


@pytest.mark.parametrize('flooders', [0, 32])
def test_r2_payload_rpc_flood_cannot_blind_the_guard_for_other_scopes(monitored, monkeypatch, flooders):
    """Scope A loops on gpu_observation; scope B is over its GPU budget.

    The production monitor (1 s interval, 5 s continuity) must still stop B.
    Each census holds the shared sample lock for 0.25 s (a GB10-scale
    nvidia-smi plus /proc scan); A's RPCs are unthrottled.
    """
    a, backend, rows, _, monitor = monitored
    (req_a, rec_a, _), (req_b, rec_b, _) = rows
    gpu = _ScriptedGpu(rows)
    gpu.delay = 0.25
    gpu.script = []
    original_collect = gpu.collect

    def collect(scopes, *, timeout_s, **kw):
        gpu.script.append('over_b')
        return original_collect(scopes, timeout_s=timeout_s, **kw)

    gpu.collect = collect
    live = type(monitor)(a, gpu)
    polls = []
    inner = live._poll_once

    def timed_poll():
        polls.append(time.monotonic())
        return inner()

    live._poll_once = timed_poll
    stopped = threading.Event()
    original_stop = backend.stop

    def stop(scope):
        original_stop(scope)
        if scope == rec_b['scope_id']:
            stopped.set()

    monkeypatch.setattr(backend, 'stop', stop)
    _peer_in(monkeypatch, rec_a['scope_id'])
    done = threading.Event()

    def flood():
        while not done.is_set():
            try:
                a.handle(a.uid, PEER, {'op': 'gpu_observation', 'action_key': req_a['action_key'],
                                       'nonce': req_a['nonce']})
            except Exception:                                    # noqa: BLE001
                pass

    threads = [threading.Thread(target=flood, daemon=True) for _ in range(flooders)]
    for thread in threads:
        thread.start()
    started = time.monotonic()
    live.start()
    try:
        was_stopped = stopped.wait(15)
    finally:
        done.set()
        live.close()
        for thread in threads:
            thread.join(timeout=5)
    gaps = [round(b - a_, 2) for a_, b in zip(polls, polls[1:])]
    assert was_stopped, (
        f"over-budget scope B never stopped in {time.monotonic() - started:.1f}s while scope A "
        f"made {len(gpu.samples) - len(polls)} gpu_observation censuses; periodic poll gaps {gaps} "
        f"exceed the Guard's 5 s continuity window")


# -- r1 B3 / Q3: complete-attempt histories, Core vs Pool --------------------

HISTORIES = {
    # name: (samples while payload alive, final observation, samples after final)
    'early_zero_then_final_failure': (['zero'], 'fail', []),
    'earlier_timeout_then_final_success': (['fail', 'zero'], 'zero', []),
    'final_only_use': (['zero'], 'used', []),
    'earlier_use_then_final_failure': (['used'], 'fail', []),
    'no_sample_during_lifetime': ([], 'zero', []),
    'all_success_zero': (['zero', 'zero'], 'zero', []),
    'zero_then_post_final_failed_sample': (['zero'], 'zero', ['fail']),
    'zero_then_post_final_lingering_use': (['zero'], 'zero', ['used']),
}
EXPECTED = {
    'early_zero_then_final_failure': 'unobserved',
    'earlier_timeout_then_final_success': 'unobserved',
    'final_only_use': 'used',
    'earlier_use_then_final_failure': 'used',
    'no_sample_during_lifetime': 'unobserved',
    'all_success_zero': 'zero',
    'zero_then_post_final_failed_sample': 'zero',
    'zero_then_post_final_lingering_use': 'zero',
}


@pytest.mark.parametrize('name', sorted(HISTORIES))
def test_q3_core_and_pool_land_in_the_same_outcome(generic_gpu, monitored, monkeypatch, name):
    queue, item, action, cas, checkout = generic_gpu
    a, backend, rows, _, monitor = monitored
    req, rec, _ = rows[0]
    alive_samples, final, after = HISTORIES[name]
    gpu = _ScriptedGpu(rows)
    live = type(monitor)(a, gpu)
    alive = {'value': False}
    monkeypatch.setattr(live, '_payload_live', lambda record: alive['value'])
    live.poll_once()                                  # before the argv child starts
    alive['value'] = True
    for step in alive_samples:
        gpu.script = [step]
        live.poll_once()
    alive['value'] = False                             # argv exited; Core observes
    core_scope = _scope_for(action['action_key'], req['nonce'])
    _bind_env(monkeypatch, req['nonce'], core_scope)
    _peer_in(monkeypatch, rec['scope_id'])

    def route(action_, op, **fields):
        if op != 'gpu_observation':
            raise PermissionError('registration is simulated by _payload_live')
        gpu.script = [final]
        reply = a.handle(a.uid, PEER, {'op': op, 'action_key': req['action_key'],
                                       'nonce': req['nonce'], **fields})
        # The fixture broker record has another action key; bind its reply
        # to Core's own scope, as the production broker would.
        return {**reply, 'scope_id': core_scope}

    monkeypatch.setattr(pb, '_gpu_broker_request', route)
    try:
        result = pb.run_local_action(action, cas_root=cas.root, checkout_root=checkout,
                                     resource_demand=queue.demand_of(item))
        core = result['receipt']['resource_telemetry']['gpu_use']
        status, detail = 'executed', {'status': 'executed', 'returncode': 0, 'stdout': '', 'stderr': ''}
    except pb.LocalActionError as error:
        assert 'zero distinct GPU UUIDs' in str(error)
        core = 'zero'
        status, detail = 'failed', {'status': 'failed', 'returncode': 1, 'stdout': '', 'stderr': str(error)}
    for step in after:                                 # periodic polls before Pool cleanup
        gpu.script = [step]
        live.poll_once()
    record = _bind_claim(queue, item, req['nonce'], core_scope)
    _broker_final(monkeypatch, queue, a.records[rec['scope_id']]['resource_telemetry'])
    path = queue.finish(item['action_key'], status=status, detail=detail, claim_snapshot=record)
    terminal = json.loads(path.read_text())
    pool_outcome = terminal['detail'].get('gpu_use')
    assert core == EXPECTED[name], f'Core judged {core}'
    assert pool_outcome == core, (
        f"{name}: Core judged {core} (receipt {'refused' if core == 'zero' else 'published'}), "
        f"Pool filed {terminal['status']} in {path.parent.name}/ with gpu_use={pool_outcome} "
        f"({terminal['detail'].get('gpu_use_reason')})")
    assert (path.parent.name == pool.record_store.FAILED) == (core == 'zero')


# -- Q3: the late-finish path re-judges a zero attempt from empty evidence ---

@pytest.mark.parametrize('scoped', ['gpu'], indirect=True)
def test_q3_late_finisher_keeps_the_zero_verdict(scoped, monkeypatch):
    queue, first, calls = scoped
    key = first['action_key']
    queue._start_resource_scope(first)
    # As test_pool_late_scope._replaced_scope, with a GPU-capable reclaim.
    queue.records.item_path(pool.record_store.CLAIMED, key).unlink()
    queue.records.lease_path(key).unlink()
    queue.ledger().release(key)
    queue.publish(action_key=key, cas_root=first['cas_root'],
                  checkout_root=first['checkout_root'], worker_script=first['worker_script'],
                  resources=first['resources'], max_attempts=2, retry_safe=True)
    newer = queue.claim(owner='newer-attempt', has_gpu=True, capacity=first['resources'])
    assert newer is not None
    queue._start_resource_scope(newer)
    old = first['resource_scope']['nonce']
    assert old != newer['resource_scope']['nonce']
    request = resource_scope.ResourceScope._request

    def status_with_zero(scope, op, **extra):
        reply = request(scope, op, **extra)
        if op == 'status' and scope.nonce == old:
            reply = {**reply, 'resource_telemetry': dict(ZERO)}
        return reply

    monkeypatch.setattr(resource_scope.ResourceScope, '_request', status_with_zero)
    result = queue.finish(first['action_key'], status='failed',
                          detail={'status': 'failed', 'returncode': 1, 'stdout': '',
                                  'stderr': 'GPU action used zero distinct GPU UUIDs'},
                          claim_snapshot=first)
    archived = json.loads(result.read_text())
    detail = archived['detail']
    assert (detail.get('gpu_use'), archived.get('disposition')) == ('zero', 'failed'), (
        f"late finisher filed disposition={archived.get('disposition')} gpu_use={detail.get('gpu_use')} "
        f"({detail.get('gpu_use_reason')}) with termination_reason={detail.get('termination_reason')} "
        f"over broker telemetry that judged zero")
