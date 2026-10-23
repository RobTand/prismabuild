"""Opus review r3 probes for prismabuild-1744-v2 (dd3535da0c).

Each test asserts what the lead's three-outcome ruling, its r3 lost-reply
refinement or repair-r3.md requires; a failing test is a finding. All GPU
evidence is simulated and no GPU is used.

Adaptations from review-r2-probes.py:
- gpu_observation now requires ``deadline_monotonic`` and the launched
  worker's PID/start time, so callers go through ``worker_peer`` (worker) or a
  live child identity (payload).
- r2 re-wrapped broker telemetry for Core's action key. Here ``e2e`` creates
  the broker scope for Core's OWN action key and nonce, so Core, the broker,
  Pool and every reader judge one shared frozen record.
- The production mirror reader requires root ownership; probes run as rob, so
  ``_rob_reader`` keeps the same path and judgment validation without the
  ownership checks (verified separately by code reading).
"""
import ast
import json
import os
import re
import secrets
import socket
import subprocess
import sys
import textwrap
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from gpu_evidence_fixtures import frozen_gpu, worker_peer
from prismabuild import (action_client, action_result, census, core as pb, endings,
                         gpu_memory as gm, lifecycle, pool, resource_scope)
from test_core import _body
from test_pbwait import pbwait
from test_pool_resource_scope import scoped  # noqa: F401
from test_resource_broker import authority, monitored  # noqa: F401

MIB, GIB = 1024 ** 2, 1024 ** 3
WORKER, PAYLOAD = 123456789, 123456790
REPO = Path(pb.__file__).resolve().parents[2]


def _broker(a):
    return type(a).__init__.__globals__


def _job(record, path, *, uuid=None, used=128 * MIB, cgroup_suffix='/payload'):
    info = path.stat()
    rows = ({'pid': 4242, 'start_ticks': 1, 'cgroup': '/prismabuild.slice/' + record['scope_id'] + cgroup_suffix,
             'gpu_uuid': uuid, 'used_bytes': used, 'memory_domain': 'discrete'},) if uuid else ()
    gpu = used if uuid and used is not None else 0
    return gm.JobSample(
        scope_id=record['scope_id'], cgroup_identity=(info.st_dev, info.st_ino),
        budget_bytes=64 * MIB, host_bytes=MIB, gpu_reported_bytes=gpu,
        gpu_lower_bound_bytes=gpu, lower_bound_bytes=MIB, upper_bound_bytes=MIB + gpu,
        complete=True, processes=rows, gpu_budget_bytes=64 * MIB, memory_domain='discrete',
        shared_gpu_lower_bound_bytes=0, system_lower_bound_bytes=MIB)


class _Gpu:
    """Production Guard; each census follows ``plan()``: {scope: uuid} or 'fail'."""
    Scope = gm.Scope

    def __init__(self, rows):
        self.rows, self.samples, self.delay = rows, [], 0.0
        self.plan = lambda: {}

    def Guard(self):
        return gm.Guard()

    def collect(self, scopes, *, timeout_s, **kw):
        if self.delay:
            time.sleep(self.delay)
        step = self.plan()
        self.samples.append((threading.current_thread().name, time.monotonic()))
        if step == 'fail':
            raise subprocess.TimeoutExpired('nvidia-smi', timeout_s)
        wanted = {scope.scope_id for scope in scopes}
        jobs = [_job(rec, path, uuid=step.get(rec['scope_id'])) for rec, path in self.rows
                if rec['scope_id'] in wanted]
        return gm.Snapshot(time.monotonic(), time.time(), 0.001, 64 * GIB, 32 * GIB, 0.0, 0.0,
                           tuple(jobs), True)


STEP = {'zero': {}, 'fail': 'fail'}


def _step(e, name):
    return STEP.get(name, {e.scope: 'GPU-' + name} if name == 'used' else {})


@pytest.fixture
def e2e(tmp_path, monkeypatch, authority):
    """Core, broker, Pool and readers sharing one action, nonce and scope."""
    a, backend = authority
    checkout = tmp_path / 'checkout'
    checkout.mkdir()
    body = _body(checkout)
    body['inputs'] = []
    action = pb.seal_action(body)
    cas = pb.PrismaBuildCAS(tmp_path / 'cas')
    cas.publish_action_request(action)
    queue = pool.PoolQueue(tmp_path / 'queue')
    demand = {'cpu': 1, 'mem_gb': 2, 'gpu': 1}
    queue.publish(action_key=action['action_key'], cas_root=cas.root, checkout_root=checkout,
                  worker_script='/worker.py', resources=demand, max_attempts=2, retry_safe=True)
    item = queue.claim(has_gpu=True, capacity=demand)
    assert item is not None
    monkeypatch.setattr(pool.cpu_admission, 'record_completion', lambda *x: None)
    monkeypatch.setattr(pool.cpu_admission.adaptive_snapshot, 'publish', lambda *x, **k: None)
    monkeypatch.setenv('PRISMABUILD_READER_HELPER_ROOT', str(REPO))
    monkeypatch.delenv(pb.ACTION_STATUS_PATH_ENV, raising=False)
    monkeypatch.setattr(pb.socket, 'socket', lambda *x: (_ for _ in ()).throw(OSError('no broker socket')))
    monkeypatch.setattr(pb, 'GPU_OBSERVATION_TIMEOUT_S', 0.3)
    cgroot = tmp_path / 'cg'
    monkeypatch.setattr(backend, 'path', lambda scope: cgroot / 'prismabuild.slice' / scope, raising=False)
    nonce = secrets.token_hex(16)
    created = a.handle(os.getuid(), os.getpid(), {'op': 'create', 'action_key': action['action_key'],
                                                  'nonce': nonce, 'memory_max_bytes': 64 * MIB})
    scope = created['scope_id']
    path = backend.path(scope)
    path.mkdir(parents=True)
    (path / 'memory.events').write_text('oom_kill 0\n')
    (path / 'memory.events.local').write_text('oom 0\n')
    (path / 'memory.current').write_text(str(MIB))
    info = path.stat()
    a.records[scope]['cgroup_identity'] = [info.st_dev, info.st_ino]
    a.records[scope]['memory_oom_kill_baseline'] = 0
    monkeypatch.setenv('PRISMABUILD_RESOURCE_BROKER_SOCKET', str(tmp_path / 'resources.sock'))
    assert a.gpu_evidence_dir == tmp_path / 'gpu-observations'
    monkeypatch.setenv(pb.ACTION_NONCE_ENV, nonce)
    monkeypatch.setenv(pb.ACTION_SCOPE_ENV, scope)
    worker_peer(a, created, monkeypatch, WORKER)
    gpu = _Gpu([(created, path)])
    live = _broker(a)['ResourceMonitor'](a, gpu)
    alive = {'value': False}
    monkeypatch.setattr(live, '_payload_live', lambda record: alive['value'])
    monkeypatch.setattr(pb, '_read_broker_gpu_evidence', _rob_reader)
    return SimpleNamespace(a=a, backend=backend, action=action, cas=cas, queue=queue, item=item,
                           checkout=checkout, nonce=nonce, scope=scope, token=created['token'],
                           path=path, gpu=gpu, live=live, alive=alive, monkeypatch=monkeypatch,
                           tmp_path=tmp_path, cgroot=cgroot)


def _rob_reader(action):
    """pb._read_broker_gpu_evidence minus root ownership (probes run as rob)."""
    scope, nonce = os.environ[pb.ACTION_SCOPE_ENV], os.environ[pb.ACTION_NONCE_ENV]
    leaf = Path(os.environ['PRISMABUILD_RESOURCE_BROKER_SOCKET']).parent / 'gpu-observations' / (scope + '.json')
    evidence = pb.digest_primitives.decode_strict_json(leaf.read_bytes(), where='broker GPU evidence')
    pb.gpu_execution_judgment(evidence, action_key=str(action['action_key']), nonce=nonce, scope_id=scope)
    return evidence


def _lifetime(e, alive_steps, before=True):
    if before:
        e.alive['value'] = False
        e.gpu.plan = lambda: {}
        e.live.poll_once()
    e.alive['value'] = True
    for name in alive_steps:
        step = _step(e, name)
        e.gpu.plan = lambda step=step: step
        e.live.poll_once()
    e.alive['value'] = False


def _route(e, mode, final='zero'):
    """Core's broker transport; the broker is the real Authority."""
    def request(action_, op, **fields):
        if op != 'gpu_observation':
            raise PermissionError('payload registration is modelled by _payload_live')
        step = _step(e, final)
        e.gpu.plan = lambda: step
        msg = {'op': op, 'action_key': action_['action_key'], 'nonce': e.nonce, **fields}
        if mode == 'deliver':
            return e.a.handle(e.a.uid, WORKER, msg)
        if mode == 'lose_reply':
            e.a.handle(e.a.uid, WORKER, msg)
            raise OSError('reply lost after the broker answered')
        if mode == 'refused':
            return e.a.handle(e.a.uid, PAYLOAD, msg)
        if mode == 'timeout':
            box = {}
            worker = threading.Thread(target=lambda: box.update(reply=e.a.handle(e.a.uid, WORKER, msg)),
                                      name='broker-handler', daemon=True)
            worker.start()
            e.handler = worker
            worker.join(max(0.0, fields['deadline_monotonic'] - time.monotonic()) + 0.02)
            if worker.is_alive():
                raise socket.timeout('reply timed out at the deadline')
            return box['reply']
        raise AssertionError(mode)
    return request


def _run_core(e, mode, final='zero'):
    e.monkeypatch.setattr(pb, '_gpu_broker_request', _route(e, mode, final))
    try:
        result = pb.run_local_action(e.action, cas_root=e.cas.root, checkout_root=e.checkout,
                                     resource_demand=e.queue.demand_of(e.item))
    except pb.LocalActionError as error:
        assert 'zero distinct GPU UUIDs' in str(error)
        return {'outcome': 'zero', 'error': str(error), 'result': None}
    return {'outcome': result['receipt']['resource_telemetry']['gpu_use'], 'result': result, 'error': ''}


def _finish_and_read(e, core):
    key = e.action['action_key']
    claimed = e.queue.records.item_path(pool.record_store.CLAIMED, key)
    record = json.loads(claimed.read_text())
    record['resource_scope'] = {'nonce': e.nonce, 'scope_id': e.scope}
    e.queue.records.replace_record_at(claimed, record)

    def cleanup(*args, **kwargs):
        # As pool.py:14575-14578, from the real broker status reply.
        status = e.a.handle(e.a.uid, os.getpid(), {'op': 'status', 'action_key': key,
                                                   'nonce': e.nonce, 'token': e.token})
        telemetry = {}
        telemetry.update(status.get('resource_telemetry') or {})
        if status.get('gpu_record') is not None:
            telemetry['gpu_record'] = status['gpu_record']
        return {'complete': True, 'resource_scope': {'telemetry': telemetry}}

    e.monkeypatch.setattr(e.queue, 'cleanup_action_containers', cleanup)
    if core['outcome'] == 'zero':
        status, detail = 'failed', {'status': 'failed', 'returncode': 1, 'stdout': '', 'stderr': core['error']}
    else:
        status, detail = 'executed', {'status': 'executed', 'returncode': 0,
                                      'stdout': json.dumps(core['result']) + '\n', 'stderr': ''}
    path = e.queue.finish(key, status=status, detail=detail, claim_snapshot=record)
    terminal = json.loads(path.read_text())
    adopted = endings.resolve_attempt(e.queue.attempt_outcomes(terminal), invalid=pool.PoolContractError,
                                      validate_output_retry_stop=e.queue._validate_output_retry_stop)
    try:
        entry = next(x for x in os.scandir(path.parent) if x.path == str(path))
        row = census.ending_row(entry, e.tmp_path / 'queue').get('gpu_use')
    except Exception as exc:                                      # noqa: BLE001
        row = f'census error {type(exc).__name__}: {exc}'[:120]
    verified = action_result.read_verified_ending_result(e.queue, key, published_unix=record['published_unix'],
                                                         attempt=1)
    return {'dir': path.parent.name, 'pool': terminal['detail'].get('gpu_use'),
            'reason': terminal['detail'].get('gpu_use_reason'), 'adopted': adopted['detail'].get('gpu_use'),
            'census': row, 'summary_ok': action_client.outcome_summary(e.queue, path, terminal)['succeeded'],
            'report_rc': action_client.report_pool_outcome(e.queue, key, wait_s=0,
                                                           generation=record['published_unix']),
            'verified': verified['verified'], 'receipt': verified['receipt_sha256'] is not None,
            'pbwait_ok': pbwait._from_ending(e.queue, (path, terminal))['succeeded']}


def _agreement(core, readers, expected):
    zero = expected == 'zero'
    want = {'dir': 'failed' if zero else 'done', 'pool': expected, 'adopted': expected, 'census': expected,
            'summary_ok': not zero, 'report_rc': 1 if zero else 0, 'verified': True, 'receipt': not zero,
            'pbwait_ok': not zero}
    got = {k: readers[k] for k in want}
    return core['outcome'] == expected and got == want, f"core={core['outcome']} readers={readers}"


# -- Q1 / R2-B2 and r1-B3: histories on ONE shared frozen record -------------

HISTORIES = {
    'early_zero_then_final_failure': (['zero'], 'fail', [], 'unobserved'),
    'earlier_timeout_then_final_success': (['fail', 'zero'], 'zero', [], 'unobserved'),
    'final_only_use': (['zero'], 'used', [], 'used'),
    'earlier_use_then_final_failure': (['used'], 'fail', [], 'used'),
    'no_sample_during_lifetime': ([], 'zero', [], 'unobserved'),
    'all_success_zero': (['zero', 'zero'], 'zero', [], 'zero'),
    'zero_then_post_final_failed_sample': (['zero'], 'zero', ['fail'], 'zero'),
    'zero_then_post_final_lingering_use': (['zero'], 'zero', ['used'], 'zero'),
}


@pytest.mark.parametrize('name', sorted(HISTORIES))
def test_r2b2_histories_core_pool_and_readers_share_one_record(e2e, name):
    alive_steps, final, after, expected = HISTORIES[name]
    _lifetime(e2e, alive_steps)
    core = _run_core(e2e, 'deliver', final)
    for step in after:
        e2e.gpu.plan = lambda step=_step(e2e, step): step
        e2e.live.poll_once()
    ok, message = _agreement(core, _finish_and_read(e2e, core), expected)
    assert ok, f'{name}: {message}'


# -- Q4: lost replies, nothing committed, late commits, redelivery ----------

LOST = {
    'zero_committed_reply_lost': (['zero'], 'lose_reply', 'zero'),
    'used_committed_reply_lost': (['used'], 'lose_reply', 'used'),
    'nothing_committed': (['zero'], 'refused', 'unobserved'),
    'commit_attempted_after_deadline': (['zero'], 'late', 'unobserved'),
    'redelivery': (['zero'], 'redeliver', 'zero'),
}


@pytest.mark.parametrize('case', sorted(LOST))
def test_q4_lost_reply_matrix_agrees_everywhere(e2e, case):
    alive_steps, mode, expected = LOST[case]
    _lifetime(e2e, alive_steps)
    if mode == 'late':
        real = e2e.a.gpu_observer

        def late(scope, deadline):
            time.sleep(max(0.0, deadline - time.monotonic()) + 0.02)
            return real(scope, deadline)

        e2e.a.gpu_observer = late
        mode = 'deliver'
    if mode == 'redeliver':
        core = _run_core(e2e, 'deliver')
        first = e2e.a.records[e2e.scope]['gpu_record']
        again = e2e.a.handle(e2e.a.uid, WORKER, {'op': 'gpu_observation', 'action_key': e2e.action['action_key'],
                                                'nonce': e2e.nonce, 'deadline_monotonic': time.monotonic() + 5})
        assert again['gpu_record'] == first
    else:
        core = _run_core(e2e, mode)
    if case == 'commit_attempted_after_deadline':
        assert e2e.a.records[e2e.scope].get('gpu_record') is None
    ok, message = _agreement(core, _finish_and_read(e2e, core), expected)
    assert ok, f'{case}: {message}'


@pytest.mark.parametrize('check', ['readers_agree', 'broker_never_commits_after_deadline'])
def test_q4_commit_lands_after_core_read_at_deadline(e2e, check):
    """Broker checks the deadline, then os.replace lands after it (preemption).

    Core times out at the deadline and reads an absent mirror; the broker's
    commit then lands. The ruling: the broker refuses to commit after the
    deadline; Core decides only after it.
    """
    _lifetime(e2e, ['zero'])
    real_replace = os.replace
    deadline = {}

    def slow_replace(src, dst, *args, **kwargs):
        if str(dst).startswith('/proc/self/fd/') and str(dst).endswith(e2e.scope + '.json'):
            time.sleep(max(0.0, deadline['value'] - time.monotonic()) + 0.2)
        return real_replace(src, dst, *args, **kwargs)

    e2e.monkeypatch.setattr(os, 'replace', slow_replace)
    route = _route(e2e, 'timeout')

    def capture(action_, op, **fields):
        if op == 'gpu_observation':
            deadline['value'] = fields['deadline_monotonic']
        return route(action_, op, **fields)

    e2e.monkeypatch.setattr(pb, '_gpu_broker_request', capture)
    try:
        result = pb.run_local_action(e2e.action, cas_root=e2e.cas.root, checkout_root=e2e.checkout,
                                     resource_demand=e2e.queue.demand_of(e2e.item))
        core = {'outcome': result['receipt']['resource_telemetry']['gpu_use'], 'result': result, 'error': ''}
    except pb.LocalActionError as error:
        core = {'outcome': 'zero', 'error': str(error), 'result': None}
    e2e.handler.join(10)
    committed = e2e.a.records[e2e.scope].get('gpu_record')
    if check == 'broker_never_commits_after_deadline':
        assert committed is None, (
            f"broker committed {pb.gpu_execution_judgment(committed)['gpu_use']} after the deadline; "
            f"Core had already decided {core['outcome']} from an absent mirror")
    else:
        ok, message = _agreement(core, _finish_and_read(e2e, core), core['outcome'])
        assert ok and core['outcome'] == 'unobserved', message


# -- NEW: a lifetime sample in flight at the final observation ---------------

@pytest.mark.parametrize('inflight', ['attributes_use', 'fails'])
def test_r3_inflight_lifetime_sample_is_not_discarded_by_the_freeze(e2e, inflight):
    """A periodic census starts while argv is alive and finishes after the freeze.

    The final census no longer waits for the periodic Guard poll. The ruling:
    used = a successful sample attributes a UUID; zero requires EVERY
    attempted sample to succeed.
    """
    _lifetime(e2e, ['zero'])                       # a complete lifetime sample: live_sample
    entered, release = threading.Event(), threading.Event()
    periodic = 'periodic-guard-poll'

    def plan():
        if threading.current_thread().name == periodic:
            entered.set()
            assert release.wait(10)
            return 'fail' if inflight == 'fails' else {e2e.scope: 'GPU-inflight'}
        return {}

    e2e.alive['value'] = True
    e2e.gpu.plan = plan
    poller = threading.Thread(target=e2e.live.poll_once, name=periodic, daemon=True)
    poller.start()
    assert entered.wait(5)                         # census started during the payload's lifetime
    e2e.alive['value'] = False                     # argv exits; the worker observes
    core = _run_core(e2e, 'deliver')
    release.set()
    poller.join(10)
    frozen = pb.gpu_execution_judgment(e2e.a.records[e2e.scope]['gpu_record'])
    readers = _finish_and_read(e2e, core)
    expected = 'used' if inflight == 'attributes_use' else 'unobserved'
    assert (core['outcome'], frozen['gpu_use'], readers['pool'], readers['dir']) == (
        expected, expected, expected, 'done'), (
        f"lifetime census {'attributed GPU-inflight' if inflight == 'attributes_use' else 'failed'} but "
        f"retained after the freeze: Core={core['outcome']}, frozen={frozen['gpu_use']} "
        f"({frozen['gpu_use_reason']}), Pool filed {readers['dir']}/ gpu_use={readers['pool']}, "
        f"receipt={readers['receipt']}")


# -- Q5: false positives through the production attribution ------------------

def _fake_host(e, processes):
    proc = e.tmp_path / 'proc'
    (proc / 'pressure').mkdir(parents=True, exist_ok=True)
    (proc / 'meminfo').write_text('MemTotal: 67108864 kB\nMemAvailable: 33554432 kB\n')
    (proc / 'pressure' / 'memory').write_text('some avg10=0.00 avg60=0.00 avg300=0.00 total=0\n'
                                              'full avg10=0.00 avg60=0.00 avg300=0.00 total=0\n')
    for pid, cgroup in processes.items():
        d = proc / str(pid)
        d.mkdir(exist_ok=True)
        (d / 'stat').write_text(f'{pid} (python3) S ' + ' '.join(['1'] * 18) + ' 777 0 0\n')
        (d / 'cgroup').write_text('0::' + cgroup + '\n')
    return proc


@pytest.mark.parametrize('shape', ['container_nested_cgroup', 'grandchild_deep_leaf', 'gb10_na_bytes'])
def test_q5_real_attribution_files_used_not_zero(e2e, monkeypatch, shape):
    base = '/prismabuild.slice/' + e2e.scope
    cgroup = {'container_nested_cgroup': base + '/docker-' + 'c' * 64 + '.scope',
              'grandchild_deep_leaf': base + '/payload/torchrun/rank0',
              'gb10_na_bytes': base + '/payload'}[shape]
    proc = _fake_host(e2e, {5001: cgroup, 6001: '/system.slice/vllm.service'})
    rows = {'value': {}}
    monkeypatch.setattr(gm, '_gpu_processes', lambda timeout_s, devices=None, **kw: (
        rows['value'], None, True, gm.FOREIGN_SCOPE_COMPUTE_APPS))

    def collect(scopes, *, timeout_s, **kw):
        return gm.collect(scopes, proc_root=proc, cgroup_root=e2e.cgroot, timeout_s=timeout_s, **kw)

    e2e.gpu.collect = collect
    used = None if shape == 'gb10_na_bytes' else 512 * MIB
    e2e.alive['value'] = True
    rows['value'] = {(5001, 'GPU-gb10-0'): used, (6001, 'GPU-gb10-0'): None}
    e2e.live.poll_once()
    e2e.alive['value'] = False
    rows['value'] = {(6001, 'GPU-gb10-0'): None}   # the payload's processes have exited
    e2e.monkeypatch.setattr(pb, '_gpu_broker_request', lambda action_, op, **fields: (
        e2e.a.handle(e2e.a.uid, WORKER, {'op': op, 'action_key': action_['action_key'], 'nonce': e2e.nonce,
                                         **fields}) if op == 'gpu_observation'
        else (_ for _ in ()).throw(PermissionError('modelled'))))
    try:
        result = pb.run_local_action(e2e.action, cas_root=e2e.cas.root, checkout_root=e2e.checkout,
                                     resource_demand=e2e.queue.demand_of(e2e.item))
        core = {'outcome': result['receipt']['resource_telemetry']['gpu_use'], 'result': result, 'error': ''}
    except pb.LocalActionError as error:
        core = {'outcome': 'zero', 'error': str(error), 'result': None}
    ok, message = _agreement(core, _finish_and_read(e2e, core), 'used')
    assert ok, f'{shape}: {message}'


def test_q5_failed_lifetime_sample_is_never_zero(e2e):
    _lifetime(e2e, ['zero', 'fail', 'zero'])
    core = _run_core(e2e, 'deliver')
    ok, message = _agreement(core, _finish_and_read(e2e, core), 'unobserved')
    assert ok, message


# -- Q3: RPC authority through real SO_PEERCRED ------------------------------

WORKER_SCRIPT = r'''
import json, os, socket, subprocess, sys
name, payload = sys.argv[1], sys.argv[2]
def call(request):
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.connect("\0" + name)
    s.sendall((json.dumps(request) + "\n").encode())
    data = b""
    while not data.endswith(b"\n"):
        chunk = s.recv(65536)
        if not chunk:
            break
        data += chunk
    s.close()
    return json.loads(data)
for line in sys.stdin:
    command = json.loads(line)
    who, request = command["who"], command["req"]
    if who == "worker":
        out = call(request)
    elif who == "fork":
        r, w = os.pipe()
        child = os.fork()
        if child == 0:
            os.close(r)
            os.write(w, json.dumps(call(request)).encode())
            os._exit(0)
        os.close(w)
        data = b""
        while True:
            chunk = os.read(r, 65536)
            if not chunk:
                break
            data += chunk
        os.waitpid(child, 0)
        out = json.loads(data)
    else:
        done = subprocess.run([sys.executable, "-I", payload, name, json.dumps(request)],
                              capture_output=True, text=True, timeout=30)
        out = json.loads(done.stdout)
    print(json.dumps(out), flush=True)
'''

PAYLOAD_SCRIPT = r'''
import json, socket, sys
s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
s.connect("\0" + sys.argv[1])
s.sendall((sys.argv[2] + "\n").encode())
data = b""
while not data.endswith(b"\n"):
    chunk = s.recv(65536)
    if not chunk:
        break
    data += chunk
print(data.decode().strip())
'''


def test_q3_rpc_authority_with_real_peer_credentials(monitored, monkeypatch, tmp_path):
    a, backend, rows, _, monitor = monitored
    req, rec, path = rows[0]
    m = _broker(a)
    gpu = _Gpu([(r, p) for _, r, p in rows])
    m['ResourceMonitor'](a, gpu)
    original = Path.read_text
    # Strongest same-UID payload: every process claims membership of the scope.
    monkeypatch.setattr(Path, 'read_text', lambda f, *x, **k: (
        '0::/prismabuild.slice/' + rec['scope_id'] + '/payload\n')
        if re.fullmatch(r'/proc/\d+/cgroup', str(f)) else original(f, *x, **k))
    name = 'pbr3-' + secrets.token_hex(8)
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    listener.bind('\0' + name)
    listener.listen(64)
    server = SimpleNamespace(authority=a)

    def serve(conn):
        try:
            m['Handler'](conn, '', server)
        finally:
            conn.close()

    def accept():
        while True:
            try:
                conn, _ = listener.accept()
            except OSError:
                return
            threading.Thread(target=serve, args=(conn,), daemon=True).start()

    threading.Thread(target=accept, daemon=True).start()
    (tmp_path / 'w.py').write_text(WORKER_SCRIPT)
    (tmp_path / 'p.py').write_text(PAYLOAD_SCRIPT)
    worker = subprocess.Popen([sys.executable, '-I', str(tmp_path / 'w.py'), name, str(tmp_path / 'p.py')],
                              stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
    try:
        scope = rec['scope_id']
        a.records[scope]['worker_process'] = a._live_process(worker.pid)
        assert a.records[scope]['worker_process']['parent'] == os.getpid()

        def ask(who, offset=10.0):
            msg = {'op': 'gpu_observation', 'action_key': req['action_key'], 'nonce': req['nonce'],
                   'deadline_monotonic': time.monotonic() + offset}
            worker.stdin.write(json.dumps({'who': who, 'req': msg}) + '\n')
            worker.stdin.flush()
            return json.loads(worker.stdout.readline())

        got = {'payload_child': ask('payload'), 'forked_child': ask('fork')}
        assert 'gpu_observation_started' not in a.records[scope]
        got['expired_deadline'] = ask('worker', -1.0)
        assert 'gpu_observation_started' not in a.records[scope]
        got['worker'] = ask('worker')
        got['worker_redelivery'] = ask('worker', 4.0)
        got['payload_after_commit'] = ask('payload')
        got['fork_after_commit'] = ask('fork')
        a.records[scope]['worker_process'] = {**a._live_process(worker.pid), 'starttime': '1'}
        got['reused_pid_other_start'] = ask('worker')
    finally:
        worker.stdin.close()
        worker.wait(10)
        listener.close()
    refused = {k: v.get('error', '')[:80] for k, v in got.items() if not v.get('ok')}
    assert set(refused) == {'payload_child', 'forked_child', 'expired_deadline', 'payload_after_commit',
                            'fork_after_commit', 'reused_pid_other_start'}, got
    assert all('launched worker' in refused[k] for k in refused if k != 'expired_deadline'), refused
    assert 'deadline' in refused['expired_deadline'], refused
    assert got['worker']['gpu_record'] == got['worker_redelivery']['gpu_record']
    assert len([s for s in gpu.samples if s[0] != 'resource-monitor']) == 1


@pytest.mark.parametrize('mode', ['none', 'payload_observation', 'payload_registration',
                                  'worker_precommit_race', 'worker_redelivery'])
def test_q3_flood_cannot_blind_the_guard(monitored, monkeypatch, mode):
    """r2 R2-B1 probe, adapted: 32 callers loop while scope B is over budget."""
    a, backend, rows, _, monitor = monitored
    (req_a, rec_a, _), (req_b, rec_b, _) = rows
    gpu = _Gpu([(r, p) for _, r, p in rows])
    gpu.delay = 0.25
    gpu.plan = lambda: {rec_b['scope_id']: 'GPU-b'}
    live = _broker(a)['ResourceMonitor'](a, gpu)
    polls = []
    inner = live._poll_once
    live._poll_once = lambda: (polls.append(time.monotonic()), inner())[1]
    stopped = threading.Event()
    original_stop = backend.stop

    def stop(scope):
        original_stop(scope)
        if scope == rec_b['scope_id']:
            stopped.set()

    monkeypatch.setattr(backend, 'stop', stop)
    worker_peer(a, rec_a, monkeypatch, WORKER)
    child = {'pid': PAYLOAD, 'starttime': '18', 'parent': WORKER}
    live_process = a._live_process
    monkeypatch.setattr(a, '_live_process', lambda pid: child if pid == PAYLOAD else live_process(pid))
    source = Path.read_text
    monkeypatch.setattr(Path, 'read_text', lambda f, *x, **k: (
        '0::/prismabuild.slice/' + rec_a['scope_id'] + '/payload\n')
        if str(f) == f'/proc/{PAYLOAD}/cgroup' else source(f, *x, **k))
    if mode == 'worker_redelivery':
        a.handle(a.uid, WORKER, {'op': 'gpu_observation', 'action_key': req_a['action_key'],
                                 'nonce': req_a['nonce'], 'deadline_monotonic': time.monotonic() + 10})
    pid = PAYLOAD if mode.startswith('payload') else WORKER
    op = 'gpu_payload' if mode == 'payload_registration' else 'gpu_observation'
    done = threading.Event()
    outcomes = {}

    def flood():
        while not done.is_set():
            msg = {'op': op, 'action_key': req_a['action_key'], 'nonce': req_a['nonce'],
                   **({'payload_pid': PAYLOAD} if op == 'gpu_payload'
                      else {'deadline_monotonic': time.monotonic() + 10})}
            try:
                a.handle(a.uid, pid, msg)
                outcomes['ok'] = outcomes.get('ok', 0) + 1
            except Exception as exc:                             # noqa: BLE001
                outcomes[type(exc).__name__] = outcomes.get(type(exc).__name__, 0) + 1

    threads = [threading.Thread(target=flood, daemon=True) for _ in range(0 if mode == 'none' else 32)]
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
    gaps = [round(y - x, 2) for x, y in zip(polls, polls[1:])]
    finals = len([s for s in gpu.samples if s[0] != 'resource-monitor'])
    assert was_stopped and finals <= 1, (
        f"{mode}: B stopped={was_stopped} after {time.monotonic() - started:.1f}s; "
        f"{finals} final censuses; poll gaps {gaps}; caller outcomes {outcomes}")


def test_r1b2_final_observation_does_not_shorten_guard_debounce(monitored, monkeypatch):
    a, backend, rows, _, monitor = monitored
    (req_a, rec_a, _), (req_b, rec_b, _) = rows
    gpu = _Gpu([(r, p) for _, r, p in rows])
    gpu.plan = lambda: {rec_b['scope_id']: 'GPU-b'}
    live = _broker(a)['ResourceMonitor'](a, gpu)
    live.poll_once()
    worker_peer(a, rec_a, monkeypatch, WORKER)
    a.handle(a.uid, WORKER, {'op': 'gpu_observation', 'action_key': req_a['action_key'],
                             'nonce': req_a['nonce'], 'deadline_monotonic': time.monotonic() + 10})
    assert backend.stopped == []


# -- N7 re-run: the late finisher keeps the zero verdict ---------------------

@pytest.mark.parametrize('scoped', ['gpu'], indirect=True)
def test_n7_late_finisher_keeps_the_zero_verdict(scoped, monkeypatch):
    queue, first, calls = scoped
    key = first['action_key']
    queue._start_resource_scope(first)
    queue.records.item_path(pool.record_store.CLAIMED, key).unlink()
    queue.records.lease_path(key).unlink()
    queue.ledger().release(key)
    queue.publish(action_key=key, cas_root=first['cas_root'], checkout_root=first['checkout_root'],
                  worker_script=first['worker_script'], resources=first['resources'], max_attempts=2,
                  retry_safe=True)
    newer = queue.claim(owner='newer-attempt', has_gpu=True, capacity=first['resources'])
    assert newer is not None
    queue._start_resource_scope(newer)
    old = first['resource_scope']['nonce']
    request = resource_scope.ResourceScope._request
    zero = {'gpu_uuids': [], 'gpu_evidence': {'sample_failed': False, 'live_sample': True,
                                              'final_succeeded': True}}

    def status_with_zero(scope, op, **extra):
        reply = request(scope, op, **extra)
        if op == 'status' and scope.nonce == old:
            reply = {**reply, 'gpu_record': frozen_gpu(zero, action_key=key, nonce=old)}
        return reply

    monkeypatch.setattr(resource_scope.ResourceScope, '_request', status_with_zero)
    result = queue.finish(key, status='failed', detail={'status': 'failed', 'returncode': 1, 'stdout': '',
                          'stderr': 'GPU action used zero distinct GPU UUIDs'}, claim_snapshot=first)
    archived = json.loads(result.read_text())
    assert (archived['detail'].get('gpu_use'), archived.get('disposition')) == ('zero', 'failed'), archived.get(
        'disposition')


# -- Q2: mutation test of the "no other route" guard -------------------------

GUARD_FILE = REPO / 'tests' / 'test_gpu_frozen_record_1744.py'
GUARD = 'test_live_telemetry_is_not_a_judgment_route'
LIFECYCLE_ANCHOR = '    state, reason = observation["gpu_use"], observation["gpu_use_reason"]\n'
ENDINGS_ANCHOR = '        gpu_judgment = pb.gpu_execution_judgment(observation)\n'
POOL_ANCHOR = ('            if status.get("gpu_record") is not None:\n'
               '                telemetry["gpu_record"] = status["gpu_record"]\n')
RETAIN_ANCHOR = ("                if (current is None or current.get('released_unix') "
                 "or current.get('gpu_record') is not None\n")
DEMAND_ANCHOR = '        return pb.normalized_resource_demand(raw)\n'
CENSUS_ANCHOR = '        "gpu_use": (pb.gpu_execution_judgment(detail["resource_telemetry"])["gpu_use"]\n'
ZERO_FROM_LIVE = ('        observation = {**observation, "gpu_use": "zero", '
                  '"gpu_use_reason": "successful_payload_sample", "gpu_devices_used": 0}\n')
MUTANTS = {
    # name: [(file, anchor, replacement)]
    'live_key_local_variable': [('src/prismabuild/lifecycle.py', LIFECYCLE_ANCHOR,
        '    if observation["gpu_use"] == "unobserved" and telemetry.get("gpu_devices_used") == 0:\n'
        + ZERO_FROM_LIVE + LIFECYCLE_ANCHOR)],
    'live_key_getattr': [('src/prismabuild/lifecycle.py', LIFECYCLE_ANCHOR,
        '    if observation["gpu_use"] == "unobserved" and getattr(telemetry, "get")("gpu_devices_used") == 0:\n'
        + ZERO_FROM_LIVE + LIFECYCLE_ANCHOR)],
    'computed_banned_keys': [('src/prismabuild/lifecycle.py', LIFECYCLE_ANCHOR,
        '    _live = telemetry.get("gpu_" + "evidence") or {}\n'
        '    if (observation["gpu_use"] == "unobserved" and not telemetry.get("gpu_" + "uuids")\n'
        '            and _live.get("live_" + "sample") and not _live.get("sample_" + "failed")):\n'
        + ZERO_FROM_LIVE + LIFECYCLE_ANCHOR)],
    'second_judgment_function': [('src/prismabuild/endings.py', ENDINGS_ANCHOR,
        '        gpu_judgment = _second_route(observation)\n'),
        ('src/prismabuild/endings.py', None,
         '\n\ndef _second_route(observation):\n'
         '    count = observation.get("gpu_devices_used")\n'
         '    return {**observation, "gpu_use": "used" if count else "zero",\n'
         '            "gpu_use_reason": "successful_payload_sample", "gpu_devices_used": count or 0}\n')],
    'unscanned_pool_local_receiver': [('src/prismabuild/pool.py', POOL_ANCHOR, POOL_ANCHOR +
        '            elif status.get("resource_telemetry"):\n'
        '                live = status["resource_telemetry"]\n'
        '                telemetry["gpu_record"] = {"action_key": scope.action_key, "nonce": scope.nonce,\n'
        '                    "scope_id": scope.scope_id, "deadline_monotonic": 0.0, "resource_telemetry": {\n'
        '                        "gpu_uuids": live["gpu_uuids"],\n'
        '                        "gpu_evidence": {**live["gpu_evidence"], "final_succeeded": True}}}\n')],
    'unscanned_pool_self_receiver': [('src/prismabuild/pool.py', DEMAND_ANCHOR, DEMAND_ANCHOR +
        '\n    def _gpu_live_judgment(self, status):\n'
        '        live = status.get("resource_telemetry") or {}\n'
        '        evidence = live.get("gpu_evidence") or {}\n'
        '        return "zero" if not live.get("gpu_uuids") and evidence.get("final_succeeded") else "unobserved"\n')],
    'census_unbanned_key': [('src/prismabuild/census.py', CENSUS_ANCHOR,
        '        "gpu_use": ("used" if detail["resource_telemetry"].get("gpu_devices_used") else "zero")\n'
        '                   if detail.get("resource_telemetry") else (pb.gpu_execution_judgment(detail["resource_telemetry"])["gpu_use"]\n')],
    'retain_check_satisfied_by_comment': [('tools/fleet/resource_broker.py', RETAIN_ANCHOR,
        "                if (current is None or current.get('released_unix')  # gpu_record: see freeze\n")],
}
POSITIVE_CONTROL = [('src/prismabuild/lifecycle.py', LIFECYCLE_ANCHOR,
                     '    _unused = telemetry.get("gpu_uuids")\n' + LIFECYCLE_ANCHOR)]


def _scratch(tmp_path, edits):
    root = tmp_path / 'scratch'
    for rel in ['src/prismabuild/lifecycle.py', 'src/prismabuild/endings.py', 'src/prismabuild/census.py',
                'src/prismabuild/action_result.py', 'src/prismabuild/pool.py', 'tools/fleet/resource_broker.py']:
        target = root / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text((REPO / rel).read_text())
    (root / 'tests').mkdir(exist_ok=True)
    for rel, anchor, replacement in edits:
        target = root / rel
        text = target.read_text()
        if anchor is None:
            text += replacement
        else:
            assert text.count(anchor) == 1, f'stale mutant anchor in {rel}'
            text = text.replace(anchor, replacement)
        compile(text, rel, 'exec')
        target.write_text(text)
    return root


def _run_guard(root):
    source = GUARD_FILE.read_text()
    node = next(n for n in ast.parse(source).body if isinstance(n, ast.FunctionDef) and n.name == GUARD)
    ns = {'ast': ast, 'Path': Path, 'pytest': pytest, 'pb': pb,
          '__file__': str(root / 'tests' / GUARD_FILE.name)}
    exec(compile(ast.get_source_segment(source, node), str(GUARD_FILE), 'exec'), ns)
    try:
        ns[GUARD]()
    except AssertionError:
        return 'guard_failed'
    return 'guard_passed'


def test_q2_guard_harness_controls(tmp_path):
    assert _run_guard(_scratch(tmp_path / 'clean', [])) == 'guard_passed'
    assert _run_guard(_scratch(tmp_path / 'positive', POSITIVE_CONTROL)) == 'guard_failed'


@pytest.mark.parametrize('mutant', sorted(MUTANTS))
def test_q2_no_other_route_guard_catches_second_judgment_mutant(tmp_path, mutant):
    assert _run_guard(_scratch(tmp_path, MUTANTS[mutant])) == 'guard_failed', (
        f'{GUARD} passes with mutant {mutant}: a second GPU judgment route goes undetected')


def _mutated_function(root, rel, name, module, extra=()):
    source = (root / rel).read_text()
    tree = ast.parse(source)
    glb = dict(module.__dict__)
    for fn in [name, *extra]:
        node = next(n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == fn)
        exec(compile(textwrap.dedent(ast.get_source_segment(source, node)), rel, 'exec'), glb)
    return glb[name]


@pytest.mark.parametrize('mutant', ['live_key_local_variable', 'live_key_getattr', 'computed_banned_keys'])
def test_q2_lifecycle_mutants_are_live_second_routes(e2e, tmp_path, mutant):
    """Behaviour proof: each mutant flips an unobserved success into terminal zero."""
    root = _scratch(tmp_path, MUTANTS[mutant])
    e2e.monkeypatch.setattr(lifecycle, '_gpu_finish_verdict',
                            _mutated_function(root, 'src/prismabuild/lifecycle.py', '_gpu_finish_verdict', lifecycle))
    _lifetime(e2e, ['zero'])
    core = _run_core(e2e, 'refused')
    assert core['outcome'] == 'unobserved'
    readers = _finish_and_read(e2e, core)
    assert (readers['dir'], readers['pool']) == ('failed', 'zero'), readers


def test_q2_endings_mutant_is_a_live_second_route(e2e, tmp_path):
    root = _scratch(tmp_path, MUTANTS['second_judgment_function'])
    e2e.monkeypatch.setattr(endings, 'resolve_attempt', _mutated_function(
        root, 'src/prismabuild/endings.py', 'resolve_attempt', endings, extra=['_second_route']))
    _lifetime(e2e, ['zero'])
    core = _run_core(e2e, 'refused')
    readers = _finish_and_read(e2e, core)
    # Baseline (no mutant) is unobserved everywhere: test_q4 nothing_committed.
    assert readers['adopted'] == 'zero' and readers['dir'] == 'done', readers


def test_q2_pool_mutant_is_a_live_second_route(tmp_path):
    root = _scratch(tmp_path, MUTANTS['unscanned_pool_local_receiver'])
    key, nonce = 'a' * 64, 'b' * 32
    scope_id = 'prismabuild-job' + pb.raw_sha256((key + nonce).encode())[:32] + '.slice'
    live = {'gpu_devices_used': 0, 'gpu_uuids': [],
            'gpu_evidence': {'sample_failed': False, 'live_sample': True, 'final_succeeded': None}}
    fake = SimpleNamespace(sample=lambda: {}, _request=lambda op: {'resource_telemetry': live},
                           write_telemetry=lambda telemetry: None, action_key=key, nonce=nonce,
                           scope_id=scope_id, _pool_accounting_valid=True)
    judge = lambda t: pb.gpu_execution_judgment(t.get('gpu_record'), action_key=key, nonce=nonce,  # noqa: E731
                                                scope_id=scope_id)['gpu_use']
    assert judge(pool.PoolQueue._sample_resource_scope(fake)) == 'unobserved'
    mutated = _mutated_function(root, 'src/prismabuild/pool.py', '_sample_resource_scope', pool)
    assert judge(mutated(fake)) == 'zero'


# -- NEW: a final census that never sampled the scope reports success --------

def test_r3_final_census_that_excluded_the_scope_is_not_a_success(e2e):
    """A stop lands as the worker's final RPC is processed (timeout, disconnect).

    _records() drops stopped scopes, so the final census samples nothing, yet
    observe_gpu freezes final_succeeded=True. The killed attempt is then filed
    as terminal zero instead of its retryable stop.
    """
    _lifetime(e2e, ['zero'])
    real = e2e.a.gpu_observer
    stop = {'op': 'stop', 'action_key': e2e.action['action_key'], 'nonce': e2e.nonce, 'token': e2e.token,
            'reason': 'action execution timeout'}
    when = 'stop_after_admission_before_census'

    def observer(scope, deadline):
        e2e.a.handle(e2e.a.uid, os.getpid(), stop)
        return real(scope, deadline)
    e2e.a.gpu_observer = observer
    e2e.gpu.plan = lambda: {}
    e2e.a.handle(e2e.a.uid, WORKER, {'op': 'gpu_observation', 'action_key': e2e.action['action_key'],
                                     'nonce': e2e.nonce, 'deadline_monotonic': time.monotonic() + 5})
    frozen = pb.gpu_execution_judgment(e2e.a.records[e2e.scope]['gpu_record'])
    key = e2e.action['action_key']
    claimed = e2e.queue.records.item_path(pool.record_store.CLAIMED, key)
    record = json.loads(claimed.read_text())
    record['resource_scope'] = {'nonce': e2e.nonce, 'scope_id': e2e.scope}
    e2e.queue.records.replace_record_at(claimed, record)

    def cleanup(*args, **kwargs):
        status = e2e.a.handle(e2e.a.uid, os.getpid(), {'op': 'status', 'action_key': key,
                                                       'nonce': e2e.nonce, 'token': e2e.token})
        telemetry = dict(status.get('resource_telemetry') or {})
        for field, name in (('gpu_record', 'gpu_record'), ('stop_reason', 'termination_reason'),
                            ('termination_evidence', 'termination_evidence')):
            if status.get(field):
                telemetry[name] = status[field]
        return {'complete': True, 'resource_scope': {'telemetry': telemetry}}

    e2e.monkeypatch.setattr(e2e.queue, 'cleanup_action_containers', cleanup)
    path = e2e.queue.finish(key, status='failed', detail={'status': 'failed', 'returncode': 137, 'stdout': '',
                            'stderr': 'killed by scope stop'}, claim_snapshot=record)
    terminal = json.loads(path.read_text())
    assert frozen['gpu_use'] != 'zero' and terminal['detail'].get('termination_reason') != 'zero_gpu_devices_used', (
        f"{when}: final census sampled no scope, frozen={frozen['gpu_use']} ({frozen['gpu_use_reason']}); "
        f"killed attempt filed {path.parent.name}/ termination_reason={terminal['detail'].get('termination_reason')}")
