"""A pool attempt keeps its tokens until its exact aggregate scope is empty."""
import json
import hashlib
import socket
import subprocess
import sys
from pathlib import Path

import pytest
from prismabuild import core as pb, pool, resource_scope


@pytest.fixture
def scoped(tmp_path, monkeypatch, request):
    checkout = tmp_path / 'checkout'
    checkout.mkdir()
    (checkout / 'task.py').write_text('print("ok")\n')
    action = pb.seal_action({
        'schema': pb.ACTION_SCHEMA_V2,
        'task': {'definition_id': 'tests/scope', 'definition_version': 'v1',
                 'task_class': 'generation', 'determinism': 'deterministic',
                 'artifact_family': 'generic', 'artifact_kind': 'generic',
                 'argv': [sys.executable, 'task.py'], 'working_directory': '.',
                 'result_path': 'result'},
        'inputs': [], 'code_closure': pb.build_code_closure(checkout, ['task.py']),
        'params': {'demand': {'mem_gb': 2, 'cpu': 1}} if getattr(request, 'param', True) else {},
        'environment': {'variables': {}, 'toolchain': {}},
        'execution_scope': {'portability': 'portable', 'platform_key': None, 'host_class': None},
    })
    cas = pb.PrismaBuildCAS(tmp_path / 'cas')
    cas.publish_action_request(action)
    queue = pool.PoolQueue(tmp_path / 'queue')
    queue.publish(action_key=action['action_key'], cas_root=cas.root,
                  checkout_root=checkout, worker_script='/worker.py',
                  resources={'mem_gb': 2, 'cpu': 1}, max_attempts=2, retry_safe=True)
    item = queue.claim(capacity={'mem_gb': 2, 'cpu': 1})
    calls = []

    def request(scope, op, **extra):
        calls.append(op)
        if op == 'create':
            unit = 'prismabuild-job' + hashlib.sha256((scope.action_key + scope.nonce).encode()).hexdigest()[:32] + '.slice'
            return {'ok': True, 'scope_id': unit, 'token': 'b'*64,
                    'cgroup_path': '/sys/fs/cgroup/prismabuild.slice/' + unit}
        return {'ok': True}

    def sample(scope):
        calls.append('sample')
        value = {'action_key': scope.action_key, 'nonce': scope.nonce,
                 'sampled_unix': pool._now(), 'wall_seconds': 1.,
                 'cpu_seconds': .25, 'memory_peak_bytes': 1024,
                 'memory_current_bytes': 0, 'oom_kill': 0, 'complete': True}
        resource_scope._atomic_json(scope.telemetry_path, value)
        return value

    monkeypatch.setattr(resource_scope.ResourceScope, '_request', request)
    monkeypatch.setattr(resource_scope.ResourceScope, 'sample', sample)
    monkeypatch.setattr(pool.cpu_admission, 'record_completion',
                        lambda *args: calls.append('learn'))
    return queue, item, calls


def _process(monkeypatch, queue, item, calls, *, ticks=0):
    class Process:
        pid = 999999999
        returncode = 0
        def __init__(self, argv, **kw):
            persisted = json.loads(queue.item_path(pool.CLAIMED, item['action_key']).read_text())
            assert persisted['resource_scope']['token'] == 'b'*64
            assert persisted['resource_scope']['memory_max_bytes'] == 2 * 1024**3
            assert '--token' in argv
            calls.append('launch')
            self.remaining = ticks
        def communicate(self, *, timeout):
            if self.remaining:
                assert timeout <= 2.0, 'scope telemetry must not wait for a 30s heartbeat'
                self.remaining -= 1
                raise subprocess.TimeoutExpired('worker', timeout)
            return 'ok', ''
    monkeypatch.setattr(pool.subprocess, 'Popen', Process)


def test_scope_is_durable_before_launch_and_completion_precedes_release(scoped, monkeypatch):
    queue, item, calls = scoped
    _process(monkeypatch, queue, item, calls, ticks=2)
    outcome = queue.execute(item, containment=True, heartbeat_s=30)
    assert outcome['status'] == 'executed'
    queue.finish(item['action_key'], status='executed', detail=outcome, claim_snapshot=item)
    assert calls.index('create') < calls.index('launch')
    assert calls.count('sample') >= 3
    assert calls.index('stop') < calls.index('release') < calls.index('learn')
    assert queue.ledger().held() == {}


def test_mutable_queue_cannot_raise_sealed_memory_limit(scoped, monkeypatch):
    queue, item, calls = scoped
    item['resources']['mem_gb'] = 200
    with pytest.raises(pool.PoolContractError, match='sealed.*demand|demand.*sealed'):
        queue.execute(item, containment=True)
    assert 'create' not in calls


def test_broker_cleanup_failure_retains_claim_and_reservation(scoped, monkeypatch):
    queue, item, calls = scoped
    _process(monkeypatch, queue, item, calls)
    outcome = queue.execute(item, containment=True)
    monkeypatch.setattr(resource_scope.ResourceScope, 'release',
                        lambda scope: (_ for _ in ()).throw(OSError('broker down')))
    path = queue.finish(item['action_key'], status='executed', detail=outcome, claim_snapshot=item)
    assert path == queue.item_path(pool.CLAIMED, item['action_key'])
    assert queue.ledger().held() == {'cpu': 1, 'mem_gb': 2}
    assert queue.lease_path(item['action_key']).exists()
    assert 'learn' not in calls


def test_reaper_stops_scope_before_requeue_and_strips_ownership(scoped, monkeypatch):
    queue, item, calls = scoped
    _process(monkeypatch, queue, item, calls)
    queue.execute(item, containment=True)
    assert queue.reap_stale(timeout_s=-1) == [item['action_key']]
    assert 'stop' in calls and 'release' in calls
    ready = json.loads(queue.item_path(pool.READY, item['action_key']).read_text())
    assert 'resource_scope' not in ready
    assert 'resource_scope_cleanup' not in ready
    assert queue.ledger().held() == {}


def test_foreign_reaper_never_controls_local_scope(scoped, monkeypatch):
    queue, item, calls = scoped
    _process(monkeypatch, queue, item, calls)
    queue.execute(item, containment=True)
    calls.clear()
    monkeypatch.setattr(pool.socket, 'gethostname', lambda: 'other-host')
    assert queue.reap_stale(timeout_s=-1) == []
    assert 'stop' not in calls and 'release' not in calls
    assert queue.item_path(pool.CLAIMED, item['action_key']).exists()


def test_kernel_oom_is_failure_even_if_launcher_reports_zero(scoped, monkeypatch):
    queue, item, calls = scoped
    _process(monkeypatch, queue, item, calls)
    original = resource_scope.ResourceScope.sample
    monkeypatch.setattr(resource_scope.ResourceScope, 'sample',
                        lambda scope: {**original(scope), 'oom_kill': 1, 'oom_local': 1})
    outcome = queue.execute(item, containment=True)
    assert outcome['status'] == 'failed'
    assert outcome['returncode'] != 0
    assert outcome['termination_reason'] == 'memory_limit_oom'


def test_contained_child_oom_does_not_fail_a_successful_parent(scoped, monkeypatch):
    queue, item, calls = scoped
    _process(monkeypatch, queue, item, calls)
    original = resource_scope.ResourceScope.sample
    monkeypatch.setattr(resource_scope.ResourceScope, 'sample',
                        lambda scope: {**original(scope), 'oom_kill': 1, 'oom_local': 0})
    outcome = queue.execute(item, containment=True)
    assert outcome['status'] == 'executed'
    assert outcome['returncode'] == 0


def test_withdraw_stops_scope_and_releases_only_after_broker_proof(scoped, monkeypatch):
    queue, item, calls = scoped
    _process(monkeypatch, queue, item, calls)
    queue.execute(item, containment=True)
    result = queue.withdraw(item['action_key'])
    assert result['released'] == 3
    assert 'stop' in calls and 'release' in calls
    assert not queue.item_path(pool.CLAIMED, item['action_key']).exists()


@pytest.mark.parametrize('scoped', [False], indirect=True)
def test_generic_producer_keeps_existing_queue_resource_contract(scoped, monkeypatch):
    queue, item, calls = scoped
    _process(monkeypatch, queue, item, calls)
    assert queue.execute(item, containment=True)['status'] == 'executed'
    assert 'create' in calls


def test_timeout_stops_exact_scope_before_proxy_signal(scoped, monkeypatch):
    queue, item, calls = scoped
    _process(monkeypatch, queue, item, calls, ticks=1)
    monkeypatch.setattr(pool.pb, '_terminate_process_group',
                        lambda *args, **kwargs: calls.append('proxy-signal'))
    outcome = queue.execute(item, containment=True, timeout_s=1e-9)
    assert outcome['status'] == 'timeout'
    assert calls.index('stop') < calls.index('proxy-signal')
    queue.finish(item['action_key'], status='timeout', detail=outcome, claim_snapshot=item)
    assert 'release' in calls
    assert queue.ledger().held() == {}


def test_orphan_lease_retains_scope_authority_for_cleanup(scoped, monkeypatch):
    queue, item, calls = scoped
    _process(monkeypatch, queue, item, calls)
    queue.execute(item, containment=True)
    key = item['action_key']
    lease = json.loads(queue.lease_path(key).read_text())
    assert lease['resource_scope']['nonce'] == item['resource_scope']['nonce']
    queue.item_path(pool.CLAIMED, key).unlink()
    assert queue.sweep_widowed_leases(timeout_s=-1) == [key]
    assert 'stop' in calls and 'release' in calls
    assert queue.ledger().held() == {}


def test_completed_scope_cleanup_is_idempotent_and_never_releases_new_nonce(scoped, monkeypatch):
    queue, item, calls = scoped
    _process(monkeypatch, queue, item, calls)
    queue.execute(item, containment=True)
    assert queue.cleanup_action_containers(item)['complete']
    calls.clear()
    assert queue.cleanup_action_containers(item)['complete']
    assert calls == []
    item['resource_scope']['nonce'] = 'e' * 32
    assert not queue.cleanup_action_containers(item)['complete']
    assert calls == []


def test_live_oom_stops_remaining_payload_before_waiting_for_exit(scoped, monkeypatch):
    queue, item, calls = scoped
    _process(monkeypatch, queue, item, calls, ticks=1)
    original = resource_scope.ResourceScope.sample
    monkeypatch.setattr(resource_scope.ResourceScope, 'sample',
                        lambda scope: {**original(scope), 'oom_kill': 1, 'oom_local': 1})
    monkeypatch.setattr(pool.pb, '_terminate_process_group',
                        lambda *args, **kwargs: calls.append('proxy-signal'))
    outcome = queue.execute(item, containment=True, heartbeat_s=30)
    assert outcome['termination_reason'] == 'memory_limit_oom'
    assert calls.index('stop') < calls.index('proxy-signal')
    assert outcome['returncode'] == 137


def test_broker_wrapper_encloses_taskset_affinity(scoped, monkeypatch):
    import os
    queue, item, calls = scoped
    cpu = min(os.sched_getaffinity(0))
    tiers = {'preferred': [cpu], 'fallback': []}
    pool._write_json_atomic(queue.ledger().base / 'cpu-map.json', tiers)
    item['cpu_allocation'] = queue.ledger().cpu_allocation(item['action_key'], tiers)
    seen = []
    original = resource_scope.ResourceScope.wrap_argv
    def wrap(scope, argv):
        seen.append(argv)
        return original(scope, argv)
    monkeypatch.setattr(resource_scope.ResourceScope, 'wrap_argv', wrap)
    _process(monkeypatch, queue, item, calls)
    queue.execute(item, containment=True)
    assert seen[0][:3] == ['/usr/bin/taskset', '--cpu-list', str(cpu)]


def test_reboot_invalidates_cpu_learning_without_blocking_exact_cleanup(scoped, monkeypatch):
    queue, item, calls = scoped
    _process(monkeypatch, queue, item, calls)
    queue.execute(item, containment=True)
    item['resource_scope']['started_monotonic'] = 10**20
    item['resource_scope']['boot_id'] = 'previous-boot'
    result = queue.cleanup_action_containers(item)
    assert result['complete']
    assert result['resource_scope']['telemetry']['complete'] is False
    assert 'stop' in calls and 'release' in calls


def test_broker_guard_first_cause_reaches_terminal_outcome(scoped, monkeypatch):
    queue, item, calls = scoped
    _process(monkeypatch, queue, item, calls)
    request = resource_scope.ResourceScope._request
    evidence = {'source': 'exact-scope-gpu-accounting', 'gpu_bytes': 3 * 1024**3}
    def status(scope, op, **extra):
        answer = request(scope, op, **extra)
        if op == 'status':
            answer.update(stop_reason='gpu_budget_exceeded', termination_evidence=evidence)
        return answer
    monkeypatch.setattr(resource_scope.ResourceScope, '_request', status)
    outcome = queue.execute(item, containment=True)
    assert outcome['status'] == 'failed'
    assert outcome['termination_reason'] == 'gpu_budget_exceeded'
    assert outcome['termination_evidence'] == evidence
    destination = queue.finish(item['action_key'], status='failed', detail=outcome, claim_snapshot=item)
    assert json.loads(destination.read_text())['detail']['termination_evidence'] == evidence


def test_reaper_preserves_original_resource_failure_in_attempt_history(scoped, monkeypatch):
    queue, item, calls = scoped
    _process(monkeypatch, queue, item, calls)
    queue.execute(item, containment=True)
    request = resource_scope.ResourceScope._request
    def status(scope, op, **extra):
        answer = request(scope, op, **extra)
        if op == 'status':
            answer.update(stop_reason='gpu_budget_exceeded',
                          termination_evidence={'source': 'broker-resource-monitor'})
        return answer
    monkeypatch.setattr(resource_scope.ResourceScope, '_request', status)
    assert queue.reap_stale(timeout_s=-1) == [item['action_key']]
    ready = json.loads(queue.item_path(pool.READY, item['action_key']).read_text())
    assert ready['detail']['termination_reason'] == 'gpu_budget_exceeded'
    assert ready['detail']['termination_evidence'] == {'source': 'broker-resource-monitor'}


@pytest.mark.parametrize('budget', [1e300, 1e-12, 2**33])
def test_invalid_sealed_gpu_budget_is_rejected_before_scope_creation(tmp_path, monkeypatch, budget):
    from test_core import _body
    (tmp_path / 'task_code.py').write_text('print(1)\n')
    body = _body(tmp_path)
    demand = {'gpu': 1, 'mem_gb': 4, 'cpu': 1}
    body['params'] = {'demand': demand, 'gpu_memory_gb': budget}
    action = pb.seal_action(body)
    cas = pb.PrismaBuildCAS(tmp_path / 'cas')
    cas.publish_action_request(action)
    def forbidden(*args, **kwargs):
        pytest.fail('invalid GPU budget reached the resource broker')
    monkeypatch.setattr(resource_scope.ResourceScope, '_request', forbidden)
    queue = pool.PoolQueue(tmp_path / 'queue')
    with pytest.raises(pool.PoolContractError, match='gpu_memory_gb'):
        queue._start_resource_scope({'action_key': action['action_key'],
                                    'cas_root': str(cas.root), 'resources': demand})


def _raising_hook(exc):
    def hook(*args):
        raise exc
    return hook


def test_a_raising_learning_hook_does_not_escape_the_token_return_gate(scoped, monkeypatch):
    """The gate must answer, not raise, when only the learning hook failed.

    ``record_completion`` runs after ``scope.release()``, so by then the
    payload has stopped and the tokens are back.  It reaches the admission
    lock, and two of that subsystem's honest refusals are bare
    ``RuntimeError`` -- neither of which was in the ``except`` tuple that used
    to guard this call (#286).
    """

    queue, item, calls = scoped
    _process(monkeypatch, queue, item, calls)
    outcome = queue.execute(item, containment=True)
    monkeypatch.setattr(pool.cpu_admission, 'record_completion',
                        _raising_hook(RuntimeError('unsafe PrismaBuild admission lock file')))
    record = json.loads(queue.item_path(pool.CLAIMED, item['action_key']).read_text())

    cleanup = queue.cleanup_action_containers(record)

    assert cleanup['complete'] is True
    assert cleanup['resource_scope']['complete'] is True
    # Recorded rather than swallowed: a box whose admission is unusable should
    # be readable from the claim it could not learn from.
    assert 'RuntimeError' in cleanup['resource_scope']['learning_error']
    assert 'unsafe PrismaBuild admission lock file' in cleanup['resource_scope']['learning_error']


def test_a_raising_learning_hook_still_completes_the_claim(scoped, monkeypatch):
    """The observed failure: the action ran, and then lost its lease anyway."""

    queue, item, calls = scoped
    _process(monkeypatch, queue, item, calls)
    outcome = queue.execute(item, containment=True)
    monkeypatch.setattr(pool.cpu_admission, 'record_completion',
                        _raising_hook(RuntimeError('unsafe PrismaBuild admission lock directory')))

    path = queue.finish(item['action_key'], status='executed', detail=outcome,
                        claim_snapshot=item)

    # Not still claimed, tokens back, lease gone: the three things the escape
    # cost.  Under the old tuple this call raised and none of them happened.
    assert path != queue.item_path(pool.CLAIMED, item['action_key'])
    assert queue.ledger().held() == {}
    assert not queue.lease_path(item['action_key']).exists()


def test_an_unexpected_failure_in_the_hook_is_recorded_the_same_way(scoped, monkeypatch):
    """Not a RuntimeError special case -- the guard is the call's contract.

    Enumerating what a whole subsystem can raise is what failed here, so a
    kind nobody predicted must also be recorded rather than cost the action.
    """

    queue, item, calls = scoped
    _process(monkeypatch, queue, item, calls)
    queue.execute(item, containment=True)
    monkeypatch.setattr(pool.cpu_admission, 'record_completion',
                        _raising_hook(ZeroDivisionError('division by zero')))
    record = json.loads(queue.item_path(pool.CLAIMED, item['action_key']).read_text())

    cleanup = queue.cleanup_action_containers(record)

    assert cleanup['complete'] is True
    assert cleanup['resource_scope']['learning_error'] == 'ZeroDivisionError: division by zero'
