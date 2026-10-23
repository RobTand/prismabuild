"""Final observation authority, deadline recovery and the one GPU judgment route."""
import ast
import json
import os
from pathlib import Path
import time

import pytest

from prismabuild import action_client, action_result, core as pb, pool, endings
from gpu_evidence_fixtures import frozen_gpu, worker_peer
from test_gpu_review_r2 import generic_gpu, _RealGuardGpu, _job, MIB  # noqa: F401
from test_resource_broker import authority, monitored  # noqa: F401


def _query(request, deadline=None):
    return {'op': 'gpu_observation', 'action_key': request['action_key'], 'nonce': request['nonce'],
            'deadline_monotonic': time.monotonic() + 10 if deadline is None else deadline}


@pytest.mark.parametrize('state', ['zero', 'used', 'absent'])
def test_lost_reply_uses_committed_record_across_readers(generic_gpu, monkeypatch, state):
    queue, item, action, cas, checkout = generic_gpu
    captured = []
    recovered_at = []
    monkeypatch.setattr(pb, 'GPU_OBSERVATION_TIMEOUT_S', .02)

    def lost(action, op, **fields):
        if op == 'gpu_observation':
            captured.append(frozen_gpu({'gpu_uuids': ['GPU-real'] if state == 'used' else [],
                'gpu_evidence': {'sample_failed': False, 'live_sample': True, 'final_succeeded': True}},
                action_key=action['action_key'], deadline=fields['deadline_monotonic']))
        raise OSError('reply lost')

    def protected_read(action):
        recovered_at.append(time.monotonic())
        assert recovered_at[-1] >= captured[0]['deadline_monotonic']
        if state == 'absent':
            raise FileNotFoundError('no commit before deadline')
        return captured[0]

    monkeypatch.setattr(pb, '_gpu_broker_request', lost)
    monkeypatch.setattr(pb, '_read_broker_gpu_evidence', protected_read)
    if state == 'zero':
        with pytest.raises(pb.LocalActionError, match='zero distinct GPU UUIDs'):
            pb.run_local_action(action, cas_root=cas.root, checkout_root=checkout,
                                resource_demand=queue.demand_of(item))
        assert cas.lookup(action) is None
    else:
        result = pb.run_local_action(action, cas_root=cas.root, checkout_root=checkout,
                                     resource_demand=queue.demand_of(item))
        assert result['receipt']['resource_telemetry']['gpu_use'] == ('used' if state == 'used' else 'unobserved')
    telemetry = {} if state == 'absent' else {'gpu_record': captured[0]}
    monkeypatch.setattr(queue, 'cleanup_action_containers', lambda *a, **k: {
        'complete': True, 'resource_scope': {'telemetry': telemetry}})
    path = queue.finish(item['action_key'], status='failed' if state == 'zero' else 'executed',
        detail={'returncode': 1 if state == 'zero' else 0, 'stdout': '', 'stderr': ''}, claim_snapshot=item)
    terminal = json.loads(path.read_text())
    expected = 'unobserved' if state == 'absent' else state
    assert terminal['detail']['gpu_use'] == expected
    assert (path.parent.name == pool.record_store.FAILED) == (state == 'zero')
    # Both wait and MCP/result readers adopt the same immutable attempt.
    adopted = endings.resolve_attempt(queue.attempt_outcomes(terminal), invalid=pool.PoolContractError,
        validate_output_retry_stop=queue._validate_output_retry_stop)
    assert adopted['detail']['gpu_use'] == expected
    assert action_client.outcome_summary(queue, path, terminal)['succeeded'] == (state != 'zero')
    assert action_client.report_pool_outcome(queue, item['action_key'], wait_s=0,
                                            generation=item['published_unix']) == (1 if state == 'zero' else 0)
    verified = action_result.read_verified_ending_result(queue, item['action_key'],
        published_unix=item['published_unix'], attempt=1)
    assert verified['record']['detail']['gpu_use'] == expected
    if state == 'absent':
        assert terminal['detail']['gpu_use_reason'] == 'final_observation_failed'


def test_worker_redelivery_is_identical_and_never_resamples(monitored, monkeypatch):
    a, backend, rows, _, monitor = monitored
    request, record, path = rows[0]
    gpu = _RealGuardGpu(lambda: [_job(record, path, gpu_bytes=MIB)])
    live = type(monitor)(a, gpu)
    pid = worker_peer(a, record, monkeypatch)
    reply = a.handle(a.uid, pid, _query(request))
    leaf = a.gpu_evidence_dir / (record['scope_id'] + '.json')
    before = leaf.read_bytes(), leaf.stat().st_mtime_ns
    samples = len(gpu.samples)
    again = a.handle(a.uid, pid, _query(request, time.monotonic() - 1))
    assert again == reply
    assert (leaf.read_bytes(), leaf.stat().st_mtime_ns) == before
    assert len(gpu.samples) == samples
    live.poll_once()
    assert a.records[record['scope_id']]['gpu_record'] == reply['gpu_record']


@pytest.mark.parametrize('expired_during_sample', [False, True])
def test_broker_never_commits_after_deadline(monitored, monkeypatch, expired_during_sample):
    a, backend, rows, _, monitor = monitored
    request, record, path = rows[0]
    pid = worker_peer(a, record, monkeypatch)
    deadline = time.monotonic() - 1
    if expired_during_sample:
        deadline = time.monotonic() + .02
        def late(scope, until):
            # Wait for a moment in time: this models a timed-out sampler.
            time.sleep(max(0, until - time.monotonic()) + .005)
            a.freeze_gpu_record(scope, until, final_succeeded=True)
        monkeypatch.setattr(a, 'gpu_observer', late)
    with pytest.raises(ValueError, match='deadline'):
        a.handle(a.uid, pid, _query(request, deadline))
    assert 'gpu_record' not in a.records[record['scope_id']]
    assert not (a.gpu_evidence_dir / (record['scope_id'] + '.json')).exists()
    # A fresh request cannot extend the first attempt's expired commit window.
    if expired_during_sample:
        with pytest.raises(ValueError, match='already in progress or expired'):
            a.handle(a.uid, pid, _query(request))


def test_payload_and_reused_worker_pid_cannot_finalize(monitored, monkeypatch):
    a, backend, rows, _, monitor = monitored
    request, record, path = rows[0]
    pid = worker_peer(a, record, monkeypatch)
    with pytest.raises(PermissionError, match='launched worker'):
        a.handle(a.uid, pid + 1, _query(request))
    a.records[record['scope_id']]['worker_process'] = {'pid': pid, 'starttime': 'old', 'parent': 1}
    with pytest.raises(PermissionError, match='launched worker'):
        a.handle(a.uid, pid, _query(request))
    assert not a.gpu_evidence_dir.exists()


def test_final_collection_does_not_wait_on_guard_lock(monitored, monkeypatch):
    a, backend, rows, _, monitor = monitored
    request, record, path = rows[0]
    pid = worker_peer(a, record, monkeypatch)
    monitor.sample_lock.acquire()
    try:
        reply = a.handle(a.uid, pid, _query(request))
    finally:
        monitor.sample_lock.release()
    assert reply['gpu_record']


def test_sampler_exception_is_unobserved_and_unhealthy(monitored, monkeypatch):
    a, backend, rows, gpu, monitor = monitored
    request, record, path = rows[0]
    def broken(*a, **kw):
        raise RuntimeError('census broke')
    monkeypatch.setattr(gpu, 'collect', broken)
    status = monitor.poll_once()
    assert any('sampler_error' in row['error'] for row in status['errors'])
    assert monitor.last_success_monotonic is None
    assert not monitor.healthy()
    frozen = monitor.observe_gpu(record['scope_id'], time.monotonic() + 10)
    judgment = pb.gpu_execution_judgment(frozen)
    assert (judgment['gpu_use'], judgment['gpu_use_reason']) == ('unobserved', 'final_observation_failed')
    assert frozen['resource_telemetry']['gpu_evidence']['sample_failed']


def test_settlement_removes_public_mirror(monitored, monkeypatch):
    a, backend, rows, _, monitor = monitored
    request, record, path = rows[0]
    pid = worker_peer(a, record, monkeypatch)
    reply = a.handle(a.uid, pid, _query(request))
    leaf = a.gpu_evidence_dir / (record['scope_id'] + '.json')
    assert leaf.exists()
    for op in ['stop', 'release']:
        a.handle(a.uid, pid, {'op': op, 'action_key': request['action_key'],
                            'nonce': request['nonce'], 'token': record['token']})
    assert not leaf.exists()
    assert a.records[record['scope_id']]['gpu_record'] == reply['gpu_record']


def test_live_telemetry_is_not_a_judgment_route():
    with pytest.raises(pb.ActionContractError):
        pb.gpu_execution_judgment({'gpu_uuids': ['GPU-forged'], 'gpu_evidence': {
            'sample_failed': False, 'live_sample': True, 'final_succeeded': True}})
    root = Path(__file__).resolve().parents[1]
    # Domain outcomes may be constructed only inside the one judgment. Other
    # consumers project it or test it for routing; none inspect sampling flags.
    for name in ['lifecycle.py', 'endings.py', 'census.py', 'action_result.py']:
        tree = ast.parse((root / 'src/prismabuild' / name).read_text())
        assert not any(isinstance(node, ast.Constant) and node.value in {
            'sample_failed', 'live_sample', 'final_succeeded', 'gpu_evidence', 'gpu_uuids'}
            for node in ast.walk(tree)), name
    source = (root / 'tools/fleet/resource_broker.py').read_text()
    tree = ast.parse(source)
    retain = next(node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)
                  and node.name == '_retain_gpu_observation')
    assert 'gpu_record' in ast.get_source_segment(source, retain)
    assert 'gpu_evidence_dir' not in ast.get_source_segment(source, retain)


def test_broker_restart_recovers_commit_before_private_update(monitored, monkeypatch):
    a, backend, rows, _, monitor = monitored
    request, record, path = rows[0]
    pid = worker_peer(a, record, monkeypatch)
    expected = a.handle(a.uid, pid, _query(request))['gpu_record']
    state = a.state_dir / (record['scope_id'] + '.json')
    private = json.loads(state.read_text())
    del private['gpu_record']
    state.write_text(json.dumps(private))
    restored = type(a)(a.state_dir, a.uid, backend, max_memory_bytes=a.max_memory_bytes)
    assert restored.records[record['scope_id']]['gpu_record'] == expected
