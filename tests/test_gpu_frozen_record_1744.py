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
from test_pbwait import pbwait
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
    scope = {'nonce': os.environ[pb.ACTION_NONCE_ENV], 'scope_id': os.environ[pb.ACTION_SCOPE_ENV]}
    item['resource_scope'] = scope
    queue.records.replace_record_at(queue.records.item_path(pool.record_store.CLAIMED, item['action_key']), item)
    stdout = '' if state == 'zero' else json.dumps(result) + '\n'
    telemetry = {} if state == 'absent' else {'gpu_record': captured[0]}
    monkeypatch.setattr(queue, 'cleanup_action_containers', lambda *a, **k: {
        'complete': True, 'resource_scope': {'telemetry': telemetry}})
    path = queue.finish(item['action_key'], status='failed' if state == 'zero' else 'executed',
        detail={'returncode': 1 if state == 'zero' else 0, 'stdout': stdout, 'stderr': ''}, claim_snapshot=item)
    terminal = json.loads(path.read_text())
    if state != 'zero':
        assert terminal['detail']['resource_telemetry'] == result['receipt']['resource_telemetry']
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
    assert verified['verified']
    assert (verified['receipt_sha256'] is None) == (state == 'zero')
    row = pbwait._from_ending(queue, (path, terminal))
    assert row['succeeded'] == (state != 'zero')
    assert row['status'] == terminal['status']
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
    with pytest.raises(PermissionError, match='launched worker'):
        a.handle(a.uid, pid + 1, _query(request))
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


def test_final_collection_has_bounded_guard_lock_wait(monitored, monkeypatch):
    a, backend, rows, _, monitor = monitored
    request, record, path = rows[0]
    pid = worker_peer(a, record, monkeypatch)
    monitor.sample_lock.acquire()
    try:
        reply = a.handle(a.uid, pid, _query(request, time.monotonic() + .15))
    finally:
        monitor.sample_lock.release()
    judgment = pb.gpu_execution_judgment(reply['gpu_record'])
    assert (judgment['gpu_use'], judgment['gpu_use_reason']) == ('unobserved', 'sampler_in_flight_at_freeze')


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


def _gpu_constructor_calls(source):
    """Resolve class names and aliases, independently of receiver spelling."""
    tree = ast.parse(source)
    aliases = {"GpuUseJudgment"}

    def constructor(node):
        return ((isinstance(node, ast.Name) and node.id in aliases)
                or (isinstance(node, ast.Attribute) and node.attr == "GpuUseJudgment")
                or (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                    and node.func.id == "getattr" and len(node.args) == 2
                    and isinstance(node.args[1], ast.Constant) and node.args[1].value == "GpuUseJudgment"))

    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            aliases.update(alias.asname or alias.name for alias in node.names if alias.name == "GpuUseJudgment")
        if isinstance(node, ast.Assign) and constructor(node.value):
            aliases.update(target.id for target in node.targets if isinstance(target, ast.Name))
    parents = {child: parent for parent in ast.walk(tree) for child in ast.iter_child_nodes(parent)}
    found = []
    for node in ast.walk(tree):
        if (not isinstance(node, ast.Call)
                or not (constructor(node.func) or any(key.arg == "_authority" for key in node.keywords))):
            continue
        parent = parents.get(node)
        while parent is not None and not isinstance(parent, (ast.FunctionDef, ast.AsyncFunctionDef)):
            parent = parents.get(parent)
        found.append((parent.name if parent is not None else None, node.lineno))
    return found


def test_live_telemetry_is_not_a_judgment_route():
    with pytest.raises(pb.ActionContractError):
        pb.gpu_execution_judgment({'gpu_uuids': ['GPU-forged'], 'gpu_evidence': {
            'sample_failed': False, 'live_sample': True, 'final_succeeded': True}})
    root = Path(__file__).resolve().parents[1]
    sites = [(path, owner) for directory in (root / 'src', root / 'tools')
             for path in directory.rglob('*.py') for owner, _ in _gpu_constructor_calls(path.read_text())]
    assert sites and all(path == root / 'src/prismabuild/core.py' and owner == 'gpu_execution_judgment'
                         for path, owner in sites), sites
    for directory in (root / 'src', root / 'tools'):
        for path in directory.rglob('*.py'):
            if path == root / 'src/prismabuild/core.py':
                continue
            for node in ast.walk(ast.parse(path.read_text())):
                name = (node.id if isinstance(node, ast.Name) else node.attr if isinstance(node, ast.Attribute)
                        else node.value if isinstance(node, ast.Constant) else None)
                assert name not in {'_authority', '_GPU_JUDGMENT_AUTHORITY', '_GPU_JUDGMENTS'}, path


@pytest.mark.parametrize('spelling', ['GpuUseJudgment', 'other.GpuUseJudgment', 'getattr(other, "GpuUseJudgment")', 'alias'])
def test_constructor_ast_tracks_receiver_and_alias(spelling):
    source = 'alias = other.GpuUseJudgment\ndef illicit():\n    return ' + spelling + '({})\n'
    assert _gpu_constructor_calls(source) == [('illicit', 3)]


def test_gpu_judgment_cannot_be_constructed_or_mutated_by_consumers():
    plain = {'gpu_use': 'used', 'gpu_devices_used': 1}
    with pytest.raises(pb.ActionContractError, match='authority'):
        pb.GpuUseJudgment(plain)
    for forged in (plain, object.__new__(pb.GpuUseJudgment)):
        with pytest.raises(pb.ActionContractError, match='GpuUseJudgment'):
            pb.GpuUseJudgment.require(forged)
    judgment = pb.gpu_execution_judgment(None)
    with pytest.raises(AttributeError, match='immutable'):
        judgment._encoded = b'{}'
    projection = judgment.as_dict()
    projection['gpu_use'] = 'used'
    assert judgment['gpu_use'] == 'unobserved'


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


def test_settled_scope_cannot_gain_a_late_mirror(monitored, monkeypatch):
    a, backend, rows, _, monitor = monitored
    request, record, path = rows[0]
    for op in ['stop', 'release']:
        a.handle(a.uid, 1, {'op': op, 'action_key': request['action_key'],
                           'nonce': request['nonce'], 'token': record['token']})
    with pytest.raises(ValueError, match='settled'):
        a.freeze_gpu_record(record['scope_id'], time.monotonic() + 10, final_succeeded=False)
    assert not (a.gpu_evidence_dir / (record['scope_id'] + '.json')).exists()


def test_scope_inventory_race_does_not_downgrade_sampler_health(monitored, monkeypatch):
    a, backend, rows, gpu, monitor = monitored
    real = backend.path
    missing = rows[0][1]['scope_id']
    monkeypatch.setattr(backend, 'path', lambda scope: Path('/nonexistent-prismabuild-fixture')
                        if scope == missing else real(scope))
    # The disappeared scope is dropped at inventory checking before the census.
    monkeypatch.setattr(monitor, '_sample_gpu', lambda records: (None, None))
    status = monitor.poll_once()
    assert status['errors'] and monitor.sample_error is None
    assert monitor.last_success_monotonic is not None


@pytest.mark.parametrize('state', ['used', 'zero', 'unobserved'])
def test_recordless_projection_cannot_establish_gpu_use(state):
    raw = {'gpu_use': state, 'gpu_use_reason': 'forged', 'gpu_devices_used': 1 if state == 'used' else 0,
           'gpu_uuids': ['GPU-forged'] if state == 'used' else [], 'nonce': 'a' * 32,
           'scope_id': 'prismabuild-job' + 'b' * 32 + '.slice'}
    with pytest.raises(pb.ActionContractError):
        pb.gpu_execution_judgment(raw)
    judgment = pb.gpu_execution_judgment(raw.get('gpu_record'))
    assert (judgment['gpu_use'], judgment['gpu_use_reason']) == ('unobserved', 'final_observation_failed')


def test_readonly_gpu_publication_descriptor_cannot_take_an_exclusive_ofd_lock(tmp_path):
    import fcntl
    import struct
    path = tmp_path / 'publication.lock'
    path.write_bytes(b'')
    descriptor = os.open(path, os.O_RDONLY)
    try:
        with pytest.raises(OSError):
            fcntl.fcntl(descriptor, fcntl.F_OFD_SETLK,
                        struct.pack('hhqqii', fcntl.F_WRLCK, os.SEEK_SET, 0, 0, 0, 0))
        fcntl.fcntl(descriptor, fcntl.F_OFD_SETLK,
                    struct.pack('hhqqii', fcntl.F_RDLCK, os.SEEK_SET, 0, 0, 0, 0))
    finally:
        os.close(descriptor)


def test_a_payload_read_lock_can_only_refuse_commit_without_blocking_guard(monitored, monkeypatch):
    import fcntl
    import struct
    a, backend, rows, _, monitor = monitored
    request, record, path = rows[0]
    worker_peer(a, record, monkeypatch)
    a.gpu_evidence_dir.mkdir(mode=0o755)
    a.gpu_evidence_dir.chmod(0o755)
    lock = a.gpu_evidence_dir / (record['scope_id'] + '.lock')
    lock.write_bytes(b'')
    # Root owns this file in production. Keep it writable for the simulated
    # broker UID until its open; the broker restores 0444 before locking.
    descriptor = os.open(lock, os.O_RDONLY)
    try:
        fcntl.fcntl(descriptor, fcntl.F_OFD_SETLK,
                    struct.pack('hhqqii', fcntl.F_RDLCK, os.SEEK_SET, 0, 0, 0, 0))
        with pytest.raises(BlockingIOError):
            monitor.observe_gpu(record['scope_id'], time.monotonic() + .2)
        assert not a.records[record['scope_id']].get('gpu_record')
        assert monitor.sample_lock.acquire(blocking=False)
        monitor.sample_lock.release()
        assert a.lock.acquire(blocking=False)
        a.lock.release()
        # Evidence readers' shared locks are compatible with payload readers.
        other = os.open(lock, os.O_RDONLY)
        try:
            fcntl.fcntl(other, fcntl.F_OFD_SETLK,
                        struct.pack('hhqqii', fcntl.F_RDLCK, os.SEEK_SET, 0, 0, 0, 0))
        finally:
            os.close(other)
    finally:
        os.close(descriptor)


def test_judgment_cannot_be_rebound_to_another_action():
    record = frozen_gpu({'gpu_uuids': ['GPU-real'], 'gpu_evidence': {
        'sample_failed': False, 'live_sample': True, 'final_succeeded': True}})
    judgment = pb.gpu_execution_judgment(record)
    with pytest.raises(pb.ActionContractError, match='another action'):
        pb.GpuUseJudgment.require(judgment, action_key='f' * 64)
