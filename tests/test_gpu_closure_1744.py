"""GPU success requires device use attributed to the executed payload scope."""
from dataclasses import replace
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from prismabuild import action_client, client, core as pb, gpu_memory as gm, pool, submission
from test_core import _body
from test_gpu_memory import system, process, query, SID
from test_pool_resource_scope import scoped
from test_resource_broker import authority, monitored


def test_attributed_gpu_uuid_union_survives_empty_samples(monitored, tmp_path, monkeypatch):
    a, backend, rows, gpu, monitor = monitored
    request, record, path = rows[0]
    proc, cgroup, scope = system(tmp_path / 'census')
    process(proc, 10, f'/prismabuild.slice/{SID}/payload')
    process(proc, 11, f'/prismabuild.slice/{SID}/payload/container')
    process(proc, 20, '/foreign.slice')
    query(monkeypatch, rc=1)

    def sample():
        value = gm.collect([scope], proc_root=proc, cgroup_root=cgroup)
        info = path.stat()
        job = replace(value.jobs[0], scope_id=record['scope_id'],
                      cgroup_identity=(info.st_dev, info.st_ino))
        return replace(value, jobs=(job,))

    monkeypatch.setattr(gpu, 'collect', lambda *args, **kw: sample())
    monitor.poll_once()
    status = a.handle(a.uid, 1, {**request, 'op': 'status', 'token': record['token']})
    assert status['resource_telemetry']['gpu_devices_used'] is None
    query(monkeypatch, '20, GPU-visible, 1\n')
    monitor.poll_once()
    status = a.handle(a.uid, 1, {**request, 'op': 'status', 'token': record['token']})
    assert status['resource_telemetry']['gpu_devices_used'] == 0
    query(monkeypatch, '10, GPU-first, [N/A]\n11, GPU-first, 1\n10, GPU-second, 2\n20, GPU-visible, 3\n')
    monitor.poll_once()
    query(monkeypatch, '')
    monitor.poll_once()
    status = a.handle(a.uid, 1, {**request, 'op': 'status', 'token': record['token']})
    assert status['resource_telemetry']['gpu_devices_used'] == 2
    assert status['resource_telemetry']['gpu_uuids'] == ['GPU-first', 'GPU-second']


@pytest.mark.parametrize('scoped', ['gpu'], indirect=True)
@pytest.mark.parametrize('count', [0, 1, None])
def test_pool_finish_adjudicates_gpu_use(scoped, count):
    queue, item, calls = scoped
    detail = {'status': 'executed', 'returncode': 0, 'stdout': '', 'stderr': '',
              'resource_telemetry': {'gpu_devices_used': count}}
    path = queue.finish(item['action_key'], status='executed', detail=detail,
                        claim_snapshot=item)
    terminal = json.loads(path.read_text())
    if count == 0:
        assert path.parent.name == pool.record_store.FAILED
        assert terminal['status'] == 'failed'
        assert terminal['detail']['termination_reason'] == 'zero_gpu_devices_used'
        assert 'zero' in terminal['detail']['stderr'].lower()
        assert action_client.outcome_summary(queue, path, terminal)['succeeded'] is False
        assert action_client.report_pool_outcome(queue, item['action_key'], wait_s=0,
                                                generation=item['published_unix']) == 1
    else:
        assert terminal['status'] == 'executed'


def test_cpu_finish_accepts_no_gpu_use(scoped):
    queue, item, calls = scoped
    path = queue.finish(item['action_key'], status='executed',
                        detail={'returncode': 0, 'resource_telemetry': {'gpu_devices_used': 0}},
                        claim_snapshot=item)
    assert json.loads(path.read_text())['status'] == 'executed'


@pytest.mark.parametrize('count', [0, 1, 2])
def test_gpu_cas_publication_and_cache_keep_execution_evidence(tmp_path, monkeypatch, count):
    checkout = tmp_path / 'checkout'
    checkout.mkdir()
    body = _body(checkout)
    body['inputs'] = []
    body['params']['demand'] = {'cpu': 1, 'mem_gb': 1, 'gpu': 1}
    action = pb.seal_action(body)
    nonce = 'a' * 32
    scope = 'prismabuild-job' + pb.raw_sha256((action['action_key'] + nonce).encode())[:32] + '.slice'
    telemetry = {'gpu_devices_used': count, 'gpu_uuids': [f'GPU-{i}' for i in range(count)]}
    monkeypatch.setenv(pb.ACTION_NONCE_ENV, nonce)
    monkeypatch.setenv(pb.ACTION_SCOPE_ENV, scope)
    monkeypatch.setenv('PRISMABUILD_READER_HELPER_ROOT', str(Path(pb.__file__).resolve().parents[2]))
    status_file = tmp_path / 'status.json'
    monkeypatch.setenv(pb.ACTION_STATUS_PATH_ENV, str(status_file))
    requests = []

    class GpuReply:
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def settimeout(self, timeout): assert timeout == 10
        def connect(self, path): pass
        def sendall(self, raw): requests.append(json.loads(raw))
        def recv(self, bound):
            return (json.dumps({'ok': True, 'scope_id': scope, 'nonce': nonce,
                                'resource_telemetry': telemetry}) + '\n').encode()

    monkeypatch.setattr(pb.socket, 'socket', lambda *args: GpuReply())
    cas = pb.PrismaBuildCAS(tmp_path / 'cas')
    if count == 0:
        with pytest.raises(pb.LocalActionError, match='zero distinct GPU UUIDs'):
            pb.run_local_action(action, cas_root=cas.root, checkout_root=checkout)
        assert cas.lookup(action) is None
        assert json.loads(status_file.read_text())['termination_reason'] == 'zero_gpu_devices_used'
    else:
        first = pb.run_local_action(action, cas_root=cas.root, checkout_root=checkout)
        recorded = {**telemetry, 'scope_id': scope, 'nonce': nonce}
        assert first['receipt']['resource_telemetry'] == recorded
        assert client.cas_receipt_self_check(first['receipt']) is None
        assert cas.lookup(action)['resource_telemetry'] == recorded
        telemetry.update(gpu_devices_used=0, gpu_uuids=[])
        again = pb.run_local_action(action, cas_root=cas.root, checkout_root=checkout)
        assert again['status'] == 'cache_hit'
        assert again['receipt']['resource_telemetry'] == recorded
        assert json.loads(status_file.read_text())['resource_telemetry'] == recorded
        assert json.loads(status_file.read_text())['cached_execution'] is True
        receipt_file = cas.receipt_path(action['action_key'])
        damaged = json.loads(receipt_file.read_text())
        damaged['resource_telemetry'].update(gpu_devices_used=0, gpu_uuids=[])
        damaged['receipt_sha256'] = pb.canonical_sha256({k: v for k, v in damaged.items() if k != 'receipt_sha256'})
        receipt_file.chmod(0o644)
        receipt_file.write_text(json.dumps(damaged))
        receipt_file.chmod(0o444)
        assert client.cas_receipt_self_check(damaged) == 'cas-receipt-shape'
        with pytest.raises(pb.CASTamperError, match='zero distinct GPU UUIDs'):
            cas.lookup(action)
    assert requests == [{'op': 'gpu_observation', 'action_key': action['action_key'], 'nonce': nonce}]


@pytest.mark.parametrize('scoped', ['gpu'], indirect=True)
def test_pool_cache_retains_used_devices_over_idle_replay_scope(scoped, monkeypatch):
    queue, item, calls = scoped
    monkeypatch.setattr(queue, 'cleanup_action_containers', lambda *args, **kw: {
        'complete': True, 'resource_scope': {'telemetry': {'gpu_devices_used': 0}}})
    detail = {'status': 'cache_hit', 'returncode': 0, 'cached_execution': True,
              'resource_telemetry': {'gpu_devices_used': 1}}
    path = queue.finish(item['action_key'], status='cache_hit', detail=detail, claim_snapshot=item)
    record = json.loads(path.read_text())
    assert record['status'] == 'cache_hit'
    assert record['detail']['resource_telemetry']['gpu_devices_used'] == 1


def test_submission_and_shard_cpu_defaults_agree(monkeypatch):
    import importlib.util
    path = Path(__file__).resolve().parents[1] / 'tools/fleet/pbtest.py'
    spec = importlib.util.spec_from_file_location('gpu_closure_pbtest', path)
    pbtest = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(pbtest)
    monkeypatch.setattr(pbtest, 'RUNTIME_ROOT', Path('/mnt/shared/prismabuild-fleet/repo'))
    assert submission.default_gpu_placement(needs_gpu=False) == []
    assert pbtest._pbtest_tags(SimpleNamespace(tag=[], gpu=False)) == []
    assert pbtest._pbtest_tags(SimpleNamespace(tag=[], gpu=True)) == ['gb10']
    assert pbtest._pbtest_tags(SimpleNamespace(tag=['x86'], gpu=False)) == ['x86']
