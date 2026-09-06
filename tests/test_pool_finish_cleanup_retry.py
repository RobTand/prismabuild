"""A completed payload's cleanup retry preserves its real attempt outcome."""
import json

import pytest

from prismabuild import pool, resource_scope
from test_pool_resource_scope import scoped, _process


@pytest.mark.parametrize('status,expected', [('executed', pool.DONE), ('failed', pool.READY), ('timeout', pool.READY)])
def test_fresh_lease_cleanup_retry_preserves_finished_attempt(scoped, monkeypatch, status, expected):
    queue, item, calls = scoped
    key = item['action_key']
    _process(monkeypatch, queue, item, calls)
    queue.execute(item, containment=True)
    release = resource_scope.ResourceScope.release
    unavailable = True

    def delayed_release(scope):
        if unavailable:
            raise OSError('scope still populated')
        return release(scope)

    monkeypatch.setattr(resource_scope.ResourceScope, 'release', delayed_release)
    detail = {'returncode': 0 if status == 'executed' else 137,
              'stdout': 'the original result', 'termination_reason': 'timeout' if status == 'timeout' else status}
    path = queue.finish(key, status=status, detail=detail, claim_snapshot=item)
    pending = json.loads(path.read_text())
    assert pending['finish_pending']['status'] == status
    assert pending['finish_pending']['detail'] == detail
    assert pending['attempts'] == 0
    assert queue.lease_age(key) < pool.LEASE_TIMEOUT_S
    assert queue.reap_stale() == []
    assert queue.ledger().held() == {'cpu': 1, 'mem_gb': 2}
    assert queue.lease_path(key).exists()

    unavailable = False
    assert queue.reap_stale() == ([key] if expected == pool.READY else [])
    result = json.loads(queue.item_path(expected, key).read_text())
    attempts = queue.attempt_outcomes(result)
    assert len(attempts) == 1
    assert attempts[0]['status'] == status
    assert attempts[0]['detail'] == {k: v for k, v in detail.items() if k != 'stdout'}
    assert (queue.root / attempts[0]['logs']['stdout']['path']).read_text() == detail['stdout']
    assert 'finish_pending' not in result
    assert not queue.item_path(pool.CLAIMED, key).exists()
    assert not queue.lease_path(key).exists()
    assert queue.ledger().held() == {}
    assert queue.reap_stale() == []


def test_oom_cleanup_retry_preserves_kernel_evidence(scoped, monkeypatch):
    queue, item, calls = scoped
    key = item['action_key']
    _process(monkeypatch, queue, item, calls)
    item['max_attempts'] = 1
    pool._write_json_atomic(queue.item_path(pool.CLAIMED, key), item)
    sample = resource_scope.ResourceScope.sample
    monkeypatch.setattr(resource_scope.ResourceScope, 'sample',
                        lambda scope: {**sample(scope), 'oom_kill': 5, 'oom_local': 1, 'memory_peak_bytes': 2 * 1024**3})
    outcome = queue.execute(item, containment=True)
    assert outcome['termination_reason'] == 'memory_limit_oom'
    release = resource_scope.ResourceScope.release
    monkeypatch.setattr(resource_scope.ResourceScope, 'release',
                        lambda scope: (_ for _ in ()).throw(OSError('scope still populated')))
    queue.finish(key, status=outcome['status'], detail=outcome, claim_snapshot=item)
    monkeypatch.setattr(resource_scope.ResourceScope, 'release', release)
    assert queue.reap_stale() == []
    failed = json.loads(queue.item_path(pool.FAILED, key).read_text())
    assert failed['attempts'] == 1
    assert not queue.item_path(pool.READY, key).exists()
    attempt = queue.attempt_outcomes(failed)[0]
    assert attempt['status'] == 'failed'
    assert attempt['detail']['termination_reason'] == 'memory_limit_oom'
    assert attempt['detail']['resource_telemetry']['oom_kill'] == 5
    assert attempt['detail']['resource_telemetry']['memory_peak_bytes'] == 2 * 1024**3
    assert 'lease' not in attempt['detail'].get('reason', '')
    assert queue.ledger().held() == {}


def test_foreign_host_does_not_retry_finished_cleanup(scoped, monkeypatch):
    queue, item, calls = scoped
    _process(monkeypatch, queue, item, calls)
    queue.execute(item, containment=True)
    monkeypatch.setattr(resource_scope.ResourceScope, 'release',
                        lambda scope: (_ for _ in ()).throw(OSError('broker down')))
    path = queue.finish(item['action_key'], status='executed', claim_snapshot=item)
    before = path.read_bytes()
    calls.clear()
    monkeypatch.setattr(pool.socket, 'gethostname', lambda: 'foreign-host')
    assert queue.reap_stale(timeout_s=-1) == []
    assert path.read_bytes() == before
    assert calls == []


def test_cleanup_retry_does_not_remove_a_newly_claimed_attempt(scoped, monkeypatch):
    queue, item, calls = scoped
    key = item['action_key']
    _process(monkeypatch, queue, item, calls)
    queue.execute(item, containment=True)
    release = resource_scope.ResourceScope.release
    monkeypatch.setattr(resource_scope.ResourceScope, 'release',
                        lambda scope: (_ for _ in ()).throw(OSError('scope still populated')))
    queue.finish(key, status='failed', detail={'returncode': 1}, claim_snapshot=item)
    monkeypatch.setattr(resource_scope.ResourceScope, 'release', release)
    archive = queue.archive_attempt
    raced = []

    def concurrent_finish(record, **kw):
        result = archive(record, **kw)
        if not raced:
            raced.append(True)
            queue.finish(key, status='failed', detail={'returncode': 1}, claim_snapshot=item)
            retry = queue.claim(owner='second-attempt', capacity={'cpu': 1, 'mem_gb': 2})
            assert retry is not None
        return result

    monkeypatch.setattr(queue, 'archive_attempt', concurrent_finish)
    assert queue.reap_stale() == []
    live = json.loads(queue.item_path(pool.CLAIMED, key).read_text())
    assert live['claimed_by'] == 'second-attempt'
    assert live['attempts'] == 1
    assert 'finish_pending' not in live
    assert 'resource_scope' not in live
    assert json.loads(queue.lease_path(key).read_text())['owner'] == 'second-attempt'
    assert queue.ledger().held() == {'cpu': 1, 'mem_gb': 2}
    assert not queue.item_path(pool.READY, key).exists()
