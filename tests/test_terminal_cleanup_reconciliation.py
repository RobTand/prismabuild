"""Fail-closed boundaries for the legacy terminal cleanup repair (#1838)."""
import json

import pytest

from prismabuild import lifecycle, pool
from test_pool_resource_scope import scoped, _process


@pytest.fixture
def pending_terminal(scoped, monkeypatch):
    queue, item, calls = scoped
    _process(monkeypatch, queue, item, calls)
    queue.execute(item, containment=True)
    key = item['action_key']
    claimed = queue.records.item_path(pool.record_store.CLAIMED, key)
    record = json.loads(claimed.read_text())
    record['max_attempts'] = 1
    pool.immutable_files.write_json_atomic(claimed, record)
    assert queue.reap_stale(timeout_s=-1) == [key]
    path = queue.records.item_path(pool.record_store.FAILED, key)
    terminal = json.loads(path.read_text())
    terminal['container_cleanup_pending'] = {
        'complete': False, 'remaining': [], 'error': 'claiming host unavailable',
    }
    pool.immutable_files.write_json_atomic(path, terminal)
    calls.clear()
    return queue, path, terminal, calls


@pytest.mark.parametrize('boundary', [
    'foreign', 'missing_proof', 'wrong_nonce', 'wrong_scope', 'finish_pending',
    'successor_ready', 'successor_claim', 'lease', 'ambiguous_terminal',
])
def test_terminal_repair_refuses_uncertain_authority(pending_terminal, monkeypatch, boundary):
    queue, path, record, calls = pending_terminal
    key = record['action_key']
    if boundary == 'foreign':
        monkeypatch.setattr(pool.socket, 'gethostname', lambda: 'foreign-host')
    elif boundary == 'missing_proof':
        record.pop('resource_scope_cleanup')
    elif boundary == 'wrong_nonce':
        record['resource_scope_cleanup']['nonce'] = 'e' * 32
    elif boundary == 'wrong_scope':
        record['resource_scope']['scope_id'] = 'foreign-scope'
    elif boundary == 'finish_pending':
        record['finish_pending'] = {'status': 'done', 'detail': {}}
    else:
        target = {
            'successor_ready': queue.records.item_path(pool.record_store.READY, key),
            'successor_claim': queue.records.item_path(pool.record_store.CLAIMED, key),
            'lease': queue.records.lease_path(key),
            'ambiguous_terminal': queue.records.item_path(pool.record_store.DONE, key),
        }[boundary]
        pool.immutable_files.write_json_atomic(target, {**record, 'published_unix': 123})
    pool.immutable_files.write_json_atomic(path, record)
    before = path.read_bytes()
    monkeypatch.setattr(queue, 'cleanup_action_containers',
                        lambda *a, **kw: pytest.fail('untrusted terminal reached cleanup'))
    assert lifecycle.reconcile_terminal_cleanup(queue) == []
    assert path.read_bytes() == before
    assert calls == []


def test_terminal_repair_waits_for_public_scratch_finalization(pending_terminal, monkeypatch):
    queue, path, record, calls = pending_terminal
    before = path.read_bytes()
    monkeypatch.setattr(pool.scratch_lifetime, 'finalize',
                        lambda *a, **kw: {'used': True, 'complete': False})
    assert lifecycle.reconcile_terminal_cleanup(queue) == []
    assert path.read_bytes() == before
    assert 'stop' not in calls and 'release' not in calls
    monkeypatch.setattr(pool.scratch_lifetime, 'finalize',
                        lambda *a, **kw: {'used': True, 'complete': True})
    assert lifecycle.reconcile_terminal_cleanup(queue) == [record['action_key']]
    expected = dict(record)
    expected.pop('container_cleanup_pending')
    assert json.loads(path.read_text()) == expected
    assert lifecycle.reconcile_terminal_cleanup(queue) == []


def test_expiry_keeps_claim_when_scope_finished_but_scratch_pending(scoped, monkeypatch):
    queue, item, calls = scoped
    _process(monkeypatch, queue, item, calls)
    queue.execute(item, containment=True)
    monkeypatch.setattr(pool.scratch_lifetime, 'finalize',
                        lambda *a, **kw: {'used': True, 'complete': False})
    assert queue.reap_stale(timeout_s=-1) == []
    key = item['action_key']
    record = json.loads(queue.records.item_path(pool.record_store.CLAIMED, key).read_text())
    assert record['resource_scope_cleanup']['complete'] is True
    assert record['container_cleanup_pending']['complete'] is False
    assert record['container_cleanup_attempts'] == 1
    assert queue.ledger().held() == {'cpu': 1, 'mem_gb': 2}
    assert queue.records.lease_path(key).exists()
