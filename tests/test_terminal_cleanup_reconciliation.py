"""Fail-closed boundaries for the legacy terminal cleanup repair (#1838)."""
import copy
import json

import pytest

from prismabuild import lifecycle, pool
from test_pool_resource_scope import scoped, _process
from test_scratch_lifetime_pool import lifetime, launch, leaf, runtime  # noqa: F401


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


@pytest.fixture(params=['finish', 'lease_expiry'])
def scratch_terminal(lifetime, monkeypatch, request):
    queue, item, variables, outcome = launch(lifetime, monkeypatch)
    key = item['action_key']
    if request.param == 'finish':
        path = queue.finish(key, status=outcome['status'], detail=outcome, claim_snapshot=item)
    else:
        claimed = queue.records.item_path(pool.record_store.CLAIMED, key)
        record = json.loads(claimed.read_text())
        record['max_attempts'] = 1
        pool.immutable_files.write_json_atomic(claimed, record)
        assert queue.reap_stale(timeout_s=-1) == [key]
        path = queue.records.item_path(pool.record_store.FAILED, key)
    terminal = json.loads(path.read_text())
    assert terminal['resource_scope_cleanup']['complete'] is True
    assert all(entry['cleaned'] for entry in terminal['scratch_lifetime_record']['entries'])
    assert not leaf(item).exists()
    terminal['container_cleanup_pending'] = {
        'complete': False, 'remaining': [], 'error': 'claiming host unavailable',
    }
    pool.immutable_files.write_json_atomic(path, terminal)
    return queue, item, path, terminal


@pytest.mark.parametrize('already_cleaned', [True, False])
def test_owner_poll_reconciles_terminal_with_real_scratch(scratch_terminal, monkeypatch, already_cleaned):
    queue, item, path, terminal = scratch_terminal
    key = item['action_key']
    # A crash may leave deletion complete but its last evidence write missing.
    # Exercise real idempotent directory cleanup and terminal-only persistence.
    if not already_cleaned:
        terminal['scratch_lifetime_record']['entries'][0]['cleaned'] = False
        pool.immutable_files.write_json_atomic(path, terminal)
    restarted = pool.PoolQueue(queue.root)
    monkeypatch.setattr(restarted, '_sweep_due', lambda **kw: True)
    monkeypatch.setattr(restarted, 'execute', lambda *a, **kw: pytest.fail('re-executed terminal'))
    if not already_cleaned:
        before = path.read_bytes()
        with monkeypatch.context() as fault:
            def failed_cleanup(*args):
                raise OSError('fixture scratch cleanup incomplete')
            fault.setattr(pool.scratch_lifetime, '_clean_scratch_directory', failed_cleanup)
            assert restarted.serve_once(capacity=item['resources']) is None
        assert path.read_bytes() == before
    assert restarted.serve_once(capacity=item['resources']) is None
    expected = copy.deepcopy(terminal)
    expected.pop('container_cleanup_pending')
    expected['scratch_lifetime_record']['entries'][0]['cleaned'] = True
    assert json.loads(path.read_text()) == expected
    assert not restarted.records.item_path(pool.record_store.CLAIMED, key).exists()
    assert not restarted.records.item_path(pool.record_store.READY, key).exists()
    assert not restarted.records.lease_path(key).exists()
    assert restarted.ledger().held() == {}


@pytest.mark.parametrize('conflict', [
    'snapshot_generation', 'terminal_generation', 'attempts', 'scope',
    'successor_ready', 'successor_claim', 'lease', 'tombstone', 'finish_pending',
])
def test_terminal_scratch_owner_refuses_conflicts(scratch_terminal, conflict):
    queue, item, path, terminal = scratch_terminal
    key = item['action_key']
    snapshot = copy.deepcopy(terminal)
    if conflict == 'snapshot_generation':
        snapshot['published_unix'] += 1
    elif conflict == 'terminal_generation':
        terminal['published_unix'] += 1
    elif conflict == 'attempts':
        terminal['attempts'] += 1
    elif conflict == 'scope':
        terminal['resource_scope']['nonce'] = 'e' * 32
    elif conflict == 'finish_pending':
        terminal['finish_pending'] = {'status': 'done', 'detail': {}}
    else:
        target = {
            'successor_ready': queue.records.item_path(pool.record_store.READY, key),
            'successor_claim': queue.records.item_path(pool.record_store.CLAIMED, key),
            'lease': queue.records.lease_path(key),
            'tombstone': queue.records.dir(pool.record_store.CLAIMED) / (
                key + '.fixture' + pool.record_store.TOMBSTONE_SUFFIX),
        }[conflict]
        pool.immutable_files.write_json_atomic(target, {**terminal, 'published_unix': 123})
        successor_before = target.read_bytes()
    pool.immutable_files.write_json_atomic(path, terminal)
    before = path.read_bytes()
    # Call the public wrapper directly as well as testing polling above: the
    # persistence boundary itself must refuse stale/contradictory authority.
    result = queue.cleanup_action_containers(snapshot, scratch_owner_path=path)
    assert result['complete'] is False
    assert path.read_bytes() == before
    if conflict in ('successor_ready', 'successor_claim', 'lease', 'tombstone'):
        assert target.read_bytes() == successor_before
