"""A released non-attempt cannot occupy the successor's execution history (#234)."""
import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'tools' / 'fleet'))
from prismabuild import pool, resource_scope
from test_pool_resource_scope import scoped  # noqa: F401
import pbrun


@pytest.mark.parametrize('late_status', ['failed', 'executed'])
@pytest.mark.parametrize('successor_state', [pool.READY, pool.CLAIMED])
def test_released_claim_cannot_supply_the_successors_outcome(
    tmp_path, monkeypatch, late_status, successor_state,
):
    queue = pool.PoolQueue(tmp_path / 'queue')
    key = 'a' * 64
    queue.publish(action_key=key, cas_root=tmp_path / 'cas',
                  checkout_root=tmp_path / 'checkout', worker_script=tmp_path / 'worker.py',
                  resources={'cpu': 1}, max_attempts=1, retry_safe=True)
    first = queue.claim(owner='original', capacity={'cpu': 1})
    assert first is not None
    # The supported missing-lease recovery shape, as in the #222 regression.
    # This models a lost/stalled publication, not a real NFS outage.
    queue.lease_path(key).unlink()
    later = pool._now() + pool.LEASE_TIMEOUT_S
    with monkeypatch.context() as patch:
        patch.setattr(pool, '_now', lambda: later)
        assert queue.reap_stale() == [key]
    if successor_state == pool.CLAIMED:
        successor = queue.claim(owner='successor', capacity={'cpu': 1})
        assert successor is not None
        paths = (queue.item_path(pool.CLAIMED, key), queue.lease_path(key))
    else:
        successor = json.loads(queue.item_path(pool.READY, key).read_text())
        paths = (queue.item_path(pool.READY, key),)
    assert successor['attempts'] == 0 and successor['unstarted_releases'] == 1
    saved = {p: p.read_bytes() for p in paths}

    # The original caller resumes; its snapshot is deliberately unchanged.
    with pytest.raises(pool.PoolContractError, match='claim changed'):
        queue.write_lease(key, owner='original', claim_snapshot=first)
    evidence = queue.finish(key, status=late_status,
                            detail={'returncode': 7 if late_status == 'failed' else 0,
                                    'stdout': 'original outcome'}, claim_snapshot=first)
    assert evidence.exists(), 'the late report must remain attributable'
    report = json.loads(evidence.read_text())
    assert report['claimed_by'] == 'original'
    assert report['detail']['stdout'] == 'original outcome'
    assert all(p.read_bytes() == data for p, data in saved.items())
    assert not queue.attempt_path(first, 1).exists()
    assert not queue.item_path(pool.DONE, key).exists()
    assert not queue.item_path(pool.FAILED, key).exists()
    assert queue.ledger().held() == ({'cpu': 1} if successor_state == pool.CLAIMED else {})
    if successor_state == pool.READY:
        successor = queue.claim(owner='successor', capacity={'cpu': 1})
        assert successor is not None

    ending = queue.finish(key, status='executed', detail={'returncode': 0,
                          'stdout': 'successor output'}, claim_snapshot=successor)
    assert ending == queue.item_path(pool.DONE, key)
    record = json.loads(ending.read_text())
    history = queue.attempt_outcomes(record)
    assert len(history) == 1
    assert history[0]['claimed_by'] == 'successor', (
        'an unstarted original claim supplied the successor execution outcome')
    assert history[0]['stdout'] == 'successor output'
    assert pbrun.await_outcome(queue, key, wait_s=1,
                              generation=first['published_unix']) == 0
    assert queue.ledger().held() == {}


def test_uncharged_late_scope_cleanup_refusal_keeps_the_successors_slot_free(scoped, monkeypatch):
    queue, first, calls = scoped
    key = first['action_key']
    queue._start_resource_scope(first)
    queue.lease_path(key).unlink()
    later = pool._now() + pool.LEASE_TIMEOUT_S
    with monkeypatch.context() as patch:
        patch.setattr(pool, '_now', lambda: later)
        assert queue.reap_stale() == [key]
    successor = queue.claim(owner='successor', capacity=first['resources'])
    assert successor is not None
    queue._start_resource_scope(successor)
    saved = {p: p.read_bytes() for p in (
        queue.item_path(pool.CLAIMED, key), queue.lease_path(key))}
    request = resource_scope.ResourceScope._request
    observed = []

    def refuse_old_stop(scope, op, **extra):
        observed.append((scope.nonce, op))
        if scope.nonce == first['resource_scope']['nonce'] and op == 'stop':
            raise OSError('original scope cleanup unavailable')
        return request(scope, op, **extra)

    monkeypatch.setattr(resource_scope.ResourceScope, '_request', refuse_old_stop)
    pending = queue.finish(key, status='failed', detail={'returncode': 1}, claim_snapshot=first)
    assert pending.name.endswith(pool.LATE_FINISH_SUFFIX)
    assert json.loads(pending.read_text())['container_cleanup_pending']['complete'] is False
    assert not queue.attempt_path(first, 1).exists()
    assert all(p.read_bytes() == data for p, data in saved.items())
    assert queue.ledger().held() == successor['resources']
    assert all(nonce == first['resource_scope']['nonce'] for nonce, _ in observed)

    monkeypatch.setattr(resource_scope.ResourceScope, '_request', request)
    assert queue.sweep_finish_tombstones(grace_s=-1) == [key]
    assert not pending.exists()
    assert not queue.attempt_path(first, 1).exists()
    assert all(p.read_bytes() == data for p, data in saved.items())
    assert queue.ledger().held() == successor['resources']
    ending = queue.finish(key, status='executed', detail={'returncode': 0},
                          claim_snapshot=successor)
    history = queue.attempt_outcomes(json.loads(ending.read_text()))
    assert len(history) == 1 and history[0]['claimed_by'] == 'successor'
