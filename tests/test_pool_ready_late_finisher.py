"""A charged retry remains pending when its previous caller reports late (#234)."""
import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'tools' / 'fleet'))
from prismabuild import pool, resource_scope
import pbrun
from test_pool_resource_scope import scoped  # noqa: F401


@pytest.mark.parametrize('late_status', ['executed', 'failed'])
@pytest.mark.parametrize('republish', [False, True])
def test_late_result_preserves_a_ready_charged_retry(tmp_path, late_status, republish):
    queue = pool.PoolQueue(tmp_path / 'queue')
    key = 'a' * 64
    queue.publish(action_key=key, cas_root=tmp_path / 'cas',
                  checkout_root=tmp_path / 'checkout', worker_script=tmp_path / 'worker.py',
                  resources={'cpu': 1}, max_attempts=2, retry_safe=True)
    first = queue.claim(owner='original', capacity={'cpu': 1})
    assert first is not None
    # A real production reaper transition, with an expired lease and a charged
    # immutable attempt. No successor has claimed READY when the caller resumes.
    assert queue.reap_stale(timeout_s=-1) == [key]
    ready = queue.item_path(pool.READY, key)
    assert json.loads(ready.read_bytes())['attempts'] == 1
    original_outcome = queue.attempt_path(first, 1)
    original_bytes = original_outcome.read_bytes()
    if republish:
        queue.publish(action_key=key, cas_root=tmp_path / 'cas',
                      checkout_root=tmp_path / 'checkout', worker_script=tmp_path / 'worker.py',
                      resources={'cpu': 1}, max_attempts=2, retry_safe=True)
    saved = ready.read_bytes()
    retry = json.loads(saved)
    assert (retry['published_unix'] != first['published_unix']) == republish

    result = queue.finish(key, status=late_status,
                          detail={'returncode': 0 if late_status == 'executed' else 7,
                                  'stdout': 'late original result'}, claim_snapshot=first)

    assert ready.read_bytes() == saved
    assert not queue.item_path(pool.DONE, key).exists(), 'late caller ended the READY retry'
    assert not queue.item_path(pool.FAILED, key).exists(), 'late caller failed the READY retry'
    assert result == original_outcome
    assert original_outcome.read_bytes() == original_bytes
    assert pbrun.landed_outcome(queue, key, wait_s=0.01,
                               generation=retry['published_unix']) is None
    successor = queue.claim(owner='successor', capacity={'cpu': 1})
    assert successor is not None and successor['attempts'] == (0 if republish else 1)
    ending = queue.finish(key, status='executed', detail={'returncode': 0,
                          'stdout': 'successor result'}, claim_snapshot=successor)
    history = queue.attempt_outcomes(json.loads(ending.read_text()))
    assert [row['claimed_by'] for row in history] == (
        ['successor'] if republish else ['original', 'successor'])
    assert history[-1]['stdout'] == 'successor result'
    assert pbrun.await_outcome(queue, key, wait_s=1,
                              generation=retry['published_unix']) == 0
    assert queue.ledger().held() == {}


def test_late_ready_cleanup_refusal_is_recoverable_without_ending_the_retry(scoped, monkeypatch):
    queue, first, calls = scoped
    key = first['action_key']
    queue._start_resource_scope(first)
    assert queue.reap_stale(timeout_s=-1) == [key]
    ready = queue.item_path(pool.READY, key)
    saved = ready.read_bytes()
    outcome = queue.attempt_path(first, 1)
    original = outcome.read_bytes()
    request = resource_scope.ResourceScope._request
    observed = []

    def refuse_stop(scope, op, **extra):
        observed.append((scope.nonce, op))
        if op == 'stop':
            raise OSError('late scope cleanup unavailable')
        return request(scope, op, **extra)

    def no_action_cleanup(*args):
        pytest.fail('late caller reached action-wide Docker cleanup')

    with monkeypatch.context() as patch:
        patch.setattr(resource_scope.ResourceScope, '_request', refuse_stop)
        patch.setattr(pool.PoolQueue, '_cleanup_action_containers', no_action_cleanup)
        pending = queue.finish(key, status='executed', detail={'returncode': 0},
                               claim_snapshot=first)
        assert pending.exists() and pending.name.endswith(pool.LATE_FINISH_SUFFIX)
        assert ready.read_bytes() == saved and outcome.read_bytes() == original
        assert not queue.item_path(pool.DONE, key).exists()
        assert not queue.item_path(pool.FAILED, key).exists()
        assert all(nonce == first['resource_scope']['nonce'] for nonce, _ in observed)
        assert pbrun.landed_outcome(queue, key, wait_s=0.01,
                                   generation=first['published_unix']) is None

    recovered = pool.PoolQueue(queue.root)
    assert recovered.sweep_finish_tombstones(grace_s=-1) == [key]
    assert not pending.exists()
    assert ready.read_bytes() == saved and outcome.read_bytes() == original
    successor = recovered.claim(owner='successor', capacity=first['resources'])
    assert successor is not None
    recovered.finish(key, status='executed', detail={'returncode': 0},
                     claim_snapshot=successor)
    assert pbrun.await_outcome(recovered, key, wait_s=1,
                              generation=first['published_unix']) == 0
    assert recovered.ledger().held() == {}
