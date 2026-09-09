"""A stale claim read cannot authorize cleanup of a different leased attempt."""
import json

import pytest
from prismabuild import pool


@pytest.mark.parametrize('reused_owner', [False, True])
@pytest.mark.parametrize('pending_finish', [False, True])
def test_reaper_preserves_successor_when_claim_and_lease_disagree(
    tmp_path, monkeypatch, capsys, reused_owner, pending_finish,
):
    queue = pool.PoolQueue(tmp_path / 'queue')
    resources = {'cpu': 1, 'mem_gb': 1}
    key, healthy_key = 'a' * 64, 'b' * 64

    def publish(key):
        queue.publish(action_key=key, cas_root=tmp_path / 'cas',
                      checkout_root=tmp_path, worker_script='worker.py',
                      resources=resources, max_attempts=2, retry_safe=True)

    publish(key)
    first = queue.claim(owner='original', capacity=resources)
    claim_path, lease_path = queue.item_path(pool.CLAIMED, key), queue.lease_path(key)
    # Reconstruct a legacy handoff, then inject only the old claim observation.
    # The durable claim, lease and reservation on disk belong to the successor.
    claim_path.unlink()
    lease_path.unlink()
    queue.ledger().release(key)
    publish(key)
    newer = queue.claim(owner='original' if reused_owner else 'successor', capacity=resources)
    assert not pool._same_claim(first, newer)
    saved = {path: path.read_bytes() for path in (claim_path, lease_path)}

    # One contradictory key must not prevent unrelated expired claims recovering.
    publish(healthy_key)
    healthy = queue.claim(owner='healthy', capacity={'cpu': 2, 'mem_gb': 2})
    assert healthy['action_key'] == healthy_key
    if pending_finish:
        first['finish_pending'] = {'status': 'executed', 'detail': {'returncode': 0}}
    cleanup_calls = []
    cleanup = queue.cleanup_action_containers
    read = pool._read_json
    later = pool._now() + pool.LEASE_TIMEOUT_S + 1

    def observed_cleanup(record, **kwargs):
        cleanup_calls.append(record['action_key'])
        return cleanup(record, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(pool, '_read_json',
                      lambda path, **kwargs: dict(first) if path == claim_path else read(path, **kwargs))
        patch.setattr(pool, '_now', lambda: later)
        patch.setattr(queue, 'cleanup_action_containers', observed_cleanup)
        requeued = queue.reap_stale()

    assert key not in cleanup_calls, 'contradictory claim reached payload cleanup'
    assert requeued == [healthy_key]
    assert cleanup_calls == [healthy_key]
    assert 'contradictory claim and lease identity' in capsys.readouterr().err
    assert all(path.read_bytes() == data for path, data in saved.items())
    assert queue.ledger().held() == resources
    for state in (pool.READY, pool.DONE, pool.FAILED):
        assert not queue.item_path(state, key).exists()
    assert not queue.attempt_path(first, 1).exists()

    # A later consistent read can recover the expired successor normally.
    with monkeypatch.context() as patch:
        patch.setattr(pool, '_now', lambda: later)
        assert queue.reap_stale() == [key]
    assert queue.ledger().held() == {}
    ending = json.loads(queue.attempt_path(newer, 1).read_text())
    assert ending['status'] == 'lease_lost'
