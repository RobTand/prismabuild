"""Lease recovery retires pending cleanup only after the owner proves it complete."""
import json

from prismabuild import pool
from test_pool_resource_scope import scoped  # noqa: F401


def test_reboot_cleanup_retry_clears_pending_before_terminal(scoped, monkeypatch):
    queue, item, calls = scoped
    key = item['action_key']
    queue._start_resource_scope(item)
    item['resource_scope']['boot_id'] = 'previous-boot'
    item['max_attempts'] = 1
    claim_path = queue.records.item_path(pool.record_store.CLAIMED, key)
    pool.immutable_files.write_json_atomic(claim_path, item)

    with monkeypatch.context() as foreign:
        foreign.setattr(pool.socket, 'gethostname', lambda: 'other-host')
        assert queue.reap_stale(timeout_s=-1) == []
    pending = json.loads(claim_path.read_text())
    assert pending['container_cleanup_pending']['complete'] is False
    assert 'claiming host' in pending['container_cleanup_pending']['error']
    assert queue.ledger().held() == item['resources']
    assert 'release' not in calls

    queue.reap_stale(timeout_s=-1)
    terminal = json.loads(queue.records.item_path(pool.record_store.FAILED, key).read_text())
    assert terminal['status'] == 'lease_lost_max_attempts'
    assert terminal['resource_scope_cleanup']['complete'] is True
    assert queue.ledger().held() == {}
    assert 'container_cleanup_pending' not in terminal
