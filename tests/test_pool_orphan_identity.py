"""Quarantining an old stub must preserve its replacement and reservations."""
from prismabuild import pool

KEY = "b" * 64


def _queue(tmp_path):
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.records.ensure_layout()
    pool._write_json_atomic(queue.records.item_path(pool.record_store.READY, KEY), {"action_key": KEY})
    return queue


def test_quarantine_rereads_the_record_it_takes(tmp_path, monkeypatch):
    queue = _queue(tmp_path)
    path = queue.records.item_path(pool.record_store.READY, KEY)
    replacement = {"action_key": KEY, "worker_script": "/new-worker.py",
                   "cas_root": "/cas", "checkout_root": "/new-checkout",
                   "published_unix": 123.0}
    read = pool.record_store.read_queue_record
    replaced = False

    def replace_after_read(source, *args, **kwargs):
        nonlocal replaced
        record = read(source, *args, **kwargs)
        if source == path and not replaced:
            replaced = True
            pool._write_json_atomic(path, replacement)
        return record

    monkeypatch.setattr(pool.record_store, '_read_json', replace_after_read)
    assert queue.quarantine_orphans() == []
    assert read(path) == replacement
    assert not queue.records.item_path(pool.record_store.FAILED, KEY).exists()


def test_orphan_ready_copy_does_not_authorize_releasing_a_claim(tmp_path):
    queue = _queue(tmp_path)
    # A damaged duplicate in ready does not own an action already claimed.
    pool._write_json_atomic(queue.records.item_path(pool.record_store.CLAIMED, KEY),
                            {"action_key": KEY, "claimed_by": "live"})
    queue.ledger().ensure_capacity({"cpu": 1})
    assert queue.ledger().acquire(KEY, {"cpu": 1})
    assert queue.quarantine_orphans() == [KEY]
    assert queue.ledger().held() == {"cpu": 1}
    assert queue.records.item_path(pool.record_store.CLAIMED, KEY).exists()


def test_orphan_does_not_overwrite_existing_failure(tmp_path):
    queue = _queue(tmp_path)
    failed = queue.records.item_path(pool.record_store.FAILED, KEY)
    pool._write_json_atomic(failed, {"action_key": KEY, "status": "failed",
                                   "detail": {"stderr": "attributable error"}})
    original = failed.read_bytes()
    assert queue.quarantine_orphans() == [KEY]
    assert failed.read_bytes() == original
