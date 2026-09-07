"""Malformed scope metadata must remain inside the cleanup refusal gate."""
import pytest
from prismabuild import pool


@pytest.mark.parametrize("control", [[], "invalid", 17])
def test_cached_cleanup_with_invalid_scope_preserves_finish(tmp_path, control):
    queue = pool.PoolQueue(tmp_path / "queue")
    key = "d" * 64
    queue.publish(action_key=key, cas_root=tmp_path / "cas",
                  checkout_root=tmp_path / "checkout", worker_script=tmp_path / "worker.py",
                  resources={"cpu": 1})
    record = queue.claim(capacity={"cpu": 1})
    record["resource_scope"] = control
    record["resource_scope_cleanup"] = {"complete": True, "nonce": "old"}
    path = queue.item_path(pool.CLAIMED, key)
    pool._write_json_atomic(path, record)
    detail = {"returncode": 0, "stdout": "completed payload"}
    assert queue.finish(key, status="executed", detail=detail, claim_snapshot=record) == path
    pending = pool._read_json(path)
    assert pending["finish_pending"] == {"status": "executed", "detail": detail}
    assert pending["container_cleanup_pending"]["complete"] is False
    assert queue.ledger().held() == {"cpu": 1}
