"""A failed claim move cannot authorize reaper cleanup (#318)."""
import errno
from pathlib import Path

import pytest

from prismabuild import pool

KEY = "a" * 64


@pytest.mark.parametrize("unstarted", [False, True])
@pytest.mark.parametrize("err", [errno.EACCES, errno.ESTALE])
def test_failed_entomb_retains_claim_lease_and_capacity(tmp_path, monkeypatch, unstarted, err):
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.publish(action_key=KEY, cas_root=tmp_path / "cas",
                  checkout_root=tmp_path / "checkout", worker_script=tmp_path / "worker.py",
                  resources={"cpu": 1}, max_attempts=2, retry_safe=True)
    item = queue.claim(capacity={"cpu": 1})
    claim = queue.item_path(pool.CLAIMED, KEY)
    if unstarted:
        queue.lease_path(KEY).unlink()
        item["claimed_unix"] = pool._now() - 2 * pool.HEARTBEAT_S
        pool._write_json_atomic(claim, item)
    before = claim.read_bytes()
    lease = queue.lease_path(KEY)
    before_lease = lease.read_bytes() if lease.exists() else None
    real_rename = pool.os.rename

    def refuse(src, dst, *args, **kwargs):
        if Path(src) == claim and str(dst).endswith(pool.TOMBSTONE_SUFFIX):
            raise OSError(err, "injected claim rename refusal")
        return real_rename(src, dst, *args, **kwargs)

    monkeypatch.setattr(pool.os, "rename", refuse)
    assert queue.reap_stale(timeout_s=-1) == []
    assert claim.read_bytes() == before
    assert (lease.read_bytes() if lease.exists() else None) == before_lease
    assert queue.ledger().held() == {"cpu": 1}
    assert not queue.item_path(pool.READY, KEY).exists()
    assert not queue.item_path(pool.FAILED, KEY).exists()

    monkeypatch.setattr(pool.os, "rename", real_rename)
    assert queue.reap_stale(timeout_s=-1) == [KEY]
    assert queue.ledger().held() == {}
