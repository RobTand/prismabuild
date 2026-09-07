"""A concluded tombstone remains the owner of unfinished capacity cleanup."""
import pytest

from prismabuild import pool


KEY = "d" * 64


def publish(queue, tmp_path):
    return queue.publish(action_key=KEY, cas_root=tmp_path / "cas",
                         checkout_root=tmp_path / "checkout",
                         worker_script=tmp_path / "worker.py", resources={"cpu": 1})


@pytest.mark.parametrize("operation", ["finish", "reap_stale"])
@pytest.mark.parametrize("successor", [False, True])
def test_cancelled_tombstone_recovers_capacity_after_lease_removal(
    tmp_path, monkeypatch, operation, successor,
):
    queue = pool.PoolQueue(tmp_path / "queue")
    publish(queue, tmp_path)
    first = queue.claim(owner="original", capacity={"cpu": 1})
    queue.withdraw(KEY, signal_child=False)
    def crash_before_release(self, key):
        raise RuntimeError("interrupted before reservation return")

    with monkeypatch.context() as patch:
        patch.setattr(pool.ResourceLedger, "release", crash_before_release)
        with pytest.raises(RuntimeError, match="interrupted before reservation return"):
            if operation == "finish":
                queue.finish(KEY, status="withdrawn", claim_snapshot=first)
            else:
                queue.reap_stale(timeout_s=-1)
    assert not queue.lease_path(KEY).exists()
    assert not queue.item_path(pool.CLAIMED, KEY).exists()
    assert queue.ledger().held() == {"cpu": 1}
    if successor:
        publish(queue, tmp_path)
        pending = queue.item_path(pool.READY, KEY).read_bytes()
    assert queue.sweep_finish_tombstones(grace_s=-1) == [KEY]
    assert queue.ledger().held() == {}
    assert queue.ledger().available() == {"cpu": 1}
    assert not list(queue.dir(pool.CLAIMED).glob(f"*{pool.TOMBSTONE_SUFFIX}"))
    if successor:
        assert queue.item_path(pool.READY, KEY).read_bytes() == pending
        assert queue.claim(owner="successor", capacity={"cpu": 1}) is not None


@pytest.mark.parametrize("refusal", ["scope", "release", "ambiguous"])
def test_tombstone_retains_recovery_authority_when_cleanup_is_unknown(
    tmp_path, monkeypatch, refusal,
):
    queue = pool.PoolQueue(tmp_path / "queue")
    publish(queue, tmp_path)
    first = queue.claim(owner="original", capacity={"cpu": 1})
    queue.withdraw(KEY, signal_child=False)
    tombstone, _ = queue._entomb_claim(KEY, expect=first)
    queue.lease_path(KEY).unlink()
    if refusal == "scope":
        monkeypatch.setattr(queue, "cleanup_action_containers",
                            lambda *args, **kwargs: {"complete": False})
    elif refusal == "release":
        monkeypatch.setattr(pool.ResourceLedger, "release", lambda *args: 0)
    else:
        foreign = queue.ledger("other-holder")
        foreign.ensure_capacity({"cpu": 1})
        assert foreign.acquire(KEY, {"cpu": 1})
    assert queue.sweep_finish_tombstones(grace_s=-1) == []
    assert tombstone.exists()
    assert queue.ledger().held() == {"cpu": 1}


def test_tombstone_does_not_release_a_claimed_successors_capacity(tmp_path):
    queue = pool.PoolQueue(tmp_path / "queue")
    publish(queue, tmp_path)
    first = queue.claim(owner="original", capacity={"cpu": 1})
    queue.withdraw(KEY, signal_child=False)
    saved = pool._read_json(queue.item_path(pool.CLAIMED, KEY))
    queue.finish(KEY, status="withdrawn", claim_snapshot=first)
    publish(queue, tmp_path)
    successor = queue.claim(owner="successor", capacity={"cpu": 1})
    # An old runtime's delayed tombstone appears beside the new live owner.
    tombstone = queue.dir(pool.CLAIMED) / f"{KEY}.1.old{pool.TOMBSTONE_SUFFIX}"
    pool._write_json_atomic(tombstone, saved)
    lease = queue.lease_path(KEY).read_bytes()
    queue.sweep_finish_tombstones(grace_s=-1)
    assert pool._same_claim(pool._read_json(queue.item_path(pool.CLAIMED, KEY)), successor)
    assert queue.lease_path(KEY).read_bytes() == lease
    assert queue.ledger().held() == {"cpu": 1}
