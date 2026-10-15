"""A remote live claim keeps its authority until its own conclusion."""
from prismabuild import pool


def test_withdrawal_on_another_host_retains_the_real_claim_and_its_reservation(
        tmp_path, monkeypatch):
    queue = pool.PoolQueue(tmp_path / "queue")
    key = "a" * 64
    demand = {"cpu": 1, "mem_gb": 1}
    clock = [100.0]
    monkeypatch.setattr(pool, "_now", lambda: clock[0])
    queue.publish(action_key=key, cas_root=tmp_path / "cas",
                  checkout_root=tmp_path, worker_script="worker.py",
                  resources=demand)
    foreign = "history-foreign-host"
    with monkeypatch.context() as host:
        host.setattr(pool.socket, "gethostname", lambda: foreign)
        clock[0] += 1
        claimed = queue.claim(owner="foreign-holder", capacity=demand)
        assert claimed is not None and claimed["attempts"] == 0
        ledger = queue.ledger()
        before = queue.item_path(pool.CLAIMED, key).read_bytes()
        assert ledger.holder_tokens(key) == demand

    clock[0] += 1
    result = queue.withdraw(key, reason="remote operator", signal_child=False)
    assert result["released"] == 0
    assert queue.item_path(pool.CLAIMED, key).read_bytes() == before
    assert ledger.holder_tokens(key) == demand
    assert queue.ledger().held() == {}
    assert not queue.attempt_path(claimed, 1).exists()

    with monkeypatch.context() as host:
        host.setattr(pool.socket, "gethostname", lambda: foreign)
        clock[0] += 1
        queue.finish(key, status="withdrawn", claim_snapshot=claimed)
    assert not queue.item_path(pool.CLAIMED, key).exists()
    assert not queue.item_path(pool.READY, key).exists()
    assert ledger.holder_tokens(key) == {}
    assert ledger.available() == demand
    assert queue.attempt_path(claimed, 1).exists()
