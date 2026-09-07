"""A withdrawal already superseded cannot mutate the successor claim (#318)."""
import pytest
from prismabuild import pool, resource_scope
from test_pool_resource_scope import scoped, _process

KEY = "c" * 64


def test_withdraw_preserves_successor_that_claims_after_marker(tmp_path, monkeypatch):
    queue = pool.PoolQueue(tmp_path / "queue")

    def publish():
        queue.publish(action_key=KEY, cas_root=tmp_path / "cas",
                      checkout_root=tmp_path / "checkout", worker_script=tmp_path / "worker.py",
                      resources={"cpu": 1}, max_attempts=2, retry_safe=True)

    publish()
    first = queue.claim(owner="first", capacity={"cpu": 1})
    write = pool._write_json_atomic
    successor = {}

    def advance_after_marker(path, record):
        write(path, record)
        if path == queue.item_path(pool.WITHDRAWN, KEY) and not successor:
            # The holder sees the marker and stops; a fresh submission then
            # supersedes it and is admitted before the withdrawing host resumes.
            queue.finish(KEY, status="withdrawn", claim_snapshot=first)
            publish()
            successor.update(queue.claim(owner="second", capacity={"cpu": 1}))

    monkeypatch.setattr(pool, "_write_json_atomic", advance_after_marker)
    monkeypatch.setattr(pool, "find_launcher_pids", lambda key: [])
    queue.withdraw(KEY, signal_child=False)
    live = pool._read_json(queue.item_path(pool.CLAIMED, KEY))
    assert live is not None and pool._same_claim(live, successor)
    assert live["max_attempts"] == 2
    assert pool._read_json(queue.lease_path(KEY))["owner"] == "second"
    assert queue.ledger().held() == {"cpu": 1}


def test_resubmission_does_not_erase_original_stop_request(tmp_path):
    queue = pool.PoolQueue(tmp_path / "queue")
    def publish():
        queue.publish(action_key=KEY, cas_root=tmp_path / "cas",
                      checkout_root=tmp_path / "checkout", worker_script=tmp_path / "worker.py")
    publish()
    first = queue.claim(owner="first")
    queue.withdraw(KEY, signal_child=False)
    publish()
    second = pool._read_json(queue.item_path(pool.READY, KEY))
    assert queue.withdrawal_covers(first) is not None
    assert queue.withdrawal_covers(second) is None


def test_withdraw_restores_ready_replacement_without_cancelling_it(tmp_path, monkeypatch):
    queue = pool.PoolQueue(tmp_path / "queue")
    def publish():
        queue.publish(action_key=KEY, cas_root=tmp_path / "cas",
                      checkout_root=tmp_path / "checkout", worker_script=tmp_path / "worker.py")
    publish()
    first = pool._read_json(queue.item_path(pool.READY, KEY))
    write = pool._write_json_atomic
    replacement = {}
    def advance_after_marker(path, record):
        write(path, record)
        if path == queue.item_path(pool.WITHDRAWN, KEY) and not replacement:
            publish()
            replacement.update(pool._read_json(queue.item_path(pool.READY, KEY)))
    monkeypatch.setattr(pool, "_write_json_atomic", advance_after_marker)
    queue.withdraw(KEY, signal_child=False)
    assert pool._read_json(queue.item_path(pool.READY, KEY)) == replacement
    assert queue.withdrawal_covers(first) is not None
    assert queue.withdrawal_covers(replacement) is None


def test_local_stop_uses_exact_scope_and_retains_cleanup_ownership(scoped, monkeypatch):
    queue, item, calls = scoped
    _process(monkeypatch, queue, item, calls)
    queue.execute(item, containment=True)
    key = item["action_key"]
    path = queue.item_path(pool.CLAIMED, key)
    before = path.read_bytes()
    lease = queue.lease_path(key).read_bytes()
    calls.clear()
    result = queue.withdraw(key)
    assert result["signalled"]["nonce"] == item["resource_scope"]["nonce"]
    assert result["signalled"]["scope_id"] == item["resource_scope"]["scope_id"]
    assert calls == ["stop"]
    assert result["released"] == 0
    assert path.read_bytes() == before
    assert queue.lease_path(key).read_bytes() == lease
    assert queue.ledger().held() == {"cpu": 1, "mem_gb": 2}


def test_broker_stop_refusal_keeps_durable_decision(scoped, monkeypatch):
    queue, item, calls = scoped
    _process(monkeypatch, queue, item, calls)
    queue.execute(item, containment=True)
    def refuse(*args, **kwargs):
        raise OSError("broker unavailable")
    monkeypatch.setattr(resource_scope.ResourceScope, "terminate_owned", refuse)
    result = queue.withdraw(item["action_key"])
    assert result["signalled"] is None
    assert "broker unavailable" in result["stop_pending"]["stop_error"]
    assert queue.withdrawal_covers(item) is not None
    assert queue.ledger().held() == {"cpu": 1, "mem_gb": 2}


def test_corrupted_durable_decision_refuses_other_generation(tmp_path):
    queue = pool.PoolQueue(tmp_path / "queue")
    record = {"action_key": KEY, "published_unix": 12.0}
    decision = queue.withdrawal_decision_path(record)
    pool._write_json_atomic(decision, {**record, "published_unix": 13.0, "status": "withdrawn"})
    with pytest.raises(pool.PoolContractError, match="invalid withdrawal decision"):
        queue.withdrawal_covers(record)


def test_retiring_marker_does_not_remove_another_concurrent_decision(tmp_path, monkeypatch):
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    first = {"action_key": KEY, "published_unix": 12.0, "status": "withdrawn"}
    successor = {**first, "published_unix": 13.0}
    path = queue.item_path(pool.WITHDRAWN, KEY)
    pool._write_json_atomic(path, first)
    archive = queue._file_superseded
    def publish_after_archive(*args, **kwargs):
        result = archive(*args, **kwargs)
        pool._write_json_atomic(path, successor)
        return result
    monkeypatch.setattr(queue, "_file_superseded", publish_after_archive)
    queue._supersede_withdrawal(KEY)
    assert pool._read_json(path) == successor


@pytest.mark.parametrize("operation", ["finish", "reap_stale"])
def test_cancelled_cleanup_does_not_conclude_a_successor(tmp_path, monkeypatch, operation):
    queue = pool.PoolQueue(tmp_path / "queue")
    def publish():
        queue.publish(action_key=KEY, cas_root=tmp_path / "cas",
                      checkout_root=tmp_path / "checkout", worker_script=tmp_path / "worker.py",
                      resources={"cpu": 1})
    publish()
    first = queue.claim(owner="first", capacity={"cpu": 1})
    queue.withdraw(KEY, signal_child=False)
    cleanup = queue.cleanup_action_containers
    successor = {}
    def advance_during_cleanup(record, **kwargs):
        if not successor:
            successor["advancing"] = True
            queue.finish(KEY, status="withdrawn", claim_snapshot=first)
            publish()
            successor.update(queue.claim(owner="second", capacity={"cpu": 1}))
        return cleanup(record, **kwargs)
    monkeypatch.setattr(queue, "cleanup_action_containers", advance_during_cleanup)
    if operation == "finish":
        queue.finish(KEY, status="withdrawn", claim_snapshot=first)
    else:
        queue.reap_stale(timeout_s=-1)
    live = pool._read_json(queue.item_path(pool.CLAIMED, KEY))
    assert live is not None and pool._same_claim(live, successor)
    assert pool._read_json(queue.lease_path(KEY))["owner"] == "second"
    assert queue.ledger().held() == {"cpu": 1}


def test_empty_durable_decision_refuses_cancellation_verdict(tmp_path):
    queue = pool.PoolQueue(tmp_path / "queue")
    record = {"action_key": KEY, "published_unix": 12.0}
    decision = queue.withdrawal_decision_path(record)
    decision.parent.mkdir(parents=True)
    decision.write_bytes(b"")
    with pytest.raises(pool.PoolContractError, match="invalid withdrawal decision"):
        queue.withdrawal_covers(record)


@pytest.mark.parametrize("old_terminal", [False, True])
def test_durable_decision_is_an_ending_before_visible_marker(tmp_path, monkeypatch, old_terminal):
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
    import pbrun
    import pbstatus
    import pbwait
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.publish(action_key=KEY, cas_root=tmp_path / "cas",
                  checkout_root=tmp_path / "checkout", worker_script=tmp_path / "worker.py")
    original = pool._read_json(queue.item_path(pool.READY, KEY))
    if old_terminal:
        pool._write_json_atomic(queue.item_path(pool.DONE, KEY), {
            "action_key": KEY, "status": "executed", "published_unix": 1.0,
            "detail": {"returncode": 0}})
    write = pool._write_json_atomic
    def crash(path, record):
        if path == queue.item_path(pool.WITHDRAWN, KEY):
            raise OSError("injected lost visible-marker write")
        return write(path, record)
    monkeypatch.setattr(pool, "_write_json_atomic", crash)
    with pytest.raises(OSError, match="lost visible-marker"):
        queue.withdraw(KEY)
    assert queue.claim() is None
    assert queue.find_key(KEY[:12]) == KEY
    assert pbwait.resolve_key(queue, KEY[:12], lane_root=tmp_path / "slurm") == KEY
    path, ending = pbrun.landed_outcome(
        queue, KEY, wait_s=0, generation=original["published_unix"])
    assert ending["status"] == "withdrawn"
    assert path == queue.withdrawal_decision_path(original)
    statuses = [row["status"] for row in pbstatus.read_endings(queue.root)]
    assert statuses[0] == "withdrawn"
    assert len(statuses) == (2 if old_terminal else 1)


@pytest.mark.parametrize("kind", ["writable", "symlink"])
def test_durable_decision_refuses_mutable_authority(tmp_path, kind):
    queue = pool.PoolQueue(tmp_path / "queue")
    record = {"action_key": KEY, "published_unix": 12.0, "status": "withdrawn"}
    decision = queue.withdrawal_decision_path(record)
    decision.parent.mkdir(parents=True)
    if kind == "writable":
        pool._write_json_atomic(decision, record)
        decision.chmod(0o644)
    else:
        target = tmp_path / "decision"
        pool._write_json_atomic(target, record)
        target.chmod(0o444)
        decision.symlink_to(target)
    with pytest.raises(pool.PoolContractError, match="invalid withdrawal decision"):
        queue.withdrawal_covers(record)
