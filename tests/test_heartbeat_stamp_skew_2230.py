"""S1: bounded writer clock skew must never make a live incarnation stale."""
import subprocess
import sys

import pytest

from prismabuild import census, checkout_recovery, pool, record_store
from test_heartbeat_liveness_2230 import holder  # noqa: F401
from test_checkout_recovery_1465 import case, make_case  # noqa: F401


@pytest.mark.parametrize("skew", [0.5, 2., 59., pool.OFFER_FUTURE_TOLERANCE_S])
@pytest.mark.parametrize("judge", ["reaper", "mover", "order", "widow", "census"])
def test_bounded_future_stamp_keeps_every_judge_live(holder, monkeypatch, skew, judge):
    queue, key, item, clock = holder
    lease_path = queue.records.lease_path(key)
    lease = record_store.read_document(lease_path)
    clock[0] += 360
    stamp = dict(lease, heartbeat_unix=clock[0] + skew)
    queue.records.replace_record_at(queue._heartbeat_stamp_path(lease), stamp)
    before = lease_path.read_bytes()
    cleanup = []
    original = queue.cleanup_action_containers
    def observed(*args, **kwargs):
        cleanup.append(True)
        return original(*args, **kwargs)
    monkeypatch.setattr(queue, "cleanup_action_containers", observed)
    if judge == "reaper":
        assert queue.reap_stale() == []
        assert queue.records.item_path(record_store.CLAIMED, key).exists()
    elif judge == "mover":
        assert queue._mover_lease(key, item, now=clock[0])["lease_live"] is True
    elif judge == "order":
        live, evidence = queue._claim_order_ahead_live(key, now=clock[0], window_s=60.,
            prior={"evidence_unix": clock[0] - 3600, "accepted_count": 0, "staged_wait_exempt_s": 0.})
        assert live and evidence["evidence"] == "ahead-within-grace"
    elif judge == "widow":
        queue.records.item_path(record_store.CLAIMED, key).unlink()
        assert queue.sweep_widowed_leases() == []
    else:
        row = next(row for row in census.action_rows(queue.root).value["jobs"] if row["action_key"] == key)
        assert row["state"] == "CLAIMED" and row["stale"] is False
        assert row["lease_age_s"] == 0.
    assert cleanup == [], "live containers reached lease-lost cleanup"
    assert queue.lease_age(key) == 0.
    assert lease_path.read_bytes() == before
    assert queue.ledger().holder_tokens(key) == {"cpu": 1}


@pytest.mark.parametrize("skew", [0.5, 2., 59., pool.OFFER_FUTURE_TOLERANCE_S])
def test_checkout_recovery_refuses_a_bounded_future_stamp(case, monkeypatch, skew):
    queue, key = case.queue.queue, case.key
    now = pool._now()
    monkeypatch.setattr(pool, "_now", lambda: now)
    lease = {**case.claim, "schema": pool.POOL_LEASE_SCHEMA_V1, "action_key": key,
             "owner": case.claim["claimed_by"], "host": case.claim["claimed_host"],
             "pid": 99999999, "pid_start_ticks": 42, "child_pid": None,
             "heartbeat_unix": now - 360}
    queue.records.replace_record_at(queue.records.lease_path(key), lease)
    queue.records.replace_record_at(queue._heartbeat_stamp_path(lease), dict(lease, heartbeat_unix=now + skew))
    with pytest.raises(ValueError, match="lease.*live"):
        checkout_recovery._checkout_recovery_terminal(queue, key, case.proc_root)
    assert case.directory.exists()


def _owned_dead_lease(holder, *, stamp_skew):
    queue, key, _, clock = holder
    child = subprocess.Popen([sys.executable, "-c", "import sys; sys.stdin.buffer.read()"], stdin=subprocess.PIPE)
    try:
        lease = record_store.read_document(queue.records.lease_path(key))
        lease.update(pid=child.pid, pid_start_ticks=pool._contained_worker_start_ticks(child.pid))
        assert lease["pid_start_ticks"] is not None
        queue.records.replace_record_at(queue.records.lease_path(key), lease)
        clock[0] += 360
        queue.records.replace_record_at(queue._heartbeat_stamp_path(lease),
            dict(lease, heartbeat_unix=clock[0] + stamp_skew))
    finally:
        child.stdin.close()
        child.wait(timeout=10)
    return queue, key, lease, clock


def test_excessive_future_stamp_adds_no_credit_and_dead_owner_recovers(holder):
    queue, key, lease, clock = _owned_dead_lease(holder, stamp_skew=pool.OFFER_FUTURE_TOLERANCE_S + 1)
    assert queue.lease_age(key) > pool.LEASE_TIMEOUT_S
    assert queue.reap_stale() == [key]
    assert not queue.ledger().holder_tokens(key)


def test_dead_owner_with_two_second_skew_is_reaped_within_the_bound(holder):
    queue, key, lease, clock = _owned_dead_lease(holder, stamp_skew=2.)
    last_observed, skew = clock[0], 2.
    stamped = last_observed + skew
    assert queue.reap_stale() == []
    clock[0] = last_observed + pool.LEASE_TIMEOUT_S
    assert queue.reap_stale() == []
    clock[0] = stamped + pool.LEASE_TIMEOUT_S
    assert queue.reap_stale() == []
    clock[0] += .1
    assert queue.reap_stale() == [key]
    assert stamped + pool.LEASE_TIMEOUT_S < clock[0] <= stamped + pool.LEASE_TIMEOUT_S + skew
    assert clock[0] - last_observed <= pool.LEASE_TIMEOUT_S + pool.OFFER_FUTURE_TOLERANCE_S
    assert queue.reap_stale() == []
    assert not queue.ledger().holder_tokens(key)
