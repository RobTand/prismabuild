"""Mutable claim hosts must agree with exact committed reservation ledgers."""

import pytest

from prismabuild import pool

KEY = "9" * 64
REAL = "real-holder"
WRONG = "wrong-holder"


def _reservation(queue):
    ledger = queue.ledger(REAL)
    ledger.ensure_capacity({"cpu": 1})
    assert ledger.acquire(KEY, {"cpu": 1})
    return ledger


def _conflicting_claim(queue, tmp_path):
    queue.publish(
        action_key=KEY,
        cas_root=tmp_path / "cas",
        checkout_root=tmp_path / "checkout",
        worker_script=tmp_path / "worker.py",
        resources={"cpu": 1},
        max_attempts=2,
        retry_safe=True,
    )
    queue.item_path(pool.READY, KEY).replace(queue.item_path(pool.CLAIMED, KEY))
    ledger = _reservation(queue)
    claim = pool._read_json(queue.item_path(pool.CLAIMED, KEY))
    claim.update({
        "claimed_by": "owner",
        "claimed_unix": pool._now() - 10_000,
        "claimed_host": WRONG,
        "reserved_on": WRONG,
    })
    pool._write_json_atomic(queue.item_path(pool.CLAIMED, KEY), claim)
    pool._write_json_atomic(queue.lease_path(KEY), {
        "action_key": KEY,
        "owner": "owner",
        "host": WRONG,
        "heartbeat_unix": pool._now() - 10_000,
    })
    return ledger, claim


def test_widowed_lease_retains_conflicting_host_evidence(tmp_path):
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    ledger = _reservation(queue)
    pool._write_json_atomic(queue.lease_path(KEY), {
        "action_key": KEY,
        "host": WRONG,
        "heartbeat_unix": pool._now() - 10_000,
    })

    swept = queue.sweep_widowed_leases(timeout_s=60)
    assert {
        "swept": swept,
        "lease_exists": queue.lease_path(KEY).exists(),
        "held": ledger.held(),
    } == {
        "swept": [],
        "lease_exists": True,
        "held": {"cpu": 1},
    }


def test_stale_claim_retains_conflicting_host_evidence(tmp_path):
    queue = pool.PoolQueue(tmp_path / "queue")
    ledger, _ = _conflicting_claim(queue, tmp_path)

    reaped = queue.reap_stale(timeout_s=60)
    assert {
        "reaped": reaped,
        "claimed_exists": queue.item_path(pool.CLAIMED, KEY).exists(),
        "ready_exists": queue.item_path(pool.READY, KEY).exists(),
        "lease_exists": queue.lease_path(KEY).exists(),
        "held": ledger.held(),
    } == {
        "reaped": [],
        "claimed_exists": True,
        "ready_exists": False,
        "lease_exists": True,
        "held": {"cpu": 1},
    }


def test_finish_retains_a_claim_when_its_host_contradicts_the_ledger(tmp_path):
    queue = pool.PoolQueue(tmp_path / "queue")
    ledger, claim = _conflicting_claim(queue, tmp_path)

    with pytest.raises(pool.AmbiguousClaimHolder, match="contradictory claim holder"):
        queue.finish(KEY, status="executed", detail={"returncode": 0},
                     claim_snapshot=claim)

    assert queue.item_path(pool.CLAIMED, KEY).exists()
    assert queue.lease_path(KEY).exists()
    assert ledger.held() == {"cpu": 1}
    assert not queue.item_path(pool.DONE, KEY).exists()
    assert not queue.item_path(pool.FAILED, KEY).exists()


def test_unstarted_deferral_retains_contradictory_ownership(tmp_path):
    queue = pool.PoolQueue(tmp_path / "queue")
    ledger, claim = _conflicting_claim(queue, tmp_path)

    with pytest.raises(pool.AmbiguousClaimHolder, match="contradictory claim holder"):
        queue._defer_unstarted_claim(claim)

    assert queue.item_path(pool.CLAIMED, KEY).exists()
    assert queue.lease_path(KEY).exists()
    assert ledger.held() == {"cpu": 1}
    assert not queue.item_path(pool.READY, KEY).exists()


def test_withdraw_refuses_before_recording_a_contradictory_holder(tmp_path):
    queue = pool.PoolQueue(tmp_path / "queue")
    ledger, _ = _conflicting_claim(queue, tmp_path)

    with pytest.raises(pool.AmbiguousClaimHolder, match="contradictory claim holder"):
        queue.withdraw(KEY, by="operator", reason="test", signal_child=False)

    assert queue.item_path(pool.CLAIMED, KEY).exists()
    assert queue.lease_path(KEY).exists()
    assert ledger.held() == {"cpu": 1}
    assert not queue.item_path(pool.WITHDRAWN, KEY).exists()
    assert queue.withdrawal_decisions(KEY) == []


def test_recorded_host_remains_the_no_ledger_legacy_fallback(tmp_path):
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()

    assert queue.resolve_claim_holder(KEY, {"claimed_host": WRONG}) == WRONG
    assert queue.resolve_claim_holder(KEY, {"host": WRONG}) == WRONG
