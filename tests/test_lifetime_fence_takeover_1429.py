"""Claim custody and owner isolation under the lifetime contract (#1429).

A refusal after the reservation committed must return the unstarted claim's
tokens, and a stale owner must never touch a live successor.
"""
from __future__ import annotations

import subprocess
import sys

import pytest

from prismabuild import core as pb, lifetime_fence, pool
from lifetime_fixtures_1429 import FENCE_S, claim as claim_fenced, fenced_queue


@pytest.mark.parametrize("fault", ["expired", "mismatched", "unreadable", "missing"])
def test_refusal_after_commit_returns_unstarted_claim_resources(tmp_path, monkeypatch, fault):
    queue, action = fenced_queue(tmp_path)
    key = action["action_key"]
    ready_path = queue.item_path(pool.READY, key)
    before = ready_path.read_bytes()
    original = pool.ResourceLedger.commit_acquire
    real_now = pool._now
    jumped = [False]

    def commit(ledger, action_key, handle):
        count = original(ledger, action_key, handle)
        assert action_key == key
        assert ledger.held_keys() == [key]
        if fault == "expired":
            jumped[0] = True
        elif fault == "mismatched":
            path = queue.item_path(pool.CLAIMED, key)
            row = pool._read_json(path)
            row["lifetime_deadline_unix"] += 1
            pool._write_json_atomic(path, row)
        else:
            request = queue.root.parent / "cas" / "requests" / key[:2] / f"{key}.json"
            if fault == "missing":
                request.unlink()
            else:
                request.chmod(0o600)
                request.write_text("unreadable request")
        return count

    monkeypatch.setattr(pool.ResourceLedger, "commit_acquire", commit)
    monkeypatch.setattr(pool, "_now", lambda: real_now() + (FENCE_S if jumped[0] else 0))
    claimed = queue.queue.claim(capacity={"cpu": 8, "mem_gb": 16},
                                tags=[lifetime_fence.LIFETIME_TAG])
    assert claimed is None
    assert queue.ledger().held_keys() == []
    assert not queue.item_path(pool.CLAIMED, key).exists()
    assert not queue.lease_path(key).exists()
    assert not queue.item_path(pool.INTENT, key).exists()
    # The rollback restores the bytes that won the rename, not caller data.
    if fault != "mismatched":
        assert ready_path.read_bytes() == before
    else:
        assert pool._read_json(ready_path)["lifetime_deadline_unix"] == (
            pool._read_json(ready_path)["published_unix"] + FENCE_S + 1)


def test_expiry_during_lease_write_rolls_back_persisted_claim(tmp_path, monkeypatch):
    queue, action = fenced_queue(tmp_path)
    key = action["action_key"]
    before = queue.item_path(pool.READY, key).read_bytes()
    real_write = queue.write_lease
    real_now = pool._now
    expired = [False]

    def write(*args, **kwargs):
        real_write(*args, **kwargs)
        expired[0] = True

    monkeypatch.setattr(queue, "write_lease", write)
    monkeypatch.setattr(pool, "_now", lambda: real_now() + (FENCE_S if expired[0] else 0))
    assert queue.queue.claim(capacity={"cpu": 8, "mem_gb": 16},
                             tags=[lifetime_fence.LIFETIME_TAG]) is None
    assert queue.item_path(pool.READY, key).read_bytes() == before
    assert queue.ledger().held() == {}
    assert not queue.item_path(pool.CLAIMED, key).exists()
    assert not queue.lease_path(key).exists()
    assert not queue.item_path(pool.INTENT, key).exists()


def test_stale_owner_cannot_release_or_renew_a_live_successor(tmp_path):
    queue, action = fenced_queue(tmp_path, max_attempts=3)
    first = claim_fenced(queue)
    key = first["action_key"]
    queue.finish(key, status="failed", detail={"returncode": 1}, claim_snapshot=first)
    successor = claim_fenced(queue)
    process = subprocess.Popen(
        [sys.executable, "-c", "import time; print('successor', flush=True); time.sleep(30)"],
        start_new_session=True, stdout=subprocess.PIPE, text=True)
    try:
        assert process.stdout.readline().strip() == "successor"
        queue.write_lease(key, owner=successor["claimed_by"], child_pid=process.pid,
                          claim_snapshot=successor)
        path = queue.item_path(pool.CLAIMED, key)
        live = path.read_bytes()
        lease = queue.lease_path(key).read_bytes()
        held = queue.ledger().held()
        queue.finish(key, status="timeout", detail={"termination_reason": "lifetime_fence"},
                     claim_snapshot=first)
        with pytest.raises(pool.PoolContractError):
            queue.write_lease(key, owner=first["claimed_by"], claim_snapshot=first)
        assert process.poll() is None
        assert path.read_bytes() == live
        assert queue.lease_path(key).read_bytes() == lease
        assert queue.ledger().held() == held
        assert pool._same_claim(pool._read_json(path), successor)
        assert successor["claimed_by"] != first["claimed_by"]
    finally:
        pb._terminate_process_group(process, grace_s=0.1)
        process.communicate(timeout=2)
