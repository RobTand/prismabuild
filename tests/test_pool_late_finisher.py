"""A worker may conclude only the attempt it actually executed.

``finish`` read whichever record occupied ``claimed/<key>.json`` and preferred
it over ``claim_snapshot`` without comparing identity. When a lease expired
while its launcher was still alive, the reaper requeued the action and a second
worker claimed the retry, the first worker's ``finish`` then advanced that
record's attempt counter, archived its own result under the second worker's
identity, filed the generation terminal, released the second worker's tokens
and removed its claim.
"""

from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from prismabuild import pool  # noqa: E402

KEY_A = "a" * 64


@pytest.fixture()
def queue(tmp_path: Path) -> pool.PoolQueue:
    q = pool.PoolQueue(tmp_path / "pb-queue")
    q.ensure_layout()
    return q


def _publish(q: pool.PoolQueue, **kw: object) -> None:
    q.publish(
        action_key=KEY_A,
        cas_root=q.root / "cas",
        checkout_root=q.root / "co",
        worker_script=q.root / "worker.py",
        resources={"cpu": 1},
        **kw,
    )


def test_a_late_finisher_does_not_conclude_the_newer_retry(
    queue: pool.PoolQueue,
) -> None:
    """The issue #62 probe, inverted: the retry keeps everything it owns."""

    _publish(queue, max_attempts=2, retry_safe=True)
    first = queue.claim(owner="attempt-1", capacity={"cpu": 1})
    assert first is not None
    # The documented path: the heartbeat is lost, the reaper requeues, and a
    # second worker claims the retry before attempt 1 calls finish.
    assert queue.reap_stale(timeout_s=-1) == [KEY_A]
    retry = queue.claim(owner="attempt-2", capacity={"cpu": 1})
    assert retry is not None and retry["attempts"] == 1

    result = queue.finish(
        KEY_A, status="executed", detail={"stdout": "old attempt result"},
        claim_snapshot=first,
    )

    # The retry is untouched: its claim, its lease and its reservation.
    live_path = queue.item_path(pool.CLAIMED, KEY_A)
    assert live_path.exists()
    live = json.loads(live_path.read_text())
    assert live["claimed_by"] == "attempt-2"
    assert live["attempts"] == 1
    assert queue.lease_path(KEY_A).exists()
    assert queue.ledger().held() == {"cpu": 1}
    assert queue.ledger().held_keys() == [KEY_A]

    # The generation is still running: no terminal record was filed for it.
    assert not queue.item_path(pool.DONE, KEY_A).exists()
    assert not queue.item_path(pool.FAILED, KEY_A).exists()
    assert not queue.item_path(pool.READY, KEY_A).exists()

    # Attempt 1's slot still describes attempt 1. The reaper filed the lease
    # loss there first, and an immutable outcome is first-writer-wins, so this
    # late result did not overwrite it.
    assert result == queue.attempt_path(first, 1)
    assert result.exists()
    archived = json.loads(result.read_text())
    assert archived["attempt"] == 1
    assert archived["claimed_by"] == "attempt-1"
    assert archived["status"] == "lease_lost"
    # And nothing was filed under attempt 2, which this worker never ran.
    assert not queue.attempt_path(first, 2).exists()


def test_a_late_result_is_archived_when_its_attempt_is_free(
    queue: pool.PoolQueue,
) -> None:
    """With no reaper record in the way, the late attempt files its own."""

    _publish(queue, max_attempts=3, retry_safe=True)
    first = queue.claim(owner="attempt-1", capacity={"cpu": 1})
    assert first is not None
    # A second claim of the same key without a reaper in between: the claim
    # was concluded by an operator's reset and re-claimed, so no attempt was
    # archived for attempt 1.
    queue.item_path(pool.CLAIMED, KEY_A).unlink()
    queue.ledger().release(KEY_A)
    _publish(queue, max_attempts=3, retry_safe=True)
    retry = queue.claim(owner="attempt-2", capacity={"cpu": 1})
    assert retry is not None
    assert retry["published_unix"] != first["published_unix"]

    result = queue.finish(
        KEY_A, status="failed", detail={"returncode": 1, "stdout": "mine"},
        claim_snapshot=first,
    )

    assert result == queue.attempt_path(first, 1)
    archived = json.loads(result.read_text())
    assert archived["claimed_by"] == "attempt-1"
    assert archived["status"] == "failed"
    assert archived["disposition"] == "requeued"
    assert archived["detail"]["late_finisher"]["live_claimed_by"] == "attempt-2"
    # Its own generation, so the live generation's history is not touched.
    assert archived["published_unix"] == first["published_unix"]
    live = json.loads(queue.item_path(pool.CLAIMED, KEY_A).read_text())
    assert live["claimed_by"] == "attempt-2"
    assert "attempt_history" not in live
    assert queue.ledger().held() == {"cpu": 1}


def test_the_finisher_of_its_own_claim_is_not_treated_as_late(
    queue: pool.PoolQueue,
) -> None:
    """The guard must not fire on the ordinary path, or nothing ever ends."""

    _publish(queue)
    claimed = queue.claim(owner="attempt-1", capacity={"cpu": 1})
    assert claimed is not None
    result = queue.finish(
        KEY_A, status="executed", detail={"returncode": 0},
        claim_snapshot=claimed,
    )
    assert result == queue.item_path(pool.DONE, KEY_A)
    assert json.loads(result.read_text())["claimed_by"] == "attempt-1"
    assert queue.ledger().held() == {}
    assert not queue.item_path(pool.CLAIMED, KEY_A).exists()
    assert not queue.lease_path(KEY_A).exists()


def test_a_rewritten_claimed_record_is_still_the_same_claim(
    queue: pool.PoolQueue,
) -> None:
    """The guards that rewrite a claim in place must not look like a retry."""

    _publish(queue)
    claimed = queue.claim(owner="attempt-1", capacity={"cpu": 1})
    assert claimed is not None
    live_path = queue.item_path(pool.CLAIMED, KEY_A)
    record = json.loads(live_path.read_text())
    # What withdraw's poison and a pending stop both do to a live claim.
    record["max_attempts"] = 1
    record["withdrawn_note"] = "withdrawn by an operator"
    record["stop_pending"] = {"holder_host": "sparky"}
    pool._write_json_atomic(live_path, record)

    assert pool._same_claim(record, claimed)
    result = queue.finish(
        KEY_A, status="failed", detail={"returncode": 1},
        claim_snapshot=claimed,
    )
    assert result == queue.item_path(pool.FAILED, KEY_A)
    assert queue.ledger().held() == {}
