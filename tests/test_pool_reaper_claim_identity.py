"""The reaper moves aside only the claim it judged.

``reap_stale`` reads a claim, decides whether it is requeued or concluded,
archives that decision, and only then moves the claim out of the way. The
archive is an NFS write, and the claim can conclude and be re-claimed while it
is in flight: the worker that held it finishes, the item returns to ``ready``,
and a second worker claims the retry. The reaper then moved *that* claim aside,
deleted its lease, released its reservation and republished the item, so a
worker was left running an action nothing recorded it held, on capacity the
ledger had already handed back.

The claim's identity decides. Two live claims of one key are the shape #61 and
#62 already covered from the finisher's side; this is the same shape from the
reaper's.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys
from unittest import mock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from prismabuild import pool  # noqa: E402

KEY_A = "a" * 64


@pytest.fixture()
def queue(tmp_path: Path) -> pool.PoolQueue:
    return pool.PoolQueue(tmp_path / "queue")


def _publish(q: pool.PoolQueue, **kw: object) -> None:
    q.publish(
        action_key=KEY_A, cas_root=q.root / "cas", checkout_root=q.root / "co",
        worker_script=q.root / "worker.py", resources={"cpu": 1}, **kw,
    )


def _reap_while_the_claim_turns_over(q: pool.PoolQueue, first: dict) -> dict:
    """Run one reap, concluding and re-claiming inside its archive write."""

    real = pool.PoolQueue.archive_attempt
    seen: dict[str, object] = {}

    def archive(self, record, **kw):          # type: ignore[no-untyped-def]
        out = real(self, record, **kw)
        if "done" not in seen:
            seen["done"] = True
            q.finish(KEY_A, status="failed", detail={"rc": 1},
                     claim_snapshot=first)
            seen["retry"] = q.claim(owner="attempt-2", capacity={"cpu": 1})
        return out

    with mock.patch.object(pool.PoolQueue, "archive_attempt", archive):
        seen["reaped"] = q.reap_stale(timeout_s=-1)
    return seen


def test_the_reaper_leaves_a_retry_that_claimed_under_it(
    queue: pool.PoolQueue,
) -> None:
    """The retry keeps its claim, its lease and its reservation."""

    _publish(queue, max_attempts=4, retry_safe=True)
    first = queue.claim(owner="attempt-1", capacity={"cpu": 1})
    assert first is not None

    seen = _reap_while_the_claim_turns_over(queue, first)

    retry = seen["retry"]
    assert retry is not None and retry["claimed_by"] == "attempt-2"
    # The reaper did not report a requeue it had no claim to requeue.
    assert seen["reaped"] == []
    live = queue.item_path(pool.CLAIMED, KEY_A)
    assert live.exists(), "the retry's claim was deleted by a reaper that never judged it"
    assert json.loads(live.read_text())["claimed_by"] == "attempt-2"
    assert queue.lease_path(KEY_A).exists()
    # The token the retry is running on is still charged to it.
    assert queue.ledger().held() == {"cpu": 1}
    # And the item was not put back in ready under a live claim.
    assert list(queue.dir(pool.READY).glob("*.json")) == []
    assert list(queue.dir(pool.CLAIMED).glob(f"*{pool.TOMBSTONE_SUFFIX}")) == []


def test_the_reaper_still_requeues_the_claim_it_did_judge(
    queue: pool.PoolQueue,
) -> None:
    """The ordinary path is unchanged: nothing turns over, so the claim moves."""

    _publish(queue, max_attempts=4, retry_safe=True)
    assert queue.claim(owner="attempt-1", capacity={"cpu": 1}) is not None

    assert queue.reap_stale(timeout_s=-1) == [KEY_A]

    assert not queue.item_path(pool.CLAIMED, KEY_A).exists()
    assert not queue.lease_path(KEY_A).exists()
    assert queue.item_path(pool.READY, KEY_A).exists()
    assert queue.ledger().held() == {}
    assert list(queue.dir(pool.CLAIMED).glob(f"*{pool.TOMBSTONE_SUFFIX}")) == []


def test_a_rewritten_claim_is_still_the_claim_the_reaper_judged(
    queue: pool.PoolQueue,
) -> None:
    """A guard rewriting the claimed record in place does not read as another.

    ``withdraw`` and the container-cleanup retry both rewrite this record while
    the claim stands. Treating a rewrite as a different claim would stop the
    reaper concluding a claim nobody else holds.
    """

    _publish(queue, max_attempts=4, retry_safe=True)
    first = queue.claim(owner="attempt-1", capacity={"cpu": 1})
    assert first is not None
    path = queue.item_path(pool.CLAIMED, KEY_A)

    real = pool.PoolQueue.archive_attempt

    def archive(self, record, **kw):          # type: ignore[no-untyped-def]
        out = real(self, record, **kw)
        touched = json.loads(path.read_text())
        touched["container_cleanup_checked_unix"] = 1.0
        path.write_text(json.dumps(touched))
        return out

    with mock.patch.object(pool.PoolQueue, "archive_attempt", archive):
        assert queue.reap_stale(timeout_s=-1) == [KEY_A]

    assert not path.exists()
    assert queue.item_path(pool.READY, KEY_A).exists()
    assert queue.ledger().held() == {}
