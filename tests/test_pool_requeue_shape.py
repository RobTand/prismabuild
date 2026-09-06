"""A ready item has one shape, whichever path put it there.

Three producers write into ``ready``: ``publish``, ``finish``'s retry branch
and ``reap_stale``'s.  The two requeues had drifted from each other and from
``publish``, so the same directory held items a consumer could tell apart by
the writer rather than by the work.  The fix is one writer, not a reader that
tolerates both.
"""

from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from prismabuild import pool  # noqa: E402

KEY_A = "a" * 64
KEY_B = "b" * 64

#: Fields that describe the claim that has just ended.  A ready item is not
#: claimed by anybody, so none of them may survive a requeue.
CLAIM_TRANSIENTS = ("claimed_by", "claimed_unix", "claimed_host", "reserved_on",
                    "cpu_allocation")

#: Fields whose values legitimately differ between two requeues of two
#: different actions: which action it is, where it may run, when it was
#: published, and how the attempt that has just ended went.  Everything else
#: is the shape, and the shape must not depend on which writer produced it.
PER_ACTION = (
    "action_key", "tags", "published_unix", "published_by", "requeued_unix",
    "status", "detail", "finished_unix", "finished_host", "attempt_history",
)


@pytest.fixture()
def queue(tmp_path: Path) -> pool.PoolQueue:
    q = pool.PoolQueue(tmp_path / "pb-queue")
    q.ensure_layout()
    return q


def _publish(q: pool.PoolQueue, key: str, **kw: object) -> None:
    q.publish(
        action_key=key,
        cas_root=kw.pop("cas_root", "/cas"),
        checkout_root=kw.pop("checkout_root", "/co"),
        worker_script=kw.pop("worker_script", "/w.py"),
        max_attempts=kw.pop("max_attempts", 3),
        retry_safe=kw.pop("retry_safe", True),
        resources={"cpu": 1},
        **kw,
    )


def _tag(key: str) -> str:
    """One placement tag per key, so each helper claims only its own item."""

    return key[0] * 2


def _requeued_by_finish(q: pool.PoolQueue, key: str) -> dict:
    _publish(q, key, tags=[_tag(key)])
    claimed = q.claim(tags=[_tag(key)], capacity={"cpu": 1},
                      cpu_tiers={"preferred": [0], "fallback": []})
    assert claimed is not None and claimed["action_key"] == key
    q.finish(key, status="failed", detail={"returncode": 3},
             claim_snapshot=claimed)
    return json.loads(
        q.item_path(pool.READY, key).read_text(encoding="utf-8")
    )


def _requeued_by_the_reaper(q: pool.PoolQueue, key: str) -> dict:
    _publish(q, key, tags=[_tag(key)])
    claimed = q.claim(tags=[_tag(key)], capacity={"cpu": 1},
                      cpu_tiers={"preferred": [0], "fallback": []})
    assert claimed is not None and claimed["action_key"] == key
    assert q.reap_stale(timeout_s=-1.0) == [key]
    return json.loads(
        q.item_path(pool.READY, key).read_text(encoding="utf-8")
    )


def _shape(record: dict) -> dict:
    return {k: v for k, v in record.items() if k not in PER_ACTION}


def test_both_requeues_write_the_same_shape(queue: pool.PoolQueue) -> None:
    by_finish = _requeued_by_finish(queue, KEY_A)
    by_reaper = _requeued_by_the_reaper(queue, KEY_B)
    assert set(by_finish) == set(by_reaper)
    assert _shape(by_finish) == _shape(by_reaper)


def test_a_requeued_item_says_it_is_an_item(queue: pool.PoolQueue) -> None:
    """``schema`` is the field that says which directory a record belongs in."""

    by_finish = _requeued_by_finish(queue, KEY_A)
    by_reaper = _requeued_by_the_reaper(queue, KEY_B)
    assert by_finish["schema"] == pool.POOL_ITEM_SCHEMA_V1
    assert by_reaper["schema"] == pool.POOL_ITEM_SCHEMA_V1


def test_a_requeued_item_carries_no_claim(queue: pool.PoolQueue) -> None:
    for record in (_requeued_by_finish(queue, KEY_A),
                   _requeued_by_the_reaper(queue, KEY_B)):
        for field in CLAIM_TRANSIENTS:
            assert field not in record, field
        assert "passes" not in record


def test_a_requeued_item_is_addressed_by_its_own_name(
    queue: pool.PoolQueue,
) -> None:
    """The filename is the identity; ``claim`` skips a record that disagrees."""

    for key, record in (
        (KEY_A, _requeued_by_finish(queue, KEY_A)),
        (KEY_B, _requeued_by_the_reaper(queue, KEY_B)),
    ):
        assert record["action_key"] == key


def test_a_consumer_handles_both(queue: pool.PoolQueue) -> None:
    _requeued_by_finish(queue, KEY_A)
    _requeued_by_the_reaper(queue, KEY_B)
    # Nothing here is an orphan: both are addressable and both are executable.
    assert queue.quarantine_orphans() == []
    assert {r["action_key"] for r in queue.ready_items()} == {KEY_A, KEY_B}
    for _ in range(2):
        claimed = queue.claim(tags=[_tag(KEY_A), _tag(KEY_B)])
        assert claimed is not None
        assert claimed["worker_script"] == "/w.py"
        assert claimed["cas_root"] == "/cas"
        assert claimed["checkout_root"] == "/co"
