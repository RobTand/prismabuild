"""A finisher's own cleanup must not delete the claim that replaced it.

``finish`` and ``reap_stale`` used to publish the item's next home and only
afterwards unlink ``claimed/<key>.json`` and its lease. A worker polling inside
that window claims the newly published retry, and the old finisher then deletes
that new claim and its new lease. The retry disappears from the live queue, and
because no claimed record and no lease survive it, a crash of the second worker
leaves a reservation no reaper can find.
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


def _tombstones(q: pool.PoolQueue) -> list[Path]:
    return sorted(q.dir(pool.CLAIMED).glob(f"*{pool.TOMBSTONE_SUFFIX}"))


def test_finish_does_not_delete_the_retrys_claim_or_lease(
    queue: pool.PoolQueue,
) -> None:
    """The issue #61 probe, inverted: attempt 2 keeps what it acquired."""

    _publish(queue, max_attempts=2, retry_safe=True)
    first = queue.claim(owner="attempt-1", capacity={"cpu": 1})
    assert first is not None

    original_write = pool._write_json_atomic
    observed: dict[str, object] = {}

    def write(path: Path, value: object) -> None:
        original_write(path, value)
        if path == queue.item_path(pool.READY, KEY_A):
            # Another poll sees the retry the moment it is published.
            observed["retry"] = queue.claim(
                owner="attempt-2", capacity={"cpu": 1})

    with mock.patch.object(pool, "_write_json_atomic", write):
        result = queue.finish(
            KEY_A, status="failed", detail={"returncode": 1},
            claim_snapshot=first,
        )

    assert observed["retry"] is None, "cleanup retains ownership until its tombstone is removed"
    retry = queue.claim(owner="attempt-2", capacity={"cpu": 1})
    assert retry is not None and retry["claimed_by"] == "attempt-2"
    # Everything attempt 2 acquired survives attempt 1's cleanup.
    assert queue.item_path(pool.CLAIMED, KEY_A).exists()
    assert queue.lease_path(KEY_A).exists()
    live = json.loads(queue.item_path(pool.CLAIMED, KEY_A).read_text())
    assert live["claimed_by"] == "attempt-2"
    assert json.loads(queue.lease_path(KEY_A).read_text())["owner"] == "attempt-2"
    assert queue.ledger().held() == {"cpu": 1}
    assert queue.ledger().held_keys() == [KEY_A]
    # The next poll claims the retry only after its predecessor's cleanup.
    assert result == queue.item_path(pool.READY, KEY_A)
    # No tombstone is left behind on the ordinary path.
    assert _tombstones(queue) == []


def test_reap_stale_does_not_delete_the_retrys_claim_or_lease(
    queue: pool.PoolQueue,
) -> None:
    """The same ordering, on the reaper's copy of it."""

    _publish(queue, max_attempts=3, retry_safe=True)
    first = queue.claim(owner="attempt-1", capacity={"cpu": 1})
    assert first is not None

    original_write = pool._write_json_atomic
    observed: dict[str, object] = {}

    def write(path: Path, value: object) -> None:
        original_write(path, value)
        if path == queue.item_path(pool.READY, KEY_A):
            observed["retry"] = queue.claim(
                owner="attempt-2", capacity={"cpu": 1})

    with mock.patch.object(pool, "_write_json_atomic", write):
        assert queue.reap_stale(timeout_s=-1) == [KEY_A]

    assert observed["retry"] is None, "cleanup retains ownership until its tombstone is removed"
    retry = queue.claim(owner="attempt-2", capacity={"cpu": 1})
    assert retry is not None and retry["claimed_by"] == "attempt-2"
    assert queue.item_path(pool.CLAIMED, KEY_A).exists()
    assert queue.lease_path(KEY_A).exists()
    live = json.loads(queue.item_path(pool.CLAIMED, KEY_A).read_text())
    assert live["claimed_by"] == "attempt-2"
    assert queue.ledger().held() == {"cpu": 1}
    assert _tombstones(queue) == []


def test_a_finisher_interrupted_mid_publish_is_recovered(
    queue: pool.PoolQueue,
) -> None:
    """A crash in the window leaves a tombstone, and it is put back."""

    _publish(queue, max_attempts=2, retry_safe=True)
    first = queue.claim(owner="attempt-1", capacity={"cpu": 1})
    assert first is not None

    class Interrupted(Exception):
        pass

    original_write = pool._write_json_atomic

    def write(path: Path, value: object) -> None:
        if path == queue.item_path(pool.READY, KEY_A):
            raise Interrupted
        original_write(path, value)

    with mock.patch.object(pool, "_write_json_atomic", write):
        with pytest.raises(Interrupted):
            queue.finish(
                KEY_A, status="failed", detail={"returncode": 1},
                claim_snapshot=first,
            )

    # The key is addressable nowhere, which is what the sweep exists to fix.
    assert not queue.item_path(pool.READY, KEY_A).exists()
    assert not queue.item_path(pool.CLAIMED, KEY_A).exists()
    tombstone = _tombstones(queue)
    assert len(tombstone) == 1

    # Not while a finisher could still be mid-publish.
    assert queue.sweep_finish_tombstones() == []
    assert _tombstones(queue) == [tombstone[0]]

    assert queue.sweep_finish_tombstones(grace_s=-1.0) == [KEY_A]
    assert _tombstones(queue) == []
    restored = queue.item_path(pool.CLAIMED, KEY_A)
    assert restored.exists()
    record = json.loads(restored.read_text())
    assert record["action_key"] == KEY_A
    assert record["worker_script"] == str(queue.root / "worker.py")

    # The ordinary reaper concludes it, and it adopts the attempt the finisher
    # archived rather than filing a lease loss over it. Age the restored claim
    # first: it has no lease, so the reaper reads claimed_unix and gives a
    # fresh claim one grace period. On the fleet the tombstone sat for the
    # lease timeout before the sweep looked, so that clock was long past.
    record["claimed_unix"] = pool._now() - 2 * pool.HEARTBEAT_S
    pool._write_json_atomic(restored, record)
    assert queue.reap_stale(timeout_s=-1) == [KEY_A]
    requeued = json.loads(queue.item_path(pool.READY, KEY_A).read_text())
    assert requeued["attempts"] == 1
    assert queue.attempt_outcomes(requeued)[-1]["status"] == "failed"


def test_a_redundant_tombstone_is_filed_and_never_restored(
    queue: pool.PoolQueue,
) -> None:
    """A key that has moved on must not have an old generation re-injected."""

    _publish(queue, max_attempts=2, retry_safe=True)
    first = queue.claim(owner="attempt-1", capacity={"cpu": 1})
    assert first is not None
    tombstone = queue._entomb_claim(KEY_A)
    assert tombstone is not None

    # A submitter, finding the key nowhere, publishes it again.
    queue.item_path(pool.READY, KEY_A).unlink(missing_ok=True)
    _publish(queue)
    fresh = json.loads(queue.item_path(pool.READY, KEY_A).read_text())
    assert fresh["published_unix"] != first["published_unix"]

    assert queue.sweep_finish_tombstones(grace_s=-1.0) == [KEY_A]
    assert _tombstones(queue) == []
    # The new generation is untouched, and the old bytes are kept as evidence.
    assert json.loads(
        queue.item_path(pool.READY, KEY_A).read_text()) == fresh
    assert not queue.item_path(pool.CLAIMED, KEY_A).exists()
    filed = sorted(queue.superseded_dir().glob(f"{KEY_A}.*.finish-tombstone.json"))
    assert len(filed) == 1
    kept = json.loads(filed[0].read_text())
    assert kept["claimed_by"] == "attempt-1"
    assert kept["published_unix"] == first["published_unix"]


def test_a_tombstone_is_invisible_to_every_reader_of_claimed(
    queue: pool.PoolQueue,
) -> None:
    """The suffix is what keeps the recovery from becoming a second defect."""

    _publish(queue)
    first = queue.claim(owner="attempt-1", capacity={"cpu": 1})
    assert first is not None
    assert queue._entomb_claim(KEY_A) is not None

    # The lease sweep reads claimed/ by its own glob and does not see a claim
    # beside the lease, so it treats the lease as widowed. That is the correct
    # reading: the claim has been moved aside and its finisher owns it.
    assert queue.sweep_widowed_leases(timeout_s=-1) == [KEY_A]
    # The reaper's glob does not see it as a claim, so nothing is requeued and
    # no attempt is charged, and its own tombstone sweep waits out the grace.
    assert queue.reap_stale(timeout_s=-1) == []
    assert len(_tombstones(queue)) == 1
    # An operator's prefix does not resolve while the record is aside. This is
    # the cost of the window, and it lasts as long as one publish.
    with pytest.raises(pool.PoolContractError):
        queue.find_key(KEY_A[:12])
