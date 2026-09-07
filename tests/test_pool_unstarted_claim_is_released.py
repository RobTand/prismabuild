"""A claim that never started is released, not charged as a failed attempt.

``reap_stale`` gives a claimant ``HEARTBEAT_S`` to write its lease after the
rename, on the reasoning that the window is hundreds of milliseconds wide.
Measured on ``dl380g10`` on 2026-09-06, read-only, at load 0.44 across 80 CPUs
with nothing in ``D``, a single pool-record operation took **45.001 s** -- NFSv4
delegation recalls, on exactly the directories a different box rewrites.  The
window has no upper bound on this filesystem, so the grace is a guess about a
delegation recall and a bigger constant is a bigger guess.

What is not a guess is what the reaper does when it fires.  ``sparklina``
claimed ``0a44f2e0f62c``, its lease never appeared, and the record came back
``lease_lost_max_attempts`` with empty stdout and empty stderr: the payload
never started, and the action burned its only attempt anyway.  That is lost
*work*, not lost capacity -- the item left ``ready``, where another box could
have taken it, and returned unrunnable.

So: a claim with no lease, and no immutable attempt published under the number
it would take, never executed anything.  Release it to ``ready`` with its
attempt count untouched and let another box have it.  The discriminator is both
halves, because a lease is also absent from a claim a *finisher* archived and
then died holding -- ``finish`` publishes the attempt (``pool.py``
``archive_attempt``) before it entombs the claim, so the attempt is on disk and
that record must keep taking the charged path, where first-writer-wins makes
the finisher's real outcome the one the item adopts.

This does not stop a reaper taking a blocked-but-live claimant's claim.  That
is not knowable across boxes.  It stops the taking from destroying the work.

Issue #222.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys
from unittest import mock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from prismabuild import pool  # noqa: E402

KEY = "d" * 64


@pytest.fixture()
def queue(tmp_path: Path) -> pool.PoolQueue:
    return pool.PoolQueue(tmp_path / "queue")


def _publish(q: pool.PoolQueue, **kw: object) -> None:
    q.publish(
        action_key=KEY, cas_root=q.root / "cas", checkout_root=q.root / "co",
        worker_script=q.root / "worker.py", resources={"cpu": 1}, **kw,
    )


def _reap_later(q: pool.PoolQueue) -> list[str]:
    """Reap with the clock advanced past every grace, as a stalled box would."""

    later = pool._now() + pool.LEASE_TIMEOUT_S
    with mock.patch.object(pool, "_now", lambda: later):
        return q.reap_stale(timeout_s=pool.LEASE_TIMEOUT_S)


def test_a_claim_whose_lease_never_arrived_is_released(
    queue: pool.PoolQueue,
) -> None:
    """The observed loss: one attempt, no lease, no output, gone for good."""

    _publish(queue, max_attempts=1, retry_safe=True)
    assert queue.claim(owner="sparklina:43944:a4bcd411",
                       capacity={"cpu": 1}) is not None
    # The claimant blocked between the rename and ``write_lease``.
    queue.lease_path(KEY).unlink()

    assert _reap_later(queue) == [KEY]

    assert not queue.item_path(pool.FAILED, KEY).exists(), (
        "an action that never started was filed as a failed attempt"
    )
    ready = queue.item_path(pool.READY, KEY)
    assert ready.exists(), "the work was neither run nor returned to the queue"
    item = json.loads(ready.read_text())
    assert item.get("attempts", 0) == 0, "an attempt was charged for no execution"
    assert item["schema"] == pool.POOL_ITEM_SCHEMA_V1
    # Nothing was published under an attempt number nothing ran.
    assert list((queue.root / pool.ATTEMPTS).rglob("*.json")) == []
    # The claim, its lease and its tokens are all gone from the reaper's side.
    assert not queue.item_path(pool.CLAIMED, KEY).exists()
    assert not queue.lease_path(KEY).exists()
    assert queue.ledger().held() == {}


def test_the_release_is_recorded_where_an_investigator_looks(
    queue: pool.PoolQueue,
) -> None:
    """A release that left no trace would be a silent retry."""

    _publish(queue, max_attempts=1, retry_safe=True)
    assert queue.claim(owner="sparklina:1:a", capacity={"cpu": 1}) is not None
    queue.lease_path(KEY).unlink()

    _reap_later(queue)

    filed = list(queue.superseded_dir().glob(f"{KEY}.*.unstarted-claim.json"))
    assert len(filed) == 1, "a claim was released with nothing recorded about it"
    record = json.loads(filed[0].read_text())
    assert record["claimed_by"] == "sparklina:1:a"
    assert record["status"] == "released"
    item = json.loads(queue.item_path(pool.READY, KEY).read_text())
    assert item["unstarted_releases"] == 1


def test_a_lease_that_expired_is_still_a_charged_attempt(
    queue: pool.PoolQueue,
) -> None:
    """A claim that ran and stopped heartbeating is unchanged: it lost a lease."""

    _publish(queue, max_attempts=4, retry_safe=True)
    assert queue.claim(owner="attempt-1", capacity={"cpu": 1}) is not None
    # The lease is there and stale -- the shape of a worker that died running.

    assert queue.reap_stale(timeout_s=-1) == [KEY]

    item = json.loads(queue.item_path(pool.READY, KEY).read_text())
    assert item["attempts"] == 1
    assert item["status"] == "lease_lost"
    assert "unstarted_releases" not in item


def test_a_finisher_that_died_holding_the_claim_keeps_its_outcome(
    queue: pool.PoolQueue,
) -> None:
    """No lease, but an attempt is on disk: the charged path must still run.

    ``sweep_finish_tombstones`` restores exactly this record -- a finisher
    published its attempt, entombed the claim and died before publishing the
    item -- and relies on the missing-lease path archiving at the same attempt
    number so first-writer-wins hands the item the finisher's real outcome.  A
    release here would requeue an action that has already executed and drop
    what it did.
    """

    _publish(queue, max_attempts=4, retry_safe=True)
    claimed = queue.claim(owner="attempt-1", capacity={"cpu": 1})
    assert claimed is not None
    # The finisher's own attempt, published before it entombed the claim.
    queue.archive_attempt(
        {**claimed, "finished_unix": pool._now(), "finished_host": "sparklina"},
        attempt=1, status="executed", disposition=pool.DONE,
        detail={"returncode": 0, "stdout": "ran", "stderr": ""},
    )
    queue.lease_path(KEY).unlink()

    assert _reap_later(queue) == [KEY]

    assert not queue.item_path(pool.READY, KEY).exists(), (
        "an action that already ran was requeued as never started"
    )
    done = queue.item_path(pool.DONE, KEY)
    assert done.exists()
    filed = json.loads(done.read_text())
    assert filed["status"] == "executed", "the finisher's outcome was dropped"
    assert filed["attempts"] == 1


def test_a_withdrawn_claim_is_never_released_back_into_the_queue(
    queue: pool.PoolQueue,
) -> None:
    """Durable cancellation prevents an unstarted release after marker retirement."""

    _publish(queue, max_attempts=3, retry_safe=True)
    claimed = queue.claim(owner="attempt-1", capacity={"cpu": 1})
    assert claimed is not None
    original = queue.item_path(pool.CLAIMED, KEY).read_bytes()
    queue.withdraw(KEY, signal_child=False)
    assert queue.item_path(pool.CLAIMED, KEY).read_bytes() == original

    # A later publication may retire the summary while the cancelled holder
    # is still stopping. Its immutable generation decision remains authoritative.
    queue._supersede_withdrawal(KEY)
    queue.lease_path(KEY).unlink(missing_ok=True)
    blind = pool.PoolQueue(queue.root)
    assert blind.withdrawn_keys() == frozenset()

    later = pool._now() + pool.LEASE_TIMEOUT_S
    with mock.patch.object(pool, "_now", lambda: later):
        blind.reap_stale(timeout_s=pool.LEASE_TIMEOUT_S)

    assert not queue.item_path(pool.READY, KEY).exists(), (
        "an action the operator cancelled was released back into the queue"
    )
    assert not queue.item_path(pool.CLAIMED, KEY).exists()
    assert queue.ledger().held() == {}
    assert queue.withdrawal_covers(claimed)["status"] == "withdrawn"


def test_a_claim_lost_before_its_record_was_rewritten_is_released(
    queue: pool.PoolQueue,
) -> None:
    """The second confirmed drop, ``347e30ef932f``: the rename landed, the
    record rewrite did not.

    That claim carried no ``claimed_by``, no ``claimed_host`` and no
    ``claimed_unix`` at all, so the reaper reached it on the claim-intent clock
    rather than the claim's own.  It is the same loss on an earlier statement
    of ``claim``, and it must release for the same reason: the lease that
    proves a launch is exactly as absent.
    """

    _publish(queue, max_attempts=1, retry_safe=True)
    assert queue.claim(owner="sparklina:1:a", capacity={"cpu": 1}) is not None
    path = queue.item_path(pool.CLAIMED, KEY)
    record = json.loads(path.read_text())
    for field in ("claimed_by", "claimed_unix", "claimed_host"):
        record.pop(field, None)
    pool._write_json_atomic(path, record)
    queue.lease_path(KEY).unlink()

    assert _reap_later(queue) == [KEY]

    assert not queue.item_path(pool.FAILED, KEY).exists()
    item = json.loads(queue.item_path(pool.READY, KEY).read_text())
    assert item.get("attempts", 0) == 0
    assert item["unstarted_releases"] == 1
