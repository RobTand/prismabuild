"""One READY record the queue cannot order must not blank the measurement census (#1506).

``ready_items`` files a READY record it cannot order and keeps going
(``test_a_record_the_queue_cannot_order_is_filed_not_thrown``).  The measurement
census, which runs in the same claim pass, raised ``CensusUnavailable
("unreadable publication priority")`` on the same record, so the claim recorded
``measurement_census_unavailable`` and every good record beside it was denied.

A READY record holds no tokens and runs nothing, so its priority cannot make
the census wrong.  A CLAIMED record is a running incumbent, and one the census
cannot read is unknown, so that stays strict.  Identity mismatches stay strict.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from prismabuild import _measurement_reservation as mr  # noqa: E402
from prismabuild import pool  # noqa: E402

GOOD = "a" * 64
POISON = "c" * 64


@pytest.fixture()
def queue(tmp_path: Path) -> pool.PoolQueue:
    q = pool.PoolQueue(tmp_path / "pb-queue")
    q.ensure_layout()
    return q


def _publish(queue: pool.PoolQueue, key: str) -> Path:
    queue.publish(action_key=key, cas_root=queue.root / "cas",
                  checkout_root=queue.root / "co",
                  worker_script=queue.root / "worker.py", resources={"cpu": 1})
    return queue.item_path(pool.READY, key)


def _rewrite(path: Path, **changes: object) -> None:
    record = json.loads(path.read_text())
    record.update(changes)
    path.write_text(json.dumps(record), encoding="utf-8")


@pytest.mark.parametrize("value", ["high", 5.5, None, True, [1]])
def test_a_ready_record_with_an_unreadable_priority_does_not_blank_the_census(
        queue: pool.PoolQueue, value: object) -> None:
    _publish(queue, GOOD)
    _rewrite(_publish(queue, POISON), priority=value)

    census = mr._capture(queue)

    assert set(census) == {"measurements", "elections", "selections",
                           "opportunities", "keys", "gang_elections"}


def test_a_claimed_record_with_an_unreadable_priority_still_refuses(
        queue: pool.PoolQueue) -> None:
    _publish(queue, POISON)
    assert queue.claim(owner="worker", capacity={"cpu": 4}) is not None
    _rewrite(queue.item_path(pool.CLAIMED, POISON), priority="high")

    with pytest.raises(mr.CensusUnavailable, match="unreadable publication priority"):
        mr._capture(queue)


def test_a_ready_record_whose_identity_is_wrong_still_refuses(
        queue: pool.PoolQueue) -> None:
    _rewrite(_publish(queue, POISON), action_key=GOOD, priority="high")

    with pytest.raises(mr.CensusUnavailable, match="identity mismatch"):
        mr._capture(queue)


def test_the_claim_beside_an_unorderable_ready_record_is_not_denied_for_the_census(
        queue: pool.PoolQueue) -> None:
    """The end the issue named: the denial the good record received."""

    _publish(queue, GOOD)
    _rewrite(_publish(queue, POISON), priority="high")

    claimed = queue.claim(owner="worker", capacity={"cpu": 4})

    assert claimed is not None and claimed["action_key"] == GOOD
