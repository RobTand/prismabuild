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

sys.path.insert(0, str(Path(__file__).resolve().parent))
from test_gang_reservation_1517 import (  # noqa: E402,F401
    HOSTS, _busy_both, fleet, gang_fleet)

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


@pytest.mark.parametrize("value", ["high", None, [1], {"p": 1}])
def test_a_ready_record_the_queue_cannot_order_does_not_blank_the_census(
        queue: pool.PoolQueue, value: object) -> None:
    _publish(queue, GOOD)
    _rewrite(_publish(queue, POISON), priority=value)
    assert pool.PoolQueue._unorderable_queue_field(
        json.loads(queue.item_path(pool.READY, POISON).read_text())) is not None

    census = mr._capture(queue)

    assert not census["measurements"]
    assert not census["elections"]
    assert not census["gang_elections"]


@pytest.mark.parametrize("value", [5.5, True, "5"])
def test_a_priority_the_queue_can_order_is_not_skipped(
        queue: pool.PoolQueue, value: object) -> None:
    """The queue reads these with ``int(...)``, so they can be claimed and then
    reach the CLAIMED census, which refuses them.  Skipping them as READY would
    launder them into that refusal, so the exception follows the queue's own
    definition of unorderable and nothing wider."""

    _rewrite(_publish(queue, POISON), priority=value)
    assert pool.PoolQueue._unorderable_queue_field(
        json.loads(queue.item_path(pool.READY, POISON).read_text())) is None

    with pytest.raises(mr.CensusUnavailable, match="unreadable publication priority"):
        mr._capture(queue)


@pytest.mark.parametrize("bad_field", [
    {"passes": "garbage"}, {"published_unix": "not a time"},
    {"passes": [1], "published_unix": None}])
def test_an_orderable_priority_beside_another_bad_field_is_not_skipped(
        queue: pool.PoolQueue, bad_field: dict) -> None:
    """``_unorderable_queue_field`` names the first bad ordering field, so a
    record with a readable-by-``int`` priority and an unreadable ``passes`` is
    reported as bad on ``passes``.  The queue reads that field as 0 before it
    orders, so it still lists and can claim the record, which would then reach
    the CLAIMED census.  The exception is for a result that names priority."""

    _rewrite(_publish(queue, POISON), priority=5.5, **bad_field)
    field = pool.PoolQueue._unorderable_queue_field(
        json.loads(queue.item_path(pool.READY, POISON).read_text()))
    assert field is not None and field[0] != "priority"

    with pytest.raises((mr.CensusUnavailable, pool.PoolContractError, ValueError)):
        mr._capture(queue)


def test_an_unreadable_priority_beside_another_bad_field_is_still_skipped(
        queue: pool.PoolQueue) -> None:
    """Priority is checked first, so the helper names it and the skip stands."""

    _publish(queue, GOOD)
    _rewrite(_publish(queue, POISON), priority="high", passes="garbage")
    field = pool.PoolQueue._unorderable_queue_field(
        json.loads(queue.item_path(pool.READY, POISON).read_text()))
    assert field is not None and field[0] == "priority"

    mr._capture(queue)


def test_a_skipped_record_is_never_claimed_so_it_cannot_reach_the_claimed_census(
        queue: pool.PoolQueue) -> None:
    _publish(queue, GOOD)
    _rewrite(_publish(queue, POISON), priority="high")

    first = queue.claim(owner="worker", capacity={"cpu": 4})
    second = queue.claim(owner="worker", capacity={"cpu": 4})

    assert first is not None and first["action_key"] == GOOD
    assert second is None
    assert queue.item_path(pool.READY, POISON).exists()
    assert not queue.item_path(pool.CLAIMED, POISON).exists()
    mr._capture(queue)          # and the census is still available after


def test_an_elected_gang_keeps_its_fences_when_its_ready_members_are_unorderable(
        gang_fleet) -> None:
    """A gang election fences its host while any member row is READY or
    CLAIMED.  A member the census skips as a candidate is still a live member."""

    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    _busy_both(publish, gclaim)
    group, keys = members("busy")
    for host in HOSTS:
        assert gclaim(host) is None
    before = mr._capture(queue)["gang_elections"]
    assert len(before) == 2, before

    for key in keys:
        _rewrite(queue.item_path(pool.READY, key), priority="high")

    after = mr._capture(queue)["gang_elections"]
    assert after == before, "the census dropped the host fences of a live gang"


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
