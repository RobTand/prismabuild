"""One foreign sort field must not take the whole ready listing down (#612).

``ready_items`` has skipped a record nobody can parse since #212, and then
sorted the survivors on their own content:

    out.sort(key=lambda r: (-int(r.get("priority", 0)), ...))

A record whose ``priority`` or ``published_unix`` is a string parses fine and
raises ``ValueError`` out of that sort.  ``claim`` calls ``ready_items``
*before* its loop starts, so the raise escapes exactly the way #592's three
did -- and it takes the listing rather than the item: the claim scan, the
prewarm loop's claim-order read and the supervisor view all stop on one
foreign writer's file.

#592 deliberately left this one out, because its three sites sit inside the
per-item loop where ``record_denial`` names the item and the poll carries on,
and an enumerator has no such channel.  The policy chosen here is the one the
queue already keeps for a record nobody can parse: **skip it from the listing
and let the sweep file it.**  A record the queue cannot place in its own order
is a record no consumer can address -- it is never listed, never claimed,
never runs and never leaves ``ready`` -- which is the permanent-resident
defect ``quarantine_orphans`` exists for.  So the same three fields decide
both, in one definition, and the filed record names the field and the value,
in ``failed/`` where ``pbstatus`` counts it.

The alternative, keeping it visible in the listing at a defined position, was
rejected: it leaves a record this pool never wrote in front of every claim
scan on every box, and ``record_denial`` -- the only per-item channel there is
-- refuses an item whose ``published_unix`` is not a number, which is one of
the two poisons in the issue.

Nothing here touches the live queue.
"""

from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from prismabuild import pool  # noqa: E402

GOOD = "a" * 64
OTHER = "b" * 64
POISON = "c" * 64


@pytest.fixture()
def queue(tmp_path: Path) -> pool.PoolQueue:
    q = pool.PoolQueue(tmp_path / "pb-queue")
    q.ensure_layout()
    return q


def _publish(queue: pool.PoolQueue, key: str, **kw: object) -> None:
    queue.publish(
        action_key=key, cas_root=queue.root / "cas",
        checkout_root=queue.root / "co",
        worker_script=queue.root / "worker.py",
        resources={"cpu": 1}, **kw)


def _poison(queue: pool.PoolQueue, key: str, field: str, value: object) -> Path:
    """A ready record that parses and cannot be ordered.

    Written by rewriting a published one rather than by hand: ``publish``
    refuses both of these on the way in, so the only way one exists is a
    writer that is not this queue, and the rest of the record must stay
    exactly what this queue writes or the test would be proving something
    else.
    """

    _publish(queue, key)
    path = queue.item_path(pool.READY, key)
    record = json.loads(path.read_text())
    record[field] = value
    path.write_text(json.dumps(record))
    return path


# -- the listing survives ---------------------------------------------------


@pytest.mark.parametrize("field,value", [
    ("priority", "high"),
    ("priority", {"band": "high"}),
    ("published_unix", {"at": "noon"}),
    ("published_unix", "not a time"),
])
def test_a_foreign_sort_field_does_not_stop_the_listing(
    queue: pool.PoolQueue, field: str, value: object
) -> None:
    """The issue's own reproduction: before this, ``ready_items`` raised."""

    _publish(queue, GOOD)
    _publish(queue, OTHER)
    _poison(queue, POISON, field, value)

    listed = [str(item["action_key"]) for item in queue.ready_items()]

    assert sorted(listed) == sorted([GOOD, OTHER])


def test_the_other_records_still_claim(queue: pool.PoolQueue) -> None:
    """A listing that raises is a box that claims nothing, not one item lost."""

    _publish(queue, GOOD)
    _poison(queue, POISON, "priority", "high")

    claimed = queue.claim(owner="worker", capacity={"cpu": 4})

    assert claimed is not None and claimed["action_key"] == GOOD


@pytest.mark.parametrize("field,value", [("priority", "5"),
                                         ("published_unix", "1789727518")])
def test_a_numeric_string_keeps_the_place_it_has_always_had(
    queue: pool.PoolQueue, field: str, value: object
) -> None:
    """``int("5")`` and ``float("1789727518")`` parse, and always have.

    Left orderable on purpose: the guard turns a raise into an answer and
    changes no reading the sort already made.  Widening it to "the writer must
    have used the right JSON type" would file records the queue can place
    perfectly well, which is a harsher disposition than the defect earns.
    """

    _poison(queue, POISON, field, value)

    assert [str(item["action_key"]) for item in queue.ready_items()] == [POISON]
    assert queue.quarantine_orphans() == []


def test_the_order_of_orderable_records_is_unchanged(
    queue: pool.PoolQueue,
) -> None:
    """Regression: band first, then aging, then oldest -- with a poison present."""

    _publish(queue, GOOD, priority=-10)
    _publish(queue, OTHER, priority=5)
    _poison(queue, POISON, "published_unix", "not a time")

    assert [str(item["action_key"]) for item in queue.ready_items()] == [
        OTHER, GOOD]


# -- and the record is filed where an operator reads ------------------------


def test_the_poison_record_is_filed_rather_than_left_resident(
    queue: pool.PoolQueue,
) -> None:
    """Skipping alone would be a second silence: counted nowhere, gone never."""

    _publish(queue, GOOD)
    _poison(queue, POISON, "priority", "high")

    assert queue.quarantine_orphans() == [POISON]

    assert not queue.item_path(pool.READY, POISON).exists()
    assert queue.item_path(pool.READY, GOOD).exists()
    filed = json.loads(queue.item_path(pool.FAILED, POISON).read_text())
    assert filed["status"] == "orphaned_stub"
    assert filed["detail"]["unorderable_field"] == "priority"
    assert filed["detail"]["unorderable_value"] == repr("high")


def test_the_filed_record_names_a_foreign_published_unix_too(
    queue: pool.PoolQueue,
) -> None:
    _poison(queue, POISON, "published_unix", "not a time")

    assert queue.quarantine_orphans() == [POISON]

    filed = json.loads(queue.item_path(pool.FAILED, POISON).read_text())
    assert filed["detail"]["unorderable_field"] == "published_unix"


def test_an_executable_record_is_not_filed_by_this_guard(
    queue: pool.PoolQueue,
) -> None:
    """Regression: the sweep files what cannot run, and nothing else."""

    _publish(queue, GOOD, priority=-10)

    assert queue.quarantine_orphans() == []
    assert queue.item_path(pool.READY, GOOD).exists()
