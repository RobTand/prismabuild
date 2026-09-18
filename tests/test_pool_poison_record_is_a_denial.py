"""A record this pool never wrote is a denial for that item, not a dead loop (#592).

``publish`` is the only writer the claim loop owes anything to: it refuses a
malformed tier id on the way in, and it writes residency blocks it has
checked.  A hand-edit or a foreign publisher can still leave a ``ready/``
record carrying a tier id that ``split_demand`` accepts but
``_check_tier_id`` refuses, or naming a lead whose ``done/`` record no
longer reads -- corrupt bytes, or an ``ESTALE`` off a cold NFS handle.  Both
used to raise out of ``_claim`` into a handler whose only move is to
re-raise, so one bad record stopped every worker's loop on the same file
instead of being recorded as a denial for the one item that carries it.

Every test here puts the poison item in front of a good one --
``ready_items`` sorts priority first -- and makes a single claim poll.  The
bite is the good item coming back claimed by that same poll: the loop did
not merely survive the poison, it went on to the next record.
"""

from __future__ import annotations

from pathlib import Path
import errno
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from prismabuild import adaptive_cpu, pool  # noqa: E402

POISON = "a" * 64
GOOD = "b" * 64
MOVER = "1" * 64
CONSUMER = "2" * 64
TIER = "prismabuild-stage:dl380g10"
STAGE = f"stage_gib@{TIER}"


@pytest.fixture()
def queue(tmp_path: Path) -> pool.PoolQueue:
    q = pool.PoolQueue(tmp_path / "pb-queue")
    q.ensure_layout()
    return q


def _publish(q: pool.PoolQueue, key: str, resources: dict[str, int], **kw: object) -> None:
    q.publish(
        action_key=key,
        cas_root=q.root / "cas",
        checkout_root=q.root / "co",
        worker_script=q.root / "worker.py",
        resources=resources,
        **kw,
    )


def _denial(q: pool.PoolQueue, key: str) -> dict[str, object] | None:
    """This box's newest claim verdict for ``key``, from its host-local denial log."""

    path = adaptive_cpu.local_state_base(q.ledger().base) / pool.CLAIM_DENIALS
    records = adaptive_cpu.read_json(path).get("records", {})
    matching = [entry for entry in records.values()
                if isinstance(entry, dict) and entry.get("action_key") == key]
    if not matching:
        return None
    return max(matching, key=lambda entry: float(entry.get("denied_unix", 0.0)))


def _rewrite_resources(q: pool.PoolQueue, key: str, resources: object) -> None:
    """A foreign edit of a record this pool published: same bytes, new demand.

    ``published_unix`` rides along untouched -- the denial log is keyed by it,
    so an edit that dropped it would silence exactly the verdict this file
    exists to demand.
    """

    path = q.item_path(pool.READY, key)
    item = pool._read_json(path)
    assert item is not None
    item["resources"] = resources
    pool._write_json_atomic(path, item)


def _write_foreign_bytes(path: Path, payload: bytes) -> None:
    """A writer this pool never sanctioned leaves whatever bytes it likes."""

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)


def _publish_consumer(q: pool.PoolQueue, leads: list[str]) -> None:
    _publish(q, CONSUMER, {"cpu": 1}, priority=1, residency={
        "schema": pool.RESIDENCY_SCHEMA_V1,
        "manifest_sha256": "a" * 64, "manifest_bytes": 4096, "leads": leads,
    })


# -- a tier id ``publish`` would have refused -------------------------------


@pytest.mark.parametrize("bad_tier", [".bad", "bad/slash"])
def test_a_foreign_tier_id_is_a_denial_and_the_poll_claims_the_next_item(
    queue: pool.PoolQueue, bad_tier: str,
) -> None:
    # Published valid, then hand-edited: the poison tier id can only exist
    # in a record this pool never wrote.
    _publish(queue, POISON, {"cpu": 1, STAGE: 1}, priority=1)
    _rewrite_resources(queue, POISON, {"cpu": 1, f"stage_gib@{bad_tier}": 1})
    _publish(queue, GOOD, {"cpu": 1})

    claimed = queue.claim(owner="worker", capacity={"cpu": 4})
    # One poll, and it did both things: filed the poison as a denial and
    # carried straight on to the good record.
    assert claimed is not None and claimed["action_key"] == GOOD
    denial = _denial(queue, POISON)
    assert denial is not None and denial["reason"] == "malformed_tier_demand"
    assert bad_tier in str(denial["evidence"]["error"])
    assert f"stage_gib@{bad_tier}" in denial["evidence"]["demand"]
    # A denial is not a removal and not an ageing: the record stays for an
    # operator to look at, and this box spent no pass and no token on it.
    assert queue.item_path(pool.READY, POISON).exists()
    assert queue.passes(POISON) == 0
    assert queue.tier_holdings(POISON) == {}
    assert queue.ledger().held() == {"cpu": 1}


# -- a demand block ``publish`` would have refused ---------------------------


@pytest.mark.parametrize("bad_resources", ["garbage", [1], {"cpu": "many"}])
def test_a_foreign_resources_block_is_a_denial_and_the_poll_carries_on(
    queue: pool.PoolQueue, bad_resources: object,
) -> None:
    """``demand_of`` is the same class of raise, one step before the tier id.

    It sits ahead of the admission ``try`` too, and ``publish`` refuses every
    shape it rejects -- so a record that reaches it arrived from somewhere
    this pool does not write, and the poll owes it a denial rather than a
    raise nothing in the worker loop catches.
    """

    _publish(queue, POISON, {"cpu": 1}, priority=1)
    _rewrite_resources(queue, POISON, bad_resources)
    _publish(queue, GOOD, {"cpu": 1})

    claimed = queue.claim(owner="worker", capacity={"cpu": 4})
    assert claimed is not None and claimed["action_key"] == GOOD
    denial = _denial(queue, POISON)
    assert denial is not None and denial["reason"] == "malformed_demand"
    assert denial["evidence"]["error"]
    assert queue.item_path(pool.READY, POISON).exists()
    assert queue.passes(POISON) == 0
    assert queue.ledger().held() == {"cpu": 1}


# -- a lead record that cannot be read --------------------------------------


def test_a_corrupt_done_record_for_a_lead_is_a_denial_not_an_escape(
    queue: pool.PoolQueue,
) -> None:
    # A foreign writer truncated the mover's done record: the bytes parse to
    # nothing, and the consumer naming that lead must not take the loop down.
    _write_foreign_bytes(queue.item_path(pool.DONE, MOVER), b'{"status": "execut')
    _publish_consumer(queue, [MOVER])
    _publish(queue, GOOD, {"cpu": 1})

    claimed = queue.claim(owner="worker", capacity={"cpu": 4})
    assert claimed is not None and claimed["action_key"] == GOOD
    denial = _denial(queue, CONSUMER)
    assert denial is not None
    assert denial["reason"] == "residency_lead_record_unreadable"
    assert "not valid JSON" in str(denial["evidence"]["error"])
    assert denial["evidence"]["leads"] == [MOVER]
    assert queue.item_path(pool.READY, CONSUMER).exists()
    assert queue.passes(CONSUMER) == 0
    assert queue.ledger().held() == {"cpu": 1}


def test_an_estale_done_record_for_a_lead_is_a_denial_not_an_escape(
    queue: pool.PoolQueue, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The record is there and valid; NFS hands the read a stale handle.

    ``ESTALE`` is the on-the-wire form of "not there to read" that a by-key
    reader must stay loud about -- a poll that answered "absent" here would
    be a confident wrong verdict.  The containment is at the call site: the
    item gets a denial naming the error, and the loop moves on.
    """

    done_path = queue.item_path(pool.DONE, MOVER)
    pool._write_json_atomic(done_path, {"status": "executed"})
    real_read_json = pool._read_json

    def stale_for_the_done_path(
        path: Path, *, tolerate_stale: bool = False,
    ) -> dict[str, object] | None:
        if Path(path) == done_path:
            raise OSError(errno.ESTALE, "Stale file handle")
        return real_read_json(path, tolerate_stale=tolerate_stale)

    monkeypatch.setattr(pool, "_read_json", stale_for_the_done_path)
    _publish_consumer(queue, [MOVER])
    _publish(queue, GOOD, {"cpu": 1})

    claimed = queue.claim(owner="worker", capacity={"cpu": 4})
    assert claimed is not None and claimed["action_key"] == GOOD
    denial = _denial(queue, CONSUMER)
    assert denial is not None
    assert denial["reason"] == "residency_lead_record_unreadable"
    assert "Stale file handle" in str(denial["evidence"]["error"])
    assert denial["evidence"]["leads"] == [MOVER]
    assert queue.item_path(pool.READY, CONSUMER).exists()
    assert queue.passes(CONSUMER) == 0
    assert queue.ledger().held() == {"cpu": 1}
