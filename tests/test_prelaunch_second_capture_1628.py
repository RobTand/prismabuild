"""A second prelaunch capture of the same manifest publishes its chunks (#1628).

Movers are keyed by manifest range, so a second consumer that captures the same
manifest names the first consumer's mover keys.  After the first capture ends,
each mover keeps a ``consumed`` funding record bound to the first consumer and
plan.  On 2026-10-08 the second capture's group held 102 GiB, its two chunk
movers sat in ``ready`` with no tokens, and every cycle logged
``prelaunch-mover-occupied | refused``: nothing could move.

Everything runs on a tmp_path queue through ``tier_loop.residency_window``.
"""
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import pytest  # noqa: E402

from prismabuild import pool  # noqa: E402
from prismabuild import prelaunch_group as pg  # noqa: E402
import tier_loop  # noqa: E402
from test_prelaunch_group_reconcile_1594 import _hexkey, _queue  # noqa: E402
from test_prelaunch_tier_module_1594 import _declared_plan, TIER  # noqa: E402
from test_prelaunch_tier_publish_1594 import (  # noqa: E402
    _declared_movers, _live, _stage)

SPECS = [("phase-a", 4, True, 2), ("phase-b", 4, False, 1)]
SHARED = {("phase-a", 0): _hexkey("shared-chunk-0"),
          ("phase-a", 1): _hexkey("shared-chunk-1")}


def _publish_all(queue, tiers, movers, *, rounds=10):
    seen: list[dict] = []
    for _ in range(rounds):
        seen.extend(tier_loop.residency_window(queue, tiers=tiers))
        if all(queue.item_path(pool.READY, m).exists()
               or queue.item_path(pool.CLAIMED, m).exists() for m in movers):
            break
    return seen


def _publish_until_bound(queue, tiers, movers, *, rounds=14):
    """Cycle until every mover's record is ``transferring``, for recovery tests.

    ``_publish_all`` stops when the rows exist, which an interrupted
    publication reaches before its funding is bound.
    """
    seen: list[dict] = []
    for _ in range(rounds):
        seen.extend(tier_loop.residency_window(queue, tiers=tiers))
        records = [queue.read_funding(m, TIER) for m in movers]
        if all(r is not None and r["state"] == "transferring" for r in records):
            break
    return seen


def _end_first_capture(queue, consumer, unit, movers, final="consumed"):
    """What a finished capture leaves: spent records, no rows, no tokens."""
    for mover in movers:
        record = queue.read_funding(mover, TIER)
        assert record is not None and record["state"] == "transferring"
        assert queue.advance_funding_state(
            mover, TIER, expect="transferring", advance_to=final,
            generation=str(record["generation"]))
        queue.item_path(pool.READY, mover).unlink()
        queue.release_tier_reservations(mover)
    queue.item_path(pool.READY, consumer).unlink()
    queue.release_tier_reservations(unit.holder)


def _first_then_second(tmp_path, *, stage_gib=300, final="consumed",
                       end_first=True):
    queue = _queue(tmp_path, stage_gib=stage_gib)
    tiers = {TIER: _stage(queue, TIER)}
    first, second = _hexkey("capture-one"), _hexkey("capture-two")
    plan_one = _declared_plan(queue, first, SPECS, tag="one", shared=SHARED)
    _live(queue, plan_one, first, SPECS)
    unit_one, movers = _declared_movers(queue, first)
    _publish_all(queue, tiers, movers)
    if end_first:
        _end_first_capture(queue, first, unit_one, movers, final)
    plan_two = _declared_plan(queue, second, SPECS, tag="two", shared=SHARED)
    _live(queue, plan_two, second, SPECS)
    unit_two, movers_two = _declared_movers(queue, second)
    assert movers_two == movers, "the two captures must name the same movers"
    return queue, tiers, second, unit_two, movers


@pytest.mark.parametrize("final", ["consumed", "released"])
def test_a_second_capture_of_the_same_manifest_publishes_its_chunks(
        tmp_path, final) -> None:
    queue, tiers, second, unit, movers = _first_then_second(
        tmp_path, final=final)
    for mover in movers:
        record = queue.read_funding(mover, TIER)
        assert record["state"] == final and (
            record["consumer_action_key"] != second), "the fixture is stale"
    seen = _publish_all(queue, tiers, movers)
    refused = [event for event in seen
               if "prelaunch-mover-occupied" in str(event)]
    assert not refused, f"{len(refused)} chunk publications refused as occupied"
    assert all(queue.item_path(pool.READY, m).exists()
               or queue.item_path(pool.CLAIMED, m).exists() for m in movers)
    for mover in movers:
        record = queue.read_funding(mover, TIER)
        assert record["state"] == "transferring"
        assert record["consumer_action_key"] == second


def test_a_live_record_of_another_consumer_still_refuses(tmp_path) -> None:
    """Control: only a SPENT record rotates.  A live one stays occupied."""
    queue, tiers, second, unit, movers = _first_then_second(
        tmp_path, end_first=False)
    before = {m: queue.read_funding(m, TIER) for m in movers}
    assert all(r["state"] == "transferring" for r in before.values())
    seen: list[dict] = []
    for _ in range(6):
        seen.extend(tier_loop.residency_window(queue, tiers=tiers))
    assert not [e for e in seen if e.get("event") == "prelaunch-chunk-published"
                and e.get("unit") == unit.unit]
    for mover in movers:
        after = queue.read_funding(mover, TIER)
        assert after["generation"] == before[mover]["generation"]
        assert after["consumer_action_key"] == before[mover]["consumer_action_key"]
        assert after["state"] == "transferring"


def _holder_tokens(queue, name):
    return int(queue.tier_ledger(TIER).holder_tokens(name).get("stage_gib", 0))


def test_a_deferred_rotation_moves_nothing_and_the_retry_completes(
        tmp_path, monkeypatch) -> None:
    """Recovery: the new binding is written BEFORE any token moves.

    The first rotation is deferred (its lock was busy).  Nothing may have left
    the group's holder, so the next cycle finds nothing held and publishes.
    """
    queue, tiers, second, unit, movers = _first_then_second(tmp_path)
    real = pg._rotate_record
    calls = {"n": 0}
    deferred: list[str] = []

    def flaky(*args, **kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            deferred.append(str(args[1]))      # (queue, mover, tier, old, new)
            return None
        return real(*args, **kwargs)

    monkeypatch.setattr(pg, "_rotate_record", flaky)
    group_before = _holder_tokens(queue, unit.holder)
    for _ in range(10):                    # the group begins, then publishes
        tier_loop.residency_window(queue, tiers=tiers)
        if calls["n"] >= 1:
            break
    assert calls["n"] >= 1, "the rotation was never attempted"
    assert deferred and deferred[0] in movers
    assert _holder_tokens(queue, deferred[0]) == 0, (
        "a deferred rotation moved tokens under its mover")
    assert queue.read_funding(deferred[0], TIER)["state"] == "consumed", (
        "the old record is untouched until the new one is written")
    seen = _publish_until_bound(queue, tiers, movers)
    assert [e for e in seen if e.get("event") == "prelaunch-chunk-published"]
    for mover in movers:
        record = queue.read_funding(mover, TIER)
        assert record["state"] == "transferring"
        assert record["consumer_action_key"] == second
    assert group_before >= _holder_tokens(queue, unit.holder)


def test_a_partial_transfer_resumes_from_the_bound_record(
        tmp_path, monkeypatch) -> None:
    """Recovery: a transfer that moved some tokens and then failed retries.

    The binding already names the group's tokens, so the retry finds a matching
    ``reserved`` record with partial holdings and moves only what is missing:
    no mover holds more than its leg, and none is refused as occupied.
    """
    queue, tiers, second, unit, movers = _first_then_second(tmp_path)
    real = queue.transfer_tier_reservation_count
    state = {"failed": False}

    def partial(tier_id, holder, mover, count):
        if not state["failed"] and count >= 2:
            state["failed"] = True
            real(tier_id, holder, mover, 1)          # half of it lands
            raise OSError("interrupted mid-transfer")
        return real(tier_id, holder, mover, count)

    monkeypatch.setattr(queue, "transfer_tier_reservation_count", partial)
    seen = _publish_until_bound(queue, tiers, movers)
    assert state["failed"], "the fixture never interrupted a transfer"
    assert not [e for e in seen if "prelaunch-mover-occupied" in str(e)]
    for mover in movers:
        record = queue.read_funding(mover, TIER)
        assert record["state"] == "transferring"
        assert record["consumer_action_key"] == second
        assert _holder_tokens(queue, mover) == 2, "a leg holds exactly its tokens"
