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
