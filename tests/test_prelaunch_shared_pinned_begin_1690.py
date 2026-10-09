"""A prelaunch group sharing pinned movers of earlier consumers (#1690).

The tier loop's window counts only the missing part of the demand.
The fresh-begin path asked the ledger for the whole demand.
When free tokens fell between the two numbers, the group never
began: the window reported 41 blocked while the begin asked for
121 and declined. One computation serves both now.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from prismabuild import pool  # noqa: E402
from prismabuild import prelaunch_group as pg  # noqa: E402
from prismabuild import residency_plan  # noqa: E402
import stage_release  # noqa: E402
import tier_loop  # noqa: E402
from test_prelaunch_group_reconcile_1594 import (  # noqa: E402
    _hexkey, _queue)
from test_prelaunch_tier_module_1594 import (  # noqa: E402
    _consumer, _declared_plan, TIER)

CAPACITY = 156
# Four chunks: 40 + 40 + 40 + 1 = 121.
SPECS = [("c0", 40, True, 1), ("c1", 40, True, 1),
         ("c2", 40, True, 1), ("c3", 1, True, 1)]


def _stage(queue, tier):
    root = queue.root.parent / f"stage-{tier.replace(':', '-')}"
    root.mkdir(parents=True, exist_ok=True)
    assert stage_release.register_stage_root(
        queue, tier_id=tier, stage_root=str(root)) == "registered"
    return {"tier_id": tier, "tier": "stage", "mountpoint": str(root)}


def _holder_tokens(queue, name) -> int:
    return int(queue.tier_ledger(TIER).holder_tokens(name).get("stage_gib", 0))


def _movers_of(unit):
    return [leg["mover_key"] for leg in unit.legs]


def _shared() -> dict:
    out = {}
    for name in ("c0", "c1", "c2", "c3"):
        out[name] = _hexkey(f"shared-1690-{name}-mover-key")
    return out


def _state_1690(tmp_path):
    """Capacity 156, earlier movers hold 80, others hold 6, free 70."""
    from test_prelaunch_tier_publish_1594 import _live
    queue = _queue(tmp_path, stage_gib=CAPACITY)
    tiers = {TIER: _stage(queue, TIER)}
    shared = _shared()
    first = _hexkey("first-consumer-1690")
    plan_one = _declared_plan(queue, first, SPECS, tag="first1690",
                              shared=shared)
    _live(queue, plan_one, first, SPECS)
    for _ in range(10):
        tier_loop.residency_window(queue, tiers=tiers)
    movers = [shared[name] for name, _, _, _ in SPECS]
    # The PACT shape: the earlier movers hold pinned tokens under live
    # transferring fences. Their consumers ended before the orphan sweep,
    # which freed the group holder remainder but kept the mover fences.
    import shutil
    shutil.rmtree(pg.group_dir(queue, first, TIER))
    for state in (pool.READY, pool.CLAIMED):
        try:
            queue.item_path(state, first).unlink()
        except OSError:
            pass
    for mover in movers[2:]:
        queue.release_tier_reservations(mover)
        try:
            queue.funding_path(mover, TIER).unlink()
        except OSError:
            pass
        for state in (pool.READY, pool.CLAIMED, pool.DONE,
                      pool.FAILED, pool.WITHDRAWN):
            try:
                queue.item_path(state, mover).unlink()
            except OSError:
                pass
    ledger = queue.tier_ledger(TIER)
    free_now = ledger.available().get("stage_gib")
    assert ledger.acquire("filler-1690", {"stage_gib": free_now - 70}) is True
    assert ledger.available().get("stage_gib") == 70
    second = _hexkey("second-consumer-1690")
    plan_two = _declared_plan(queue, second, SPECS, tag="second1690",
                              shared=shared)
    _live(queue, plan_two, second, SPECS)
    return queue, tiers, second, plan_two


def test_shared_pinned_movers_begin_with_the_deficit(tmp_path) -> None:
    """The group acquires only the true deficit and then stages."""
    queue, tiers, second, _plan = _state_1690(tmp_path)
    import prelaunch_tier as pt
    units = pt.declared_units(queue, {TIER: {"tier": "stage"}},
                              [_consumer(second)])
    assert len(units) == 1
    unit = units[0]
    assert unit.demand_gib == 121
    events, _authority = pt.reserve_pass(queue, TIER, units,
                                        admitted=lambda found: True)
    assert any(event["event"] == "prelaunch-group-begun" for event in events)
    found = pg.census(queue, TIER, unit.unit, unit.holder,
                      unit.demand_gib, _movers_of(unit))
    assert (found.h, found.p, found.m, found.s) == (0, 41, 0, 80)
    assert pg.incremental_need_gib(found, unit.demand_gib) == 0
    ledger = queue.tier_ledger(TIER)
    assert ledger.available().get("stage_gib") == 70 - 41
    events, _authority = pt.reserve_pass(queue, TIER, units,
                                        admitted=lambda found: True)
    assert any(event["event"] == "prelaunch-acquisition-committed"
               for event in events)
    events, authority = pt.reserve_pass(queue, TIER, units,
                                        admitted=lambda found: True)
    assert any(event["event"] == "prelaunch-group-committed" for event in events)
    assert authority == {second: True}
    total = _holder_tokens(queue, unit.holder) + sum(
        _holder_tokens(queue, mover) for mover in _movers_of(unit))
    assert total == 121, total
    capacity, _unreadable = ledger.capacity_census()
    assert capacity.get("stage_gib", 0) == CAPACITY
    seen: list[dict] = []
    for _ in range(10):
        seen.extend(tier_loop.residency_window(queue, tiers=tiers))
    movers = _movers_of(unit)
    assert all(queue.item_path(pool.READY, mover).exists()
               or queue.item_path(pool.CLAIMED, mover).exists()
               for mover in movers[2:])
    # The republished chunks fund their claims from the group fence:
    # each covers its whole demand with no second stage charge.
    for mover in movers[2:]:
        record = queue.read_funding(mover, TIER)
        assert record is not None and record["state"] == "transferring"
        assert record["consumer_action_key"] == second
        row = pool.read_queue_record(queue.item_path(pool.READY, mover))
        assert isinstance(row, dict)
        covered, _generation = queue.funded_cover(TIER, row, "stage_gib", 40 if mover != movers[3] else 1)
        assert covered == (40 if mover != movers[3] else 1), (mover[-8:], covered)


def test_window_blocked_and_begin_need_come_from_one_computation(
        tmp_path) -> None:
    """The stall's blocked amount equals the begin's requested amount."""
    queue, tiers, second, plan = _state_1690(tmp_path)
    import prelaunch_tier as pt
    units = pt.declared_units(queue, {TIER: {"tier": "stage"}},
                              [_consumer(second)])
    unit = units[0]
    events, _authority = pt.reserve_pass(queue, TIER, units,
                                        admitted=lambda found: True)
    assert any(event["event"] == "prelaunch-group-begun" for event in events)
    begun = [event for event in events
             if event["event"] == "prelaunch-group-begun"][0]
    found = pg.census(queue, TIER, unit.unit, unit.holder,
                      unit.demand_gib, _movers_of(unit))
    assert (found.h, found.p, found.m, found.s) == (0, 41, 0, 80)
    already, _staged = tier_loop._mover_state(queue, plan, TIER)
    decision = residency_plan.window(
        plan, accepted_phase=None, free_gib=70, capacity_gib=CAPACITY,
        published=sorted(already), staged=[],
        withdrawn=[], prelaunch_held=False)
    assert decision["stall"] is not None
    assert decision["stall"]["blocked_gib"] == 41
    assert begun.get("need_gib") == 41


def test_a_declined_begin_reports_its_need_and_reason(tmp_path) -> None:
    """No room: the begin names its amount and the ledger's own reason."""
    queue = _queue(tmp_path, stage_gib=CAPACITY)
    tiers = {TIER: _stage(queue, TIER)}
    queue.tier_ledger(TIER).acquire("squatter-1690", {"stage_gib": 150})
    second = _hexkey("starved-1690")
    plan = _declared_plan(queue, second, SPECS, tag="starved1690")
    from test_prelaunch_tier_publish_1594 import _live
    _live(queue, plan, second, SPECS)
    import prelaunch_tier as pt
    units = pt.declared_units(queue, {TIER: {"tier": "stage"}},
                              [_consumer(second)])
    unit = units[0]
    events, _authority = pt.reserve_pass(queue, TIER, units,
                                        admitted=lambda found: True)
    declines = [event for event in events
                if event["event"] == "prelaunch-begin-declined"]
    assert len(declines) == 1
    assert declines[0]["need_gib"] == 121
    assert isinstance(declines[0].get("reason"), str)
    assert declines[0]["reason"]
    seen: list[dict] = []
    for _ in range(6):
        seen.extend(tier_loop.residency_window(queue, tiers=tiers))
    stalls = [event for event in seen
              if event.get("event") == "window-stalled"]
    assert stalls
    assert any(event.get("reason") == "prelaunch_waiting_for_room"
               for event in stalls)
    assert any(event.get("need_gib") == 121 for event in stalls)
