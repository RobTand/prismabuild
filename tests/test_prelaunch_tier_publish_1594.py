"""The prelaunch wiring inside the residency window (#1594).

Neighbour style: the queue, plan and leg helpers follow
test_prelaunch_tier_module_1594.py, and the live consumer rows follow
test_window_two_consumers_hold_and_wait.py. Each test drives
tier_loop.residency_window through real transitions. The integrator runs
them; nothing here runs anything itself.
"""
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from prismabuild import pool, residency_plan  # noqa: E402
from prismabuild import prelaunch_group as pg  # noqa: E402
import prelaunch_tier as pt  # noqa: E402
import stage_release  # noqa: E402
import tier_loop  # noqa: E402
from test_prelaunch_group_reconcile_1594 import (  # noqa: E402
    _hexkey, _queue, GIB, MANIFEST)
from test_prelaunch_tier_module_1594 import (  # noqa: E402
    _consumer, _declared_plan, OTHER_TIER, TIER, TIERS)

RAM_TIER = "ram:dl380g10"
EPOCH = "1695052800-1a2b3c4d5e6f7a8b"


def _stage(queue, tier):
    """A registered stage root with the tier record the window reads."""
    root = queue.root.parent / f"stage-{tier.replace(':', '-')}"
    root.mkdir(parents=True, exist_ok=True)
    assert stage_release.register_stage_root(
        queue, tier_id=tier, stage_root=str(root)) == "registered"
    return {"tier_id": tier, "tier": "stage", "mountpoint": str(root)}


def _live(queue, plan, consumer, specs, *, tier=TIER, gang=None):
    """Freeze one plan and publish its consumer row with leads."""
    residency_plan.freeze(queue, plan)
    total = sum(gib for _, gib, _, _ in specs) * GIB
    extra = {} if gang is None else {"gang": gang}
    queue.publish(
        action_key=consumer, cas_root=queue.root / "cas",
        checkout_root=queue.root / "co",
        worker_script=queue.root / "worker.py",
        tags=["dl380g10"], resources={"cpu": 1, "mem_gb": 1},
        residency={"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": tier,
                   "manifest_sha256": MANIFEST, "manifest_bytes": total,
                   "leads": residency_plan.leads_for(plan)},
        **extra)


def _declared_movers(queue, consumer):
    """The live declared unit's leg movers with its holder."""
    units = pt.declared_units(queue, TIERS, [_consumer(consumer)])
    assert len(units) == 1
    return units[0], [leg["mover_key"] for leg in units[0].legs]


def test_declared_window_reserves_then_publishes_and_binds(tmp_path) -> None:
    """A declared window holds its prefix, then publishes and funds it."""
    queue = _queue(tmp_path, stage_gib=300)
    tiers = {TIER: _stage(queue, TIER)}
    consumer = _hexkey("publish-consumer")
    specs = [("phase-a", 4, True, 2), ("phase-b", 4, False, 1)]
    plan = _declared_plan(queue, consumer, specs, tag="publish")
    _live(queue, plan, consumer, specs)
    unit, movers = _declared_movers(queue, consumer)
    seen: list[dict] = []
    for _ in range(10):
        seen.extend(tier_loop.residency_window(queue, tiers=tiers))
        if all(queue.item_path(pool.READY, mover).exists()
               or queue.item_path(pool.CLAIMED, mover).exists()
               for mover in movers):
            break
    assert all(queue.item_path(pool.READY, mover).exists()
               or queue.item_path(pool.CLAIMED, mover).exists()
               for mover in movers), [
        (event.get("event"), event.get("reason"), event.get("phase"))
        for event in seen]
    bound = [event for event in seen
             if event.get("event") == "prelaunch-chunk-published"]
    assert {event["mover"] for event in bound} == set(movers)
    for mover in movers:
        record = queue.read_funding(mover, TIER)
        assert record is not None and record["state"] == "transferring"


def test_declared_window_without_room_stalls_for_room(tmp_path) -> None:
    """A declared window with no room publishes nothing and waits."""
    queue = _queue(tmp_path, stage_gib=3)
    tiers = {TIER: _stage(queue, TIER)}
    consumer = _hexkey("starved-consumer")
    specs = [("phase-a", 4, True, 1), ("phase-b", 2, False, 1)]
    plan = _declared_plan(queue, consumer, specs, tag="starved")
    _live(queue, plan, consumer, specs)
    unit, movers = _declared_movers(queue, consumer)
    seen: list[dict] = []
    for _ in range(4):
        seen.extend(tier_loop.residency_window(queue, tiers=tiers))
    assert all(not queue.item_path(pool.READY, mover).exists()
               and not queue.item_path(pool.CLAIMED, mover).exists()
               for mover in movers)
    stalls = [event for event in seen
              if event.get("event") == "window-stalled"]
    assert any(event.get("reason") == "prelaunch_waiting_for_room"
               for event in stalls)
    assert queue.tier_ledger(TIER).holder_tokens(unit.holder).get(
        "stage_gib", 0) == 0


def test_undeclared_window_calls_window_without_held_flag(
        tmp_path, monkeypatch) -> None:
    """An undeclared window passes no held flag and files no group event."""
    queue = _queue(tmp_path, stage_gib=300)
    tiers = {TIER: _stage(queue, TIER)}
    consumer = _hexkey("plain-consumer")
    specs = [("phase-a", 2, False, 1), ("phase-b", 2, False, 1)]
    plan = _declared_plan(queue, consumer, specs, tag="plain")
    _live(queue, plan, consumer, specs)
    calls: list[dict] = []
    real_window = residency_plan.window

    def spy(plan, *args, **kwargs):
        calls.append(dict(kwargs))
        return real_window(plan, *args, **kwargs)

    monkeypatch.setattr(residency_plan, "window", spy)
    events = tier_loop.residency_window(queue, tiers=tiers)
    assert calls
    assert all("prelaunch_held" not in kwargs for kwargs in calls)
    assert all(not str(event.get("event", "")).startswith("prelaunch")
               for event in events)


def test_multi_tier_gang_gets_unsupported_and_no_holder(tmp_path) -> None:
    """A gang across tiers reserves nowhere and names its reason."""
    queue = _queue(tmp_path, stage_gib=300)
    queue.mint_tier_capacity(OTHER_TIER, {"stage_gib": 300})
    tiers = {TIER: _stage(queue, TIER), OTHER_TIER: _stage(queue, OTHER_TIER)}
    group = _hexkey("gang-group")[:32]
    first = _hexkey("gang-first")
    second = _hexkey("gang-second")
    specs = [("phase-a", 2, True, 1)]
    _live(queue, _declared_plan(queue, first, specs, tag="gang1"),
          first, specs, gang={"group": group, "size": 2, "index": 0})
    _live(queue, _declared_plan(queue, second, specs, tag="gang2",
                                tier=OTHER_TIER),
          second, specs, tier=OTHER_TIER,
          gang={"group": group, "size": 2, "index": 1})
    events = tier_loop.residency_window(queue, tiers=tiers)
    assert any(event.get("event") == "prelaunch-turn-unsupported"
               and event.get("reason") == pt.UNSUPPORTED_MULTI_TIER
               for event in events)
    for tier_id in (TIER, OTHER_TIER):
        held = queue.tier_ledger(tier_id).held_keys()
        assert not [key for key in held
                    if key.startswith(pg.HOLDER_PREFIX)]


def test_superseded_declared_unit_releases_its_group(tmp_path) -> None:
    """A superseded window frees its unit's unsplit remainder."""
    queue = _queue(tmp_path, stage_gib=300)
    tiers = {TIER: _stage(queue, TIER)}
    consumer = _hexkey("ended-consumer")
    specs = [("phase-a", 4, True, 2), ("phase-b", 4, False, 1)]
    plan = _declared_plan(queue, consumer, specs, tag="ended")
    _live(queue, plan, consumer, specs)
    unit, _movers = _declared_movers(queue, consumer)
    seen: list[dict] = []
    for _ in range(6):
        seen.extend(tier_loop.residency_window(queue, tiers=tiers))
    assert pg.has_holdings(queue, unit.unit), [
        (event.get("event"), event.get("reason")) for event in seen]
    filed, incarnation = residency_plan.read_filed(queue, consumer)
    assert filed is not None
    assert residency_plan.mark_superseded(
        queue, consumer, plan=filed, filing=incarnation,
        reason="test-ended", movers=[], by="test") is not None
    events = tier_loop.residency_window(queue, tiers=tiers)
    assert any(event.get("event") == "prelaunch-group-released"
               for event in events)
    assert queue.tier_ledger(TIER).holder_tokens(unit.holder).get(
        "stage_gib", 0) == 0


def test_ram_ghost_census_keeps_a_group_holder(tmp_path) -> None:
    """The incomplete census never frees a prelaunch group holder."""
    queue = _queue(tmp_path, stage_gib=300)
    ram = tmp_path / "ram"
    ram.mkdir(exist_ok=True)
    assert stage_release.register_stage_root(
        queue, tier_id=RAM_TIER, stage_root=str(ram)) == "registered"
    tiers = {RAM_TIER: {"tier": "ram", "tier_id": RAM_TIER,
                        "host": "dl380g10", "mountpoint": str(ram),
                        "epoch": EPOCH}}
    holder = pg.holder_name(_hexkey("ram-unit"), RAM_TIER, ["phase-a"])
    queue.mint_tier_capacity(RAM_TIER, {"ram_gib": 8})
    assert queue.tier_ledger(RAM_TIER).acquire(holder, {"ram_gib": 2})
    queue.record_move(holder, {
        "consumer_action_key": _hexkey("ram-consumer"), "tier_id": RAM_TIER,
        "ram_root": str(ram), "manifest_sha256": MANIFEST,
        "range_start_bytes": 0, "range_end_bytes": 2 * GIB,
        "bytes_staged": 0, "complete": False, "epoch": EPOCH,
        "seconds": 1.0, "unix": 1000.0, "errors": []})
    events = tier_loop.release_incomplete_ram_promotions(
        queue, tiers=tiers)
    assert all(event.get("holder") != holder and event.get("mover") != holder
               for event in events)
    assert queue.tier_ledger(RAM_TIER).holder_tokens(holder) == {"ram_gib": 2}
