"""A prelaunch group sharing pinned movers of earlier consumers (#1690).

The tier loop's window counts only the missing part of the demand.
The fresh-begin path asked the ledger for the whole demand.
When free tokens fell between the two numbers, the group never
began: the window reported 41 blocked while the begin asked for
121 and declined. One computation serves both now.

The fixture keeps the exact incident shape: two pinned leads hold
80 tokens under earlier consumers, two leads stand done/executed
with no tokens after the orphan sweep, free is 70, demand is 121.
The free 70 must admit the deficit 41, and the republished
chunks must carry the consumer to a composed resident map.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from prismabuild import pool  # noqa: E402
from prismabuild import prelaunch_group as pg  # noqa: E402
from prismabuild import reader_lease  # noqa: E402
from prismabuild import residency_map  # noqa: E402
from prismabuild import residency_plan  # noqa: E402
from prismabuild import storage_tiers  # noqa: E402
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

TOOLS = {"mover_python": "/gen-1690/venv/bin/python",
         "mover_tools_root": "/gen-1690/tools/fleet"}


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


def _declared_shared_plan(queue, consumer, specs, *, label, manifest):
    """One declared plan whose movers the shared registry names.

    The first consumer registers each range; the second takes the same
    mover rows from the registry, the way two PACT campaigns over one
    manifest seal the same content-addressed chunk keys.
    """
    total = sum(gib for _, gib, _, _ in specs) * storage_tiers.GIB
    built = []
    position = 0
    for ordinal, (name, gib, declared, _chunks) in enumerate(specs):
        start, end = position, position + gib * storage_tiers.GIB
        row = {
            "action_key": _hexkey(f"1690-{label}-mover{ordinal}"),
            "cas_root": str(queue.root / "cas"),
            "checkout_root": str(queue.root / "co"),
            "worker_script": str(queue.root / "worker.py"),
            "tags": ["dl380g10"],
            "resources": {"cpu": 1, "mem_gb": 1,
                          f"stage_gib@{TIER}": gib},
            "residency": {
                "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                "manifest_sha256": manifest, "manifest_bytes": total,
                "range_start_bytes": start, "range_end_bytes": end},
        }
        record, _sealed = residency_plan.register_shared_range(
            queue, manifest_sha256=manifest, tier_id=TIER, start=start,
            end=end, seal=lambda row=row: (row, {}),
            registered_by=consumer, sealed_against=dict(TOOLS))
        row = dict(record["mover_row"])
        phase = {"name": name, "start_bytes": start, "end_bytes": end,
                 "stage_gib": gib, "mover_row": row,
                 "egress_row": {
                     "action_key": _hexkey(f"1690-{label}-egress{ordinal}"),
                     "cas_root": str(queue.root / "cas"),
                     "checkout_root": str(queue.root / "co"),
                     "worker_script": str(queue.root / "worker.py"),
                     "tags": ["dl380g10"], "resources": {"mem_gb": 1}}}
        if declared:
            phase["resident_before_launch"] = True
        built.append(phase)
        position = end
    return residency_plan.build_plan(
        consumer_action_key=consumer, tier_id=TIER, stage_root="/stage/prewarm",
        manifest_sha256=manifest, manifest_bytes=total, phases=built)


def _land_range(queue, stage_root: Path, *, consumer: str, mover: str,
                manifest: str, name: str, start: int, end: int) -> None:
    """Tokens, sparse files, a fragment, dated material, a complete receipt."""
    size = end - start
    path = stage_root / manifest[:8] / name / "part-0.bin"
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as stream:
        stream.truncate(size)
    key = residency_map.residency_map_key(
        f"/pool/{manifest[:8]}/{name}/part-0.bin", 0)
    entries = {key: {"stage_path": str(path), "bytes": size, "offset": 0,
                     "sha256": "b" * 64}}
    assert queue.tier_ledger(TIER).acquire(
        mover, {"stage_gib": storage_tiers.stage_tokens_for_bytes(size)})
    residency_map.write_fragment(queue.residency_fragment_root(), {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": consumer, "mover_action_key": mover,
        "tier_id": TIER, "stage_root": str(stage_root),
        "manifest_sha256": manifest, "entries": entries})
    reader_lease.write_material(
        queue.residency_fragment_root(), consumer_action_key=consumer,
        mover_action_key=mover, tier_id=TIER, stage_root=str(stage_root),
        manifest_sha256=manifest, generation="c" * 32,
        entries={key: {**entries[key],
                       "file_id": reader_lease.stat_identity(str(path))}})
    queue.record_move(mover, {
        "consumer_action_key": consumer, "tier_id": TIER,
        "stage_root": str(stage_root), "manifest_sha256": manifest,
        "range_start_bytes": start, "range_end_bytes": end,
        "range_bytes": size, "bytes_staged": size,
        "entries_declared": 1, "entries_staged": 1,
        "complete": True, "seconds": 60.0, "unix": 2000.0})


def _live_shared(queue, plan, consumer, specs, manifest):
    """Freeze one shared-manifest plan and publish its consumer row."""
    from test_prelaunch_group_reconcile_1594 import GIB
    residency_plan.freeze(queue, plan)
    total = sum(gib for _, gib, _, _ in specs) * GIB
    queue.publish(
        action_key=consumer, cas_root=queue.root / "cas",
        checkout_root=queue.root / "co",
        worker_script=queue.root / "worker.py",
        tags=["dl380g10"], resources={"cpu": 1, "mem_gb": 1},
        residency={"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                   "manifest_sha256": manifest, "manifest_bytes": total,
                   "leads": residency_plan.leads_for(plan)})


def _done_record(queue, mover: str, manifest: str, gib: int, start: int,
                 end: int) -> None:
    """One done/executed record on the manifest, as a worker files it."""
    node = {"action_key": mover, "cas_root": str(queue.root / "cas"),
            "checkout_root": str(queue.root / "co"),
            "worker_script": str(queue.root / "worker.py"),
            "tags": ["dl380g10"], "resources": {"cpu": 1, "mem_gb": 1},
            "residency": {"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                          "manifest_sha256": manifest,
                          "manifest_bytes": 121 * storage_tiers.GIB,
                          "range_start_bytes": start,
                          "range_end_bytes": end}}
    queue.item_path(pool.DONE, mover).write_text(json.dumps({
        **node, "schema": pool.POOL_OUTCOME_SCHEMA_V1, "status": "executed",
        "attempts": 1, "finished_unix": 2100.0,
        "finished_host": "dl380g10", "detail": {}}))


def _state_1690(tmp_path):
    """Capacity 156, earlier movers hold 80, others hold 6, free 70.

    The exact incident shape: all four leads ran to done/executed on
    the same manifest. The orphan sweep later evicted two ranges and
    released their tokens; their done records stand, exactly as the
    PACT leads' did. The pinned pair keep 80 tokens. A filler holds
    6 more, so free is 70 and demand is 121.
    """
    queue = _queue(tmp_path, stage_gib=CAPACITY)
    tiers = {TIER: _stage(queue, TIER)}
    stage_root = Path(tiers[TIER]["mountpoint"])
    manifest = _hexkey("manifest-1690")
    first = _hexkey("first-consumer-1690")
    plan_one = _declared_shared_plan(queue, first, SPECS, label="first",
                                     manifest=manifest)
    _live_shared(queue, plan_one, first, SPECS, manifest)
    movers = residency_plan.leads_for(plan_one)
    gib_of = {movers[0]: 40, movers[1]: 40, movers[2]: 40, movers[3]: 1}
    # Every lead staged its range under its share namespace and ran
    # to done/executed on the same manifest.
    for mover in movers:
        leg = residency_plan.find_mover_leg(plan_one, mover)
        assert leg is not None
        start, end = int(leg["start_bytes"]), int(leg["end_bytes"])
        namespace = residency_plan.share_namespace(
            manifest, TIER, start, end)
        _land_range(queue, stage_root, consumer=namespace, mover=mover,
                    manifest=manifest, name=str(leg["phase"]),
                    start=start, end=end)
        _done_record(queue, mover, manifest, gib_of[mover], start, end)
    queue.item_path(pool.FAILED, first).write_text(json.dumps(
        {"schema": pool.POOL_OUTCOME_SCHEMA_V1, "action_key": first,
         "status": "failed", "attempts": 1}))
    for state in (pool.READY, pool.CLAIMED):
        try:
            queue.item_path(state, first).unlink()
        except OSError:
            pass
    for mover in movers[2:]:
        consumer = str(queue.move_record(mover)["consumer_action_key"])
        receipt = stage_release.evict(
            queue, mover, consumer_action_key=consumer,
            stage_root=str(stage_root),
            residency_root=queue.root / pool.RESIDENCY,
            reason="orphan-sweep")
        assert receipt["complete"] is True, receipt
        assert not queue.tier_ledger(TIER).holder_tokens(mover)
    # The incident's own ledger shape: the pinned pair hold 80, the
    # swept pair stand done with no tokens.
    assert _holder_tokens(queue, movers[0]) == 40
    assert _holder_tokens(queue, movers[1]) == 40
    for mover in movers:
        done = pool._read_json(queue.item_path(pool.DONE, mover))
        assert isinstance(done, dict) and done.get("status") == "executed"
    ledger = queue.tier_ledger(TIER)
    held = int(ledger.held().get("stage_gib", 0))
    assert held == 80, held
    assert ledger.acquire("filler-1690", {"stage_gib": 6}) is True
    assert ledger.available().get("stage_gib") == 70
    second = _hexkey("second-consumer-1690")
    plan_two = _declared_shared_plan(queue, second, SPECS, label="second",
                                     manifest=manifest)
    assert residency_plan.leads_for(plan_two) == movers
    _live_shared(queue, plan_two, second, SPECS, manifest)
    return queue, tiers, second, plan_two, movers


def _claim_chunk(queue, gib: int) -> str:
    """Claim one staged chunk through its fence with no second charge."""
    ledger = queue.tier_ledger(TIER)
    free_before = ledger.available().get("stage_gib")
    got = queue.claim(tags=["dl380g10"], owner="w-1690-chunk")
    assert got is not None, "a staged chunk must claim"
    mover = str(got["action_key"])
    assert ledger.available().get("stage_gib") == free_before
    record = queue.read_funding(mover, TIER)
    assert record is not None and record["state"] == "consumed"
    return mover


def _finish_claimed(queue, stage_root: Path, mover: str, consumer: str,
                    manifest: str) -> None:
    """Land the claimed chunk's bytes, then finish it executed pinned."""
    claimed = pool.read_queue_record(queue.item_path(pool.CLAIMED, mover))
    assert isinstance(claimed, dict)
    sealed = dict(claimed.get("residency"))
    start = int(sealed.get("range_start_bytes"))
    end = int(sealed.get("range_end_bytes"))
    size = end - start
    namespace = residency_plan.share_namespace(
        str(sealed.get("manifest_sha256")), TIER, start, end)
    key = residency_map.residency_map_key(
        f"/pool/{manifest[:8]}/recopy-{mover}/part-0.bin", 0)
    path = stage_root / manifest[:8] / f"recopy-{mover}" / "part-0.bin"
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as stream:
        stream.truncate(size)
    entries = {key: {"stage_path": str(path), "bytes": size, "offset": 0,
                     "sha256": "d" * 64}}
    residency_map.write_fragment(queue.residency_fragment_root(), {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": namespace, "mover_action_key": mover,
        "tier_id": TIER, "stage_root": str(stage_root),
        "manifest_sha256": manifest, "entries": entries})
    reader_lease.write_material(
        queue.residency_fragment_root(), consumer_action_key=namespace,
        mover_action_key=mover, tier_id=TIER, stage_root=str(stage_root),
        manifest_sha256=manifest, generation="e" * 32,
        entries={key: {**entries[key],
                       "file_id": reader_lease.stat_identity(str(path))}})
    queue.record_move(mover, {
        "consumer_action_key": namespace, "tier_id": TIER,
        "stage_root": str(stage_root), "manifest_sha256": manifest,
        "range_start_bytes": start, "range_end_bytes": end,
        "range_bytes": size, "bytes_staged": size,
        "entries_declared": 1, "entries_staged": 1,
        "complete": True, "seconds": 60.0, "unix": 3000.0})
    queue.finish(mover, status="executed")
    done = pool._read_json(queue.item_path(pool.DONE, mover))
    assert isinstance(done, dict) and done.get("status") == "executed"
    assert queue.tier_ledger(TIER).holder_tokens(mover)


def test_shared_pinned_movers_begin_with_the_deficit(tmp_path) -> None:
    """The group acquires only the true deficit and then stages."""
    queue, tiers, second, _plan, _movers = _state_1690(tmp_path)
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
    # No token is held twice: the group holder, the pinned pair and
    # the filler name disjoint sets under the same capacity.
    names: set[str] = set()
    for holder in [unit.holder, "filler-1690", *_movers_of(unit)]:
        held = set(pool.held_names_visible(ledger, holder))
        assert not (held & names), holder
        names |= held
    seen: list[dict] = []
    for _ in range(10):
        seen.extend(tier_loop.residency_window(queue, tiers=tiers))
    movers = _movers_of(unit)
    # The window republishes only the two unpinned chunks; the pinned pair
    # already stand done with their tokens and are never republished.
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
    # Execute both unpinned chunks through their fences; each claim pays
    # no second stage charge and consumes its fence on landing.
    stage_root = Path(tiers[TIER]["mountpoint"])
    plan = residency_plan.read(queue, second)
    assert plan is not None
    manifest = str(plan["manifest_sha256"])
    for _ in range(2):
        mover = _claim_chunk(queue, 0)
        assert mover in movers[2:], mover[-8:]
        _finish_claimed(queue, stage_root, mover, second, manifest)
    # The consumer is now admissible: the fan-out vouches every lead
    # under its own name, the map composes, and the claim gate reads
    # resident. The claim itself proves it, with no token taken twice.
    planned = tier_loop._planned_consumers(queue, tiers)
    tier_loop.fan_out_shared_ranges(queue, planned)
    for _ in range(4):
        seen.extend(tier_loop.residency_window(queue, tiers=tiers))
    row = pool.read_queue_record(queue.item_path(pool.READY, second))
    assert isinstance(row, dict)
    verdict = queue.residency_verdict(row)
    assert verdict["state"] == "resident", verdict
    held_before = dict(ledger.held())
    got = queue.claim(tags=["dl380g10"], owner="w-1690-consumer")
    assert got is not None and got["action_key"] == second, got
    assert got["residency_verdict"]["state"] == "resident"
    assert dict(ledger.held()) == held_before
    assert ledger.available().get("stage_gib", 0) >= 0
    capacity, _unreadable = ledger.capacity_census()
    assert sum(held_before.values()) <= capacity.get("stage_gib", 0)


def test_window_blocked_and_begin_need_come_from_one_computation(
        tmp_path) -> None:
    """The stall's blocked amount equals the begin's requested amount."""
    queue, tiers, second, plan, _movers = _state_1690(tmp_path)
    import prelaunch_tier as pt
    units = pt.declared_units(queue, {TIER: {"tier": "stage"}},
                              [_consumer(second)])
    unit = units[0]
    # One tier-loop cycle files both: the reserve pass begins the deficit,
    # then the window stalls on the same need. Both must name one amount.
    seen: list[dict] = []
    seen.extend(tier_loop.residency_window(queue, tiers=tiers))
    begun = [event for event in seen
             if event.get("event") == "prelaunch-group-begun"]
    assert begun, [event.get("event") for event in seen]
    need = int(begun[0].get("need_gib"))
    assert need == 41
    stalls = [event for event in seen
              if event.get("event") == "window-stalled"
              and event.get("consumer") == second]
    assert stalls, [event.get("event") for event in seen]
    # One computation: every stall filed this cycle equals the begin.
    for stall in stalls:
        assert stall.get("blocked_gib") == need, stall
        assert stall.get("need_gib") == need, stall


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
    assert "no room" in declines[0]["reason"], declines[0]["reason"]
    seen: list[dict] = []
    for _ in range(6):
        seen.extend(tier_loop.residency_window(queue, tiers=tiers))
    stalls = [event for event in seen
              if event.get("event") == "window-stalled"]
    assert stalls
    assert any(event.get("reason") == "prelaunch_waiting_for_room"
               for event in stalls)
    # The window reports what the begin needs, with the begin's real cause.
    assert any(event.get("blocked_gib") == 121 for event in stalls)
    assert any(event.get("need_gib") == 121 for event in stalls)
    assert any(event.get("decline_reason") == declines[0]["reason"]
               for event in stalls)
    journal = json.loads((pg.group_dir(queue, unit.unit, TIER) / "declined.json").read_text())
    assert journal["need_gib"] == 121
    assert journal["reason"] == declines[0]["reason"]


def test_a_busy_ledger_lock_reports_contention_not_a_shortage(
        tmp_path, monkeypatch) -> None:
    """A contended mint lock names the lock, never a capacity shortage."""
    import contextlib
    queue = _queue(tmp_path, stage_gib=CAPACITY)
    tiers = {TIER: _stage(queue, TIER)}
    second = _hexkey("locked-1690")
    plan = _declared_plan(queue, second, SPECS, tag="locked1690")
    from test_prelaunch_tier_publish_1594 import _live
    _live(queue, plan, second, SPECS)
    import prelaunch_tier as pt
    units = pt.declared_units(queue, {TIER: {"tier": "stage"}},
                              [_consumer(second)])
    unit = units[0]
    ledger = queue.tier_ledger(TIER)
    assert ledger.available().get("stage_gib") == CAPACITY
    # Another process holds the tier mint lock: the same-thread lock
    # would nest, so refuse the guard the way the held lock answers.
    real_ledger = queue.tier_ledger

    def busy_ledger(tier_id: str):
        found = real_ledger(tier_id)

        @contextlib.contextmanager
        def refused(*, blocking: bool = True):
            yield False

        found._mutation_guard = refused
        return found

    monkeypatch.setattr(queue, "tier_ledger", busy_ledger)
    events, _authority = pt.reserve_pass(queue, TIER, units,
                                         admitted=lambda found: True)
    declines = [event for event in events
                if event["event"] == "prelaunch-begin-declined"]
    assert len(declines) == 1
    assert declines[0]["need_gib"] == 121
    assert declines[0]["reason"] == (
        "ledger lock busy: another writer holds the tier mint lock")
    assert ledger.available().get("stage_gib") == CAPACITY
    seen: list[dict] = []
    seen.extend(tier_loop.residency_window(queue, tiers=tiers))
    stalls = [event for event in seen
              if event.get("event") == "window-stalled"
              and event.get("consumer") == second]
    assert stalls
    assert any(event.get("decline_reason") == declines[0]["reason"]
               for event in stalls), stalls


def _prepared_shared_recovery(tmp_path):
    """Lose an unstaged chunk's row and tokens after the shared group commits."""
    import prelaunch_tier as pt
    queue, tiers, consumer, plan, movers = _state_1690(tmp_path)
    ledger = queue.tier_ledger(TIER)
    unit = pt.declared_units(queue, tiers, [_consumer(consumer)])[0]
    for _ in range(10):
        tier_loop.residency_window(queue, tiers=tiers)
        assert ledger.held().get("stage_gib", 0) <= CAPACITY
    found = pg.census(queue, TIER, unit.unit, unit.holder,
                      unit.demand_gib, movers)
    assert (found.h, found.p, found.m, found.s) == (0, 0, 41, 80)
    assert found.receipts["committed.json"]["demand_gib"] == 121
    assert queue.item_path(pool.READY, movers[2]).exists()
    assert not queue.item_path(pool.CLAIMED, movers[2]).exists()
    shared = {mover: set(pool.held_names_visible(ledger, mover))
              for mover in movers[:2]}
    # The orphan sweep removed this range. Its replacement has not run,
    # so this fault loses tokens without leaving staged bytes uncovered.
    namespace = residency_plan.share_namespace(
        str(plan["manifest_sha256"]), TIER,
        80 * storage_tiers.GIB, 120 * storage_tiers.GIB)
    assert not residency_map.read_fragments(
        queue.residency_fragment_root(), namespace)
    assert ledger.release(movers[2]) == 40
    queue.item_path(pool.READY, movers[2]).unlink()
    found = pg.census(queue, TIER, unit.unit, unit.holder,
                      unit.demand_gib, movers)
    assert (found.h, found.p, found.m, found.s) == (0, 0, 1, 80)
    return queue, tiers, consumer, plan, movers, unit, shared


@pytest.mark.parametrize("shortage", [False, True])
def test_shared_recovery_preserves_request_amount_and_admits(
        tmp_path, shortage) -> None:
    """Recovery reports its actual request, then admits through shared pins."""
    queue, tiers, consumer, plan, movers, unit, shared = (
        _prepared_shared_recovery(tmp_path))
    ledger = queue.tier_ledger(TIER)
    intent_before = (unit.receipt_dir / "intent.json").read_bytes()
    if shortage:
        assert ledger.acquire("recovery-obstruction", {"stage_gib": 50})
        assert ledger.available()["stage_gib"] == 19
        events = tier_loop.residency_window(queue, tiers=tiers)
        decline = next(event for event in events
                       if event["event"] == "prelaunch-begin-declined"
                       and event["unit"] == unit.unit)
        stalls = [event for event in events
                  if event["event"] == "window-stalled"
                  and event["consumer"] == consumer]
        assert decline["need_gib"] == 40
        assert "asked 40" in decline["reason"]
        assert "free 19" in decline["reason"]
        assert stalls
        for stall in stalls:
            assert stall["blocked_gib"] == stall["need_gib"] == 40
            assert stall["decline_reason"] == decline["reason"]
        assert ledger.available()["stage_gib"] == 19
        assert ledger.release("recovery-obstruction") == 50

    free_before = ledger.available()["stage_gib"]
    events = tier_loop.residency_window(queue, tiers=tiers)
    recovered = next(event for event in events
                     if event["event"] == "prelaunch-group-topped-up"
                     and event["unit"] == unit.unit)
    stalls = [event for event in events
              if event["event"] == "window-stalled"
              and event["consumer"] == consumer]
    assert stalls
    amounts = [recovered.get("need_gib")]
    amounts.extend(stall[field] for stall in stalls
                   for field in ("blocked_gib", "need_gib"))
    assert amounts == [40] * len(amounts), (recovered, stalls)
    assert ledger.available()["stage_gib"] == free_before - 40
    assert all("decline_reason" not in stall for stall in stalls)

    for _ in range(6):
        tier_loop.residency_window(queue, tiers=tiers)
        assert ledger.held().get("stage_gib", 0) <= CAPACITY
        for mover, tokens in shared.items():
            assert set(pool.held_names_visible(ledger, mover)) == tokens
    names = set()
    for holder in [unit.holder, "filler-1690", *movers]:
        held = set(pool.held_names_visible(ledger, holder))
        assert not names.intersection(held)
        names.update(held)
    assert len(names) == 127
    for mover, gib in zip(movers[2:], (40, 1)):
        row = pool.read_queue_record(queue.item_path(pool.READY, mover))
        assert queue.funded_cover(TIER, row, "stage_gib", gib)[0] == gib
    stage_root = Path(tiers[TIER]["mountpoint"])
    for _ in range(2):
        mover = _claim_chunk(queue, 0)
        assert mover in movers[2:]
        _finish_claimed(queue, stage_root, mover, consumer,
                        str(plan["manifest_sha256"]))
    tier_loop.fan_out_shared_ranges(
        queue, tier_loop._planned_consumers(queue, tiers))
    for _ in range(4):
        tier_loop.residency_window(queue, tiers=tiers)
    held_before = dict(ledger.held())
    claimed = queue.claim(tags=["dl380g10"], owner="w-1690-recovered")
    assert claimed is not None and claimed["action_key"] == consumer
    assert claimed["residency_verdict"]["state"] == "resident"
    assert dict(ledger.held()) == held_before
    assert held_before["stage_gib"] == 127 <= CAPACITY
    for mover, gib in zip(movers, (40, 40, 40, 1)):
        assert _holder_tokens(queue, mover) == gib
        assert queue.move_record(mover)["bytes_staged"] == gib * storage_tiers.GIB
    assert (unit.receipt_dir / "intent.json").read_bytes() == intent_before
