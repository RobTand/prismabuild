"""The prelaunch tier pass over live declared units (#1594).

Neighbour style: the queue, tier and plan fixtures follow
test_prelaunch_group_reconcile_1594.py, whose base helpers this file
imports. Plans here declare a resident prefix; every number asserted
comes from the plan the test builds. The integrator runs them;
nothing here runs anything itself.
"""
from pathlib import Path
import sys
from typing import Mapping

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from prismabuild import pool, residency_plan  # noqa: E402
from prismabuild import prelaunch_group as pg  # noqa: E402
import prelaunch_tier as pt  # noqa: E402
from test_prelaunch_group_reconcile_1594 import (  # noqa: E402
    GIB, MANIFEST, STAGE_KIND, TIER, _chunks, _drive_to_committed, _hexkey,
    _plan, _queue, _row)

OTHER_TIER = "prismabuild-stage:dl380g11"
TIERS = {TIER: {"tier": "stage"}, OTHER_TIER: {"tier": "stage"}}


def _mover_row(queue, key, start, end, gib, tier, manifest, total):
    """A stage mover row pinning one range of one manifest."""
    return {
        **_row(key, {"cpu": 1, "mem_gb": 1, f"stage_gib@{tier}": gib}, queue),
        "residency": {
            "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": tier,
            "manifest_sha256": manifest, "manifest_bytes": total,
            "range_start_bytes": start, "range_end_bytes": end},
    }


def _declared_plan(queue, consumer, specs, *, tier=TIER, manifest=MANIFEST,
                   tag="", shared=None):
    """A plan from (name, gib, declared, chunks) specs, ranges tiling.

    ``shared`` maps a phase name or a (name, chunk) pair to a mover key,
    so two members can name one shared mover.
    """
    shared = shared or {}
    total = sum(gib for _, gib, _, _ in specs) * GIB
    short = _hexkey(consumer)[:8]
    phases = []
    position = 0
    for name, gib, declared, nchunks in specs:
        span = gib * GIB
        start, end = position, position + span
        phase = {"name": name, "start_bytes": start, "end_bytes": end,
                 "stage_gib": gib}
        if declared:
            phase["resident_before_launch"] = True
        if nchunks == 1:
            key = shared.get(name, _hexkey(f"{tag}-{short}-{name}-m"))
            phase["mover_row"] = _mover_row(queue, key, start, end, gib,
                                            tier, manifest, total)
            phase["egress_row"] = _row(
                _hexkey(f"{tag}-{short}-{name}-e"), {"mem_gb": 1}, queue)
        else:
            chunks = []
            for index in range(nchunks):
                cstart = start + index * (span // nchunks)
                cend = cstart + (span // nchunks)
                key = shared.get((name, index),
                                 _hexkey(f"{tag}-{short}-{name}-c{index}"))
                chunks.append({
                    "chunk_index": index, "start_bytes": cstart,
                    "end_bytes": cend, "stage_gib": gib // nchunks,
                    "mover_row": _mover_row(queue, key, cstart, cend,
                                            gib // nchunks, tier, manifest,
                                            total),
                    "egress_row": _row(
                        _hexkey(f"{tag}-{short}-{name}-e{index}"),
                        {"mem_gb": 1}, queue)})
            phase["stage_chunks"] = chunks
        phases.append(phase)
        position = end
    return residency_plan.build_plan(
        consumer_action_key=consumer, tier_id=tier, stage_root="/stage/prewarm",
        manifest_sha256=manifest, manifest_bytes=total, phases=phases)


def _consumer(key, *, state="ready", priority=0, published=0.0, group=None):
    """One live-consumers entry naming a filed plan."""
    item: dict = {"action_key": key, "priority": priority,
                  "published_unix": published}
    if group is not None:
        item["gang"] = {"group": group, "size": 2, "index": 0}
    return {"action_key": key, "state": state, "accepted_phase": None,
            "item": item}


def _mover_of(plan, phase):
    """The whole-phase mover key of one plan phase."""
    phases = plan["phases"]
    assert isinstance(phases, list)
    for entry in phases:
        if isinstance(entry, Mapping) and entry.get("name") == phase:
            row = entry["mover_row"]
            assert isinstance(row, Mapping)
            return str(row["action_key"])
    raise AssertionError(f"phase {phase} names no whole mover")


def _publish_leg(queue, plan, mover):
    """Freeze once, then publish one mover row, chunked or whole."""
    residency_plan.freeze(queue, plan)
    phases = plan["phases"]
    assert isinstance(phases, list)
    mover_row = None
    for phase in phases:
        assert isinstance(phase, Mapping)
        chunks = phase.get("stage_chunks")
        if isinstance(chunks, list):
            for chunk in chunks:
                assert isinstance(chunk, Mapping)
                row = chunk["mover_row"]
                assert isinstance(row, Mapping)
                if str(row["action_key"]) == mover:
                    mover_row = dict(row)
        else:
            row = phase["mover_row"]
            assert isinstance(row, Mapping)
            if str(row["action_key"]) == mover:
                mover_row = dict(row)
    assert mover_row is not None
    queue.publish(
        action_key=mover, cas_root=mover_row["cas_root"],
        checkout_root=mover_row["checkout_root"],
        worker_script=mover_row["worker_script"],
        tags=["dl380g10"], resources=mover_row["resources"],
        residency=mover_row["residency"])
    row = pool.read_queue_record(queue.item_path(pool.READY, mover))
    assert isinstance(row, dict)
    return row


def _file_group(queue, plan, movers, *, demand):
    """File the intent under the unit's own declared-phase holder."""
    unit = str(plan["consumer_action_key"])
    holder = pg.holder_name(
        unit, TIER, residency_plan.prelaunch_phase_names(plan))
    assert pg.file_intent(queue, unit, holder, TIER, demand,
                          _chunks(plan, movers)) is True
    return unit, holder


# ---------------------------------------------------------- declared_units


def test_declared_whole_phase_becomes_one_unit(tmp_path) -> None:
    """A declared prefix yields one unit with its bound and its rows."""
    queue = _queue(tmp_path, stage_gib=300)
    consumer = _hexkey("whole-consumer")
    plan = _declared_plan(queue, consumer, [("phase-a", 6, True, 1),
                                            ("phase-b", 2, False, 1)],
                          tag="whole")
    residency_plan.freeze(queue, plan)
    units = pt.declared_units(queue, TIERS, [_consumer(consumer)])
    assert len(units) == 1
    found = units[0]
    assert found.key == consumer and found.keys == [consumer]
    assert found.unit == consumer and found.tier_id == TIER
    assert found.holder == pg.holder_name(consumer, TIER, ["phase-a"])
    assert found.phase_names == ["phase-a"]
    assert found.demand_gib == 6 and found.peak_gib == 8
    assert found.unsupported is None
    assert found.consumer_state == {consumer: "ready"}
    assert len(found.legs) == 1
    leg = found.legs[0]
    assert (leg["mover_key"], leg["stage_gib"], leg["start_bytes"],
            leg["end_bytes"]) == (_mover_of(plan, "phase-a"), 6, 0, 6 * GIB)
    assert isinstance(leg["mover_row"], Mapping)
    assert isinstance(leg["egress_row"], Mapping)


def test_declared_chunked_prefix_lists_legs_in_read_order(tmp_path) -> None:
    """A chunked declared phase contributes one leg per chunk."""
    queue = _queue(tmp_path, stage_gib=300)
    consumer = _hexkey("chunk-consumer")
    plan = _declared_plan(queue, consumer, [("phase-a", 4, True, 2),
                                            ("phase-b", 4, False, 1)],
                          tag="chunk")
    residency_plan.freeze(queue, plan)
    units = pt.declared_units(queue, TIERS, [_consumer(consumer)])
    assert len(units) == 1
    found = units[0]
    assert [leg["stage_gib"] for leg in found.legs] == [2, 2]
    assert [leg["start_bytes"] for leg in found.legs] == [0, 2 * GIB]
    for leg in found.legs:
        row = leg["mover_row"]
        assert isinstance(row, Mapping)
        assert str(row["action_key"]) == leg["mover_key"]
        assert isinstance(leg["egress_row"], Mapping)
    assert found.demand_gib == 4 and found.peak_gib == 8


def test_gang_group_merges_into_one_unit_with_deduplicated_legs(
        tmp_path) -> None:
    """Members sharing a mover count its range once in one unit."""
    queue = _queue(tmp_path, stage_gib=300)
    group = "gang-group-merge"
    shared_mover = _hexkey("gang-shared-mover")
    first = _hexkey("gang-first")
    second = _hexkey("gang-second")
    plan_a = _declared_plan(
        queue, first, [("phase-a", 4, True, 1), ("phase-b", 4, True, 1),
                       ("phase-c", 2, False, 1)],
        tag="ganga", shared={"phase-a": shared_mover})
    plan_b = _declared_plan(
        queue, second, [("phase-a", 4, True, 1), ("phase-b", 4, True, 1),
                        ("phase-c", 2, False, 1)],
        tag="gangb", shared={"phase-a": shared_mover})
    residency_plan.freeze(queue, plan_a)
    residency_plan.freeze(queue, plan_b)
    units = pt.declared_units(queue, TIERS, [_consumer(first, group=group),
                                             _consumer(second, group=group)])
    assert len(units) == 1
    found = units[0]
    assert found.unit == group and found.keys == [first, second]
    assert found.key == first
    assert sorted(leg["mover_key"] for leg in found.legs) == sorted(
        [shared_mover, _mover_of(plan_a, "phase-b"),
         _mover_of(plan_b, "phase-b")])
    assert found.demand_gib == 12 and found.peak_gib == 14


def test_multi_tier_gang_is_unsupported(tmp_path) -> None:
    """Members on two tiers keep one tier and reserve on neither."""
    queue = _queue(tmp_path, stage_gib=300)
    group = "gang-group-split"
    first = _hexkey("split-first")
    second = _hexkey("split-second")
    plan_a = _declared_plan(queue, first, [("phase-a", 2, True, 1)],
                            tag="splita")
    plan_b = _declared_plan(queue, second, [("phase-a", 2, True, 1)],
                            tag="splitb", tier=OTHER_TIER)
    residency_plan.freeze(queue, plan_a)
    residency_plan.freeze(queue, plan_b)
    units = pt.declared_units(queue, TIERS, [_consumer(first, group=group),
                                             _consumer(second, group=group)])
    assert len(units) == 1
    found = units[0]
    assert found.unit == group and found.unsupported == "multi-tier"
    assert found.tier_id == TIER
    assert len(found.legs) == 2


def test_undeclared_consumer_stays_absent(tmp_path) -> None:
    """No declaration means no unit and no obligation."""
    queue = _queue(tmp_path, stage_gib=300)
    bare = _hexkey("bare-consumer")
    plan = _plan(queue, bare, _hexkey("bare-m1"), _hexkey("bare-m2"))
    residency_plan.freeze(queue, plan)
    units = pt.declared_units(queue, TIERS, [_consumer(bare)])
    assert units == []
    assert pt.obligations([], {}, STAGE_KIND) == ({}, {})


def test_units_sort_by_priority_then_publish_time(tmp_path) -> None:
    """Best rank first: higher priority, then earlier publish, then id."""
    queue = _queue(tmp_path, stage_gib=300)
    low = _hexkey("sort-low")
    early = _hexkey("sort-early")
    high = _hexkey("sort-high")
    for key, tag in ((low, "sortlow"), (early, "sortearly"),
                     (high, "sorthigh")):
        residency_plan.freeze(
            queue, _declared_plan(queue, key, [("phase-a", 1, True, 1)],
                                  tag=tag))
    units = pt.declared_units(queue, TIERS, [
        _consumer(low, priority=5, published=10.0),
        _consumer(early, priority=5, published=3.0),
        _consumer(high, priority=9, published=20.0)])
    assert [found.key for found in units] == [high, early, low]


# ------------------------------------------------------------- reserve_pass


def test_reserve_pass_reserves_a_whole_group_in_one_pass(tmp_path) -> None:
    """An admitted unit files once and owns its whole demand at once."""
    queue = _queue(tmp_path, stage_gib=300)
    ledger = queue.tier_ledger(TIER)
    consumer = _hexkey("reserve-consumer")
    plan = _declared_plan(queue, consumer, [("phase-a", 4, True, 2),
                                            ("phase-b", 4, False, 1)],
                          tag="reserve")
    residency_plan.freeze(queue, plan)
    units = pt.declared_units(queue, TIERS, [_consumer(consumer)])
    events, authority = pt.reserve_pass(queue, TIER, units,
                                        admitted=lambda unit: True)
    assert authority == {consumer: False}
    assert any(event["event"] == "prelaunch-group-begun" for event in events)
    found = pg.census(queue, TIER, consumer, units[0].holder, 4,
                      [leg["mover_key"] for leg in units[0].legs])
    assert (found.h, found.p) == (0, 4)
    events, authority = pt.reserve_pass(queue, TIER, units,
                                        admitted=lambda unit: True)
    assert any(event["event"] == "prelaunch-acquisition-committed"
               for event in events)
    events, authority = pt.reserve_pass(queue, TIER, units,
                                        admitted=lambda unit: True)
    assert authority == {consumer: True}
    assert any(event["event"] == "prelaunch-group-committed"
               for event in events)
    assert ledger.holder_tokens(units[0].holder).get("stage_gib", 0) == 4


def test_reserve_pass_holds_back_a_non_admitted_unit(tmp_path) -> None:
    """A refused unit waits: intent stands, the ledger never moves."""
    queue = _queue(tmp_path, stage_gib=300)
    ledger = queue.tier_ledger(TIER)
    consumer = _hexkey("wait-consumer")
    plan = _declared_plan(queue, consumer, [("phase-a", 4, True, 2),
                                            ("phase-b", 4, False, 1)],
                          tag="wait")
    residency_plan.freeze(queue, plan)
    units = pt.declared_units(queue, TIERS, [_consumer(consumer)])
    events, authority = pt.reserve_pass(queue, TIER, units,
                                        admitted=lambda unit: False)
    assert [event["event"] for event in events] == ["prelaunch-waiting"]
    assert authority == {consumer: False}
    assert ledger.available().get("stage_gib") == 300
    assert ledger.holder_tokens(units[0].holder).get("stage_gib", 0) == 0
    assert (pg.group_dir(queue, consumer, TIER) / "intent.json").exists()


# ---------------------------------------------------------- publish_declared


def test_publish_declared_binds_only_rowed_legs(tmp_path) -> None:
    """Funding follows the rows the window published; the rest waits."""
    queue = _queue(tmp_path, stage_gib=300)
    ledger = queue.tier_ledger(TIER)
    consumer = _hexkey("bind-consumer")
    plan = _declared_plan(queue, consumer, [("phase-a", 4, True, 2),
                                            ("phase-b", 4, False, 1)],
                          tag="bind")
    residency_plan.freeze(queue, plan)
    units = pt.declared_units(queue, TIERS, [_consumer(consumer)])
    for _ in range(3):
        pt.reserve_pass(queue, TIER, units, admitted=lambda unit: True)
    movers = [leg["mover_key"] for leg in units[0].legs]
    row = _publish_leg(queue, plan, movers[0])
    rows = [{"mover_action_key": movers[0],
             "published_unix": float(row["published_unix"])},
            {"mover_action_key": movers[1]}]
    events = pt.publish_declared(queue, TIER, units[0], rows)
    assert [event["event"] for event in events] == ["prelaunch-chunk-published"]
    assert events[0]["mover"] == movers[0]
    record = queue.read_funding(movers[0], TIER)
    assert record is not None and record["state"] == "transferring"
    assert ledger.holder_tokens(units[0].holder).get("stage_gib", 0) == 2
    assert ledger.holder_tokens(movers[1]).get("stage_gib", 0) == 0


# -------------------------------------------------------------- obligations


def test_obligations_counts_peak_minus_owned(tmp_path) -> None:
    """T=90 held with B=170 obliges 78 after the unit owns 92."""
    queue = _queue(tmp_path, stage_gib=300)
    consumer = _hexkey("oblig-consumer")
    other = _hexkey("oblig-other")
    plan = _declared_plan(queue, consumer, [("phase-a", 90, True, 1),
                                            ("phase-b", 40, False, 1),
                                            ("phase-c", 40, False, 1)],
                          tag="oblig")
    residency_plan.freeze(queue, plan)
    units = pt.declared_units(queue, TIERS, [_consumer(consumer),
                                             _consumer(other)])
    assert units[0].demand_gib == 90 and units[0].peak_gib == 170
    held = {units[0].holder: {STAGE_KIND: 90},
            _mover_of(plan, "phase-b"): {STAGE_KIND: 2}}
    totals, detail = pt.obligations(units, held, STAGE_KIND)
    assert totals == {TIER: 78}
    assert detail[consumer]["owned_gib"] == 92
    assert detail[consumer]["obligation_gib"] == 78
    assert other not in detail


# -------------------------------------------------------------- is_admitted


def test_is_admitted_three_cases(tmp_path) -> None:
    """Tokens, a published leg, or a committed receipt each admits."""
    queue = _queue(tmp_path, stage_gib=300)
    consumer = _hexkey("admit-consumer")
    fresh = _hexkey("admit-fresh")
    plan = _declared_plan(queue, consumer, [("phase-a", 2, True, 1),
                                            ("phase-b", 2, False, 1)],
                          tag="admit")
    residency_plan.freeze(queue, plan)
    residency_plan.freeze(
        queue, _declared_plan(queue, fresh, [("phase-a", 2, True, 1)],
                              tag="admitfresh"))
    units = pt.declared_units(queue, TIERS, [_consumer(consumer),
                                             _consumer(fresh)])
    by_key = {found.key: found for found in units}
    unit = by_key[consumer]
    mover = unit.legs[0]["mover_key"]
    assert pt.is_admitted(unit, {unit.holder: {STAGE_KIND: 2}}, STAGE_KIND,
                          []) is True
    assert pt.is_admitted(unit, {}, STAGE_KIND, [mover]) is True
    assert pt.is_admitted(by_key[fresh], {}, STAGE_KIND, []) is False
    _file_group(queue, plan, [mover], demand=2)
    _drive_to_committed(queue, TIER, consumer, unit.holder, 2, [mover])
    assert pg.release_unit(queue, TIER, consumer, unit.holder, [mover],
                           terminal=True) == ["prelaunch-group-released"]
    assert pt.is_admitted(unit, {}, STAGE_KIND, []) is True


# --------------------------------------------------------- declared_leg_keys


def test_declared_leg_keys_names_live_prefixes(tmp_path) -> None:
    """Retention sees every live declared mover and nothing else."""
    queue = _queue(tmp_path, stage_gib=300)
    consumer = _hexkey("legs-consumer")
    plan = _declared_plan(queue, consumer, [("phase-a", 4, True, 2),
                                            ("phase-b", 4, False, 1)],
                          tag="legs")
    residency_plan.freeze(queue, plan)
    units = pt.declared_units(queue, TIERS, [_consumer(consumer)])
    keys = pt.declared_leg_keys(units)
    assert keys == frozenset(leg["mover_key"] for leg in units[0].legs)
    assert pt.is_prelaunch_leg(keys, units[0].legs[0]["mover_key"]) is True
    assert pt.is_prelaunch_leg(keys, _hexkey("legs-stranger")) is False
    assert pt.declared_leg_keys([]) == frozenset()


# ----------------------------------------------------------------- dangling


def _orphan(queue, tag):
    """A committed group whose consumer never goes live."""
    consumer = _hexkey(f"{tag}-orphan")
    mover = _hexkey(f"{tag}-orphan-mover")
    plan = _declared_plan(queue, consumer, [("phase-a", 2, True, 1)],
                          tag=tag)
    residency_plan.freeze(queue, plan)
    unit, holder = _file_group(queue, plan, [mover], demand=2)
    _drive_to_committed(queue, TIER, unit, holder, 2, [mover])
    return consumer, holder


def test_dangling_releases_an_orphan_holder(tmp_path) -> None:
    """A holder no live unit names returns its tokens."""
    queue = _queue(tmp_path, stage_gib=8)
    ledger = queue.tier_ledger(TIER)
    _, holder = _orphan(queue, "orphan")
    events = pt.dangling(queue, TIER, ledger, [], complete_census=True)
    assert [event["event"] for event in events] == [
        "prelaunch-dangling-released"]
    assert events[0]["holder"] == holder
    assert ledger.holder_tokens(holder).get("stage_gib", 0) == 0
    assert ledger.available().get("stage_gib") == 8


def test_dangling_retains_on_incomplete_census(tmp_path) -> None:
    """Half a census releases nothing and files nothing."""
    queue = _queue(tmp_path, stage_gib=8)
    ledger = queue.tier_ledger(TIER)
    _, holder = _orphan(queue, "half")
    assert pt.dangling(queue, TIER, ledger, [], complete_census=False) == []
    assert ledger.holder_tokens(holder).get("stage_gib", 0) == 2


def test_dangling_never_touches_a_live_unit(tmp_path) -> None:
    """A live unit's holder stays out of the orphan census."""
    queue = _queue(tmp_path, stage_gib=8)
    ledger = queue.tier_ledger(TIER)
    consumer, holder = _orphan(queue, "live")
    units = pt.declared_units(queue, TIERS, [_consumer(consumer)])
    assert [found.key for found in units] == [consumer]
    assert pt.dangling(queue, TIER, ledger, units,
                       complete_census=True) == []
    assert ledger.holder_tokens(holder).get("stage_gib", 0) == 2


# ---------------------------------------------------------- release_terminal


def test_release_terminal_returns_tokens_but_keeps_a_copy(tmp_path) -> None:
    """An ended unit frees its remainder; a copying chunk keeps its own."""
    queue = _queue(tmp_path, stage_gib=8)
    ledger = queue.tier_ledger(TIER)
    consumer = _hexkey("end-consumer")
    plan = _declared_plan(queue, consumer, [("phase-a", 4, True, 2)],
                          tag="end")
    residency_plan.freeze(queue, plan)
    units = pt.declared_units(queue, TIERS, [_consumer(consumer)])
    for _ in range(3):
        pt.reserve_pass(queue, TIER, units, admitted=lambda unit: True)
    movers = [leg["mover_key"] for leg in units[0].legs]
    first_row = _publish_leg(queue, plan, movers[0])
    assert pt.publish_declared(
        queue, TIER, units[0],
        [{"mover_action_key": movers[0],
          "published_unix": float(first_row["published_unix"])}])
    got = queue.claim(tags=["dl380g10"], owner="w-end")
    assert got is not None and got["action_key"] == movers[0]
    second_row = _publish_leg(queue, plan, movers[1])
    assert pt.publish_declared(
        queue, TIER, units[0],
        [{"mover_action_key": movers[1],
          "published_unix": float(second_row["published_unix"])}])
    events = pt.release_terminal(queue, TIER, units, shared_owned=[])
    assert any(event["event"] == "prelaunch-group-released"
               for event in events)
    assert ledger.holder_tokens(movers[0]).get("stage_gib", 0) == 2
    assert ledger.holder_tokens(movers[1]).get("stage_gib", 0) == 0
    assert ledger.available().get("stage_gib") == 6
