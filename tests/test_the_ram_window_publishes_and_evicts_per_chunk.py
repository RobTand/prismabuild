"""The ram window publishes and evicts one chunk at a time, in read order.

A phase's promotion used to be one node over the whole range: 123 GiB or
nothing, admitted only when the tmpfs held a phase-sized hole and the
run-ahead budget held a phase-sized allowance.  Sealed per chunk, the same
window publishes the next chunk when free ``ram_gib`` covers it and the
budget admits it: chunks of the phase being read are the reader's near-term
food and promote as soon as their turn comes, while chunks of later phases
spend the run-ahead budget -- which now buys several chunks instead of zero
phases.  Egress frees a chunk when accepted progress passes its range: every
chunk of a passed phase egresses through its own node, in chunk order.
"""
from __future__ import annotations

from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import pool, residency_plan, storage_tiers  # noqa: E402

import pbstatus  # noqa: E402
import residency_publication  # noqa: E402

CONSUMER = "c" * 64
MANIFEST = "9" * 64
EPOCH = "1695052800-1a2b3c4d5e6f7a8b"
STAGE_TIER = "prismabuild-stage:dl380g10"
RAM_TIER = "ram:dl380g10"
RAM_KIND = f"ram_gib@{RAM_TIER}"
STAGE_KIND = f"stage_gib@{STAGE_TIER}"
GIB = storage_tiers.GIB


def _hexkey(seed: str) -> str:
    return (seed.encode().hex() * 64)[:64]


def _row(key: str, resources: dict[str, int]) -> dict:
    return {"action_key": key, "cas_root": "/cas", "checkout_root": "/co",
            "worker_script": "/worker.py", "tags": ["dl380g10"],
            "resources": resources}


def _chunk(phase: int, chunk: int, start: int, end: int) -> dict:
    gib = (end - start) // GIB
    return {
        "chunk_index": chunk, "start_bytes": start, "end_bytes": end,
        "stage_gib": gib,
        "ram_mover_row": {
            **_row(_hexkey(f"rampromote{phase}c{chunk}"),
                   {RAM_KIND: gib, "mem_gb": 1}),
            "residency": {
                "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": RAM_TIER,
                "manifest_sha256": MANIFEST, "manifest_bytes": 6 * GIB,
                "range_start_bytes": start, "range_end_bytes": end}},
        "ram_egress_row": _row(_hexkey(f"ramrelease{phase}c{chunk}"),
                               {"mem_gb": 1}),
    }


def _plan(*, phases: int = 3, chunk_gib: int = 1) -> dict[str, object]:
    """Phases of two chunks each, all sealed, all frozen through build_plan."""

    built = []
    start = 0
    for ordinal in range(phases):
        end = start + 2 * chunk_gib * GIB
        mid = start + chunk_gib * GIB
        built.append({
            "name": f"phase-{ordinal:04d}",
            "start_bytes": start, "end_bytes": end, "stage_gib": 2 * chunk_gib,
            "mover_row": {
                **_row(_hexkey(f"mover{ordinal}"),
                       {STAGE_KIND: 2 * chunk_gib, "mem_gb": 1}),
                "residency": {
                    "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": STAGE_TIER,
                    "manifest_sha256": MANIFEST, "manifest_bytes": 6 * GIB,
                    "range_start_bytes": start, "range_end_bytes": end}},
            "egress_row": _row(_hexkey(f"egress{ordinal}"), {"mem_gb": 1}),
            "ram_chunks": [_chunk(ordinal, 0, start, mid),
                           _chunk(ordinal, 1, mid, end)],
        })
        start = end
    return residency_plan.build_plan(
        consumer_action_key=CONSUMER, tier_id=STAGE_TIER,
        stage_root="/stage/prewarm", manifest_sha256=MANIFEST,
        manifest_bytes=start, phases=built, ram_tier_id=RAM_TIER)


def _key(phase: int, chunk: int) -> str:
    return _hexkey(f"rampromote{phase}c{chunk}")


def test_chunks_publish_in_read_order_behind_free_space() -> None:
    plan = _plan()

    decision = residency_plan.window(
        plan, accepted_phase="phase-0000", free_gib=3, capacity_gib=4,
        published=[_key(0, 0)], staged=[_key(0, 0)],
        mover_role="ram_mover_row")

    assert [(entry["phase"], entry["chunk_index"]) for entry in decision["publish"]] == [
        ("phase-0000", 1), ("phase-0001", 0), ("phase-0001", 1)]
    assert decision["evict"] == []
    assert decision["stall"] is None


def test_the_current_phases_chunks_ignore_the_runahead_budget() -> None:
    """Near-term food is not run-ahead: the phase being read promotes as its
    turn comes, and only later phases spend the budget."""

    plan = _plan()

    decision = residency_plan.window(
        plan, accepted_phase="phase-0000", free_gib=3, capacity_gib=4,
        published=[_key(0, 0), _key(1, 0), _key(1, 1), _key(2, 0)],
        staged=[_key(0, 0)],
        mover_role="ram_mover_row")

    assert [(entry["phase"], entry["chunk_index"]) for entry in decision["publish"]] == [
        ("phase-0000", 1)]
    stall = decision["stall"]
    assert stall is not None
    assert stall["blocked_phase"] == "phase-0002"
    assert stall["chunk_index"] == 1
    assert stall["blocked_gib"] == 1
    assert stall["runahead_gib"] == 3 and stall["runahead_budget_gib"] == 3
    assert stall["reason"] == "runahead_budget"


def test_each_chunk_of_a_passed_phase_egresses_through_its_own_node() -> None:
    plan = _plan()

    decision = residency_plan.window(
        plan, accepted_phase="phase-0001", free_gib=4, capacity_gib=4,
        published=[_key(0, 0), _key(0, 1)],
        staged=[_key(0, 0), _key(0, 1)],
        mover_role="ram_mover_row")

    assert [(entry["phase"], entry["chunk_index"]) for entry in decision["evict"]] == [
        ("phase-0000", 0), ("phase-0000", 1)]
    for entry, chunk in zip(decision["evict"], (0, 1)):
        assert entry["mover_action_key"] == _key(0, chunk)
        assert entry["egress_row"] == plan["phases"][0]["ram_chunks"][chunk][  # type: ignore[index]
            "ram_egress_row"]
        assert entry["stage_gib"] == 1


def _whole_ram_leg(suffix: str, start: int, end: int) -> dict:
    """Today's shape: one promotion node over the phase's whole range."""

    gib = (end - start) // GIB
    return {
        "ram_mover_row": {
            **_row(_hexkey(f"rampromote{suffix}"),
                   {RAM_KIND: gib, "mem_gb": 1}),
            "residency": {
                "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": RAM_TIER,
                "manifest_sha256": MANIFEST, "manifest_bytes": 6 * GIB,
                "range_start_bytes": start, "range_end_bytes": end}},
        "ram_egress_row": _row(_hexkey(f"ramrelease{suffix}"), {"mem_gb": 1}),
    }


def test_a_whole_phase_leg_beside_chunks_decides_as_it_always_did() -> None:
    """Mixed plans happen: a phase that fits in one chunk seals today's
    shape, and the window must not wrap it in chunk clothing."""

    plan = _plan()
    whole = dict(plan["phases"][0])  # type: ignore[index]
    whole.pop("ram_chunks")
    whole.update(_whole_ram_leg("whole0", 0, 2 * GIB))
    mixed = residency_plan.build_plan(
        consumer_action_key=CONSUMER, tier_id=STAGE_TIER,
        stage_root="/stage/prewarm", manifest_sha256=MANIFEST,
        manifest_bytes=6 * GIB,
        phases=[whole, *plan["phases"][1:]], ram_tier_id=RAM_TIER)  # type: ignore[index]

    decision = residency_plan.window(
        mixed, accepted_phase=None, free_gib=4, capacity_gib=4,
        published=[], staged=[], mover_role="ram_mover_row")

    first = decision["publish"][0]
    assert first["phase"] == "phase-0000"
    assert "chunk_index" not in first


def test_chunks_must_tile_their_phase_without_gap_or_overlap() -> None:
    plan = _plan()
    bad = dict(plan["phases"][0])  # type: ignore[index]
    gapped = [dict(chunk) for chunk in bad.pop("ram_chunks")]
    gapped[1]["start_bytes"] = gapped[1]["start_bytes"] + 1
    with pytest.raises(residency_plan.ResidencyPlanError):
        residency_plan.build_plan(
            consumer_action_key=CONSUMER, tier_id=STAGE_TIER,
            stage_root="/stage/prewarm", manifest_sha256=MANIFEST,
            manifest_bytes=6 * GIB,
            phases=[{**bad, "ram_chunks": gapped},
                    *plan["phases"][1:]],  # type: ignore[index]
            ram_tier_id=RAM_TIER)


def test_a_phase_cannot_carry_both_a_leg_and_chunks() -> None:
    plan = _plan()
    both = dict(plan["phases"][0])  # type: ignore[index]
    chunk = both["ram_chunks"][0]
    both["ram_mover_row"] = chunk["ram_mover_row"]
    with pytest.raises(residency_plan.ResidencyPlanError):
        residency_plan.build_plan(
            consumer_action_key=CONSUMER, tier_id=STAGE_TIER,
            stage_root="/stage/prewarm", manifest_sha256=MANIFEST,
            manifest_bytes=6 * GIB,
            phases=[both, *plan["phases"][1:]],  # type: ignore[index]
            ram_tier_id=RAM_TIER)


def test_chunk_mover_keys_join_the_plans_key_set() -> None:
    """The orphan sweep reads ``ram_mover_keys``: a chunk promotion no live
    item names is an orphan on the ram tier exactly as its stage sibling is."""

    plan = _plan()

    assert residency_plan.ram_mover_keys(plan) == [
        _key(phase, chunk) for phase in range(3) for chunk in range(2)]
    assert set(residency_plan.ram_mover_keys(plan)) <= set(
        residency_plan.mover_keys(plan))


def test_the_census_reports_a_chunked_phase_per_chunk(tmp_path) -> None:
    """The starvation census must not read a chunked phase as having no ram
    leg: its per-chunk promotion state rides beside the phase's own
    (whole-phase) ``ram``, which stays ``None`` so the existing shape holds."""

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    plan = _plan()
    residency_plan.freeze(queue, plan)
    queue.publish(**_row(CONSUMER, {"mem_gb": 1}), residency={
        "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": STAGE_TIER,
        "manifest_sha256": MANIFEST, "manifest_bytes": 6 * GIB,
        "leads": residency_plan.leads_for(plan)})
    queue.mint_tier_capacity(RAM_TIER, {"ram_gib": 8})
    assert queue.tier_ledger(RAM_TIER).acquire(_key(0, 0), {"ram_gib": 1})
    # The booking above is the room; these are the records the finished
    # promotion left, which is what the census now reports (#759).
    queue.announce_tier({
        "schema": storage_tiers.TIER_RECORD_SCHEMA_V1, "tier": "ram",
        "tier_id": RAM_TIER, "host": "dl380g10",
        "mountpoint": str(tmp_path / "ram"), "epoch": EPOCH,
        "capacity_bytes": 8 * GIB})
    residency_publication.vouch_landed(
        queue, consumer_action_key=CONSUMER, mover_action_key=_key(0, 0),
        tier_id=RAM_TIER, stage_root=tmp_path / "ram",
        manifest_sha256=MANIFEST, range_start_bytes=0,
        range_end_bytes=GIB, epoch=EPOCH)

    entry = pbstatus._starvation_plan_entry(
        queue, CONSUMER, plan, ready={CONSUMER}, claimed=set("#"),
        notes=[], unreadable=[], now=0.0)

    first = entry["phases"][0]
    assert first["ram"] is None
    assert [(chunk["chunk_index"], chunk["staged"])
            for chunk in first["ram_chunks"]] == [(0, True), (1, False)]
    assert entry["cursor_gap"]["ram"]["remaining_phases"] == 3
    assert entry["cursor_gap"]["ram"]["unstaged_phases"] == [
        "phase-0000", "phase-0001", "phase-0002"]
