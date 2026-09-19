"""One phase may slide on both tiers at once, each tiled on its own.

The SSD leg and the tmpfs leg chunk the same phase independently: the stage
window reads ``stage_chunks``, the ram window reads ``ram_chunks``, and a
gap in one table refuses the plan while the other table tiles cleanly --
tiling is per leg, not per phase, because a gap is bytes nobody moves on
that tier and the other tier's cover cannot vouch for them.  The two tables
need not agree on chunk counts or boundaries: the ram window's stage-source
gate is overlap-aware, so a promotion stages exactly when the stage chunks
under its own range have landed.
"""
from __future__ import annotations

from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import pool, residency_plan, storage_tiers  # noqa: E402

CONSUMER = "c" * 64
MANIFEST = "9" * 64
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


def _pin(tier: str, start: int, end: int) -> dict:
    return {
        "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": tier,
        "manifest_sha256": MANIFEST, "manifest_bytes": 6 * GIB,
        "range_start_bytes": start, "range_end_bytes": end}


def _stage_chunk(chunk: int, start: int, end: int) -> dict:
    gib = (end - start) // GIB
    return {
        "chunk_index": chunk, "start_bytes": start, "end_bytes": end,
        "stage_gib": gib,
        "mover_row": {
            **_row(_hexkey(f"mover0c{chunk}"), {STAGE_KIND: gib}),
            "residency": _pin(STAGE_TIER, start, end)},
        "egress_row": _row(_hexkey(f"egress0c{chunk}"), {"mem_gb": 1}),
    }


def _ram_chunk(chunk: int, start: int, end: int) -> dict:
    gib = (end - start) // GIB
    return {
        "chunk_index": chunk, "start_bytes": start, "end_bytes": end,
        "stage_gib": gib,
        "ram_mover_row": {
            **_row(_hexkey(f"rampromote0c{chunk}"), {RAM_KIND: gib}),
            "residency": _pin(RAM_TIER, start, end)},
        "ram_egress_row": _row(_hexkey(f"ramrelease0c{chunk}"),
                               {"mem_gb": 1}),
    }


def _phases(*, stage=None, ram=None) -> list[dict]:
    return [{
        "name": "phase-0000",
        "start_bytes": 0, "end_bytes": 6 * GIB, "stage_gib": 6,
        "stage_chunks": stage if stage is not None else [
            _stage_chunk(0, 0, 3 * GIB), _stage_chunk(1, 3 * GIB, 6 * GIB)],
        "ram_chunks": ram if ram is not None else [
            _ram_chunk(0, 0, 2 * GIB), _ram_chunk(1, 2 * GIB, 4 * GIB),
            _ram_chunk(2, 4 * GIB, 6 * GIB)],
    }]


def _build(phases) -> dict[str, object]:
    return residency_plan.build_plan(
        consumer_action_key=CONSUMER, tier_id=STAGE_TIER,
        stage_root="/stage/prewarm", manifest_sha256=MANIFEST,
        manifest_bytes=6 * GIB, phases=phases, ram_tier_id=RAM_TIER)


def test_both_legs_chunk_one_phase_on_their_own_tiling() -> None:
    """Two stage chunks, three ram chunks, one phase: both tables validate
    and both key sets join the plan's."""

    plan = _build(_phases())

    assert [chunk["chunk_index"]
            for chunk in plan["phases"][0]["stage_chunks"]] == [0, 1]  # type: ignore[index]
    assert [chunk["chunk_index"]
            for chunk in plan["phases"][0]["ram_chunks"]] == [0, 1, 2]  # type: ignore[index]
    assert residency_plan.stage_mover_keys(plan) == [
        _hexkey("mover0c0"), _hexkey("mover0c1")]
    assert residency_plan.ram_mover_keys(plan) == [
        _hexkey("rampromote0c0"), _hexkey("rampromote0c1"),
        _hexkey("rampromote0c2")]
    assert residency_plan.mover_keys(plan) == (
        residency_plan.stage_mover_keys(plan)
        + residency_plan.ram_mover_keys(plan))


def test_each_leg_decides_from_its_own_table() -> None:
    """The stage window publishes stage chunks, the ram window ram chunks;
    neither decision names the other's rows."""

    plan = _build(_phases())

    stage = residency_plan.window(
        plan, accepted_phase=None, free_gib=6, capacity_gib=6,
        published=[], staged=[])
    ram = residency_plan.window(
        plan, accepted_phase=None, free_gib=6, capacity_gib=6,
        published=[], staged=[], mover_role="ram_mover_row")

    assert [(entry["phase"], entry["chunk_index"])
            for entry in stage["publish"]] == [
        ("phase-0000", 0), ("phase-0000", 1)]
    assert {entry["mover_action_key"] for entry in stage["publish"]} == {
        _hexkey("mover0c0"), _hexkey("mover0c1")}
    assert [(entry["phase"], entry["chunk_index"])
            for entry in ram["publish"]] == [
        ("phase-0000", 0), ("phase-0000", 1), ("phase-0000", 2)]
    assert {entry["mover_action_key"] for entry in ram["publish"]} == {
        _hexkey("rampromote0c0"), _hexkey("rampromote0c1"),
        _hexkey("rampromote0c2")}
    assert stage["stall"] is None and ram["stall"] is None


def test_a_gap_in_either_table_refuses_while_the_other_tiles() -> None:
    """A gap is bytes nobody moves on that tier: the clean table cannot
    cover for the gapped one, whichever leg gapped."""

    gapped_stage = [_stage_chunk(0, 0, 3 * GIB),
                    {**_stage_chunk(1, 3 * GIB, 6 * GIB),
                     "start_bytes": 3 * GIB + 1}]
    with pytest.raises(residency_plan.ResidencyPlanError):
        _build(_phases(stage=gapped_stage))

    gapped_ram = [_ram_chunk(0, 0, 2 * GIB),
                  {**_ram_chunk(1, 2 * GIB, 4 * GIB),
                   "start_bytes": 2 * GIB + 1},
                  _ram_chunk(2, 4 * GIB, 6 * GIB)]
    with pytest.raises(residency_plan.ResidencyPlanError):
        _build(_phases(ram=gapped_ram))


def test_a_chunk_pin_must_name_its_own_leg() -> None:
    """A stage chunk pinned to the ram tier -- or the reverse -- stages
    bytes its own window never publishes under that name."""

    pinned = [{**chunk, "mover_row": {
        **chunk["mover_row"],
        "residency": _pin(RAM_TIER, chunk["start_bytes"],
                          chunk["end_bytes"])}}  # type: ignore[index]
        for chunk in _phases()[0]["stage_chunks"]]
    with pytest.raises(residency_plan.ResidencyPlanError):
        _build(_phases(stage=pinned))
