"""The window slides: a chunk-sized hole admits the next chunk mid-phase.

The defect, verified live 2026-09-19: promotion and egress are phase-granular,
so a 123 GiB phase promotes only after accepted progress passes the previous
phase and frees it all at the next boundary -- the tmpfs sawtooths 0->123->0
GiB while the GPU idles through each full-phase copy, and the tier loop files
``ram-window-stalled`` with memory free and the reader reading, blocked on
'accepted progress past layer-N'.  Rob's directive: refill the tmpfs as it
frees up from SSD.

Chunked, the same window needs only a chunk-sized hole: while phase-0000 is
still being read, its own tail chunk and the next phase's head chunk promote
into room no whole phase could fit in, and the phase-granular shape sealed
before this change decides byte-identically to today -- chunking is a
sealing-time property, and a node whose range is its phase's whole range
follows the whole-phase rules.
"""
from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import pool, residency_plan, storage_tiers  # noqa: E402

CONSUMER = "c" * 64
MANIFEST = "9" * 64
STAGE_TIER = "prismabuild-stage:dl380g10"
RAM_TIER = "ram:dl380g10"
RAM_KIND = f"ram_gib@{RAM_TIER}"
STAGE_KIND = f"stage_gib@{STAGE_TIER}"
GIB = storage_tiers.GIB
#: Two 6 GiB phases in 2 GiB chunks against an 8 GiB window: no whole phase
#: fits beside the one being read, so only chunks can slide.
CHUNK = 2 * GIB


def _hexkey(seed: str) -> str:
    return (seed.encode().hex() * 64)[:64]


def _row(key: str, resources: dict[str, int]) -> dict:
    return {"action_key": key, "cas_root": "/cas", "checkout_root": "/co",
            "worker_script": "/worker.py", "tags": ["dl380g10"],
            "resources": resources}


def _chunked_phase(ordinal: int, start: int) -> dict:
    chunks = []
    for chunk in range(3):
        cstart, cend = start + chunk * CHUNK, start + (chunk + 1) * CHUNK
        chunks.append({
            "chunk_index": chunk, "start_bytes": cstart, "end_bytes": cend,
            "stage_gib": 2,
            "ram_mover_row": {
                **_row(_hexkey(f"rampromote{ordinal}c{chunk}"),
                       {RAM_KIND: 2, "mem_gb": 1}),
                "residency": {
                    "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": RAM_TIER,
                    "manifest_sha256": MANIFEST, "manifest_bytes": 12 * GIB,
                    "range_start_bytes": cstart, "range_end_bytes": cend}},
            "ram_egress_row": _row(_hexkey(f"ramrelease{ordinal}c{chunk}"),
                                   {"mem_gb": 1}),
        })
    end = start + 3 * CHUNK
    return {
        "name": f"phase-{ordinal:04d}",
        "start_bytes": start, "end_bytes": end, "stage_gib": 6,
        "mover_row": {
            **_row(_hexkey(f"mover{ordinal}"),
                   {STAGE_KIND: 6, "mem_gb": 1}),
            "residency": {
                "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": STAGE_TIER,
                "manifest_sha256": MANIFEST, "manifest_bytes": 12 * GIB,
                "range_start_bytes": start, "range_end_bytes": end}},
        "egress_row": _row(_hexkey(f"egress{ordinal}"), {"mem_gb": 1}),
        "ram_chunks": chunks,
    }


def _whole_phase(ordinal: int, start: int) -> dict:
    end = start + 3 * CHUNK
    return {
        "name": f"phase-{ordinal:04d}",
        "start_bytes": start, "end_bytes": end, "stage_gib": 6,
        "mover_row": {
            **_row(_hexkey(f"mover{ordinal}"),
                   {STAGE_KIND: 6, "mem_gb": 1}),
            "residency": {
                "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": STAGE_TIER,
                "manifest_sha256": MANIFEST, "manifest_bytes": 12 * GIB,
                "range_start_bytes": start, "range_end_bytes": end}},
        "egress_row": _row(_hexkey(f"egress{ordinal}"), {"mem_gb": 1}),
        "ram_mover_row": {
            **_row(_hexkey(f"rampromote{ordinal}"),
                   {RAM_KIND: 6, "mem_gb": 1}),
            "residency": {
                "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": RAM_TIER,
                "manifest_sha256": MANIFEST, "manifest_bytes": 12 * GIB,
                "range_start_bytes": start, "range_end_bytes": end}},
        "ram_egress_row": _row(_hexkey(f"ramrelease{ordinal}"),
                               {"mem_gb": 1}),
    }


def _build(phases) -> dict[str, object]:
    return residency_plan.build_plan(
        consumer_action_key=CONSUMER, tier_id=STAGE_TIER,
        stage_root="/stage/prewarm", manifest_sha256=MANIFEST,
        manifest_bytes=12 * GIB, phases=phases, ram_tier_id=RAM_TIER)


def test_a_chunk_sized_hole_admits_the_next_chunk_while_the_phase_reads() -> None:
    """THE test that names the directive: phase-0000 still reads, 4 GiB of
    its 6 GiB are promoted, and the window publishes its tail chunk and the
    next phase's head chunk into the hole -- where the phase-granular window
    below stalls for a phase-sized hole that never comes."""

    plan = _build([_chunked_phase(0, 0), _chunked_phase(1, 6 * GIB)])

    decision = residency_plan.window(
        plan, accepted_phase="phase-0000", free_gib=4, capacity_gib=8,
        published=[_hexkey("rampromote0c0"), _hexkey("rampromote0c1")],
        staged=[_hexkey("rampromote0c0"), _hexkey("rampromote0c1")],
        mover_role="ram_mover_row")

    assert [(entry["phase"], entry["chunk_index"])
            for entry in decision["publish"]] == [
        ("phase-0000", 2), ("phase-0001", 0)]
    assert decision["evict"] == []
    assert decision["stall"] is None


def test_the_phase_granular_shape_stalls_where_chunks_slide() -> None:
    """Tonight's sealed shape under the identical window: the whole of
    phase-0000 is promoted, 2 GiB are free, and phase-0001 needs 6 -- so
    nothing publishes and the stall names the phase, exactly as today."""

    plan = _build([_whole_phase(0, 0), _whole_phase(1, 6 * GIB)])

    decision = residency_plan.window(
        plan, accepted_phase="phase-0000", free_gib=2, capacity_gib=8,
        published=[_hexkey("rampromote0")],
        staged=[_hexkey("rampromote0")],
        mover_role="ram_mover_row")

    assert decision["publish"] == []
    stall = decision["stall"]
    assert stall is not None
    assert stall["blocked_phase"] == "phase-0001"
    assert stall["reason"] == "runahead_budget"
    assert stall["waiting_for"] == "accepted progress past phase-0000"


def test_a_whole_phase_plan_decides_byte_identically_to_today() -> None:
    """Back-compat pin: no chunk keys, no reshaped payloads, the exact rows
    the coordinator has published since #640."""

    plan = _build([_whole_phase(0, 0), _whole_phase(1, 6 * GIB)])

    decision = residency_plan.window(
        plan, accepted_phase="phase-0001", free_gib=6, capacity_gib=12,
        published=[_hexkey("rampromote0")],
        staged=[_hexkey("rampromote0")],
        mover_role="ram_mover_row")

    phase0 = plan["phases"][0]  # type: ignore[index]
    phase1 = plan["phases"][1]  # type: ignore[index]
    assert decision["publish"] == [{
        "phase": "phase-0001",
        "mover_action_key": _hexkey("rampromote1"),
        "start_bytes": 6 * GIB, "end_bytes": 12 * GIB, "stage_gib": 6,
        "mover_row": phase1["ram_mover_row"]}]
    assert decision["evict"] == [{
        "phase": "phase-0000",
        "mover_action_key": _hexkey("rampromote0"),
        "egress_row": phase0["ram_egress_row"],
        "stage_gib": 6}]
    assert decision["stall"] is None
    for entry in (*decision["publish"], *decision["evict"]):
        assert "chunk_index" not in entry
