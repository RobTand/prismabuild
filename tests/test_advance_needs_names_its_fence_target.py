"""advance_needs names the frontier advance on every nonfinal result (#832).

``tier_loop._protect_tier_advances`` reads ``needs.get("fence_target")``: a
missing dictionary is final/no-required-advance, so the window is permitted
with ``advance="final"`` and publishes its current without the advance
reservation the current-plus-next gate exists to guarantee. The computation
lived above the early return only; the normal waiting path dropped it while
its own docstring promised both fields.

The ledger test below is the boundary that matters, not the dict shape: an
admitted nonfinal window must hold its named advance on the tier ledger
before it is permitted, exactly once per consumer/tier/leg.
"""
from __future__ import annotations

from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

from prismabuild import pool, residency_plan, storage_tiers, window_credit  # noqa: E402
import tier_loop  # noqa: E402

CONSUMER = "c" * 64
MANIFEST = "9" * 64
STAGE_TIER = "prismabuild-stage:dl380g10"
RAM_TIER = "ram:dl380g10"
STAGE_KIND = f"stage_gib@{STAGE_TIER}"
GIB = storage_tiers.GIB


def _hexkey(seed: str) -> str:
    return (seed.encode().hex() * 64)[:64]


def _row(key: str, resources: dict[str, int]) -> dict:
    return {"action_key": key, "cas_root": "/cas", "checkout_root": "/co",
            "worker_script": "/worker.py", "tags": ["dl380g10"],
            "resources": resources}


def _whole_phase(ordinal: int, gib: int) -> dict:
    start, end = ordinal * gib * GIB, (ordinal + 1) * gib * GIB
    return {
        "name": f"phase-{ordinal:04d}",
        "start_bytes": start, "end_bytes": end, "stage_gib": gib,
        "mover_row": {
            **_row(_hexkey(f"mover{ordinal}"), {STAGE_KIND: gib, "mem_gb": 1}),
            "residency": {
                "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": STAGE_TIER,
                "manifest_sha256": MANIFEST, "manifest_bytes": end,
                "range_start_bytes": start, "range_end_bytes": end}},
        "egress_row": _row(_hexkey(f"egress{ordinal}"), {"mem_gb": 1}),
        "ram_mover_row": {
            **_row(_hexkey(f"rampromote{ordinal}"),
                   {f"ram_gib@{RAM_TIER}": gib, "mem_gb": 1}),
            "residency": {
                "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": RAM_TIER,
                "manifest_sha256": MANIFEST, "manifest_bytes": end,
                "range_start_bytes": start, "range_end_bytes": end}},
        "ram_egress_row": _row(_hexkey(f"ramrelease{ordinal}"), {"mem_gb": 1}),
    }


def _whole_plan(phases: int = 3, gib: int = 2) -> dict[str, object]:
    built = [_whole_phase(ordinal, gib) for ordinal in range(phases)]
    return residency_plan.build_plan(
        consumer_action_key=CONSUMER, tier_id=STAGE_TIER,
        stage_root="/stage/prewarm", manifest_sha256=MANIFEST,
        manifest_bytes=phases * gib * GIB, phases=built,
        ram_tier_id=RAM_TIER)


def _chunk(phase: int, chunk: int, start: int, end: int) -> dict:
    gib = (end - start) // GIB
    return {
        "chunk_index": chunk, "start_bytes": start, "end_bytes": end,
        "stage_gib": gib,
        "mover_row": {
            **_row(_hexkey(f"mover{phase}c{chunk}"),
                   {STAGE_KIND: gib, "mem_gb": 1}),
            "residency": {
                "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": STAGE_TIER,
                "manifest_sha256": MANIFEST, "manifest_bytes": 4 * GIB,
                "range_start_bytes": start, "range_end_bytes": end}},
        "egress_row": _row(_hexkey(f"egress{phase}c{chunk}"), {"mem_gb": 1}),
    }


def _chunked_plan() -> dict[str, object]:
    built = []
    start = 0
    for ordinal in range(2):
        end = start + 2 * GIB
        mid = start + GIB
        built.append({
            "name": f"phase-{ordinal:04d}",
            "start_bytes": start, "end_bytes": end, "stage_gib": 2,
            "stage_chunks": [_chunk(ordinal, 0, start, mid),
                             _chunk(ordinal, 1, mid, end)],
        })
        start = end
    return residency_plan.build_plan(
        consumer_action_key=CONSUMER, tier_id=STAGE_TIER,
        stage_root="/stage/prewarm", manifest_sha256=MANIFEST,
        manifest_bytes=start, phases=built)


@pytest.mark.parametrize("mover_role", ["mover_row", "ram_mover_row"])
def test_nonfinal_result_names_the_frontier_advance(mover_role: str) -> None:
    """The lead plus its protected next, on either leg."""

    plan = _whole_plan()
    needs = residency_plan.advance_needs(plan, None, mover_role=mover_role)

    assert needs["final"] is False
    assert needs["current_min_gib"] == 2
    assert needs["next_min_gib"] == 2
    target = needs["fence_target"]
    assert isinstance(target, dict)
    assert target["phase"] == "phase-0001"
    assert target["stage_gib"] == 2
    assert target["chunk_index"] is None
    assert needs["fence_prior"] == []


def test_frontier_advance_moves_past_landed_ranges() -> None:
    """A landed (or adopted) ahead range is not the frontier; the leg after
    the earliest unstaged one is the fenced advance, with earlier legs as
    the safe-retire prior."""

    plan = _whole_plan()
    needs = residency_plan.advance_needs(
        plan, None, staged=[_hexkey("mover0")])

    assert needs["final"] is False
    target = needs["fence_target"]
    assert isinstance(target, dict)
    assert target["phase"] == "phase-0002"
    assert target["mover_action_key"] == _hexkey("mover2")
    prior = needs["fence_prior"]
    assert [leg["mover_action_key"] for leg in prior] == [_hexkey("mover0")]


def test_final_leg_needs_no_advance() -> None:
    """The one case that permits without a fence: nothing ahead of the last
    leg. This guards the fix against overcorrection."""

    plan = _whole_plan(phases=1)
    needs = residency_plan.advance_needs(plan, None)

    assert needs["final"] is True
    assert needs["next_min_gib"] is None
    assert needs["fence_target"] is None


def test_chunked_leg_targets_the_next_chunk() -> None:
    """Chunked legs fence per chunk: the frontier's immediate next chunk,
    never the next phase."""

    plan = _chunked_plan()
    fresh = residency_plan.advance_needs(plan, None, mover_role="mover_row")

    target = fresh["fence_target"]
    assert isinstance(target, dict)
    assert (target["phase"], target["chunk_index"]) == ("phase-0000", 1)
    assert target["stage_gib"] == 1

    advanced = residency_plan.advance_needs(
        plan, None, mover_role="mover_row", staged=[_hexkey("mover0c0")])
    following = advanced["fence_target"]
    assert isinstance(following, dict)
    assert (following["phase"], following["chunk_index"]) == ("phase-0001", 0)


def test_protection_holds_the_named_advance_before_it_permits(
        tmp_path: Path) -> None:
    """End to end of the consuming gate: an admitted nonfinal window is
    permitted with its advance held on the ledger, once, before its current
    may publish. Without the named target this same window reads final and
    holds nothing."""

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    plan = _whole_plan(phases=2)
    residency_plan.freeze(queue, plan)
    queue.publish(action_key=CONSUMER, cas_root=queue.root / "cas",
                  checkout_root=queue.root / "co",
                  worker_script=queue.root / "worker.py",
                  resources={"cpu": 1, "mem_gb": 1}, tags=["x86"],
                  residency={"schema": pool.RESIDENCY_SCHEMA_V1,
                             "tier_id": STAGE_TIER, "manifest_sha256": MANIFEST,
                             "manifest_bytes": 4 * GIB,
                             "leads": residency_plan.leads_for(plan)})
    queue.mint_tier_capacity(STAGE_TIER, {"stage_gib": 4})

    result = tier_loop._protect_tier_advances(
        queue,
        {STAGE_TIER: {"tier_id": STAGE_TIER, "tier": "stage",
                      "mountpoint": str(tmp_path / "stage")}},
        mover_role="mover_row",
        tier_of=lambda plan: plan.get("tier_id"),
        state_of=tier_loop._mover_state)

    assert result["gated"] == {}
    entry = result["permitted"][(CONSUMER, STAGE_TIER)]
    assert entry["advance"] == "blind-held"
    grant = result["grants"][(CONSUMER, STAGE_TIER)]
    assert grant == window_credit.grant_key(
        CONSUMER, STAGE_TIER, "mover_row", "phase-0001", None)
    assert queue.tier_ledger(STAGE_TIER).holder_tokens(grant) == {"stage_gib": 2}
    assert [key for key in result["grants"] if key[0] == CONSUMER] == [
        (CONSUMER, STAGE_TIER)]
