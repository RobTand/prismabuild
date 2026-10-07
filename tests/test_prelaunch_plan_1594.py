"""The prelaunch-resident plan: validation, leads, bound, demand (#1594).

A frozen plan carries the declared prefix per phase.  Admission waits on
all chunk movers of all declared phases.  Submission refuses a peak above
the minted tier capacity.  A plan with no declaration serializes as today.
"""
from __future__ import annotations

import hashlib
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from prismabuild import core as pb  # noqa: E402
from prismabuild import pool, residency_plan, storage_tiers  # noqa: E402

TIER = "prismabuild-stage:dl380g10"
STAGE_KIND = f"stage_gib@{TIER}"
DIGEST = "9" * 64
GIB = storage_tiers.GIB


def _hexkey(seed: str) -> str:
    return hashlib.sha256(seed.encode()).hexdigest()


def _mover_row(key: str, start: int, end: int, total: int) -> dict:
    gib = storage_tiers.stage_tokens_for_bytes(end - start)
    return {"action_key": key,
            "resources": {STAGE_KIND: gib, "cpu": 1, "mem_gb": 1},
            "residency": {"schema": pool.RESIDENCY_SCHEMA_V1,
                          "tier_id": TIER, "manifest_sha256": DIGEST,
                          "manifest_bytes": total,
                          "range_start_bytes": start,
                          "range_end_bytes": end}}


def _egress_row(key: str) -> dict:
    return {"action_key": key, "resources": {"mem_gb": 1}}


def _whole(name: str, start: int, end: int, total: int, seed: str,
           declared: bool = False) -> dict:
    phase = {"name": name, "start_bytes": start, "end_bytes": end,
             "mover_row": _mover_row(_hexkey(f"{seed}mover"), start, end,
                                    total),
             "egress_row": _egress_row(_hexkey(f"{seed}egress"))}
    if declared:
        phase["resident_before_launch"] = True
    return phase


def _chunked_at(name: str, ranges: list[tuple[int, int]], total: int,
                seed: str, declared: bool = False) -> dict:
    """Build a submitter phase from explicit chunk ranges in read order."""
    chunks = []
    for index, (cstart, cend) in enumerate(ranges):
        chunks.append({
            "chunk_index": index, "start_bytes": cstart, "end_bytes": cend,
            "stage_gib": storage_tiers.stage_tokens_for_bytes(cend - cstart),
            "mover_row": _mover_row(_hexkey(f"{seed}mover{index}"),
                                   cstart, cend, total),
            "egress_row": _egress_row(_hexkey(f"{seed}egress{index}"))})
    phase = {"name": name, "start_bytes": ranges[0][0],
             "end_bytes": ranges[-1][1], "stage_chunks": chunks}
    if declared:
        phase["resident_before_launch"] = True
    return phase


def _build(phases, consumer="c" * 64, digest=DIGEST, tier=TIER):
    total = phases[-1]["end_bytes"]
    return residency_plan.build_plan(
        consumer_action_key=consumer, tier_id=tier, stage_root="/stage",
        manifest_sha256=digest, manifest_bytes=total, phases=phases)


def test_declared_whole_phase_prefix_validates() -> None:
    plan = _build([_whole("a", 0, 90 * GIB, 170 * GIB, "w0", declared=True),
                   _whole("b", 90 * GIB, 170 * GIB, 170 * GIB, "w1")])
    assert residency_plan.prelaunch_phase_names(plan) == ["a"]
    assert plan["phases"][0]["resident_before_launch"] is True
    assert "resident_before_launch" not in plan["phases"][1]


def test_non_true_declaration_refuses() -> None:
    phases = [_whole("a", 0, 10 * GIB, 20 * GIB, "n0")]
    phases[0]["resident_before_launch"] = False
    with pytest.raises(residency_plan.ResidencyPlanError, match="must be true"):
        residency_plan.build_plan(
            consumer_action_key="c" * 64, tier_id=TIER, stage_root="/stage",
            manifest_sha256=DIGEST, manifest_bytes=20 * GIB, phases=phases)


def test_non_prefix_declaration_refuses() -> None:
    plan = _build([_whole("a", 0, 10 * GIB, 20 * GIB, "p0"),
                   _whole("b", 10 * GIB, 20 * GIB, 20 * GIB, "p1")])
    plan["phases"][1]["resident_before_launch"] = True
    with pytest.raises(residency_plan.ResidencyPlanError,
                       match="contiguous prefix"):
        residency_plan.validate_plan(plan)


def test_gap_declaration_refuses() -> None:
    plan = _build([_whole("a", 0, 10 * GIB, 30 * GIB, "g0"),
                   _whole("b", 10 * GIB, 20 * GIB, 30 * GIB, "g1"),
                   _whole("c", 20 * GIB, 30 * GIB, 30 * GIB, "g2")])
    plan["phases"][0]["resident_before_launch"] = True
    plan["phases"][2]["resident_before_launch"] = True
    with pytest.raises(residency_plan.ResidencyPlanError,
                       match="contiguous prefix"):
        residency_plan.validate_plan(plan)


# -- leads -----------------------------------------------------------------


def test_undeclared_leads_are_the_first_mover_only() -> None:
    plan = _build([_whole("a", 0, 10 * GIB, 20 * GIB, "u0"),
                   _whole("b", 10 * GIB, 20 * GIB, 20 * GIB, "u1")])
    assert residency_plan.leads_for(plan) == [
        str(plan["phases"][0]["mover_row"]["action_key"])]


def test_declared_whole_phase_prefix_expands_leads() -> None:
    plan = _build([_whole("a", 0, 10 * GIB, 30 * GIB, "e0", declared=True),
                   _whole("b", 10 * GIB, 20 * GIB, 30 * GIB, "e1",
                          declared=True),
                   _whole("c", 20 * GIB, 30 * GIB, 30 * GIB, "e2")])
    assert residency_plan.leads_for(plan) == [
        str(plan["phases"][0]["mover_row"]["action_key"]),
        str(plan["phases"][1]["mover_row"]["action_key"])]


def test_declared_chunked_phase_expands_to_every_chunk() -> None:
    plan = _build([_chunked_at(
        "a", [(0, 40 * GIB), (40 * GIB, 80 * GIB),
              (80 * GIB, 90 * GIB)], 170 * GIB, "k0", declared=True),
        _whole("b", 90 * GIB, 170 * GIB, 170 * GIB, "k1")])
    assert residency_plan.leads_for(plan) == [
        str(chunk["mover_row"]["action_key"])
        for chunk in plan["phases"][0]["stage_chunks"]]


def test_declared_multi_phase_mixed_prefix_expands_in_read_order() -> None:
    plan = _build([_chunked_at(
        "a", [(0, 45 * GIB), (45 * GIB, 90 * GIB)], 170 * GIB, "m0",
        declared=True),
        _whole("b", 90 * GIB, 130 * GIB, 170 * GIB, "m1", declared=True),
        _whole("c", 130 * GIB, 170 * GIB, 170 * GIB, "m2")])
    assert residency_plan.leads_for(plan) == [
        str(plan["phases"][0]["stage_chunks"][0]["mover_row"]["action_key"]),
        str(plan["phases"][0]["stage_chunks"][1]["mover_row"]["action_key"]),
        str(plan["phases"][1]["mover_row"]["action_key"])]


def test_frozen_reuse_and_campaign_read_the_same_leads() -> None:
    # pbrun reuse and pbcampaign write ``leads_for`` of the filed plan.
    # One call gives one answer on both paths.
    plan = _build([_chunked_at(
        "a", [(0, 45 * GIB), (45 * GIB, 90 * GIB)], 130 * GIB, "r0",
        declared=True),
        _whole("b", 90 * GIB, 130 * GIB, 130 * GIB, "r1")])
    frozen_block = {"leads": residency_plan.leads_for(plan)}
    campaign_block = {"leads": residency_plan.leads_for(plan)}
    assert frozen_block == campaign_block
    assert len(frozen_block["leads"]) == 2


# -- bound -----------------------------------------------------------------


def _sized_plan(prefix_gibs, suffix_gibs, seed, chunked_prefix=False):
    """Build a whole-phase plan from GiB sizes.  The first prefix phases declare."""
    sizes = prefix_gibs + suffix_gibs
    total = sum(sizes) * GIB
    phases = []
    start = 0
    for index, gib in enumerate(sizes):
        end = start + gib * GIB
        declared = index < len(prefix_gibs)
        if chunked_prefix and declared and gib > 1:
            half = gib // 2
            phases.append(_chunked_at(
                f"phase-{index}",
                [(start, start + half * GIB),
                 (start + half * GIB, end)], total, f"{seed}{index}",
                declared=True))
        else:
            phases.append(_whole(f"phase-{index}", start, end, total,
                                 f"{seed}{index}", declared=declared))
        start = end
    return _build(phases)


def test_bound_design_example_first() -> None:
    # T=90 with suffix 1,1,40,40 gives B=170.
    plan = _sized_plan([90], [1, 1, 40, 40], "b0")
    assert residency_plan.prelaunch_bound(plan) == {
        "retained_gib": 90, "suffix_gib": 80, "peak_gib": 170}


def test_bound_design_example_second() -> None:
    # T=160 with suffix 40,40 gives B=240.
    plan = _sized_plan([160], [40, 40], "b1")
    assert residency_plan.prelaunch_bound(plan) == {
        "retained_gib": 160, "suffix_gib": 80, "peak_gib": 240}


def test_bound_without_suffix_is_the_prefix() -> None:
    plan = _sized_plan([90], [], "b2")
    assert residency_plan.prelaunch_bound(plan) == {
        "retained_gib": 90, "suffix_gib": 0, "peak_gib": 90}


def test_bound_without_declaration_is_none() -> None:
    plan = _sized_plan([], [40, 40], "b3")
    assert residency_plan.prelaunch_bound(plan) is None
    assert residency_plan.prelaunch_phase_names(plan) == []


def test_bound_excludes_legs_owned_by_others() -> None:
    plan = _sized_plan([90], [40], "b4", chunked_prefix=True)
    chunks = plan["phases"][0]["stage_chunks"]
    assert len(chunks) == 2
    owned = {str(chunks[0]["mover_row"]["action_key"])}
    assert residency_plan.prelaunch_bound(plan, owned_by_others=owned) == {
        "retained_gib": 45, "suffix_gib": 40, "peak_gib": 85}


def test_bound_counts_chunk_demands_not_rederived_bytes() -> None:
    # Two 45 GiB chunk demands retain 90.  The bound reads sealed demands,
    # not bytes.
    plan = _sized_plan([90], [40], "b5", chunked_prefix=True)
    bound = residency_plan.prelaunch_bound(plan)
    assert bound == {"retained_gib": 90, "suffix_gib": 40,
                     "peak_gib": 130}


# -- byte identity ---------------------------------------------------------


def test_undeclared_plan_is_byte_identical() -> None:
    first = _build([_whole("a", 0, 10 * GIB, 20 * GIB, "z0"),
                    _whole("b", 10 * GIB, 20 * GIB, 20 * GIB, "z1")])
    second = _build([_whole("a", 0, 10 * GIB, 20 * GIB, "z0"),
                     _whole("b", 10 * GIB, 20 * GIB, 20 * GIB, "z1")])
    for plan in (first, second):
        for phase in plan["phases"]:
            assert "resident_before_launch" not in phase
    canonical = pb._canonical_bytes(residency_plan.validate_plan(first))
    assert b"resident_before_launch" not in canonical
    assert residency_plan.plan_sha256(first) == hashlib.sha256(
        canonical).hexdigest()
    assert residency_plan.plan_sha256(second) == residency_plan.plan_sha256(
        first)
    assert pb._canonical_bytes(
        residency_plan.validate_plan(second)) == canonical


# -- submission refusal (pure terms) ---------------------------------------


def _minted(tier_record, tier_id=TIER):
    return storage_tiers.tier_tokens(tier_record).get(
        storage_tiers.capacity_kind_of(tier_id))


def _submission_refusal(prelaunch_names, stage_cuts, tier, tier_id=TIER):
    """Repeat the pbrun refusal in pure terms.  Compare the cut-based peak
    with the minted tier capacity."""
    def _cut_gib(cuts):
        return [sum(storage_tiers.stage_tokens_for_bytes(cend - cstart)
                    for cstart, cend in chunks) for chunks in cuts]
    bound = residency_plan.prelaunch_peak_gib(
        _cut_gib(stage_cuts[:len(prelaunch_names)]),
        _cut_gib(stage_cuts[len(prelaunch_names):]))
    minted = _minted(tier, tier_id)
    if minted is not None and bound["peak_gib"] > minted:
        return (f"prelaunch prefix {prelaunch_names} needs peak "
                f"{bound['peak_gib']} GiB (retained "
                f"{bound['retained_gib']} GiB + suffix "
                f"{bound['suffix_gib']} GiB) on stage tier {tier_id}, "
                f"above the tier's minted capacity of {minted} GiB")
    return None


def test_submission_refuses_peak_above_capacity() -> None:
    tier = {"tier": "stage", "capacity_bytes": 210 * GIB}
    assert _minted(tier) == 210
    cuts = [[(0, 160 * GIB)], [(160 * GIB, 200 * GIB)],
            [(200 * GIB, 240 * GIB)]]
    refusal = _submission_refusal(["phase-0"], cuts, tier)
    assert refusal is not None
    assert "240" in refusal and "210" in refusal


def test_submission_passes_peak_equal_to_capacity() -> None:
    tier = {"tier": "stage", "capacity_bytes": 240 * GIB}
    cuts = [[(0, 160 * GIB)], [(160 * GIB, 200 * GIB)],
            [(200 * GIB, 240 * GIB)]]
    assert _submission_refusal(["phase-0"], cuts, tier) is None


def test_submission_never_refuses_unknown_capacity() -> None:
    cuts = [[(0, 160 * GIB)], [(160 * GIB, 200 * GIB)],
            [(200 * GIB, 240 * GIB)]]
    assert _submission_refusal(["phase-0"], cuts, {}) is None


def test_submission_peak_matches_the_sealed_plan_bound() -> None:
    # The pre-seal refusal and the sealed bound read the same demands.
    tier = {"tier": "stage", "capacity_bytes": 210 * GIB}
    ranges = [(0, 160 * GIB), (160 * GIB, 200 * GIB),
              (200 * GIB, 240 * GIB)]
    cuts = [[span] for span in ranges]
    total = 240 * GIB
    phases = [_whole(f"phase-{i}", s, e, total, f"s{i}",
                     declared=(i == 0))
              for i, (s, e) in enumerate(ranges)]
    plan = _build(phases)
    assert residency_plan.prelaunch_bound(plan) == (
        residency_plan.prelaunch_peak_gib([160], [40, 40]))
    assert _submission_refusal(["phase-0"], cuts, tier) is not None


# -- gang demand -----------------------------------------------------------


def _member(sizes, declared, digest, consumer_seed, tier=TIER):
    total = sum(sizes) * GIB
    phases = []
    start = 0
    for index, gib in enumerate(sizes):
        end = start + gib * GIB
        phases.append(_whole(f"phase-{index}", start, end, total,
                             f"{consumer_seed}{index}",
                             declared=index < declared))
        start = end
    return residency_plan.build_plan(
        consumer_action_key=_hexkey(consumer_seed), tier_id=tier,
        stage_root="/stage", manifest_sha256=digest,
        manifest_bytes=total, phases=phases)


def test_gang_shared_ranges_count_once() -> None:
    first = _member([90, 40, 40], 1, "a" * 64, "g0")
    second = _member([90, 40, 40], 1, "a" * 64, "g1")
    demand = residency_plan.gang_prelaunch_demand(
        [first, second], {TIER: 210})
    assert demand[TIER]["peak_gib"] == 170
    assert demand[TIER]["retained_gib"] == 90
    assert demand[TIER]["over_capacity"] is False


def test_gang_disjoint_ranges_sum() -> None:
    first = _member([90, 40, 40], 1, "a" * 64, "h0")
    second = _member([90, 40, 40], 1, "b" * 64, "h1")
    demand = residency_plan.gang_prelaunch_demand(
        [first, second], {TIER: 210})
    assert demand[TIER]["retained_gib"] == 180
    assert demand[TIER]["peak_gib"] == 170 + 170
    assert demand[TIER]["over_capacity"] is True


def test_gang_no_declaration_never_refuses() -> None:
    first = _member([90, 40, 40], 0, "a" * 64, "n0")
    second = _member([90, 40, 40], 0, "a" * 64, "n1")
    assert residency_plan.gang_prelaunch_demand(
        [first, second], {TIER: 1}) == {}


def test_gang_unknown_capacity_never_refuses() -> None:
    first = _member([160, 40, 40], 1, "a" * 64, "u0")
    second = _member([160, 40, 40], 1, "b" * 64, "u1")
    demand = residency_plan.gang_prelaunch_demand([first, second], {})
    assert demand[TIER]["peak_gib"] == 240 + 240
    assert demand[TIER]["capacity_gib"] is None
    assert demand[TIER]["over_capacity"] is False
