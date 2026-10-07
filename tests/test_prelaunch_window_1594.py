"""The prelaunch window: held prefix publishes, unheld prefix waits (#1594).

Each test seals its plan first.  Every number below comes from that plan.
"""
from __future__ import annotations

import hashlib
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import pool, residency_plan, storage_tiers  # noqa: E402

TIER = "prismabuild-stage:dl380g10"
STAGE_KIND = f"stage_gib@{TIER}"
DIGEST = "9" * 64
GIB = storage_tiers.GIB
CONSUMER = "c" * 64


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


def _build(phases, consumer=CONSUMER):
    total = phases[-1]["end_bytes"]
    return residency_plan.build_plan(
        consumer_action_key=consumer, tier_id=TIER, stage_root="/stage",
        manifest_sha256=DIGEST, manifest_bytes=total, phases=phases)


def _prefix_plan():
    """Declared 90 GiB whole phase ahead of two 40 GiB streaming phases."""
    return _build([_whole("d0", 0, 90 * GIB, 170 * GIB, "w0d0",
                          declared=True),
                   _whole("s1", 90 * GIB, 130 * GIB, 170 * GIB, "w0s1"),
                   _whole("s2", 130 * GIB, 170 * GIB, 170 * GIB, "w0s2")])


def _chunked_plan():
    """Declared 2x45 GiB chunked phase ahead of one 40 GiB streaming phase."""
    return _build([_chunked_at("d0", [(0, 45 * GIB), (45 * GIB, 90 * GIB)],
                               130 * GIB, "w1d0", declared=True),
                   _whole("s1", 90 * GIB, 130 * GIB, 130 * GIB, "w1s1")])


def _prefix_only_plan():
    """Declared 2x45 GiB chunked phase and no streaming phase."""
    return _build([_chunked_at("d0", [(0, 45 * GIB), (45 * GIB, 90 * GIB)],
                               90 * GIB, "w2d0", declared=True)])


def _multi_prefix_plan():
    """Two declared phases (90, 40 GiB) ahead of two 40 GiB streaming phases."""
    return _build([_whole("d0", 0, 90 * GIB, 210 * GIB, "w3d0",
                          declared=True),
                   _whole("d1", 90 * GIB, 130 * GIB, 210 * GIB, "w3d1",
                          declared=True),
                   _whole("s1", 130 * GIB, 170 * GIB, 210 * GIB, "w3s1"),
                   _whole("s2", 170 * GIB, 210 * GIB, 210 * GIB, "w3s2")])


def _stream_plan():
    """Undeclared analog of the prefix plan's suffix, with the same keys."""
    return _build([_whole("s1", 0, 40 * GIB, 80 * GIB, "w0s1"),
                   _whole("s2", 40 * GIB, 80 * GIB, 80 * GIB, "w0s2")])


def _mover(plan, ordinal):
    return str(plan["phases"][ordinal]["mover_row"]["action_key"])


def _triples(published):
    return [(row["phase"], row["mover_action_key"], row["stage_gib"])
            for row in published]


def test_unheld_whole_prefix_publishes_nothing_and_stalls() -> None:
    """No group means no declared leg and a prelaunch stall (R1')."""
    plan = _prefix_plan()
    out = residency_plan.window(plan, accepted_phase=None, free_gib=200,
                                capacity_gib=210)
    assert out["publish"] == []
    assert out["evict"] == []
    assert out["stall"] == {
        "consumer_action_key": CONSUMER, "tier_id": TIER,
        "accepted_phase": None, "reading_phase": "d0",
        "blocked_phase": "d0", "blocked_gib": 90,
        "runahead_gib": 0, "runahead_budget_gib": 40,
        "free_gib": 200, "capacity_gib": 210,
        "reason": "prelaunch_waiting_for_room",
        "waiting_for": "the prelaunch group reservation"}


def test_unheld_chunked_prefix_blocks_on_first_phase_with_full_sum() -> None:
    """The stall names the first pending declared phase and all its bytes."""
    plan = _chunked_plan()
    args: dict = {"accepted_phase": None, "free_gib": 200,
                  "capacity_gib": 210}
    none_out = residency_plan.window(plan, **args)
    false_out = residency_plan.window(plan, prelaunch_held=False, **args)
    assert none_out == false_out
    assert none_out["publish"] == []
    stall = none_out["stall"]
    assert stall["reason"] == "prelaunch_waiting_for_room"
    assert stall["blocked_phase"] == "d0"
    assert stall["blocked_gib"] == 90
    assert stall["waiting_for"] == "the prelaunch group reservation"
    assert "chunk_index" not in stall


def test_held_prefix_only_publishes_every_leg_without_room() -> None:
    """Held declared legs publish in read order with 1 GiB free (R1')."""
    plan = _prefix_only_plan()
    out = residency_plan.window(plan, accepted_phase=None, free_gib=1,
                                capacity_gib=210, prelaunch_held=True)
    assert [row["chunk_index"] for row in out["publish"]] == [0, 1]
    assert [row["stage_gib"] for row in out["publish"]] == [45, 45]
    assert [row["mover_action_key"] for row in out["publish"]] == [
        _hexkey("w2d0mover0"), _hexkey("w2d0mover1")]
    assert out["stall"] is None


def test_held_prefix_skips_published_and_withdrawn_legs() -> None:
    """A held leg publishes once; a withdrawn leg never publishes."""
    plan = _prefix_only_plan()
    first = _hexkey("w2d0mover0")
    published_out = residency_plan.window(
        plan, accepted_phase=None, free_gib=1000, capacity_gib=210,
        published=[first], prelaunch_held=True)
    assert [row["mover_action_key"] for row in published_out["publish"]] == [
        _hexkey("w2d0mover1")]
    withdrawn_out = residency_plan.window(
        plan, accepted_phase=None, free_gib=1000, capacity_gib=210,
        withdrawn=[first], prelaunch_held=True)
    assert [row["mover_action_key"] for row in withdrawn_out["publish"]] == [
        _hexkey("w2d0mover1")]


def test_passed_prefix_never_evicts_while_streaming_does() -> None:
    """Progress past the prefix evicts the passed streaming phase only (R2)."""
    plan = _prefix_plan()
    out = residency_plan.window(
        plan, accepted_phase="s2", free_gib=1000, capacity_gib=210,
        staged=[_mover(plan, 0), _mover(plan, 1), _mover(plan, 2)],
        prelaunch_held=True)
    assert [entry["phase"] for entry in out["evict"]] == ["s1"]
    assert [entry["mover_action_key"] for entry in out["evict"]] == [
        _mover(plan, 1)]
    assert [row["phase"] for row in out["publish"]] == ["s2"]
    assert out["stall"] is None


def test_published_declared_legs_do_not_count_as_runahead() -> None:
    """Held prefix legs stay outside the run-ahead budget (R1')."""
    plan = _multi_prefix_plan()
    out = residency_plan.window(
        plan, accepted_phase=None, free_gib=1000, capacity_gib=1000,
        runahead_cap_gib=1, published=[_mover(plan, 0), _mover(plan, 1)],
        prelaunch_held=True)
    assert [row["phase"] for row in out["publish"]] == ["s1"]
    assert out["stall"]["reason"] == "no_accepted_progress"
    assert out["stall"]["blocked_phase"] == "s2"
    assert out["stall"]["runahead_gib"] == 0
    assert out["stall"]["runahead_budget_gib"] == 1


def test_horizon_cuts_streaming_but_not_declared_legs() -> None:
    """The horizon stops the far streaming leg with no stall (R1')."""
    plan = _prefix_plan()
    out = residency_plan.window(
        plan, accepted_phase=None, free_gib=1000, capacity_gib=1000,
        horizon_end_bytes=130 * GIB, prelaunch_held=True)
    assert [row["phase"] for row in out["publish"]] == ["d0", "s1"]
    assert out["stall"] is None


def test_first_streaming_phase_matches_the_undeclared_plan() -> None:
    """The suffix follows today's rules with the prefix held (R1')."""
    held_args: dict = {"accepted_phase": None, "free_gib": 1000,
                       "capacity_gib": 1000, "runahead_cap_gib": 30}
    declared_out = residency_plan.window(
        _prefix_plan(), prelaunch_held=True, **held_args)
    plain_out = residency_plan.window(_stream_plan(), **held_args)
    assert _triples(declared_out["publish"])[1:] == _triples(
        plain_out["publish"])
    assert [row["phase"] for row in declared_out["publish"]] == ["d0", "s1"]
    for field in ("reason", "blocked_phase", "blocked_gib", "runahead_gib",
                  "runahead_budget_gib"):
        assert declared_out["stall"][field] == plain_out["stall"][field]
    assert declared_out["stall"]["reason"] == "no_accepted_progress"
    assert declared_out["stall"]["reading_phase"] == "d0"
    assert plain_out["stall"]["reading_phase"] == "s1"


def test_undeclared_plan_ignores_prelaunch_held() -> None:
    """Every held value gives the same output as no argument (R5)."""
    plan = _stream_plan()
    for accepted in (None, "s1"):
        args: dict = {"accepted_phase": accepted, "free_gib": 1000,
                      "capacity_gib": 1000}
        base = residency_plan.window(plan, **args)
        assert residency_plan.window(plan, prelaunch_held=None, **args) == base
        assert residency_plan.window(plan, prelaunch_held=True, **args) == base
        assert residency_plan.window(plan, prelaunch_held=False,
                                     **args) == base


def test_unheld_suffix_waits_while_unheld_prefix_blocks() -> None:
    """Without the group even roomy streaming legs publish nothing (R1')."""
    plan = _prefix_plan()
    out = residency_plan.window(plan, accepted_phase=None, free_gib=1000,
                                capacity_gib=1000, prelaunch_held=False)
    assert out["publish"] == []
    assert out["stall"]["reason"] == "prelaunch_waiting_for_room"


def test_fully_published_prefix_releases_the_suffix() -> None:
    """No pending declared leg means no prelaunch stall for them."""
    plan = _prefix_plan()
    out = residency_plan.window(
        plan, accepted_phase=None, free_gib=1000, capacity_gib=1000,
        runahead_cap_gib=30, published=[_mover(plan, 0)])
    assert [row["phase"] for row in out["publish"]] == ["s1"]
    assert out["stall"]["reason"] == "no_accepted_progress"
    assert out["stall"]["blocked_phase"] == "s2"
