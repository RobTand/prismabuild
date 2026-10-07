"""Declared prefix needs: suffix-only gate view plus the prefix keys (#1594).

Each test seals its plan first.  Every number below comes from that plan.
"""
from __future__ import annotations

import hashlib
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import pool, residency_plan, storage_tiers  # noqa: E402
from prismabuild import window_credit  # noqa: E402

TIER = "prismabuild-stage:dl380g10"
STAGE_KIND = f"stage_gib@{TIER}"
DIGEST = "9" * 64
GIB = storage_tiers.GIB
CONSUMER = "c" * 64
GROUP = "prelaunch-0123456789abcdef-0123456789ab-0123456789ab"


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


def _build(phases, consumer=CONSUMER):
    total = phases[-1]["end_bytes"]
    return residency_plan.build_plan(
        consumer_action_key=consumer, tier_id=TIER, stage_root="/stage",
        manifest_sha256=DIGEST, manifest_bytes=total, phases=phases)


def _prefix_plan():
    """Declared 90 GiB whole phase ahead of two 40 GiB streaming phases."""
    return _build([_whole("d0", 0, 90 * GIB, 170 * GIB, "n0d0",
                          declared=True),
                   _whole("s1", 90 * GIB, 130 * GIB, 170 * GIB, "n0s1"),
                   _whole("s2", 130 * GIB, 170 * GIB, 170 * GIB, "n0s2")])


def _declared_only_plan():
    """One declared 90 GiB whole phase and no streaming phase."""
    return _build([_whole("d0", 0, 90 * GIB, 90 * GIB, "n1d0",
                          declared=True)])


def _stream_plan():
    """Two undeclared 40 GiB phases."""
    return _build([_whole("s1", 0, 40 * GIB, 80 * GIB, "n2s1"),
                   _whole("s2", 40 * GIB, 80 * GIB, 80 * GIB, "n2s2")])


def _mover(plan, ordinal):
    return str(plan["phases"][ordinal]["mover_row"]["action_key"])


def _egress(plan, ordinal):
    return str(plan["phases"][ordinal]["egress_row"]["action_key"])


def test_declared_needs_cover_suffix_only_plus_prefix_keys() -> None:
    """Waiting and fence read the suffix; two keys carry the prefix (R1)."""
    plan = _prefix_plan()
    out = residency_plan.advance_needs(plan, None)
    assert [entry["phase"] for entry in out["waiting"]] == ["s1", "s2"]
    assert out["current_min_gib"] == 40
    assert out["next_min_gib"] == 40
    assert out["final"] is False
    assert out["reading_phase"] == "d0"
    # ``next`` is the second waiting leg, as for any plan: the lead is the
    # first.  The suffix reads exactly as an undeclared two-phase plan does.
    assert out["next_phase"] == "s2"
    assert out["next_mover_action_key"] == _mover(plan, 2)
    assert out["lead_mover_action_key"] == _mover(plan, 1)
    assert out["fence_target"] == {
        "phase": "s2", "mover_action_key": _mover(plan, 2),
        "egress_action_key": _egress(plan, 2), "stage_gib": 40,
        "chunk_index": None, "start_bytes": 130 * GIB,
        "end_bytes": 170 * GIB}
    assert out["fence_prior"] == []
    assert out["prior"] == []
    assert out["prelaunch_legs"] == [{
        "phase": "d0", "mover_action_key": _mover(plan, 0),
        "egress_action_key": _egress(plan, 0), "stage_gib": 90,
        "chunk_index": None, "start_bytes": 0, "end_bytes": 90 * GIB}]
    assert out["prelaunch_gib"] == 90


def test_declared_needs_keep_streaming_lead_after_publish() -> None:
    """The lead stays the first streaming leg, published or not."""
    plan = _prefix_plan()
    out = residency_plan.advance_needs(plan, None,
                                       published=[_mover(plan, 1)])
    assert [entry["phase"] for entry in out["waiting"]] == ["s2"]
    assert out["current_min_gib"] == 40
    assert out["next_min_gib"] is None
    assert out["lead_mover_action_key"] == _mover(plan, 1)
    assert out["prelaunch_gib"] == 90
    assert [entry["phase"] for entry in out["prelaunch_legs"]] == ["d0"]


def test_declared_plan_without_suffix_uses_idle_shape_plus_keys() -> None:
    """No streaming phase gives the idle shape with the two keys."""
    declared_out = residency_plan.advance_needs(_declared_only_plan(), None)
    plain_out = residency_plan.advance_needs(_stream_plan(), None,
                                             published=[_mover(
                                                 _stream_plan(), 0),
                                                 _mover(_stream_plan(), 1)])
    assert set(declared_out) == set(plain_out) | {
        "prelaunch_legs", "prelaunch_gib"}
    assert declared_out["waiting"] == []
    assert declared_out["final"] is True
    assert declared_out["current_min_gib"] == 0
    assert declared_out["lead_mover_action_key"] is None
    assert declared_out["fence_target"] is None
    assert declared_out["prelaunch_gib"] == 90
    assert [entry["phase"] for entry in
            declared_out["prelaunch_legs"]] == ["d0"]


def test_undeclared_needs_carry_no_prelaunch_keys() -> None:
    """A plan with no declaration keeps the old shape exactly."""
    out = residency_plan.advance_needs(_stream_plan(), None)
    assert "prelaunch_legs" not in out
    assert "prelaunch_gib" not in out
    assert out["current_min_gib"] == 40
    assert out["next_min_gib"] == 40
    assert out["lead_mover_action_key"] == _mover(_stream_plan(), 0)


def test_prelaunch_owned_gib_counts_holder_and_movers() -> None:
    """Owned sums the group holder and stage movers for one kind (R3''')."""
    plan = _prefix_plan()
    held = {GROUP: {STAGE_KIND: 80},
            _mover(plan, 0): {STAGE_KIND: 10, "mem_gb": 3},
            _mover(plan, 1): {STAGE_KIND: 5},
            "stranger": {STAGE_KIND: 100}}
    assert residency_plan.prelaunch_owned_gib(
        plan, held, GROUP, STAGE_KIND) == 95
    assert residency_plan.prelaunch_owned_gib(plan, {}, GROUP,
                                              STAGE_KIND) == 0
    assert residency_plan.prelaunch_owned_gib(plan, held, "absent",
                                              STAGE_KIND) == 15


def test_prelaunch_obligation_gib_is_peak_minus_owned() -> None:
    """The admitted window obliges its peak less what it owns (R3''')."""
    assert window_credit.prelaunch_obligation_gib(170, 92) == 78
    assert window_credit.prelaunch_obligation_gib(90, 90) == 0
    assert window_credit.prelaunch_obligation_gib(90, 120) == 0
    with pytest.raises(ValueError):
        window_credit.prelaunch_obligation_gib(-1, 0)
    with pytest.raises(ValueError):
        window_credit.prelaunch_obligation_gib(0, -5)


def test_prelaunch_footprint_gib_is_the_newcomer_remainder() -> None:
    """A newcomer reserves its peak less what it already owns (R1'')."""
    assert window_credit.prelaunch_footprint_gib(170, 92) == 78
    assert window_credit.prelaunch_footprint_gib(90, 90) == 0
    assert window_credit.prelaunch_footprint_gib(90, 120) == 0
    with pytest.raises(ValueError):
        window_credit.prelaunch_footprint_gib(-1, 0)
    with pytest.raises(ValueError):
        window_credit.prelaunch_footprint_gib(0, -5)


def test_prelaunch_wait_reason_names_the_room_wait() -> None:
    """The tier loop files this reason on the window stall."""
    assert (window_credit.REASON_PRELAUNCH_WAIT
            == "prelaunch_waiting_for_room")
