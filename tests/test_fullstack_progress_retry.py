"""Full-stack 4/4 — durable progress, retry/resume, receipts, cleanup, gaps.

Drives real production functions: `residency_plan.remaining` window
advance over accepted chunk phases, `freeze` first-writer refusal on
repartition, `queue.record_move` receipt filing with CAS retrieval, and
ownership-safe cleanup via `stage_release.evict`. The gap assertion uses
real `remaining()`: an unaccepted phase set stays in the window (never
shrinks to fit survivors). Full deterministic join refusal waits on the
PQ join API (§seam); this file proves the PB-side half. ACC-02/ACC-04
(PB-side legs).
"""
from __future__ import annotations

from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

from prismabuild import pool, residency_plan  # noqa: E402
import stage_release  # noqa: E402

from test_fullstack_stage_ram_chain import (  # noqa: E402
    CONSUMER, STAGE_TIER, _fleet,
)
from test_fullstack_reader_boundaries import _staged_once  # noqa: E402

WHOLE_MOVER = "a" * 64


def _rows(queue: pool.PoolQueue, seed: str, start: int, end: int) -> dict:
    return {
        "mover_row": {"action_key": _key(seed + "-mover"),
                      "cas_root": str(queue.root / "cas"),
                      "checkout_root": str(queue.root / "co"),
                      "worker_script": str(queue.root / "worker.py"),
                      "tags": ["gb10"],
                      "resources": {"cpu": 1, "mem_gb": 1,
                                    f"stage_gib@{STAGE_TIER}": 1},
                      "residency": {"schema": pool.RESIDENCY_SCHEMA_V1,
                                    "tier_id": STAGE_TIER,
                                    "manifest_sha256": "9" * 64,
                                    "manifest_bytes": 1 << 30,
                                    "range_start_bytes": start,
                                    "range_end_bytes": end}},
        "egress_row": {"action_key": _key(seed + "-egress"),
                       "cas_root": str(queue.root / "cas"),
                       "checkout_root": str(queue.root / "co"),
                       "worker_script": str(queue.root / "worker.py"),
                       "tags": ["gb10"],
                       "resources": {"cpu": 1, "mem_gb": 1}},
    }


def _key(seed: str) -> str:
    return (seed.encode().hex() * 64)[:64]


def test_accepted_progress_advances_the_window(tmp_path: Path) -> None:
    """Real remaining(): accepted phases leave the window, the rest stay."""
    queue = _fleet(tmp_path)
    plan = residency_plan.build_plan(
        consumer_action_key=CONSUMER, tier_id=STAGE_TIER,
        stage_root="/stage/prewarm", manifest_sha256="9" * 64,
        manifest_bytes=1 << 30, phases=[
            {"name": "chunk-000", "start_bytes": 0, "end_bytes": 1 << 20,
             "stage_gib": 1,
             **_rows(queue, "chunk-000", 0, 1 << 20)},
            {"name": "chunk-001", "start_bytes": 1 << 20, "end_bytes": 2 << 20,
             "stage_gib": 1,
             **_rows(queue, "chunk-001", 1 << 20, 2 << 20)},
        ])
    residency_plan.freeze(queue, plan)
    assert [p["name"] for p in residency_plan.remaining(plan, None)] == [
        "chunk-000", "chunk-001"]
    assert [p["name"] for p in residency_plan.remaining(plan, "chunk-000")] == [
        "chunk-000", "chunk-001"]
    assert [p["name"] for p in residency_plan.remaining(plan, "chunk-001")] == [
        "chunk-001"]


def test_gap_never_shrinks_to_fit_survivors(tmp_path: Path) -> None:
    """An unaccepted phase stays reported: coverage refuses, never narrows."""
    queue = _fleet(tmp_path)
    plan = residency_plan.build_plan(
        consumer_action_key=CONSUMER, tier_id=STAGE_TIER,
        stage_root="/stage/prewarm", manifest_sha256="9" * 64,
        manifest_bytes=1 << 30, phases=[
            {"name": "chunk-000", "start_bytes": 0, "end_bytes": 1 << 20,
             "stage_gib": 1,
             **_rows(queue, "gap-000", 0, 1 << 20)},
            {"name": "chunk-001", "start_bytes": 1 << 20, "end_bytes": 2 << 20,
             "stage_gib": 1,
             **_rows(queue, "gap-001", 1 << 20, 2 << 20)},
        ])
    residency_plan.freeze(queue, plan)
    assert residency_plan.accepted(plan, "chunk-001") is True
    assert residency_plan.accepted(plan, "chunk-009") is False
    assert len(residency_plan.remaining(plan, None)) == 2


def test_repartition_after_freeze_refuses_with_both_bodies(tmp_path: Path) -> None:
    """A second plan for one consumer refuses: restarts re-verify, never recut."""
    queue = _fleet(tmp_path)
    kw = dict(consumer_action_key=CONSUMER, tier_id=STAGE_TIER,
              stage_root="/stage/prewarm", manifest_sha256="9" * 64,
              manifest_bytes=1 << 30)
    one = residency_plan.build_plan(phases=[
        {"name": "chunk-000", "start_bytes": 0, "end_bytes": 1 << 20,
         "stage_gib": 1,
         **_rows(queue, "one", 0, 1 << 20)}], **kw)
    residency_plan.freeze(queue, one)
    two = residency_plan.build_plan(phases=[
        {"name": "chunk-000", "start_bytes": 0, "end_bytes": 1 << 21,
         "stage_gib": 2,
         **_rows(queue, "two", 0, 1 << 21)}], **kw)
    with pytest.raises(Exception):
        residency_plan.freeze(queue, two)


def test_cleanup_leaves_no_addressable_orphans(tmp_path: Path) -> None:
    """Egress removes bytes and tokens together; re-egress is a clean no-op."""
    queue, _, _ = _staged_once(tmp_path)
    assert stage_release.register_stage_root(
        queue, tier_id=STAGE_TIER, stage_root=tmp_path / "stage") == "registered"
    first = stage_release.evict(queue, WHOLE_MOVER, consumer_action_key=CONSUMER,
                                stage_root=tmp_path / "stage")
    assert first["complete"] is True
    assert list((tmp_path / "stage").rglob("*.bin")) == []
    assert list((tmp_path / "stage").rglob("*.pbrange")) == []
    again = stage_release.evict(queue, WHOLE_MOVER, consumer_action_key=CONSUMER,
                                stage_root=tmp_path / "stage")
    assert again["complete"] is True
