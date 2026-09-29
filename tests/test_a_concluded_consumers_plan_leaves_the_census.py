"""A concluded consumer's residency plan leaves the census (#1041).

Nothing removed a frozen plan when its consumer finished, so 324 of 330 plans
on the live queue described no work and every census reader walked them.
``PoolQueue.finish`` now retires the plan at the consumer's terminal
transition, through ``residency_plan.reap`` -- which archives the body only
when no child mover or egress row is still queued or claimed, so the dead-
consumer pass that needs the plan to reap those children keeps it.
"""

from __future__ import annotations

from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

from prismabuild import pool, residency_plan  # noqa: E402
import pbrun  # noqa: E402
import test_pbrun_residency_stage_submission as submission  # noqa: E402
from test_pbrun_residency_stage_submission import _detach_key  # noqa: E402


def _archived(queue: pool.PoolQueue, key: str) -> list[Path]:
    directory = queue.residency_plan_path(key).parent / residency_plan.SUPERSEDED
    if not directory.is_dir():
        return []
    return [path for path in directory.iterdir()
            if path.name.startswith(key) and path.suffix == ".json"
            and not path.name.endswith((".superseded.json", ".marker.json"))]


def test_finishing_a_consumer_with_no_live_children_retires_its_plan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys,
) -> None:
    prepared = submission._prepare(tmp_path, monkeypatch)
    queue = prepared["queue"]
    assert pbrun.main() == 0
    key = _detach_key(capsys)
    assert queue.residency_plan_path(key).exists()

    submission._claim(queue, key)
    queue.finish(key, status="executed")

    assert not queue.residency_plan_path(key).exists(), (
        "the concluded consumer's plan is still in the census")
    assert residency_plan.read(queue, key) is None
    assert _archived(queue, key), "the plan body was deleted, not archived"


def test_a_plan_whose_children_are_still_queued_stays_filed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys,
) -> None:
    prepared = submission._prepare(tmp_path, monkeypatch)
    queue = prepared["queue"]
    assert pbrun.main() == 0
    key = _detach_key(capsys)
    submission._tier_cycle(queue, tmp_path / "stage")
    plan = residency_plan.read(queue, key)
    lead = str(plan["phases"][0]["mover_row"]["action_key"])
    assert queue.item_path(pool.READY, lead).exists()

    submission._claim(queue, key)
    queue.finish(key, status="executed")

    assert queue.residency_plan_path(key).exists(), (
        "a queued child still needs the plan to be reaped")
    assert not _archived(queue, key)
