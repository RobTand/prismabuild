"""The #1041 backfill tool: dry run by default, --apply archives."""

from __future__ import annotations

from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

from prismabuild import pool  # noqa: E402
import pbrun  # noqa: E402
import retire_residency_plans as tool  # noqa: E402
import test_pbrun_residency_stage_submission as submission  # noqa: E402
from test_pbrun_residency_stage_submission import _detach_key  # noqa: E402


def test_dry_run_changes_nothing_and_apply_archives_a_concluded_plan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys,
) -> None:
    prepared = submission._prepare(tmp_path, monkeypatch)
    queue = prepared["queue"]
    assert pbrun.main() == 0
    key = _detach_key(capsys)
    # A live (ready) consumer is not a candidate.
    assert tool.candidates(queue) == []
    # Conclude it the old way: the terminal file appears with no plan retirement.
    queue.item_path(pool.READY, key).unlink()
    assert tool.candidates(queue) == [key]

    assert tool.retire_concluded_plans(queue, apply=False)["would_retire"] == [key]
    assert queue.residency_plan_path(key).exists()

    assert tool.retire_concluded_plans(queue, apply=True)["retired"] == [key]
    assert not queue.residency_plan_path(key).exists()
    assert tool.candidates(queue) == []
