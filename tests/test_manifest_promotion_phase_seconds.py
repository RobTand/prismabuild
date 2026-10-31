"""Cold and warm planner cycles pass plain rows and advance manifest rows."""
from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tests"))
from prewarm_fixture import Fleet, phase_table  # noqa: E402
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
from prismabuild import manifest_promotion  # noqa: E402

from test_manifest_rows_gain_residency_plans import (  # noqa: E402
    NAMED_FILES, _manifest_row, _ready_item, _stage_tier)


def test_cold_and_warm_cycles_plan_the_next_manifest_row(tmp_path):
    fleet = Fleet(tmp_path)
    files = [fleet.file(name, size) for name, size in NAMED_FILES]
    annotations = {"phases": phase_table(NAMED_FILES)}
    plain = fleet.action("plain", files, with_manifest=False)
    manifest_rows = [
        _manifest_row(fleet, f"mrow-{index:03d}", annotations=annotations,
                      files=[fleet.file(f"m{index}-{name}", size)
                             for name, size in NAMED_FILES])
        for index in range(2)]
    ordered = [plain, *manifest_rows]
    ready = [_ready_item(fleet, key) for key in ordered]
    tier = _stage_tier(fleet)

    first = manifest_promotion.promote_ready_manifest_rows(
        fleet.queue, fleet.cas_root, tier, ready=ready)
    second = manifest_promotion.promote_ready_manifest_rows(
        fleet.queue, fleet.cas_root, tier, ready=ready)
    assert [outcome["outcome"] for outcome in first] == ["no_manifest", "planned"]
    assert first[-1]["action_key"] == manifest_rows[0]
    assert [outcome["outcome"] for outcome in second] == ["no_manifest", "stands_down", "planned"]
    assert second[-1]["action_key"] == manifest_rows[1]
