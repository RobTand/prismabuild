"""#1252 review item 3: the planner's phase cost on a realistic READY set.

The tier loop's cycle time has a history, so the planner's share of it is
measured, not asserted: ~190 READY rows (the 2026-09-27 G2 backlog shape),
the first 64 in claim order plain (no manifest -- the examination bound
pays its request reads there), manifest rows behind them.  The first cycle
pays the reads; the second answers from the memo.  Timings are printed for
the PR; the assertions are correctness only.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tests"))
from prewarm_fixture import Fleet, phase_table  # noqa: E402
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import manifest_promotion  # noqa: E402

from test_manifest_rows_gain_residency_plans import (  # noqa: E402
    NAMED_FILES, _manifest_row, _ready_item, _stage_tier)

HOST = "dl380g10"


def test_the_planner_phase_cost_on_a_ready_backlog(tmp_path):
    fleet = Fleet(tmp_path)
    files = [fleet.file(name, size) for name, size in NAMED_FILES]
    annotations = {"phases": phase_table(NAMED_FILES)}
    plain: list[str] = [
        fleet.action(f"bulk-{index:03d}", files, with_manifest=False)
        for index in range(184)]
    manifest_rows = [
        _manifest_row(fleet, f"mrow-{index:03d}", annotations=annotations,
                      files=[fleet.file(f"m{index}-{name}", size)
                             for name, size in NAMED_FILES])
        for index in range(6)]
    ready = [_ready_item(fleet, key) for key in plain + manifest_rows]
    tier = _stage_tier(fleet)

    started = time.monotonic()
    first = manifest_promotion.promote_ready_manifest_rows(
        fleet.queue, fleet.cas_root, tier, ready=ready)
    cold_s = time.monotonic() - started
    started = time.monotonic()
    second = manifest_promotion.promote_ready_manifest_rows(
        fleet.queue, fleet.cas_root, tier, ready=ready)
    warm_s = time.monotonic() - started

    # Correctness only: one row planned (the first manifest row), the plain
    # rows passed over, and the memo answers identically the second time.
    assert first[-1]["outcome"] == "planned", first[-1]
    assert {outcome["outcome"] for outcome in first[:-1]} == {"no_manifest"}
    assert second == first
    print(f"MANIFEST_PROMOTION_PHASE ready_rows={len(ready)} "
          f"cold_cycle_s={cold_s:.3f} warm_cycle_s={warm_s:.3f}",
          flush=True)
