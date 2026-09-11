"""A plan that ignores its own budget is not a plan (issue #487).

``cycle`` subtracted ``result["bytes_warmed"]`` from the remaining budget
after every row.  A dry run reads nothing, so that number is 0 and the budget
never moved: with ``--lookahead 16`` the loop reported it would warm all
sixteen ready rows -- 1023 GB -- against a 206 GB budget, and the only row
whose refusal was honest was one no read would have fitted anyway.

The number a dry run must charge is the number it would have read.
"""
from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
from prewarm_fixture import Fleet  # noqa: E402

#: Two of these fit in three: the second row is exactly what the first row's
#: charge must push out of the budget.
ROW = 8 << 20


def test_the_second_row_is_refused_for_the_headroom_the_first_would_spend(
    tmp_path: Path,
) -> None:
    fleet = Fleet(tmp_path)
    first = fleet.action("first", [fleet.file("first.bin", ROW)], priority=2)
    second = fleet.action("second", [fleet.file("second.bin", ROW)], priority=1)
    args = fleet.args(
        dry_run=True, lookahead=2,
        # Budget = c_max * reserve, and nothing is claimed, so this is one and
        # a half rows: room for the first row and not for both.
        arcstats=fleet.arcstats(size=0, c=1 << 40, c_max=ROW * 3 // 2))

    event = fleet.cycle(args)

    assert [w["action_key"] for w in event["warmed"]] == [first]
    assert event["warmed"][0]["status"] == "dry-run"
    refused = [s for s in event["skipped"] if s["action_key"] == second]
    assert refused and refused[0]["reason"] == "headroom", event
    assert refused[0]["budget"] == event["budget_after_reserve"] - ROW
