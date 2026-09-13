"""A lookahead of more than one is bounded by the budget, not by the count.

``--lookahead N`` says how many rows ahead the loop may hold resident; the
budget says whether they fit.  Both have to be true at once, and the second is
the one that keeps the warm honest: two rows warmed by evicting each other
arrive as two cold rows.

The rule this file pins is what happens to the rows *behind* one that does not
fit.  A big row at the head of the queue must not disable prewarm for the rows
behind it that do fit, because the queue's order is the coordinator's and the
loop does not get to stop at the first refusal.
"""
from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
from prewarm_fixture import Fleet  # noqa: E402


def test_two_fitting_rows_are_warmed_in_the_queues_own_order(
        tmp_path: Path) -> None:
    fleet = Fleet(tmp_path)
    first = fleet.action("first", [fleet.file("first.pt", 4096)], priority=10)
    second = fleet.action("second", [fleet.file("second.pt", 4096)], priority=5)
    third = fleet.action("third", [fleet.file("third.pt", 4096)], priority=1)
    stats = fleet.arcstats(size=0, c=12288, c_max=12288)

    event = fleet.cycle(fleet.args(arcstats=stats, lookahead=2))

    assert [row["action_key"] for row in event["warmed"]] == [first, second]
    assert fleet.queue.prewarm(third) is None, (
        "the lookahead is two rows, and the budget still had room for a third")


def test_a_row_that_does_not_fit_does_not_block_the_rows_behind_it(
        tmp_path: Path) -> None:
    """The big row is refused; the small ones behind it are still warmed.

    A refusal for headroom is a statement about one manifest against today's
    budget, not about the queue.  Spending the lookahead on it would let one
    oversized submission turn prewarm off for everything behind it -- the same
    defect a trivial row at the head of the queue used to cause.
    """

    fleet = Fleet(tmp_path)
    small = fleet.action("small", [fleet.file("small.pt", 4096)], priority=10)
    big = fleet.action("big", [fleet.file("big.pt", 1 << 20)], priority=5)
    behind = fleet.action("behind", [fleet.file("behind.pt", 4096)], priority=1)
    stats = fleet.arcstats(size=0, c=12288, c_max=12288)

    event = fleet.cycle(fleet.args(arcstats=stats, lookahead=2))

    assert [row["action_key"] for row in event["warmed"]] == [small, behind]
    refused = [row for row in event["skipped"] if row["reason"] == "headroom"]
    assert [row["action_key"] for row in refused] == [big]
    assert refused[0]["manifest_bytes"] == 1 << 20
    assert fleet.queue.prewarm(big) is None


def test_the_budget_stops_the_lookahead_before_the_count_does(
        tmp_path: Path) -> None:
    """Room for one row and a lookahead of three warms one row.

    The count is a ceiling on how far ahead the loop looks, never a promise
    about how much it holds: what it holds is the budget's answer.
    """

    fleet = Fleet(tmp_path)
    first = fleet.action("first", [fleet.file("first.pt", 6144)], priority=10)
    second = fleet.action("second", [fleet.file("second.pt", 6144)], priority=5)
    stats = fleet.arcstats(size=0, c=8192, c_max=8192)

    event = fleet.cycle(fleet.args(arcstats=stats, lookahead=3))

    assert [row["action_key"] for row in event["warmed"]] == [first]
    assert [row["reason"] for row in event["skipped"]] == ["headroom"]
    assert fleet.queue.prewarm(second) is None
