"""A row that is already resident must not cost the row behind it its window.

#523's 2026-09-12 readback of the live storage role: "From 18:50:04 through
the preceding polls, the loop kept skipping ``3580717d5514…`` as **already
warm**.  That single ready row consumed the configured one-row lookahead.  The
19:00:45 cycle first saw it claimed and selected row 0065."  Row 0065's warm
then started 4.3 s before its own claim and finished 245 s after it -- it ran
alongside the row's own reads instead of ahead of them.  The same event
recorded 142 262 053 176 bytes of budget left after reservations, so the row
behind the warm head fit; the delay was the lookahead rule, not headroom.

``--lookahead`` counts rows the loop can still do something about.  An
already-warm row is not one of them, exactly as a manifest below
``--min-manifest-bytes`` is not one of them.  What bounds how much the loop
holds resident is the budget: ``warmed_reserve`` charges every warmed,
unclaimed row's bytes against it, so advancing past a warm head cannot warm
more than the ARC ceiling leaves.
"""
from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
from prewarm_fixture import Fleet  # noqa: E402


def test_a_warm_head_does_not_stop_the_next_row_being_warmed(
        tmp_path: Path) -> None:
    fleet = Fleet(tmp_path)
    head = fleet.action("head", [fleet.file("head.pt", 1 << 16)], priority=10)
    following = fleet.action("next", [fleet.file("next.pt", 1 << 16)],
                             priority=5)

    first = fleet.cycle(fleet.args(lookahead=1))
    assert [w["action_key"] for w in first["warmed"]] == [head]
    warmed_at = fleet.queue.prewarm(head)["started_unix"]

    second = fleet.cycle(fleet.args(lookahead=1))
    assert [w["action_key"] for w in second["warmed"]] == [following]
    assert [(s["action_key"], s["reason"]) for s in second["skipped"]] == [
        (head, "already warm")]
    # The head keeps its record and its protection: it is skipped because it
    # is done, not re-read and not forgotten by the budget.
    assert fleet.queue.prewarm(head)["started_unix"] == warmed_at
    assert head in second["warmed_reserved_keys"]


def test_the_lookahead_depth_still_bounds_how_many_rows_are_warmed(
        tmp_path: Path) -> None:
    """Advancing past a warm row is not the same as ignoring the depth."""

    fleet = Fleet(tmp_path)
    head = fleet.action("head", [fleet.file("head.pt", 1 << 16)], priority=10)
    second = fleet.action("second", [fleet.file("second.pt", 1 << 16)],
                          priority=5)
    third = fleet.action("third", [fleet.file("third.pt", 1 << 16)],
                         priority=0)

    assert [w["action_key"]
            for w in fleet.cycle(fleet.args(lookahead=1))["warmed"]] == [head]

    one = fleet.cycle(fleet.args(lookahead=1))
    assert [w["action_key"] for w in one["warmed"]] == [second]
    assert fleet.queue.prewarm(third) is None

    two = fleet.cycle(fleet.args(lookahead=2))
    assert [w["action_key"] for w in two["warmed"]] == [third]
    assert fleet.queue.prewarm(third)["status"] == "complete"


def test_nothing_warm_yet_still_warms_exactly_the_head(tmp_path: Path) -> None:
    """The cold case is unchanged: one cycle, one row, the queue's own first."""

    fleet = Fleet(tmp_path)
    head = fleet.action("head", [fleet.file("head.pt", 1 << 16)], priority=10)
    following = fleet.action("next", [fleet.file("next.pt", 1 << 16)],
                             priority=5)

    event = fleet.cycle(fleet.args(lookahead=1))
    assert [w["action_key"] for w in event["warmed"]] == [head]
    assert event["skipped"] == []
    assert fleet.queue.prewarm(following) is None


def test_an_empty_ready_list_warms_nothing_and_skips_nothing(
        tmp_path: Path) -> None:
    fleet = Fleet(tmp_path)

    event = fleet.cycle(fleet.args(lookahead=1))
    assert event["ready"] == 0
    assert event["warmed"] == []
    assert event["skipped"] == []
