"""The prewarm loop warms what the queue says is next, and nothing else.

Rob's constraint on #487 was "no second dispatcher": claim order stays the
coordinator's.  So the loop must not have an opinion -- it reads the same
``ready_items()`` list a worker reads, in the same order (``-priority``,
``-passes``, ``published_unix``), and warms a prefix of it.  If it ever sorted
on its own, a box would arrive at a row whose data somebody else had decided
to warm, which is the failure mode this test exists to make loud.
"""
from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
from prewarm_fixture import Fleet  # noqa: E402


def test_the_head_of_the_queue_is_warmed_first(tmp_path: Path) -> None:
    fleet = Fleet(tmp_path)
    low = fleet.action("low", [fleet.file("low.pt", 4096)], priority=-10)
    high = fleet.action("high", [fleet.file("high.pt", 4096)], priority=10)
    middle = fleet.action("mid", [fleet.file("mid.pt", 4096)], priority=0)

    event = fleet.cycle(fleet.args(lookahead=1))
    assert [w["action_key"] for w in event["warmed"]] == [high]
    assert fleet.queue.prewarm(high)["status"] == "complete"
    # Not merely "not first": the other two were not touched at all.
    assert fleet.queue.prewarm(middle) is None
    assert fleet.queue.prewarm(low) is None


def test_a_denied_action_is_warmed_before_a_younger_one_in_its_band(
        tmp_path: Path) -> None:
    """Aging is the queue's, too.

    ``ready_items`` promotes an item that admission has denied, so the loop
    inherits that promotion for free -- which is the point of reading the
    queue's list rather than rebuilding it from priorities and timestamps.
    """

    fleet = Fleet(tmp_path)
    first = fleet.action("first", [fleet.file("first.pt", 4096)])
    second = fleet.action("second", [fleet.file("second.pt", 4096)])
    fleet.queue.record_pass(second)
    fleet.queue.record_pass(second)

    event = fleet.cycle(fleet.args(lookahead=1))
    assert [w["action_key"] for w in event["warmed"]] == [second]
    assert fleet.queue.prewarm(first) is None


def test_an_action_without_a_manifest_is_passed_over_not_counted(
        tmp_path: Path) -> None:
    """Lookahead counts actions the loop can act on.

    Most of the fleet's work carries no manifest.  If those consumed the
    lookahead budget, one ordinary submission at the head of the queue would
    silently disable prewarm for everything behind it.
    """

    fleet = Fleet(tmp_path)
    fleet.action("plain", [], priority=10, with_manifest=False)
    warmable = fleet.action("warmable", [fleet.file("w.pt", 4096)], priority=5)

    event = fleet.cycle(fleet.args(lookahead=1))
    assert [w["action_key"] for w in event["warmed"]] == [warmable]
