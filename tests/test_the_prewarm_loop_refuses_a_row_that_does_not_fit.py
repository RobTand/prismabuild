"""The budget is ARC capacity minus what is still wanted, not free space.

A cache in steady state is full.  Measured on dl380g10 at 2026-09-11 01:59
UTC, hours into the campaign: ARC ``size`` 254.05 GB against ``c_max``
257.70 GB -- 3.65 GB free, against rows of 64 GB.  A loop that budgeted on
free space would have refused every row forever while the cache sat full of
bytes nobody would read again, which is the exact failure prewarm exists to
fix.  Warming evicts, and it is meant to.  The question the budget answers is
not "is there room" but "is what I would displace still wanted", and the
answer is the protected set: actions claimed inside their load window, and
rows this loop already warmed that nobody has claimed yet.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parent))
from prewarm_fixture import Fleet  # noqa: E402


def test_a_manifest_larger_than_the_whole_budget_is_refused(
        tmp_path: Path) -> None:
    fleet = Fleet(tmp_path)
    key = fleet.action("big", [fleet.file("big.pt", 8192)])
    # A cache too small to hold the row at all, full or empty.
    stats = fleet.arcstats(size=0, c=4096, c_max=4096)

    event = fleet.cycle(fleet.args(arcstats=stats))
    assert [s["reason"] for s in event["skipped"]] == ["headroom"]
    assert event["skipped"][0]["manifest_bytes"] == 8192
    # Refusal is not a receipt: nothing was warmed, so nothing is recorded,
    # and a later poll with a larger budget is free to do the work.
    assert fleet.queue.prewarm(key) is None


def test_a_full_cache_does_not_stop_the_loop(tmp_path: Path) -> None:
    """The case the free-space budget got wrong, stated as a test.

    ``size`` sits one page below ``c_max``.  There is no free ARC at all, and
    the row is warmed anyway, because nothing in the cache is protected.
    """

    fleet = Fleet(tmp_path)
    key = fleet.action("row", [fleet.file("row.pt", 8192)])
    stats = fleet.arcstats(size=(1 << 20) - 4096, c=1 << 20, c_max=1 << 20)

    event = fleet.cycle(fleet.args(arcstats=stats))
    assert event["headroom_nominal"] == 4096
    assert [w["action_key"] for w in event["warmed"]] == [key]
    assert fleet.queue.prewarm(key)["status"] == "complete"


def test_a_recently_claimed_actions_bytes_are_subtracted_from_the_budget(
        tmp_path: Path) -> None:
    """A claim inside its load window still owns its share of the cache.

    The reserve is read from the claimed action's own sealed summary, so it
    costs one small JSON read per claim rather than a manifest fetch, and it
    expires with ``--claim-grace-min`` because an action past its load phase
    is no longer reading.
    """

    fleet = Fleet(tmp_path)
    running = fleet.action("running", [fleet.file("running.pt", 6144)])
    waiting = fleet.action("waiting", [fleet.file("waiting.pt", 4096)])
    claimed = fleet.queue.root / "claimed" / f"{running}.json"
    claimed.write_text(json.dumps(
        {"action_key": running, "claimed_unix": time.time()}))
    (fleet.queue.root / "ready" / f"{running}.json").unlink()
    # Room for one of the two, not both.
    stats = fleet.arcstats(size=0, c=8192, c_max=8192)

    event = fleet.cycle(fleet.args(arcstats=stats))
    assert event["claimed_reserved_bytes"] == 6144
    assert event["claimed_reserved_keys"] == [running]
    assert [s["reason"] for s in event["skipped"]] == ["headroom"]
    assert fleet.queue.prewarm(waiting) is None

    # The same claim, now past its load window, releases the reservation.
    claimed.write_text(json.dumps(
        {"action_key": running, "claimed_unix": time.time() - 3600}))
    event = fleet.cycle(fleet.args(arcstats=stats, claim_grace_min=1.0))
    assert event["claimed_reserved_bytes"] == 0
    assert [w["action_key"] for w in event["warmed"]] == [waiting]


def test_a_row_already_warmed_and_still_unclaimed_holds_its_share(
        tmp_path: Path) -> None:
    """Otherwise the loop warms the second row by evicting the first.

    Two rows, a cache big enough for one.  The first is warmed; on the next
    poll the second is refused rather than displacing it, because a row warmed
    and then evicted before its claim is worse than a row never warmed -- it
    cost the disks a full read and bought nothing.
    """

    fleet = Fleet(tmp_path)
    first = fleet.action("first", [fleet.file("first.pt", 6144)], priority=10)
    second = fleet.action("second", [fleet.file("second.pt", 6144)], priority=5)
    stats = fleet.arcstats(size=0, c=8192, c_max=8192)

    event = fleet.cycle(fleet.args(arcstats=stats, lookahead=1))
    assert [w["action_key"] for w in event["warmed"]] == [first]

    event = fleet.cycle(fleet.args(arcstats=stats, lookahead=2))
    assert event["warmed_reserved_bytes"] == 6144
    assert event["warmed_reserved_keys"] == [first]
    assert "already warm" in [s["reason"] for s in event["skipped"]]
    assert "headroom" in [s["reason"] for s in event["skipped"]]
    assert fleet.queue.prewarm(second) is None


def test_a_host_with_no_arc_warms_nothing(tmp_path: Path) -> None:
    """Absent counters are a zero budget, not an unbounded one.

    The loop is deployed by a published generation, and a generation reaches
    every box.  On a box with no ZFS the honest budget is zero.
    """

    fleet = Fleet(tmp_path)
    key = fleet.action("any", [fleet.file("any.pt", 4096)])
    event = fleet.cycle(fleet.args(arcstats=str(tmp_path / "absent")))
    assert event["budget_after_reserve"] == 0
    assert [s["reason"] for s in event["skipped"]] == ["headroom"]
    assert fleet.queue.prewarm(key) is None
