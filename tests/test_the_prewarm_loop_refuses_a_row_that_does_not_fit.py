"""Without ARC headroom, warming the next row would evict the running one.

There is no way to pin a page in the ARC, so "never evict a claimed action's
data" can only be a budget rule: never *ask* for more than the ceiling leaves
after the actions claimed inside their load window are subtracted.  A loop
that warmed regardless would be worse than no loop at all -- it would trade a
row that is reading now for a row that has not started.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parent))
from prewarm_fixture import Fleet  # noqa: E402

GiB = 1 << 30


def test_a_manifest_larger_than_the_budget_is_refused(tmp_path: Path) -> None:
    fleet = Fleet(tmp_path)
    key = fleet.action("big", [fleet.file("big.pt", 8192)])
    # size within 4 KiB of c_max: the budget cannot cover an 8 KiB manifest.
    stats = fleet.arcstats(size=GiB - 4096, c=GiB, c_max=GiB)

    event = fleet.cycle(fleet.args(arcstats=stats))
    assert [s["reason"] for s in event["skipped"]] == ["headroom"]
    assert event["skipped"][0]["manifest_bytes"] == 8192
    # Refusal is not a receipt: nothing was warmed, so nothing is recorded,
    # and a later poll with room is free to do the work.
    assert fleet.queue.prewarm(key) is None


def test_a_recently_claimed_actions_bytes_are_subtracted_from_the_budget(
        tmp_path: Path) -> None:
    """A claim inside its load window still owns its share of the ARC.

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
    stats = fleet.arcstats(size=GiB - 8192, c=GiB, c_max=GiB)

    args = fleet.args(arcstats=stats)
    event = fleet.cycle(args)
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
