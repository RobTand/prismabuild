"""The next row is warmed while the running one reads, not when it is claimed.

#523 measured the defect this file pins.  The loop warmed the row behind the
running one only when the lookahead slot freed -- which is the instant that
row was claimed and started its own cold reads.  The warm ran 4.3 s ahead of
the claim, overlapped the row's own reads at 256.7 MB/s, and the client saw no
speedup at all: the bytes arrived exactly as late as if nobody had warmed them.

A claim is the wrong signal because it is the *end* of the useful window.  The
right one is the running action saying where it has got to: an action that has
finished its load phase is not reading those bytes again, so they stop being
worth reserving and the next row becomes warmable while there is still time to
make a difference.  Actions that report nothing keep the claim-plus-grace
behaviour they have now.
"""
from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
from prewarm_fixture import Fleet, phase_table  # noqa: E402

#: Room for one row of either size, never for both.
CACHE = 8192
RUNNING = [("load", 4096), ("compute", 2048)]


def _fleet(tmp_path: Path):
    """One running row that reports progress, one cold row behind it."""

    fleet = Fleet(tmp_path)
    running = fleet.action(
        "running",
        [fleet.file("load.pt", 4096), fleet.file("compute.pt", 2048)],
        priority=10,
        annotations={"phases": phase_table(RUNNING)},
        progress_phases=[name for name, _ in RUNNING],
    )
    waiting = fleet.action("waiting", [fleet.file("waiting.pt", 4096)],
                           priority=5)
    return fleet, running, waiting


def test_a_claim_alone_still_holds_the_next_row_back(tmp_path: Path) -> None:
    """The behaviour #523 measured, kept for an action that reports nothing.

    This is the baseline the fix is measured against: with the running row
    claimed inside its grace window and no progress record to read, its whole
    manifest is still reserved and the row behind it is refused for headroom.
    """

    fleet, running, waiting = _fleet(tmp_path)
    stats = fleet.arcstats(size=0, c=CACHE, c_max=CACHE)

    first = fleet.cycle(fleet.args(arcstats=stats, lookahead=2))
    assert [w["action_key"] for w in first["warmed"]] == [running]
    assert [s["reason"] for s in first["skipped"]] == ["headroom"]

    fleet.claim(running)
    event = fleet.cycle(fleet.args(arcstats=stats, lookahead=2))

    assert event["claimed_reserved_bytes"] == 6144
    assert event["claimed_released_bytes"] == 0
    assert event["progress_triggers"] == []
    assert [s["reason"] for s in event["skipped"]] == ["headroom"]
    assert fleet.queue.prewarm(waiting) is None


def test_the_next_row_is_warmed_when_the_running_one_passes_its_load_phase(
        tmp_path: Path) -> None:
    """The same queue, one progress record later, and the warm happens.

    Nothing else moved: the claim is the same claim, inside the same grace
    window, and the budget is the same budget.  What changed is that the
    running action said it had reached ``compute``, so the 4096 bytes of its
    load phase left the reserve and the cold row fits.
    """

    fleet, running, waiting = _fleet(tmp_path)
    stats = fleet.arcstats(size=0, c=CACHE, c_max=CACHE)
    fleet.cycle(fleet.args(arcstats=stats, lookahead=2))
    fleet.claim(running)
    fleet.report_progress(running, "compute", units=1)

    event = fleet.cycle(fleet.args(arcstats=stats, lookahead=2))

    assert event["claimed_released_bytes"] == 4096
    assert event["progress_triggers"] == [
        {"action_key": running, "phase": "compute", "released_bytes": 4096}]
    assert event["claimed_reserved_bytes"] == 2048, (
        "only the phase the action is still reading stays reserved")
    warmed = event["warmed"]
    assert [row["action_key"] for row in warmed] == [waiting]
    assert warmed[0]["trigger"] == "progress", (
        "the release is what made the row fit, and the event has to say so")
    record = fleet.queue.prewarm(waiting)
    assert record["status"] == "complete"
    assert record["bytes_warmed"] == 4096
    # The point of the whole exercise: the row was resident before anybody
    # claimed it, which is the only moment a warm can still help it.
    assert (fleet.queue.root / "ready" / f"{waiting}.json").exists()


def test_a_report_from_an_earlier_launch_releases_nothing(
        tmp_path: Path) -> None:
    """The launcher unlinks the path and mints a token before every launch.

    The token is not published to the queue, so the loop cannot check it; what
    it can check is that the report is younger than the claim it is reading.  A
    leftover record from a previous attempt would otherwise release a phase
    this run has not reached.
    """

    fleet, running, waiting = _fleet(tmp_path)
    stats = fleet.arcstats(size=0, c=CACHE, c_max=CACHE)
    fleet.cycle(fleet.args(arcstats=stats, lookahead=2))
    claimed = fleet.claim(running)
    import json
    claimed_unix = json.loads(claimed.read_text())["claimed_unix"]
    # The raw reporter path is deliberately not authority for prewarm: a
    # previous launch can leave it behind, but it has no matching accepted
    # observation in this launch's lease.
    fleet.queue.action_progress_path(running).write_text(json.dumps({
        "schema": "prismabuild.action_progress.v1", "phase": "compute",
        "reported_unix": claimed_unix - 60.0,
    }))

    event = fleet.cycle(fleet.args(arcstats=stats, lookahead=2))

    assert event["claimed_released_bytes"] == 0
    assert [s["reason"] for s in event["skipped"]] == ["headroom"]


def test_a_record_beside_an_action_that_declared_no_policy_is_not_read(
        tmp_path: Path) -> None:
    """Progress is something an action seals into its request, not a file.

    A record the platform never asked for must not move the budget: the sealed
    policy is what makes a report a report.
    """

    fleet = Fleet(tmp_path)
    running = fleet.action(
        "unsealed",
        [fleet.file("load.pt", 4096), fleet.file("compute.pt", 2048)],
        priority=10, annotations={"phases": phase_table(RUNNING)})
    fleet.action("waiting", [fleet.file("waiting.pt", 4096)], priority=5)
    stats = fleet.arcstats(size=0, c=CACHE, c_max=CACHE)
    fleet.cycle(fleet.args(arcstats=stats, lookahead=2))
    fleet.claim(running)
    fleet.report_progress(running, "compute")

    event = fleet.cycle(fleet.args(arcstats=stats, lookahead=2))

    assert event["claimed_released_bytes"] == 0
    assert [s["reason"] for s in event["skipped"]] == ["headroom"]
