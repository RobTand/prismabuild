"""The stage is bounded by the reader it follows, not by hope.

Release is delete-behind-the-accepted-phase: the frontier a running action's
own progress record moves is what frees the bytes behind it, which is the same
signal that already releases ARC reserve.  Nothing new decides anything.

Two guards make that safe rather than merely small.  The stage tree is keyed
by path, and three measured GLM-5.3-Flash prepare manifests share 469 007 of
469 008 entries, so a release that asked only the row in front of it would
delete the bytes the row behind it has not reached; the predicate is therefore
path-exact across every queued row.  And a row's last phase leaves no frontier
behind it, so a row that has left the queue has its whole band swept -- read
from the receipt the warm wrote, because the loop keeps no memory across a
restart.
"""
from __future__ import annotations

from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from prewarm_fixture import Fleet, StagePool, phase_table  # noqa: E402

PHASES = [("a", 4096), ("b", 4096), ("c", 4096)]


def _fleet(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    fleet = Fleet(tmp_path)
    files = [fleet.file("a.pt", 4096), fleet.file("b.pt", 4096),
             fleet.file("c.pt", 4096)]
    key = fleet.action("run", files, priority=10,
                       annotations={"phases": phase_table(PHASES)},
                       progress_phases=[name for name, _ in PHASES])
    stage = StagePool(tmp_path)
    stage.install(monkeypatch)
    return fleet, key, stage, files


def args(fleet: Fleet, **overrides):
    return fleet.args(stage=True, stage_free_floor_bytes=0, **overrides)


def test_the_accepted_phase_releases_the_bytes_behind_it(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    fleet, key, stage, _ = _fleet(tmp_path, monkeypatch)

    fleet.cycle(args(fleet))
    assert stage.objects() == ["a.pt.pbstage@0+4096", "b.pt.pbstage@0+4096",
                              "c.pt.pbstage@0+4096"]

    # The action reports that it is reading ``c``, so ``a`` and ``b`` are read
    # and nothing is waiting for them.
    fleet.claim(key)
    fleet.report_progress(key, "c")
    event = fleet.cycle(args(fleet))

    released = event["stage"]["released"]
    assert [row["action_key"] for row in released] == [key]
    assert released[0]["consumed_bytes"] == 8192
    assert released[0]["released_entries"] == 2
    assert released[0]["released_bytes"] == 8192
    assert released[0]["retained_entries"] == 0
    assert stage.objects() == ["c.pt.pbstage@0+4096"]
    assert fleet.queue.prewarm(key)["stage"]["evicted_through_bytes"] == 8192


def test_a_release_is_not_repeated_on_the_next_cycle(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The frontier the last release reached is read back from the receipt,
    so an unchanged frontier costs nothing and releases nothing twice."""

    fleet, key, stage, _ = _fleet(tmp_path, monkeypatch)
    fleet.cycle(args(fleet))
    fleet.claim(key)
    fleet.report_progress(key, "c")
    fleet.cycle(args(fleet))

    event = fleet.cycle(args(fleet))
    released = event["stage"]["released"]
    assert released[0]["candidate_entries"] == 0
    assert released[0]["released_entries"] == 0
    assert event["stage"]["released_bytes"] == 0
    assert stage.objects() == ["c.pt.pbstage@0+4096"]


def test_bytes_another_queued_row_still_wants_are_kept(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Total overlap is the measured shape of this campaign, not a corner.

    A second row over the same files has read nothing, so every object the
    first row has passed is still one the second will ask for.  Nothing reads
    the stage today, so nothing breaks today -- the predicate is path-exact
    now so that it is still right on the day a consumer arrives.
    """

    fleet, key, stage, files = _fleet(tmp_path, monkeypatch)
    fleet.cycle(args(fleet))
    fleet.claim(key)
    fleet.report_progress(key, "c")
    # The next artifact over the same bytes, queued behind it.
    fleet.action("next", files, priority=5)

    event = fleet.cycle(args(fleet, lookahead=0))

    released = event["stage"]["released"]
    assert released[0]["candidate_entries"] == 2
    assert released[0]["retained_entries"] == 2
    assert released[0]["released_entries"] == 0
    assert stage.objects() == ["a.pt.pbstage@0+4096", "b.pt.pbstage@0+4096",
                              "c.pt.pbstage@0+4096"]


def test_a_row_that_has_left_the_queue_is_swept(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Delete-behind follows a frontier, and a finished row leaves none.

    Without this the tail of every completed action would sit on the stage
    until somebody noticed, which is the unbounded growth this tier must not
    have.
    """

    fleet, key, stage, _ = _fleet(tmp_path, monkeypatch)
    fleet.cycle(args(fleet))
    assert len(stage.objects()) == 3

    # The action is claimed, runs and finishes: nothing names it any more.
    claimed = fleet.claim(key)
    claimed.unlink()
    event = fleet.cycle(args(fleet))

    orphans = event["stage"]["orphans"]
    assert [row["action_key"] for row in orphans] == [key]
    assert orphans[0]["status"] == "swept"
    assert orphans[0]["released_entries"] == 3
    assert stage.objects() == []
    assert fleet.queue.prewarm(key)["stage"]["swept"] is True

    # And the sweep does not run again on the row it already swept.
    assert fleet.cycle(args(fleet))["stage"]["orphans"] == []


def test_a_sweep_that_cannot_name_its_objects_says_so(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A leak with a receipt is the only kind worth having.

    Without the sealed manifest the loop cannot name what the row staged, so
    it reports the row blocked rather than marking it swept -- which would be
    a claim that the bytes are gone.
    """

    fleet, key, stage, _ = _fleet(tmp_path, monkeypatch)
    fleet.cycle(args(fleet))
    claimed = fleet.claim(key)
    claimed.unlink()
    (fleet.cas_root / "requests" / key[:2] / f"{key}.json").unlink()

    event = fleet.cycle(args(fleet))
    orphans = event["stage"]["orphans"]
    assert orphans[0]["status"] == "blocked"
    assert len(stage.objects()) == 3
    assert fleet.queue.prewarm(key)["stage"]["sweep_blocked"]


def test_a_dry_run_plans_the_release_and_performs_none_of_it(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """``--dry-run`` reads the box and writes nothing to it.

    Unlinking a staged object is a write, and so is moving the frontier in a
    receipt.  A plan that quietly evicted would be worse than one that warmed:
    it would spend the tier while claiming to spend nothing.
    """

    fleet, key, stage, _ = _fleet(tmp_path, monkeypatch)
    fleet.cycle(args(fleet))
    fleet.claim(key)
    fleet.report_progress(key, "c")
    before = stage.objects()

    event = fleet.cycle(args(fleet, dry_run=True))

    planned = event["stage"]["released"][0]
    assert planned["applied"] is False
    assert planned["deletable_entries"] == 2
    assert planned["released_entries"] == 0
    assert stage.objects() == before
    assert fleet.queue.prewarm(key)["stage"]["evicted_through_bytes"] == 0

    # And the real cycle still does it, so the plan was a plan and not a
    # silent refusal.
    fleet.cycle(args(fleet))
    assert stage.objects() == ["c.pt.pbstage@0+4096"]


def test_a_dry_run_does_not_sweep_a_row_that_left_the_queue(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    fleet, key, stage, _ = _fleet(tmp_path, monkeypatch)
    fleet.cycle(args(fleet))
    fleet.claim(key).unlink()

    event = fleet.cycle(args(fleet, dry_run=True))

    assert event["stage"]["orphans"][0]["status"] == "planned"
    assert event["stage"]["orphans"][0]["deletable_entries"] == 3
    assert len(stage.objects()) == 3
    assert fleet.queue.prewarm(key)["stage"].get("swept") is None


def test_a_resubmitted_key_starts_a_new_stage_ledger(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A swept key that stages again is a new life, and a new life is a new
    ledger.

    ``swept`` is what makes the sweep skip a row, so a receipt that carried it
    forward into the next life would leave the band that life staged
    unreleasable and invisible: not swept, not released by delete-behind --
    which would start at the *previous* life's frontier -- and not even named
    among the orphans.  The fleet re-queues a key rather than cache-hitting
    it after a stop, so this is the ordinary way a key gets a second life.
    """

    fleet, key, stage, files = _fleet(tmp_path, monkeypatch)
    # A budget that covers one phase: the row is warmed a window at a time,
    # which is the shape whose second life stages anything at all.
    one_phase = fleet.args(stage=True, stage_free_floor_bytes=0,
                           arcstats=fleet.arcstats(size=0, c=4096, c_max=4096))
    fleet.cycle(one_phase)
    assert stage.objects() == ["a.pt.pbstage@0+4096"]
    assert fleet.queue.prewarm(key)["status"] == "partial"

    # It is claimed, runs, finishes: the sweep releases the band it staged.
    fleet.claim(key).unlink()
    swept = fleet.cycle(one_phase)
    assert [row["action_key"] for row in swept["stage"]["orphans"]] == [key]
    assert stage.objects() == []

    # The same action is queued again, and this time the budget covers the
    # whole manifest, so the second life stages the rest of it.
    fleet.action("run", files, priority=10,
                 annotations={"phases": phase_table(PHASES)},
                 progress_phases=[name for name, _ in PHASES])
    fleet.cycle(args(fleet))
    assert stage.objects() == ["b.pt.pbstage@0+4096", "c.pt.pbstage@0+4096"]
    receipt = fleet.queue.prewarm(key)["stage"]
    assert receipt.get("swept") is not True
    assert receipt["evicted_through_bytes"] == 0

    # And when the second life leaves the queue, its band is swept like any
    # other -- rather than sitting on the tier with a receipt that says the
    # last life's sweep already dealt with it.
    fleet.claim(key).unlink()
    event = fleet.cycle(args(fleet))
    assert [row["action_key"] for row in event["stage"]["orphans"]] == [key]
    assert event["stage"]["orphans"][0]["status"] == "swept"
    assert stage.objects() == []


def test_a_claimed_row_past_its_grace_keeps_the_band_it_is_reading(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A row dropped from the reserve is still a row that is running.

    A claimed action that declares no progress policy stops being counted
    against the ARC budget once its grace expires -- a budget decision, made
    because nothing more can be learned about what it still needs.  It is not
    a statement that the action has finished, and a sweep that read it as one
    would delete the staged band under an action that is two hours into
    reading it, and write ``swept`` on the receipt.
    """

    fleet = Fleet(tmp_path)
    files = [fleet.file("a.pt", 4096), fleet.file("b.pt", 4096)]
    key = fleet.action("run", files)
    stage = StagePool(tmp_path)
    stage.install(monkeypatch)

    fleet.cycle(args(fleet))
    assert len(stage.objects()) == 2

    # Claimed, running, reporting nothing, well past --claim-grace-min.
    fleet.claim(key, age_s=21 * 60)
    event = fleet.cycle(args(fleet))

    assert event["stage"]["orphans"] == []
    assert stage.objects() == ["a.pt.pbstage@0+4096", "b.pt.pbstage@0+4096"]
    assert fleet.queue.prewarm(key)["stage"].get("swept") is not True


def test_a_claimed_row_with_no_window_still_holds_the_bytes_it_shares(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Delete-behind asks every live row, not only the windowed ones.

    A row claimed before this loop ever warmed it has no window: nothing is
    resident for it and no phase table names a frontier, so it has read
    nothing of what it will read.  Another row's delete-behind over the same
    files must therefore keep every object -- which is the 469 007-of-469 008
    overlap this campaign actually has.
    """

    fleet, key, stage, files = _fleet(tmp_path, monkeypatch)
    # Queued behind it over the same files, and claimed before it is warmed.
    other = fleet.action("next", files, priority=5)
    fleet.cycle(args(fleet, lookahead=1))
    assert len(stage.objects()) == 3
    fleet.claim(other)

    fleet.claim(key)
    fleet.report_progress(key, "c")
    event = fleet.cycle(args(fleet, lookahead=1))

    released = event["stage"]["released"]
    by_key = {row["action_key"]: row for row in released}
    assert by_key[key]["candidate_entries"] == 2
    assert by_key[key]["retained_entries"] == 2
    assert by_key[key]["released_entries"] == 0
    assert stage.objects() == ["a.pt.pbstage@0+4096", "b.pt.pbstage@0+4096",
                               "c.pt.pbstage@0+4096"]


#: The v2 read timeline the design doc names: a forward pass, a compute phase
#: that reads nothing, and a reverse pass over the same two ranges.
REVISIT_PLAN = {"phases": [
    {"name": "forward-0", "entry_indices": [0], "bytes": 4,
     "cumulative_bytes": 4},
    {"name": "forward-1", "entry_indices": [1], "bytes": 4,
     "cumulative_bytes": 8},
    {"name": "compute", "entry_indices": [], "bytes": 0,
     "cumulative_bytes": 8},
    {"name": "reverse-1", "entry_indices": [1], "bytes": 4,
     "cumulative_bytes": 12},
    {"name": "reverse-0", "entry_indices": [0], "bytes": 4,
     "cumulative_bytes": 16},
], "read_bytes": 16}


def test_a_range_a_later_phase_reads_again_is_not_released_behind_it(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Delete-behind asks the releasing row about its own band first.

    A v2 read plan may name the same range in two phases, which is a
    supported and documented shape.  The band behind the accepted phase then
    holds objects the *next* phase reads, and a retain predicate that asked
    only the other rows would delete the revisit at the moment it begins --
    an SSD write repeated immediately today, and a residency map that loses
    the range at the phase that re-reads it once a consumer exists.
    """

    fleet = Fleet(tmp_path)
    files = [fleet.file("a.pt", 4), fleet.file("b.pt", 4)]
    key = fleet.action(
        "revisit", files, read_plan=REVISIT_PLAN,
        progress_phases=[phase["name"] for phase in REVISIT_PLAN["phases"]])
    stage = StagePool(tmp_path)
    stage.install(monkeypatch)

    fleet.cycle(args(fleet))
    assert stage.objects() == ["a.pt.pbstage@0+4", "b.pt.pbstage@0+4"]

    # The forward pass is done and the reverse pass is starting: both ranges
    # are behind the frontier, and both are about to be read again.
    fleet.claim(key)
    fleet.report_progress(key, "reverse-1")
    event = fleet.cycle(args(fleet))

    released = event["stage"]["released"][0]
    assert released["consumed_bytes"] == 8
    assert released["candidate_entries"] == 2
    assert released["retained_entries"] == released["candidate_entries"]
    assert released["released_entries"] == 0
    assert stage.objects() == ["a.pt.pbstage@0+4", "b.pt.pbstage@0+4"]
