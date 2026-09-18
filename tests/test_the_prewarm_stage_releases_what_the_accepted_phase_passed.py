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
