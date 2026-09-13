"""A manifest bigger than the cache is warmed a window at a time.

The jobs that need prewarm most were the ones it refused.  The GLM joint pass
reads about 4.75 TB layer-major -- 45 hidden layers, most of them 111-124 GB --
against an ARC ceiling of 257.7 GB, so a whole-manifest ``total > budget``
check refused it by construction and the pass read every byte off the
spindles.

What has to fit in the cache was never the manifest.  It is the distance
between what the action has read and what the loop has made resident.  When
the producer writes ``annotations.phases`` -- a running byte sum over
``entries`` in the order the action consumes them -- the loop warms through the
last boundary that fits, records how far it got, and moves the window forward
as the action's own progress records say where it is reading.
"""
from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
from prewarm_fixture import Fleet, phase_table  # noqa: E402

#: Four layers of 4096 bytes against a cache that holds two of them: the same
#: shape as 45 layers of ~118 GB against 206 GB of budget, in bytes a test can
#: count.
LAYERS = [(f"layer-{index}", 4096) for index in range(4)]
CACHE = 8192


def _fleet(tmp_path: Path):
    fleet = Fleet(tmp_path)
    key = fleet.action(
        "joint",
        [fleet.file(f"{name}.pt", size) for name, size in LAYERS],
        annotations={"phases": phase_table(LAYERS), "row_id": "joint-3c"},
        progress_phases=[name for name, _ in LAYERS],
    )
    return fleet, key, fleet.arcstats(size=0, c=CACHE, c_max=CACHE)


def test_a_manifest_that_cannot_fit_is_warmed_through_the_last_phase_that_does(
        tmp_path: Path) -> None:
    fleet, key, stats = _fleet(tmp_path)

    event = fleet.cycle(fleet.args(arcstats=stats))

    assert event["skipped"] == [], "a phased manifest is not refused for size"
    record = fleet.queue.prewarm(key)
    assert record["status"] == "partial"
    assert record["manifest_bytes"] == 16384
    assert record["warmed_bytes"] == CACHE
    assert record["warmed_through_phase"] == "layer-1"
    assert record["entries_warmed"] == 2, "two layers, read in manifest order"


def test_a_window_that_reaches_as_far_as_the_budget_allows_is_already_warm(
        tmp_path: Path) -> None:
    """"Already warm" means "everything the budget allows is resident".

    Nothing has moved: the action has not been claimed, so its read frontier
    is still zero and the window cannot extend.  Re-reading the same two
    layers every poll would cost the disks the row twice and change nothing.
    """

    fleet, key, stats = _fleet(tmp_path)
    fleet.cycle(fleet.args(arcstats=stats))
    first = fleet.queue.prewarm(key)["finished_unix"]

    event = fleet.cycle(fleet.args(arcstats=stats))

    assert [s["reason"] for s in event["skipped"]] == ["already warm"]
    assert event["warmed_reserved_bytes"] == CACHE, (
        "a partial window is as resident as a whole one, and is protected")
    assert fleet.queue.prewarm(key)["finished_unix"] == first


def test_the_window_advances_with_the_read_frontier_and_never_runs_further_ahead(
        tmp_path: Path) -> None:
    """The claimed action reports a layer; the window moves one layer on.

    The invariant the advance keeps is the one the budget is for: warmed minus
    consumed never exceeds the budget, so the loop is never holding more of
    this manifest resident than the cache was going to keep anyway.
    """

    fleet, key, stats = _fleet(tmp_path)
    fleet.cycle(fleet.args(arcstats=stats))
    fleet.claim(key)
    fleet.report_progress(key, "layer-1", units=1)

    event = fleet.cycle(fleet.args(arcstats=stats))

    advanced = event["advanced"]
    assert [row["action_key"] for row in advanced] == [key]
    assert advanced[0]["trigger"] == "progress"
    assert advanced[0]["warmed_through_phase"] == "layer-2"
    record = fleet.queue.prewarm(key)
    assert record["status"] == "partial"
    assert record["warmed_bytes"] == 12288
    assert record["window_start_bytes"] == CACHE, (
        "the advance reads the next layer, not the ones already resident")
    assert record["bytes_warmed"] == 4096
    consumed = 4096                       # layer-0, the phase before layer-1
    assert record["warmed_bytes"] - consumed <= CACHE
    # One record per key, updated in place, so ``already_warm`` still answers
    # the same question about the same manifest.
    assert len(list((fleet.queue.root / "prewarm").glob("*.json"))) == 1


def test_a_phase_table_that_does_not_describe_this_manifest_is_ignored(
        tmp_path: Path) -> None:
    """A window on the wrong boundaries reads the wrong bytes confidently.

    So a table whose last boundary is not the manifest's own total is treated
    as absent, and the row falls back to the whole-manifest rule -- which
    refuses it, loudly, instead of recording a window nobody can trust.
    """

    fleet = Fleet(tmp_path)
    key = fleet.action(
        "mislabelled",
        [fleet.file(f"{name}.pt", size) for name, size in LAYERS],
        annotations={"phases": phase_table(LAYERS[:2])})
    stats = fleet.arcstats(size=0, c=CACHE, c_max=CACHE)

    event = fleet.cycle(fleet.args(arcstats=stats))

    assert [s["reason"] for s in event["skipped"]] == ["headroom"]
    assert event["skipped"][0]["phased"] is False
    assert fleet.queue.prewarm(key) is None
