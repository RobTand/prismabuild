"""A manifest with no phase table is windowed too, not refused whole.

Windowing arrived with ``annotations.phases`` and was wired only to it, so a
producer that writes no phase table kept the original all-or-nothing rule:
``total <= budget`` or nothing at all.  That rule refuses exactly the rows
prewarm exists for.  The 2026-09-17 GLM-5.3-Flash joint-AURA ``prepare``
declares 469,008 entries and 6,840,192,952,914 bytes against a 257.7 GB ARC
ceiling; a sibling manifest without phases is refused on every poll for as
long as it is queued, and its action reads every byte off the spindles at
168 MB/s while the cache stays free.

Phases are accepted-progress frontiers, not the unit a warm has to be cut on.
``entries`` is already the consumption order -- the phase table is a running
byte sum *over* it -- so any entry-aligned prefix is as safe to make resident
without a phase table as with one.  What the loop cannot do without phases is
*advance* the window: no worker report maps to a byte count, so the prefix is
warmed once and stays where the budget put it.

The same claim has a second half.  A window that is warmed has to be reserved
as a window: a claimed row whose prefix is resident must hold its prefix
against the budget, not its whole manifest, or one 6.8 TB row with 8 KB warmed
takes the cache away from every other row in the queue.
"""
from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
from prewarm_fixture import Fleet  # noqa: E402

#: Four entries of 4096 bytes against a cache that holds two: the same shape
#: as 469,008 entries and 6.84 TB against 206 GB of budget, in bytes a test
#: can count.
ENTRIES = [(f"part-{index}", 4096) for index in range(4)]
CACHE = 8192


def _fleet(tmp_path: Path, *, sizes=ENTRIES):
    fleet = Fleet(tmp_path)
    key = fleet.action(
        "unphased",
        [fleet.file(f"{name}.pt", size) for name, size in sizes],
        # No ``annotations.phases``: the producer wrote none.
        annotations={"row_id": "unphased-row"},
    )
    return fleet, key, fleet.arcstats(size=0, c=CACHE, c_max=CACHE)


def test_a_manifest_with_no_phases_is_warmed_up_to_the_budget(
        tmp_path: Path) -> None:
    fleet, key, stats = _fleet(tmp_path)

    event = fleet.cycle(fleet.args(arcstats=stats))

    assert event["skipped"] == [], "an oversized manifest is not refused whole"
    assert [w["action_key"] for w in event["warmed"]] == [key]
    record = fleet.queue.prewarm(key)
    assert record["status"] == "partial"
    assert record["manifest_bytes"] == 16384
    assert record["warmed_bytes"] == CACHE
    assert record["entries_warmed"] == 2, "two entries, in manifest order"
    assert record["phased"] is False
    assert record["warmed_through_phase"] == ""


def test_the_prefix_is_read_in_the_manifests_own_order(
        tmp_path: Path) -> None:
    """The head of the manifest, because that is what the action reads first.

    A prefix chosen by any other rule warms bytes the action reaches last and
    leaves the bytes it reaches first on the spindles.
    """

    fleet, key, stats = _fleet(tmp_path)

    fleet.cycle(fleet.args(arcstats=stats))

    record = fleet.queue.prewarm(key)
    assert record["window_start_bytes"] == 0
    assert record["warmed_bytes"] == CACHE


def test_a_warmed_prefix_is_not_warmed_again(tmp_path: Path) -> None:
    """Without phases the window cannot advance, so it must not churn.

    Re-reading the same prefix every poll would spend the whole allowance on
    bytes that are already resident and keep the disks busy for nothing.
    """

    fleet, key, stats = _fleet(tmp_path)
    fleet.cycle(fleet.args(arcstats=stats))
    first = fleet.queue.prewarm(key)

    event = fleet.cycle(fleet.args(arcstats=stats))

    assert [s["reason"] for s in event["skipped"]] == ["already warm"]
    assert event["warmed"] == []
    again = fleet.queue.prewarm(key)
    assert again["finished_unix"] == first["finished_unix"]


def test_one_entry_larger_than_the_budget_is_still_refused(
        tmp_path: Path) -> None:
    """A warm reads files, not byte ranges.

    There is no entry-aligned prefix inside a single oversized entry, so the
    refusal that predates windowing is still the right answer for it.
    """

    fleet, key, stats = _fleet(tmp_path, sizes=[("whole", CACHE * 4)])

    event = fleet.cycle(fleet.args(arcstats=stats))

    assert [s["reason"] for s in event["skipped"]] == ["headroom"]
    assert fleet.queue.prewarm(key) is None


def test_a_claimed_unphased_window_reserves_its_prefix_not_its_manifest(
        tmp_path: Path) -> None:
    """Otherwise the first windowed row starves every row behind it.

    The reserve exists to stop the loop displacing bytes a running action is
    still reading.  A row that is 8 KB resident out of 16 KB can only have
    8 KB displaced, and reserving the manifest instead would take 16 KB of a
    16 KB cache away from the rest of the queue for one claim.
    """

    fleet, key, stats = _fleet(tmp_path)
    fleet.cycle(fleet.args(arcstats=stats))
    assert fleet.queue.prewarm(key)["warmed_bytes"] == CACHE
    fleet.claim(key)
    # A second row, and a cache with room for both prefixes but not for the
    # first row's whole manifest.
    behind = fleet.action("behind", [fleet.file("behind.pt", 4096)])
    roomy = fleet.arcstats(size=0, c=CACHE + 4096, c_max=CACHE + 4096)

    event = fleet.cycle(fleet.args(arcstats=roomy))

    assert event["claimed_reserved_bytes"] == CACHE
    assert event["claimed_reserved_keys"] == [key]
    assert [w["action_key"] for w in event["warmed"]] == [behind]
