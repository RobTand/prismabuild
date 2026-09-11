"""The prewarm record is evidence, so it reports the read, not the intent.

A receipt that said "warmed" because a warm was attempted would be worse than
none: the done row would claim residency for a run that read off the disks.
So the record carries both numbers -- what the manifest asked for and what was
actually read -- plus the ARC counters either side, and its status is derived
from the comparison rather than asserted.
"""
from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
from prewarm_fixture import Fleet  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import pool  # noqa: E402



def test_a_complete_warm_records_the_bytes_and_the_arc_either_side(
        tmp_path: Path) -> None:
    fleet = Fleet(tmp_path)
    key = fleet.action("row", [fleet.file("a.pt", 4096),
                               fleet.file("b.pt", 8192)])

    fleet.cycle(fleet.args())
    record = fleet.queue.prewarm(key)

    assert record["schema"] == pool.POOL_PREWARM_SCHEMA_V1
    assert record["action_key"] == key
    assert record["status"] == "complete"
    assert record["manifest_bytes"] == 12288
    assert record["bytes_warmed"] == 12288
    assert record["entry_count"] == 2
    assert record["entries_warmed"] == 2
    assert record["mount_prefix"] == str(fleet.mount)
    assert record["finished_unix"] >= record["started_unix"]
    for side in ("arc_before", "arc_after"):
        assert set(record[side]) == {
            "arc_size", "arc_c", "arc_c_max",
            "headroom_nominal", "headroom_effective"}


def test_a_warm_that_could_not_read_everything_says_partial(
        tmp_path: Path) -> None:
    """A prefix is a useful outcome and a dishonest "complete" is not.

    Bytes can go missing between the submission that priced them and the poll
    that warms them -- a file truncated, a shard replaced, a budget that ran
    out mid-read because ``c`` moved.  The record reports what was read, and
    ``status`` is derived from that rather than asserted, so the done row can
    never claim residency the loop did not achieve.
    """

    fleet = Fleet(tmp_path)
    path, _ = fleet.file("short.pt", 1 << 20)
    key = fleet.action("row", [fleet.file("whole.pt", 1 << 20), (path, 1 << 20)])
    Path(path).write_bytes(b"\0" * 4096)

    fleet.cycle(fleet.args(readers=1))
    record = fleet.queue.prewarm(key)
    assert record["manifest_bytes"] == 2 << 20
    assert record["bytes_warmed"] == (1 << 20) + 4096
    assert record["status"] == "partial"


def test_a_dry_run_decides_without_reading_or_recording(tmp_path: Path) -> None:
    fleet = Fleet(tmp_path)
    key = fleet.action("row", [fleet.file("a.pt", 4096)])
    event = fleet.cycle(fleet.args(dry_run=True))
    assert [w["status"] for w in event["warmed"]] == ["dry-run"]
    assert event["warmed"][0]["manifest_bytes"] == 4096
    assert event["warmed"][0]["bytes_warmed"] == 0
    assert fleet.queue.prewarm(key) is None


def test_a_row_already_warmed_at_this_digest_is_not_warmed_again(
        tmp_path: Path) -> None:
    fleet = Fleet(tmp_path)
    key = fleet.action("row", [fleet.file("a.pt", 4096)])
    fleet.cycle(fleet.args())
    first = fleet.queue.prewarm(key)["finished_unix"]

    event = fleet.cycle(fleet.args())
    assert [s["reason"] for s in event["skipped"]] == ["already warm"]
    assert fleet.queue.prewarm(key)["finished_unix"] == first
