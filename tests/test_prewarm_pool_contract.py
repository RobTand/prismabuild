"""Pool prewarm keeps its receipt and performs no experimental stage I/O."""
from __future__ import annotations

import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
from prewarm_fixture import Fleet  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import prewarm_loop  # noqa: E402

#: Every field a prewarm receipt carried before #582, and nothing else.
RECEIPT_FIELDS = frozenset({
    "schema", "action_key", "row_id", "host", "manifest_sha256",
    "manifest_bytes", "entry_count", "mount_prefix", "started_unix",
    "finished_unix", "started_utc", "finished_utc", "status", "warmed_bytes",
    "phased", "warmed_through_phase", "window_start_bytes", "trigger",
    "arc_before", "arc_after", "bytes_warmed", "contiguous_bytes",
    "entries_warmed", "seconds", "mb_per_s", "readers", "readers_peak",
    "reader_seconds", "per_reader_mb_s", "keep_pace_depth", "disk_pacing",
    "errors",
})


def test_the_default_receipt_carries_no_stage_field(tmp_path: Path) -> None:
    fleet = Fleet(tmp_path)
    key = fleet.action("row", [fleet.file("row.pt", 8192)])

    event = fleet.cycle(fleet.args())

    assert set(fleet.queue.prewarm(key)) == RECEIPT_FIELDS
    assert "stage" not in event
    assert "staged_bytes" not in json.dumps(event)


def test_pool_warming_does_not_discover_a_stage(tmp_path, monkeypatch):
    fleet = Fleet(tmp_path)
    key = fleet.action("row", [fleet.file("row.pt", 8192)])

    def refuse(*args, **kwargs):
        raise AssertionError("pool prewarm performed storage discovery")

    monkeypatch.setattr(prewarm_loop, "run_tool", refuse)
    event = fleet.cycle(fleet.args())
    assert [w["action_key"] for w in event["warmed"]] == [key]
    assert fleet.queue.prewarm(key)["status"] == "complete"


def test_a_claimed_row_past_arc_grace_keeps_its_receipt(tmp_path):
    fleet = Fleet(tmp_path)
    key = fleet.action("row", [fleet.file("row.pt", 8192)])
    args = fleet.args()
    fleet.cycle(args)
    original = fleet.queue.prewarm(key)
    fleet.claim(key, age_s=args.claim_grace_min * 60 + 1)

    event = fleet.cycle(args)

    assert event["claimed_reserved_keys"] == []
    assert event["claimed_reserved_bytes"] == 0
    assert fleet.queue.prewarm(key) == original
    assert event["pruned"] == []


def test_the_configured_storage_arguments_still_parse():
    boxes = json.loads(
        (Path(__file__).resolve().parents[1]
         / "tools/fleet/fleet_boxes.json").read_text())
    for box in boxes["boxes"].values():
        argv = box.get("roles", {}).get("storage")
        if argv is not None:
            assert prewarm_loop._parser().parse_args(argv).mount_map
