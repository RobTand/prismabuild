"""Without ``--stage`` the loop is the loop it was before #582.

The stage tier ships off, and off has to mean *nothing*: no ``zpool`` asked,
no directory touched, and a receipt whose fields are the ones a pre-#582
reader already knows how to read.  Rob's release gate for this feature is that
the default flips on a measured campaign result, and a default that quietly
changed the receipt would have spent that decision before anybody made it.

The frozen key set below is the whole claim.  It is written out rather than
computed from the code it checks, because a set derived from the record would
agree with any change the record made.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

import pytest

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


def test_a_loop_that_was_not_asked_never_looks_for_a_stage(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Discovery is a subprocess on the file server; off must not run it.

    Both ends are nailed down: the discovery entry point and the subprocess
    boundary underneath it.  A cycle that touched either would raise here
    rather than record a tier nobody asked about.
    """

    fleet = Fleet(tmp_path)
    key = fleet.action("row", [fleet.file("row.pt", 8192)])

    def refuse(*args, **kwargs):
        raise AssertionError("the stage was discovered without --stage")

    monkeypatch.setattr(prewarm_loop, "discover_stage", refuse)
    monkeypatch.setattr(prewarm_loop, "run_tool", refuse)

    event = fleet.cycle(fleet.args())
    assert [w["action_key"] for w in event["warmed"]] == [key]
    assert fleet.queue.prewarm(key)["status"] == "complete"


def test_a_command_line_that_predates_the_flag_still_means_off() -> None:
    """The role's argument list is the pre-#582 one, and a namespace without
    the attribute at all -- which is what an older sealed command line
    produces -- reads as off rather than raising."""

    assert prewarm_loop.stage_requested(argparse.Namespace()) is False
    assert prewarm_loop.stage_requested(
        argparse.Namespace(stage=False)) is False
    assert prewarm_loop.stage_requested(
        argparse.Namespace(stage=True)) is True


def test_the_deployed_storage_role_asks_for_no_stage() -> None:
    """Off by default is a property of the fleet, not only of the parser.

    ``fleet_boxes.json`` is what the supervisor spawns the storage role with.
    Turning the tier on is an edit to this list, made deliberately, after the
    measurement Rob's release gate names.
    """

    boxes = json.loads(
        (Path(__file__).resolve().parents[1]
         / "tools/fleet/fleet_boxes.json").read_text())
    for name, box in boxes["boxes"].items():
        for flag in box.get("roles", {}).get("storage", []):
            assert not str(flag).startswith("--stage"), name
