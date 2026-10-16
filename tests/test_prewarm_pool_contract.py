"""Pool prewarm keeps its receipt and performs no experimental stage I/O."""
from __future__ import annotations

import json
from pathlib import Path
import sys
import subprocess
import os

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


@pytest.mark.parametrize("option", ["--stage", "--stage-pool-prefix=other",
                                    "--stage-free-floor-bytes=0"])
def test_retired_stage_options_are_rejected_before_io(tmp_path, option):
    forbidden = tmp_path / "forbidden"
    forbidden.mkdir()
    root = Path(__file__).resolve().parents[1]
    tripwire = tmp_path / "sitecustomize.py"
    tripwire.write_text("""
import os
import socket
import subprocess
import sys
from pathlib import Path
forbidden = Path(os.environ['PREWARM_FORBIDDEN'])
def audit(event, args):
    if event in ('socket.connect', 'subprocess.Popen', 'os.mkdir', 'os.remove', 'os.rename'):
        raise RuntimeError('pre-parse I/O: ' + event)
    if event == 'open':
        path = args[0]
        if args[2] & (os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC | os.O_APPEND):
            raise RuntimeError('pre-parse write: ' + str(path))
        if isinstance(path, (str, bytes)) and os.fsdecode(path).startswith(str(forbidden)):
            raise RuntimeError('pre-parse path access: ' + os.fsdecode(path))
sys.addaudithook(audit)
""")
    environment = dict(os.environ, PYTHONPATH=str(tmp_path), PYTHONDONTWRITEBYTECODE="1",
                       PREWARM_FORBIDDEN=str(forbidden))
    result = subprocess.run([
        sys.executable, str(root / "tools/fleet/prewarm_loop.py"), option,
        "--pool-root", str(forbidden / "queue"),
        "--cas-root", str(forbidden / "cas"),
        "--mount-map", f"{forbidden}={forbidden}",
        "--log", str(forbidden / "cycle.jsonl")],
        capture_output=True, text=True, env=environment, timeout=20)
    assert result.returncode == 2, result.stderr
    assert "unrecognized arguments" in result.stderr
    assert list(forbidden.iterdir()) == []
