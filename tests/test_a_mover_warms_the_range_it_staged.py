"""A mover reads its staged range back, so layer 2 is caused rather than hoped for (#638).

The copy writes the stage and never reads it, so stage blocks reached the file
server's ARC only incidentally -- and a repeat read on dl380g10 fell from 9580
to 7423 MiB/s as other shards evicted them.  The warm step closes that: after
the range is copied and verified, the mover reads it back on the box that owns
the stage, which is the one place a read puts those blocks in that ARC.

Two claims, and they are separate: the warm happened (bytes really passed
through ``read(2)``, counted here off this process's own ``/proc/self/io``
rather than off a log line), and it is refused when the dataset's
``primarycache`` says the ARC may not hold the data anyway.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import prismabuild.core as pb  # noqa: E402
from prismabuild import pool, storage_tiers  # noqa: E402
import stage_move  # noqa: E402

CONSUMER = "c" * 64
MOVER = "a" * 64
MANIFEST_SHA = "9" * 64
TIER = "prismabuild-stage:dl380g10"
#: Big enough that the read is visible against the fixture's own file traffic.
CHUNK = 1 << 20


def _write(path: Path, payload: bytes) -> dict[str, object]:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    return {"path": str(path), "offset": 0, "bytes": len(payload),
            "sha256": hashlib.sha256(payload).hexdigest()}


def _manifest(mount: Path, entries: list[dict[str, object]]) -> dict[str, object]:
    return {
        "schema": pb.DATA_MANIFEST_SCHEMA_V1,
        "produced_by": {"tool": "test"},
        "mount_prefix": str(mount),
        "entries": entries,
        "entry_count": len(entries),
        "total_bytes": sum(int(entry["bytes"]) for entry in entries),
        "annotations": {},
    }


def _fleet(tmp_path: Path, primarycache: str) -> pool.PoolQueue:
    """A queue whose stage tier announces what ZFS said about its dataset."""

    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    queue.announce_tier({
        "schema": storage_tiers.TIER_RECORD_SCHEMA_V1,
        "tier_id": TIER, "host": "dl380g10", "tier": "stage",
        "mountpoint": str(tmp_path / "stage"),
        "capacity_bytes": 4 * storage_tiers.GIB,
        "primarycache": primarycache,
        "arc_warm": dict(storage_tiers.stage_arc_eligibility(
            {"primarycache": primarycache})),
    })
    return queue


def _args(tmp_path: Path, manifest: dict, *, start: int, end: int, **overrides):
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest))
    args = stage_move.build_parser().parse_args([
        "--pool-root", str(tmp_path / "queue"),
        "--cas-root", str(tmp_path / "cas"),
        "--action-key", MOVER,
        "--consumer-action-key", CONSUMER,
        "--tier-id", TIER,
        "--stage-root", str(tmp_path / "stage"),
        "--manifest-sha256", MANIFEST_SHA,
        "--range-start-bytes", str(start),
        "--range-end-bytes", str(end),
        "--manifest", str(manifest_path),
        "--residency-root", str(tmp_path / "queue" / pool.RESIDENCY),
        "--block", str(1 << 16),
        "--readers", "2",
        "--max-readers", "2",
        "--unpaced",
    ])
    for name, value in overrides.items():
        setattr(args, name, value)
    return args


def _two_files(tmp_path: Path) -> tuple[Path, list[dict[str, object]]]:
    mount = tmp_path / "mnt"
    return mount, [_write(mount / "shard-1.bin", b"a" * CHUNK),
                   _write(mount / "sub" / "shard-2.bin", b"b" * CHUNK)]


def test_the_staged_bytes_are_read_back_after_the_copy(tmp_path: Path) -> None:
    mount, entries = _two_files(tmp_path)
    _fleet(tmp_path, "all")
    args = _args(tmp_path, _manifest(mount, entries), start=0, end=2 * CHUNK)

    before = stage_move.proc_io()
    receipt = stage_move.move(args)
    after = stage_move.proc_io()

    warm = receipt["arc_warm"]
    assert warm["state"] == "warmed"
    assert warm["bytes"] == receipt["bytes_staged"] == 2 * CHUNK
    assert warm["entries"] == 2
    assert warm["errors"] == []
    # The copy reads the source once and the warm reads the stage once, so a
    # mover that only copied cannot account for the second 2 MiB.
    assert after["rchar"] - before["rchar"] >= 4 * CHUNK


def test_a_stage_the_arc_may_not_hold_is_not_warmed(tmp_path: Path) -> None:
    """Reads that cannot land anywhere are not spent."""

    mount, entries = _two_files(tmp_path)
    _fleet(tmp_path, "metadata")
    args = _args(tmp_path, _manifest(mount, entries), start=0, end=2 * CHUNK)

    before = stage_move.proc_io()
    receipt = stage_move.move(args)
    after = stage_move.proc_io()

    assert receipt["complete"] is True
    assert receipt["arc_warm"]["state"] == "refused"
    assert receipt["arc_warm"]["bytes"] == 0
    assert "primarycache" in receipt["arc_warm"]["reason"]
    assert after["rchar"] - before["rchar"] < 4 * CHUNK


def test_the_warm_reports_what_it_could_not_read(tmp_path: Path) -> None:
    """A staged file that is gone is an error on the receipt, never a raise:
    the copy is published and the map is written; a warm is an optimization."""

    staged = tmp_path / "stage" / "shard-1.bin"
    staged.parent.mkdir(parents=True, exist_ok=True)
    staged.write_bytes(b"a" * CHUNK)

    outcome = stage_move.warm_staged(
        [str(staged), str(tmp_path / "stage" / "missing.bin")],
        workers=2, block=1 << 16)

    assert outcome["bytes"] == CHUNK
    assert outcome["entries"] == 1
    assert len(outcome["errors"]) == 1
    assert "missing.bin" in outcome["errors"][0]
