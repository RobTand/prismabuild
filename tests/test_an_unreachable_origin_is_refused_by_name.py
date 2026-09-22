"""A mover that cannot reach its origin says so, and only when it is proven.

A manifest names absolute origin paths, and the mover runs on the tier host,
which need not be the box that wrote them (#804). The old failure was silent
about its cause: every entry failed to open, the receipt said the mover
staged nothing, and the owner read the same symptom a mover defect produces.
These tests pin the typed classification -- proven absence or denial is
`origin_unreachable`, transient I/O stays unknown and conservative -- that a
reachable origin still stages exactly as before, and that the diagnosis runs
only on the empty path, never as a pre-copy census over healthy work.

Every test drives the real tool or the real classifier; `--unpaced` is the
only concession, for the same reason the mover suite gives: the pacer needs
a live ZFS pool's member devices, and refusing to run without one is the
storage role's correct behaviour rather than something to fake.
"""
from __future__ import annotations

import errno
import hashlib
import json
import os
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import prismabuild.core as pb  # noqa: E402
import prismabuild.pool as pool  # noqa: E402
import prewarm_loop  # noqa: E402
import stage_move  # noqa: E402

CONSUMER = "c" * 64
MOVER = "a" * 64
MANIFEST_SHA = "9" * 64


def _entry(path: Path, payload: bytes) -> dict[str, object]:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    return {"path": str(path), "offset": 0, "bytes": len(payload),
            "sha256": hashlib.sha256(payload).hexdigest()}


def _declared(path: Path, size: int) -> dict[str, object]:
    """One manifest entry over a path that may not exist on this host."""

    return {"path": str(path), "offset": 0, "bytes": size,
            "sha256": hashlib.sha256(b"x" * size).hexdigest()}


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


def _args(tmp_path: Path, manifest: dict, *, start: int, end: int, **overrides):
    """The tool's own parsed arguments, then this case's differences."""

    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest))
    args = stage_move.build_parser().parse_args([
        "--pool-root", str(tmp_path / "queue"),
        "--cas-root", str(tmp_path / "cas"),
        "--action-key", MOVER,
        "--consumer-action-key", CONSUMER,
        "--tier-id", "prismabuild-stage:dl380g10",
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


def _mounts() -> prewarm_loop.MountMap:
    return prewarm_loop.MountMap([])


# --------------------------------------------------------------------------
# The classifier
# --------------------------------------------------------------------------

def test_an_absent_origin_directory_is_unreachable(tmp_path: Path) -> None:
    """ENOENT on the origin directory is positive proof, with its last seen
    ancestor named so the owner can tell a box-local prefix from a lost
    mount."""

    origin = tmp_path / "gone" / "shard-1.bin"
    diagnosis = stage_move.origin_reachability_diagnosis(
        [_declared(origin, 16)], _mounts())

    assert diagnosis["state"] == "unreachable"
    assert diagnosis["checked"] == 1
    assert diagnosis["unreachable_count"] == 1
    assert diagnosis["unreachable"] == [{
        "path": str(tmp_path / "gone"), "reason": "absent",
        "deepest_existing": str(tmp_path)}]
    assert diagnosis["unknown"] == []


def test_a_present_origin_directory_is_reachable(tmp_path: Path) -> None:
    """The diagnosis distinguishes a reachable root from a missing file under
    it, so an origin whose declared file is gone is not called unreachable."""

    entry = _entry(tmp_path / "there" / "shard-1.bin", b"a" * 16)
    present = stage_move.origin_reachability_diagnosis([entry], _mounts())
    assert present["state"] == "reachable"
    assert present["unreachable"] == []
    assert present["unknown"] == []

    missing = stage_move.origin_reachability_diagnosis(
        [_declared(tmp_path / "there" / "never-written.bin", 16)], _mounts())
    assert missing["state"] == "reachable"


def test_a_denied_origin_directory_is_unreachable(tmp_path: Path) -> None:
    """A search this process may not make is proven, and is not silence."""

    def denied(path: str) -> os.stat_result:
        raise PermissionError(errno.EACCES, "Permission denied", path)

    diagnosis = stage_move.origin_reachability_diagnosis(
        [_declared(tmp_path / "private" / "shard-1.bin", 16)], _mounts(),
        stat=denied)

    assert diagnosis["state"] == "unreachable"
    assert diagnosis["unreachable"][0]["reason"] == "denied"


def test_transient_io_is_unknown_and_never_unreachable(tmp_path: Path) -> None:
    """EIO and its family are unknown evidence, not a typed refusal.

    A transient I/O failure must not be turned into the same verdict a
    configuration mistake earns; the diagnosis answers `unknown`, which
    `move` never types as `origin_unreachable`.
    """

    def broken(path: str) -> os.stat_result:
        raise OSError(errno.EIO, "Input/output error", path)

    diagnosis = stage_move.origin_reachability_diagnosis(
        [_declared(tmp_path / "there" / "shard-1.bin", 16)], _mounts(),
        stat=broken)

    assert diagnosis["state"] == "unknown"
    assert diagnosis["unknown_count"] == 1
    assert diagnosis["unreachable"] == []
    assert diagnosis["unknown"][0]["path"] == str(tmp_path / "there")


def test_a_mixed_window_is_partial_and_never_refused_by_name(
        tmp_path: Path) -> None:
    """One reachable origin directory beside an absent one is `partial`: the
    mover stages what it can, and the missing half is not a whole-window
    reachability verdict."""

    present = _entry(tmp_path / "there" / "shard-1.bin", b"a" * 16)
    diagnosis = stage_move.origin_reachability_diagnosis(
        [present, _declared(tmp_path / "gone" / "shard-2.bin", 16)], _mounts())

    assert diagnosis["state"] == "partial"
    assert diagnosis["unreachable_count"] == 1


# --------------------------------------------------------------------------
# The mover
# --------------------------------------------------------------------------

def test_a_mover_over_an_absent_origin_refuses_by_name(tmp_path: Path) -> None:
    """The #804 regression: the mover's receipt names the unreachable origin.

    Before the fix the receipt said only `residency_moved_nothing`, which is
    the same symptom a tier-loop or mover defect produces. The owner can now
    tell a configuration mistake from a mover defect on the first attempt.
    """

    origin = tmp_path / "owner-local"
    entries = [_declared(origin / "shard-1.bin", 4096)]
    args = _args(tmp_path, _manifest(origin, entries), start=0, end=4096)

    receipt = stage_move.move(args)

    assert receipt["complete"] is False
    assert receipt["bytes_staged"] == 0
    assert receipt["entries_staged"] == 0
    assert receipt["refusal"] == "origin_unreachable"
    assert receipt["origin_reachability"]["state"] == "unreachable"
    assert receipt["origin_reachability"]["unreachable"] == [{
        "path": str(origin), "reason": "absent",
        "deepest_existing": str(tmp_path)}]


def test_a_reachable_origin_still_stages_and_is_never_censused(
        tmp_path: Path) -> None:
    """A healthy mover pays nothing for the diagnosis: bytes land, no refusal
    is filed, and no directory census runs over successful work."""

    origin = tmp_path / "shared"
    entries = [_entry(origin / "shard-1.bin", b"a" * 4096)]
    args = _args(tmp_path, _manifest(origin, entries), start=0, end=4096)

    receipt = stage_move.move(args)

    assert receipt["complete"] is True
    assert receipt["bytes_staged"] == 4096
    assert "refusal" not in receipt
    assert "origin_reachability" not in receipt
    staged = Path(args.stage_root) / stage_move.stage_relative(
        "/m/shard-1.bin", 0, 4096, mount_prefix="/m")
    assert staged.read_bytes() == b"a" * 4096


def test_a_missing_file_under_a_present_root_is_not_an_unreachable_origin(
        tmp_path: Path) -> None:
    """A reachable root whose declared file is gone keeps the mover's
    ordinary empty-move refusal: the origin was reachable, so nothing here
    claims otherwise, and the diagnosis says so."""

    origin = tmp_path / "shared"
    origin.mkdir(parents=True)
    args = _args(tmp_path, _manifest(origin, [
        _declared(origin / "never-written.bin", 4096)]), start=0, end=4096)

    receipt = stage_move.move(args)

    assert receipt["complete"] is False
    assert receipt["bytes_staged"] == 0
    assert receipt["refusal"] == "residency_moved_nothing"
    assert receipt["origin_reachability"]["state"] == "reachable"
    assert any("never-written.bin" in error for error in receipt["errors"])


def test_transient_io_never_becomes_a_typed_refusal(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Unknown I/O evidence is conservative on the empty path too: the
    diagnosis reports unknown, and the receipt keeps the ordinary
    empty-move refusal rather than a reachability verdict it cannot prove."""

    def broken(path: str) -> os.stat_result:
        raise OSError(errno.EIO, "Input/output error", path)

    monkeypatch.setattr(stage_move, "_ORIGIN_STAT", broken)
    origin = tmp_path / "shared"
    origin.mkdir(parents=True)
    args = _args(tmp_path, _manifest(origin, [
        _declared(origin / "never-written.bin", 4096)]), start=0, end=4096)

    receipt = stage_move.move(args)

    assert receipt["complete"] is False
    assert receipt["bytes_staged"] == 0
    assert receipt["refusal"] == "residency_moved_nothing"
    assert receipt["origin_reachability"]["state"] == "unknown"
    assert receipt["origin_reachability"]["unreachable"] == []
