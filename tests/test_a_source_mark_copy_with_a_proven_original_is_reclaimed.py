"""Source-mark-only stage copies with proven originals are reclaimed (#1636).

A staged range names its own original, so the verb needs no receipt.
The dry run lists each copy with its proof. Apply moves only
digest-equal pairs to quarantine on another device. Referenced
copies never move. Restore round-trips.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
from prismabuild import pool, residency_map, storage_tiers  # noqa: E402
import prewarm_loop  # noqa: E402
import stage_reclaim  # noqa: E402
import stage_release  # noqa: E402
from stage_move import stage_relative  # noqa: E402

TIER = "prismabuild-stage:dl380g10"
KIND = "stage_gib"
SIZE = 4096
NAMES = ["alpha.bin", "beta.bin"]


def _staged(stage: Path, mount: Path, name: str) -> Path:
    return stage / stage_relative(f"{mount}/{name}", 0, SIZE,
                                  mount_prefix=str(mount))


def _stage_copy(stage: Path, mount: Path, name: str,
                payload: bytes) -> Path:
    path = _staged(stage, mount, name)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    os.setxattr(path, prewarm_loop.STAGE_SOURCE_XATTR, b"0:0:0")
    return path


@pytest.fixture()
def fleet(tmp_path: Path, monkeypatch):
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    stage = tmp_path / "stage"
    stage.mkdir()
    assert stage_release.register_stage_root(
        queue, tier_id=TIER, stage_root=stage) == "registered"
    mount = tmp_path / "originals"
    mount.mkdir()
    good = hashlib.sha256(b"g" * SIZE).digest() * (SIZE // 32)
    for name in NAMES:
        (mount / name).write_bytes(good)
        _stage_copy(stage, mount, name, good)
    quarantine = tmp_path / "quarantine"
    quarantine.mkdir()
    real_stat = os.stat

    class _Stat:
        def __init__(self, wrapped: os.stat_result, dev: int) -> None:
            self._wrapped = wrapped
            self._dev = dev

        def __getattr__(self, name: str) -> object:
            if name == "st_dev":
                return self._dev
            return getattr(self._wrapped, name)

    def _fake_stat(path, *args, **kwargs):
        info = real_stat(path, *args, **kwargs)
        text = os.fspath(path)
        if text == str(stage_resolved[0]) or text.startswith(
                str(stage_resolved[0]) + os.sep):
            return _Stat(info, 11)
        if text == str(quarantine_resolved[0]) or text.startswith(
                str(quarantine_resolved[0]) + os.sep):
            return _Stat(info, 12)
        return info

    stage_resolved = [stage.resolve()]
    quarantine_resolved = [quarantine.resolve()]
    monkeypatch.setattr(os, "stat", _fake_stat)
    return queue, stage, mount, quarantine, good


def _reclaim(fleet, **kwargs):
    queue, stage, mount, quarantine, _good = fleet
    params = {"tier_id": TIER, "stage_root": str(stage),
              "mount_prefix": str(mount),
              "quarantine_root": str(quarantine),
              "run_id": "run-1"}
    params.update(kwargs)
    return stage_reclaim.reclaim_source_marks(queue, **params)


def test_dry_run_lists_proven_pairs_and_changes_nothing(fleet) -> None:
    queue, stage, mount, _quarantine, _good = fleet
    before = {name: _staged(stage, mount, name).read_bytes()
              for name in NAMES}
    before_xattr = {
        name: os.getxattr(_staged(stage, mount, name),
                          prewarm_loop.STAGE_SOURCE_XATTR)
        for name in NAMES}

    got = _reclaim(fleet)

    assert got["complete"] is True and got["applied"] is False
    assert got["entries_paired"] == len(NAMES)
    assert got["bytes_paired"] == SIZE * len(NAMES)
    assert "entries_moved" not in got
    for row in got["entries"]:
        assert row["status"] == "paired"
        assert row["original_path"] == str(fleet[2] / row["stage_rel"][:-len(
            ".pbrange/0-4096")])
        assert len(row["sha256"]) == 64
    for name in NAMES:
        assert _staged(stage, mount, name).read_bytes() == before[name]
        assert os.getxattr(_staged(stage, mount, name),
                           prewarm_loop.STAGE_SOURCE_XATTR) == before_xattr[
            name]


def test_apply_moves_only_digest_equal_pairs_to_quarantine(fleet) -> None:
    queue, stage, mount, quarantine, good = fleet
    other = hashlib.sha256(b"x" * SIZE).digest() * (SIZE // 32)
    (mount / "gamma.bin").write_bytes(other)
    _stage_copy(stage, mount, "gamma.bin", good)

    got = _reclaim(fleet, apply=True, run_id="run-apply")

    assert got["complete"] is True
    assert got["entries_moved"] == len(NAMES)
    assert got["bytes_moved"] == SIZE * len(NAMES)
    assert got["refused_reasons"] == {"digest_differs": 1}
    for name in NAMES:
        assert not _staged(stage, mount, name).exists()
        assert (quarantine / "run-apply"
                / _staged(stage, mount, name).relative_to(stage)).exists()
    assert _staged(stage, mount, "gamma.bin").exists()
    manifest = json.loads(
        (quarantine / "run-apply" / "manifest.json").read_text())
    assert manifest["schema"] == stage_reclaim.MANIFEST_SCHEMA_V1
    assert len(manifest["entries"]) == len(NAMES)


def test_a_crash_between_copy_and_removal_loses_no_data(
        fleet, monkeypatch) -> None:
    queue, stage, mount, quarantine, good = fleet
    target = _staged(stage, mount, NAMES[0])
    copied: list[str] = []
    real_copy = stage_reclaim._copy_to_quarantine
    real_unlink = os.unlink

    def _record(source, dest, size):
        got = real_copy(source, dest, size)
        if got is not None:
            copied.append(str(source))
        return got

    def _crash(path, *args, **kwargs):
        if os.fspath(path) == str(target):
            raise OSError("killed between copy and removal")
        return real_unlink(path, *args, **kwargs)

    monkeypatch.setattr(stage_reclaim, "_copy_to_quarantine", _record)
    monkeypatch.setattr(os, "unlink", _crash)

    got = _reclaim(fleet, apply=True, run_id="run-crash")

    # The stage copy is unlinked only after its quarantine copy
    # verifies, so the crash loses nothing: the copy verified, the
    # stage copy stands with its bytes, and the journal names the
    # move the manifest does not yet claim.
    assert str(target) in copied
    assert got["entries_moved"] == len(NAMES) - 1
    assert target.read_bytes() == good
    run_dir = quarantine / "run-crash"
    journaled = stage_reclaim._read_journal(run_dir)
    assert sorted(one["stage_rel"] for one in journaled) == sorted(
        str(_staged(stage, mount, name).relative_to(stage))
        for name in NAMES)
    manifest = stage_reclaim.read_manifest(run_dir)
    assert len(manifest["entries"]) == len(NAMES) - 1
    assert any(str(target.relative_to(stage)) in str(error)
               for error in got["errors"])
    # The manifest names only the completed move, and it restores.
    revived = stage_reclaim.restore_run(
        queue, stage_root=str(stage),
        quarantine_root=str(quarantine), run_id="run-crash")
    assert revived["entries_restored"] == len(NAMES) - 1


def test_a_crash_before_the_manifest_still_restores(fleet) -> None:
    queue, stage, mount, quarantine, _good = fleet
    run_dir = quarantine / "run-journal"
    run_dir.mkdir(parents=True)
    target = _staged(stage, mount, NAMES[0])
    dest = run_dir / target.relative_to(stage)
    sha = stage_reclaim._copy_to_quarantine(target, dest, SIZE)
    assert sha is not None
    captured = stage_reclaim._xattrs_and_times(target)
    assert captured is not None
    xattrs, times = captured
    stage_reclaim._append_journal(run_dir, {
        "stage_path": str(target),
        "stage_rel": str(target.relative_to(stage)),
        "original_path": str(mount / NAMES[0]),
        "offset": 0, "size": SIZE, "sha256": sha,
        "xattrs": xattrs, "times": times,
        "quarantine_path": str(dest)})
    target.unlink()

    restored = stage_reclaim.restore_run(
        queue, stage_root=str(stage),
        quarantine_root=str(quarantine), run_id="run-journal")

    assert restored["complete"] is True
    assert restored["entries_restored"] == 1
    assert target.read_bytes() == dest.read_bytes()


def test_unpaired_copies_never_move(fleet) -> None:
    queue, stage, mount, _quarantine, good = fleet
    (mount / NAMES[0]).unlink()
    short = mount / NAMES[1]
    short.write_bytes(b"s" * (SIZE - 1))

    got = _reclaim(fleet, apply=True, run_id="run-unpaired")

    assert got["entries_moved"] == 0
    assert got["refused_reasons"] == {"original_missing": 1,
                                      "original_short": 1}
    for name in NAMES:
        assert _staged(stage, mount, name).exists()


def test_a_produced_output_copy_never_moves(fleet) -> None:
    queue, stage, mount, _quarantine, good = fleet
    lane = stage / "produced-output" / ("a" * 64)
    staged = lane / (NAMES[0] + ".pbrange") / f"0-{SIZE}"
    staged.parent.mkdir(parents=True, exist_ok=True)
    staged.write_bytes(good)
    os.setxattr(staged, prewarm_loop.STAGE_SOURCE_XATTR, b"0:0:0")

    got = _reclaim(fleet, apply=True, run_id="run-lane")

    assert staged.exists()
    assert got["refused_reasons"].get("produced_output_lane") == 1


def test_a_fragment_owner_never_moves(fleet) -> None:
    queue, stage, mount, _quarantine, _good = fleet
    consumer, mover = "c" * 64, "d" * 64
    residency_map.write_fragment(queue.root / pool.RESIDENCY, {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": consumer, "mover_action_key": mover,
        "tier_id": TIER, "stage_root": str(stage),
        "manifest_sha256": "e" * 64,
        "entries": {
            residency_map.residency_map_key(
                str(_staged(stage, mount, NAMES[0])), 0): {
                "stage_path": str(_staged(stage, mount, NAMES[0])),
                "bytes": SIZE, "sha256": "f" * 64, "offset": 0}}})

    got = _reclaim(fleet, apply=True, run_id="run-fragment")

    assert got["entries_moved"] == 1
    assert got["entries_paired"] == 1
    assert got["refused_reasons"] == {"attributed": 1}
    assert _staged(stage, mount, NAMES[0]).exists()
    assert not _staged(stage, mount, NAMES[1]).exists()


def test_a_pin_owner_never_moves(fleet, monkeypatch) -> None:
    queue, stage, mount, _quarantine, _good = fleet
    pinned = os.path.normpath(str(_staged(stage, mount, NAMES[0])))
    monkeypatch.setattr(
        stage_reclaim.stage_release.reader_lease, "live_for",
        lambda *a, **k: ({pinned: ["pin-1"]}, []))

    got = _reclaim(fleet, apply=True, run_id="run-pin")

    assert got["entries_moved"] == 1
    assert _staged(stage, mount, NAMES[0]).exists()


def test_an_in_flight_claim_never_moves(fleet, monkeypatch) -> None:
    queue, stage, mount, _quarantine, _good = fleet
    rel = str(_staged(stage, mount, NAMES[0]).relative_to(stage))
    monkeypatch.setattr(stage_reclaim.stage_release, "_claimed_paths",
                        lambda *a, **k: ({rel}, []))

    got = _reclaim(fleet, apply=True, run_id="run-claim")

    assert got["entries_moved"] == 1
    assert _staged(stage, mount, NAMES[0]).exists()


def test_a_promotion_handoff_never_moves(fleet, monkeypatch) -> None:
    queue, stage, mount, _quarantine, _good = fleet
    pinned = os.path.normpath(str(_staged(stage, mount, NAMES[0])))
    monkeypatch.setattr(stage_reclaim.stage_release,
                        "_claimed_source_paths",
                        lambda *a, **k: ({pinned}, []))

    got = _reclaim(fleet, apply=True, run_id="run-handoff")

    assert got["entries_moved"] == 1
    assert _staged(stage, mount, NAMES[0]).exists()


def test_a_mover_in_flight_refuses_the_pass(fleet) -> None:
    queue, stage, mount, _quarantine, _good = fleet
    mover = "9" * 64
    queue.publish(action_key=mover, cas_root=str(queue.root / "cas"),
                  checkout_root=str(queue.root),
                  worker_script="w.py", tags=["dl380g10"],
                  resources={"cpu": 1, "mem_gb": 1,
                             f"stage_gib@{TIER}": 1},
                  residency={"schema": pool.RESIDENCY_SCHEMA_V1,
                             "manifest_sha256": "a" * 64,
                             "manifest_bytes": SIZE, "tier_id": TIER,
                             "range_start_bytes": 0,
                             "range_end_bytes": SIZE})

    got = _reclaim(fleet, apply=True, run_id="run-flight")

    assert got["skipped"] == "movers_in_flight"
    for name in NAMES:
        assert _staged(stage, mount, name).exists()


def test_an_unreadable_reference_record_refuses_the_pass(
        fleet, monkeypatch) -> None:
    queue, stage, mount, _quarantine, _good = fleet
    monkeypatch.setattr(stage_reclaim.stage_release,
                        "_attributed_census",
                        lambda *a, **k: (set(), ["unreadable"]))

    got = _reclaim(fleet)

    assert got["complete"] is False
    assert got["skipped"] == "attribution_unreadable"
    for name in NAMES:
        assert _staged(stage, mount, name).exists()


def test_restore_round_trips_bytes_marks_and_times(fleet) -> None:
    queue, stage, mount, quarantine, _good = fleet
    before = {name: (
        _staged(stage, mount, name).read_bytes(),
        os.getxattr(_staged(stage, mount, name),
                    prewarm_loop.STAGE_SOURCE_XATTR),
        os.stat(_staged(stage, mount, name))) for name in NAMES}

    got = _reclaim(fleet, apply=True, run_id="run-restore")
    assert got["entries_moved"] == len(NAMES)
    restored = stage_reclaim.restore_run(
        queue, stage_root=str(stage),
        quarantine_root=str(quarantine), run_id="run-restore")

    assert restored["complete"] is True
    assert restored["entries_restored"] == len(NAMES)
    for name in NAMES:
        path = _staged(stage, mount, name)
        assert path.read_bytes() == before[name][0]
        assert os.getxattr(path,
                            prewarm_loop.STAGE_SOURCE_XATTR) == before[
            name][1]
        live = os.stat(path)
        assert live.st_mtime_ns == before[name][2].st_mtime_ns
        assert live.st_atime_ns == before[name][2].st_atime_ns
    again = stage_reclaim.restore_run(
        queue, stage_root=str(stage),
        quarantine_root=str(quarantine), run_id="run-restore")
    assert again["entries_restored"] == 0
    assert again["entries_skipped_identical"] == len(NAMES)


def test_restore_refuses_a_conflicting_destination(fleet) -> None:
    queue, stage, mount, quarantine, _good = fleet
    got = _reclaim(fleet, apply=True, run_id="run-conflict")
    assert got["entries_moved"] == len(NAMES)
    _staged(stage, mount, NAMES[0]).parent.mkdir(parents=True,
                                                 exist_ok=True)
    _staged(stage, mount, NAMES[0]).write_bytes(b"z" * SIZE)

    restored = stage_reclaim.restore_run(
        queue, stage_root=str(stage),
        quarantine_root=str(quarantine), run_id="run-conflict")

    assert restored["complete"] is False
    assert restored["entries_restored"] == 1


def test_next_mint_exposes_the_reclaimed_room(fleet) -> None:
    queue, stage, mount, quarantine, _good = fleet

    def staged_bytes() -> int:
        total = 0
        for root, _dirs, files in os.walk(stage):
            for name in files:
                if name == stage_release.STAGE_ROOT_MARKER:
                    continue
                total += (Path(root) / name).stat().st_size
        return total

    held_staged = staged_bytes()
    assert held_staged == SIZE * len(NAMES)
    # The fake dataset sits one moved byte below a whole GiB, so the
    # reclaimed bytes cross the mint boundary the tier_tokens math
    # reads. The mint needs no ledger edit: capacity is available.
    room_before = 2 * (1024 ** 3) - held_staged
    mint_before = storage_tiers.tier_tokens(
        {"tier": "stage", "capacity_bytes": room_before})
    assert mint_before == {KIND: 1}
    queue.mint_tier_capacity(TIER, mint_before)
    assert queue.tier_ledger(TIER).acquire("b" * 64, {KIND: 2}) is False

    got = _reclaim(fleet, apply=True, run_id="run-mint")

    assert got["complete"] is True
    assert got["bytes_moved"] == held_staged
    assert staged_bytes() == 0
    room_after = room_before + got["bytes_moved"]
    mint_after = storage_tiers.tier_tokens(
        {"tier": "stage", "capacity_bytes": room_after})
    assert mint_after == {KIND: 2}
    queue.mint_tier_capacity(TIER, mint_after)
    # Admission sees the room the mint exposes.
    assert queue.tier_ledger(TIER).acquire("c" * 64, {KIND: 2}) is True
    assert queue.tier_ledger(TIER).held() == {KIND: 2}


def test_quarantine_on_the_stage_device_is_refused(fleet) -> None:
    queue, stage, mount, _quarantine, _good = fleet

    got = _reclaim(fleet, apply=True, quarantine_root=str(stage / "q"))

    assert got["complete"] is False
    assert "quarantine" in str(got["skipped"])
    for name in NAMES:
        assert _staged(stage, mount, name).exists()


def test_a_run_id_outside_its_directory_is_refused(fleet) -> None:
    queue, stage, mount, _quarantine, _good = fleet

    got = _reclaim(fleet, apply=True, run_id="../escape")

    assert got["complete"] is False
    assert "run_id" in str(got["skipped"])
    for name in NAMES:
        assert _staged(stage, mount, name).exists()


def test_a_ranged_name_inside_a_ranged_name_still_pairs(fleet) -> None:
    queue, stage, mount, quarantine, good = fleet
    nested = "nested.pbrange/0-5"
    (mount / "nested.pbrange").mkdir(parents=True, exist_ok=True)
    (mount / nested).write_bytes(good)
    staged = stage / stage_relative(
        f"{mount}/{nested}", 0, SIZE, mount_prefix=str(mount))
    staged.parent.mkdir(parents=True, exist_ok=True)
    staged.write_bytes(good)
    os.setxattr(staged, prewarm_loop.STAGE_SOURCE_XATTR, b"0:0:0")

    got = _reclaim(fleet)

    assert got["complete"] is True
    assert got["entries_paired"] == len(NAMES) + 1
    nested_row = [row for row in got["entries"]
                  if row["stage_rel"] == str(
                      staged.relative_to(stage))]
    assert len(nested_row) == 1
    assert nested_row[0]["original_path"] == str(mount / nested)


def test_an_apply_reuses_a_dry_run_memo(fleet, monkeypatch) -> None:
    queue, stage, mount, quarantine, _good = fleet
    memo_path = quarantine / "dry-memo.json"

    dry = _reclaim(fleet, run_id="memo-run", memo_out=str(memo_path))

    assert dry["complete"] is True
    assert dry["entries_paired"] == len(NAMES)
    assert dry["memo_hits"] == 0
    assert dry["memo"] == str(memo_path)
    assert memo_path.exists()
    real_prove = stage_reclaim.prove_pair
    calls: list[str] = []

    def _count(stage_path, original, offset, size):
        calls.append(str(stage_path))
        return real_prove(stage_path, original, offset, size)

    monkeypatch.setattr(stage_reclaim, "prove_pair", _count)

    got = _reclaim(fleet, apply=True, run_id="run-memo",
                   memo_path=memo_path)

    assert got["complete"] is True
    assert got["entries_moved"] == len(NAMES)
    assert got["memo_hits"] == len(NAMES)
    assert calls == []
    assert (quarantine / "run-memo" / "memo.json").exists()


def test_a_memo_entry_for_a_changed_file_rehashes(fleet) -> None:
    queue, stage, mount, quarantine, _good = fleet
    memo_path = quarantine / "stale-memo.json"
    dry = _reclaim(fleet, run_id="memo-stale", memo_out=str(memo_path))
    assert dry["entries_paired"] == len(NAMES)
    changed = mount / NAMES[0]
    changed.write_bytes(b"n" * SIZE)

    got = _reclaim(fleet, run_id="memo-stale-2", memo_path=memo_path)

    assert got["complete"] is True
    assert got["memo_hits"] == len(NAMES) - 1
    assert got["refused_reasons"] == {"digest_differs": 1}


def test_an_unreadable_ledger_refuses_the_pass(fleet, monkeypatch) -> None:
    queue, stage, mount, _quarantine, _good = fleet

    def _refuse(_tier_id):
        raise pool.PoolContractError("ledger gone")

    monkeypatch.setattr(queue, "tier_ledger", _refuse)

    got = _reclaim(fleet)

    assert got["complete"] is False
    assert "ledger_unreadable" in str(got["skipped"])
    for name in NAMES:
        assert _staged(stage, mount, name).exists()
