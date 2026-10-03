"""The reconcile's stage walk re-reads a directory or a mark only when it moved (#992).

The live ``/stage/prewarm`` held 50,303 prewarm-marked files (302 GB) on
2026-09-26 and the reconcile re-walked and re-probed every one of them every
cycle, deleting none: ``os.walk`` of the whole stage plus
``getxattr(user.pbstage.mover)`` and ``getxattr(user.pbstage.source)`` per
file, 2.81 s of a 10 s ``tier_loop.cycle``.  The stage files had not changed.

These tests pin the two halves of the fix: a steady walk neither lists an
unchanged directory nor probes an unchanged file's marks, and every way a
file can change -- create, unlink, rename, in-place write, mark change,
symlink replacement, a directory that cannot be listed -- is still seen on
the next pass.  The trusted-stamp and version rules are the ones the tier
loop already keeps (#992, #1045): a directory whose stamp holds was not
listed, and a file whose version holds was not probed.  Where no stamp can
be trusted (a network mount, the clock tick a directory changed in) the walk
runs as it always did.

Nothing here touches the live stage.
"""
from __future__ import annotations

import os
from pathlib import Path
import shutil
import sys
import tempfile

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"
                       / "fleet"))
from prismabuild import pool, residency_map  # noqa: E402
import prewarm_loop  # noqa: E402
import stage_move  # noqa: E402
import stage_release  # noqa: E402

TIER = "prismabuild-stage:dl380g10"
CONSUMER = "c" * 64
MOVER = "4" * 64
MANIFEST = "a" * 64


class StageCalls:
    """The filesystem calls a walk makes on the stage, counted for real.

    ``scandir``/``listdir`` are the directory listings, ``getxattr`` the mark
    probes, ``lstat`` the per-entry identity the walk must take every pass.
    Only calls under the stage root are counted.
    """

    def __init__(self, stage: Path, monkeypatch) -> None:
        self.stage = os.path.normpath(str(stage))
        self.scandir = 0
        self.listdir = 0
        self.getxattr = 0
        self.lstat = 0
        self.scandir_paths: list[str] = []
        real_scandir = os.scandir
        real_listdir = os.listdir
        real_getxattr = os.getxattr
        real_lstat = os.lstat

        def under(path) -> bool:
            try:
                probe = os.path.normpath(os.fspath(path))
            except TypeError:
                return False
            return probe == self.stage or probe.startswith(self.stage + os.sep)

        def counted_scandir(path, *args, **kwargs):
            if under(path):
                self.scandir += 1
                self.scandir_paths.append(os.path.normpath(os.fspath(path)))
            return real_scandir(path, *args, **kwargs)

        def counted_listdir(path=".", *args, **kwargs):
            if under(path):
                self.listdir += 1
            return real_listdir(path, *args, **kwargs)

        def counted_getxattr(path, name, *args, **kwargs):
            if under(path):
                self.getxattr += 1
            return real_getxattr(path, name, *args, **kwargs)

        def counted_lstat(path, *args, **kwargs):
            if under(path):
                self.lstat += 1
            return real_lstat(path, *args, **kwargs)

        monkeypatch.setattr(stage_release.os, "scandir", counted_scandir)
        monkeypatch.setattr(stage_release.os, "listdir", counted_listdir)
        monkeypatch.setattr(stage_release.os, "getxattr", counted_getxattr)
        monkeypatch.setattr(stage_release.os, "lstat", counted_lstat)

    def reset(self) -> None:
        self.scandir = self.listdir = self.getxattr = self.lstat = 0
        self.scandir_paths.clear()


def _filesystem_type(path: Path) -> str | None:
    """The type ``/proc/self/mountinfo`` names for ``path``'s device."""

    device = os.stat(path).st_dev
    wanted = f"{os.major(device)}:{os.minor(device)}"
    with open("/proc/self/mountinfo") as stream:
        for line in stream:
            fields = line.split()
            if len(fields) > 2 and fields[2] == wanted and " - " in line:
                return line.split(" - ", 1)[1].split()[0]
    return None


#: Filesystems whose directory times come from this kernel's clock: the only
#: ones whose listings and versions may be kept (``stage_move``).
LOCAL_CLOCK = {"zfs", "ext4", "xfs", "btrfs", "tmpfs"}


def _clock_root(tmp_path: Path) -> Path:
    """A fresh fixture directory on a local-clock filesystem.

    The reuse assertions need a directory whose listings and versions the
    walk may keep, which ``stage_move`` trusts only where the mount table
    names zfs/ext4/xfs/btrfs/tmpfs.  ``/dev/shm`` and ``/tmp`` are tried
    first, so the tests run on any Linux box whose temp storage is one of
    those; a box with none of them skips.  The unrecognized fallback is
    covered separately by :func:`test_a_refused_directory_stamp_lists_again`.
    """

    for candidate in (Path("/dev/shm"), Path("/tmp"), tmp_path):
        try:
            if not candidate.is_dir() or not os.access(candidate, os.W_OK):
                continue
        except OSError:
            continue
        if _filesystem_type(candidate) in LOCAL_CLOCK:
            return Path(tempfile.mkdtemp(prefix="pb-stage-walk-", dir=candidate))
    pytest.skip("no writable local-clock filesystem for the fixture")


def _fleet(root: Path) -> tuple[pool.PoolQueue, Path]:
    queue = pool.PoolQueue(root / "pb-queue")
    queue.ensure_layout()
    stage = root / "stage"
    stage.mkdir(parents=True)
    stage_release.register_stage_root(queue, tier_id=TIER, stage_root=stage)
    return queue, stage


def _staged_file(stage: Path, relative: str, size: int = 4096) -> Path:
    path = stage / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"\0" * size)
    return path


def _mark(path: Path, attribute: str = prewarm_loop.STAGE_SOURCE_XATTR,
          value: bytes = b"/mnt/shared/x@0") -> Path:
    try:
        os.setxattr(path, attribute, value)
    except OSError:
        pytest.skip("this filesystem carries no user extended attributes")
    return path


def _fragment(queue: pool.PoolQueue, paths: list[Path]) -> Path:
    residency_map.write_fragment(queue.root / pool.RESIDENCY, {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": CONSUMER, "mover_action_key": MOVER,
        "tier_id": TIER, "stage_root": str(paths[0].parent),
        "manifest_sha256": MANIFEST,
        "entries": {
            residency_map.residency_map_key(f"/mnt/shared/{index}", 0): {
                "stage_path": str(path), "bytes": path.stat().st_size,
                "sha256": "b" * 64, "offset": 0,
            }
            for index, path in enumerate(paths)
        },
    })
    return residency_map.fragment_path(
        queue.root / pool.RESIDENCY, CONSUMER, MOVER)


def _settle() -> None:
    """Let the coarse clock tick so the first listing's stamp is trusted.

    ``_trusted_directory_stamp`` refuses a version inside the tick it was
    taken in; without the wait the first pass keeps nothing and the reuse
    assertions below would pass for the wrong reason.
    """

    import time
    time.sleep(0.05)


@pytest.fixture()
def stage_fleet(tmp_path: Path):
    root = _clock_root(tmp_path)
    try:
        yield _fleet(root)
    finally:
        shutil.rmtree(root, ignore_errors=True)


def test_a_steady_walk_lists_no_directory_and_probes_no_mark(
        stage_fleet, monkeypatch):
    queue, stage = stage_fleet
    for index in range(12):
        _mark(_staged_file(stage, f"prewarm/p{index % 3}/obj-{index}.bin"))
    _settle()
    index = stage_release.CensusIndex()
    calls = StageCalls(stage, monkeypatch)

    first = stage_release.reconcile(queue, tier_id=TIER,
                                    stage_root=str(stage), wanted=set(),
                                    index=index)
    assert calls.scandir > 0 and calls.getxattr > 0, "the cold pass reads"
    assert first["unowned_left"] == 12, first

    calls.reset()
    second = stage_release.reconcile(queue, tier_id=TIER,
                                     stage_root=str(stage), wanted=set(),
                                     index=index)

    assert calls.scandir == 0, calls.scandir_paths
    assert calls.listdir == 0, calls.scandir_paths
    assert calls.getxattr == 0, "an unchanged file's marks are not probed"
    assert calls.lstat >= 12, "every entry still carries its identity"
    assert second["unowned_left"] == first["unowned_left"] == 12
    assert second["entries_deleted"] == 0


def test_a_created_file_is_seen_and_only_its_directory_is_listed(
        stage_fleet, monkeypatch):
    queue, stage = stage_fleet
    for index in range(6):
        _mark(_staged_file(stage, f"prewarm/obj-{index}.bin"))
    _settle()
    index = stage_release.CensusIndex()
    calls = StageCalls(stage, monkeypatch)
    stage_release.reconcile(queue, tier_id=TIER, stage_root=str(stage),
                            wanted=set(), index=index)
    calls.reset()

    _mark(_staged_file(stage, "prewarm/obj-new.bin"))
    receipt = stage_release.reconcile(queue, tier_id=TIER,
                                      stage_root=str(stage), wanted=set(),
                                      index=index)

    assert receipt["unowned_left"] == 7, receipt
    assert calls.scandir_paths == [os.path.normpath(str(stage / "prewarm"))], (
        calls.scandir_paths)
    assert calls.getxattr >= 1, "the new file is probed"


def test_an_unlinked_file_leaves_the_count(stage_fleet, monkeypatch):
    queue, stage = stage_fleet
    for index in range(6):
        _mark(_staged_file(stage, f"prewarm/obj-{index}.bin"))
    _settle()
    index = stage_release.CensusIndex()
    calls = StageCalls(stage, monkeypatch)
    stage_release.reconcile(queue, tier_id=TIER, stage_root=str(stage),
                            wanted=set(), index=index)
    calls.reset()

    (stage / "prewarm/obj-0.bin").unlink()
    receipt = stage_release.reconcile(queue, tier_id=TIER,
                                      stage_root=str(stage), wanted=set(),
                                      index=index)

    assert receipt["unowned_left"] == 5, receipt
    assert calls.scandir_paths == [os.path.normpath(str(stage / "prewarm"))]


def test_a_renamed_file_is_classified_under_its_new_name(
        stage_fleet, monkeypatch):
    queue, stage = stage_fleet
    _mark(_staged_file(stage, "prewarm/obj-old.bin"))
    _settle()
    index = stage_release.CensusIndex()
    calls = StageCalls(stage, monkeypatch)
    stage_release.reconcile(queue, tier_id=TIER, stage_root=str(stage),
                            wanted=set(), index=index)
    calls.reset()

    os.replace(stage / "prewarm/obj-old.bin", stage / "prewarm/obj-new.bin")
    receipt = stage_release.reconcile(queue, tier_id=TIER,
                                      stage_root=str(stage), wanted=set(),
                                      index=index)

    assert receipt["unowned_left"] == 1, receipt
    assert calls.getxattr >= 1, "the renamed entry is probed"
    assert os.path.exists(stage / "prewarm/obj-new.bin")


def test_an_in_place_write_is_probed_again(stage_fleet, monkeypatch):
    queue, stage = stage_fleet
    target = _mark(_staged_file(stage, "prewarm/obj.bin"))
    _settle()
    index = stage_release.CensusIndex()
    calls = StageCalls(stage, monkeypatch)
    stage_release.reconcile(queue, tier_id=TIER, stage_root=str(stage),
                            wanted=set(), index=index)
    calls.reset()

    target.write_bytes(b"\1" * 4096)

    receipt = stage_release.reconcile(queue, tier_id=TIER,
                                      stage_root=str(stage), wanted=set(),
                                      index=index)

    assert receipt["unowned_left"] == 1, receipt
    assert calls.scandir == 0, "the directory did not move"
    assert calls.getxattr >= 1, "the rewritten file is probed again"


def test_a_changed_mark_is_probed_again(stage_fleet, monkeypatch):
    queue, stage = stage_fleet
    target = _mark(_staged_file(stage, "prewarm/obj.bin"))
    _settle()
    index = stage_release.CensusIndex()
    calls = StageCalls(stage, monkeypatch)
    stage_release.reconcile(queue, tier_id=TIER, stage_root=str(stage),
                            wanted=set(), index=index)
    calls.reset()

    os.setxattr(target, stage_move.STAGE_MOVER_XATTR, MOVER.encode())
    candidates, errors = stage_release._unattributed_candidates(
        stage, stage.resolve(strict=True), set(), set(), memo=index)

    assert errors == []
    kinds = {kind for _path, _identity, kind, _size, _mover in candidates}
    assert kinds == {"mover_residue"}, candidates
    assert calls.getxattr >= 1, "the new mark is read"


def test_a_symlink_replacement_is_not_a_staged_file(stage_fleet, monkeypatch):
    queue, stage = stage_fleet
    target = _mark(_staged_file(stage, "prewarm/obj.bin"))
    _settle()
    index = stage_release.CensusIndex()
    StageCalls(stage, monkeypatch)
    stage_release.reconcile(queue, tier_id=TIER, stage_root=str(stage),
                            wanted=set(), index=index)

    target.unlink()
    os.symlink(stage / "elsewhere", target)

    receipt = stage_release.reconcile(queue, tier_id=TIER,
                                      stage_root=str(stage), wanted=set(),
                                      index=index)

    assert receipt["unowned_left"] == 0, receipt
    assert os.path.islink(target), "a symlink is left alone, not deleted"


def test_a_removed_fragment_still_unattributes_on_a_reused_listing(
        stage_fleet, monkeypatch):
    """Attribution is read fresh even when the stage listing is reused."""

    queue, stage = stage_fleet
    attributed = _staged_file(stage, "prewarm/obj.bin")
    fragment = _fragment(queue, [attributed])
    _settle()
    index = stage_release.CensusIndex()
    calls = StageCalls(stage, monkeypatch)

    first = stage_release.reconcile(queue, tier_id=TIER,
                                    stage_root=str(stage), wanted={MOVER},
                                    index=index)
    assert first["entries_deleted"] == 0, first
    assert attributed.exists()
    calls.reset()

    fragment.unlink()
    second = stage_release.reconcile(queue, tier_id=TIER,
                                     stage_root=str(stage), wanted={MOVER},
                                     index=index)

    assert not attributed.exists(), second
    assert second["entries_deleted"] == 1, second
    assert calls.scandir == 0, "the stage itself did not change"


def test_a_live_pin_is_honored_on_a_reused_listing(stage_fleet, monkeypatch):
    queue, stage = stage_fleet
    pinned = _staged_file(stage, "prewarm/obj.bin")
    _settle()
    index = stage_release.CensusIndex()
    StageCalls(stage, monkeypatch)
    real_live_for = stage_release.reader_lease.live_for
    pins: set[str] = set()

    def with_pins(queue_, wanted, **kwargs):
        owners, taint = real_live_for(queue_, wanted, **kwargs)
        return set(owners) | pins, taint

    monkeypatch.setattr(stage_release.reader_lease, "live_for", with_pins)
    pins.add(str(pinned))
    first = stage_release.reconcile(queue, tier_id=TIER,
                                    stage_root=str(stage), wanted=set(),
                                    index=index)
    assert first["entries_deleted"] == 0 and pinned.exists(), first

    pins.clear()
    second = stage_release.reconcile(queue, tier_id=TIER,
                                     stage_root=str(stage), wanted=set(),
                                     index=index)
    assert not pinned.exists(), second
    assert second["entries_deleted"] == 1, second


def test_a_refused_directory_stamp_lists_again(stage_fleet, monkeypatch):
    """A network mount or the tick a directory changed in keeps no listing.

    Neither a stamp nor a version can be trusted there, so every pass lists
    the directory and probes the mark: the state the memo is in on such a
    box, where the first pass already keeps nothing.
    """

    queue, stage = stage_fleet
    _mark(_staged_file(stage, "prewarm/obj.bin"))
    _settle()
    monkeypatch.setattr(stage_release, "_trusted_directory_stamp",
                        lambda path: None)
    monkeypatch.setattr(stage_release, "_keepable_version",
                        lambda info, fence, **context: None)
    index = stage_release.CensusIndex()
    calls = StageCalls(stage, monkeypatch)
    stage_release.reconcile(queue, tier_id=TIER, stage_root=str(stage),
                            wanted=set(), index=index)
    calls.reset()

    stage_release.reconcile(queue, tier_id=TIER, stage_root=str(stage),
                            wanted=set(), index=index)

    assert calls.scandir > 0, "no stamp means the directory is listed"
    assert calls.getxattr > 0, "no version means the mark is probed"


def _flaky_lstat(monkeypatch, broken: set[str]) -> None:
    """Make ``os.lstat`` fail for ``broken``, wrapping the installed counter."""

    counted = stage_release.os.lstat

    def flaky(path, *args, **kwargs):
        if os.path.normpath(os.fspath(path)) in broken:
            raise OSError("transient input/output error")
        return counted(path, *args, **kwargs)

    monkeypatch.setattr(stage_release.os, "lstat", flaky)


def test_a_name_that_cannot_be_stat_ed_is_retried_on_the_next_pass(
        stage_fleet, monkeypatch):
    """A transient stat failure must not hide an unchanged name forever.

    The failure does not move the directory's version, so a listing kept
    across it would never carry the name again.  The directory must be
    listed again until every name in it can be read.
    """

    queue, stage = stage_fleet
    flaky = _mark(_staged_file(stage, "prewarm/flaky.bin"))
    _mark(_staged_file(stage, "prewarm/steady.bin"))
    _settle()
    index = stage_release.CensusIndex()
    StageCalls(stage, monkeypatch)
    broken = {os.path.normpath(str(flaky))}
    _flaky_lstat(monkeypatch, broken)

    first = stage_release.reconcile(queue, tier_id=TIER,
                                    stage_root=str(stage), wanted=set(),
                                    index=index)
    assert first["unowned_left"] == 1, first
    assert any("flaky.bin" in error for error in first["errors"]), first

    broken.clear()
    second = stage_release.reconcile(queue, tier_id=TIER,
                                     stage_root=str(stage), wanted=set(),
                                     index=index)
    assert second["unowned_left"] == 2, (
        "a name the first pass could not read is read again")


def test_a_stat_failure_on_a_cached_listing_is_retried(
        stage_fleet, monkeypatch):
    """A reused listing must be listed again after one of its names fails."""

    queue, stage = stage_fleet
    flaky = _mark(_staged_file(stage, "prewarm/flaky.bin"))
    _mark(_staged_file(stage, "prewarm/steady.bin"))
    _settle()
    index = stage_release.CensusIndex()
    calls = StageCalls(stage, monkeypatch)
    first = stage_release.reconcile(queue, tier_id=TIER,
                                    stage_root=str(stage), wanted=set(),
                                    index=index)
    assert first["unowned_left"] == 2, first
    calls.reset()

    broken = {os.path.normpath(str(flaky))}
    _flaky_lstat(monkeypatch, broken)
    second = stage_release.reconcile(queue, tier_id=TIER,
                                     stage_root=str(stage), wanted=set(),
                                     index=index)
    assert calls.scandir == 0, "the listing was reused"
    assert second["unowned_left"] == 1, second

    broken.clear()
    third = stage_release.reconcile(queue, tier_id=TIER,
                                    stage_root=str(stage), wanted=set(),
                                    index=index)
    assert third["unowned_left"] == 2, (
        "the name the cached pass could not read is listed again")


def test_a_pin_added_after_the_cached_walk_still_protects(
        stage_fleet, monkeypatch):
    """Attribution and pins are read fresh on a reused listing."""

    queue, stage = stage_fleet
    protected = _staged_file(stage, "prewarm/obj.bin")
    _settle()
    index = stage_release.CensusIndex()
    calls = StageCalls(stage, monkeypatch)
    real_live_for = stage_release.reader_lease.live_for
    pins: set[str] = set()

    def with_pins(queue_, wanted, **kwargs):
        owners, taint = real_live_for(queue_, wanted, **kwargs)
        return set(owners) | pins, taint

    monkeypatch.setattr(stage_release.reader_lease, "live_for", with_pins)
    pins.add(str(protected))
    first = stage_release.reconcile(queue, tier_id=TIER,
                                    stage_root=str(stage), wanted={MOVER},
                                    index=index)
    assert first["entries_deleted"] == 0 and protected.exists(), first
    calls.reset()

    # The pin ends and a fragment covers the file instead.
    pins.clear()
    fragment = _fragment(queue, [protected])
    second = stage_release.reconcile(queue, tier_id=TIER,
                                     stage_root=str(stage), wanted={MOVER},
                                     index=index)
    assert second["entries_deleted"] == 0 and protected.exists(), second
    assert calls.scandir == 0, "the listing was reused"

    fragment.unlink()
    third = stage_release.reconcile(queue, tier_id=TIER,
                                    stage_root=str(stage), wanted={MOVER},
                                    index=index)
    assert not protected.exists(), third
    assert third["entries_deleted"] == 1, third
    assert calls.scandir == 0, "the listing was reused again"
