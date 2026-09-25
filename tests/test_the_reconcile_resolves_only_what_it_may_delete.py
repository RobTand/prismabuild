"""The stage reconcile resolves only a file it may delete (#1073).

On generation ``02b27a8804d3`` the tier loop spent 48.1% of its samples on
one line of ``stage_release._unattributed_candidates``: the containment check
``stage_resolved not in path.resolve().parents``, run on every regular file
of the stage walk before attribution or the prewarm mark.  The live stage
held 54,400 files, and 38,422 of them were unowned but marked, so they are
only counted and never deleted.  For those files, and for attributed ones,
the check decided nothing.

The fix runs the check only for a file about to become a deletable
candidate, still after the ``lstat`` that captures its identity, because the
locked re-check in ``reconcile`` compares identities and never containment.
These tests pin the two halves of that: a file whose directory resolves
outside the stage is still never deleted, and on an ordinary tree the census
and the deletions are exactly what the old code produced.
"""
from __future__ import annotations

import os
from pathlib import Path
import shutil
import stat as statmod
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"
                       / "fleet"))
from prismabuild import pool, residency_map, storage_tiers  # noqa: E402
import prewarm_loop  # noqa: E402
import stage_release  # noqa: E402

CONSUMER = "c" * 64
MOVER = "4" * 64
TIER = "prismabuild-stage:dl380g10"


def _old_unattributed_candidates(stage: Path, stage_resolved: Path,
                                 attributed: set[str]):
    """``_unattributed_candidates`` as of ``02b27a8804d3``, verbatim.

    The reference the equivalence tests compare against: containment was
    checked on every regular file, before attribution and the mark.
    """

    candidates: list[tuple[Path, tuple | None, bool]] = []
    errors: list[str] = []
    for base, _directories, names in os.walk(stage):
        for name in sorted(names):
            path = Path(base) / name
            if (name == stage_release.STAGE_ROOT_MARKER
                    and Path(base) == stage):
                continue
            if (name == storage_tiers.RAM_EPOCH_MARKER
                    and Path(base) == stage):
                continue
            try:
                info = os.lstat(path)
                if not statmod.S_ISREG(info.st_mode):
                    continue
                if stage_resolved not in path.resolve().parents:
                    continue
            except OSError as exc:
                errors.append(f"{path.name}: {exc}")
                continue
            if os.path.normpath(str(path)) in attributed:
                continue
            partial = stage_release._is_mover_partial(name)
            if not partial:
                if prewarm_loop._STAGE_TEMPORARY.search(name):
                    continue
                marked = stage_release._marked_by_the_prewarm_stage(path)
                if marked is None or marked:
                    candidates.append((path, None, False))
                    continue
            candidates.append(
                (path, stage_release._metadata_version(info), partial))
    return candidates, errors


def _as_candidates(walk):
    """``walk``'s candidates in the shape ``reconcile`` reads since #1088.

    The old walk decides every candidate; this only adds what the receipt
    now sizes.  It had no mover kind, so a file it left carries the source
    mark or an unanswerable one, and a file it may delete is a partial or
    unmarked.
    """

    def lifted(stage, stage_resolved, attributed, named=None):
        candidates, errors = walk(stage, stage_resolved, attributed)
        out = []
        for path, identity, partial in candidates:
            if identity is None:
                kind = ("source_mark_only"
                        if stage_release._marked_by_the_prewarm_stage(path)
                        else "mark_unanswerable")
            else:
                kind = "partial" if partial else "unmarked"
            out.append((path, identity, kind, os.lstat(path).st_size, ""))
        return out, errors

    return lifted


def _staged_file(stage: Path, relative: str, size: int = 4096) -> Path:
    path = stage / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"\0" * size)
    return path


def _mark(path: Path) -> None:
    try:
        os.setxattr(path, prewarm_loop.STAGE_SOURCE_XATTR, b"/mnt/shared/x@0")
    except OSError:
        pytest.skip("this filesystem carries no user extended attributes")


def _fragment(queue: pool.PoolQueue, stage: Path, paths: list[Path]) -> None:
    residency_map.write_fragment(queue.root / pool.RESIDENCY, {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": CONSUMER, "mover_action_key": MOVER,
        "tier_id": TIER, "stage_root": str(stage),
        "manifest_sha256": "a" * 64,
        "entries": {
            residency_map.residency_map_key(f"/mnt/shared/{index}", 0): {
                "stage_path": str(path), "bytes": path.stat().st_size,
                "sha256": "b" * 64, "offset": 0,
            }
            for index, path in enumerate(paths)
        },
    })


def _fleet(root: Path) -> tuple[pool.PoolQueue, Path]:
    queue = pool.PoolQueue(root / "pb-queue")
    queue.ensure_layout()
    stage = root / "stage"
    stage.mkdir(parents=True)
    stage_release.register_stage_root(queue, tier_id=TIER, stage_root=stage)
    return queue, stage


def _ordinary_tree(queue: pool.PoolQueue, stage: Path) -> None:
    """Attributed, marked, unmarked, partial and temporary files, nested."""

    attributed = [
        _staged_file(stage, f"models/m-{index}.safetensors.pbrange/0-4096")
        for index in range(4)]
    _fragment(queue, stage, attributed)
    for index in range(5):
        _mark(_staged_file(stage, f"prewarm/p-{index}.pbrange/0-4096"))
    # An attributed file the prewarm stage also marked: attribution wins.
    both = _staged_file(stage, "models/both.pbrange/0-4096")
    _mark(both)
    _fragment(queue, stage, attributed + [both])
    for index in range(3):
        _staged_file(stage, f"orphans/o-{index}.pbrange/0-4096")
    _staged_file(stage, "orphans/.o-9.bin.partial")
    _staged_file(stage, "prewarm/obj.bin.pbstage@0+4096.7.1.tmp")
    _staged_file(stage, "top-level-orphan.bin")
    os.symlink(stage / "top-level-orphan.bin", stage / "a-symlink.bin")


def _snapshot(stage: Path) -> set[str]:
    return {str(path.relative_to(stage)) for path in stage.rglob("*")
            if path.is_file() or path.is_symlink()}


# ---- a directory that resolves outside the stage ---------------------------

@pytest.fixture()
def swapped(tmp_path: Path, monkeypatch):
    """A stage whose walk meets a directory that resolves outside it.

    ``os.walk`` does not follow directory symlinks, so a directory seen as a
    symlink by the walk is one swapped for a symlink while the walk ran.
    Following links in the walk stands in for that race: the walk yields
    ``stage/swapped`` while every path under it resolves to ``outside``.
    """

    queue, stage = _fleet(tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir()
    os.symlink(outside, stage / "swapped")
    real_walk = os.walk
    monkeypatch.setattr(stage_release.os, "walk",
                        lambda top, **kwargs: real_walk(
                            top, **{**kwargs, "followlinks": True}))
    return queue, stage, outside


def test_an_unmarked_orphan_under_a_directory_outside_the_stage_stays(swapped):
    queue, stage, outside = swapped
    beyond = _staged_file(outside, "unmarked.bin")
    control = _staged_file(stage, "orphans/unmarked.bin")

    receipt = stage_release.reconcile(queue, tier_id=TIER,
                                      stage_root=str(stage), wanted=set())

    assert beyond.exists(), "a path resolving outside the stage is not ours"
    # The in-stage twin is deleted, so the file outside reached the rule
    # that deletes and was refused by containment alone.
    assert not control.exists(), receipt
    assert receipt["entries_deleted"] == 1, receipt


def test_a_mover_partial_under_a_directory_outside_the_stage_stays(swapped):
    queue, stage, outside = swapped
    beyond = _staged_file(outside, ".shard.bin.partial")
    control = _staged_file(stage, "orphans/.shard.bin.partial")

    receipt = stage_release.reconcile(queue, tier_id=TIER,
                                      stage_root=str(stage), wanted=set())

    assert beyond.exists(), "a partial resolving outside the stage is not ours"
    assert not control.exists(), receipt
    assert receipt["partials_deleted"] == 1, receipt


def test_no_candidate_carries_an_identity_for_a_path_outside_the_stage(
        swapped):
    """The walk result itself: nothing outside is ever handed to the lock."""

    _queue, stage, outside = swapped
    _staged_file(outside, "unmarked.bin")
    _staged_file(outside, ".shard.bin.partial")
    candidates, errors = stage_release._unattributed_candidates(
        stage, stage.resolve(strict=True), set())
    assert errors == []
    assert not [path for path, identity, *_rest in candidates
                if identity is not None], candidates


# ---- equivalence on an ordinary tree ----------------------------------------

def test_the_walk_returns_what_the_old_walk_returned(tmp_path: Path):
    queue, stage = _fleet(tmp_path)
    _ordinary_tree(queue, stage)
    attributed = stage_release.attributed_stage_paths(queue, wanted={MOVER})
    assert attributed, "the fragment must attribute something"
    resolved = stage.resolve(strict=True)

    new = stage_release._unattributed_candidates(stage, resolved, attributed)
    old = _old_unattributed_candidates(stage, resolved, attributed)

    # Since #1088 a candidate also carries its kind, size and writer; on a
    # tree no mover marked, the old three fields are exactly the old walk's.
    candidates, errors = new
    assert ([(path, identity, kind == "partial")
             for path, identity, kind, _size, _mover in candidates],
            errors) == old
    assert sum(1 for one in candidates if one[1] is None) == 5
    assert sum(1 for one in candidates if one[1] is not None) == 5
    assert sorted(one[2] for one in candidates) == (
        ["partial"] + ["source_mark_only"] * 5 + ["unmarked"] * 4)


def test_reconcile_counts_and_deletes_what_the_old_code_did(
        tmp_path: Path, monkeypatch):
    receipts, left_behind = [], []
    for arm in ("old", "new"):
        queue, stage = _fleet(tmp_path / arm)
        _ordinary_tree(queue, stage)
        with monkeypatch.context() as patch:
            if arm == "old":
                patch.setattr(stage_release, "_unattributed_candidates",
                              _as_candidates(_old_unattributed_candidates))
            receipts.append(stage_release.reconcile(
                queue, tier_id=TIER, stage_root=str(stage), wanted={MOVER}))
        left_behind.append(_snapshot(stage))
        shutil.rmtree(tmp_path / arm)

    old, new = receipts
    for field in ("unowned_left", "entries_deleted", "partials_deleted",
                  "bytes_deleted", "entries_judged", "left_since_walk",
                  "errors", "complete"):
        assert new[field] == old[field], (field, old, new)
    assert new["unowned_left"] == 5
    assert new["entries_deleted"] == 5
    assert left_behind[0] == left_behind[1]
