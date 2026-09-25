"""A publisher's ``mkdir`` survives a concurrent egress pruning it (#1008 item 2).

``_copy_one`` makes the destination's parent directory before its temporary
exists, outside the stage ownership lock (``stage_move.py``,
:func:`stage_move._Copier._copy_one`).  An egress prunes empty directories
after it releases that same lock (``stage_release._prune_empty``, see the
comment at ``stage_release.py`` around :func:`stage_release._evict_locked`'s
caller).  Between the mkdir and the temporary's creation the directory holds
nothing, so a concurrent prune can remove it, and the temporary's own creation
then failed with ``FileNotFoundError`` -- this already happens on main, #1005
having only moved the *censuses* out of the lock, never touching this gap.

The fix retries the ``mkdir`` once, on that specific ``FileNotFoundError``,
the same single retry ``residency_map._write_atomic`` already takes for its
own fragment directory against the identical race (grep ``FileNotFoundError``
around ``mkdir`` in ``residency_map.py``).  It is bounded by the number of
observed races (one), never by time: a second removal would need a second
sweep to land inside this window, which the comment there also argues is not
worth guarding twice.

This test reproduces the race deterministically with two threads and a hook
at the racy point: a monkeypatched ``Path.mkdir`` blocks the copying thread
right after it creates the (still-empty) destination directory, until a
second thread has removed it -- exactly the interleaving a real concurrent
egress could produce, made certain rather than probabilistic.
"""
from __future__ import annotations

import hashlib
import os
from pathlib import Path
import sys
import threading

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

import prewarm_loop  # noqa: E402
import stage_move  # noqa: E402

#: Bounds a broken run; a passing run never waits it out.
BOUND_S = 10.0


def _fleet(tmp_path: Path) -> tuple[Path, Path, dict[str, object]]:
    """A stage root and one real source nested under a fresh subdirectory.

    Nested (``sub/shard.bin``, not directly under the mount) so the staged
    destination's parent is itself a fresh directory the copy must create --
    the stage root already exists, so creating it costs exactly one
    ``mkdir`` call, with no recursive ancestor creation to also intercept.
    """

    mount = tmp_path / "mnt"
    (mount / "sub").mkdir(parents=True)
    stage = tmp_path / "stage"
    stage.mkdir()
    payload = b"q" * 4096
    source = mount / "sub" / "shard.bin"
    source.write_bytes(payload)
    entry = {"path": str(source), "offset": 0, "bytes": len(payload),
             "sha256": hashlib.sha256(payload).hexdigest()}
    destination = stage / stage_move.stage_relative(
        str(source), 0, len(payload), mount_prefix=str(mount))
    return mount, stage, entry, destination


def test_a_publishers_mkdir_survives_a_concurrent_empty_dir_prune(
        tmp_path: Path, monkeypatch) -> None:
    """RED on the pre-fix tree: the pruned directory fails the copy outright."""

    mount, stage, entry, destination = _fleet(tmp_path)
    real_mkdir = Path.mkdir
    made, pruned = threading.Event(), threading.Event()
    hits: list[Path] = []

    def hook(self, *args, **kwargs):
        result = real_mkdir(self, *args, **kwargs)
        if self == destination.parent and not hits:
            hits.append(self)
            made.set()
            assert pruned.wait(BOUND_S), "the pruning thread never ran"
        return result

    monkeypatch.setattr(Path, "mkdir", hook)

    def prune() -> None:
        assert made.wait(BOUND_S), "the destination directory was never made"
        # The exact race #1008 item 2 names: an egress pruning an empty
        # directory (``stage_release._prune_empty``) lands in the gap
        # between the publisher's ``mkdir`` and its temporary's creation.
        os.rmdir(destination.parent)
        pruned.set()

    pruner = threading.Thread(target=prune)
    pruner.start()
    try:
        copier = stage_move._Copier(
            mounts=prewarm_loop.MountMap([f"{mount}={mount}"]),
            pacer=None, stage_root=stage, mount_prefix=str(mount),
            block=1 << 16, workers=1, owner="a" * 40)
        copier.run([entry], stop=threading.Event())
    finally:
        pruner.join(BOUND_S)

    assert hits == [destination.parent], "the hook never saw the racy mkdir"
    assert not pruner.is_alive()
    assert copier.errors == [], (
        f"the copy failed on the pruned directory instead of retrying: "
        f"{copier.errors}")
    assert copier.staged, "nothing landed"
    assert destination.read_bytes() == b"q" * 4096
    assert destination.parent.is_dir()


def test_two_racing_creators_of_the_same_fresh_directory_both_land(
        tmp_path: Path, monkeypatch) -> None:
    """Two concurrent copies of different entries under one new directory:
    the second's ``mkdir`` finds the first's already there (``exist_ok``),
    and neither ever needs the retry -- the ordinary, non-racing case stays
    byte-identical."""

    mount = tmp_path / "mnt"
    (mount / "sub").mkdir(parents=True)
    stage = tmp_path / "stage"
    stage.mkdir()
    entries = []
    for name, payload in (("a.bin", b"a" * 4096), ("b.bin", b"b" * 4096)):
        source = mount / "sub" / name
        source.write_bytes(payload)
        entries.append({"path": str(source), "offset": 0,
                        "bytes": len(payload),
                        "sha256": hashlib.sha256(payload).hexdigest()})

    copier = stage_move._Copier(
        mounts=prewarm_loop.MountMap([f"{mount}={mount}"]),
        pacer=None, stage_root=stage, mount_prefix=str(mount),
        block=1 << 16, workers=2, owner="b" * 40)
    copier.run(entries, stop=threading.Event())

    assert copier.errors == []
    assert len(copier.staged) == 2
    for name, payload in (("a.bin", b"a" * 4096), ("b.bin", b"b" * 4096)):
        destination = stage / stage_move.stage_relative(
            str(mount / "sub" / name), 0, len(payload), mount_prefix=str(mount))
        assert destination.read_bytes() == payload
