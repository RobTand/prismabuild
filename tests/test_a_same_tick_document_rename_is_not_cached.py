"""A skip checkpoint fences its own documents only at a version a change must move (#1070).

The #1056 skip checkpoint fences an owner's own fragment and material by
their file versions ``(dev, ino, size, mtime_ns, ctime_ns)``.  Every writer
replaces those files by rename (``residency_map.write_fragment``,
``reader_lease.write_material``), so the first change after the census read
normally shows a new inode.  But two rename cycles within one coarse clock
tick can be given the freed inode number the census read, and at the same
size all five fields match: the checkpoint then hits on a changed document
and the pass that should have run is skipped until something else moves a
fence.

#1062 closed that hole for the parent-directory stamps and #1045 for the
co-owner fragments (the census memo keeps only a version older than a clock
read taken before the open).  These tests hold the owner's own fragment and
material to the same rule (``stage_move._keepable_version``): a document
whose ctime is not strictly before a coarse clock read taken before its
``lstat`` is not installed, the owner takes one extra uncached pass, and a
document older than that read still installs and hits.

The tick is modelled, not raced (as in ``test_a_same_tick_rename_is_not_cached``):
the parent directories report a stamp one tick old, the coarse clock reports
a chosen tick, and the document under test reports the pinned version it was
read at -- which a same-tick replacement given the freed inode number would
reproduce.
"""
from __future__ import annotations

import os
from pathlib import Path
import stat as statmod
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools" / "fleet"))
sys.path.insert(0, str(ROOT / "tests"))

import test_stale_material_done_owner_retires as red  # noqa: E402
from test_stale_material_done_owner_retires import fleet  # noqa: E402,F401
import test_a_same_tick_rename_is_not_cached as tick  # noqa: E402
from prismabuild import reader_lease, residency_map  # noqa: E402
import stage_release  # noqa: E402

NAMES = red.NAMES
TICK_NS = tick.TICK_NS

pytestmark = pytest.mark.skipif(
    tick.COARSE is None, reason="the version fence needs Linux's coarse clock")


@pytest.fixture(autouse=True)
def _fresh_checkpoints():
    stage_release.reset_skip_checkpoints()
    yield
    stage_release.reset_skip_checkpoints()


class _Version:
    """One document's ``lstat``: the real one, with its times pinned."""

    def __init__(self, info: os.stat_result, ns: int) -> None:
        self._info = info
        self.st_mtime_ns = ns
        self.st_ctime_ns = ns

    def __getattr__(self, name: str):
        return getattr(self._info, name)


def _documents(queue, consumer: str, mover: str) -> dict[str, Path]:
    root = queue.residency_fragment_root()
    return {"fragment": residency_map.fragment_path(root, consumer, mover),
            "material": reader_lease.material_path(root, consumer, mover)}


def _pin_documents(monkeypatch, documents: dict[str, Path],
                   times: dict[str, int]) -> None:
    """Each document's ``lstat`` reports the pinned times and first inode.

    What a same-tick replacement given the freed inode number looks like:
    the version the census read, reproduced.
    """

    wrapped = os.lstat
    first: dict[str, os.stat_result] = {}
    by_path = {os.path.normpath(str(path)): name
               for name, path in documents.items()}

    def lstat(path, *args, **kwargs):
        info = wrapped(path, *args, **kwargs)
        try:
            name = by_path.get(os.path.normpath(os.fspath(path)))
        except TypeError:
            return info
        if name is None or not statmod.S_ISREG(info.st_mode):
            return info
        first.setdefault(name, info)
        return _Version(first[name], times[name])

    monkeypatch.setattr(os, "lstat", lstat)


def _rewrite_in_place_of(path: Path) -> None:
    """A rename cycle onto the document's name, keeping its bytes' size."""

    raw = path.read_bytes()
    temporary = path.parent / f".{path.name}.same-tick"
    temporary.write_bytes(raw)
    os.replace(temporary, path)


@pytest.mark.parametrize("filesystem", ["zfs", "tmpfs"])
@pytest.mark.parametrize("document", ["fragment", "material"])
def test_a_document_changed_in_the_fence_tick_is_not_fenced_on(
        fleet, monkeypatch, filesystem, document):
    queue, stage, _ = fleet
    consumer, mover = red.stale_owner(fleet, coherent_names=set(NAMES),
                                      replace=False)
    documents = _documents(queue, consumer, mover)
    assert all(path.exists() for path in documents.values())
    state = tick._pin(monkeypatch, stage, filesystem)
    now = state["clock"]
    # The directories changed a tick ago: their stamps are trusted, so only
    # the document under test can refuse the checkpoint.
    state["stamp"] = now - TICK_NS
    times = {name: now - TICK_NS for name in documents}
    times[document] = now                       # changed in the fence tick
    _pin_documents(monkeypatch, documents, times)

    first = tick._one(tick._pass(queue, stage), mover)
    assert first["cacheable"] is False, (
        f"a {document} version read in the tick of its last change was "
        f"installed as a fence: {first}")

    # Still in that tick: a rename cycle onto the name, given the freed
    # inode, reproduces the version the census read.
    _rewrite_in_place_of(documents[document])
    again = tick._receipts_for(tick._pass(queue, stage), mover)
    assert len(again) == 1, (
        "the checkpoint hit on a document replaced in the tick of its "
        "fence: the pass that should have run was skipped")

    # A tick later the same version is older than the clock read: it
    # installs, and the next pass skips (the #1056 saving holds).
    state["clock"] = now + TICK_NS
    assert tick._one(tick._pass(queue, stage), mover)["cacheable"] is True
    assert tick._receipts_for(tick._pass(queue, stage), mover) == []

