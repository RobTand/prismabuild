"""A skip checkpoint stands only on a directory stamp a later change must move (#1062).

The stale-mention prune fences each checkpoint on the ``(device, inode,
mtime, ctime)`` stamps of its paths' parent directories.  Every change to a
directory's entries stamps its times from the coarse realtime clock, whose
tick is 1 ms on the Sparks, so two changes in one tick share a stamp.  A stamp
sampled between them is one the second change does not move: a rename that
replaces a leaf in that tick leaves the recorded stamp current, the next pass
hits the checkpoint, and the stale path is never pruned while nothing else
touches the directory.

``stage_move._trusted_directory_stamp`` closes that hole for the #992
listings: it reads the coarse clock first and refuses a stamp whose mtime or
ctime is not strictly before it.  These tests hold the checkpoint to the same
rule.  A directory stamped in the tick of its last change is never cached
over, and the owner takes one extra uncached pass.  A directory stamped after
that tick still installs and hits, on ZFS (the stage dataset) and tmpfs (the
RAM tier) alike, and a later change still moves it.

The tick is modelled, not raced: every directory under the stage reports one
pinned stamp, the coarse clock reports a chosen tick, and the filesystem-type
probe the trusted stamp reads names the filesystem under test.
"""
from __future__ import annotations

import os
from pathlib import Path
import stat as statmod
import sys
import time

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools" / "fleet"))
sys.path.insert(0, str(ROOT / "tests"))

import test_stale_material_done_owner_retires as red  # noqa: E402
from test_stale_material_done_owner_retires import fleet  # noqa: E402,F401
import stage_move  # noqa: E402
import stage_release  # noqa: E402
import tier_loop  # noqa: E402

NAMES = red.NAMES
TIER = red.TIER
COARSE = stage_move._COARSE_REALTIME
TICK_NS = 1_000_000

pytestmark = pytest.mark.skipif(
    COARSE is None, reason="the trusted stamp needs Linux's coarse clock")


@pytest.fixture(autouse=True)
def _fresh_checkpoints():
    stage_release.reset_skip_checkpoints()
    yield
    stage_release.reset_skip_checkpoints()


class _Pinned:
    """One directory's ``lstat`` with its mtime and ctime pinned."""

    def __init__(self, info: os.stat_result, stamp: int) -> None:
        self._info = info
        self.st_mtime_ns = stamp
        self.st_ctime_ns = stamp

    def __getattr__(self, name: str):
        return getattr(self._info, name)


def _pin(monkeypatch, stage: Path, filesystem: str) -> dict[str, int]:
    """Pin the stage's directory stamps and the coarse clock to chosen ticks.

    Every directory at or under ``stage`` reports ``state["stamp"]`` as its
    mtime and ctime; the coarse realtime clock reports ``state["clock"]``;
    the mount-table probe names ``filesystem`` for every device.  Nothing
    else about ``lstat`` or the clocks changes.
    """

    now = time.clock_gettime_ns(COARSE)
    state = {"stamp": now, "clock": now}
    root = os.path.normpath(str(stage))
    real_lstat = os.lstat
    real_clock = time.clock_gettime_ns

    def lstat(path, *args, **kwargs):
        info = real_lstat(path, *args, **kwargs)
        try:
            name = os.path.normpath(os.fspath(path))
        except TypeError:
            return info
        if statmod.S_ISDIR(info.st_mode) and (
                name == root or name.startswith(root + os.sep)):
            return _Pinned(info, state["stamp"])
        return info

    def clock(clock_id):
        if clock_id == COARSE:
            return state["clock"]
        return real_clock(clock_id)

    monkeypatch.setattr(os, "lstat", lstat)
    monkeypatch.setattr(time, "clock_gettime_ns", clock)
    monkeypatch.setattr(stage_move, "_filesystem_type",
                        lambda device: filesystem)
    return state


def _path(stage: Path, name: str) -> Path:
    return stage / red.staged_name(name)


def _pass(queue, stage, index=None):
    return stage_release.sweep_dead_owner_fragments(
        queue, stage_roots={TIER: str(stage)},
        residency_root=queue.residency_fragment_root(), index=index)


def _receipts_for(receipts, mover: str) -> list[dict]:
    return [entry for entry in receipts
            if entry.get("event") == stage_release.STALE_MENTION_EVENT
            and entry.get("action_key") == mover]


def _one(receipts, mover: str) -> dict:
    found = _receipts_for(receipts, mover)
    assert len(found) == 1, receipts
    return found[0]


def _replace_leaf(stage: Path, name: str) -> None:
    """A later publication renamed over one staged leaf: a fresh inode."""

    replacement = red._stage(stage, name + ".later", red.NEW_PAYLOAD)
    os.replace(replacement, _path(stage, name))


def _counts(cache) -> tuple[int, int]:
    counts = tier_loop._read_counts(cache)
    return (counts.get("census_stale_skipped", 0),
            counts.get("census_stale_censused", 0))


# --------------------------------------------------------------------------
# A rename in the tick of the stamp
# --------------------------------------------------------------------------

@pytest.mark.parametrize("filesystem", ["zfs", "tmpfs"])
def test_a_rename_in_the_tick_of_the_stamp_is_seen_on_the_next_pass(
        fleet, monkeypatch, filesystem):
    queue, stage, _ = fleet
    consumer, mover = red.stale_owner(fleet, coherent_names=set(NAMES),
                                      replace=False)
    # The pass samples its directory stamps in the tick of their last change.
    _pin(monkeypatch, stage, filesystem)
    first = _one(_pass(queue, stage), mover)

    # Still in that tick, a rename replaces a leaf: the stamp does not move.
    _replace_leaf(stage, NAMES[1])
    found = _receipts_for(_pass(queue, stage), mover)

    assert len(found) == 1, (
        "the checkpoint hit on the next pass: a stamp sampled in the tick of "
        "its directory's last change was installed, and a rename in that "
        "tick left it current")
    assert found[0]["partial"] is True and found[0]["entries_pruned"] == 1, (
        found[0])
    assert not _path(stage, NAMES[1]).exists()
    assert _path(stage, NAMES[0]).exists()
    assert first["cacheable"] is False, (
        f"a stamp the trusted rule refuses must not be installed: {first}")


# --------------------------------------------------------------------------
# A directory stamped after the tick installs and hits
# --------------------------------------------------------------------------

@pytest.mark.parametrize("filesystem", ["zfs", "tmpfs"])
def test_a_directory_stamped_after_the_tick_installs_and_hits(
        fleet, monkeypatch, filesystem):
    queue, stage, _ = fleet
    consumer, mover = red.stale_owner(fleet, coherent_names=set(NAMES),
                                      replace=False)
    state = _pin(monkeypatch, stage, filesystem)
    state["clock"] = state["stamp"] + TICK_NS
    key = stage_release._skip_checkpoint_key(
        queue, queue.residency_fragment_root(), stage, TIER, consumer, mover)

    assert _one(_pass(queue, stage), mover)["cacheable"] is True
    assert key in stage_release._skip_checkpoints
    assert _receipts_for(_pass(queue, stage), mover) == []

    # A change after the stamped tick moves the stamp: the fence still bites.
    _replace_leaf(stage, NAMES[1])
    state["stamp"] = state["clock"]
    pruned = _one(_pass(queue, stage), mover)
    assert pruned["partial"] is True and pruned["entries_pruned"] == 1, pruned
    assert not _path(stage, NAMES[1]).exists()
    assert _path(stage, NAMES[0]).exists()


@pytest.mark.parametrize("filesystem", ["zfs", "tmpfs"])
def test_a_steady_cycle_caches_after_at_most_one_uncached_pass(
        fleet, monkeypatch, filesystem):
    """The production filesystems keep #1056's steady cycle.

    The first pass lands in the tick of the stage's last change and is
    refused; every later cycle is at least a tick later, so the next pass
    installs and every pass after it skips.
    """

    queue, stage, _ = fleet
    consumer, mover = red.stale_owner(fleet, coherent_names=set(NAMES),
                                      replace=False)
    state = _pin(monkeypatch, stage, filesystem)
    cache = tier_loop.ReceiptCache()

    base = _counts(cache)
    assert _one(_pass(queue, stage, index=cache.census), mover)[
        "cacheable"] is False
    state["clock"] += TICK_NS
    assert _one(_pass(queue, stage, index=cache.census), mover)[
        "cacheable"] is True
    for _ in range(3):
        state["clock"] += TICK_NS
        assert _receipts_for(
            _pass(queue, stage, index=cache.census), mover) == []

    skipped, censused = _counts(cache)
    assert (skipped - base[0], censused - base[1]) == (3, 2), (
        f"two censused passes, then three skips: {_counts(cache)} from {base}")


def test_an_unlisted_filesystem_is_never_cached(fleet, monkeypatch):
    """A network stage root keeps no checkpoint, as it keeps no listing (#992).

    Its directory times come from the server's clock and its client may
    answer from an attribute cache, so no stamp of it proves a rename moved it.
    """

    queue, stage, _ = fleet
    consumer, mover = red.stale_owner(fleet, coherent_names=set(NAMES),
                                      replace=False)
    state = _pin(monkeypatch, stage, "nfs4")
    state["clock"] = state["stamp"] + TICK_NS
    for _ in range(3):
        assert _one(_pass(queue, stage), mover)["cacheable"] is False
