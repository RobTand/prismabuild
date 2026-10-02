"""Tier token names reuse trusted stamps without hiding mutations or errors (#1027)."""
from __future__ import annotations

import errno
from functools import partial
import os
from pathlib import Path
import shutil
import sys
import time

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools" / "fleet"))

from prismabuild import pool  # noqa: E402
import stage_move  # noqa: E402
import stage_release  # noqa: E402
import tier_loop  # noqa: E402

TIER = "prismabuild-stage:test-ledger"
MOVER = "a" * 64
OTHER = "b" * 64
KIND = "stage_gib"


def _settle() -> None:
    # Only executed inside an admitted test action, never on the coordinator.
    time.sleep(0.05)


def _names(reader: stage_release.DirectoryRecords):
    return partial(reader.names, select=lambda _name: True, missing_ok=False)


@pytest.fixture()
def ledger(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    if stage_move._COARSE_REALTIME is None:
        pytest.fail("ledger stamp qualification requires Linux's coarse clock")
    monkeypatch.setattr(stage_move, "_filesystem_type", lambda _device: "tmpfs")
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    ledger = queue.tier_ledger(TIER)
    ledger.ensure_capacity({KIND: 4})
    assert ledger.acquire(MOVER, {KIND: 1})
    _settle()
    return queue, ledger, stage_release.DirectoryRecords()


def _view(ledger: pool.ResourceLedger) -> tuple[int, int, int]:
    total, unreadable = ledger.capacity_census()
    assert unreadable == []
    return (total.get(KIND, 0), ledger.available().get(KIND, 0),
            ledger.holder_tokens(MOVER).get(KIND, 0))


def test_a_token_glob_does_not_materialize_unmatched_paths(ledger, monkeypatch):
    """Eliminating directory reads must also eliminate redundant Python work."""
    _queue, held, reader = ledger
    for ordinal in range(64):
        (held.free_dir / f"unrelated-{ordinal:04d}.tmp").touch()
    _settle()
    pattern = f"{KIND}-*"
    directory = held.free_dir
    expected = sorted(directory.glob(pattern))
    original = type(directory).__truediv__
    materialized = []

    def join_path(parent, name):
        if parent == directory:
            materialized.append(name)
        return original(parent, name)

    monkeypatch.setattr(type(directory), "__truediv__", join_path)
    with pool.tier_ledger_names_from(_names(reader)):
        assert held._census_glob(directory, pattern) == expected
    assert sorted(materialized) == sorted(path.name for path in expected), (
        "the token filter constructed or sorted unrelated paths")


@pytest.mark.parametrize("pattern", ["*", "stage_gib-*", "*.tmp", "[a-m]*", "missing-*"])
def test_cached_token_selection_keeps_legacy_order(ledger, pattern):
    _queue, held, reader = ledger
    for name in ("aux-10.tmp", "aux-2.tmp", ".hidden.tmp", "AUX-1.tmp", "é.tmp"):
        (held.free_dir / name).touch()
    _settle()
    expected = sorted(held.free_dir.glob(pattern))
    with pool.tier_ledger_names_from(_names(reader)):
        assert held._census_glob(held.free_dir, pattern) == expected
        assert held._census_glob(held.free_dir, pattern) == expected


@pytest.mark.parametrize("change,expected", [
    ("acquire", (4, 2, 1)),
    ("release", (4, 4, 0)),
    ("grow", (6, 5, 1)),
    ("shrink", (2, 1, 1)),
    ("retire-held", (3, 3, 0)),
    ("unlink", (3, 2, 1)),
    ("rename-holder", (4, 3, 0)),
])
def test_a_warm_census_sees_the_next_token_transition(ledger, change, expected):
    _queue, held, reader = ledger
    with pool.tier_ledger_names_from(_names(reader)):
        assert _view(held) == (4, 3, 1)
        listed = reader.listed
        assert _view(held) == (4, 3, 1)
        assert reader.listed == listed and reader.kept > 0
        if change == "acquire":
            assert held.acquire(OTHER, {KIND: 1})
        elif change == "release":
            assert held.release(MOVER) == 1
        elif change == "grow":
            held.ensure_capacity({KIND: 6})
        elif change == "shrink":
            assert held.retire_free_capacity({KIND: 2}) == {KIND: 2}
        elif change == "retire-held":
            assert held.retire_held(MOVER, {KIND: 1}) == {KIND: 1}
        elif change == "unlink":
            next(held.free_dir.glob(f"{KIND}-*")).unlink()
        else:
            os.rename(held.held_dir / MOVER, held.held_dir / OTHER)
        assert _view(held) == expected
        _settle()
        assert _view(held) == expected
        listed = reader.listed
        assert _view(held) == expected
        assert reader.listed == listed
        if change == "rename-holder":
            assert held.holder_tokens(OTHER) == {KIND: 1}
        if change == "retire-held":
            assert held._minted_and_dead_names()[1]


@pytest.mark.parametrize("filesystem", ["nfs", "nfs4", None])
def test_a_warm_ledger_is_not_kept_after_filesystem_trust_leaves(
        ledger, monkeypatch, filesystem):
    _queue, held, reader = ledger
    with pool.tier_ledger_names_from(_names(reader)):
        assert _view(held) == (4, 3, 1)
        assert _view(held) == (4, 3, 1)
        listed, kept = reader.listed, reader.kept
        monkeypatch.setattr(stage_move, "_filesystem_type",
                            lambda _device: filesystem)
        assert _view(held) == (4, 3, 1)
        assert _view(held) == (4, 3, 1)
        assert reader.listed > listed
        assert reader.kept == kept


def test_a_same_tick_listing_does_not_hide_an_added_token(ledger, monkeypatch):
    _queue, held, reader = ledger
    real_lstat = os.lstat
    real_clock = time.clock_gettime_ns
    tick = real_clock(stage_move._COARSE_REALTIME)

    class Pinned:
        def __init__(self, info):
            self.info = info
            self.st_mtime_ns = self.st_ctime_ns = tick

        def __getattr__(self, name):
            return getattr(self.info, name)

    def lstat(path, *args, **kwargs):
        info = real_lstat(path, *args, **kwargs)
        if isinstance(path, (str, os.PathLike)) and str(path).startswith(str(held.base)):
            return Pinned(info)
        return info

    def clock(clock_id):
        return tick if clock_id == stage_move._COARSE_REALTIME else real_clock(clock_id)

    monkeypatch.setattr(os, "lstat", lstat)
    monkeypatch.setattr(time, "clock_gettime_ns", clock)
    with pool.tier_ledger_names_from(_names(reader)):
        assert _view(held) == (4, 3, 1)
        (held.free_dir / f"{KIND}-0004").touch()
        assert _view(held) == (5, 4, 1)
        assert reader.kept == 0


@pytest.mark.parametrize("namespace", ["free", "held", "holder", "minted"])
def test_an_unreadable_namespace_cannot_authorize_minting(
        ledger, monkeypatch, namespace):
    _queue, held, reader = ledger
    with pool.tier_ledger_names_from(_names(reader)):
        assert _view(held) == (4, 3, 1)
        held.ensure_capacity({KIND: 4})
        _settle()
        held.ensure_capacity({KIND: 4})
        before = {p.name for p in held.free_dir.iterdir()}
        directory = {"free": held.free_dir, "held": held.held_dir,
                     "holder": held.held_dir / MOVER,
                     "minted": held.minted_dir}[namespace]
        # Refuse the warm stamp before injecting the actual listing failure.
        monkeypatch.setattr(stage_move, "_filesystem_type", lambda _device: "nfs4")
        real = os.listdir

        def unreadable(path):
            if os.fspath(path) == str(directory):
                raise OSError(errno.EIO, "ledger census unreadable", str(directory))
            return real(path)

        monkeypatch.setattr(os, "listdir", unreadable)
        held.ensure_capacity({KIND: 6})
        # Verify physical state without sending the verifier through the
        # injected failure (Path.iterdir uses os.listdir on Python 3.12).
        assert set(real(held.free_dir)) == before
        if namespace == "holder":
            total, unknown = held.capacity_census()
            assert total == {KIND: 3}
            assert len(unknown) == 1 and unknown[0]["errno"] == errno.EIO
            with pytest.raises(OSError, match="ledger census unreadable"):
                pool.held_names_visible(held, MOVER)
            assert held.retire_free_capacity({KIND: 2}) == {KIND: 2}
            assert held.last_census_report is not None
            assert len(real(held.held_dir / MOVER)) == 1


def test_a_missing_required_directory_is_not_a_cached_empty(ledger):
    _queue, held, reader = ledger
    with pool.tier_ledger_names_from(_names(reader)):
        assert _view(held) == (4, 3, 1)
        missing = held.minted_dir / "missing"
        with pytest.raises(FileNotFoundError):
            held._census_scan(missing)
        assert str(missing) not in reader._names
        assert pool.held_names_visible(held, OTHER) == set()
        assert reader.names(missing, select=lambda _name: True) == frozenset()


def test_optional_dead_absence_is_proved_by_its_parent_and_tracks_creation(
        ledger, monkeypatch):
    _queue, held, reader = ledger
    dead = held.minted_dir / "dead"
    probes = []
    real = os.listdir

    def listdir(path):
        if os.fspath(path) == str(dead):
            probes.append(str(path))
        return real(path)

    monkeypatch.setattr(os, "listdir", listdir)
    with pool.tier_ledger_names_from(_names(reader)):
        assert held._census_scan(dead, optional=True) == []
        assert held._census_scan(dead, optional=True) == []
        assert probes == [] and reader.kept > 0
        # An optional absence proof must not leak into a required census.
        with pytest.raises(FileNotFoundError):
            held._census_scan(dead)
        probes.clear()
        dead.mkdir()
        token = dead / f"{KIND}-0004"
        token.touch()
        assert held._census_scan(dead, optional=True) == [token]
        assert probes == [str(dead)]
        shutil.rmtree(dead)
        assert held._census_scan(dead, optional=True) == []


@pytest.mark.parametrize("failure", ["parent", "child", "non-directory"])
def test_an_optional_dead_namespace_never_hides_an_unknown_parent_or_child(
        ledger, monkeypatch, failure):
    _queue, held, reader = ledger
    dead = held.minted_dir / "dead"
    if failure == "non-directory":
        dead.touch()
    elif failure == "child":
        dead.mkdir()
    _settle()
    monkeypatch.setattr(stage_move, "_filesystem_type", lambda _device: "nfs4")
    real = os.listdir
    target = held.minted_dir if failure == "parent" else dead

    def listdir(path):
        if failure != "non-directory" and os.fspath(path) == str(target):
            raise OSError(errno.EIO, "optional census unreadable", str(target))
        return real(path)

    monkeypatch.setattr(os, "listdir", listdir)
    with pool.tier_ledger_names_from(_names(reader)):
        with pytest.raises(OSError):
            held._census_scan(dead, optional=True)
        assert str(dead) not in reader._names


def test_a_replaced_holder_directory_does_not_keep_its_old_names(ledger):
    _queue, held, reader = ledger
    with pool.tier_ledger_names_from(_names(reader)):
        assert _view(held) == (4, 3, 1)
        directory = held.held_dir / MOVER
        shutil.rmtree(directory)
        directory.mkdir()
        assert _view(held) == (3, 3, 0)


def test_host_ledgers_and_reads_outside_the_cycle_are_fresh(ledger):
    queue, held, reader = ledger
    host = queue.ledger("test-host")
    host.ensure_capacity({"cpu": 2})
    with pool.tier_ledger_names_from(_names(reader)):
        assert host.available() == {"cpu": 2}
        assert host.capacity_census() == ({"cpu": 2}, [])
        assert reader.listed == reader.kept == 0
        assert _view(held) == (4, 3, 1)
    listed, kept = reader.listed, reader.kept
    assert _view(held) == (4, 3, 1)
    assert (reader.listed, reader.kept) == (listed, kept)
    assert pool._TIER_LEDGER_NAMES.get() is None


def test_a_failed_cycle_resets_scope_and_forgets_unused_directories(ledger, monkeypatch):
    queue, held, _reader = ledger
    receipts = tier_loop.ReceiptCache()
    unused = held.minted_dir
    receipts.ledger_records.names(unused, select=lambda _name: True)
    assert str(unused) in receipts.ledger_records._names

    def fail(*args, **kwargs):
        assert _view(held) == (4, 3, 1)
        raise RuntimeError("cycle failed after ledger read")

    monkeypatch.setattr(tier_loop, "_cycle", fail)
    with pytest.raises(RuntimeError, match="cycle failed after ledger read"):
        tier_loop.cycle(queue, host="test-host", source_pool="test-pool",
                        receipts=receipts)
    assert pool._TIER_LEDGER_NAMES.get() is None
    assert str(unused) not in receipts.ledger_records._names
    assert tier_loop.LAST_CYCLE["completed"] is False
    assert tier_loop.LAST_CYCLE["reads"]["ledger_listed"] > 0
