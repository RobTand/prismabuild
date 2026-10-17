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

from kept_read_clock import after_paths
from record_store_trust import ObservedTrust

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools" / "fleet"))

from prismabuild import kept_reads
from prismabuild import pool  # noqa: E402
import tier_loop  # noqa: E402
from prismabuild import tier_holdings

TIER = "prismabuild-stage:test-ledger"
MOVER = "a" * 64
OTHER = "b" * 64
KIND = "stage_gib"


def _settle(held: tier_holdings.TierLedger, monkeypatch: pytest.MonkeyPatch) -> None:
    after_paths(monkeypatch, held.paths.base, *(path for path in held.paths.base.rglob('*') if path.is_dir()))


def _names(reader: kept_reads.DirectoryRecords):
    return partial(reader.names, select=lambda _name: True, missing_ok=False)


@pytest.fixture()
def ledger(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    if kept_reads._COARSE_REALTIME is None:
        pytest.fail("ledger stamp qualification requires Linux's coarse clock")
    trust = ObservedTrust()
    trust.fence = lambda: time.clock_gettime_ns(kept_reads._COARSE_REALTIME)
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.records.ensure_layout()
    ledger = queue.tier(TIER)
    ledger.grow_supply({KIND: 4})
    assert ledger.take(MOVER, {KIND: 1})
    _settle(ledger, monkeypatch)
    return queue, ledger, kept_reads.DirectoryRecords(trust=trust)


def _view(ledger: tier_holdings.TierLedger) -> tuple[int, int, int]:
    total, unreadable = ledger.mint_census()
    assert unreadable == []
    return (total.get(KIND, 0), ledger.free_supply().get(KIND, 0), ledger.holdings(MOVER).get(KIND, 0))


def test_a_token_glob_does_not_materialize_unmatched_paths(ledger, monkeypatch):
    """Eliminating directory reads must also eliminate redundant Python work."""
    _queue, held, reader = ledger
    for ordinal in range(64):
        (held.paths.free / f'unrelated-{ordinal:04d}.tmp').touch()
    _settle(held, monkeypatch)
    pattern = f"{KIND}-*"
    directory = held.paths.free
    expected = sorted(directory.glob(pattern))
    original = type(directory).__truediv__
    materialized = []

    def join_path(parent, name):
        if parent == directory:
            materialized.append(name)
        return original(parent, name)

    monkeypatch.setattr(type(directory), "__truediv__", join_path)
    with pool.tier_ledger_names_from(_names(reader)):
        assert held.token_paths(directory, pattern) == expected
    assert sorted(materialized) == sorted(path.name for path in expected), (
        "the token filter constructed or sorted unrelated paths")


@pytest.mark.parametrize("pattern", ["*", "stage_gib-*", "*.tmp", "[a-m]*", "missing-*"])
def test_cached_token_selection_keeps_legacy_order(ledger, pattern, monkeypatch):
    _queue, held, reader = ledger
    for name in ("aux-10.tmp", "aux-2.tmp", ".hidden.tmp", "AUX-1.tmp", "é.tmp"):
        (held.paths.free / name).touch()
    _settle(held, monkeypatch)
    expected = sorted(held.paths.free.glob(pattern))
    with pool.tier_ledger_names_from(_names(reader)):
        assert held.token_paths(held.paths.free, pattern) == expected
        assert held.token_paths(held.paths.free, pattern) == expected


@pytest.mark.parametrize("change,expected", [
    ("acquire", (4, 2, 1)),
    ("release", (4, 4, 0)),
    ("grow", (6, 5, 1)),
    ("shrink", (2, 1, 1)),
    ("retire-held", (3, 3, 0)),
    ("unlink", (3, 2, 1)),
    ("rename-holder", (4, 3, 0)),
])
def test_a_warm_census_sees_the_next_token_transition(ledger, change, expected, monkeypatch):
    _queue, held, reader = ledger
    with pool.tier_ledger_names_from(_names(reader)):
        assert _view(held) == (4, 3, 1)
        listed = reader.listed
        assert _view(held) == (4, 3, 1)
        assert reader.listed == listed and reader.kept > 0
        if change == "acquire":
            assert held.take(OTHER, {KIND: 1})
        elif change == "release":
            assert held.release_unspent(MOVER, authority=tier_holdings.Unspent.NEVER_STARTED) == 1
        elif change == "grow":
            held.grow_supply({KIND: 6})
        elif change == "shrink":
            assert held.trim_supply({KIND: 2}) == {KIND: 2}
        elif change == "retire-held":
            assert held.retire_duplicate(MOVER, {KIND: 1}) == {KIND: 1}
        elif change == "unlink":
            next(held.paths.free.glob(f'{KIND}-*')).unlink()
        else:
            os.rename(held.paths.held / MOVER, held.paths.held / OTHER)
        assert _view(held) == expected
        _settle(held, monkeypatch)
        assert _view(held) == expected
        listed = reader.listed
        assert _view(held) == expected
        assert reader.listed == listed
        if change == "rename-holder":
            assert held.holdings(OTHER) == {KIND: 1}
        if change == "retire-held":
            assert held.mint_history()[1]


@pytest.mark.parametrize("filesystem", ["nfs", "nfs4", None])
def test_a_warm_ledger_is_not_kept_after_filesystem_trust_leaves(
        ledger, monkeypatch, filesystem):
    _queue, held, reader = ledger
    with pool.tier_ledger_names_from(_names(reader)):
        assert _view(held) == (4, 3, 1)
        assert _view(held) == (4, 3, 1)
        listed, kept = reader.listed, reader.kept
        reader.trust.available = False
        assert _view(held) == (4, 3, 1)
        assert _view(held) == (4, 3, 1)
        assert reader.listed > listed
        assert reader.kept == kept


def test_a_same_tick_listing_does_not_hide_an_added_token(ledger, monkeypatch):
    _queue, held, reader = ledger
    real_lstat = os.lstat
    real_clock = time.clock_gettime_ns
    tick = real_clock(kept_reads._COARSE_REALTIME)

    class Pinned:
        def __init__(self, info):
            self.info = info
            self.st_mtime_ns = self.st_ctime_ns = tick

        def __getattr__(self, name):
            return getattr(self.info, name)

    def lstat(path, *args, **kwargs):
        info = real_lstat(path, *args, **kwargs)
        if isinstance(path, (str, os.PathLike)) and str(path).startswith(str(held.paths.base)):
            return Pinned(info)
        return info

    def clock(clock_id):
        return tick if clock_id == kept_reads._COARSE_REALTIME else real_clock(clock_id)

    monkeypatch.setattr(os, "lstat", lstat)
    monkeypatch.setattr(time, "clock_gettime_ns", clock)
    with pool.tier_ledger_names_from(_names(reader)):
        assert _view(held) == (4, 3, 1)
        (held.paths.free / f'{KIND}-0004').touch()
        assert _view(held) == (5, 4, 1)
        assert reader.kept == 0


@pytest.mark.parametrize("namespace", ["free", "held", "holder", "minted"])
def test_an_unreadable_namespace_cannot_authorize_minting(
        ledger, monkeypatch, namespace):
    _queue, held, reader = ledger
    with pool.tier_ledger_names_from(_names(reader)):
        assert _view(held) == (4, 3, 1)
        held.grow_supply({KIND: 4})
        _settle(held, monkeypatch)
        held.grow_supply({KIND: 4})
        before = {p.name for p in held.paths.free.iterdir()}
        directory = {'free': held.paths.free, 'held': held.paths.held, 'holder': held.paths.held / MOVER, 'minted': held.paths.minted}[namespace]
        # Refuse the warm stamp before injecting the actual listing failure.
        reader.trust.available = False
        real = os.listdir

        def unreadable(path):
            if os.fspath(path) == str(directory):
                raise OSError(errno.EIO, "ledger census unreadable", str(directory))
            return real(path)

        monkeypatch.setattr(os, "listdir", unreadable)
        held.grow_supply({KIND: 6})
        # Verify physical state without sending the verifier through the
        # injected failure (Path.iterdir uses os.listdir on Python 3.12).
        assert set(real(held.paths.free)) == before
        if namespace == "holder":
            total, unknown = held.mint_census()
            assert total == {KIND: 3}
            assert len(unknown) == 1 and unknown[0]["errno"] == errno.EIO
            with pytest.raises(OSError, match="ledger census unreadable"):
                held.holder_state(MOVER)
            assert held.trim_supply({KIND: 2}) == {KIND: 2}
            assert held.mint_issues is not None
            assert len(real(held.paths.held / MOVER)) == 1


def test_a_missing_required_directory_is_not_a_cached_empty(ledger):
    _queue, held, reader = ledger
    with pool.tier_ledger_names_from(_names(reader)):
        assert _view(held) == (4, 3, 1)
        missing = held.paths.minted / 'missing'
        with pytest.raises(FileNotFoundError):
            held.entry_paths(missing)
        missing.mkdir()
        token = missing / f"{KIND}-0004"
        token.touch()
        assert held.entry_paths(missing) == [token]
        token.unlink()
        missing.rmdir()
        assert held.holder_state(OTHER) == set()
        assert reader.names(missing, select=lambda _name: True) == frozenset()


def test_optional_dead_absence_is_proved_by_its_parent_and_tracks_creation(
        ledger, monkeypatch):
    _queue, held, reader = ledger
    dead = held.paths.minted / 'dead'
    probes = []
    real = os.listdir

    def listdir(path):
        if os.fspath(path) == str(dead):
            probes.append(str(path))
        return real(path)

    monkeypatch.setattr(os, "listdir", listdir)
    with pool.tier_ledger_names_from(_names(reader)):
        assert held.entry_paths(dead, optional=True) == []
        assert held.entry_paths(dead, optional=True) == []
        assert probes == [] and reader.kept > 0
        # An optional absence proof must not leak into a required census.
        with pytest.raises(FileNotFoundError):
            held.entry_paths(dead)
        probes.clear()
        dead.mkdir()
        token = dead / f"{KIND}-0004"
        token.touch()
        assert held.entry_paths(dead, optional=True) == [token]
        assert probes == [str(dead)]
        shutil.rmtree(dead)
        assert held.entry_paths(dead, optional=True) == []


@pytest.mark.parametrize("failure", ["parent", "child", "non-directory"])
def test_an_optional_dead_namespace_never_hides_an_unknown_parent_or_child(
        ledger, monkeypatch, failure):
    _queue, held, reader = ledger
    dead = held.paths.minted / 'dead'
    if failure == "non-directory":
        dead.touch()
    elif failure == "child":
        dead.mkdir()
    _settle(held, monkeypatch)
    reader.trust.available = False
    real = os.listdir
    target = held.paths.minted if failure == 'parent' else dead

    def listdir(path):
        if failure != "non-directory" and os.fspath(path) == str(target):
            raise OSError(errno.EIO, "optional census unreadable", str(target))
        return real(path)

    monkeypatch.setattr(os, "listdir", listdir)
    with pool.tier_ledger_names_from(_names(reader)):
        with pytest.raises(OSError):
            held.entry_paths(dead, optional=True)
        monkeypatch.setattr(os, "listdir", real)
        if failure == "non-directory":
            dead.unlink()
        dead.mkdir(exist_ok=True)
        token = dead / f"{KIND}-0004"
        token.touch()
        assert held.entry_paths(dead, optional=True) == [token]


def test_a_replaced_holder_directory_does_not_keep_its_old_names(ledger):
    _queue, held, reader = ledger
    with pool.tier_ledger_names_from(_names(reader)):
        assert _view(held) == (4, 3, 1)
        directory = held.paths.held / MOVER
        shutil.rmtree(directory)
        directory.mkdir()
        assert _view(held) == (3, 3, 0)


def test_host_ledgers_and_reads_outside_the_cycle_are_fresh(ledger):
    queue, held, reader = ledger
    host = queue.ledger("test-host")
    host.ensure_capacity({"cpu": 2})
    assert not host._strict_census()
    assert held.uses_tier_census()
    with pool.tier_ledger_names_from(_names(reader)):
        with host._mutation_locked() as acquired:
            assert acquired
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
    unused = held.paths.minted
    expected = sorted(unused.iterdir())
    real = os.listdir

    def unreadable(path):
        if os.fspath(path) == str(unused):
            raise OSError(errno.EIO, "unused census unreadable", str(unused))
        return real(path)

    with pool.tier_ledger_names_from(_names(receipts.ledger_records)):
        assert held.entry_paths(unused) == expected
        monkeypatch.setattr(os, "listdir", unreadable)
        assert held.entry_paths(unused) == expected

    def fail(*args, **kwargs):
        assert _view(held) == (4, 3, 1)
        raise RuntimeError("cycle failed after ledger read")

    monkeypatch.setattr(tier_loop, "_cycle", fail)
    with pytest.raises(RuntimeError, match="cycle failed after ledger read"):
        tier_loop.cycle(queue, host="test-host", source_pool="test-pool",
                        receipts=receipts)
    assert pool._TIER_LEDGER_NAMES.get() is None
    with pool.tier_ledger_names_from(_names(receipts.ledger_records)):
        with pytest.raises(OSError, match="unused census unreadable"):
            held.entry_paths(unused)
    assert tier_loop.LAST_CYCLE["completed"] is False
    assert tier_loop.LAST_CYCLE["reads"]["ledger_listed"] > 0
