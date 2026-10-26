"""The physical floor consumes a closed snapshot from the token owner."""
from contextlib import contextmanager
import errno

import pytest

from prismabuild import filesystem_floor, immutable_files, pool


@pytest.fixture
def ledger(tmp_path, monkeypatch):
    monkeypatch.setenv(filesystem_floor.MODE_ENV, "off")
    owner = pool.ResourceLedger(tmp_path / "reservations", host="host")
    owner.ensure_capacity({"spool_gb": 6, "local_gib": 2})
    return owner


def test_snapshot_counts_private_committed_transferred_and_orphan_custody(ledger):
    private = ledger.begin_acquire("private", {"spool_gb": 2})
    assert private is not None
    assert ledger.acquire("committed", {"spool_gb": 2, "local_gib": 1})
    assert ledger.transfer_count("committed", "orphan", 1) == 1
    assert ledger.floor_snapshot(["spool_gb", "local_gib"]) == {
        "spool_gb": 4, "local_gib": 1}
    assert ledger.commit_acquire("private", private) == 2
    assert ledger.floor_snapshot(["spool_gb"]) == {"spool_gb": 4}


@pytest.mark.parametrize("corruption", [
    "missing-held", "missing-minted", "missing-free", "missing-token",
    "missing-marker", "duplicate-holder", "duplicate-free", "malformed",
    "symlink-holder", "symlink-token", "unmarked-token",
])
def test_snapshot_refuses_incomplete_or_ambiguous_population(ledger, corruption):
    assert ledger.acquire("owner", {"spool_gb": 1})
    token = ledger.held_dir / "owner" / "spool_gb-0000"
    if corruption.startswith("missing-"):
        target = {
            "missing-held": ledger.held_dir,
            "missing-minted": ledger.minted_dir,
            "missing-free": ledger.free_dir,
            "missing-token": token,
            "missing-marker": ledger.minted_dir / token.name,
        }[corruption]
        target.rename(target.with_name(target.name + ".missing"))
    elif corruption == "duplicate-holder":
        duplicate = ledger.held_dir / "other"
        duplicate.mkdir()
        (duplicate / token.name).touch()
    elif corruption == "duplicate-free":
        (ledger.free_dir / token.name).touch()
    elif corruption == "malformed":
        token.rename(token.with_name("spool_gb-bad"))
    elif corruption == "symlink-holder":
        (ledger.held_dir / "alias").symlink_to(token.parent)
    elif corruption == "symlink-token":
        token.unlink()
        token.symlink_to(ledger.minted_dir / token.name)
    elif corruption == "unmarked-token":
        (token.parent / "spool_gb-9999").touch()
    with pytest.raises(pool.PoolContractError):
        ledger.floor_snapshot(["spool_gb"])


def test_snapshot_refuses_unreadable_holder(ledger, monkeypatch):
    assert ledger.acquire("owner", {"spool_gb": 1})
    scan = immutable_files.scan_required

    def unreadable(path):
        if path == ledger.held_dir / "owner":
            raise PermissionError(errno.EACCES, "unreadable holder", str(path))
        return scan(path)

    monkeypatch.setattr(immutable_files, "scan_required", unreadable)
    with pytest.raises(pool.PoolContractError, match="unreadable holder"):
        ledger.floor_snapshot(["spool_gb"])


def test_snapshot_uses_owner_lock_and_fresh_names(ledger, monkeypatch):
    assert ledger.acquire("owner", {"spool_gb": 1})
    (ledger.held_dir / "owner" / pool.cpu_admission.METADATA).write_text("{}")
    (ledger.held_dir / "owner" / pool.gpu_admission.METADATA).write_text("{}")
    locked = False
    scan = immutable_files.scan_required

    @contextmanager
    def guard(*, blocking):
        nonlocal locked
        assert not blocking
        locked = True
        try:
            yield True
        finally:
            locked = False

    def fresh(path):
        assert locked
        return scan(path)

    def cached(*args, **kwargs):
        pytest.fail("floor snapshot used the cycle's cached names")

    ledger._mutation_guard = guard
    monkeypatch.setattr(immutable_files, "scan_required", fresh)
    monkeypatch.setattr(ledger, "_scan_names", cached)
    assert ledger.floor_snapshot(["spool_gb"]) == {"spool_gb": 1}
    assert not locked


def test_snapshot_refuses_busy_owner(ledger):
    @contextmanager
    def busy(*, blocking):
        yield False

    ledger._mutation_guard = busy
    with pytest.raises(pool.PoolContractError, match="lock"):
        ledger.floor_snapshot(["spool_gb"])


def test_tier_snapshot_uses_the_existing_tier_owner(tmp_path, monkeypatch):
    monkeypatch.setenv(filesystem_floor.MODE_ENV, "off")
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.records.ensure_layout()
    tier = queue.tier("local:host")
    tier.mint({"local_gib": 2})
    assert tier.take("owner", {"local_gib": 1})
    entered = []
    original = tier._ledger._mutation_guard

    @contextmanager
    def owner_lock(*, blocking):
        entered.append(blocking)
        with original(blocking=blocking) as acquired:
            yield acquired

    tier._ledger._mutation_guard = owner_lock
    assert tier.floor_snapshot(["local_gib"]) == {"local_gib": 1}
    assert entered == [False]
