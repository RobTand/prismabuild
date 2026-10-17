"""Unknown filesystem observations cannot free a RAM host mirror (ASTRA01)."""
from __future__ import annotations

import errno
import os
from pathlib import Path

import pytest

from fleet_world import FleetWorld
from prismabuild import storage_tiers

HOST = "dl380g10"
TIER = storage_tiers.tier_id("ram", HOST)
MOVER = "a" * 64
MIRROR = storage_tiers.RAM_HOST_MEMORY_PREFIX + MOVER


def _occupied(tmp_path: Path):
    queue = FleetWorld(tmp_path, queue_name="queue").queue
    host = queue.ledger(HOST)
    host.ensure_capacity({"cpu": 2, "mem_gb": 8})
    tier = queue.tier(TIER)
    tier.grow_supply({"ram_gib": 8})
    assert tier.take(MOVER, {"ram_gib": 2})
    data = tmp_path / "ram-data"
    data.write_bytes(b"still staged")
    return tier, host, data


def _names(directory: Path) -> set[str]:
    try:
        return set(os.listdir(directory))
    except FileNotFoundError:
        return set()


@pytest.mark.parametrize("release_orphans", [False, True])
def test_an_unreadable_tier_holder_retains_its_host_token_names(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, release_orphans: bool):
    tier, host, data = _occupied(tmp_path)
    directory = tier.paths.held / MOVER
    tier_names = _names(directory)
    host_names = _names(host.held_dir / MIRROR)
    real_scandir = os.scandir
    faults = []

    def unreadable(path):
        if not isinstance(path, int) and os.fspath(path) == str(directory):
            faults.append(str(path))
            raise PermissionError(errno.EACCES, "RAM holder scan denied", str(path))
        return real_scandir(path)

    with monkeypatch.context() as patch:
        patch.setattr(os, "scandir", unreadable)
        verdict = tier.reconcile_mirror((), release_orphans=release_orphans)

    assert faults, "the real directory scan must reach the injected EACCES"
    assert _names(directory) == tier_names
    assert data.read_bytes() == b"still staged"
    assert _names(host.held_dir / MIRROR) == host_names
    assert verdict["converged"] is False
    assert verdict["events"] == []
    assert tier.reconcile_mirror((), release_orphans=release_orphans)["converged"] is True
    assert _names(host.held_dir / MIRROR) == host_names


def test_an_unresolved_tier_directory_cannot_make_its_mirror_an_orphan(tmp_path: Path):
    tier, host, data = _occupied(tmp_path)
    directory = tier.paths.held / MOVER
    tier_names = _names(directory)
    host_names = _names(host.held_dir / MIRROR)
    retained = tmp_path / "retained-tier-holder"
    directory.rename(retained)
    directory.symlink_to(directory.name, target_is_directory=True)
    try:
        with pytest.raises(OSError) as error:
            directory.stat()
        assert error.value.errno == errno.ELOOP
        verdict = tier.reconcile_mirror(())
    finally:
        directory.unlink()
        retained.rename(directory)

    assert _names(directory) == tier_names
    assert data.read_bytes() == b"still staged"
    assert _names(host.held_dir / MIRROR) == host_names
    assert verdict["converged"] is False
    assert verdict["events"] == []
    assert tier.reconcile_mirror(())["converged"] is True
    assert _names(host.held_dir / MIRROR) == host_names


def test_an_unreadable_host_mirror_cannot_top_up_an_existing_hold(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    tier, host, data = _occupied(tmp_path)
    directory = host.held_dir / MIRROR
    tier_names = _names(tier.paths.held / MOVER)
    host_names = _names(directory)
    real_scandir = os.scandir
    faults = []

    def unreadable(path):
        if not isinstance(path, int) and os.fspath(path) == str(directory):
            faults.append(str(path))
            raise PermissionError(errno.EACCES, "host mirror scan denied", str(path))
        return real_scandir(path)

    with monkeypatch.context() as patch:
        patch.setattr(os, "scandir", unreadable)
        verdict = tier.reconcile_mirror(())

    assert faults, "the real host mirror scan must reach the injected EACCES"
    assert _names(tier.paths.held / MOVER) == tier_names
    assert data.read_bytes() == b"still staged"
    assert _names(directory) == host_names
    assert verdict["converged"] is False
    assert verdict["events"] == []
    assert tier.reconcile_mirror(())["converged"] is True
    assert _names(directory) == host_names


def test_an_unresolved_host_population_is_not_reported_converged(tmp_path: Path):
    tier, host, _data = _occupied(tmp_path)
    assert tier._ledger.release(MOVER) == 2
    directory = host.held_dir / MIRROR
    host_names = _names(directory)
    retained = tmp_path / "retained-host-holder"
    directory.rename(retained)
    directory.symlink_to(directory.name, target_is_directory=True)
    try:
        with pytest.raises(OSError) as error:
            directory.stat()
        assert error.value.errno == errno.ELOOP
        verdict = tier.reconcile_mirror(())
    finally:
        directory.unlink()
        retained.rename(directory)

    assert _names(directory) == host_names
    assert verdict["converged"] is False
    assert verdict["events"] == []
    retry = tier.reconcile_mirror(())
    assert retry["converged"] is True
    assert retry["events"] == [
        {"reason": "orphan_ram_host_hold", "holder": MIRROR, "released_gib": 2}]
    assert _names(directory) == set()


def test_known_complete_observations_trim_and_release_orphans(tmp_path: Path):
    tier, host, data = _occupied(tmp_path)
    assert host.acquire(MIRROR, {"mem_gb": 1})
    orphan = storage_tiers.RAM_HOST_MEMORY_PREFIX + "b" * 64
    assert host.acquire(orphan, {"mem_gb": 2})

    verdict = tier.reconcile_mirror(())

    assert verdict["converged"] is True
    assert verdict["events"] == [
        {"reason": "ram-host-hold-trimmed", "holder": MIRROR, "released_gib": 1},
        {"reason": "orphan_ram_host_hold", "holder": orphan, "released_gib": 2}]
    assert host.holder_tokens(MIRROR) == {"mem_gb": 2}
    assert host.holder_tokens(orphan) == {}
    assert tier.holdings(MOVER) == {"ram_gib": 2}
    assert data.read_bytes() == b"still staged"
    assert tier.reconcile_mirror(()) == {"events": [], "converged": True}
