"""Deterministic admission checks with private ledgers and injected sensors."""
from pathlib import Path

import pytest

from fleet_world import FleetWorld
from prismabuild import filesystem_floor as ff


@pytest.fixture
def world(tmp_path, monkeypatch):
    space = dict(size_bytes=100 * ff.GIB, free_bytes=10 * ff.GIB,
                 size_inodes=1000, free_inodes=900, floor_inodes=50,
                 inode_refusal=None)

    def identify(path):
        return dict(path=str(Path(path)), key="uuid:private-test-filesystem",
                    fstype="ext4", source="/dev/test-only", device=1, **space)

    monkeypatch.setattr(ff, "identify", identify)
    monkeypatch.setattr(ff, "_sample", lambda found: dict(space))
    monkeypatch.setattr(ff, "_host_addresses", lambda: [])
    monkeypatch.setenv(ff.MODE_ENV, "enforce")
    queue = FleetWorld(tmp_path).queue
    queue.ledger("test-host").ensure_capacity({"spool_gb": 8})
    queue.tier("test-stage").mint({"stage_gib": 8})
    queue.tier("test-local").mint({"local_gib": 8})
    return queue, space, tmp_path


def bind(world, *members):
    queue, _, root = world
    return ff.register(queue.root, root, members=members)["binding"]


def test_local_gib_requires_a_physical_binding(world):
    queue, _, _ = world
    local = queue.tier("test-local")
    assert not local.take("unmapped", {"local_gib": 1})
    assert local.shortage["reason"] == "unbound_byte_ledger"


def test_members_alone_do_not_prove_a_closed_population(world):
    queue, _, _ = world
    bind(world, ("reservations", "test-host", "spool_gb"))
    host = queue.ledger("test-host")
    assert not host.acquire("a" * 64, {"spool_gb": 1})
    assert host.last_token_shortage["reason"] == "population_unknown"


def test_unregistered_local_destination_holds_instead_of_assuming_zero_charge(world):
    queue, _, root = world
    verdicts = ff.check_paths(queue.root, [root])
    assert verdicts and not any(v["allowed"] for v in verdicts)
    assert verdicts[0]["reason"] == "mapping_unknown"


def test_missing_allowance_population_cannot_reopen_floor_headroom(world):
    queue, _, _ = world
    binding = bind(world, ("reservations", "test-host", "spool_gb"),
                   ("tier-reservations", "test-stage", "stage_gib"))
    host = queue.ledger("test-host")
    assert host.acquire("a" * 64, {"spool_gb": 4})
    host.held_dir.rename(host.base / "inaccessible-held")
    ff.refresh_binding(queue.root, binding)
    stage = queue.tier("test-stage")
    assert not stage.take("would-breach", {"stage_gib": 2})


def test_failed_grant_persistence_cannot_admit_uncharged_tokens(world, monkeypatch):
    queue, _, _ = world
    bind(world, ("reservations", "test-host", "spool_gb"))
    write = ff._floor_write

    def fail_counter(path, value):
        if path.name == "granted.json":
            raise OSError("injected counter write failure")
        write(path, value)

    monkeypatch.setattr(ff, "_floor_write", fail_counter)
    host = queue.ledger("test-host")
    assert not host.acquire("a" * 64, {"spool_gb": 4})
    assert host.holder_tokens("a" * 64) == {}
