"""Deterministic admission checks with private ledgers and injected sensors."""
from pathlib import Path
import time

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


def bind(world, *members, closed=True, **coverage_changes):
    queue, _, root = world
    binding = ff.register(queue.root, root, members=members)["binding"]
    if not closed:
        return binding
    coverage = dict(version="test-coverage-v1", mapping_version="test-mapping-v1",
                    population_version="test-population-v1", bounds_version="test-bounds-v1",
                    configuration_version="test-config-v1", writer_fence="test-owner-protocol",
                    authority="private test fixture", evidence=["synthetic test evidence"],
                    scope_sha256=ff.coverage_identity(binding),
                    valid_from_unix=time.time() - 60, valid_until_unix=time.time() + 600,
                    bytes_per_token={ff.member_id(*m): ff.BYTE_KINDS[m[2]] for m in members},
                    additional_bytes=0)
    coverage.update(coverage_changes)
    return ff.register(queue.root, root, members=members, coverage=coverage)["binding"]


def test_local_gib_requires_a_physical_binding(world):
    queue, _, _ = world
    local = queue.tier("test-local")
    assert not local.take("unmapped", {"local_gib": 1})
    assert local.shortage["reason"] == "unbound_byte_ledger"


def test_members_alone_do_not_prove_a_closed_population(world):
    queue, _, _ = world
    bind(world, ("reservations", "test-host", "spool_gb"), closed=False)
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


def test_aggregate_outstanding_host_and_local_allowances_refuse_floor_breach(world):
    queue, _, _ = world
    binding = bind(world, ("reservations", "test-host", "spool_gb"),
                   ("tier-reservations", "test-local", "local_gib"))
    host, local = queue.ledger("test-host"), queue.tier("test-local")
    assert host.acquire("a" * 64, {"spool_gb": 4})
    assert not local.take("breach", {"local_gib": 2})
    assert local.shortage["reason"] == "below_floor"
    assert ff.refresh_binding(queue.root, binding)["charge_bytes"] == 4 * ff.GIB
    assert local.take("exact-boundary", {"local_gib": 1})
    assert not local.take("past-boundary", {"local_gib": 1})


@pytest.mark.parametrize("evidence", ["path-exists", "copy-done", "later-sample"])
def test_unproven_materialization_never_discounts_a_whole_reservation(world, evidence):
    queue, space, root = world
    binding = bind(world, ("reservations", "test-host", "spool_gb"),
                   ("tier-reservations", "test-stage", "stage_gib"))
    assert queue.ledger("test-host").acquire("a" * 64, {"spool_gb": 4})
    (root / "materialized-file").write_bytes(b"payload")
    space["free_bytes"] -= 7
    ff.refresh_binding(queue.root, binding)
    path = ff.fs_dir(queue.root, binding["key"]) / "sample.json"
    sample = ff._floor_read(path)
    sample.update(materialization_evidence=evidence, materialized_bytes=4 * ff.GIB)
    ff._floor_write(path, sample)
    assert ff.published_verdict(queue.root, binding)["charge_bytes"] == 4 * ff.GIB
    assert not queue.tier("test-stage").take("no-credit", {"stage_gib": 2})


def test_private_to_committed_and_transferred_custody_keeps_one_charge(world):
    queue, _, _ = world
    binding = bind(world, ("reservations", "test-host", "spool_gb"))
    host = queue.ledger("test-host")
    handle = host.begin_acquire("a" * 64, {"spool_gb": 4})
    assert handle is not None
    assert ff.refresh_binding(queue.root, binding)["charge_bytes"] == 4 * ff.GIB
    assert host.commit_acquire("a" * 64, handle) == 4
    assert host.transfer("a" * 64, "b" * 64) == 4
    assert host.holder_tokens("a" * 64) == {}
    assert host.holder_tokens("b" * 64) == {"spool_gb": 4}
    assert ff.refresh_binding(queue.root, binding)["charge_bytes"] == 4 * ff.GIB


@pytest.mark.parametrize("field,value", [
    ("census_bytes", 0.5), ("census_bytes", True), ("census_bytes", "0"),
    ("granted_at", 0.5), ("granted_at", True), ("granted_at", "0"),
])
def test_malformed_sample_counts_hold(world, field, value):
    queue, _, _ = world
    binding = bind(world, ("reservations", "test-host", "spool_gb"))
    path = ff.fs_dir(queue.root, binding["key"]) / "sample.json"
    sample = ff._floor_read(path)
    sample[field] = value
    ff._floor_write(path, sample)
    verdict = ff.published_verdict(queue.root, binding)
    assert not verdict["allowed"] and verdict["reason"] == "population_unknown"


def test_subordinate_dataset_constraint_keeps_the_whole_charge(world, monkeypatch):
    queue, space, _ = world
    sampled = dict(space, capacity_constraints=[dict(filesystem="test-dataset",
                   size_bytes=20 * ff.GIB, free_bytes=2 * ff.GIB)])
    monkeypatch.setattr(ff, "_sample", lambda found: sampled)
    binding = bind(world, ("reservations", "test-host", "spool_gb"))
    host = queue.ledger("test-host")
    assert host.acquire("a" * 64, {"spool_gb": 1})
    assert not host.acquire("b" * 64, {"spool_gb": 1})
    assert host.last_token_shortage["reason"] == "below_dataset_floor"


def test_nfs_mapping_is_bound_to_the_exact_mount_incarnation(world, monkeypatch):
    queue, _, root = world
    import os

    alias = dict(server="192.0.2.1", source="example:/test-export",
                 machine_id=ff._machine_id(), boot_id=ff._floor_boot_id(),
                 mount_namespace=os.stat("/proc/self/ns/mnt").st_ino,
                 device=12, mount_id=34)
    bind(world, ("reservations", "test-host", "spool_gb"), nfs_aliases=[alias])
    local_identity = ff.identify(root)
    remote = dict(local_identity, key=None, fstype="nfs4", server=alias["server"],
                  source=alias["source"], device=alias["device"], mount_id=alias["mount_id"])
    monkeypatch.setattr(ff, "identify", lambda path: remote)
    assert all(v["allowed"] for v in ff.check_paths(queue.root, [root]))
    remote["mount_id"] += 1
    assert all(not v["allowed"] for v in ff.check_paths(queue.root, [root]))


def test_native_mount_probe_uses_statx_and_never_fdinfo(tmp_path, monkeypatch):
    import os

    read = Path.read_text

    def safe_read(path, *args, **kwargs):
        assert "/fdinfo/" not in str(path)
        assert str(path) != "/proc/locks"
        return read(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", safe_read)
    descriptor = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        fstype, source, options = ff._mount_row(descriptor)
        assert fstype and source and options
    finally:
        os.close(descriptor)
