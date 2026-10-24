"""The used-filesystem floor (#1483): real identity, guards, locks and ledgers.

Admission scenarios control the external capacity sample for their temporary
filesystem, then spend its headroom through the real grant counter. Sensor
tests and the live ZFS case retain their own samples. Host disk occupancy
therefore cannot turn an admission scenario into an unrelated floor refusal.
"""
from __future__ import annotations
from fleet_world import FleetWorld

from submission_support import patch_store
import pbrun
from prismabuild import submission as submit_core

import functools
import json
import multiprocessing
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import threading
import time
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

from prismabuild import filesystem_floor as ff  # noqa: E402
from prismabuild import pool, tier_holdings  # noqa: E402

GIB = ff.GIB
HOST = socket.gethostname()


def _stable(path: Path) -> dict:
    found = ff.identify(path)
    if found["key"] is None or str(found["key"]).startswith("volatile:"):
        pytest.skip(f"{path} is on {found['fstype']}, which has no stable identity")
    return found


def _queue(tmp_path: Path, *, spool_gb: int = 4) -> pool.PoolQueue:
    queue = FleetWorld(tmp_path).queue
    queue.ledger(HOST).ensure_capacity({"spool_gb": spool_gb, "mem_gb": 2})
    return queue


def _register(queue, root: Path, *members, **kwargs) -> dict:
    root.mkdir(parents=True, exist_ok=True)
    return ff.register(queue.root, root, members=members, **kwargs)


def _headroom(queue, key: str) -> int:
    """Bytes the next grant may take: free - floor - charge."""

    v = ff.published_verdict(queue.root, ff._floor_read(ff.fs_dir(queue.root, key) / "binding.json"))
    assert "required_bytes" in v, v
    return v["free_bytes"] - v["floor_bytes"] - v["charge_bytes"]


def _leave(queue, key: str, gib: int) -> None:
    """Advance the grant counter so exactly ``gib`` GiB of headroom remain."""

    directory = ff.fs_dir(queue.root, key)
    room = _headroom(queue, key)
    assert room >= gib * GIB, room
    granted = ff._granted(directory) + room - gib * GIB
    ff._floor_write(directory / "granted.json", {"granted_bytes": granted})


@pytest.fixture
def ample_space(tmp_path, monkeypatch):
    """Control only the space sensor for this test's temporary filesystem."""

    read_room = ff.filesystem_capacity.local_disk_room
    sampled = os.statvfs_result((4096, 4096, 64 * GIB // 4096,
                                48 * GIB // 4096, 48 * GIB // 4096,
                                1_000_000, 900_000, 900_000, 0, 255))

    def room(path, floor_percent, *, statvfs=None):
        if Path(path).is_relative_to(tmp_path):
            return read_room(path, floor_percent, statvfs=lambda _: sampled)
        return read_room(path, floor_percent, statvfs=statvfs)

    monkeypatch.setattr(ff.filesystem_capacity, "local_disk_room", room)


@pytest.fixture
def enforce(monkeypatch):
    monkeypatch.setenv(ff.MODE_ENV, "enforce")


# -- the predicate ---------------------------------------------------------------

def test_floor_is_one_twentieth_rounded_up_and_the_boundary_passes():
    assert ff.floor_bytes(100) == 5
    assert ff.floor_bytes(101) == 6
    assert ff.floor_bytes(1) == 1
    assert ff.floor_verdict("f", size_bytes=100, free_bytes=8, charge_bytes=2,
                      demand_bytes=1)["allowed"] is True
    refused = ff.floor_verdict("f", size_bytes=100, free_bytes=7, charge_bytes=2, demand_bytes=1)
    assert refused["allowed"] is False and refused["required_bytes"] == 8
    with pytest.raises(ff.FloorError):
        ff.floor_verdict("f", size_bytes=100, free_bytes=-1)
    with pytest.raises(ff.FloorError):
        ff.floor_verdict("f", size_bytes=100, free_bytes=5, charge_bytes=1.5)


def test_identify_resolves_symlinks_and_absent_paths(tmp_path):
    target = tmp_path / "real"
    target.mkdir()
    (tmp_path / "link").symlink_to(target)
    assert ff.identify(tmp_path / "link")["path"] == str(target.resolve())
    # An interpreter is usually a symlink to a file (#1490 review, finding 8).
    assert ff.identify(sys.executable)["size_bytes"] > 0
    absent = ff.identify(tmp_path / "not" / "yet")
    assert absent["path"] == str(tmp_path.resolve())


@pytest.mark.parametrize("free_inodes", [0, 49, 50])
def test_used_path_checks_inode_floor_with_free_bytes(
        tmp_path, monkeypatch, free_inodes, floor_2046_world):
    sampled = os.statvfs_result((4096, 4096, 1000, 900, 900,
                                1000, free_inodes, free_inodes, 0, 255))
    monkeypatch.setattr(ff.os, "fstatvfs", lambda fd: sampled)
    queue, _binding, _ledger = floor_2046_world
    verdicts = ff.check_paths(queue.root, [tmp_path])
    local, = [v for v in verdicts if v.get("sample") == "fresh-local"]
    assert local["allowed"] is (free_inodes >= 50), verdicts
    assert local["free_inodes"] == free_inodes
    assert local["floor_inodes"] == 50
    if free_inodes < 50:
        assert local["reason"] == "below_inode_floor"
        assert "inodes" in ff.describe_verdict(local)


def test_zfs_sample_keeps_pool_capacity_when_dataset_device_changes(monkeypatch):
    found = {"path": "/mnt/stage-work", "fstype": "zfs",
             "pool": "prismabuild-stage", "device": 10}
    sampled = os.statvfs_result((4096, 4096, 1000 * GIB // 4096,
                                900 * GIB // 4096, 900 * GIB // 4096,
                                1000, 900, 900, 0, 255))
    original_stat = os.stat

    def changed_device(path, *args, **kwargs):
        if path == found["path"]:
            return SimpleNamespace(st_dev=11)
        return original_stat(path, *args, **kwargs)

    monkeypatch.setattr(os, "stat", changed_device)
    monkeypatch.setattr(os, "statvfs", lambda path: sampled)
    pools_read = []

    def pool_space(name):
        pools_read.append(name)
        return 1000 * GIB, 900 * GIB

    monkeypatch.setattr(ff, "_zfs_space", pool_space)
    space = ff._sample(found)
    assert pools_read == [found["pool"]]
    assert space["size_bytes"] == 1000 * GIB
    assert space["free_bytes"] == 900 * GIB
    assert space["free_inodes"] == 900
    assert space["floor_inodes"] == 50
    assert space["inode_refusal"] is None


def test_btrfs_zero_inode_totals_do_not_add_a_capacity_refusal(
        tmp_path, monkeypatch, floor_2046_world):
    # btrfs reports no fixed inode limit; the fixture filesystem may differ.
    sampled = os.statvfs_result((4096, 4096, 1000, 900, 900, 0, 0, 0, 0, 255))
    monkeypatch.setattr(os, "fstatvfs", lambda fd: sampled)
    queue, _binding, _ledger = floor_2046_world
    verdict, = [v for v in ff.check_paths(queue.root, [tmp_path])
                if v.get("sample") == "fresh-local"]
    assert verdict["allowed"] is True
    assert verdict["size_inodes"] == 0
    assert verdict["floor_inodes"] == 0
    assert verdict["inode_refusal"] is None


def test_available_inode_floor_does_not_spend_privileged_free_inodes(
        tmp_path, monkeypatch, floor_2046_world):
    sampled = os.statvfs_result((4096, 4096, 1000, 900, 900, 1000, 900, 49, 0, 255))
    monkeypatch.setattr(os, "fstatvfs", lambda fd: sampled)
    queue, _binding, _ledger = floor_2046_world
    verdict, = [v for v in ff.check_paths(queue.root, [tmp_path])
                if v.get("sample") == "fresh-local"]
    assert verdict["allowed"] is False
    assert verdict["reason"] == "below_inode_floor"
    assert verdict["free_inodes"] == 49
    assert verdict["floor_inodes"] == 50


# -- off is off ---------------------------------------------------------------------

def test_off_reads_nothing_and_changes_nothing(tmp_path, monkeypatch):
    monkeypatch.delenv(ff.MODE_ENV, raising=False)
    queue = _queue(tmp_path)
    assert ff.mode(queue.root) == "off"
    ledger = queue.ledger(HOST)
    assert ledger.acquire("a" * 64, {"spool_gb": 4})
    assert not (queue.root / ff.FLOOR_DIR).exists()
    with ff.operation(queue.root, used_paths=[tmp_path], growth_gib={tmp_path: 9}) as v:
        assert v == []
    assert ff.enforce_paths(queue.root, ["/nonexistent-root-xyz"], label="t") == []


def test_mode_file_sets_the_fleet_mode_and_env_overrides_it(tmp_path, monkeypatch):
    monkeypatch.delenv(ff.MODE_ENV, raising=False)
    queue = _queue(tmp_path)
    assert ff.main(["--queue", str(queue.root), "mode", "observe"]) == 0
    assert ff.mode(queue.root) == "observe"
    monkeypatch.setenv(ff.MODE_ENV, "off")
    assert ff.mode(queue.root) == "off"
    monkeypatch.setenv(ff.MODE_ENV, "enforec")
    # A typo must not downgrade an enforcement request to allowing observe.
    try:
        with pytest.raises((ff.FloorError, ff.FloorRefused)):
            ff.enforce_paths(queue.root, [tmp_path], label="invalid-mode")
    finally:
        monkeypatch.delenv(ff.MODE_ENV, raising=False)


# -- register, refresh, admit ---------------------------------------------------------

def test_register_refresh_admit_and_refuse(tmp_path, enforce, ample_space):
    found = _stable(tmp_path)
    queue = _queue(tmp_path)
    member = ("reservations", HOST, "spool_gb")
    result = _register(queue, tmp_path / "spool", member)
    key = result["binding"]["key"]
    assert key == found["key"] and result["refresh"]["allowed"] is True
    sample = ff._floor_read(ff.fs_dir(queue.root, key) / "sample.json")
    assert sample["census_bytes"] == 0 and sample["granted_at"] == 0

    ledger = queue.ledger(HOST)
    assert ledger.acquire("a" * 64, {"spool_gb": 1})
    assert ff._granted(ff.fs_dir(queue.root, key)) == GIB
    # The grant is charged before any refresh, and after one it is census.
    ff.refresh_binding(queue.root, result["binding"])
    sample = ff._floor_read(ff.fs_dir(queue.root, key) / "sample.json")
    assert sample["census_bytes"] == GIB and sample["granted_at"] == GIB

    _leave(queue, key, 1)
    assert not ledger.acquire("b" * 64, {"spool_gb": 2})
    shortage = ledger.last_token_shortage
    assert shortage["resource"] == "filesystem_floor"
    assert shortage["reason"] == "below_floor" and shortage["filesystem"] == key
    assert ledger.holder_tokens("b" * 64) == {}
    # Exactly the remaining headroom is admitted: the boundary passes.
    assert ledger.acquire("b" * 64, {"spool_gb": 1})
    # Kinds that are not bytes are never gated.
    assert ledger.acquire("c" * 64, {"mem_gb": 1})


def test_release_needs_no_floor_and_refresh_returns_the_headroom(tmp_path, enforce, ample_space):
    _stable(tmp_path)
    queue = _queue(tmp_path)
    binding = _register(queue, tmp_path / "spool", ("reservations", HOST, "spool_gb"))["binding"]
    ledger = queue.ledger(HOST)
    assert ledger.acquire("a" * 64, {"spool_gb": 2})
    _leave(queue, binding["key"], 0)
    assert not ledger.acquire("b" * 64, {"spool_gb": 1})
    assert ledger.release("a" * 64) == 2
    ff.refresh_binding(queue.root, binding)
    assert ledger.acquire("b" * 64, {"spool_gb": 1})


def test_unbound_byte_ledger_refuses_under_enforce_and_passes_under_observe(
        tmp_path, monkeypatch, capsys):
    queue = _queue(tmp_path)
    ledger = queue.ledger(HOST)
    monkeypatch.setenv(ff.MODE_ENV, "enforce")
    assert not ledger.acquire("a" * 64, {"spool_gb": 1})
    assert ledger.last_token_shortage["reason"] == "unbound_byte_ledger"
    monkeypatch.setenv(ff.MODE_ENV, "observe")
    assert ledger.acquire("a" * 64, {"spool_gb": 1})
    assert "would refuse" in capsys.readouterr().err


def test_a_stale_sample_refuses_and_a_skewed_clock_is_tolerated(tmp_path, enforce, ample_space):
    _stable(tmp_path)
    queue = _queue(tmp_path)
    binding = _register(queue, tmp_path / "spool", ("reservations", HOST, "spool_gb"))["binding"]
    now = time.time()
    assert ff.published_verdict(queue.root, binding, now=now + ff.SAMPLE_MAX_AGE_S + 5)[
        "reason"] == "sample_stale"
    assert ff.published_verdict(queue.root, binding, now=now - 10)["allowed"] is True
    ff.refresh_binding(queue.root, binding, now=now - ff.SAMPLE_MAX_AGE_S - 5)
    ledger = queue.ledger(HOST)
    assert not ledger.acquire("a" * 64, {"spool_gb": 1})
    assert ledger.last_token_shortage["reason"] == "sample_stale"
    ff.refresh_binding(queue.root, binding)
    assert ledger.acquire("a" * 64, {"spool_gb": 1})


def test_a_root_on_another_filesystem_marks_the_binding_moved(tmp_path, enforce, ample_space):
    _stable(tmp_path)
    queue = _queue(tmp_path)
    binding = _register(queue, tmp_path / "spool", ("reservations", HOST, "spool_gb"))["binding"]
    elsewhere = next((p for p in ("/dev/shm", "/run", "/proc") if os.path.isdir(p)
                      and ff.identify(p)["key"] != binding["key"]), None)
    if elsewhere is None:
        pytest.skip("no second filesystem to move the root onto")
    moved = ff.refresh_binding(queue.root, {**binding, "root": elsewhere})
    assert moved["reason"] == "binding_moved"
    ledger = queue.ledger(HOST)
    assert not ledger.acquire("a" * 64, {"spool_gb": 1})
    assert ledger.last_token_shortage["reason"] == "binding_moved"
    # Registering again at the real root reactivates the same stable key.
    _register(queue, tmp_path / "spool", ("reservations", HOST, "spool_gb"))
    assert ledger.acquire("a" * 64, {"spool_gb": 1})


def test_a_member_cannot_be_bound_to_two_filesystems(tmp_path):
    _stable(tmp_path)
    queue = _queue(tmp_path)
    binding = _register(queue, tmp_path / "spool", ("reservations", HOST, "spool_gb"))["binding"]
    ff._floor_write(ff.member_path(queue.root, "reservations", HOST, "spool_gb"),
              {"key": "uuid:elsewhere", "ledger_root": "reservations",
               "ledger": HOST, "kind": "spool_gb"})
    with pytest.raises(ff.FloorError, match="unbind it first"):
        _register(queue, tmp_path / "spool", ("reservations", HOST, "spool_gb"))
    ff._floor_write(ff.member_path(queue.root, "reservations", HOST, "spool_gb"),
              {"key": binding["key"], "ledger_root": "reservations",
               "ledger": HOST, "kind": "spool_gb"})
    assert ff.unbind(queue.root, "reservations", HOST, "spool_gb")["key"] == binding["key"]
    assert ff._floor_read(ff.fs_dir(queue.root, binding["key"]) / "binding.json")["members"] == []


def test_aggregate_charge_spans_every_ledger_on_the_filesystem(tmp_path, enforce, ample_space):
    """Two ledgers, one disk: one ledger's holdings reduce the other's room."""

    _stable(tmp_path)
    queue = _queue(tmp_path)
    tier = "prismabuild-stage:" + HOST
    queue.tier(tier).mint({'stage_gib': 4})
    binding = _register(queue, tmp_path / "spool", ("reservations", HOST, "spool_gb"),
                        ("tier-reservations", tier, "stage_gib"))["binding"]
    host, stage = (queue.ledger(HOST), queue.tier(tier))
    assert host.acquire("a" * 64, {"spool_gb": 3})
    counted = ff.refresh_binding(queue.root, binding)
    assert counted["charge_bytes"] == 3 * GIB
    _leave(queue, binding["key"], 1)
    assert not stage.take('grant-x', {'stage_gib': 2})
    assert stage.shortage['resource'] == 'filesystem_floor'
    assert stage.take('grant-x', {'stage_gib': 1})
    assert ff.floor_status(queue.root)["unbound_byte_ledgers"] == []


def test_status_names_unbound_byte_ledgers(tmp_path):
    queue = _queue(tmp_path)
    report = ff.floor_status(queue.root)
    assert report["unbound_byte_ledgers"] == [f"reservations/{HOST}/spool_gb"]


def _zfs_scratch() -> Path | None:
    """A writable directory on a local ZFS pool (the file server's shared store)."""

    base = Path("/mnt/shared/prismabuild-fleet/validation")
    try:
        found = ff.identify(base)
    except ff.FloorError:
        return None
    return base if found["fstype"] == "zfs" and os.access(base, os.W_OK) else None


@pytest.mark.live_store(reason="the fleet's only local ZFS pool is the shared store; "
                         "a private temporary directory, removed afterwards")
def test_zfs_pool_is_keyed_by_guid_sampled_usable_and_admits(enforce):
    """On the pool itself: usable space, never raw pool free (review finding 9)."""

    import shutil
    import tempfile

    base = _zfs_scratch()
    if base is None:
        pytest.skip("no local ZFS shared store on this worker")
    root = Path(tempfile.mkdtemp(prefix="pb1483-floor-", dir=base))
    try:
        found = ff.identify(root)
        assert str(found["key"]).startswith("zfs:") and found["pool"]
        raw = ff._command(["zpool", "list", "-Hp", "-o", "size,free", found["pool"]]).split()
        # zpool free counts parity and slop; the usable figure is below it.
        assert found["free_bytes"] <= int(raw[1])
        queue = _queue(root)
        binding = _register(queue, root / "spool", ("reservations", HOST, "spool_gb"))["binding"]
        assert binding["key"] == found["key"] and binding["fstype"] == "zfs"
        ledger = queue.ledger(HOST)
        assert ledger.acquire("a" * 64, {"spool_gb": 1})
        _leave(queue, binding["key"], 0)
        assert not ledger.acquire("b" * 64, {"spool_gb": 1})
        assert ledger.last_token_shortage["reason"] == "below_floor"
        assert ledger.release("a" * 64) == 1
        ff.refresh_binding(queue.root, binding)
        assert ledger.acquire("b" * 64, {"spool_gb": 1})
    finally:
        shutil.rmtree(root, ignore_errors=True)


# -- the four tier/produced-output acquisition sites ------------------------------------

def test_tier_advance_reports_the_floor_as_tier_short(tmp_path, enforce, ample_space):
    """take_tier_advance goes through begin_acquire; the floor is a shortage."""

    _stable(tmp_path)
    queue = _queue(tmp_path)
    tier = "prismabuild-stage:" + HOST
    queue.tier(tier).mint({'stage_gib': 4})
    binding = _register(queue, tmp_path / "stage", ("tier-reservations", tier, "stage_gib"))[
        "binding"]
    _leave(queue, binding["key"], 1)
    assert queue.tier(tier).take_advance('grant-advance', 2, 'stage_gib')[0] == 'tier-short'
    ledger = queue.tier(tier)
    assert not ledger.take('grant-probe', {'stage_gib': 2})
    assert ledger.shortage['resource'] == 'filesystem_floor'
    assert queue.tier(tier).take_advance('grant-advance', 1, 'stage_gib')[0] == 'taken'
    # The tokens were there all along: a refresh, not more capacity, admits 2.
    ff.refresh_binding(queue.root, binding)
    assert queue.tier(tier).take_advance('grant-advance-2', 2, 'stage_gib')[0] == 'taken'


def test_fence_top_up_is_refused_by_the_floor_and_admitted_after_refresh(
        tmp_path, monkeypatch, ample_space):
    """_reserve_fence_locked's top-up acquire goes through the same gate."""

    import test_tier_funding as tf
    from prismabuild import window_credit

    _stable(tmp_path)
    queue = tf._queue(tmp_path, stage_gib=6)
    mover, consumer = tf._hexkey("floor-mover"), tf._hexkey("floor-consumer")
    plan = tf._plan(queue, consumer, mover, tag="floor")
    row = tf._publish_mover(queue, plan, mover)
    grant = window_credit.grant_key(consumer, tf.TIER, "mover_row", "phase-floor")
    fields = tf._fields(queue, plan, mover, row, kind="stage_gib")
    binding = _register(queue, tmp_path / "stage", ("tier-reservations", tf.TIER, "stage_gib"))[
        "binding"]
    monkeypatch.setenv(ff.MODE_ENV, "enforce")
    _leave(queue, binding["key"], 1)
    assert queue.funding.reserve_window(tf.TIER, grant, fields, 2).bound is False
    assert queue.tier(tf.TIER).holdings(grant) == {}
    ff.refresh_binding(queue.root, binding)
    assert queue.funding.reserve_window(tf.TIER, grant, fields, 2).bound is True


def test_produced_output_refill_is_refused_by_the_floor_not_raised(tmp_path, monkeypatch, ample_space):
    """refill_window's acquire returns a typed result under a floor refusal."""

    import test_produced_output_restage as restage

    _stable(tmp_path)
    world = restage._World(tmp_path)
    monkeypatch.setenv(ff.MODE_ENV, "enforce")
    world.stage_root.mkdir(parents=True, exist_ok=True)
    binding = ff.register(world.q.root, world.stage_root, members=[
        ("tier-reservations", restage.TIER, "stage_gib")])["binding"]
    _leave(world.q, binding["key"], 0)
    from prismabuild import produced_output as po
    owner = str(world.inst["owner_action_key"])
    # Empty the owner's window, as a retire returning its tokens would, so
    # the refill has room to take.
    world.ledger.release_unspent(owner, authority=tier_holdings.Unspent.NEVER_STARTED)
    refill = po.refill_window(world.q, world.inst, world.template, tier=restage.TIER)
    assert refill.get("acquired", 0) == 0, refill
    assert world.ledger.holdings(owner) == {}
    ff.refresh_binding(world.q.root, binding)
    refill = po.refill_window(world.q, world.inst, world.template, tier=restage.TIER)
    assert refill.get("ok") is True and refill.get("acquired", 0) >= 1, refill


# -- used paths and NFS ------------------------------------------------------------------

def test_used_paths_unbound_local_is_sampled_fresh_and_bound_is_published(tmp_path, ample_space):
    found = _stable(tmp_path)
    queue = _queue(tmp_path)
    fresh = ff.check_paths(queue.root, [tmp_path, tmp_path / "x"])
    assert fresh and all(not v["allowed"] for v in fresh), fresh
    # Zero declared demand is not proof of a closed allowance population.
    _register(queue, tmp_path / "spool", ("reservations", HOST, "spool_gb"))
    bound = ff.check_paths(queue.root, [tmp_path])
    assert [v["filesystem"] for v in bound] == [found["key"]] * 2
    assert {v.get("sample") for v in bound} == {None, "fresh-local"}


def _nfs_path() -> str | None:
    for line in Path("/proc/self/mountinfo").read_text().splitlines():
        left, _, right = line.partition(" - ")
        if right.split()[0] in ff.NFS_TYPES and os.path.isdir(left.split()[4]):
            try:
                os.listdir(left.split()[4])
            except OSError:
                continue
            return left.split()[4]
    return None


def test_server_addresses_leave_out_loopback_link_local_and_bridges():
    import ipaddress

    addresses = [ipaddress.ip_address(a) for a in ff._host_addresses()]
    assert addresses, "this host names no address an NFS client could use"
    assert not any(a.is_loopback or a.is_link_local for a in addresses)
    assert "172.17.0.1" not in ff._host_addresses()


def test_nfs_server_address_alone_cannot_authenticate_capacity(tmp_path, enforce, ample_space):
    nfs = _nfs_path()
    if nfs is None:
        pytest.skip("no readable NFS mount on this worker")
    _stable(tmp_path)
    queue = _queue(tmp_path)
    unattributed = ff.check_paths(queue.root, [nfs])
    assert unattributed[0]["reason"] == "unattributed_nfs"
    with pytest.raises(ff.FloorRefused):
        ff.enforce_paths(queue.root, [nfs], label="t")
    # An address match is not authenticated export-to-capacity mapping:
    # an NFS client FSID must not be equated to a local pool by address.
    server = ff.identify(nfs)["server"]
    binding = _register(queue, tmp_path / "spool", ("reservations", HOST, "spool_gb"))["binding"]
    path = ff.fs_dir(queue.root, binding["key"]) / "binding.json"
    ff._floor_write(path, {**ff._floor_read(path), "server_addresses": [server]})
    assert queue.ledger(HOST).acquire("a" * 64, {"spool_gb": 2})
    verdicts = ff.check_paths(queue.root, [nfs])
    assert verdicts and any(not v["allowed"] for v in verdicts), verdicts
    with pytest.raises(ff.FloorRefused):
        ff.enforce_paths(queue.root, [nfs], label="address-is-not-mapping")


# -- coordinator growth, release on exit, the reaper -------------------------------------

def _growth_queue(tmp_path):
    _stable(tmp_path)
    queue = _queue(tmp_path)
    result = _register(queue, tmp_path / "shared", filesystem_gib=4,
                       filesystem_ledger="fs-test")
    ledger = pool.ResourceLedger(queue.root / ff.FILESYSTEM_RESERVATIONS, host="fs-test")
    return queue, result["binding"], ledger


def test_operation_holds_growth_for_the_body_and_releases_on_any_exit(tmp_path, enforce, ample_space):
    queue, binding, ledger = _growth_queue(tmp_path)
    assert {"ledger_root": ff.FILESYSTEM_RESERVATIONS, "ledger": "fs-test",
            "kind": "filesystem_gib"} in binding["members"]
    with ff.operation(queue.root, used_paths=[tmp_path], growth_gib={tmp_path / "x": 3}):
        assert ledger.held().get("filesystem_gib") == 3
        assert len(list((ledger.base / "operations").glob("*.json"))) == 1
    assert ledger.held().get("filesystem_gib", 0) == 0
    # A released grant stays charged until the owner's next census.
    ff.refresh_binding(queue.root, binding)
    with pytest.raises(SystemExit):
        with ff.operation(queue.root, growth_gib={tmp_path: 3}):
            raise SystemExit(2)
    assert ledger.held().get("filesystem_gib", 0) == 0
    assert list((ledger.base / "operations").glob("*.json")) == []
    ff.refresh_binding(queue.root, binding)
    _leave(queue, binding["key"], 1)
    with pytest.raises(ff.FloorRefused, match="growth_refused"):
        with ff.operation(queue.root, growth_gib={tmp_path: 2}):
            pytest.fail("the body must not run")
    assert ledger.held().get("filesystem_gib", 0) == 0


def _killed_owner(queue_root: str, path: str, ready) -> None:
    os.environ[ff.MODE_ENV] = "enforce"
    with ff.operation(queue_root, growth_gib={path: 2}):
        ready.set()
        time.sleep(600)


def test_a_killed_owner_is_reaped_and_a_lease_expiry_is_too(tmp_path, enforce, ample_space):
    queue, _binding, ledger = _growth_queue(tmp_path)
    ready = multiprocessing.get_context("fork").Event()
    child = multiprocessing.get_context("fork").Process(
        target=_killed_owner, args=(str(queue.root), str(tmp_path), ready))
    child.start()
    assert ready.wait(60)
    assert ledger.held().get("filesystem_gib") == 2
    assert ff.reap_operations(queue.root) == []          # alive: kept
    os.kill(child.pid, signal.SIGKILL)
    child.join(30)
    reaped = ff.reap_operations(queue.root)
    assert len(reaped) == 1 and reaped[0].startswith(ff.OPERATION_PREFIX)
    assert ledger.held().get("filesystem_gib", 0) == 0
    # A holder from another machine is reaped only by its lease.
    with ff.operation(queue.root, growth_gib={tmp_path: 1}, lease_s=60):
        record = next((ledger.base / "operations").glob("*.json"))
        owner = json.loads(record.read_text())
        ff._floor_write(record, {**owner, "machine_id": "0" * 32})
        assert ff.reap_operations(queue.root) == []
        assert ff.reap_operations(queue.root, now=time.time() + 120) == [owner["holder"]]
        assert ledger.held().get("filesystem_gib", 0) == 0


# -- locks: wait, bounded, innermost, no deadlock ------------------------------------------

def test_a_busy_floor_lock_is_waited_for_then_refused_as_a_shortage(
        tmp_path, enforce, monkeypatch, ample_space):
    _stable(tmp_path)
    queue = _queue(tmp_path)
    binding = _register(queue, tmp_path / "spool", ("reservations", HOST, "spool_gb"))["binding"]
    directory = ff.fs_dir(queue.root, binding["key"])
    script = (f"import sys,time; sys.path.insert(0,{str(Path(ff.__file__).parents[1])!r});"
              "from prismabuild import filesystem_floor as ff;"
              "from pathlib import Path;"
              f"cm=ff.floor_locked(Path({str(directory)!r}));"
              "assert cm.__enter__(); print('held', flush=True); time.sleep(float(sys.argv[1]))")
    monkeypatch.setattr(ff, "LOCK_WAIT_S", 0.5)
    holder = subprocess.Popen([sys.executable, "-c", script, "30"], stdout=subprocess.PIPE,
                              text=True)
    try:
        assert holder.stdout.readline().strip() == "held"
        ledger = queue.ledger(HOST)
        started = time.monotonic()
        assert not ledger.acquire("a" * 64, {"spool_gb": 1})
        assert time.monotonic() - started >= 0.5
        assert ledger.last_token_shortage["reason"] == "floor_lock_busy"
    finally:
        holder.kill()
        holder.wait()
    # A holder that lets go within the wait is waited for, not refused.
    monkeypatch.setattr(ff, "LOCK_WAIT_S", 30.0)
    holder = subprocess.Popen([sys.executable, "-c", script, "1"], stdout=subprocess.PIPE,
                              text=True)
    try:
        assert holder.stdout.readline().strip() == "held"
        assert queue.ledger(HOST).acquire("a" * 64, {"spool_gb": 1})
    finally:
        holder.wait()


def _contend(queue_root: str, tier: str, host: str, rounds: int, errors, wins) -> None:
    os.environ[ff.MODE_ENV] = "enforce"
    queue = pool.PoolQueue(queue_root)
    host_ledger, tier_owner = queue.ledger(host), queue.tier(tier)
    takes = [host_ledger.acquire, tier_owner.take]
    returns = [host_ledger.release, functools.partial(
        tier_owner.release_unspent, authority=tier_holdings.Unspent.NEVER_STARTED)]
    kinds = ["spool_gb", "stage_gib"]
    try:
        for i in range(rounds):
            which = (os.getpid() + i) % 2
            key = f"k{os.getpid()}-{i}"
            if takes[which](key, {kinds[which]: 1}):
                with wins.get_lock():
                    wins.value += 1
                returns[which](key)
            ff.refresh_local(queue_root, force=True)
    except Exception as exc:                                    # noqa: BLE001
        errors.put(repr(exc))


def test_concurrent_acquire_release_refresh_across_two_ledgers_never_deadlocks(
        tmp_path, enforce, ample_space):
    """Ledger lock then floor lock everywhere; refresh holds only the floor."""

    _stable(tmp_path)
    queue = _queue(tmp_path, spool_gb=8)
    tier = "prismabuild-stage:" + HOST
    queue.tier(tier).mint({'stage_gib': 8})
    _register(queue, tmp_path / "spool", ("reservations", HOST, "spool_gb"),
              ("tier-reservations", tier, "stage_gib"))
    context = multiprocessing.get_context("fork")
    errors = context.Queue()
    wins = context.Value("i", 0)
    workers = [context.Process(target=_contend,
                               args=(str(queue.root), tier, HOST, 25, errors, wins))
               for _ in range(4)]
    for w in workers:
        w.start()
    for w in workers:
        w.join(120)
    assert all(w.exitcode == 0 for w in workers), [w.exitcode for w in workers]
    assert errors.empty(), errors.get()
    # Contention may refuse some takes (a busy lock is a shortage), but the
    # floor never wedges: most of the hundred succeed.
    assert wins.value >= 50, wins.value
    assert queue.ledger(HOST).held().get("spool_gb", 0) == 0
    assert queue.tier(tier).holdings().get('stage_gib', 0) == 0


def test_floor_section_runs_inside_a_held_ledger_lock_without_deadlock(tmp_path, enforce, ample_space):
    """A caller holding the ledger lock nests begin_acquire (main's contract)."""

    _stable(tmp_path)
    queue = _queue(tmp_path)
    _register(queue, tmp_path / "spool", ("reservations", HOST, "spool_gb"))
    ledger = queue.ledger(HOST)
    done = []

    def run():
        with ledger._mutation_locked():
            done.append(ledger.acquire("a" * 64, {"spool_gb": 1}))

    thread = threading.Thread(target=run)
    thread.start()
    thread.join(30)
    assert done == [True]


# -- loop hooks ---------------------------------------------------------------------------

def test_loop_tick_refreshes_owned_bindings_and_never_raises(tmp_path, monkeypatch, capsys):
    monkeypatch.delenv(ff.MODE_ENV, raising=False)
    _stable(tmp_path)
    queue = _queue(tmp_path)
    binding = _register(queue, tmp_path / "spool", ("reservations", HOST, "spool_gb"))["binding"]
    before = ff._floor_read(ff.fs_dir(queue.root, binding["key"]) / "sample.json")["sampled_unix"]
    ff._LAST_REFRESH.clear()
    time.sleep(0.01)
    ff.loop_tick(queue.root, label="test")
    after = ff._floor_read(ff.fs_dir(queue.root, binding["key"]) / "sample.json")["sampled_unix"]
    assert after > before
    # A queue root the tick cannot list is logged, never raised.
    not_a_queue = tmp_path / "file"
    not_a_queue.write_text("x")
    ff._LAST_REFRESH.clear()
    capsys.readouterr()
    ff.loop_tick(not_a_queue, label="test")
    assert "tick skipped" in capsys.readouterr().err


def test_host_admission_skips_the_poll_only_under_enforce(tmp_path, monkeypatch):
    nfs = _nfs_path()
    if nfs is None:
        pytest.skip("no readable NFS mount on this worker")
    queue = _queue(tmp_path)
    monkeypatch.setenv(ff.MODE_ENV, "observe")
    assert ff.host_admission(queue.root, [nfs], label="t") is True
    monkeypatch.setenv(ff.MODE_ENV, "enforce")
    assert ff.host_admission(queue.root, [nfs], label="t") is False
    monkeypatch.setenv(ff.MODE_ENV, "off")
    assert ff.host_admission(queue.root, [nfs], label="t") is True


def test_cli_register_status_check_refresh(tmp_path, capsys, monkeypatch, ample_space):
    monkeypatch.delenv(ff.MODE_ENV, raising=False)
    _stable(tmp_path)
    queue = _queue(tmp_path)
    (tmp_path / "spool").mkdir()
    assert ff.main(["--queue", str(queue.root), "register", str(tmp_path / "spool"),
                    "--member", f"reservations/{HOST}:spool_gb"]) == 0
    registered = json.loads(capsys.readouterr().out)
    assert registered["refresh"]["allowed"] is True
    assert ff.main(["--queue", str(queue.root), "status"]) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["mode"] == "off" and report["unbound_byte_ledgers"] == []
    assert report["bindings"][0]["verdict"]["allowed"] is True
    assert ff.main(["--queue", str(queue.root), "refresh"]) == 0
    assert json.loads(capsys.readouterr().out)[0]["allowed"] is True
    assert ff.main(["--queue", str(queue.root), "check", str(tmp_path)]) == 0
    assert json.loads(capsys.readouterr().out)[0]["allowed"] is True


# -- pbrun: the coordinator's submission ----------------------------------------------------

def _pbrun_world(tmp_path, monkeypatch):
    import pbrun
    from prismabuild import submission
    from submission_support import patch_store

    _stable(tmp_path)
    fleet = tmp_path / "fleet"
    queue = pool.PoolQueue(fleet / "pb-queue")
    queue.records.ensure_layout()
    (fleet / "cas").mkdir(parents=True)
    _register(queue, fleet / "cas", filesystem_gib=4, filesystem_ledger="fs-test")
    patch_store(monkeypatch, client=pbrun, root=fleet)
    monkeypatch.setattr(sys, "argv", ["pbrun.py", "--cwd", str(tmp_path),
                                      "--filesystem-growth-gib", "2", "--", "true"])
    ledger = pool.ResourceLedger(queue.root / ff.FILESYSTEM_RESERVATIONS, host="fs-test")
    return pbrun, queue, ledger


def test_pbrun_holds_growth_while_publishing_and_releases_before_waiting(
        tmp_path, monkeypatch, enforce, ample_space):
    pbrun, _queue_, ledger = _pbrun_world(tmp_path, monkeypatch)
    seen = {}

    def publish(args, *, report, **_kw):
        # The real submission body is replaced only here, to observe the
        # allowance while it would run; the guard and ledger are real.
        seen["held"] = ledger.held().get("filesystem_gib", 0)
        return lambda: report.finish(submit_core.action_client.ClientCommand(
            seen.setdefault("waited_with", ledger.held().get("filesystem_gib", 0))))

    monkeypatch.setattr(submit_core, '_prepare_and_publish', publish)
    pbrun.main()
    assert seen == {"held": 2, "waited_with": 0}


def test_pbrun_below_the_floor_submits_nothing_and_says_why(
        tmp_path, monkeypatch, enforce, ample_space):
    pbrun, queue, ledger = _pbrun_world(tmp_path, monkeypatch)
    key = ff.bindings(queue.root)[0]["key"]
    _leave(queue, key, 1)
    monkeypatch.setattr(submit_core, '_prepare_and_publish',
                        lambda *a, **k: pytest.fail("nothing may be submitted"))
    with pytest.raises(SystemExit, match="below its floor; nothing submitted"):
        pbrun.main()
    assert ledger.held().get("filesystem_gib", 0) == 0


def test_off_with_bindings_present_takes_no_lock_and_counts_nothing(tmp_path, monkeypatch):
    _stable(tmp_path)
    queue = _queue(tmp_path)
    binding = _register(queue, tmp_path / "spool", ("reservations", HOST, "spool_gb"))["binding"]
    monkeypatch.setenv(ff.MODE_ENV, "off")
    directory = ff.fs_dir(queue.root, binding["key"])
    (directory / ".floor.lock").unlink(missing_ok=True)
    (directory / ".floor.lock").mkdir()          # any lock attempt would fail
    assert queue.ledger(HOST).acquire("a" * 64, {"spool_gb": 2})
    assert ff._granted(directory) == 0


def test_a_lock_that_cannot_be_opened_is_a_verdict_not_an_exception(
        tmp_path, monkeypatch, capsys):
    _stable(tmp_path)
    queue = _queue(tmp_path)
    binding = _register(queue, tmp_path / "spool", ("reservations", HOST, "spool_gb"))["binding"]
    directory = ff.fs_dir(queue.root, binding["key"])
    (directory / ".floor.lock").unlink(missing_ok=True)
    (directory / ".floor.lock").mkdir()          # e.g. created by another uid
    ledger = queue.ledger(HOST)
    monkeypatch.setenv(ff.MODE_ENV, "enforce")
    assert not ledger.acquire("a" * 64, {"spool_gb": 1})
    assert ledger.last_token_shortage["reason"] == "floor_lock_error"
    monkeypatch.setenv(ff.MODE_ENV, "observe")
    assert ledger.acquire("a" * 64, {"spool_gb": 1})
    assert "would refuse" in capsys.readouterr().err
    # Admitted without its lock, so its counter was not written unlocked.
    assert ff._granted(directory) == 0


def test_observe_past_a_busy_lock_admits_without_touching_the_counter(
        tmp_path, monkeypatch):
    _stable(tmp_path)
    queue = _queue(tmp_path)
    binding = _register(queue, tmp_path / "spool", ("reservations", HOST, "spool_gb"))["binding"]
    directory = ff.fs_dir(queue.root, binding["key"])
    script = (f"import sys,time; sys.path.insert(0,{str(Path(ff.__file__).parents[1])!r});"
              "from prismabuild import filesystem_floor as ff;"
              "from pathlib import Path;"
              f"cm=ff.floor_locked(Path({str(directory)!r}));"
              "assert cm.__enter__(); print('held', flush=True); time.sleep(30)")
    monkeypatch.setattr(ff, "LOCK_WAIT_S", 0.3)
    monkeypatch.setenv(ff.MODE_ENV, "observe")
    holder = subprocess.Popen([sys.executable, "-c", script], stdout=subprocess.PIPE, text=True)
    try:
        assert holder.stdout.readline().strip() == "held"
        assert queue.ledger(HOST).acquire("a" * 64, {"spool_gb": 1})
        assert ff._granted(directory) == 0
    finally:
        holder.kill()
        holder.wait()


# -- #2046 identity and closed-accounting acceptance -------------------------

@pytest.fixture
def floor_2046_world(tmp_path, monkeypatch, enforce):
    """Portable local backing identity; only the OS capacity sensors are fake."""
    monkeypatch.setattr(ff, "_mount_row", lambda fd: ("ext4", "none", "rw"))
    monkeypatch.setattr(ff, "_block_uuid", lambda dev: "floor-2046-device")
    monkeypatch.setattr(ff, "_host_addresses", lambda: [])
    vfs = os.statvfs_result((1, 1, 100 * GIB, 90 * GIB, 90 * GIB,
                            1000, 900, 900, 0, 255))
    monkeypatch.setattr(ff.os, "statvfs", lambda path: vfs)
    monkeypatch.setattr(ff.os, "fstatvfs", lambda fd: vfs)
    queue = _queue(tmp_path)
    result = ff.register(queue.root, tmp_path, filesystem_gib=32,
                         filesystem_ledger="floor2046")
    binding = result["binding"]
    ledger = pool.ResourceLedger(queue.root / ff.FILESYSTEM_RESERVATIONS, host="floor2046")
    return queue, binding, ledger


def _floor_2046_begin(ledger, count=1):
    """Both explicit refusal and the existing no-handle API are safe denial."""
    try:
        return ledger.begin_acquire("candidate", {ff.FILESYSTEM_KIND: count})
    except ff.FloorError:
        return None


def test_2046_unbound_zero_demand_is_not_population_proof(tmp_path, floor_2046_world):
    _queue, _binding, _ledger = floor_2046_world
    other = tmp_path / "unregistered-queue"
    verdicts = ff.check_paths(other, [tmp_path])
    assert verdicts and any(not v["allowed"] for v in verdicts), verdicts
    with pytest.raises(ff.FloorRefused):
        ff.enforce_paths(other, [tmp_path], label="unregistered")


def test_2046_exact_floor_aliases_and_aggregate(tmp_path, monkeypatch, floor_2046_world):
    queue, binding, ledger = floor_2046_world
    alias = tmp_path / "alias"
    alias.symlink_to(tmp_path, target_is_directory=True)
    second = pool.ResourceLedger(queue.root / "tier-reservations", host="second")
    second.ensure_capacity({"stage_gib": 8})
    ff.register(queue.root, tmp_path,
                members=[("tier-reservations", "second", "stage_gib")])
    private = ledger.begin_acquire("incumbent", {ff.FILESYSTEM_KIND: 2})
    assert private is not None
    assert ledger.commit_acquire("action-a", private) == 2
    assert second.acquire("action-b", {"stage_gib": 3})
    # Even demonstrably resident data is not an owner/sample-bound credit.
    # W1 has approved no exact credit producer; keep the complete allowances.
    (tmp_path / "materialized-output").write_bytes(b"resident" * 1024)
    charge = 5 * GIB
    for size in (100 * GIB, 100 * GIB + 1, 100 * GIB + 19):
        for delta in (0, -1):
            free = (size + 19) // 20 + charge + GIB + delta
            vfs = os.statvfs_result((1, 1, size, free, free, 1000, 900, 900, 0, 255))
            monkeypatch.setattr(ff.os, "statvfs", lambda path: vfs)
            monkeypatch.setattr(ff.os, "fstatvfs", lambda fd: vfs)
            assert ff.refresh_binding(queue.root, binding)["allowed"]
            verdicts = ff.check_paths(queue.root, [tmp_path, alias])
            assert all(v["allowed"] for v in verdicts), verdicts
            local, = [v for v in verdicts if v.get("sample") == "fresh-local"]
            assert local["charge_bytes"] == charge
            assert local["required_bytes"] == (size + 19) // 20 + charge
            proposed = _floor_2046_begin(ledger)
            assert (proposed is not None) is (delta == 0)
            if proposed is not None:
                assert ledger.abandon_acquire(proposed) == 1
    # Equal/below arithmetic is meaningful only for a closed population.
    # The same roomy physical alias without an accounting registry must refuse.
    unregistered = ff.check_paths(tmp_path / "other-queue", [alias])
    assert any(not v["allowed"] for v in unregistered), unregistered


def test_2046_sample_rejects_descriptor_identity_mismatch(tmp_path, monkeypatch, floor_2046_world):
    original_fstat = ff.os.fstat
    monkeypatch.setattr(ff, "_block_uuid", lambda dev: f"device-{dev}")
    found = ff.identify(tmp_path)

    def substituted_fstat(fd):
        fields = list(original_fstat(fd))
        fields[2] = found["device"] + 1
        return os.stat_result(fields)

    monkeypatch.setattr(ff.os, "fstat", substituted_fstat)
    assert ff.identify(tmp_path)["key"] != found["key"]
    with pytest.raises(ff.FloorError):
        ff._sample(found)


def test_2046_zfs_sample_rejects_changed_pool_at_path(tmp_path, monkeypatch, floor_2046_world):
    source = ["pool-a/work"]
    monkeypatch.setattr(ff, "_mount_row", lambda fd: ("zfs", source[0], "rw"))
    monkeypatch.setattr(ff, "_command", lambda argv: {"pool-a": "101", "pool-b": "202"}[argv[-1]])
    monkeypatch.setattr(ff, "_zfs_space", lambda name: (100 * GIB, 90 * GIB))
    found = ff.identify(tmp_path)
    assert found["key"] == "zfs:101"
    source[0] = "pool-b/work"
    assert ff.identify(tmp_path)["key"] == "zfs:202"
    with pytest.raises(ff.FloorError):
        ff._sample(found)


def test_2046_same_server_exports_not_suppressed(tmp_path, monkeypatch, floor_2046_world):
    queue, binding, _ledger = floor_2046_world
    ff._floor_write(ff.fs_dir(queue.root, binding["key"]) / "binding.json",
                    {**binding, "server_addresses": ["192.0.2.9"]})
    original = ff.identify(tmp_path)
    first, second = tmp_path / "export-a", tmp_path / "export-b"
    calls = []

    def identify_export(path):
        calls.append(str(path))
        index = 1 if Path(path) == first else 2
        return {**original, "path": str(path), "fstype": "nfs4", "key": None,
                "device": index, "source": f"192.0.2.9:/export-{index}",
                "server": "192.0.2.9", "free_bytes": 90 * GIB if index == 1 else 0}

    monkeypatch.setattr(ff, "identify", identify_export)
    one = ff.check_paths(queue.root, [first])
    both = ff.check_paths(queue.root, [first, second])
    reverse = ff.check_paths(queue.root, [second, first])
    assert all(any(not v["allowed"] for v in vs) for vs in (one, both, reverse)), (one, both, reverse)
    assert str(first) in calls and str(second) in calls


@pytest.mark.parametrize("damage", ["missing", "regressed"])
def test_2046_counter_damage_refuses_acquisition(tmp_path, floor_2046_world, damage):
    queue, binding, ledger = floor_2046_world
    handle = ledger.begin_acquire("incumbent", {ff.FILESYSTEM_KIND: 2})
    assert handle and ledger.commit_acquire("incumbent", handle) == 2
    assert ff.refresh_binding(queue.root, binding)["allowed"]
    directory = ff.fs_dir(queue.root, binding["key"])
    if damage == "missing":
        (directory / "granted.json").unlink()
    else:
        ff._floor_write(directory / "granted.json", {"granted_bytes": 0})
    verdict = ff.published_verdict(queue.root, binding)
    assert not verdict["allowed"], verdict
    assert _floor_2046_begin(ledger) is None


def test_2046_grant_write_failure_cannot_leave_success(tmp_path, monkeypatch, floor_2046_world):
    queue, binding, ledger = floor_2046_world
    write = ff._floor_write

    def broken_grant_write(path, value):
        if Path(path).name == "granted.json":
            raise OSError("injected floor grant persistence failure")
        return write(path, value)

    monkeypatch.setattr(ff, "_floor_write", broken_grant_write)
    assert _floor_2046_begin(ledger) is None, "tokens escaped without a durable floor charge"
    assert (ledger.available()[ff.FILESYSTEM_KIND] == 32
            or not ff.published_verdict(queue.root, binding)["allowed"])


def test_2046_missing_member_population_is_not_empty(tmp_path, floor_2046_world):
    queue, binding, ledger = floor_2046_world
    handle = ledger.begin_acquire("incumbent", {ff.FILESYSTEM_KIND: 2})
    assert handle and ledger.commit_acquire("incumbent", handle) == 2
    ledger.held_dir.rename(ledger.held_dir.with_name("held-unreadable"))
    try:
        result = ff.refresh_binding(queue.root, binding)
    except (ff.FloorError, OSError):
        pass
    else:
        assert not result["allowed"], result
    assert _floor_2046_begin(ledger) is None


def test_2046_transfer_during_census_cannot_erase_charge(tmp_path, monkeypatch, floor_2046_world):
    queue, binding, ledger = floor_2046_world
    handle = ledger.begin_acquire("operation", {ff.FILESYSTEM_KIND: 1})
    assert handle and ledger.commit_acquire("z", handle) == 1
    (ledger.held_dir / "a").mkdir(exist_ok=True)
    original_listdir = os.listdir
    in_transfer = [False]

    def moving_listdir(path):
        path = Path(path)
        if in_transfer[0]:
            return original_listdir(path)
        if path == ledger.held_dir:
            return ["a", "z"]
        if path.parent == ledger.held_dir and path.name in ("a", "z"):
            try:
                result = original_listdir(path)
            except FileNotFoundError:
                result = []
            # Capture A before Z->A, then Z before A->Z. Neither pass can
            # see this token, although the existing owner always holds it.
            src, dst = ("z", "a") if path.name == "a" else ("a", "z")
            in_transfer[0] = True
            try:
                ledger.transfer(src, dst)
            finally:
                in_transfer[0] = False
            return result
        return original_listdir(path)

    monkeypatch.setattr(ff.os, "listdir", moving_listdir)
    try:
        result = ff.refresh_binding(queue.root, binding)
    except ff.FloorError:
        pass
    else:
        assert not result["allowed"] or result["charge_bytes"] >= GIB, result
    verdict = ff.published_verdict(queue.root, binding)
    assert not verdict["allowed"] or verdict["charge_bytes"] >= GIB, verdict


def test_2046_refresh_probes_outside_floor_locks(tmp_path, monkeypatch, floor_2046_world):
    from contextlib import contextmanager
    queue, binding, _ledger = floor_2046_world
    locked = [False]
    original_lock = ff.floor_locked
    original_sample = ff._sample

    @contextmanager
    def tracked_lock(*args, **kwargs):
        with original_lock(*args, **kwargs) as acquired:
            locked[0] = bool(acquired)
            try:
                yield acquired
            finally:
                locked[0] = False

    def sample(found):
        assert not locked[0], "blocking physical sample under admission floor lock"
        return original_sample(found)

    monkeypatch.setattr(ff, "floor_locked", tracked_lock)
    monkeypatch.setattr(ff, "_sample", sample)
    result = ff.refresh_binding(queue.root, binding)
    assert result["allowed"], result


def test_2046_refresh_cannot_restamp_stale_sensor_data(tmp_path, monkeypatch, floor_2046_world):
    queue, binding, ledger = floor_2046_world
    clock = [1000.0]
    monkeypatch.setattr(ff.time, "time", lambda: clock[0])
    assert ff.refresh_binding(queue.root, binding)["allowed"]
    original_sample = ff._sample

    def slow_sample(found):
        result = original_sample(found)
        clock[0] += ff.SAMPLE_MAX_AGE_S + ff.CLOCK_SKEW_S + 1
        return result

    monkeypatch.setattr(ff, "_sample", slow_sample)
    try:
        result = ff.refresh_binding(queue.root, binding)
    except ff.FloorError:
        pass
    else:
        assert not result["allowed"], result
    assert _floor_2046_begin(ledger) is None


@pytest.mark.parametrize("state", ["malformed", "unreadable"])
def test_2046_enforcement_state_errors_never_allow(tmp_path, monkeypatch, floor_2046_world, state):
    queue, _binding, ledger = floor_2046_world
    monkeypatch.delenv(ff.MODE_ENV, raising=False)
    path = ff.floor_root(queue.root) / "mode"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("not-a-mode" if state == "malformed" else "enforce")
    if state == "unreadable":
        original = Path.read_text

        def read_text(self, *args, **kwargs):
            if self == path:
                raise PermissionError("injected mode read failure")
            return original(self, *args, **kwargs)

        monkeypatch.setattr(Path, "read_text", read_text)
    assert _floor_2046_begin(ledger) is None


def test_a_malformed_owner_record_does_not_stop_the_reaper(tmp_path, enforce, ample_space):
    queue, _binding, ledger = _growth_queue(tmp_path)
    operations = ledger.base / "operations"
    operations.mkdir(parents=True, exist_ok=True)
    (operations / "aaa-bad.json").write_text('{"holder": "x", "lease_until": "soon"}')
    assert ledger.acquire("operation-dead", {"filesystem_gib": 1})
    ff._floor_write(operations / "zzz-dead.json", {
        "holder": "operation-dead", "machine_id": ff._machine_id(), "boot_id": ff._floor_boot_id(),
        "pid": 2 ** 22 + 7, "pid_start": "1", "lease_until": time.time() + 600})
    assert ff.reap_operations(queue.root) == ["operation-dead"]
    assert ledger.held().get("filesystem_gib", 0) == 0
