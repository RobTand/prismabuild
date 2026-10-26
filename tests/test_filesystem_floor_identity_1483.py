"""Live local incarnation and actual leaf identities gate existing owners."""
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace

import pytest

from filesystem_floor_fixture import closed_coverage
from prismabuild import filesystem_floor as ff
from prismabuild.digest_primitives import canonical_sha256
from test_filesystem_floor_accounting_1483 import bind, world  # noqa: F401


@pytest.mark.parametrize("changed", ["boot", "namespace"])
@pytest.mark.parametrize("route", ["published", "path", "host", "stage", "local"])
def test_local_incarnation_change_holds_before_refresh(world, monkeypatch, changed, route):
    queue, _, root = world
    binding = bind(world, ("reservations", "test-host", "spool_gb"),
                   ("tier-reservations", "test-stage", "stage_gib"),
                   ("tier-reservations", "test-local", "local_gib"))
    assert ff.published_verdict(queue.root, binding)["allowed"]
    ff.member_bindings(queue.root)
    if changed == "boot":
        monkeypatch.setattr(ff, "_floor_boot_id", lambda: "next-boot")
    else:
        stat = os.stat

        def moved_namespace(path, *args, **kwargs):
            if path == "/proc/self/ns/mnt":
                return SimpleNamespace(st_ino=binding["mount_namespace"] + 1)
            return stat(path, *args, **kwargs)

        monkeypatch.setattr(os, "stat", moved_namespace)
    if route == "published":
        assert not ff.published_verdict(queue.root, binding)["allowed"]
    elif route == "path":
        assert not all(v["allowed"] for v in ff.check_paths(queue.root, [root]))
    elif route == "host":
        host = queue.ledger("test-host")
        assert not host.acquire("b" * 64, {"spool_gb": 1})
        assert host.holder_tokens("b" * 64) == {}
    else:
        tier = queue.tier("test-" + route)
        assert not tier.take("stale-owner", {route + "_gib": 1})
        assert tier.holdings("stale-owner") == {}


def test_remote_owner_keeps_its_own_incarnation(world, monkeypatch):
    queue, _, root = world
    binding = bind(world, ("reservations", "test-host", "spool_gb"))
    alias = dict(server="192.0.2.1", source="server:/export",
                 machine_id=ff._machine_id(), boot_id=ff._floor_boot_id(),
                 mount_namespace=os.stat("/proc/self/ns/mnt").st_ino,
                 device=12, mount_id=34)
    binding.update(machine_id="remote-machine", boot_id="remote-boot", mount_namespace=-1)
    binding["coverage"] = closed_coverage(binding, nfs_aliases=[alias])
    directory = ff.fs_dir(queue.root, binding["key"])
    ff._floor_write(directory / "binding.json", binding)
    sample = ff._floor_read(directory / "sample.json")
    sample.update(binding_sha256=ff.coverage_identity(binding),
                  coverage_sha256=canonical_sha256(binding["coverage"]))
    ff._floor_write(directory / "sample.json", sample)
    ff._RECORDS.clear()
    local = ff.identify(root)
    assert not all(v["allowed"] for v in ff.check_paths(queue.root, [root]))
    assert ff.published_verdict(queue.root, binding)["allowed"]
    assert queue.ledger("test-host").acquire("c" * 64, {"spool_gb": 1})
    monkeypatch.setattr(ff, "identify", lambda path:
        {**local, "key": None, "fstype": "nfs4", "server": alias["server"],
         "source": alias["source"], "device": alias["device"], "mount_id": alias["mount_id"]})
    assert all(v["allowed"] for v in ff.check_paths(queue.root, [root]))


def test_existing_leaf_uses_its_own_mount_descriptor(tmp_path, monkeypatch):
    source = tmp_path / "input.bin"
    source.write_bytes(b"payload")
    inode = source.stat().st_ino
    monkeypatch.setattr(ff, "_statx_mount_id", lambda fd:
                        999 if os.fstat(fd).st_ino == inode else 100)
    monkeypatch.setattr(ff, "_mount_row", lambda fd, mount_id=None:
                        ("zfs", "unregistered" if mount_id == 999 else "healthy", "rw"))
    monkeypatch.setattr(ff, "_command", lambda argv:
                        "202" if argv[-1] == "unregistered" else "101")
    monkeypatch.setattr(ff, "_zfs_space", lambda name:
                        (100 * ff.GIB, 0 if name == "unregistered" else 80 * ff.GIB))
    monkeypatch.setattr(ff, "_host_addresses", lambda: [])
    monkeypatch.setenv(ff.MODE_ENV, "enforce")
    queue = tmp_path / "queue"
    binding = ff.register(queue, tmp_path)["binding"]
    ff.register(queue, tmp_path, coverage=closed_coverage(binding))
    assert ff.identify(source)["mount_id"] == 999
    assert not all(v["allowed"] for v in ff.check_paths(queue, [source]))


def test_tmpfs_requires_explicit_complete_coverage(tmp_path, monkeypatch):
    monkeypatch.setenv(ff.MODE_ENV, "enforce")
    with tempfile.TemporaryDirectory(prefix="pb-floor-", dir="/dev/shm") as scratch:
        found = ff.identify(scratch)
        assert found["fstype"] == "tmpfs"
        assert not all(v["allowed"] for v in ff.check_paths(tmp_path, [scratch]))
        binding = ff.register(tmp_path, scratch)["binding"]
        assert not ff.published_verdict(tmp_path, binding)["allowed"]
        ff.register(tmp_path, scratch, coverage=closed_coverage(binding,
                    additional_bytes=found["free_bytes"]))
        assert not all(v["allowed"] for v in ff.check_paths(tmp_path, [scratch]))
        ff.register(tmp_path, scratch, coverage=closed_coverage(binding, additional_bytes=1))
        assert all(v["allowed"] for v in ff.check_paths(tmp_path, [scratch]))


def test_unidentified_disk_still_cannot_register(world, monkeypatch):
    queue, _, root = world
    found = ff.identify(root)
    monkeypatch.setattr(ff, "identify", lambda path: {**found, "key": "volatile:test"})
    with pytest.raises(ff.FloorError, match="no stable identity"):
        ff.register(queue.root, root)


def test_special_leaf_refuses_without_blocking(tmp_path):
    fifo = tmp_path / "pipe"
    os.mkfifo(fifo)
    with pytest.raises(ff.FloorError):
        ff.identify(fifo)
