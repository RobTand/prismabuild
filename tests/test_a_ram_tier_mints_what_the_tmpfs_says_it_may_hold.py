"""A ram tier is minted from the tmpfs's own statvfs, never from MemAvailable.

The ARC tier's capacity is a counter in ``arcstats``; the ram tier's is the
mount itself.  ``f_bavail x f_frsize`` is what the tmpfs may still hold -- the
same promise ``zfs available`` makes for the stage dataset -- and it is the
only number the ledger mints from.  ``MemAvailable`` is deliberately not read:
it moves with other tenants' habits, and a tier whose supply changes because
somebody else's process exited is a tier nobody reserved against.

The ceiling is the mount's own size (``f_blocks x f_frsize``): the roof where
a full ``noswap`` tmpfs answers ENOSPC rather than paging out.  The policy's
window caps what PB fills below it, and the epoch -- stamped into a marker
file at the mount root at bootstrap, so it dies with the tmpfs on reboot --
rides on the record so every fragment and receipt the tier admits can carry
it (#640).
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import prismabuild.storage_tiers as storage_tiers  # noqa: E402

GIB = storage_tiers.GIB
HOST = "dl380g10"
#: 294 GiB, as /proc/meminfo reports it on the storage box.
MEM_TOTAL_KB = (294 * GIB) // 1024
#: An ARC already shrunk to make room, as arcstats reports it.
ARC_C_MAX = 22 * GIB
ARC_META_USED = 5 * GIB


def _no_zfs(argv: list[str]) -> str:
    raise OSError(f"no {argv[0]} on this box")


def _mount(tmp_path: Path) -> Path:
    """The policy's mountpoint, as a directory the test owns: the epoch
    marker is written into the real root otherwise, and a faked
    ``/proc/mounts`` must not license writes the mount never made."""

    root = tmp_path / "ram"
    root.mkdir(exist_ok=True)
    return root


def _policy(mount: Path, **over) -> dict[str, object]:
    policy: dict[str, object] = {
        "schema": storage_tiers.RAM_TIER_POLICY_SCHEMA_V1,
        "mountpoint": str(mount),
        "ceiling_gib_max": 256,
        "window_gib_default": 160,
        "arc_floor_gib": 20,
        "system_reserve_gib": 16,
        "prefill_depth": None,
    }
    policy.update(over)
    return policy


def _proc_files(tmp_path: Path, mount: Path, *,
                options: str = "rw,noswap,size=256G",
                memtotal_kb: int = MEM_TOTAL_KB,
                arc_c_max: int = ARC_C_MAX, arc_meta: int = ARC_META_USED):
    """Fake /proc the way the box would answer it: mounts, meminfo, arcstats."""

    proc = tmp_path / "proc"
    proc.mkdir(exist_ok=True)
    (proc / "mounts").write_text(
        f"sysfs /sys sysfs rw 0 0\n"
        f"tmpfs {mount} tmpfs {options} 0 0\n")
    (proc / "meminfo").write_text(f"MemTotal:  {memtotal_kb} kB\n")
    (proc / "arcstats").write_text(
        f"c_max 4 {arc_c_max}\nsize 4 {arc_c_max // 2}\n"
        f"arc_meta_used 4 {arc_meta}\n")
    return str(proc / "mounts"), str(proc / "meminfo"), str(proc / "arcstats")


def _statvfs(mount: Path, *, size_gib: int, free_gib: int, frsize: int = 4096):
    """A statvfs reader answering for the tmpfs, and raising anywhere else."""

    def read(path: str):
        if path != str(mount):
            raise OSError(f"no tmpfs at {path}")
        block = GIB // frsize
        return os.statvfs_result(
            (frsize, frsize, size_gib * block, free_gib * block,
             free_gib * block, 1_000_000, 900_000, 900_000, 0, 255))

    return read


def _ram_tier(tmp_path: Path, *, mount: Path | None = None,
              statvfs=None, options: str = "rw,noswap,size=256G",
              policy_over: dict | None = None,
              # The fixture box offers no jobs; the guard's own tests cover
              # a nonzero offer (#645).
              worker_mem_gb: int | None = 0) -> dict[str, object] | None:
    mount = mount if mount is not None else _mount(tmp_path)
    proc_mounts, meminfo, arcstats = _proc_files(tmp_path, mount,
                                                 options=options)
    tiers = storage_tiers.discover_tiers(
        host=HOST, runner=_no_zfs, arcstats_path=arcstats,
        ram_policy=_policy(mount, **(policy_over or {})),
        statvfs=statvfs or _statvfs(mount, size_gib=256, free_gib=200),
        proc_mounts=proc_mounts, meminfo_path=meminfo,
        worker_mem_gb=worker_mem_gb)
    return tiers.get(storage_tiers.tier_id("ram", HOST))


def test_capacity_is_what_the_mount_may_still_hold(tmp_path: Path) -> None:
    mount = _mount(tmp_path)
    tier = _ram_tier(tmp_path, mount=mount,
                     statvfs=_statvfs(mount, size_gib=256, free_gib=200))

    assert tier is not None
    assert tier["capacity_bytes"] == 200 * GIB
    assert storage_tiers.tier_tokens(tier) == {
        storage_tiers.RAM_CAPACITY_KIND: 160}, \
        "the policy window caps the mint, not the mount's whole free space"


def test_the_ceiling_is_the_mounts_own_size(tmp_path: Path) -> None:
    mount = _mount(tmp_path)
    tier = _ram_tier(tmp_path, mount=mount,
                     statvfs=_statvfs(mount, size_gib=256, free_gib=200))

    assert tier["ceiling_bytes"] == 256 * GIB
    assert tier["size_bytes"] == tier["ceiling_bytes"]
    assert tier["capacity_bytes"] < tier["ceiling_bytes"]


def test_the_epoch_is_stamped_at_the_mount_and_survives_the_cycle(
        tmp_path: Path) -> None:
    """The marker dies with the tmpfs; a fresh mount is a fresh epoch.

    The marker is written at bootstrap by the tier loop, so the first cycle
    after a mount stamps it and every later cycle reads the same one back.
    Deleting it is the reboot shape -- the tmpfs emptied -- and the next
    discovery must answer a *different* epoch, because an epoch that survived
    the bytes it named would make the whole identity a no-op.
    """

    mount = _mount(tmp_path)
    epoch_path = mount / storage_tiers.RAM_EPOCH_MARKER

    def discover() -> dict[str, object]:
        proc_mounts, meminfo, arcstats = _proc_files(tmp_path, mount)
        tiers = storage_tiers.discover_tiers(
            host=HOST, runner=_no_zfs, arcstats_path=arcstats,
            ram_policy=_policy(mount),
            statvfs=_statvfs(mount, size_gib=256, free_gib=200),
            proc_mounts=proc_mounts, meminfo_path=meminfo,
            worker_mem_gb=0)
        return tiers[storage_tiers.tier_id("ram", HOST)]

    tier = discover()
    marker = json.loads(epoch_path.read_text())
    assert marker["schema"] == storage_tiers.RAM_EPOCH_MARKER_SCHEMA_V1
    assert tier["epoch"] == marker["epoch"]

    again = discover()
    assert again["epoch"] == tier["epoch"], "one mount, one epoch"

    epoch_path.unlink()                      # the reboot: the tmpfs emptied
    rebooted = discover()
    assert rebooted["epoch"] != tier["epoch"]


def test_the_record_announces_what_an_operator_must_check(tmp_path: Path) -> None:
    tier = _ram_tier(tmp_path)

    assert tier["mountpoint"] == str(tmp_path / "ram")
    assert tier["mount_options"] == ["rw", "noswap", "size=256G"]
    assert isinstance(tier["epoch"], str) and tier["epoch"]
    assert tier["ram_admission"]["admissible"] is True
    assert tier["ram_admission"]["reason"] is None


def test_the_demand_a_ram_range_implies_is_derived_not_guessed() -> None:
    """``ram_gib@ram:<host>`` comes out of the same arithmetic as ``stage_gib``.

    The withdrawn #639 part 3 put a second tier leg on one mover's demand;
    the ram tier's own movement nodes carry that leg, and the demand key is
    derived from the range exactly the way a stage mover's is.
    """

    demand = storage_tiers.residency_demand(
        tier_id=storage_tiers.tier_id("ram", HOST),
        range_start_bytes=0, range_end_bytes=3 * GIB + 1)

    assert demand == {f"ram_gib@ram:{HOST}": 4}
