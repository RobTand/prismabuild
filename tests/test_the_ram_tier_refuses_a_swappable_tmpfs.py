"""A swappable tmpfs refuses the warm-path admission: it would be a lie.

``noswap`` is what makes a full tmpfs answer ENOSPC.  Without it the kernel
may page the "resident" bytes out to swap, and a consumer whose gate says
resident then pays a *swap read* behind a claim of RAM -- the correctness lie
this tier exists to end.  So the mount's own options, read from
``/proc/mounts``, are announced on the record, and a mount that does not
carry ``noswap`` admits nothing: no ``ram_gib`` token is minted, so no
promotion can be admitted against it, however much free space statvfs
reports (#640).
"""
from __future__ import annotations

import os
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import prismabuild.storage_tiers as storage_tiers  # noqa: E402

GIB = storage_tiers.GIB
HOST = "dl380g10"


def _no_zfs(argv: list[str]) -> str:
    raise OSError(f"no {argv[0]} on this box")


def _ram_tier(tmp_path: Path, options: str) -> dict[str, object]:
    mount = tmp_path / "ram"
    mount.mkdir(exist_ok=True)
    proc = tmp_path / "proc"
    proc.mkdir(exist_ok=True)
    (proc / "mounts").write_text(f"tmpfs {mount} tmpfs {options} 0 0\n")
    (proc / "meminfo").write_text(f"MemTotal:  {(294 * GIB) // 1024} kB\n")
    (proc / "arcstats").write_text(
        f"c_max 4 {22 * GIB}\nsize 4 {11 * GIB}\narc_meta_used 4 {5 * GIB}\n")

    def statvfs(path: str):
        if path != str(mount):
            raise OSError(f"no tmpfs at {path}")
        frsize = 4096
        block = GIB // frsize
        return os.statvfs_result(
            (frsize, frsize, 256 * block, 200 * block,
             200 * block, 1_000_000, 900_000, 900_000, 0, 255))

    tiers = storage_tiers.discover_tiers(
        host=HOST, runner=_no_zfs, arcstats_path=str(proc / "arcstats"),
        ram_policy={
            "schema": storage_tiers.RAM_TIER_POLICY_SCHEMA_V1,
            "mountpoint": str(mount), "ceiling_gib_max": 256,
            "window_gib_default": 112, "arc_floor_gib": 20,
            "system_reserve_gib": 16, "prefill_depth": None,
        },
        statvfs=statvfs, proc_mounts=str(proc / "mounts"),
        meminfo_path=str(proc / "meminfo"))
    return tiers[storage_tiers.tier_id("ram", HOST)]


def test_a_mount_without_noswap_refuses_the_warm_path(tmp_path: Path) -> None:
    tier = _ram_tier(tmp_path, options="rw,size=256G")

    assert tier["mount_options"] == ["rw", "size=256G"]
    assert tier["ram_admission"]["admissible"] is False
    assert tier["ram_admission"]["reason"] == "ram_mount_not_noswap"
    # Refused means refused: statvfs reported 200 GiB free and none of it is
    # offered, because a swappable tmpfs is not a RAM tier.
    assert storage_tiers.tier_tokens(tier) == {}


def test_a_noswap_mount_announces_its_options_and_admits(tmp_path: Path) -> None:
    tier = _ram_tier(tmp_path, options="rw,noswap,size=256G,mode=0755")

    assert tier["mount_options"] == ["rw", "noswap", "size=256G", "mode=0755"]
    assert tier["ram_admission"]["admissible"] is True
    assert storage_tiers.tier_tokens(tier) == {
        storage_tiers.RAM_CAPACITY_KIND: 112}
