"""No mount, no tier; an unreadable mount is announced empty, never guessed.

The ram tier is discovered from a mount that is actually there, the way a
stage tier is discovered from a pool that is actually imported.  A box with
no tmpfs at the policy's mountpoint announces nothing -- ``MemAvailable`` is
not a substitute, because free RAM is not placed RAM -- and the fleet keeps
working exactly as it did before the tier existed.

A mount that is there but whose ``statvfs`` cannot be read is the harsher
case, and it is announced rather than hidden: the tier record says the mount
exists, mints no tokens, and carries the refusal, so an operator reading the
announced record sees a tmpfs that is not answering rather than a tier that
quietly vanished (#640).
"""
from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import prismabuild.storage_tiers as storage_tiers  # noqa: E402

GIB = storage_tiers.GIB
HOST = "dl380g10"
RAM_TIER = storage_tiers.tier_id("ram", HOST)


def _no_zfs(argv: list[str]) -> str:
    raise OSError(f"no {argv[0]} on this box")


def _discover(tmp_path: Path, *, mounts: str, statvfs) -> dict[str, object]:
    mount = tmp_path / "ram"
    mount.mkdir(exist_ok=True)
    proc = tmp_path / "proc"
    proc.mkdir(exist_ok=True)
    (proc / "mounts").write_text(mounts.replace("@ram@", str(mount)))
    (proc / "meminfo").write_text(f"MemTotal:  {(294 * GIB) // 1024} kB\n")
    (proc / "arcstats").write_text(
        f"c_max 4 {22 * GIB}\nsize 4 {11 * GIB}\narc_meta_used 4 {5 * GIB}\n")
    return storage_tiers.discover_tiers(
        host=HOST, runner=_no_zfs, arcstats_path=str(proc / "arcstats"),
        ram_policy={
            "schema": storage_tiers.RAM_TIER_POLICY_SCHEMA_V1,
            "mountpoint": str(mount), "ceiling_gib_max": 256,
            "window_gib_default": 112, "arc_floor_gib": 20,
            "system_reserve_gib": 16, "prefill_depth": None,
        },
        statvfs=statvfs, proc_mounts=str(proc / "mounts"),
        meminfo_path=str(proc / "meminfo"))


def test_no_mount_announces_nothing(tmp_path: Path) -> None:
    def statvfs(path: str):
        raise OSError(f"nothing is mounted at {path}")

    tiers = _discover(tmp_path, mounts="sysfs /sys sysfs rw 0 0\n",
                      statvfs=statvfs)

    assert RAM_TIER not in tiers, \
        "a box with no tmpfs offers no ram tier, and that is the true answer"


def test_a_mount_whose_statvfs_cannot_be_read_mints_nothing(
        tmp_path: Path) -> None:
    def statvfs(path: str):
        raise OSError("ESTALE: the mount is not answering")

    tiers = _discover(
        tmp_path, mounts="tmpfs @ram@ tmpfs rw,noswap,size=256G 0 0\n",
        statvfs=statvfs)

    tier = tiers[RAM_TIER]
    assert tier is not None, "the mount is there, so the tier is announced"
    assert tier["capacity_bytes"] == 0
    assert tier["ram_admission"]["admissible"] is False
    assert tier["ram_admission"]["reason"] == "ram_statvfs_unreadable"
    assert storage_tiers.tier_tokens(tier) == {}
