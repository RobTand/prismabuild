"""The floor guard: a ceiling that cannot coexist with the ARC is refused.

A ``noswap`` tmpfs makes overfill ENOSPC -- fail-closed -- but only while the
roof it refuses at is real RAM.  ``size=`` is a limit on file bytes, not an
allocation, so a tmpfs whose ceiling plus the ARC's own permission plus the
system reserve exceeds ``MemTotal`` never reaches its ENOSPC: the OOM killer
arrives first, which is fail-random, not fail-closed.  The ARC is not a
number on the box either: it may be shrunk (the operator's runbook does
exactly that), but never below the policy's declared floor, and never below
the metadata it already holds, because ``arc_meta_used`` is not evictable.

So the guard refuses while ``ceiling + max(arc_c_max, arc_floor, arc_meta_used)
+ system_reserve > MemTotal``, and the refusal names every number it used --
an operator shrinking the ARC needs to know which one to move (#640).
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


def _ram_tier(tmp_path: Path, *, size_gib: int, memtotal_gib: int = 294,
              arc_c_max_gib: int = 22, arc_meta_gib: int = 5,
              arc_floor_gib: int = 20, reserve_gib: int = 16,
              ceiling_max_gib: int = 256,
               worker_mem_gb: int | None = None):
    mount = tmp_path / "ram"
    mount.mkdir(exist_ok=True)
    proc = tmp_path / "proc"
    proc.mkdir(exist_ok=True)
    (proc / "mounts").write_text(
        f"tmpfs {mount} tmpfs rw,noswap,size={size_gib}G 0 0\n")
    (proc / "meminfo").write_text(
        f"MemTotal:  {(memtotal_gib * GIB) // 1024} kB\n")
    (proc / "arcstats").write_text(
        f"c_max 4 {arc_c_max_gib * GIB}\nsize 4 {arc_c_max_gib * GIB // 2}\n"
        f"arc_meta_used 4 {arc_meta_gib * GIB}\n")

    def statvfs(path: str):
        if path != str(mount):
            raise OSError(f"no tmpfs at {path}")
        frsize = 4096
        block = GIB // frsize
        return os.statvfs_result(
            (frsize, frsize, size_gib * block, size_gib * block,
             size_gib * block, 1_000_000, 900_000, 900_000, 0, 255))

    tiers = storage_tiers.discover_tiers(
        host=HOST, runner=_no_zfs, arcstats_path=str(proc / "arcstats"),
        ram_policy={
            "schema": storage_tiers.RAM_TIER_POLICY_SCHEMA_V1,
            "mountpoint": str(mount), "ceiling_gib_max": ceiling_max_gib,
            "window_gib_default": 112, "arc_floor_gib": arc_floor_gib,
            "system_reserve_gib": reserve_gib, "prefill_depth": None,
        },
        statvfs=statvfs, proc_mounts=str(proc / "mounts"),
        meminfo_path=str(proc / "meminfo"), worker_mem_gb=worker_mem_gb)
    return tiers[storage_tiers.tier_id("ram", HOST)]


def test_a_ceiling_that_cannot_coexist_with_the_arc_is_refused(
        tmp_path: Path) -> None:
    """The box as it stands today: 256 GiB of tmpfs over a 240 GiB ARC."""

    tier = _ram_tier(tmp_path, size_gib=256, arc_c_max_gib=240)

    admission = tier["ram_admission"]
    assert admission["admissible"] is False
    assert admission["reason"] == "ram_ceiling_exceeds_memtotal_floor"
    # Every number the operator can move, named in the refusal's own record.
    assert admission["ceiling_bytes"] == 256 * GIB
    assert admission["mem_total_bytes"] == 294 * GIB
    assert admission["arc_c_max"] == 240 * GIB
    assert admission["system_reserve_bytes"] == 16 * GIB
    assert admission["allowed_ceiling_bytes"] == 294 * GIB - 240 * GIB - 16 * GIB
    # Fail-closed: the tier is announced, and it mints nothing.
    assert storage_tiers.tier_tokens(tier) == {}


def test_the_arc_floor_is_measured_not_only_declared(tmp_path: Path) -> None:
    """``arc_meta_used`` the ARC cannot drop is a real floor (#638's lesson)."""

    tier = _ram_tier(tmp_path, size_gib=256, arc_c_max_gib=22, arc_meta_gib=30)

    admission = tier["ram_admission"]
    assert admission["admissible"] is False
    assert admission["reason"] == "ram_ceiling_exceeds_memtotal_floor"
    assert admission["arc_floor_bytes"] == 30 * GIB, \
        "the larger of the declared floor and the metadata the ARC holds"
    assert admission["allowed_ceiling_bytes"] == 294 * GIB - 30 * GIB - 16 * GIB


def test_a_shrunk_arc_admits_the_ceiling(tmp_path: Path) -> None:
    """The runbook's own arithmetic: 256 + 22 + 16 = 294, exactly the box."""

    tier = _ram_tier(tmp_path, size_gib=256, arc_c_max_gib=22, arc_meta_gib=5,
                     # The fixture box offers no jobs; the window guard's own
                     # tests cover a nonzero offer (#645).
                     worker_mem_gb=0)

    assert tier["ram_admission"]["admissible"] is True
    assert storage_tiers.tier_tokens(tier) == {
        storage_tiers.RAM_CAPACITY_KIND: 112}


def test_a_ceiling_over_the_policy_maximum_is_refused(tmp_path: Path) -> None:
    """The policy records the largest ceiling Rob sanctioned; it is a bound."""

    tier = _ram_tier(tmp_path, size_gib=300, memtotal_gib=400,
                     arc_c_max_gib=22)

    admission = tier["ram_admission"]
    assert admission["admissible"] is False
    assert admission["reason"] == "ram_ceiling_exceeds_policy_max"
    assert storage_tiers.tier_tokens(tier) == {}
