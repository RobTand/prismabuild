"""The floor guard subtracts what rows actually hold, not the announce (#1222).

The guard's window budget was ``MemTotal - max(c_max, arc_floor) - reserve``
minus the *announced* worker demand (#645).  With the measured offer the
announce is the roof itself, so the subtraction both starved the tier
behind an offer nobody was using and double-counted once tier fills hold
host tokens in the same ledger.  The approved replacement is one pool, two
consumers: rows and tier fills acquire from the same host ledger, and the
window gate checks the rows' held tokens beside the window --
``window + rows_held + max(c_max, arc_floor) + reserve <= MemTotal``.

Tonight's live tier is the regression the new contract must satisfy: the
240 GiB roof still admits (240 <= 294.5 - 22 - 16), a 112 GiB window beside
88 GiB of held rows still admits (112 + 88 + 22 + 16 = 238 <= 294.5), and a
window publish toward 256 with 96 GiB of rows held refuses.
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


def _no_zfs(argv: list[str]) -> str:
    raise OSError(f"no {argv[0]} on this box")


def _ram_tier(tmp_path: Path, *, size_gib: int, window_gib: int,
              memtotal_gib: float = 294, arc_c_max_gib: int = 22,
              arc_meta_gib: int = 5, arc_floor_gib: int = 20,
              reserve_gib: int = 16, ceiling_max_gib: int = 256,
              rows_held_gib: int | None = None):
    mount = tmp_path / "ram"
    mount.mkdir(exist_ok=True)
    proc = tmp_path / "proc"
    proc.mkdir(exist_ok=True)
    (proc / "mounts").write_text(
        f"tmpfs {mount} tmpfs rw,noswap,size={size_gib}G 0 0\n")
    (proc / "meminfo").write_text(
        f"MemTotal:  {int(memtotal_gib * GIB) // 1024} kB\n")
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
            "window_gib_default": window_gib, "arc_floor_gib": arc_floor_gib,
            "system_reserve_gib": reserve_gib, "prefill_depth": None,
        },
        statvfs=statvfs, proc_mounts=str(proc / "mounts"),
        meminfo_path=str(proc / "meminfo"), rows_held_gib=rows_held_gib)
    return tiers[storage_tiers.tier_id("ram", HOST)]


def _worker_record(host: str, mem_gb: object) -> dict[str, object]:
    return {"schema": "prismaquant.prismabuild.pool_offer.v1", "host": host,
            "tags": ["x86"], "has_gpu": False,
            "capacity": {"cpu": 80, "mem_gb": mem_gb}}


def test_a_window_that_cannot_coexist_with_jobs_is_refused(
        tmp_path: Path) -> None:
    """A publish raising the window toward 256 with 96 GiB offered refuses."""

    tier = _ram_tier(tmp_path, size_gib=240, window_gib=200,
                     rows_held_gib=96)

    admission = tier["ram_admission"]
    assert admission["admissible"] is False
    assert admission["reason"] == "ram_window_exceeds_memtotal_floor"
    # Every number named, including the subtracted worker demand.
    assert admission["ceiling_bytes"] == 240 * GIB
    assert admission["window_bytes"] == 200 * GIB
    assert admission["mem_total_bytes"] == 294 * GIB
    assert admission["arc_c_max"] == 22 * GIB
    assert admission["arc_floor_bytes"] == 20 * GIB
    assert admission["system_reserve_bytes"] == 16 * GIB
    assert admission["rows_held_bytes"] == 96 * GIB
    assert admission["allowed_ceiling_bytes"] == 294 * GIB - 22 * GIB - 16 * GIB
    assert admission["allowed_window_bytes"] == (
        294 * GIB - 22 * GIB - 16 * GIB - 96 * GIB)
    # Fail-closed: the tier is announced, and it mints nothing.
    assert storage_tiers.tier_tokens(tier) == {}


def test_the_live_tier_still_admits(tmp_path: Path) -> None:
    """Tonight's box: 240 GiB roof, 112 GiB window, 96 GiB offered, and the
    ARC already shrunk to 22 GiB -- 112 + 96 + 22 + 16 = 246 <= 294.5."""

    tier = _ram_tier(tmp_path, size_gib=240, window_gib=112,
                     memtotal_gib=294.5, rows_held_gib=96)

    admission = tier["ram_admission"]
    assert admission["admissible"] is True, admission
    assert admission["reason"] is None
    assert admission["rows_held_bytes"] == 96 * GIB
    assert storage_tiers.tier_tokens(tier) == {
        storage_tiers.RAM_CAPACITY_KIND: 112}


def test_unknown_worker_demand_refuses_fail_closed(tmp_path: Path) -> None:
    """No announced offer is not evidence of no jobs: a loop can appear
    between cycles, so an unreadable offer refuses rather than admits."""

    tier = _ram_tier(tmp_path, size_gib=240, window_gib=112,
                     rows_held_gib=None)

    admission = tier["ram_admission"]
    assert admission["admissible"] is False
    assert admission["reason"] == "ram_rows_held_unknown"
    assert admission["rows_held_bytes"] is None
    assert admission["allowed_window_bytes"] is None
    assert storage_tiers.tier_tokens(tier) == {}


def test_the_announce_no_longer_feeds_the_gate() -> None:
    """One pool: the gate reads held tokens, never the announce record."""

    import inspect
    source = inspect.getsource(storage_tiers.ram_admission)
    assert "worker_mem_gb" not in source
    assert "rows_held_gib" in source
