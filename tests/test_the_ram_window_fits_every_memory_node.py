"""Configured window/ARC/reserve shares must fit every memory node (#1032).

Discover real RAM records and derive their real tokens using fixture-owned
host evidence. This guards the configured share, not actual per-node worker
or ARC placement, free pages, physical OOM safety, or fleet provisioning.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import prismabuild.storage_tiers as storage_tiers  # noqa: E402

HOST = "dl380g10"


def _no_zfs(argv: list[str]) -> str:
    raise OSError(f"no {argv[0]} on this fixture box")


def _discover_ram_tier(
        tmp_path: Path, *, node_memtotal_bytes: tuple[int, ...],
        node_ids: tuple[int, ...] | None = None,
        node_meminfo_override: dict[int, str | None] | None = None,
        ) -> dict[str, object]:
    gib = storage_tiers.GIB
    ids = tuple(range(len(node_memtotal_bytes))) if node_ids is None else node_ids
    assert len(ids) == len(node_memtotal_bytes)
    membership = "0-1" if ids == (0, 1) else ",".join(map(str, ids))
    options = "rw,noswap,size=256G"
    if len(ids) > 1:
        options += f",mpol=interleave:{membership}"
    assert sum(node_memtotal_bytes) == 294 * gib
    # Sysfs and proc spell MemTotal in integral KiB. Never round the inputs.
    assert all(total > 0 and total % 1024 == 0
               for total in node_memtotal_bytes)
    mount = tmp_path / "ram"
    mount.mkdir()
    proc = tmp_path / "proc"
    proc.mkdir()
    nodes = tmp_path / "memory_nodes"
    nodes.mkdir()
    (nodes / "has_memory").write_text(membership + "\n")
    for node, total in zip(ids, node_memtotal_bytes, strict=True):
        directory = nodes / f"node{node}"
        directory.mkdir()
        (directory / "cpulist").write_text("\n")
        text = (node_meminfo_override or {}).get(
            node, f"Node {node} MemTotal: {total // 1024} kB\n")
        if text is not None:
            (directory / "meminfo").write_text(text)
    (proc / "mounts").write_text(f"tmpfs {mount} tmpfs {options} 0 0\n")
    (proc / "meminfo").write_text(f"MemTotal: {294 * gib // 1024} kB\n")
    (proc / "arcstats").write_text(
        f"c_max 4 {22 * gib}\nsize 4 {11 * gib}\n"
        f"arc_meta_used 4 {5 * gib}\n")

    def statvfs(path: str) -> os.statvfs_result:
        if path != str(mount):
            raise OSError(f"no fixture tmpfs at {path}")
        # Byte quantities use the module's GIB, including a scaled fixture's
        # GIB; KiB blocks also preserve the proc/sysfs inputs exactly.
        frsize = 1024
        assert (256 * gib) % frsize == (200 * gib) % frsize == 0
        return os.statvfs_result(
            (frsize, frsize, 256 * gib // frsize, 200 * gib // frsize,
             200 * gib // frsize, 1_000_000, 900_000, 900_000, 0, 255))

    tiers = storage_tiers.discover_tiers(
        host=HOST, runner=_no_zfs, arcstats_path=str(proc / "arcstats"),
        proc_pressure=str(proc / "pressure" / "io"),
        ram_policy={
            "schema": storage_tiers.RAM_TIER_POLICY_SCHEMA_V1,
            "mountpoint": str(mount), "ceiling_gib_max": 256,
            # Explicit regression input, not a production-default change.
            "window_gib_default": 112, "arc_floor_gib": 20,
            "system_reserve_gib": 16, "prefill_depth": None,
        },
        statvfs=statvfs, proc_mounts=str(proc / "mounts"),
        meminfo_path=str(proc / "meminfo"), memory_numa_root=str(nodes),
        rows_held_gib=88)
    tier = tiers[storage_tiers.tier_id("ram", HOST)]
    assert tier["schema"] == storage_tiers.TIER_RECORD_SCHEMA_V1
    assert tier["tier"] == "ram"
    assert tier["host"] == HOST
    assert tier["mountpoint"] == str(mount)
    assert tier["memory_nodes"] == (None if node_meminfo_override else list(ids))
    assert tier["mount_options"] == options.split(",")
    assert isinstance(tier["epoch"], str) and tier["epoch"]
    assert tier["ceiling_bytes"] == tier["size_bytes"] == 256 * gib
    assert tier["capacity_bytes"] == 200 * gib
    assert tier["window_gib"] == 112
    admission = tier["ram_admission"]
    assert isinstance(admission, dict)
    assert admission["ceiling_bytes"] == 256 * gib
    assert admission["window_bytes"] == 112 * gib
    assert admission["mem_total_bytes"] == 294 * gib
    assert admission["arc_c_max"] == 22 * gib
    assert admission["arc_size"] == 11 * gib
    assert admission["arc_meta_used"] == 5 * gib
    assert admission["arc_floor_bytes"] == 20 * gib
    assert admission["system_reserve_bytes"] == 16 * gib
    assert admission["rows_held_bytes"] == 88 * gib
    # Ceiling fits exactly; rows remain in the existing whole-box guard.
    assert admission["allowed_ceiling_bytes"] == 256 * gib
    assert admission["allowed_window_bytes"] == 168 * gib
    return tier


def test_an_asymmetric_node_cannot_mint_a_window_its_share_exceeds(
        tmp_path: Path) -> None:
    gib = storage_tiers.GIB
    tier = _discover_ram_tier(
        tmp_path, node_memtotal_bytes=(60 * gib, 234 * gib))
    admission = tier["ram_admission"]
    assert isinstance(admission, dict)
    tokens = storage_tiers.tier_tokens(tier)
    # On old production, witness the actual unsafe discovery AND token
    # result before the attributable RED below. Keep the same test for GREEN:
    # corrected production must refuse, so this witness branch is not taken.
    if admission["admissible"] is True:
        assert admission["reason"] is None
        assert tokens == {storage_tiers.RAM_CAPACITY_KIND: 112}
    assert admission["admissible"] is False, (
        "configured (112 + max(22, 20, 5) + 16) / 2 = 75 GiB share "
        "exceeds node 0's 60 GiB; unsafe admission/token witness",
        admission, tokens)
    assert admission["reason"] == "ram_window_exceeds_node_memtotal_floor"
    totals = admission["node_memtotal_bytes"]
    assert isinstance(totals, dict)
    assert list(totals) == ["0", "1"]
    assert totals == {"0": 60 * gib, "1": 234 * gib}
    assert all(type(total) is int for total in totals.values())
    assert admission["memory_node_count"] == 2
    assert admission["node_required_numerator_bytes"] == 150 * gib
    assert type(admission["failing_memory_node"]) is int
    assert admission["failing_memory_node"] == 0
    assert tokens == {}


def test_balanced_nodes_admit_and_mint_the_same_configured_window(
        tmp_path: Path) -> None:
    gib = storage_tiers.GIB
    tier = _discover_ram_tier(
        tmp_path, node_memtotal_bytes=(147 * gib, 147 * gib))
    admission = tier["ram_admission"]
    assert isinstance(admission, dict)
    assert admission["admissible"] is True, admission
    assert admission["reason"] is None
    assert storage_tiers.tier_tokens(tier) == {
        storage_tiers.RAM_CAPACITY_KIND: 112}


def test_exact_node_share_equality_admits(tmp_path: Path) -> None:
    gib = storage_tiers.GIB
    tier = _discover_ram_tier(
        tmp_path, node_memtotal_bytes=(75 * gib, 219 * gib))
    admission = tier["ram_admission"]
    assert isinstance(admission, dict)
    assert admission["admissible"] is True, admission
    assert admission["node_required_numerator_bytes"] == 150 * gib
    assert storage_tiers.tier_tokens(tier) == {storage_tiers.RAM_CAPACITY_KIND: 112}


@pytest.mark.parametrize("small_node", [0, 1])
def test_one_kib_below_the_share_refuses_without_rounding(
        tmp_path: Path, small_node: int) -> None:
    gib = storage_tiers.GIB
    small = 75 * gib - 1024
    totals = (small, 294 * gib - small)
    if small_node == 1:
        totals = tuple(reversed(totals))
    tier = _discover_ram_tier(tmp_path, node_memtotal_bytes=totals)
    admission = tier["ram_admission"]
    assert isinstance(admission, dict)
    assert admission["admissible"] is False
    assert admission["reason"] == "ram_window_exceeds_node_memtotal_floor"
    assert admission["failing_memory_node"] == small_node
    assert admission["node_memtotal_bytes"] == {
        str(node): total for node, total in enumerate(totals)}
    assert storage_tiers.tier_tokens(tier) == {}


def test_one_memory_node_keeps_the_whole_share_and_needs_no_mpol(
        tmp_path: Path) -> None:
    gib = storage_tiers.GIB
    tier = _discover_ram_tier(tmp_path, node_memtotal_bytes=(294 * gib,))
    admission = tier["ram_admission"]
    assert isinstance(admission, dict)
    assert admission["admissible"] is True, admission
    assert admission["memory_node_count"] == 1
    assert admission["node_memtotal_bytes"] == {"0": 294 * gib}
    assert storage_tiers.tier_tokens(tier) == {storage_tiers.RAM_CAPACITY_KIND: 112}


def test_noncontiguous_cpu_less_memory_nodes_use_the_same_budget(
        tmp_path: Path) -> None:
    gib = storage_tiers.GIB
    tier = _discover_ram_tier(
        tmp_path, node_memtotal_bytes=(60 * gib, 234 * gib), node_ids=(0, 2))
    admission = tier["ram_admission"]
    assert isinstance(admission, dict)
    assert admission["admissible"] is False
    assert admission["reason"] == "ram_window_exceeds_node_memtotal_floor"
    assert admission["node_memtotal_bytes"] == {"0": 60 * gib, "2": 234 * gib}
    assert admission["failing_memory_node"] == 0
    assert storage_tiers.tier_tokens(tier) == {}


@pytest.mark.parametrize("meminfo", [
    None, "", "Node 0 MemTotal: 0 kB\n", "Node 0 MemTotal: -1 kB\n",
    "Node 0 MemTotal: invalid kB\n", "Node 2 MemTotal: 1 kB\n",
    "Node 0 MemTotal: 1 kB\nNode 0 MemTotal: 2 kB\n",
])
def test_bad_node_total_evidence_keeps_the_existing_topology_refusal(
        tmp_path: Path, meminfo: str | None) -> None:
    gib = storage_tiers.GIB
    tier = _discover_ram_tier(
        tmp_path, node_memtotal_bytes=(147 * gib, 147 * gib),
        node_meminfo_override={0: meminfo})
    admission = tier["ram_admission"]
    assert isinstance(admission, dict)
    assert admission["admissible"] is False
    assert admission["reason"] == "ram_numa_topology_unreadable"
    assert storage_tiers.tier_tokens(tier) == {}


def _admission_for_facts(
        facts: object, *, options: list[str] | None = None,
        rows: int | None = 88, window: int = 112, ceiling: int = 240,
        arc_c_max: int = 22, arc_floor: int = 20, arc_meta: int = 5,
        ) -> dict[str, object]:
    gib = storage_tiers.GIB
    return storage_tiers.ram_admission(
        ceiling_bytes=ceiling * gib,
        mount_options=(options if options is not None else
                       ["rw", "noswap", "mpol=interleave:0-1"]),
        policy={"ceiling_gib_max": 256, "window_gib_default": window,
                "arc_floor_gib": arc_floor, "system_reserve_gib": 16},
        mem_total=294 * gib,
        arc={"c_max": arc_c_max * gib, "size": 11 * gib,
             "arc_meta_used": arc_meta * gib},
        rows_held_gib=rows, memory_nodes=(0, 1),
        # Deliberately exercise invalid runtime facts through the real API.
        node_memtotal_bytes=facts)  # type: ignore[arg-type]


@pytest.mark.parametrize("facts", [
    None, {}, {0: 1}, {0: 1, 2: 1}, {"0": 1, "1": 1},
    {False: 1, 1: 1}, {0.0: 1, 1: 1}, {0: True, 1: 1},
    {0: 1.0, 1: 1}, {0: "1", 1: 1}, {0: 0, 1: 1}, {0: -1, 1: 1},
])
def test_direct_admission_requires_exact_positive_integer_node_facts(
        facts: object) -> None:
    admission = _admission_for_facts(facts)
    assert admission["admissible"] is False
    assert admission["reason"] == "ram_numa_memtotal_unreadable"
    assert admission["node_memtotal_bytes"] is None


def test_a_one_byte_node_deficit_cannot_be_rounded_down() -> None:
    gib = storage_tiers.GIB
    small = 75 * gib - 1
    admission = _admission_for_facts({0: small, 1: 294 * gib - small})
    assert admission["admissible"] is False
    assert admission["reason"] == "ram_window_exceeds_node_memtotal_floor"
    assert admission["failing_memory_node"] == 0


@pytest.mark.parametrize("c_max,floor,meta", [(32, 20, 5), (22, 32, 5), (22, 20, 32)])
def test_node_numerator_reuses_the_existing_maximum_arc_term(
        c_max: int, floor: int, meta: int) -> None:
    gib = storage_tiers.GIB
    admission = _admission_for_facts(
        {1: 147 * gib, 0: 147 * gib},
        arc_c_max=c_max, arc_floor=floor, arc_meta=meta)
    assert admission["admissible"] is True, admission
    assert admission["node_required_numerator_bytes"] == 160 * gib
    totals = admission["node_memtotal_bytes"]
    assert isinstance(totals, dict)
    assert list(totals) == ["0", "1"]


@pytest.mark.parametrize("options,rows,window,ceiling,reason", [
    (["rw", "mpol=interleave:0-1"], 88, 112, 240, "ram_mount_not_noswap"),
    (["rw", "noswap"], 88, 112, 300, "ram_ceiling_exceeds_policy_max"),
    (["rw", "noswap"], 88, 112, 240, "ram_mount_not_interleaved"),
    (["rw", "noswap"], None, 112, 240, "ram_rows_held_unknown"),
    (["rw", "noswap"], 96, 200, 240, "ram_window_exceeds_memtotal_floor"),
])
def test_existing_refusals_precede_missing_node_capacity_facts(
        options: list[str], rows: int | None, window: int, ceiling: int,
        reason: str) -> None:
    admission = _admission_for_facts(
        None, options=options, rows=rows, window=window, ceiling=ceiling)
    assert admission["admissible"] is False
    assert admission["reason"] == reason
