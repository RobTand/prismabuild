"""Multi-memory-node RAM admission must require interleaved tmpfs placement (#1032).

Exercise discovery and token minting, not a replacement admission function.
The Path redirect supplies fixture-owned NUMA evidence until production offers
an explicit node-root injection; every other module Path access is unchanged.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import prismabuild.storage_tiers as storage_tiers  # noqa: E402

GIB = storage_tiers.GIB
HOST = "dl380g10"
NUMA_ROOT = Path("/sys/devices/system/node")


def _no_zfs(argv: list[str]) -> str:
    raise OSError(f"no {argv[0]} on this box")


def _ram_tier(tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
              options: str, *, node_ids: tuple[int, ...] = (0, 1),
              membership: str | None = "0-1",
              meminfo_override: str | None = None,
              rows_held_gib: int | None = 0,
              statvfs_unreadable: bool = False,
              epoch_unwritable: bool = False) -> dict[str, object]:
    nodes = tmp_path / "sys" / "devices" / "system" / "node"
    nodes.mkdir(parents=True)
    if membership is not None:
        (nodes / "has_memory").write_text(membership + "\n")
    (nodes / "online").write_text(
        ",".join(str(node) for node in node_ids) + "\n")
    for node in node_ids:
        directory = nodes / f"node{node}"
        directory.mkdir()
        (directory / "cpulist").write_text("\n" if node == 2 else "0-19\n")
        # Actual sysfs spelling, not the whole-box /proc/meminfo spelling.
        # Default two-node fixture: 147 GiB each, matching the 294 GiB box.
        (directory / "meminfo").write_text(
            meminfo_override if meminfo_override is not None else
            f"Node {node} MemTotal:  {(294 * GIB // len(node_ids)) // 1024} kB\n")

    def fixture_path(*parts: str | os.PathLike[str]) -> Path:
        path = Path(*parts)
        if path == NUMA_ROOT or NUMA_ROOT in path.parents:
            return nodes / path.relative_to(NUMA_ROOT)
        return path

    # Patch only this module's constructor, not pathlib or any admission
    # function. Old code receives only keywords it already accepts.
    monkeypatch.setattr(storage_tiers, "Path", fixture_path)

    mount = tmp_path / "ram"
    mount.mkdir()
    if epoch_unwritable:
        # A directory at the marker path cannot be replaced by an epoch file.
        (mount / storage_tiers.RAM_EPOCH_MARKER).mkdir()
    proc = tmp_path / "proc"
    proc.mkdir()
    (proc / "mounts").write_text(f"tmpfs {mount} tmpfs {options} 0 0\n")
    (proc / "meminfo").write_text(f"MemTotal:  {(294 * GIB) // 1024} kB\n")
    (proc / "arcstats").write_text(
        f"c_max 4 {22 * GIB}\nsize 4 {11 * GIB}\narc_meta_used 4 {5 * GIB}\n")

    def statvfs(path: str) -> os.statvfs_result:
        if statvfs_unreadable or path != str(mount):
            raise OSError(f"no tmpfs at {path}")
        frsize = 4096
        block = GIB // frsize
        return os.statvfs_result(
            (frsize, frsize, 256 * block, 200 * block,
             200 * block, 1_000_000, 900_000, 900_000, 0, 255))

    tiers = storage_tiers.discover_tiers(
        host=HOST, runner=_no_zfs, arcstats_path=str(proc / "arcstats"),
        proc_pressure=str(proc / "pressure" / "io"),
        ram_policy={
            "schema": storage_tiers.RAM_TIER_POLICY_SCHEMA_V1,
            "mountpoint": str(mount), "ceiling_gib_max": 256,
            "window_gib_default": 112, "arc_floor_gib": 20,
            "system_reserve_gib": 16, "prefill_depth": None,
        },
        statvfs=statvfs, proc_mounts=str(proc / "mounts"),
        meminfo_path=str(proc / "meminfo"), rows_held_gib=rows_held_gib)
    return tiers[storage_tiers.tier_id("ram", HOST)]


def test_a_two_memory_node_mount_without_mpol_refuses_the_warm_path(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    tier = _ram_tier(tmp_path, monkeypatch, options="rw,noswap,size=256G")

    assert tier["mount_options"] == ["rw", "noswap", "size=256G"]
    admission = tier["ram_admission"]
    assert isinstance(admission, dict)
    assert admission["admissible"] is False
    assert admission["reason"] == "ram_mount_not_interleaved"
    assert storage_tiers.tier_tokens(tier) == {}


def test_a_two_memory_node_interleaved_mount_admits_and_mints_its_window(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    tier = _ram_tier(
        tmp_path, monkeypatch, options="rw,noswap,size=256G,mpol=interleave:0-1")

    assert tier["mount_options"] == [
        "rw", "noswap", "size=256G", "mpol=interleave:0-1"]
    admission = tier["ram_admission"]
    assert isinstance(admission, dict)
    assert admission["admissible"] is True
    assert admission["reason"] is None
    assert storage_tiers.tier_tokens(tier) == {
        storage_tiers.RAM_CAPACITY_KIND: 112}


@pytest.mark.parametrize("policy", [
    "mpol=interleave:0", "mpol=interleave:0-2", "mpol=interleave:1-0",
    "mpol=interleave:", "mpol=interleave:0,,1", "mpol=interleave:0-1,2",
    "mpol=interleave:0-1,bogus", "mpol=interleave:0-1,noswap,2",
    "mpol=interleave:0-999999999",
    "mpol=interleave:0-4096", "mpol=interleave:0-1,1",
    "mpol=bind:0-1", "mpol=prefer:0", "mpol=default",
    "mpol=interleave:0-1,mpol=bind:0", "mpol=interleave,mpol=interleave",
])
def test_invalid_or_partial_placement_never_mints_from_a_valid_prefix(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, policy: str) -> None:
    tier = _ram_tier(tmp_path, monkeypatch, "rw,noswap,size=256G," + policy)
    admission = tier["ram_admission"]
    assert isinstance(admission, dict)
    assert admission["reason"] == "ram_mount_not_interleaved"
    assert admission["admissible"] is False
    assert storage_tiers.tier_tokens(tier) == {}


@pytest.mark.parametrize("policy", ["mpol=interleave", "mpol=interleave:0,2,noswap"])
def test_noncontiguous_memory_nodes_include_nodes_without_online_cpus(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, policy: str) -> None:
    # Node 2 has an empty cpulist but positive memory and must participate.
    tier = _ram_tier(tmp_path, monkeypatch, "rw,noswap,size=256G," + policy,
                     node_ids=(0, 2), membership="0,2")
    assert tier["memory_nodes"] == [0, 2]
    admission = tier["ram_admission"]
    assert isinstance(admission, dict)
    assert admission["admissible"] is True
    assert storage_tiers.tier_tokens(tier) == {storage_tiers.RAM_CAPACITY_KIND: 112}


@pytest.mark.parametrize("membership", [None, "", "0,", "1-0", "0-4096", "0-2"])
def test_unknown_or_incomplete_memory_membership_is_not_a_single_node(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
        membership: str | None) -> None:
    tier = _ram_tier(tmp_path, monkeypatch,
                     "rw,noswap,size=256G,mpol=interleave", membership=membership)
    admission = tier["ram_admission"]
    assert isinstance(admission, dict)
    assert admission["reason"] == "ram_numa_topology_unreadable"
    assert tier["memory_nodes"] is None
    assert storage_tiers.tier_tokens(tier) == {}


@pytest.mark.parametrize("meminfo", [
    "MemTotal: 154140672 kB\n", "Node 0 MemTotal: 0 kB\n",
    "Node 0 MemTotal: invalid kB\n", "",
])
def test_membership_requires_positive_sysfs_memory_evidence_for_every_node(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, meminfo: str) -> None:
    tier = _ram_tier(tmp_path, monkeypatch, "rw,noswap,size=256G",
                     meminfo_override=meminfo)
    admission = tier["ram_admission"]
    assert isinstance(admission, dict)
    assert admission["reason"] == "ram_numa_topology_unreadable"
    assert storage_tiers.tier_tokens(tier) == {}


def test_known_single_memory_node_does_not_require_mpol(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    tier = _ram_tier(tmp_path, monkeypatch, "rw,noswap,size=256G",
                     node_ids=(2,), membership="2")
    assert tier["memory_nodes"] == [2]
    assert storage_tiers.tier_tokens(tier) == {storage_tiers.RAM_CAPACITY_KIND: 112}


@pytest.mark.parametrize("options,rows,stat_error,epoch_error,reason", [
    ("rw,size=256G", 0, False, False, "ram_mount_not_noswap"),
    ("rw,noswap,size=256G", None, False, False, "ram_rows_held_unknown"),
    ("rw,noswap,size=256G", 0, True, False, "ram_statvfs_unreadable"),
    ("rw,noswap,size=256G", 0, False, True, "ram_epoch_unwritable"),
])
def test_existing_refusals_and_error_overrides_precede_unknown_topology(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, options: str,
        rows: int | None, stat_error: bool, epoch_error: bool, reason: str) -> None:
    tier = _ram_tier(tmp_path, monkeypatch, options, membership=None,
                     rows_held_gib=rows, statvfs_unreadable=stat_error,
                     epoch_unwritable=epoch_error)
    admission = tier["ram_admission"]
    assert isinstance(admission, dict)
    assert admission["reason"] == reason
    assert storage_tiers.tier_tokens(tier) == {}


def test_membership_changing_during_discovery_refuses(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    read_text = Path.read_text
    reads = 0

    def changing_membership(path: Path, *args, **kwargs) -> str:
        nonlocal reads
        if path.name == "has_memory":
            reads += 1
            return "0-1\n" if reads == 1 else "0\n"
        return read_text(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", changing_membership)
    tier = _ram_tier(tmp_path, monkeypatch, "rw,noswap,size=256G,mpol=interleave")
    admission = tier["ram_admission"]
    assert isinstance(admission, dict)
    assert admission["reason"] == "ram_numa_topology_unreadable"
    assert storage_tiers.tier_tokens(tier) == {}


def test_placement_refusal_preserves_the_mounted_epoch(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    tier = _ram_tier(tmp_path, monkeypatch, "rw,noswap,size=256G")
    mount = tmp_path / "ram"
    marker = storage_tiers.read_ram_epoch(mount)
    assert marker is not None
    again = storage_tiers.ram_tier(
        {"mountpoint": str(mount), "ceiling_gib_max": 256,
         "window_gib_default": 112, "arc_floor_gib": 20,
         "system_reserve_gib": 16},
        host=HOST, statvfs=lambda path: os.statvfs_result(
            (GIB, GIB, 256, 200, 200, 1, 1, 1, 0, 255)),
        proc_mounts=str(tmp_path / "proc" / "mounts"),
        meminfo_path=str(tmp_path / "proc" / "meminfo"),
        arcstats_path=str(tmp_path / "proc" / "arcstats"), rows_held_gib=0)
    assert again is not None
    assert again["epoch"] == tier["epoch"] == marker["epoch"]
    assert storage_tiers.tier_tokens(again) == {}
