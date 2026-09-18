"""Storage tiers are discovered from the box, never configured (#583, #582).

Every number a tier record carries comes from ``zpool``, ``arcstats`` or a
receipt, so each test drives the discovery with a fake runner and fake
files and checks that the record says what the box said -- and that a box
which says nothing offers nothing.
"""

from __future__ import annotations

import os
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from prismabuild import storage_tiers as st  # noqa: E402

STATUS_WITH_AUX = """  pool: storage_pool
 state: ONLINE
config:

\tNAME                                            STATE     READ WRITE CKSUM
\tstorage_pool                                    ONLINE       0     0     0
\t  raidz1-0                                      ONLINE       0     0     0
\t    /dev/sdb1                                   ONLINE       0     0     0
\t    /dev/sdc1                                   ONLINE       0     0     0
\tcache
\t  /dev/nvme1n1p1                                ONLINE       0     0     0
\tlogs
\t  /dev/nvme1n1p2                                ONLINE       0     0     0

errors: No known data errors
"""

STAGE_STATUS = """  pool: prismabuild-stage
 state: ONLINE
config:

\tNAME                                            STATE     READ WRITE CKSUM
\tprismabuild-stage                               ONLINE       0     0     0
\t  /dev/nvme0n1p1                                ONLINE       0     0     0

errors: No known data errors
"""


def _runner(*, pools: str, statuses: dict[str, str], mountpoints: dict[str, str] | None = None):
    calls: list[list[str]] = []

    def run(argv: list[str]) -> str:
        calls.append(argv)
        if argv[1:3] == ["list", "-Hp"]:
            return pools
        if argv[1:3] == ["status", "-P"]:
            if argv[3] not in statuses:
                raise OSError(f"no such pool {argv[3]}")
            return statuses[argv[3]]
        if argv[1] == "get" and "mountpoint" in argv:
            return (mountpoints or {}).get(argv[-1], "none") + "\n"
        raise AssertionError(f"unexpected zpool call {argv}")

    run.calls = calls  # type: ignore[attr-defined]
    return run


def _sysfs(tmp_path: Path, disks: dict[str, list[str]]) -> str:
    """``/sys/class/block`` with each partition a child of its disk, as the kernel lays it out."""

    devices = tmp_path / "devices"
    cls = tmp_path / "class" / "block"
    cls.mkdir(parents=True)
    for disk, parts in disks.items():
        node = devices / disk
        node.mkdir(parents=True)
        (node / "dev").write_text("8:16\n")
        os.symlink(node, cls / disk)
        for part in parts:
            child = node / part
            child.mkdir()
            (child / "dev").write_text("8:17\n")
            os.symlink(child, cls / part)
    return str(cls)


def test_data_members_only_never_cache_or_log_devices() -> None:
    run = _runner(pools="", statuses={"storage_pool": STATUS_WITH_AUX})
    assert st.pool_member_paths("storage_pool", runner=run) == ["/dev/sdb1", "/dev/sdc1"]


def test_unreadable_pool_is_none_and_no_leaf_is_empty() -> None:
    run = _runner(pools="", statuses={})
    assert st.pool_member_paths("gone", runner=run) is None
    empty = _runner(pools="", statuses={"p": "config:\n\tNAME\n\tp ONLINE\nerrors: none\n"})
    assert st.pool_member_paths("p", runner=empty) == []


def test_whole_disk_resolves_partitions_through_sysfs(tmp_path: Path) -> None:
    sysfs = _sysfs(tmp_path, {"sdb": ["sdb1"], "nvme0n1": ["nvme0n1p1"]})
    assert st.whole_disk_of("/dev/sdb1", sysfs=sysfs) == "sdb"
    assert st.whole_disk_of("/dev/nvme0n1p1", sysfs=sysfs) == "nvme0n1"
    assert st.whole_disk_of("/dev/nvme0n1", sysfs=sysfs) == "nvme0n1"
    assert st.whole_disk_of("/dev/sdz9", sysfs=sysfs) is None


def test_member_devices_fail_closed_on_one_unresolvable_member(tmp_path: Path) -> None:
    run = _runner(pools="", statuses={"storage_pool": STATUS_WITH_AUX})
    both = _sysfs(tmp_path / "both", {"sdb": ["sdb1"], "sdc": ["sdc1"]})
    assert st.pool_member_devices("storage_pool", runner=run, sysfs=both) == ["sdb", "sdc"]
    one = _sysfs(tmp_path / "one", {"sdb": ["sdb1"]})
    assert st.pool_member_devices("storage_pool", runner=run, sysfs=one) == []


def test_stage_pools_are_selected_by_name_prefix_with_exact_bytes() -> None:
    listing = (
        "storage_pool\t16003182592000\t9000000000000\t7003182592000\tONLINE\n"
        "prismabuild-stage\t800166076416\t1048576\t800165027840\tONLINE\n"
        "rpool\t500000000000\t1\t499999999999\tDEGRADED\n"
    )
    run = _runner(pools=listing, statuses={})
    pools = st.stage_pools(runner=run)
    assert pools == [{
        "name": "prismabuild-stage", "size_bytes": 800166076416,
        "allocated_bytes": 1048576, "free_bytes": 800165027840, "health": "ONLINE",
    }]
    assert st.stage_pools(runner=_runner(pools="", statuses={})) == []
    broken = _runner(pools="", statuses={})

    def failing(argv: list[str]) -> str:
        raise OSError("zpool missing")

    assert st.stage_pools(runner=failing) is None
    del broken


def test_by_id_prefers_the_model_serial_link_over_the_nvme_uuid_twin(tmp_path: Path) -> None:
    dev = tmp_path / "dev"
    dev.mkdir()
    disk = dev / "nvme0n1"
    disk.write_bytes(b"")
    by_id = dev / "by-id"
    by_id.mkdir()
    os.symlink(disk, by_id / "nvme-nvme.8086-435644353437313030323638303042474e-4c5430383030-00000001")
    os.symlink(disk, by_id / "nvme-LT0800KEXVA_CVMD54710026800BGN")
    os.symlink(disk, by_id / "nvme-LT0800KEXVA_CVMD54710026800BGN_1")
    names = st.by_id_names([str(disk), str(dev / "absent")], by_id=str(by_id))
    assert names == {str(disk): "nvme-LT0800KEXVA_CVMD54710026800BGN", str(dev / "absent"): None}


def test_arc_tier_is_c_max_less_metadata_and_absent_without_c_max() -> None:
    assert st.arc_tier({}) is None
    assert st.arc_tier({"c_max": 0}) is None
    tier = st.arc_tier({"c_max": 257698037760, "arc_meta_used": 5423149896, "size": 7, "c": 9})
    assert tier == {
        "tier": "arc", "capacity_bytes": 257698037760 - 5423149896,
        "arc_c_max": 257698037760, "arc_meta_used": 5423149896, "arc_size": 7, "arc_c": 9,
    }
    assert st.arc_tier({"c_max": 10, "arc_meta_used": 12})["capacity_bytes"] == 0


def test_read_arcstats_parses_the_kstat_layout(tmp_path: Path) -> None:
    path = tmp_path / "arcstats"
    path.write_text(
        "12 1 0x01 123 5904 1234 5678\n"
        "name                            type data\n"
        "c_max                           4    257698037760\n"
        "arc_meta_used                   4    5423149896\n"
        "bad_line                        4    notanumber\n"
    )
    assert st.read_arcstats(str(path)) == {"c_max": 257698037760, "arc_meta_used": 5423149896}
    assert st.read_arcstats(str(tmp_path / "missing")) == {}


def test_demand_keys_split_host_kinds_from_tier_kinds() -> None:
    host, tiers = st.split_demand({
        "cpu": 1, "stage_gib@prismabuild-stage:dl380g10": 34,
        "fill_mb_s_pool_side@prismabuild-stage:dl380g10": 500, "arc_gib@arc:dl380g10": 11,
    })
    assert host == {"cpu": 1}
    assert tiers == {
        "prismabuild-stage:dl380g10": {"stage_gib": 34, "fill_mb_s_pool_side": 500},
        "arc:dl380g10": {"arc_gib": 11},
    }
    assert st.split_demand_key("gpu") == ("gpu", None)
    for bad in ("@tier", "kind@", "kind@a@b"):
        with pytest.raises(ValueError):
            st.split_demand_key(bad)
    with pytest.raises(ValueError):
        st.tier_id("stage", "host@x", "prismabuild-stage")
    assert st.tier_id("stage", "dl380g10", "prismabuild-stage") == "prismabuild-stage:dl380g10"
    assert st.tier_id("arc", "dl380g10") == "arc:dl380g10"


def test_fill_rate_is_the_pool_side_attribution_never_the_file_side_rate() -> None:
    hot_arc = {"mb_per_s": 1141.4, "bytes_warmed": 206093512356,
               "disk_pacing": {"mean_util_pct": 9.1}}
    attributed = {"mb_per_s": 298.8, "bytes_warmed": 4564950688752,
                  "disk_pacing": {"mean_self_read_mb_s": 242.5}}
    better = {"mb_per_s": 100.0, "disk_pacing": {"mean_self_read_mb_s": 410.0}}
    assert st.fill_rate_from_records([hot_arc]) is None
    assert st.fill_rate_from_records([hot_arc, attributed]) == 242.5
    assert st.fill_rate_from_records([attributed, better, {"disk_pacing": {"mean_self_read_mb_s": 0}}]) == 410.0
    assert st.fill_rate_from_records([{"disk_pacing": {"mean_self_read_mb_s": True}}]) is None


def test_tier_tokens_mint_whole_gib_and_whole_mb_s() -> None:
    assert st.tier_tokens({"tier": "stage", "capacity_bytes": 800166076416, "fill_mb_s_pool_side": 242.5}) == {
        "stage_gib": 745, "fill_mb_s_pool_side": 242,
    }
    assert st.tier_tokens({"tier": "arc", "capacity_bytes": 252274887864}) == {"arc_gib": 234}
    assert st.tier_tokens({"tier": "arc", "capacity_bytes": 0, "fill_mb_s_pool_side": None}) == {}
    assert st.tier_tokens({"tier": "pool", "capacity_bytes": 5}) == {}


def test_discover_offers_nothing_on_a_box_without_zfs(tmp_path: Path) -> None:
    def failing(argv: list[str]) -> str:
        raise OSError("no zpool")

    assert st.discover_tiers(
        host="sparky", runner=failing, arcstats_path=str(tmp_path / "absent"),
        sysfs=str(tmp_path), by_id=str(tmp_path),
    ) == {}


def test_discover_reads_every_tier_from_the_box(tmp_path: Path) -> None:
    listing = "prismabuild-stage\t800166076416\t1048576\t800165027840\tONLINE\n"
    run = _runner(
        pools=listing,
        statuses={"storage_pool": STATUS_WITH_AUX, "prismabuild-stage": STAGE_STATUS},
        mountpoints={"prismabuild-stage": "/storage_pool/prismabuild-stage"},
    )
    sysfs = _sysfs(tmp_path, {"sdb": ["sdb1"], "sdc": ["sdc1"], "nvme0n1": ["nvme0n1p1"]})
    dev = tmp_path / "dev"
    dev.mkdir()
    by_id = dev / "by-id"
    by_id.mkdir()
    for name, link in (("sdb1", "scsi-35000cca2a1f1042d-part1"),
                       ("sdc1", "scsi-35000cca2a1efba56-part1"),
                       ("nvme0n1p1", "nvme-LT0800KEXVA_CVMD54710026800BGN-part1")):
        (dev / name).write_bytes(b"")
        os.symlink(dev / name, by_id / link)
    arcstats = tmp_path / "arcstats"
    arcstats.write_text("c_max 4 257698037760\narc_meta_used 4 5423149896\nsize 4 1\nc 4 2\n")
    receipts = [{"disk_pacing": {"mean_self_read_mb_s": 242.5}}]

    # The discovery resolves ``/dev/sdb1`` through the fake ``/dev``; point the
    # parsed leaves at it by rewriting the status text to the fake paths.
    statuses = {
        "storage_pool": STATUS_WITH_AUX.replace("/dev/", f"{dev}/"),
        "prismabuild-stage": STAGE_STATUS.replace("/dev/", f"{dev}/"),
    }
    run = _runner(pools=listing, statuses=statuses,
                  mountpoints={"prismabuild-stage": "/storage_pool/prismabuild-stage"})
    tiers = st.discover_tiers(
        host="dl380g10", runner=run, arcstats_path=str(arcstats), sysfs=sysfs,
        by_id=str(by_id), source_pool="storage_pool", fill_records=receipts, now=1.0,
    )
    assert sorted(tiers) == ["arc:dl380g10", "prismabuild-stage:dl380g10"]
    stage = tiers["prismabuild-stage:dl380g10"]
    assert stage["tier"] == "stage"
    assert stage["capacity_bytes"] == 800166076416
    assert stage["free_bytes"] == 800165027840
    assert stage["mountpoint"] == "/storage_pool/prismabuild-stage"
    assert stage["members_by_id"] == ["nvme-LT0800KEXVA_CVMD54710026800BGN-part1"]
    assert stage["members_unresolved"] == []
    assert stage["source_members"] == ["sdb", "sdc"]
    assert stage["source_members_by_id"] == [
        "scsi-35000cca2a1efba56-part1", "scsi-35000cca2a1f1042d-part1"]
    assert stage["fill_mb_s_pool_side"] == 242.5
    assert stage["sampled_unix"] == 1.0
    arc = tiers["arc:dl380g10"]
    assert arc["tier"] == "arc"
    assert arc["capacity_bytes"] == 257698037760 - 5423149896
    assert arc["source_members"] == ["sdb", "sdc"]
    assert st.tier_tokens(stage) == {"stage_gib": 745, "fill_mb_s_pool_side": 242}
    assert st.tier_tokens(arc) == {"arc_gib": 234, "fill_mb_s_pool_side": 242}
