"""The two copies of pool discovery must not drift while both exist (#583, #582).

``storage_tiers`` re-implements three things ``prewarm_loop`` already does --
the ``zpool status -P`` member parse, the sysfs whole-disk resolution, and the
read-order phase table.  Two copies of one parse is the principle-8 failure
mode and it is flagged, not accepted: the intended end state is one module both
import.  Making that change means editing ``tools/fleet/prewarm_loop.py``, which
belongs to #582 and is being edited by somebody else right now, so this file
pins the duplication instead.  Both parsers see the same input and are asserted
to produce the same answer; a change to either that moves the answer fails here,
where the drift is cheap, rather than on a device where it is not.

Delete this file when ``prewarm_loop`` imports the helpers from
``storage_tiers``: at that point the property is structural.
"""

from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

from prismabuild import storage_tiers  # noqa: E402
import prewarm_loop  # noqa: E402

#: dl380g10's real topology, 2026-09-17: a four-spindle raidz1 with one cache
#: vdev on the root disk.  The cache vdev must not be paced as a data member.
ZPOOL_STATUS = """  pool: storage_pool
 state: ONLINE
config:

\tNAME                                     STATE     READ WRITE CKSUM
\tstorage_pool                             ONLINE       0     0     0
\t  raidz1-0                               ONLINE       0     0     0
\t    /dev/sdb1                            ONLINE       0     0     0
\t    /dev/sdc1                            ONLINE       0     0     0
\t    /dev/sdd1                            ONLINE       0     0     0
\t    /dev/sde1                            ONLINE       0     0     0
\tcache
\t  /dev/nvme1n1p5                         ONLINE       0     0     0

errors: No known data errors
"""


def _runner(_argv: list[str]) -> str:
    return ZPOOL_STATUS


def _sysfs(tmp_path: Path) -> str:
    """Just enough sysfs for the four spindles and the cache partition."""

    root = tmp_path / "block"
    major = 8
    for disk, partition in (("sdb", "sdb1"), ("sdc", "sdc1"), ("sdd", "sdd1"),
                            ("sde", "sde1"), ("nvme1n1", "nvme1n1p5")):
        (root / disk).mkdir(parents=True)
        (root / disk / "dev").write_text(f"{major}:0\n")
        (root / disk / partition).mkdir()
        (root / disk / partition / "dev").write_text(f"{major}:1\n")
        (root / partition).symlink_to(root / disk / partition)
        major += 1
    return str(root)


def test_both_copies_read_the_same_data_members(tmp_path: Path) -> None:
    sysfs = _sysfs(tmp_path)
    mine = storage_tiers.pool_member_devices(
        "storage_pool", runner=_runner, sysfs=sysfs)
    theirs = prewarm_loop.pool_member_devices(
        "storage_pool", runner=_runner, sysfs=sysfs)
    # The four raidz spindles, and not the cache vdev on the root disk.
    assert mine == ["sdb", "sdc", "sdd", "sde"]
    assert mine == theirs
    # The leaf paths behind them, which only the shared module exposes.
    assert storage_tiers.pool_member_paths("storage_pool", runner=_runner) == [
        "/dev/sdb1", "/dev/sdc1", "/dev/sdd1", "/dev/sde1"]


def test_both_copies_resolve_the_same_whole_disks(tmp_path: Path) -> None:
    sysfs = _sysfs(tmp_path)
    for path, expected in (("/dev/sdb1", "sdb"), ("/dev/nvme1n1p5", "nvme1n1"),
                           ("/dev/absent", None)):
        assert storage_tiers.whole_disk_of(path, sysfs=sysfs) == expected
        assert prewarm_loop.whole_disk_of(path, sysfs=sysfs) == expected


def _manifest(sizes: list[int], *, cut: bool = False) -> dict[str, object]:
    entries = [{"path": f"/mnt/shared/p{i}", "offset": 0, "bytes": size,
                "sha256": None} for i, size in enumerate(sizes)]
    phases = []
    running = 0
    for index, size in enumerate(sizes):
        running += size
        phases.append({"name": f"phase-{index}", "bytes": size,
                       "cumulative_bytes": running - (1 if cut else 0)})
    return {
        "schema": "prismaquant.prismabuild.data_manifest.v1",
        "produced_by": {}, "annotations": {"phases": phases},
        "mount_prefix": "/mnt/shared", "entries": entries,
        "entry_count": len(entries), "total_bytes": sum(sizes),
    }


def test_both_copies_read_the_same_phase_boundaries() -> None:
    manifest = _manifest([4096, 8192, 1024])
    theirs = prewarm_loop.manifest_phases(manifest)
    mine = storage_tiers.manifest_phase_ranges(manifest)
    assert [phase["cumulative_bytes"] for phase in theirs] == [
        entry["end_bytes"] for entry in mine]
    assert [phase["name"] for phase in theirs] == [entry["name"] for entry in mine]
    # A table that cuts an entry is refused by both, identically.
    cut = _manifest([4096, 8192], cut=True)
    assert prewarm_loop.manifest_phases(cut) == []
    assert storage_tiers.manifest_phase_ranges(cut) == []
