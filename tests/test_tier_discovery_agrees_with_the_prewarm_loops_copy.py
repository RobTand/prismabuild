"""Pin phase compatibility until its read-order owner is shared."""

from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

from prismabuild import storage_tiers  # noqa: E402
import prewarm_loop  # noqa: E402

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
