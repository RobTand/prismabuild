"""The storage role follows a sealed read timeline, including later revisits."""
from __future__ import annotations

from copy import deepcopy
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from prewarm_fixture import Fleet  # noqa: E402
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import prismabuild.core as pb  # noqa: E402


def _plan() -> dict:
    return {"phases": [
        {"name": "forward-0", "entry_indices": [0],
         "bytes": 4, "cumulative_bytes": 4},
        {"name": "forward-1", "entry_indices": [1],
         "bytes": 4, "cumulative_bytes": 8},
        {"name": "compute", "entry_indices": [],
         "bytes": 0, "cumulative_bytes": 8},
        {"name": "reverse-1", "entry_indices": [1],
         "bytes": 4, "cumulative_bytes": 12},
        {"name": "reverse-0", "entry_indices": [0],
         "bytes": 4, "cumulative_bytes": 16},
    ], "read_bytes": 16}


def _manifest(plan: dict | None = None) -> dict:
    return {"schema": pb.DATA_MANIFEST_SCHEMA_V2, "produced_by": {},
            "annotations": {}, "mount_prefix": "/mnt/shared",
            "entries": [
                {"path": f"/mnt/shared/{index}", "offset": 0,
                 "bytes": 4, "sha256": None} for index in range(2)],
            "entry_count": 2, "total_bytes": 8,
            "read_plan": plan or _plan()}


def test_unique_registry_and_revisit_timeline_are_distinct() -> None:
    manifest = pb.validate_data_manifest(_manifest())
    assert manifest["entry_count"] == 2
    assert manifest["total_bytes"] == 8
    assert manifest["read_plan"]["read_bytes"] == 16


@pytest.mark.parametrize("change", [
    lambda p: p["phases"][3].update(entry_indices=[2]),
    lambda p: p["phases"][3].update(entry_indices=[1, 1]),
    lambda p: p["phases"][3].update(entry_indices=[True]),
    lambda p: p["phases"][3].update(bytes=3),
    lambda p: p["phases"][3].update(cumulative_bytes=11),
    lambda p: p.update(read_bytes=8),
    lambda p: p["phases"][3].update(name="forward-0"),
    lambda p: p["phases"][1].update(entry_indices=[]),
    lambda p: p["phases"][3].update(name=" reverse-1"),
])
def test_malformed_or_ambiguous_read_plan_fails_closed(change) -> None:
    plan = deepcopy(_plan())
    change(plan)
    with pytest.raises(pb.ActionContractError):
        pb.validate_data_manifest(_manifest(plan))


def test_v2_refuses_ambiguous_v1_phase_annotations() -> None:
    manifest = _manifest()
    manifest["annotations"]["phases"] = []
    with pytest.raises(pb.ActionContractError):
        pb.validate_data_manifest(manifest)


def test_revisit_advances_at_claimed_progress_without_warming_duplicate_registry(
        tmp_path: Path) -> None:
    fleet = Fleet(tmp_path)
    key = fleet.action(
        "revisit", [fleet.file("a.pt", 4), fleet.file("b.pt", 4)],
        read_plan=_plan(),
        progress_phases=[phase["name"] for phase in _plan()["phases"]])
    args = fleet.args(arcstats=fleet.arcstats(size=0, c=8, c_max=8))
    fleet.cycle(args)
    first = fleet.queue.prewarm(key)
    assert first["manifest_bytes"] == 16
    assert first["warmed_bytes"] == 8
    fleet.claim(key)
    fleet.report_progress(key, "compute", units=900)
    fleet.cycle(args)
    second = fleet.queue.prewarm(key)
    assert second["window_start_bytes"] == 8
    assert second["warmed_bytes"] == 16
    assert second["bytes_warmed"] == 8
    assert second["status"] == "complete"
    assert second["manifest_sha256"] == first["manifest_sha256"]


def test_claimed_first_and_failed_revisit_keep_linear_frontier(tmp_path: Path) -> None:
    fleet = Fleet(tmp_path)
    key = fleet.action(
        "claimed-first", [fleet.file("a.pt", 4), fleet.file("b.pt", 4)],
        read_plan=_plan(),
        progress_phases=[phase["name"] for phase in _plan()["phases"]])
    args = fleet.args(arcstats=fleet.arcstats(size=0, c=8, c_max=8))
    fleet.claim(key)
    fleet.report_progress(key, "compute")
    fleet.cycle(args)
    first = fleet.queue.prewarm(key)
    assert first["window_start_bytes"] == 8
    assert first["warmed_bytes"] == 16
    # A failed reference cannot advance over a successful later read.
    other = Fleet(tmp_path / "other")
    absent = str(other.mount / "absent")
    second_key = other.action("failed", [(absent, 4), other.file("ok", 4)],
        read_plan={"phases": [{"name": "read", "entry_indices": [0, 1],
                               "bytes": 8, "cumulative_bytes": 8}],
                   "read_bytes": 8}, progress_phases=["read"])
    other.cycle(other.args(arcstats=other.arcstats(size=0, c=8, c_max=8)))
    record = other.queue.prewarm(second_key)
    assert record["warmed_bytes"] == 0
    assert record["status"] == "partial"


def test_read_window_can_stop_inside_one_phase_without_releasing_its_bytes(
        tmp_path: Path) -> None:
    fleet = Fleet(tmp_path)
    plan = {"phases": [
        {"name": "read", "entry_indices": [0, 1, 2],
         "bytes": 12, "cumulative_bytes": 12},
        {"name": "work", "entry_indices": [],
         "bytes": 0, "cumulative_bytes": 12},
    ], "read_bytes": 12}
    key = fleet.action("oversized", [fleet.file(f"{i}.pt", 4) for i in range(3)],
                       read_plan=plan, progress_phases=["read", "work"])
    args = fleet.args(arcstats=fleet.arcstats(size=0, c=8, c_max=8))
    fleet.cycle(args)
    assert fleet.queue.prewarm(key)["warmed_bytes"] == 8
    fleet.claim(key)
    fleet.report_progress(key, "read", units=1_000_000)
    event = fleet.cycle(args)
    assert event["claimed_reserved_bytes"] == 8
    assert fleet.queue.prewarm(key)["warmed_bytes"] == 8
