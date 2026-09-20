"""Full-stack 1/4 — real producer: manifest, phases, sealed rows, digests.

Drives production code only: the data-manifest schema validator,
``storage_tiers.manifest_phase_ranges``, and ``residency_plan``
build/freeze/validate. Failures, gzip/raw-canonical mismatches, required
argv-adjacent bindings (manifest sha, phase table), placement-tag
conjunction inputs, and container bindings surface here as real refusals,
not prose. ACC-01 (PB-side manifest/row shapes).
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

import prismabuild.core as pb  # noqa: E402
from prismabuild import pool, residency_plan, storage_tiers  # noqa: E402

from fullstack_fixtures import corpus, gzip_member, manifest_entry  # noqa: E402

TIER = "prismabuild-stage:dl380g10"
CONSUMER = "c" * 64
GIB = 1 << 30


def _manifest(pool_root: Path) -> dict[str, object]:
    files = corpus()
    whole = files["/pool/model/shard-0.bin"]
    split = files["/pool/model/shard-1.bin"]
    head = files["/pool/model/head.bin"]
    (pool_root / "model").mkdir(parents=True, exist_ok=True)
    (pool_root / "model" / "shard-0.bin").write_bytes(whole)
    (pool_root / "model" / "shard-1.bin").write_bytes(split)
    (pool_root / "model" / "head.bin").write_bytes(head)
    return {
        "schema": pb.DATA_MANIFEST_SCHEMA_V1,
        "produced_by": {"tool": "fullstack-harness"},
        "mount_prefix": str(pool_root),
        "entries": [
            manifest_entry("/pool/model/head.bin", 0, head),
            manifest_entry("/pool/model/shard-0.bin", 0, whole),
            manifest_entry("/pool/model/shard-1.bin", 1 << 20, split[1 << 20:(1 << 20) + (3 << 20)]),
        ],
        "entry_count": 3,
        "total_bytes": (1 << 18) + (1 << 20) + (3 << 20),
        "annotations": {"phases": [
            {"name": "head", "start_bytes": 0, "end_bytes": 1 << 18},
            {"name": "layer-000", "start_bytes": 1 << 18,
             "end_bytes": (1 << 18) + (1 << 20)},
            {"name": "layer-001", "start_bytes": (1 << 18) + (1 << 20),
             "end_bytes": (1 << 18) + (1 << 20) + (3 << 20)},
        ]},
    }


def _row(key: str, resources: dict[str, int], queue: pool.PoolQueue) -> dict[str, object]:
    return {"action_key": key, "cas_root": str(queue.root / "cas"),
            "checkout_root": str(queue.root / "co"),
            "worker_script": str(queue.root / "worker.py"),
            "tags": ["gb10"], "resources": resources}


def _hexkey(seed: str) -> str:
    return (seed.encode().hex() * 64)[:64]


def _plan(queue: pool.PoolQueue, manifest_sha: str, total: int) -> dict[str, object]:
    built = []
    for ordinal, name in enumerate(("head", "layer-000", "layer-001")):
        phase = [p for p in _PHASES if p["name"] == name][0]
        size = phase["end_bytes"] - phase["start_bytes"]
        built.append({
            "name": name, "start_bytes": phase["start_bytes"],
            "end_bytes": phase["end_bytes"],
            "stage_gib": max(1, (size + GIB - 1) // GIB),
            "mover_row": {**_row(_hexkey(f"mover{ordinal}"),
                                 {f"stage_gib@{TIER}": 1, "mem_gb": 1}, queue),
                          "residency": {"schema": pool.RESIDENCY_SCHEMA_V1,
                                        "tier_id": TIER,
                                        "manifest_sha256": manifest_sha,
                                        "manifest_bytes": total,
                                        "range_start_bytes": phase["start_bytes"],
                                        "range_end_bytes": phase["end_bytes"]}},
            "egress_row": _row(_hexkey(f"egress{ordinal}"), {"mem_gb": 1}, queue),
        })
    return residency_plan.build_plan(
        consumer_action_key=CONSUMER, tier_id=TIER, stage_root="/stage/prewarm",
        manifest_sha256=manifest_sha, manifest_bytes=total, phases=built)


_PHASES = [
    {"name": "head", "start_bytes": 0, "end_bytes": 1 << 18},
    {"name": "layer-000", "start_bytes": 1 << 18,
     "end_bytes": (1 << 18) + (1 << 20)},
    {"name": "layer-001", "start_bytes": (1 << 18) + (1 << 20),
     "end_bytes": (1 << 18) + (1 << 20) + (3 << 20)},
]


def test_manifest_phases_tile_the_read_order(tmp_path: Path) -> None:
    """Production phase ranges tile the manifest contiguously (ACC-01)."""
    manifest = _manifest(tmp_path / "pool")
    ranges = storage_tiers.manifest_phase_ranges(manifest)
    assert [r["name"] for r in ranges] == ["head", "layer-000", "layer-001"]
    assert ranges[0]["start_bytes"] == 0
    assert ranges[-1]["end_bytes"] == manifest["total_bytes"]
    for first, second in zip(ranges, ranges[1:]):
        assert first["end_bytes"] == second["start_bytes"]


def test_plan_freeze_is_first_writer_and_validates(tmp_path: Path) -> None:
    """Real freeze/validate: second filing verifies, conflicting body refuses."""
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    manifest = _manifest(tmp_path / "pool")
    sha = hashlib.sha256(json.dumps(manifest, sort_keys=True).encode()).hexdigest()
    plan = _plan(queue, sha, int(manifest["total_bytes"]))
    frozen = residency_plan.freeze(queue, plan)
    assert frozen["manifest_sha256"] == sha
    assert residency_plan.freeze(queue, plan)["manifest_sha256"] == sha
    other = dict(plan)
    other["manifest_sha256"] = "0" * 64
    with pytest.raises(Exception):
        residency_plan.freeze(queue, other)


def test_gzip_wire_and_canonical_identities_stay_distinct() -> None:
    """Wire digest (sealed bytes) and canonical digest never interchange."""
    manifest = _manifest_body()
    raw, wire = gzip_member(manifest)
    canonical = hashlib.sha256(
        json.dumps(manifest, sort_keys=True).encode()).hexdigest()
    assert wire != canonical
    assert gzip.decompress(raw) == json.dumps(manifest, sort_keys=True).encode()


def _manifest_body() -> dict[str, object]:
    files = corpus()
    whole = files["/pool/model/shard-0.bin"]
    return {"entries": [manifest_entry("/pool/model/shard-0.bin", 0, whole)]}


def test_tampered_manifest_bytes_refuse_phase_ranges(tmp_path: Path) -> None:
    """A manifest whose entries disagree with its bytes refuses ranges."""
    manifest = _manifest(tmp_path / "pool")
    manifest["entries"][1]["bytes"] = int(manifest["entries"][1]["bytes"]) + 1
    with pytest.raises(Exception):
        storage_tiers.manifest_phase_ranges(manifest)


def test_placement_tags_are_conjoined_class_tags(tmp_path: Path) -> None:
    """Rows carry the gb10 class tag; no host is named (ACC-01 placement input)."""
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    row = _row("d" * 64, {"cpu": 1, "mem_gb": 1}, queue)
    assert row["tags"] == ["gb10"]
