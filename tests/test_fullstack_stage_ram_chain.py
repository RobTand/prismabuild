"""Full-stack 2/4 — HDD fixture to SSD stage to RAM, whole and split ranges.

Drives real production functions: ``stage_move.move`` (HDD pool fixture to
SSD stage, whole file at offset 0 AND a nonzero-offset split staged as its
own `.pbrange` object), ``ram_promote.promote`` (stage to tmpfs with epoch),
``queue.record_move`` receipt filing, ``residency_map`` fragment
validate/compose/lookup/`overlay_ram`. Asserts staged bytes equal source
bytes through digests — never by trusting the mover's word. ACC-02/ACC-03
(PB-side chain legs).
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

import prismabuild.core as pb  # noqa: E402
from prismabuild import pool, residency_map, storage_tiers  # noqa: E402
import ram_promote  # noqa: E402
import stage_move  # noqa: E402

from fullstack_fixtures import corpus  # noqa: E402

CONSUMER = "c" * 64
WHOLE_MOVER = "a" * 64
SPLIT_MOVER = "b" * 64
WHOLE_RAM_MOVER = "d" * 64
SPLIT_RAM_MOVER = "e" * 64
STAGE_TIER = "prismabuild-stage:dl380g10"
RAM_TIER = "ram:dl380g10"

WHOLE = "shard-0.bin"
SPLIT = "shard-1.bin"
SPLIT_OFFSET = 1 << 20
SPLIT_BYTES = 3 << 20


def _pool_fixture(tmp_path: Path) -> tuple[Path, dict[str, object], str]:
    """HDD-source corpus plus the manifest naming whole and split entries."""
    files = corpus()
    mount = tmp_path / "mnt"
    (mount / "model").mkdir(parents=True, exist_ok=True)
    (mount / "model" / WHOLE).write_bytes(files["/pool/model/shard-0.bin"])
    (mount / "model" / SPLIT).write_bytes(files["/pool/model/shard-1.bin"])
    whole_path = str(mount / "model" / WHOLE)
    split_path = str(mount / "model" / SPLIT)
    entries = [
        {"path": whole_path, "offset": 0,
         "bytes": len(files["/pool/model/shard-0.bin"]),
         "sha256": hashlib.sha256(files["/pool/model/shard-0.bin"]).hexdigest()},
        {"path": split_path, "offset": SPLIT_OFFSET,
         "bytes": SPLIT_BYTES,
         "sha256": hashlib.sha256(
             files["/pool/model/shard-1.bin"][SPLIT_OFFSET:SPLIT_OFFSET + SPLIT_BYTES]).hexdigest()},
    ]
    manifest = {
        "schema": pb.DATA_MANIFEST_SCHEMA_V1,
        "produced_by": {"tool": "fullstack-harness"},
        "mount_prefix": str(mount),
        "entries": entries,
        "entry_count": 2,
        "total_bytes": sum(int(e["bytes"]) for e in entries),
        "annotations": {"phases": [
            {"name": "whole",
             "cumulative_bytes": len(files["/pool/model/shard-0.bin"])},
            {"name": "split",
             "cumulative_bytes": len(files["/pool/model/shard-0.bin"]) + SPLIT_BYTES},
        ]},
    }
    return mount, manifest, hashlib.sha256(
        json.dumps(manifest, sort_keys=True).encode()).hexdigest()


def _fleet(tmp_path: Path) -> pool.PoolQueue:
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    queue.announce_tier({
        "schema": storage_tiers.TIER_RECORD_SCHEMA_V1,
        "tier_id": STAGE_TIER, "host": "dl380g10", "tier": "stage",
        "mountpoint": str(tmp_path / "stage"),
        "capacity_bytes": 4 * storage_tiers.GIB,
        "primarycache": "all",
        "arc_warm": dict(storage_tiers.stage_arc_eligibility(
            {"primarycache": "all"})),
    })
    return queue


def _move_args(tmp_path: Path, queue: pool.PoolQueue, manifest: Path,
               mover: str, start: int, end: int):
    manifest_path = tmp_path / f"manifest-{mover[:8]}.json"
    manifest_path.write_text(json.dumps(manifest))
    return stage_move.build_parser().parse_args([
        "--pool-root", str(tmp_path / "pb-queue"),
        "--cas-root", str(tmp_path / "pb-queue" / "cas"),
        "--action-key", mover,
        "--consumer-action-key", CONSUMER,
        "--tier-id", STAGE_TIER,
        "--stage-root", str(tmp_path / "stage"),
        "--manifest-sha256", "0" * 64,
        "--range-start-bytes", str(start),
        "--range-end-bytes", str(end),
        "--manifest", str(manifest_path),
        "--residency-root", str(queue.root / pool.RESIDENCY),
        "--block", str(1 << 16),
        "--readers", "2", "--max-readers", "2", "--unpaced",
    ])


def _promote_args(tmp_path: Path, queue: pool.PoolQueue, manifest: Path,
                  mover: str, start: int, end: int):
    return ram_promote.build_parser().parse_args([
        "--pool-root", str(queue.root),
        "--action-key", mover,
        "--consumer-action-key", CONSUMER,
        "--tier-id", RAM_TIER,
        "--ram-root", str(tmp_path / "ram"),
        "--source-stage-root", str(tmp_path / "stage"),
        "--manifest-sha256", "0" * 64,
        "--range-start-bytes", str(start),
        "--range-end-bytes", str(end),
        "--manifest", str(manifest),
        "--residency-root", str(queue.root / pool.RESIDENCY),
    ])


def _run_chain(tmp_path: Path):
    queue = _fleet(tmp_path)
    mount, manifest, _ = _pool_fixture(tmp_path)
    whole_len = len(corpus()["/pool/model/shard-0.bin"])
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest))
    ram = tmp_path / "ram"
    ram.mkdir()
    epoch = storage_tiers.ensure_ram_epoch(ram, host="dl380g10")
    assert epoch is not None

    whole_receipt = stage_move.move(_move_args(
        tmp_path, queue, manifest, WHOLE_MOVER, 0, whole_len))
    assert whole_receipt["complete"] is True
    queue.record_move(WHOLE_MOVER, whole_receipt)
    split_receipt = stage_move.move(_move_args(
        tmp_path, queue, manifest, SPLIT_MOVER, whole_len, whole_len + SPLIT_BYTES))
    assert split_receipt["complete"] is True
    queue.record_move(SPLIT_MOVER, split_receipt)

    whole_promoted = ram_promote.promote(_promote_args(
        tmp_path, queue, manifest_path, WHOLE_RAM_MOVER, 0, whole_len))
    assert whole_promoted["complete"] is True
    queue.record_move(WHOLE_RAM_MOVER, whole_promoted)
    split_promoted = ram_promote.promote(_promote_args(
        tmp_path, queue, manifest_path, SPLIT_RAM_MOVER,
        whole_len, whole_len + SPLIT_BYTES))
    assert split_promoted["complete"] is True
    queue.record_move(SPLIT_RAM_MOVER, split_promoted)
    return queue, manifest, whole_len, str(epoch["epoch"])


def _staged(name: str, offset: int, size: int) -> str:
    """The stage's (and the promotion's) name for one ``model/`` range."""

    return stage_move.stage_relative(f"/m/model/{name}", offset, size,
                                     mount_prefix="/m")

def test_whole_and_split_ranges_stage_byte_identical(tmp_path: Path) -> None:
    """Staged bytes equal source bytes via digests, both layouts."""
    queue, manifest, whole_len, _ = _run_chain(tmp_path)
    files = corpus()
    staged_whole = tmp_path / "stage" / _staged(WHOLE, 0, whole_len)
    assert staged_whole.read_bytes() == files["/pool/model/shard-0.bin"]
    split_file = tmp_path / "stage" / _staged(SPLIT, SPLIT_OFFSET, SPLIT_BYTES)
    assert split_file.is_file(), "split range stages its own .pbrange object"
    staged_split = split_file.read_bytes()
    assert staged_split == files["/pool/model/shard-1.bin"][SPLIT_OFFSET:SPLIT_OFFSET + SPLIT_BYTES]
    fragments = residency_map.read_fragments(queue.root / pool.RESIDENCY, CONSUMER)
    assert len(fragments) >= 2


def test_ram_promotion_lands_under_epoch_with_map_lookup(tmp_path: Path) -> None:
    """Promoted bytes resolve through compose/lookup/overlay_ram."""
    queue, manifest, whole_len, epoch = _run_chain(tmp_path)
    files = corpus()
    ram_copy = tmp_path / "ram" / _staged(WHOLE, 0, whole_len)
    assert ram_copy.read_bytes() == files["/pool/model/shard-0.bin"]
    fragments = [residency_map.validate_fragment(f) for f in
                 residency_map.read_fragments(queue.root / pool.RESIDENCY, CONSUMER)]
    stage_frags = [f for f in fragments if f["tier_id"] == STAGE_TIER]
    ram_frags = [f for f in fragments if f["tier_id"] == RAM_TIER]
    mapping = residency_map.compose(stage_frags)
    whole_declared = str(manifest["entries"][0]["path"])
    split_declared = str(manifest["entries"][1]["path"])
    found = residency_map.lookup(mapping, whole_declared, 0)
    assert found is not None
    split_found = residency_map.lookup(mapping, split_declared, SPLIT_OFFSET)
    assert split_found is not None
    assert Path(split_found["stage_path"]).read_bytes() == files["/pool/model/shard-1.bin"][SPLIT_OFFSET:SPLIT_OFFSET + SPLIT_BYTES]
    overlaid = residency_map.overlay_ram(
        mapping, ram_frags, ram_tier_id=RAM_TIER,
        ram_root=str(tmp_path / "ram"), ram_epoch=epoch)
    overlaid_entry = residency_map.lookup(overlaid, whole_declared, 0)
    assert overlaid_entry is not None
    assert overlaid_entry.get("ram_path") is not None
    assert Path(overlaid_entry["ram_path"]).read_bytes() == files["/pool/model/shard-0.bin"]
    overlaid_split = residency_map.lookup(overlaid, split_declared, SPLIT_OFFSET)
    assert overlaid_split is not None
    assert overlaid_split.get("ram_path") is not None
    assert Path(overlaid_split["ram_path"]).read_bytes() == files["/pool/model/shard-1.bin"][SPLIT_OFFSET:SPLIT_OFFSET + SPLIT_BYTES]
