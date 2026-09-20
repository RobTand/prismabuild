"""One staged file can have two owners; an egress must not orphan either.

Two shapes of the same defect in the shared copier/fragment mechanism:

1. Forward and reverse passes stage the same source extent -- the same
   ``(path, offset, bytes)`` triple -- through different movers (different
   manifests, different consumers, or two read phases of one v2 plan), and
   ``stage_relative`` derives one staged name for both.  The first owner's
   egress unlinks the file while the second owner's fragment still vouches
   for it and its tokens stay held: a hole behind a live map.
2. A split range lives on the stage as ``<rel>.pbrange/<offset>-<size>`` from
   byte zero, but a promotion resolves its source as the original pool path
   at the manifest offset.  Whole-file ranges promote; anything split fails
   to even open its source.
"""
from __future__ import annotations

import json
import threading
from pathlib import Path
import sys
import types

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
from prismabuild import pool, residency_map, storage_tiers  # noqa: E402
import prewarm_loop  # noqa: E402
import stage_release  # noqa: E402
from stage_move import _Copier, whole_file_paths  # noqa: E402
import ram_promote  # noqa: E402

CONSUMER_A = "a" * 64
CONSUMER_B = "b" * 64
MOVER_A = "1" * 64
MOVER_B = "2" * 64
TIER = "prismabuild-stage:dl380g10"
RAM_TIER = "ram:dl380g10"
GIB = storage_tiers.GIB
MIB = 1024 * 1024


def _fragment(root: Path, stage: Path, consumer: str, mover: str,
              source: str, staged: Path, size: int, tier: str = TIER,
              epoch: str | None = None) -> None:
    body: dict[str, object] = {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": consumer, "mover_action_key": mover,
        "tier_id": tier, "stage_root": str(stage),
        "manifest_sha256": "a" * 64,
        "entries": {
            residency_map.residency_map_key(source, 0): {
                "stage_path": str(staged), "bytes": size,
                "sha256": "b" * 64, "offset": 0,
            },
        },
    }
    if epoch is not None:
        body["epoch"] = epoch
    residency_map.write_fragment(root, body)


@pytest.fixture()
def fleet(tmp_path: Path):
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    stage = tmp_path / "stage"
    stage.mkdir()
    stage_release.register_stage_root(queue, tier_id=TIER, stage_root=stage)
    return queue, stage


def test_one_owners_egress_leaves_the_other_owners_bytes(fleet) -> None:
    """The forward/reverse same-extent shape: two movers, one staged file."""

    queue, stage = fleet
    shared = stage / "model" / "layer.safetensors"
    shared.parent.mkdir(parents=True)
    shared.write_bytes(b"\0" * 4096)
    root = queue.root / pool.RESIDENCY
    _fragment(root, stage, CONSUMER_A, MOVER_A,
              "/mnt/shared/model/layer.safetensors", shared, 4096)
    _fragment(root, stage, CONSUMER_B, MOVER_B,
              "/mnt/shared/model/layer.safetensors", shared, 4096)

    first = stage_release.evict(queue, MOVER_A, consumer_action_key=CONSUMER_A,
                                stage_root=str(stage))
    assert shared.exists(), (
        "mover B still vouches for this file; A's egress must not unlink it")
    assert first["complete"] is True

    second = stage_release.evict(queue, MOVER_B, consumer_action_key=CONSUMER_B,
                                 stage_root=str(stage))
    assert not shared.exists()
    assert second["complete"] is True


def _split_manifest(pool_dir: Path) -> dict[str, object]:
    entries = [
        {"path": "/mnt/shared/shard.bin", "offset": 0, "bytes": MIB,
         "sha256": None},
        {"path": "/mnt/shared/shard.bin", "offset": MIB, "bytes": MIB,
         "sha256": None},
    ]
    return {
        "schema": "prismaquant.prismabuild.data_manifest.v1",
        "produced_by": {}, "annotations": {},
        "mount_prefix": "/mnt/shared",
        "entries": entries,
        "entry_count": 2,
        "total_bytes": 2 * MIB,
    }


def test_a_split_range_promotes_from_its_staged_shard_path(tmp_path: Path) -> None:
    """A nonzero-offset split range must promote, not fail to open its source."""

    pool_dir = tmp_path / "pool"
    pool_dir.mkdir()
    (pool_dir / "shard.bin").write_bytes(b"\x01" * MIB + b"\x02" * MIB)
    stage = tmp_path / "stage"
    stage.mkdir()
    ram = tmp_path / "ram"
    ram.mkdir()
    assert storage_tiers.ensure_ram_epoch(ram, host="test") is not None
    manifest_path = tmp_path / "manifest.json"
    manifest = _split_manifest(pool_dir)
    manifest_path.write_text(json.dumps(manifest))

    entries = prewarm_loop.manifest_read_entries(manifest)
    assert whole_file_paths(entries) == set()
    stage_copier = _Copier(
        mounts=prewarm_loop.MountMap([f"/mnt/shared={pool_dir}"]),
        pacer=None, stage_root=stage, mount_prefix="/mnt/shared",
        block=MIB, workers=2, owner="stage-mover")
    stage_copier.run(entries, whole=set(), stop=threading.Event())
    assert stage_copier.errors == []
    assert stage_copier.bytes_staged == 2 * MIB

    args = ram_promote.build_parser().parse_args([
        "--pool-root", str(tmp_path / "pool-root"),
        "--manifest", str(manifest_path),
        "--consumer-action-key", CONSUMER_A,
        "--tier-id", RAM_TIER,
        "--ram-root", str(ram),
        "--source-stage-root", str(stage),
        "--manifest-sha256", "0" * 64,
        "--range-start-bytes", "0",
        "--range-end-bytes", str(2 * MIB),
        "--action-key", MOVER_A,
        "--residency-root", str(tmp_path / "residency"),
        "--block", str(MIB),
        "--readers", "2",
    ])
    receipt = ram_promote.promote(args)
    assert receipt["complete"] is True, receipt
    assert int(receipt["bytes_staged"]) == 2 * MIB
    first = (ram / "shard.bin.pbrange" / f"0-{MIB}").read_bytes()
    second = (ram / "shard.bin.pbrange" / f"{MIB}-{MIB}").read_bytes()
    assert first == b"\x01" * MIB
    assert second == b"\x02" * MIB
