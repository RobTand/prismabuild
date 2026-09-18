"""A prior-epoch ram range is not resident, whatever the map still says.

tmpfs empties on reboot; the ledger and the residency-map fragments on the
shared mount survive it.  A map composed before the reboot names ram paths
whose bytes are gone, so the verdict that would admit a consumer onto them
must refuse: the map's ram epoch is compared against the epoch the ram tier
*announces now*, and a mismatch is ``ram_epoch_stale`` -- the same one-cycle
wait as ``map_not_composed``, because the tier loop recomposes from the
fragments that survive, which name no ram range at all until fresh ones land.

A map with no ram entries never asks the question: the stage residency the
verdict has always gated on is durable and checkable, and the ram tier is a
performance tier in front of it (#640).
"""
from __future__ import annotations

import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import pool, residency_map, storage_tiers  # noqa: E402

CONSUMER = "c" * 64
STAGE_LEAD = "1" * 64
RAM_MOVER = "2" * 64
MANIFEST = "9" * 64
STAGE_TIER = "prismabuild-stage:dl380g10"
RAM_TIER = "ram:dl380g10"
STAGE_ROOT = "/stage/prewarm"
RAM_ROOT = "/ram/prewarm"
RANGE = 2 * storage_tiers.GIB
ENTRY_KEY = residency_map.residency_map_key("/mnt/shared/model/shard-0.bin", 0)
OLD_EPOCH = "1695000000-deadbeefdeadbeef"
NEW_EPOCH = "1695052800-1a2b3c4d5e6f7a8b"


def _queue(tmp_path: Path) -> pool.PoolQueue:
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    queue.mint_tier_capacity(STAGE_TIER, {"stage_gib": 8})
    ledger = queue.tier_ledger(STAGE_TIER)
    assert ledger.acquire(STAGE_LEAD, {"stage_gib": 2})
    queue.record_move(STAGE_LEAD, {
        "consumer_action_key": CONSUMER, "tier_id": STAGE_TIER,
        "stage_root": STAGE_ROOT, "manifest_sha256": MANIFEST,
        "range_start_bytes": 0, "range_end_bytes": RANGE,
        "bytes_staged": RANGE, "complete": True, "seconds": 1.0,
        "unix": 1000.0})
    queue.item_path(pool.DONE, STAGE_LEAD).write_text(json.dumps({
        "action_key": STAGE_LEAD, "status": "executed",
        "residency": _block(range_start_bytes=0, range_end_bytes=RANGE,
                            tier_id=STAGE_TIER)}))
    queue.announce_tier({
        "schema": storage_tiers.TIER_RECORD_SCHEMA_V1, "tier": "ram",
        "tier_id": RAM_TIER, "host": "dl380g10", "mountpoint": RAM_ROOT,
        "epoch": NEW_EPOCH, "capacity_bytes": 112 * storage_tiers.GIB,
    })
    return queue


def _block(*, range_start_bytes: int, range_end_bytes: int,
           tier_id: str) -> dict[str, object]:
    return {
        "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": tier_id,
        "manifest_sha256": MANIFEST, "manifest_bytes": RANGE,
        "range_start_bytes": range_start_bytes,
        "range_end_bytes": range_end_bytes,
    }


def _consumer_item(queue: pool.PoolQueue) -> dict[str, object]:
    return {
        "action_key": CONSUMER,
        "residency": {**_block(range_start_bytes=0, range_end_bytes=RANGE,
                               tier_id=STAGE_TIER),
                      "leads": [STAGE_LEAD]},
    }


def _map_with_ram(queue: pool.PoolQueue, *, ram_epoch: str) -> Path:
    stage_fragment = {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": CONSUMER, "mover_action_key": STAGE_LEAD,
        "tier_id": STAGE_TIER, "stage_root": STAGE_ROOT,
        "manifest_sha256": MANIFEST,
        "entries": {ENTRY_KEY: {
            "stage_path": f"{STAGE_ROOT}/model/shard-0.bin", "bytes": 4096,
            "offset": 0, "sha256": "b" * 64}},
    }
    ram_fragment = {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": CONSUMER, "mover_action_key": RAM_MOVER,
        "tier_id": RAM_TIER, "stage_root": RAM_ROOT, "epoch": ram_epoch,
        "manifest_sha256": MANIFEST,
        "entries": {ENTRY_KEY: {
            "stage_path": f"{RAM_ROOT}/model/shard-0.bin", "bytes": 4096,
            "offset": 0, "sha256": "b" * 64}},
    }
    mapping = residency_map.overlay_ram(
        residency_map.compose([stage_fragment]), [ram_fragment],
        ram_tier_id=RAM_TIER, ram_root=RAM_ROOT, ram_epoch=ram_epoch)
    return residency_map.write_map(queue.residency_map_path(CONSUMER), mapping)


def test_a_map_whose_ram_epoch_is_not_the_tiers_is_stale(tmp_path: Path) -> None:
    queue = _queue(tmp_path)
    _map_with_ram(queue, ram_epoch=OLD_EPOCH)

    verdict = queue.residency_verdict(_consumer_item(queue))

    assert verdict["state"] == "ram_epoch_stale"
    assert verdict["ram_epoch"] == OLD_EPOCH


def test_a_map_whose_ram_epoch_is_the_tiers_admits(tmp_path: Path) -> None:
    queue = _queue(tmp_path)
    _map_with_ram(queue, ram_epoch=NEW_EPOCH)

    verdict = queue.residency_verdict(_consumer_item(queue))

    assert verdict["state"] == "resident"


def test_a_map_with_no_ram_entries_never_asks_the_epoch(tmp_path: Path) -> None:
    """The stage leg is the gate; ram residency is an overlay, not a lead."""

    queue = _queue(tmp_path)
    fragment = {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": CONSUMER, "mover_action_key": STAGE_LEAD,
        "tier_id": STAGE_TIER, "stage_root": STAGE_ROOT,
        "manifest_sha256": MANIFEST,
        "entries": {ENTRY_KEY: {
            "stage_path": f"{STAGE_ROOT}/model/shard-0.bin", "bytes": 4096,
            "offset": 0, "sha256": "b" * 64}},
    }
    residency_map.write_map(
        queue.residency_map_path(CONSUMER),
        residency_map.compose([fragment]))

    verdict = queue.residency_verdict(_consumer_item(queue))

    assert verdict["state"] == "resident"
