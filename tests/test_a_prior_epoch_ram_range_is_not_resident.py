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

A verdict is not a gate.  The claim pass refuses on ``residency_verdict`` and
is what keeps the item READY without aging it, takes no host token, and files
a named ``residency_ram_epoch_stale`` denial.  These tests therefore run the
real ``PoolQueue.claim`` over the same fixture and prove the stale-epoch map
is refused there, with an identical map at the announced epoch and a stage-only
map as positive controls that still claim, and a map naming a ram tier nobody
announced as the fail-closed negative.  ``_ready_gpu_row_room`` reads the same
policy; it is exercised directly, with no GPU in the room.

Fixtures: a real queue, real residency-map documents and a pinned stage lead
read through the queue's own ledger; the consumer is published through the
real ``PoolQueue.publish``.  No payload runs and no GPU is touched.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import adaptive_cpu, pool, residency_map, storage_tiers  # noqa: E402

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


def _queue(tmp_path: Path, *, announce_ram: bool = True) -> pool.PoolQueue:
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
    if announce_ram:
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


def _publish_consumer(queue: pool.PoolQueue) -> None:
    """A real consumer row over a leads-only block: no range, no tier demand."""

    queue.publish(
        action_key=CONSUMER,
        cas_root=queue.root / "cas",
        checkout_root=queue.root / "co",
        worker_script=queue.root / "worker.py",
        resources={"cpu": 1},
        residency={
            "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": STAGE_TIER,
            "manifest_sha256": MANIFEST, "manifest_bytes": RANGE,
            "leads": [STAGE_LEAD],
        },
    )


def _denial(queue: pool.PoolQueue, key: str) -> dict[str, object] | None:
    path = adaptive_cpu.local_state_base(queue.ledger().base) / pool.CLAIM_DENIALS
    records = adaptive_cpu.read_json(path).get("records", {})
    matching = [entry for entry in records.values()
                if isinstance(entry, dict) and entry.get("action_key") == key]
    if not matching:
        return None
    return max(matching, key=lambda entry: float(entry.get("denied_unix", 0.0)))


def _stage_fragment() -> dict[str, object]:
    return {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": CONSUMER, "mover_action_key": STAGE_LEAD,
        "tier_id": STAGE_TIER, "stage_root": STAGE_ROOT,
        "manifest_sha256": MANIFEST,
        "entries": {ENTRY_KEY: {
            "stage_path": f"{STAGE_ROOT}/model/shard-0.bin", "bytes": 4096,
            "offset": 0, "sha256": "b" * 64}},
    }


def _ram_fragment(ram_epoch: str) -> dict[str, object]:
    return {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": CONSUMER, "mover_action_key": RAM_MOVER,
        "tier_id": RAM_TIER, "stage_root": RAM_ROOT, "epoch": ram_epoch,
        "manifest_sha256": MANIFEST,
        "entries": {ENTRY_KEY: {
            "stage_path": f"{RAM_ROOT}/model/shard-0.bin", "bytes": 4096,
            "offset": 0, "sha256": "b" * 64}},
    }


def _map_with_ram(queue: pool.PoolQueue, *, ram_epoch: str) -> Path:
    mapping = residency_map.overlay_ram(
        residency_map.compose([_stage_fragment()]), [_ram_fragment(ram_epoch)],
        ram_tier_id=RAM_TIER, ram_root=RAM_ROOT, ram_epoch=ram_epoch)
    return residency_map.write_map(queue.residency_map_path(CONSUMER), mapping)


def _stage_only_map(queue: pool.PoolQueue) -> Path:
    return residency_map.write_map(
        queue.residency_map_path(CONSUMER),
        residency_map.compose([_stage_fragment()]))


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
    _stage_only_map(queue)

    verdict = queue.residency_verdict(_consumer_item(queue))

    assert verdict["state"] == "resident"


def test_a_stale_epoch_map_is_refused_at_claim_before_tokens(
    tmp_path: Path,
) -> None:
    """The verdict's refusal is enforced where admission runs, not only read."""

    queue = _queue(tmp_path)
    _map_with_ram(queue, ram_epoch=OLD_EPOCH)
    _publish_consumer(queue)

    assert queue.claim(owner="w-consumer", capacity={"cpu": 4}) is None
    assert queue.item_path(pool.READY, CONSUMER).exists()
    assert not queue.item_path(pool.CLAIMED, CONSUMER).exists()
    denial = _denial(queue, CONSUMER)
    assert denial is not None and denial["reason"] == "residency_ram_epoch_stale"
    assert denial["evidence"]["residency"]["state"] == "ram_epoch_stale"
    assert denial["evidence"]["residency"]["ram_epoch"] == OLD_EPOCH
    # Refused before any token moved, and without aging the row: the ram tier
    # recomposes after the reboot and a later scan admits it.
    assert queue.ledger().held() == {}
    assert queue.passes(CONSUMER) == 0


def test_a_map_naming_a_ram_tier_nobody_announced_is_refused(
    tmp_path: Path,
) -> None:
    """Unknown is not a residence: no announcement cannot be this map's epoch."""

    queue = _queue(tmp_path, announce_ram=False)
    _map_with_ram(queue, ram_epoch=NEW_EPOCH)
    _publish_consumer(queue)

    assert queue.claim(owner="w-consumer", capacity={"cpu": 4}) is None
    assert queue.item_path(pool.READY, CONSUMER).exists()
    denial = _denial(queue, CONSUMER)
    assert denial is not None and denial["reason"] == "residency_ram_epoch_stale"
    assert queue.ledger().held() == {}
    assert queue.passes(CONSUMER) == 0


def test_a_current_epoch_map_still_claims(tmp_path: Path) -> None:
    queue = _queue(tmp_path)
    _map_with_ram(queue, ram_epoch=NEW_EPOCH)
    _publish_consumer(queue)

    claimed = queue.claim(owner="w-consumer", capacity={"cpu": 4})

    assert claimed is not None and claimed["action_key"] == CONSUMER
    assert claimed["residency_verdict"]["state"] == "resident"
    assert not queue.item_path(pool.READY, CONSUMER).exists()


def test_a_stage_only_map_still_claims(tmp_path: Path) -> None:
    queue = _queue(tmp_path)
    _stage_only_map(queue)
    _publish_consumer(queue)

    claimed = queue.claim(owner="w-consumer", capacity={"cpu": 4})

    assert claimed is not None and claimed["action_key"] == CONSUMER
    assert claimed["residency_verdict"]["state"] == "resident"


def test_the_ready_gpu_row_room_reads_the_ram_epoch(tmp_path: Path) -> None:
    """The room a GPU row keeps reads the shared refusal list, stale included."""

    queue = _queue(tmp_path)
    _map_with_ram(queue, ram_epoch=OLD_EPOCH)
    _publish_consumer(queue)
    item = pool._read_json(queue.item_path(pool.READY, CONSUMER))
    assert item is not None

    def room() -> dict[str, object] | None:
        return queue._ready_gpu_row_room(  # noqa: SLF001 - policy under test
            item, ledger=queue.ledger(), total={}, controller=None,
            gpu_controller=None, observed_images=None)

    assert room() is None
    # Recomposed at the announced epoch, the same row keeps its room.
    _map_with_ram(queue, ram_epoch=NEW_EPOCH)
    assert room() == {"action_key": CONSUMER, "room": {"cpu": 1}}
