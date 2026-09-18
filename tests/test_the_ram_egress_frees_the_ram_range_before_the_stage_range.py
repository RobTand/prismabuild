"""The ram egress is published before, and with, the stage egress of the same
phase.

Both egresses fire on the same fact -- the consumer's accepted progress has
passed the phase -- and they run in the same cycle, because a ram range that
outlives its stage range is a promotion whose source is gone, and a stage
range that outlives its ram range is a window the tmpfs is still paying for.
The ram egress is published first so that, on a box that runs them in queue
order, the tokens that bound the *smaller* tier come back before the bytes
that feed it leave (#640).
"""
from __future__ import annotations

import json
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
from prismabuild import pool, residency_map, residency_plan, storage_tiers  # noqa: E402

import tier_loop  # noqa: E402

CONSUMER = "c" * 64
MANIFEST = "9" * 64
STAGE_TIER = "prismabuild-stage:dl380g10"
RAM_TIER = "ram:dl380g10"
RAM_KIND = f"ram_gib@{RAM_TIER}"
STAGE_KIND = f"stage_gib@{STAGE_TIER}"
GIB = storage_tiers.GIB
EPOCH = "1695052800-1a2b3c4d5e6f7a8b"
ENTRY_KEY = residency_map.residency_map_key("/mnt/shared/model/shard-0.bin", 0)


def _hexkey(seed: str) -> str:
    return (seed.encode().hex() * 64)[:64]


def _row(key: str, resources: dict[str, int], queue: pool.PoolQueue) -> dict:
    return {"action_key": key, "cas_root": str(queue.root / "cas"),
            "checkout_root": str(queue.root / "co"),
            "worker_script": str(queue.root / "worker.py"),
            "tags": ["dl380g10"], "resources": resources}


def _plan(queue: pool.PoolQueue, *, stage_root: str) -> dict[str, object]:
    phases = []
    start = 0
    for ordinal in range(2):
        end = start + 2 * GIB
        phases.append({
            "name": f"phase-{ordinal:04d}",
            "start_bytes": start, "end_bytes": end, "stage_gib": 2,
            "mover_row": {
                **_row(_hexkey(f"mover{ordinal}"),
                       {STAGE_KIND: 2, "mem_gb": 1}, queue),
                "residency": {
                    "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": STAGE_TIER,
                    "manifest_sha256": MANIFEST, "manifest_bytes": end,
                    "range_start_bytes": start, "range_end_bytes": end}},
            "egress_row": _row(_hexkey(f"egress{ordinal}"), {"mem_gb": 1},
                               queue),
            "ram_mover_row": {
                **_row(_hexkey(f"rampromote{ordinal}"),
                       {RAM_KIND: 2, "mem_gb": 1}, queue),
                "residency": {
                    "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": RAM_TIER,
                    "manifest_sha256": MANIFEST, "manifest_bytes": end,
                    "range_start_bytes": start, "range_end_bytes": end}},
            "ram_egress_row": _row(_hexkey(f"ramrelease{ordinal}"),
                                   {"mem_gb": 1}, queue),
        })
        start = end
    return residency_plan.build_plan(
        consumer_action_key=CONSUMER, tier_id=STAGE_TIER, stage_root=stage_root,
        manifest_sha256=MANIFEST, manifest_bytes=start, phases=phases,
        ram_tier_id=RAM_TIER)


def _fragment(*, root: str, mover: str, tier_id: str,
              epoch: str | None = None) -> dict[str, object]:
    fragment: dict[str, object] = {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": CONSUMER, "mover_action_key": mover,
        "tier_id": tier_id, "stage_root": root, "manifest_sha256": MANIFEST,
        "entries": {ENTRY_KEY: {
            "stage_path": f"{root}/model/shard-0.bin", "bytes": 4096,
            "offset": 0, "sha256": "b" * 64}},
    }
    if epoch is not None:
        fragment["epoch"] = epoch
    return fragment


def _fixture(tmp_path: Path) -> tuple[pool.PoolQueue, Path, Path]:
    """One consumer past its first phase, with that phase resident on both
    tiers: the stage range pinned, the ram promotion pinned under the current
    epoch."""

    stage = tmp_path / "stage"
    ram = tmp_path / "ram"
    (stage / "model").mkdir(parents=True)
    (ram / "model").mkdir(parents=True)
    (stage / "model" / "shard-0.bin").write_bytes(b"\0" * 4096)
    (ram / "model" / "shard-0.bin").write_bytes(b"\0" * 4096)

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    plan = _plan(queue, stage_root=str(stage))
    residency_plan.freeze(queue, plan)
    queue.publish(**_row(CONSUMER, {"mem_gb": 1}, queue), residency={
        "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": STAGE_TIER,
        "manifest_sha256": MANIFEST, "manifest_bytes": 4 * GIB,
        "leads": residency_plan.leads_for(plan)})

    queue.mint_tier_capacity(STAGE_TIER, {"stage_gib": 64})
    queue.mint_tier_capacity(RAM_TIER, {"ram_gib": 8})
    stage_mover, ram_mover = _hexkey("mover0"), _hexkey("rampromote0")
    assert queue.tier_ledger(STAGE_TIER).acquire(stage_mover, {"stage_gib": 2})
    assert queue.tier_ledger(RAM_TIER).acquire(ram_mover, {"ram_gib": 2})
    queue.record_move(stage_mover, {
        "consumer_action_key": CONSUMER, "tier_id": STAGE_TIER,
        "stage_root": str(stage), "manifest_sha256": MANIFEST,
        "range_start_bytes": 0, "range_end_bytes": 2 * GIB,
        "bytes_staged": 2 * GIB, "complete": True, "seconds": 1.0,
        "unix": 1000.0})
    queue.record_move(ram_mover, {
        "consumer_action_key": CONSUMER, "tier_id": RAM_TIER,
        "ram_root": str(ram), "manifest_sha256": MANIFEST,
        "range_start_bytes": 0, "range_end_bytes": 2 * GIB,
        "bytes_staged": 2 * GIB, "complete": True, "epoch": EPOCH,
        "seconds": 1.0, "unix": 1000.0})
    residency_map.write_fragment(queue.root / pool.RESIDENCY, _fragment(
        root=str(stage), mover=stage_mover, tier_id=STAGE_TIER))
    residency_map.write_fragment(queue.root / pool.RESIDENCY, _fragment(
        root=str(ram), mover=ram_mover, tier_id=RAM_TIER, epoch=EPOCH))

    # The consumer is claimed and has accepted the second phase, so the
    # window's release side owes the first phase both of its egresses.
    source = queue.item_path(pool.READY, CONSUMER)
    item = json.loads(source.read_text())
    source.unlink()
    item.update({"action_key": CONSUMER, "claimed_unix": time.time(),
                 "claimed_by": "ram-egress-fixture", "claimed_host": "dl380g10"})
    queue.item_path(pool.CLAIMED, CONSUMER).write_text(json.dumps(item))
    queue.write_lease(CONSUMER, owner="ram-egress-fixture",
                      claim_snapshot=item, progress_observation={
                          "source": "action-progress",
                          "last_accepted": {"phase": "phase-0001",
                                            "units_completed": 1,
                                            "reported_unix": time.time()}})
    return queue, stage, ram


def _tiers(stage: Path, ram: Path) -> dict[str, dict[str, object]]:
    return {
        STAGE_TIER: {
            "schema": storage_tiers.TIER_RECORD_SCHEMA_V1, "tier": "stage",
            "tier_id": STAGE_TIER, "host": "dl380g10",
            "mountpoint": str(stage), "capacity_bytes": 62 * GIB,
            "capacity_source": storage_tiers.WRITABLE_CAPACITY_SOURCE,
            "primarycache": "all",
        },
        RAM_TIER: {
            "schema": storage_tiers.TIER_RECORD_SCHEMA_V1, "tier": "ram",
            "tier_id": RAM_TIER, "host": "dl380g10", "mountpoint": str(ram),
            "epoch": EPOCH, "capacity_bytes": 6 * GIB,
            "ceiling_bytes": 256 * GIB, "window_gib": 8,
            "ram_admission": {"admissible": True, "reason": None},
            "mount_options": ["rw", "noswap", "size=256G"],
        },
    }


def test_both_egresses_are_published_and_the_ram_one_first(
        tmp_path: Path, capsys) -> None:
    queue, stage, ram = _fixture(tmp_path)

    def discover(**_kwargs):
        return _tiers(stage, ram)

    tier_loop.cycle(queue, host="dl380g10", source_pool="storage_pool",
                    receipts=tier_loop.ReceiptCache(), discover=discover)

    lines = [json.loads(line) for line in capsys.readouterr().out.splitlines()
             if line.startswith("{")]
    ram_egress = [index for index, line in enumerate(lines)
                  if line.get("event") == "ram-egress-published"
                  and line.get("phase") == "phase-0000"]
    stage_egress = [index for index, line in enumerate(lines)
                    if line.get("event") == "egress-published"
                    and line.get("phase") == "phase-0000"]
    assert ram_egress and stage_egress
    assert ram_egress[0] < stage_egress[0], \
        "the ram range is freed before, and with, the stage range"
    # Both rows are queued for the pool to run, and the pinned bytes are
    # still attributed -- an egress row is a request, not a deletion.
    assert queue.item_path(pool.READY, _hexkey("ramrelease0")).exists()
    assert queue.item_path(pool.READY, _hexkey("egress0")).exists()
    assert (ram / "model" / "shard-0.bin").exists()
    assert queue.tier_ledger(RAM_TIER).holder_tokens(_hexkey("rampromote0")) == {
        "ram_gib": 2}
