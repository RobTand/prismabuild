"""A failed consumer's unstarted movers keep running; their writes collide
with the successor's movers on shared stage paths (#620).

Observed 2026-09-18 on the dl380g10 stage tier: consumer ``111ab2fc`` failed
with ten movers published.  The egress evicted the four completed ones; the
six not-yet-run movers were not withdrawn and staged ~400 GB for a consumer
already in ``failed/``.  The successor's head mover then ran over the same
content paths and finished ``complete: false`` -- its ``.partial`` vanished
before the rename, taken by another mover working the same path for the dead
consumer.  Stage paths are content-addressed per manifest entry and shared
between consumers; the bookkeeping is per consumer.

Two fixes, one file each:

1. When a consumer reaches ``failed/``/``withdrawn/``, the tier loop
   withdraws its movers that are still in ``ready/`` or ``claimed/`` in the
   same egress cycle that evicts its resident ranges.  Finished movers --
   complete receipts holding tokens -- are left for adoption, live
   consumers are untouched, and a resubmitted consumer is alive again.
2. Two movers never share a stage temporary: the ``.partial`` beside each
   destination is keyed by the mover writing it, so concurrent copies of one
   entry verify and rename independently.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sys
import threading

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
from prismabuild import pool, residency_map, residency_plan, storage_tiers  # noqa: E402
import prewarm_loop  # noqa: E402
import stage_move  # noqa: E402
import tier_loop  # noqa: E402

TIER = "prismabuild-stage:dl380g10"
STAGE_KIND = f"stage_gib@{TIER}"
MANIFEST = "9" * 64
GIB = storage_tiers.GIB
PHASE_GIB = 2

FIRST = "1" * 64
SECOND = "2" * 64


def _hexkey(seed: str) -> str:
    return (seed.encode().hex() * 64)[:64]


def _row(queue: pool.PoolQueue, key: str,
         resources: dict[str, int]) -> dict[str, object]:
    return {"action_key": key, "cas_root": str(queue.root / "cas"),
            "checkout_root": str(queue.root / "co"),
            "worker_script": str(queue.root / "worker.py"),
            "tags": ["dl380g10"], "resources": resources}


def _plan(queue: pool.PoolQueue, consumer: str, *, label: str,
          phases: int = 2) -> dict[str, object]:
    built = []
    for ordinal in range(phases):
        start, end = ordinal * PHASE_GIB * GIB, (ordinal + 1) * PHASE_GIB * GIB
        built.append({
            "name": f"phase-{ordinal}",
            "start_bytes": start, "end_bytes": end, "stage_gib": PHASE_GIB,
            "mover_row": {
                **_row(queue, _hexkey(f"{label}mover{ordinal}"),
                       {STAGE_KIND: PHASE_GIB, "cpu": 1, "mem_gb": 1}),
                "residency": {
                    "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                    "manifest_sha256": MANIFEST, "manifest_bytes": 1 << 30,
                    "range_start_bytes": start, "range_end_bytes": end},
            },
            "egress_row": _row(queue, _hexkey(f"{label}egress{ordinal}"),
                               {"mem_gb": 1}),
        })
    return residency_plan.build_plan(
        consumer_action_key=consumer, tier_id=TIER, stage_root="/stage/prewarm",
        manifest_sha256=MANIFEST, manifest_bytes=1 << 30, phases=built)


def _publish_consumer(queue: pool.PoolQueue, consumer: str,
                      plan: dict[str, object]) -> None:
    residency_plan.freeze(queue, plan)
    queue.publish(
        action_key=consumer, cas_root=queue.root / "cas",
        checkout_root=queue.root / "co", worker_script=queue.root / "worker.py",
        resources={"cpu": 1, "mem_gb": 1},
        residency={"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                   "manifest_sha256": MANIFEST, "manifest_bytes": 1 << 30,
                   "leads": residency_plan.leads_for(plan)})


def _fail_consumer(queue: pool.PoolQueue, consumer: str) -> None:
    """File the consumer terminal the way a failed ending does: present in
    ``failed/``, absent from ``ready/`` and ``claimed/``."""

    source = queue.item_path(pool.READY, consumer)
    record = json.loads(source.read_text())
    record["status"] = "failed"
    queue.item_path(pool.FAILED, consumer).write_text(json.dumps(record))
    source.unlink()


def _claim(queue: pool.PoolQueue, key: str) -> None:
    source = queue.item_path(pool.READY, key)
    item = json.loads(source.read_text())
    source.unlink()
    item.update({"action_key": key, "claimed_unix": 1000.0,
                 "claimed_by": "worker", "claimed_host": "dl380g10"})
    queue.item_path(pool.CLAIMED, key).write_text(json.dumps(item))


@pytest.fixture()
def queue(tmp_path: Path) -> pool.PoolQueue:
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    return queue


def test_queued_and_claimed_movers_of_a_failed_consumer_are_withdrawn(
        queue: pool.PoolQueue) -> None:
    """RED before #620: the six not-yet-run movers staged 400 GB for nobody."""

    plan = _plan(queue, FIRST, label="first")
    _publish_consumer(queue, FIRST, plan)
    queued = _hexkey("firstmover0")
    running = _hexkey("firstmover1")
    queue.publish(**plan["phases"][0]["mover_row"], recompute=True)
    queue.publish(**plan["phases"][1]["mover_row"], recompute=True)
    # The second mover has been claimed by a worker and is copying.
    _claim(queue, running)
    _fail_consumer(queue, FIRST)

    events = tier_loop.withdraw_dead_consumer_movers(queue)

    assert {event["mover"] for event in events if event.get("withdrawn")} == {
        queued, running}
    assert not queue.item_path(pool.READY, queued).exists()
    # The queued mover never starts; the claimed one is stopped through the
    # withdrawal decision, which is the durable stop signal.  Its ``claimed/``
    # record stays until the claiming worker observes the marker and
    # concludes -- withdrawing never concludes another worker's claim.
    assert queue.item_path(pool.WITHDRAWN, queued).exists()
    assert queue.item_path(pool.WITHDRAWN, running).exists()
    filed = json.loads(queue.item_path(pool.WITHDRAWN, running).read_text())
    assert filed["status"] == "withdrawn"


def test_a_finished_mover_is_left_for_adoption_not_withdrawn(
        queue: pool.PoolQueue, tmp_path: Path) -> None:
    """A complete copy is a resident range, which is the successor's to take."""

    plan = _plan(queue, FIRST, label="first")
    _publish_consumer(queue, FIRST, plan)
    finished = _hexkey("firstmover0")
    stage = tmp_path / "stage"
    stage.mkdir()
    queue.mint_tier_capacity(TIER, {"stage_gib": 8})
    assert queue.tier_ledger(TIER).acquire(finished, {"stage_gib": PHASE_GIB})
    queue.record_move(finished, {
        "consumer_action_key": FIRST, "tier_id": TIER,
        "stage_root": str(stage), "manifest_sha256": MANIFEST,
        "range_start_bytes": 0, "range_end_bytes": PHASE_GIB * GIB,
        "bytes_staged": PHASE_GIB * GIB, "complete": True,
        "seconds": 10.0, "unix": 1000.0})
    _fail_consumer(queue, FIRST)

    events = tier_loop.withdraw_dead_consumer_movers(queue)

    assert [event for event in events
            if event.get("event") == "dead-consumer-mover-withdrawn"] == []
    assert finished in queue.tier_ledger(TIER).held_keys()
    # The dead consumer's plan is residue once nothing names it (#708): it is
    # archived, and the resident range keeps its tokens for adoption.
    assert residency_plan.read(queue, FIRST) is None


def test_a_live_consumer_and_its_egress_rows_are_untouched(
        queue: pool.PoolQueue) -> None:
    """Only movers, and only the dead consumer's: egress rows still clean up."""

    _publish_consumer(queue, FIRST, _plan(queue, FIRST, label="first"))
    second = _plan(queue, SECOND, label="second")
    _publish_consumer(queue, SECOND, second)
    live_mover = _hexkey("secondmover0")
    live_egress = str(second["phases"][0]["egress_row"]["action_key"])
    assert live_egress == _hexkey("second" + "egress0")
    queue.publish(**second["phases"][0]["mover_row"], recompute=True)
    queue.publish(**second["phases"][0]["egress_row"], recompute=True)
    _fail_consumer(queue, FIRST)

    tier_loop.withdraw_dead_consumer_movers(queue)

    assert queue.item_path(pool.READY, live_mover).exists()
    assert queue.item_path(pool.READY, live_egress).exists()


def test_a_resubmitted_consumer_is_alive_again(
        queue: pool.PoolQueue) -> None:
    """A key in ``failed/`` but also in ``ready/`` was resubmitted: hands off.

    Withdrawal names a generation, not a key for all time; the new publication
    is a new generation and its movers are live work.
    """

    plan = _plan(queue, FIRST, label="first")
    _publish_consumer(queue, FIRST, plan)
    mover = _hexkey("firstmover0")
    queue.publish(**plan["phases"][0]["mover_row"], recompute=True)
    _fail_consumer(queue, FIRST)
    _publish_consumer(queue, FIRST, plan)

    events = tier_loop.withdraw_dead_consumer_movers(queue)

    assert events == []
    assert queue.item_path(pool.READY, mover).exists()


def test_a_withdrawn_consumer_is_dead_too(queue: pool.PoolQueue) -> None:
    plan = _plan(queue, FIRST, label="first")
    _publish_consumer(queue, FIRST, plan)
    mover = _hexkey("firstmover0")
    queue.publish(**plan["phases"][0]["mover_row"], recompute=True)
    queue.withdraw(FIRST, reason="operator asked", by="test")

    events = tier_loop.withdraw_dead_consumer_movers(queue)

    assert [event["mover"] for event in events if event.get("withdrawn")] == [
        mover]


def _copier(stage: Path, source_dir: Path, *, owner: str) -> stage_move._Copier:
    return stage_move._Copier(
        mounts=prewarm_loop.MountMap([]), pacer=None,
        stage_root=stage, mount_prefix=str(source_dir), block=1 << 16,
        workers=8, owner=owner)


def test_two_movers_copying_one_entry_do_not_share_a_temporary(
        tmp_path: Path) -> None:
    """RED before #620: one ``.partial`` per destination, truncated by both.

    Sixty-four entries through eight workers each, the same bytes under two
    action keys: before the fix every destination's temporary is shared and
    the loser's rename fails with ENOENT (or worse, lands a torn prefix);
    after it each mover writes its own and both complete with no errors.
    """

    source_dir = tmp_path / "pool"
    source_dir.mkdir()
    entries = []
    for index in range(64):
        path = source_dir / f"shard-{index:04d}.bin"
        payload = bytes([index % 251]) * (1 << 19)
        path.write_bytes(payload)
        entries.append({
            "path": str(path), "bytes": len(payload), "offset": 0,
            "sha256": hashlib.sha256(payload).hexdigest()})
    whole = {str(entry["path"]) for entry in entries}

    copiers = [_copier(tmp_path / "stage", source_dir, owner=("a" if n == 0 else "b") * 64)
               for n in range(2)]
    assert (copiers[0]._temporary(tmp_path / "stage" / "shard-0000.bin")
            != copiers[1]._temporary(tmp_path / "stage" / "shard-0000.bin"))
    stop = threading.Event()
    threads = [threading.Thread(target=copier.run, args=(entries,),
                                kwargs={"whole": whole, "stop": stop})
               for copier in copiers]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    for copier in copiers:
        assert copier.errors == [], copier.errors[:3]
        assert copier.bytes_staged == sum(int(entry["bytes"]) for entry in entries)
    for entry in entries:
        staged = tmp_path / "stage" / Path(str(entry["path"])).name
        assert staged.read_bytes() == (tmp_path / "pool" / staged.name).read_bytes()


def test_one_mover_still_cleans_up_after_itself(tmp_path: Path) -> None:
    """The per-mover temporary is removed on failure, as the shared one was."""

    source_dir = tmp_path / "pool"
    source_dir.mkdir()
    missing = source_dir / "gone.bin"
    copier = _copier(tmp_path / "stage", source_dir, owner="c" * 64)
    stop = threading.Event()
    copier.run([{"path": str(missing), "bytes": 16, "offset": 0,
                 "sha256": "d" * 64}],
               whole={str(missing)}, stop=stop)

    assert len(copier.errors) == 1
    leftovers = list((tmp_path / "stage").rglob("*.partial"))
    assert leftovers == []
