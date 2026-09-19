"""A mover that fails leaves its landed bytes on the stage, held by nobody;
nothing frees them (#627).

When a mover ends without a complete receipt -- ENOSPC part-way, a lost
``.partial`` rename, a kill -- its tokens go back at ``finish``
(``residency_pin_holds``: "everything else releases, because everything else
left nothing behind").  But it did leave something behind: every entry it
renamed into place before it failed.  Those bytes stay on the dataset, reduce
ZFS ``available``, appear in its fragment (so the composed map still names
them), and are counted by no ledger token.  Nothing publishes an egress for
them: the window evicts only phases the consumer has read past, and the
orphan sweep takes back movers no live plan names.

The fix: the tier loop treats a terminal, unpinned mover that still names
bytes in a fragment as an eviction candidate whenever the window has no room
for the next phase -- it publishes that phase's egress row, which already
handles "an earlier egress removed it" and returns no tokens when none are
held.  While the egress is queued the window does not republish the mover
under it, so the recopy waits for the room instead of ENOSPCing into it.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
from prismabuild import pool, residency_map, residency_plan, storage_tiers  # noqa: E402
import tier_loop  # noqa: E402

TIER = "prismabuild-stage:dl380g10"
STAGE_KIND = f"stage_gib@{TIER}"
MANIFEST = "9" * 64
GIB = storage_tiers.GIB
PHASE_GIB = 2
CONSUMER = "2" * 64


def _hexkey(seed: str) -> str:
    return (seed.encode().hex() * 64)[:64]


def _row(queue: pool.PoolQueue, key: str,
         resources: dict[str, int]) -> dict[str, object]:
    return {"action_key": key, "cas_root": str(queue.root / "cas"),
            "checkout_root": str(queue.root / "co"),
            "worker_script": str(queue.root / "worker.py"),
            "tags": ["dl380g10"], "resources": resources}


def _plan(queue: pool.PoolQueue, *, phases: int = 2) -> dict[str, object]:
    built = []
    for ordinal in range(phases):
        start, end = ordinal * PHASE_GIB * GIB, (ordinal + 1) * PHASE_GIB * GIB
        built.append({
            "name": f"phase-{ordinal}",
            "start_bytes": start, "end_bytes": end, "stage_gib": PHASE_GIB,
            "mover_row": {
                **_row(queue, _hexkey(f"mover{ordinal}"),
                       {STAGE_KIND: PHASE_GIB, "cpu": 1, "mem_gb": 1}),
                "residency": {
                    "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                    "manifest_sha256": MANIFEST, "manifest_bytes": 1 << 30,
                    "range_start_bytes": start, "range_end_bytes": end},
            },
            "egress_row": _row(queue, _hexkey(f"egress{ordinal}"),
                               {"mem_gb": 1}),
        })
    return residency_plan.build_plan(
        consumer_action_key=CONSUMER, tier_id=TIER, stage_root="/stage/prewarm",
        manifest_sha256=MANIFEST, manifest_bytes=1 << 30, phases=built)


def _publish_consumer(queue: pool.PoolQueue, plan: dict[str, object]) -> None:
    residency_plan.freeze(queue, plan)
    queue.publish(
        action_key=CONSUMER, cas_root=queue.root / "cas",
        checkout_root=queue.root / "co", worker_script=queue.root / "worker.py",
        resources={"cpu": 1, "mem_gb": 1},
        residency={"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                   "manifest_sha256": MANIFEST, "manifest_bytes": 1 << 30,
                   "leads": residency_plan.leads_for(plan)})


@pytest.fixture()
def staged(tmp_path: Path):
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    stage = tmp_path / "stage"
    stage.mkdir()
    return queue, stage


def _tier_record() -> dict[str, object]:
    return {"schema": storage_tiers.TIER_RECORD_SCHEMA_V1, "tier": "stage",
            "tier_id": TIER, "host": "dl380g10"}


def _fail_mover(queue: pool.PoolQueue, stage: Path, *,
                mover: str, consumer: str = CONSUMER,
                ordinal: int = 0) -> list[Path]:
    """The ENOSPC state: entries renamed into place, an incomplete receipt,
    tokens already back, and the fragment still naming the bytes."""

    entries: dict[str, object] = {}
    written: list[Path] = []
    for index in range(2):
        path = stage / f"phase-{ordinal}" / f"part-{index}.bin"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"x" * 32)
        written.append(path)
        entries[residency_map.residency_map_key(
            f"/pool/phase-{ordinal}/part-{index}.bin", 0)] = {
                "stage_path": str(path), "bytes": 32, "offset": 0,
                "sha256": "b" * 64}
    queue.record_move(mover, {
        "consumer_action_key": consumer, "tier_id": TIER,
        "stage_root": str(stage), "manifest_sha256": MANIFEST,
        "range_start_bytes": ordinal * PHASE_GIB * GIB,
        "range_end_bytes": (ordinal + 1) * PHASE_GIB * GIB,
        "bytes_staged": 32, "entries_declared": 100, "entries_staged": 2,
        "complete": False, "seconds": 10.0, "unix": 1000.0,
        "errors": ["No space left on device"]})
    residency_map.write_fragment(queue.residency_fragment_root(), {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": consumer, "mover_action_key": mover,
        "tier_id": TIER, "stage_root": str(stage), "manifest_sha256": MANIFEST,
        "entries": entries})
    assert not queue.tier_ledger(TIER).holder_tokens(mover)
    return written


def _cycle(queue: pool.PoolQueue, stage: Path,
           pressure: dict[str, int]) -> list[dict[str, object]]:
    consumers = tier_loop._planned_consumers(queue, {TIER: _tier_record()})
    return tier_loop.reclaim_failed_mover_partials(
        queue, consumers, pressure)


def test_a_failed_movers_partials_are_evicted_before_the_recopy(staged) -> None:
    """RED before #627: the window republished the mover and left the partials."""

    queue, stage = staged
    plan = _plan(queue)
    _publish_consumer(queue, plan)
    mover0 = _hexkey("mover0")
    _fail_mover(queue, stage, mover=mover0)

    events = _cycle(queue, stage, pressure={TIER: PHASE_GIB})

    egress0 = _hexkey("egress0")
    assert queue.item_path(pool.READY, egress0).exists(), (
        "the failed mover's own egress row is the eviction candidate")
    assert [event["mover"] for event in events
            if event.get("event") == "failed-mover-egress-published"] == [mover0]


def test_no_pressure_means_no_reclaim(staged) -> None:
    """An orphan's partials are a cache until a window cannot be placed."""

    queue, stage = staged
    plan = _plan(queue)
    _publish_consumer(queue, plan)
    _fail_mover(queue, stage, mover=_hexkey("mover0"))
    consumers = tier_loop._planned_consumers(queue, {TIER: _tier_record()})

    events = tier_loop.reclaim_failed_mover_partials(
        queue, consumers, pressure={})

    assert events == []
    assert not queue.item_path(pool.READY, _hexkey("egress0")).exists()


def test_a_queued_mover_is_not_reclaimed_from_under_itself(staged) -> None:
    """The recopy already running is the owner; the cleanup declines."""

    queue, stage = staged
    plan = _plan(queue)
    _publish_consumer(queue, plan)
    mover0 = _hexkey("mover0")
    _fail_mover(queue, stage, mover=mover0)
    queue.publish(**plan["phases"][0]["mover_row"], recompute=True)
    consumers = tier_loop._planned_consumers(queue, {TIER: _tier_record()})

    events = tier_loop.reclaim_failed_mover_partials(
        queue, consumers, pressure={TIER: PHASE_GIB})

    assert events == []
    assert not queue.item_path(pool.READY, _hexkey("egress0")).exists()


def test_a_completed_mover_is_not_reclaimed(staged) -> None:
    """A complete receipt is a resident range: adoption's or the sweep's."""

    queue, stage = staged
    plan = _plan(queue)
    _publish_consumer(queue, plan)
    mover0 = _hexkey("mover0")
    _fail_mover(queue, stage, mover=mover0)
    receipt = queue.move_record(mover0)
    assert isinstance(receipt, dict)
    queue.record_move(mover0, {**receipt, "complete": True, "errors": []})
    consumers = tier_loop._planned_consumers(queue, {TIER: _tier_record()})

    events = tier_loop.reclaim_failed_mover_partials(
        queue, consumers, pressure={TIER: PHASE_GIB})

    assert events == []
    assert not queue.item_path(pool.READY, _hexkey("egress0")).exists()


def test_a_concluded_egress_is_not_published_again(staged) -> None:
    """The egress ran and the fragment is still there: refuse, do not spin."""

    queue, stage = staged
    plan = _plan(queue)
    _publish_consumer(queue, plan)
    mover0 = _hexkey("mover0")
    _fail_mover(queue, stage, mover=mover0)
    egress0 = _hexkey("egress0")
    queue.item_path(pool.DONE, egress0).write_text(json.dumps({
        "action_key": egress0, "status": "executed"}))
    consumers = tier_loop._planned_consumers(queue, {TIER: _tier_record()})

    events = tier_loop.reclaim_failed_mover_partials(
        queue, consumers, pressure={TIER: PHASE_GIB})

    assert events == []
    assert not queue.item_path(pool.READY, egress0).exists()


def test_the_window_holds_the_recopy_while_its_egress_is_queued(
        staged) -> None:
    """The egress frees device bytes, not ledger tokens: without the hold the
    window republishes the recopy into a stage that is still full."""

    queue, stage = staged
    plan = _plan(queue)
    _publish_consumer(queue, plan)
    mover0 = _hexkey("mover0")
    partials = _fail_mover(queue, stage, mover=mover0)
    queue.publish(**plan["phases"][0]["egress_row"], recompute=True)
    queue.mint_tier_capacity(TIER, {"stage_gib": 10})

    events = tier_loop.residency_window(queue, tiers={TIER: _tier_record()})

    published = [event["action_key"] for event in events
                 if event.get("event") == "mover-published"]
    assert mover0 not in published
    assert all(path.exists() for path in partials), (
        "the window publishes nothing over bytes only an egress may delete")
