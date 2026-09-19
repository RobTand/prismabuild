"""An incomplete ram promotion releases its tokens instead of stranding them.

A promotion that lands partially files a fragment for the landed subset while
the ledger holds ``ram_gib`` for the *declared* range -- and then
``_ram_mover_state`` counts the pinned-but-failed key as staged/published, so
the window never republishes it, no egress fires for a phase the consumer has
not passed, and the orphan sweep (which takes only orphans, and a live
consumer's promotion is not one) never takes it back.  The half-landed range
squats on its full-range tokens until its phase passes or an operator
intervenes (#644).

Refuse-and-release: a terminal promotion whose receipt says anything but a
clean landing is evicted through the egress's own read-delete-release, so the
partial files go before the tokens come back and the next window republishes
the whole range.  The event carries the receipt's errors -- the alert the
issue asks for at minimum.  Anything queued, running, or unreadable stays
held: fail closed.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
from prismabuild import pool, residency_map, residency_plan, storage_tiers  # noqa: E402

import stage_release  # noqa: E402
import tier_loop  # noqa: E402

CONSUMER = "c" * 64
MOVER = "a" * 64
OTHER = "b" * 64
MANIFEST = "9" * 64
STAGE_TIER = "prismabuild-stage:dl380g10"
RAM_TIER = "ram:dl380g10"
RAM_KIND = f"ram_gib@{RAM_TIER}"
GIB = storage_tiers.GIB
EPOCH = "1695052800-1a2b3c4d5e6f7a8b"
ENTRY_KEY = residency_map.residency_map_key("/mnt/shared/model/shard-0.bin", 0)
ENTRY_SHA = "b" * 64


def _hexkey(seed: str) -> str:
    return (seed.encode().hex() * 64)[:64]


@pytest.fixture()
def queue(tmp_path: Path) -> pool.PoolQueue:
    q = pool.PoolQueue(tmp_path / "pb-queue")
    q.ensure_layout()
    return q


def _ram_root(tmp_path: Path, queue: pool.PoolQueue) -> Path:
    """A ram mountpoint this queue owns, so the evict is not refused (#628)."""

    ram = tmp_path / "ram"
    ram.mkdir(exist_ok=True)
    assert stage_release.register_stage_root(
        queue, tier_id=RAM_TIER, stage_root=str(ram)) == "registered"
    return ram


def _tiers(ram: Path) -> dict[str, dict[str, object]]:
    return {RAM_TIER: {"tier": "ram", "tier_id": RAM_TIER, "host": "dl380g10",
                       "mountpoint": str(ram), "epoch": EPOCH}}


def _hold(queue: pool.PoolQueue, key: str, gib: int) -> None:
    queue.mint_tier_capacity(RAM_TIER, {"ram_gib": 8})
    assert queue.tier_ledger(RAM_TIER).acquire(key, {"ram_gib": gib})


def _file_partial(tmp_path: Path, queue: pool.PoolQueue, *, mover: str,
                  staged_gib: int) -> None:
    """The landed subset: bytes on the tmpfs and a fragment naming them.

    The file itself is small: the test is about the token/fragment/file
    lifecycle, not the copy bandwidth, and the ledger counts tokens while the
    evict deletes whatever the fragment names.
    """

    ram = tmp_path / "ram"
    target = ram / "model" / "shard-0.bin"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(b"x" * 4096)
    residency_map.write_fragment(queue.root / pool.RESIDENCY, {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": CONSUMER, "mover_action_key": mover,
        "tier_id": RAM_TIER, "stage_root": str(ram), "epoch": EPOCH,
        "manifest_sha256": MANIFEST,
        "entries": {ENTRY_KEY: {
            "stage_path": str(target), "bytes": staged_gib * GIB,
            "offset": 0, "sha256": ENTRY_SHA}},
    })


def _receipt(queue: pool.PoolQueue, key: str, *, staged_gib: int,
             complete: bool, errors: list[str] | None = None,
             refusal: str | None = None) -> None:
    record: dict[str, object] = {
        "consumer_action_key": CONSUMER, "tier_id": RAM_TIER,
        "ram_root": "/ram", "manifest_sha256": MANIFEST,
        "range_start_bytes": 0, "range_end_bytes": 2 * GIB,
        "bytes_staged": staged_gib * GIB, "complete": complete,
        "epoch": EPOCH, "seconds": 1.0, "unix": 1000.0,
        "errors": errors if errors is not None else [],
    }
    if refusal is not None:
        record["refusal"] = refusal
    queue.record_move(key, record)


def test_a_half_landed_promotion_returns_its_tokens_and_its_files(
        queue: pool.PoolQueue, tmp_path: Path) -> None:
    """#644: the ENOSPC shape -- one entry landed, one refused, full-range
    tokens held.  The release deletes the partial files first (tokens stand
    for bytes), drops the fragment, and names the receipt's errors."""

    ram = _ram_root(tmp_path, queue)
    _hold(queue, MOVER, 2)
    _file_partial(tmp_path, queue, mover=MOVER, staged_gib=1)
    _receipt(queue, MOVER, staged_gib=1, complete=False,
             errors=["shard-1.bin: [Errno 28] No space left on device"])

    events = tier_loop.release_incomplete_ram_promotions(
        queue, tiers=_tiers(ram))

    assert [event["event"] for event in events] == [
        "ram-mover-incomplete-released"]
    assert events[0]["mover"] == MOVER
    assert events[0]["receipt_errors"] == [
        "shard-1.bin: [Errno 28] No space left on device"]
    assert events[0]["tokens_released"] == 2
    assert queue.tier_ledger(RAM_TIER).holder_tokens(MOVER) == {}
    assert not residency_map.fragment_path(
        queue.root / pool.RESIDENCY, CONSUMER, MOVER).exists()
    assert not (ram / "model" / "shard-0.bin").exists()


def test_a_refused_promotion_with_no_fragment_still_returns_its_tokens(
        queue: pool.PoolQueue, tmp_path: Path) -> None:
    """Refused before a byte was copied: nothing to delete, tokens still held
    past finish, same release -- a fragment that was never written cannot be
    what returns them."""

    ram = _ram_root(tmp_path, queue)
    _hold(queue, MOVER, 2)
    _receipt(queue, MOVER, staged_gib=0, complete=False,
             refusal="ram_source_stage_absent")

    events = tier_loop.release_incomplete_ram_promotions(
        queue, tiers=_tiers(ram))

    assert [event["event"] for event in events] == [
        "ram-mover-incomplete-released"]
    assert queue.tier_ledger(RAM_TIER).holder_tokens(MOVER) == {}


def test_a_clean_landing_is_occupancy_not_stranding(
        queue: pool.PoolQueue, tmp_path: Path) -> None:
    """A complete promotion with no refusal and no errors is left alone: its
    tokens stand for bytes on the tmpfs, and only an egress returns them."""

    ram = _ram_root(tmp_path, queue)
    _hold(queue, MOVER, 2)
    _file_partial(tmp_path, queue, mover=MOVER, staged_gib=2)
    _receipt(queue, MOVER, staged_gib=2, complete=True)

    events = tier_loop.release_incomplete_ram_promotions(
        queue, tiers=_tiers(ram))

    assert events == []
    assert queue.tier_ledger(RAM_TIER).holder_tokens(MOVER) == {"ram_gib": 2}
    assert residency_map.fragment_path(
        queue.root / pool.RESIDENCY, CONSUMER, MOVER).exists()


def test_queued_and_running_promotions_are_not_judged(
        queue: pool.PoolQueue, tmp_path: Path) -> None:
    """A key the window published but nothing claimed yet, and a key whose
    copy is running now, both keep their tokens: reading their receipts --
    or their absence -- would race the mover filing them."""

    ram = _ram_root(tmp_path, queue)
    _hold(queue, MOVER, 2)
    _hold(queue, OTHER, 2)
    queue.item_path(pool.READY, MOVER).write_text("{}")
    queue.item_path(pool.CLAIMED, OTHER).write_text("{}")
    _receipt(queue, MOVER, staged_gib=0, complete=False,
             errors=["shard-0.bin: [Errno 28] No space left on device"])

    events = tier_loop.release_incomplete_ram_promotions(
        queue, tiers=_tiers(ram))

    assert events == []
    assert queue.tier_ledger(RAM_TIER).holder_tokens(MOVER) == {"ram_gib": 2}
    assert queue.tier_ledger(RAM_TIER).holder_tokens(OTHER) == {"ram_gib": 2}


def test_a_holder_with_no_receipt_keeps_its_tokens(
        queue: pool.PoolQueue, tmp_path: Path) -> None:
    """Fail closed: bytes this step cannot date are not bytes it releases."""

    ram = _ram_root(tmp_path, queue)
    _hold(queue, MOVER, 2)

    events = tier_loop.release_incomplete_ram_promotions(
        queue, tiers=_tiers(ram))

    assert events == []
    assert queue.tier_ledger(RAM_TIER).holder_tokens(MOVER) == {"ram_gib": 2}


def _row(key: str, resources: dict[str, int], queue: pool.PoolQueue) -> dict:
    return {"action_key": key, "cas_root": str(queue.root / "cas"),
            "checkout_root": str(queue.root / "co"),
            "worker_script": str(queue.root / "worker.py"),
            "tags": ["dl380g10"], "resources": resources}


def _plan(queue: pool.PoolQueue) -> dict[str, object]:
    gib = 2
    end = gib * GIB
    return residency_plan.build_plan(
        consumer_action_key=CONSUMER, tier_id=STAGE_TIER,
        stage_root="/stage/prewarm", manifest_sha256=MANIFEST,
        manifest_bytes=end, phases=[{
            "name": "phase-0000", "start_bytes": 0, "end_bytes": end,
            "stage_gib": gib,
            "mover_row": {
                **_row(_hexkey("mover0"), {f"stage_gib@{STAGE_TIER}": gib,
                                           "mem_gb": 1}, queue),
                "residency": {
                    "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": STAGE_TIER,
                    "manifest_sha256": MANIFEST, "manifest_bytes": end,
                    "range_start_bytes": 0, "range_end_bytes": end}},
            "egress_row": _row(_hexkey("egress0"), {"mem_gb": 1}, queue),
            "ram_mover_row": {
                **_row(MOVER, {RAM_KIND: gib, "mem_gb": 1}, queue),
                "residency": {
                    "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": RAM_TIER,
                    "manifest_sha256": MANIFEST, "manifest_bytes": end,
                    "range_start_bytes": 0, "range_end_bytes": end}},
            "ram_egress_row": _row(_hexkey("ramrelease0"), {"mem_gb": 1},
                                   queue),
        }], ram_tier_id=RAM_TIER)


def _ram_record(ram: Path) -> dict[str, object]:
    return {
        "schema": storage_tiers.TIER_RECORD_SCHEMA_V1, "tier": "ram",
        "tier_id": RAM_TIER, "host": "dl380g10", "mountpoint": str(ram),
        "epoch": EPOCH, "capacity_bytes": 110 * GIB, "ceiling_bytes": 256 * GIB,
        "window_gib": 112,
        "ram_admission": {"admissible": True, "reason": None},
        "mount_options": ["rw", "noswap", "size=256G"],
    }


def test_the_cycle_releases_a_live_consumer_stranded_promotion(
        queue: pool.PoolQueue, tmp_path: Path, capsys) -> None:
    """End to end through ``cycle``: the consumer is live, so the sweep would
    *not* take this key -- it is wanted, not orphaned -- and only the release
    returns its tokens.  The key is left terminal and unpinned, which is what
    the window republishes through the ordinary publish path, so no retry row
    is needed."""

    ram = _ram_root(tmp_path, queue)
    plan = _plan(queue)
    residency_plan.freeze(queue, plan)
    queue.publish(**_row(CONSUMER, {"mem_gb": 1}, queue), residency={
        "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": STAGE_TIER,
        "manifest_sha256": MANIFEST, "manifest_bytes": 2 * GIB,
        "leads": residency_plan.leads_for(plan)})
    _hold(queue, MOVER, 2)
    _file_partial(tmp_path, queue, mover=MOVER, staged_gib=1)
    _receipt(queue, MOVER, staged_gib=1, complete=False,
             errors=["shard-1.bin: [Errno 28] No space left on device"])

    tier_loop.cycle(queue, host="dl380g10", source_pool="storage_pool",
                    receipts=tier_loop.ReceiptCache(),
                    discover=lambda **_kwargs: {RAM_TIER: _ram_record(ram)})

    lines = [json.loads(line) for line in capsys.readouterr().out.splitlines()
             if line.startswith("{")]
    released = [line for line in lines
                if line.get("event") == "ram-mover-incomplete-released"]
    assert len(released) == 1
    assert released[0]["receipt_errors"] == [
        "shard-1.bin: [Errno 28] No space left on device"]
    # The sweep did not take it: a live consumer's promotion is wanted, and
    # the orphan path never fired for this key.
    assert not [line for line in lines
                if line.get("event") == "stage-orphan-evicted"
                and MOVER in json.dumps(line)]
    assert queue.tier_ledger(RAM_TIER).holder_tokens(MOVER) == {}
    assert not residency_map.fragment_path(
        queue.root / pool.RESIDENCY, CONSUMER, MOVER).exists()
