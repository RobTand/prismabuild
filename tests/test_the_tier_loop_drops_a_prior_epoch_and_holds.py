"""The loop drops a prior epoch on its first cycle, and holds until fresh
ranges land.

tmpfs empties on reboot; the fragments and the ledger on the shared mount do
not.  Without an explicit drop, the first cycle after a reboot would compose
a map that still names ram paths whose bytes are gone, and the ledger would
still count tokens for ranges that no longer exist -- the new window starved
by ghosts, which is the one failure the direction names ("starvation is the
failure to avoid").

So the epoch the tier announces is the only epoch that counts.  On the first
cycle after a change the loop unlinks every ram fragment that carries a
different one, releases the ghost tokens of held keys whose receipts name a
different one (their bytes were deleted by the reboot, not by an egress), and
composes from what survives -- which names no ram range at all, so nothing
reads as ram-resident until a promotion lands under the new epoch.  A
mount that is gone entirely is the same rule one step further: there is no
current epoch, so every ram fragment is a prior one (#640).
"""
from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
from prismabuild import pool, residency_map, storage_tiers  # noqa: E402

import tier_loop  # noqa: E402

CONSUMER = "c" * 64
STAGE_MOVER = "1" * 64
RAM_MOVER = "2" * 64
RAM_MOVER_NEW = "3" * 64
MANIFEST = "9" * 64
STAGE_TIER = "prismabuild-stage:dl380g10"
RAM_TIER = "ram:dl380g10"
STAGE_ROOT = "/stage/prewarm"
GIB = storage_tiers.GIB
OLD_EPOCH = "1695000000-deadbeefdeadbeef"
NEW_EPOCH = "1695052800-1a2b3c4d5e6f7a8b"
ENTRY_KEY = residency_map.residency_map_key("/mnt/shared/model/shard-0.bin", 0)


def _fragment(*, tier_id: str, root: str, mover: str,
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


@pytest.fixture()
def queue(tmp_path: Path) -> pool.PoolQueue:
    q = pool.PoolQueue(tmp_path / "pb-queue")
    q.ensure_layout()
    return q


def _ram_resident(queue: pool.PoolQueue, tmp_path: Path, *, epoch: str,
                  mover: str = RAM_MOVER) -> None:
    """A finished ram promotion: tokens held, receipt filed, fragment written."""

    queue.mint_tier_capacity(RAM_TIER, {"ram_gib": 8})
    assert queue.tier_ledger(RAM_TIER).acquire(mover, {"ram_gib": 2})
    queue.record_move(mover, {
        "consumer_action_key": CONSUMER, "tier_id": RAM_TIER,
        "ram_root": str(tmp_path / "ram"), "manifest_sha256": MANIFEST,
        "range_start_bytes": 0, "range_end_bytes": 2 * GIB,
        "bytes_staged": 2 * GIB, "complete": True, "epoch": epoch,
        "seconds": 1.0, "unix": 1000.0})
    residency_map.write_fragment(
        queue.root / pool.RESIDENCY,
        _fragment(tier_id=RAM_TIER, root=str(tmp_path / "ram"), mover=mover,
                  epoch=epoch))


def _ram_record(tmp_path: Path, *, epoch: str) -> dict[str, object]:
    return {
        "schema": storage_tiers.TIER_RECORD_SCHEMA_V1, "tier": "ram",
        "tier_id": RAM_TIER, "host": "dl380g10",
        "mountpoint": str(tmp_path / "ram"), "epoch": epoch,
        "capacity_bytes": 110 * GIB, "ceiling_bytes": 256 * GIB,
        "window_gib": 112,
        "ram_admission": {"admissible": True, "reason": None},
        "mount_options": ["rw", "noswap", "size=256G"],
    }


def test_a_prior_epoch_is_dropped_and_holds_until_fresh_ranges_land(
        queue: pool.PoolQueue, tmp_path: Path) -> None:
    residency_map.write_fragment(
        queue.root / pool.RESIDENCY,
        _fragment(tier_id=STAGE_TIER, root=STAGE_ROOT, mover=STAGE_MOVER))
    _ram_resident(queue, tmp_path, epoch=OLD_EPOCH)
    record = _ram_record(tmp_path, epoch=NEW_EPOCH)

    events = tier_loop.drop_prior_ram_epochs(queue, {RAM_TIER: record})

    assert [event["event"] for event in events] == [
        "ram-fragment-dropped", "ram-ghost-tokens-released"]
    assert events[0]["epoch"] == OLD_EPOCH
    assert events[1]["holder"] == RAM_MOVER
    # The prior epoch's fragment is gone; the stage's own is untouched.
    assert not residency_map.fragment_path(
        queue.root / pool.RESIDENCY, CONSUMER, RAM_MOVER).exists()
    assert residency_map.fragment_path(
        queue.root / pool.RESIDENCY, CONSUMER, STAGE_MOVER).exists()
    # The ghost's tokens are back: the bytes they stood for were deleted by
    # the reboot, and holding them would starve the new window.
    assert queue.tier_ledger(RAM_TIER).holder_tokens(RAM_MOVER) == {}
    # Hold: nothing reads as ram-resident until fresh ranges land.
    tier_loop.compose_map(queue, CONSUMER, ram_tiers={RAM_TIER: record})
    mapping = residency_map.read_map(queue.residency_map_path(CONSUMER))
    assert "ram_path" not in mapping["entries"][ENTRY_KEY]

    # ...and a promotion under the new epoch is announced resident again.
    residency_map.write_fragment(
        queue.root / pool.RESIDENCY,
        _fragment(tier_id=RAM_TIER, root=str(tmp_path / "ram"),
                  mover=RAM_MOVER_NEW, epoch=NEW_EPOCH))
    tier_loop.compose_map(queue, CONSUMER, ram_tiers={RAM_TIER: record})
    mapping = residency_map.read_map(queue.residency_map_path(CONSUMER))
    assert mapping["entries"][ENTRY_KEY]["ram_path"] == (
        f"{tmp_path}/ram/model/shard-0.bin")


def test_a_mount_that_is_gone_has_no_current_epoch(
        queue: pool.PoolQueue, tmp_path: Path) -> None:
    """Every ram fragment is a prior one when no ram tier is announced."""

    _ram_resident(queue, tmp_path, epoch=NEW_EPOCH)

    events = tier_loop.drop_prior_ram_epochs(queue, {})

    assert [event["event"] for event in events] == [
        "ram-fragment-dropped", "ram-ghost-tokens-released"]
    assert not residency_map.fragment_path(
        queue.root / pool.RESIDENCY, CONSUMER, RAM_MOVER).exists()
    assert queue.tier_ledger(RAM_TIER).holder_tokens(RAM_MOVER) == {}


def test_a_current_epoch_fragment_and_holder_are_left_alone(
        queue: pool.PoolQueue, tmp_path: Path) -> None:
    _ram_resident(queue, tmp_path, epoch=NEW_EPOCH)

    events = tier_loop.drop_prior_ram_epochs(
        queue, {RAM_TIER: _ram_record(tmp_path, epoch=NEW_EPOCH)})

    assert events == []
    assert residency_map.fragment_path(
        queue.root / pool.RESIDENCY, CONSUMER, RAM_MOVER).exists()
    assert queue.tier_ledger(RAM_TIER).holder_tokens(RAM_MOVER) == {
        "ram_gib": 2}


def test_the_cycle_says_the_epoch_changed_and_does_the_drop(
        queue: pool.PoolQueue, tmp_path: Path, capsys) -> None:
    """The first cycle after a reboot: one event, then the drop, then a world
    that no longer contains the prior epoch.  The announced record from the
    cycle before is what the new epoch is compared against."""

    (tmp_path / "ram").mkdir()
    queue.announce_tier(_ram_record(tmp_path, epoch=OLD_EPOCH))
    residency_map.write_fragment(
        queue.root / pool.RESIDENCY,
        _fragment(tier_id=STAGE_TIER, root=STAGE_ROOT, mover=STAGE_MOVER))
    _ram_resident(queue, tmp_path, epoch=OLD_EPOCH)

    def discover(**_kwargs):
        return {RAM_TIER: _ram_record(tmp_path, epoch=NEW_EPOCH)}

    tier_loop.cycle(queue, host="dl380g10", source_pool="storage_pool",
                    receipts=tier_loop.ReceiptCache(), discover=discover)

    lines = [json.loads(line) for line in capsys.readouterr().out.splitlines()
             if line.startswith("{")]
    changed = [line for line in lines if line.get("event") == "ram-epoch-changed"]
    assert changed and changed[0]["epoch"] == NEW_EPOCH
    assert changed[0]["previous_epoch"] == OLD_EPOCH
    assert any(line.get("event") == "ram-fragment-dropped" for line in lines)
    assert any(line.get("event") == "ram-ghost-tokens-released"
               for line in lines)
    assert not residency_map.fragment_path(
        queue.root / pool.RESIDENCY, CONSUMER, RAM_MOVER).exists()
    assert queue.tier_ledger(RAM_TIER).holder_tokens(RAM_MOVER) == {}
    # The new epoch is what the loop announced, and the retired fragments are
    # gone: nothing of the prior epoch survived the cycle.
    record = {str(r["tier_id"]): r for r in queue.tiers()}[RAM_TIER]
    assert record["epoch"] == NEW_EPOCH
