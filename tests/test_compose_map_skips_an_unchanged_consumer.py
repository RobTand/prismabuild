"""``compose_map`` skips a consumer whose inputs did not move (#604).

The loop runs every 60 s over every live consumer, and the compose was
unconditional: a read of every fragment plus an ``os.replace`` on the shared
mount per consumer per cycle, growing with phases times consumers.  The fix
keys on what the map is a function of -- the fragment set and mtimes (fragments
are written once by rename and never mutated) joined with the ram overlay's
inputs -- the same (path, mtime) idiom ``ReceiptCache`` already uses in the
same module.  An unchanged consumer keeps its map; anything unreadable
composes rather than skips, and a map that went missing is rewritten.
"""
from __future__ import annotations

from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
from prismabuild import pool, residency_map  # noqa: E402

import tier_loop  # noqa: E402

CONSUMER = "c" * 64
OTHER_CONSUMER = "d" * 64
STAGE_TIER = "prismabuild-stage:dl380g10"
RAM_TIER = "ram:dl380g10"
STAGE_ROOT = "/stage/prewarm"
MANIFEST = "9" * 64
EPOCH = "1695052800-1a2b3c4d5e6f7a8b"
ENTRY_SHA = "b" * 64


def _hexkey(seed: str) -> str:
    return (seed.encode().hex() * 64)[:64]


@pytest.fixture()
def queue(tmp_path: Path) -> pool.PoolQueue:
    tier_loop._COMPOSE_FINGERPRINTS.clear()
    q = pool.PoolQueue(tmp_path / "pb-queue")
    q.ensure_layout()
    return q


def _stage_fragment(consumer: str, mover: str, name: str) -> dict[str, object]:
    key = residency_map.residency_map_key(f"/mnt/shared/model/{name}", 0)
    return {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": consumer, "mover_action_key": mover,
        "tier_id": STAGE_TIER, "stage_root": STAGE_ROOT,
        "manifest_sha256": MANIFEST,
        "entries": {key: {
            "stage_path": f"{STAGE_ROOT}/model/{name}", "bytes": 4096,
            "offset": 0, "sha256": ENTRY_SHA}},
    }


def _ram_fragment(consumer: str, mover: str, name: str,
                  ram_root: str) -> dict[str, object]:
    key = residency_map.residency_map_key(f"/mnt/shared/model/{name}", 0)
    return {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": consumer, "mover_action_key": mover,
        "tier_id": RAM_TIER, "stage_root": ram_root, "epoch": EPOCH,
        "manifest_sha256": MANIFEST,
        "entries": {key: {
            "stage_path": f"{ram_root}/model/{name}", "bytes": 4096,
            "offset": 0, "sha256": ENTRY_SHA}},
    }


def _write(queue: pool.PoolQueue, fragment: dict[str, object]) -> None:
    residency_map.write_fragment(queue.root / pool.RESIDENCY, fragment)


def _stat(path: Path) -> tuple[int, int]:
    status = path.stat()
    return (status.st_ino, status.st_mtime_ns)


def test_an_unchanged_consumer_keeps_its_map(
        queue: pool.PoolQueue) -> None:
    """The second compose over identical inputs is a stat, not a rewrite:
    same inode, same mtime, same content."""

    _write(queue, _stage_fragment(CONSUMER, _hexkey("mover0"), "shard-0.bin"))
    first = tier_loop.compose_map(queue, CONSUMER)
    assert first is not None
    before = _stat(first)
    assert residency_map.read_map(first)["generation"] == 1

    second = tier_loop.compose_map(queue, CONSUMER)
    assert second == first
    assert _stat(second) == before


def test_a_new_fragment_recomposes(queue: pool.PoolQueue) -> None:
    """A mover finishing between cycles changes the set, so the map is
    rewritten and names the new lead."""

    _write(queue, _stage_fragment(CONSUMER, _hexkey("mover0"), "shard-0.bin"))
    first = tier_loop.compose_map(queue, CONSUMER)
    assert first is not None
    before = _stat(first)

    _write(queue, _stage_fragment(CONSUMER, _hexkey("mover1"), "shard-1.bin"))
    second = tier_loop.compose_map(queue, CONSUMER)
    assert second == first
    assert _stat(second) != before
    mapping = residency_map.read_map(second)
    assert mapping["generation"] == 2
    assert set(mapping["leads"]) == {_hexkey("mover0"), _hexkey("mover1")}

    # ...and the cycle after that is quiet again.
    third = tier_loop.compose_map(queue, CONSUMER)
    assert _stat(third) == _stat(second)


def test_a_removed_fragment_recomposes(queue: pool.PoolQueue) -> None:
    """An egress deleting a range changes the set the other way; the map
    follows it rather than serving the skip."""

    _write(queue, _stage_fragment(CONSUMER, _hexkey("mover0"), "shard-0.bin"))
    _write(queue, _stage_fragment(CONSUMER, _hexkey("mover1"), "shard-1.bin"))
    first = tier_loop.compose_map(queue, CONSUMER)
    assert first is not None
    assert residency_map.read_map(first)["generation"] == 2

    residency_map.fragment_path(
        queue.root / pool.RESIDENCY, CONSUMER,
        _hexkey("mover1")).unlink()
    second = tier_loop.compose_map(queue, CONSUMER)
    mapping = residency_map.read_map(second)
    assert mapping["generation"] == 1
    assert mapping["leads"] == [_hexkey("mover0")]


def test_a_missing_map_is_rewritten(queue: pool.PoolQueue) -> None:
    """A map deleted under an unchanged fingerprint is recomposed, not
    trusted from memory."""

    _write(queue, _stage_fragment(CONSUMER, _hexkey("mover0"), "shard-0.bin"))
    first = tier_loop.compose_map(queue, CONSUMER)
    assert first is not None
    tier_loop.compose_map(queue, CONSUMER)  # remembered as written
    first.unlink()

    second = tier_loop.compose_map(queue, CONSUMER)
    assert second == first
    assert second.exists()
    assert residency_map.read_map(second)["generation"] == 1


def test_an_empty_consumer_stays_empty(queue: pool.PoolQueue) -> None:
    """No fragments, no map -- and the repeat costs nothing either."""

    assert tier_loop.compose_map(queue, CONSUMER) is None
    assert tier_loop.compose_map(queue, CONSUMER) is None
    assert not queue.residency_map_path(CONSUMER).exists()


def test_consumers_are_independent(queue: pool.PoolQueue) -> None:
    """One consumer's movement never stills another's map."""

    _write(queue, _stage_fragment(CONSUMER, _hexkey("mover0"), "shard-0.bin"))
    _write(queue, _stage_fragment(
        OTHER_CONSUMER, _hexkey("othermover0"), "shard-9.bin"))
    first = tier_loop.compose_map(queue, CONSUMER)
    other = tier_loop.compose_map(queue, OTHER_CONSUMER)
    assert first is not None and other is not None
    before = _stat(first)

    _write(queue, _stage_fragment(
        OTHER_CONSUMER, _hexkey("othermover1"), "shard-8.bin"))
    tier_loop.compose_map(queue, OTHER_CONSUMER)
    assert _stat(tier_loop.compose_map(queue, CONSUMER)) == before


def test_the_ram_overlay_inputs_participate(
        queue: pool.PoolQueue, tmp_path: Path) -> None:
    """The same fragments under a new epoch are a different map: the overlay
    header changes, so the fingerprint must too."""

    ram_root = str(tmp_path / "ram")
    _write(queue, _stage_fragment(CONSUMER, _hexkey("mover0"), "shard-0.bin"))
    _write(queue, _ram_fragment(CONSUMER, _hexkey("ram0"), "shard-0.bin",
                                ram_root))
    tiers = {RAM_TIER: {"tier": "ram", "tier_id": RAM_TIER,
                        "mountpoint": ram_root, "epoch": EPOCH}}
    first = tier_loop.compose_map(queue, CONSUMER, ram_tiers=tiers)
    assert first is not None
    entry_key = residency_map.residency_map_key(
        "/mnt/shared/model/shard-0.bin", 0)
    assert residency_map.read_map(first)["entries"][entry_key]["ram_path"] == (
        f"{ram_root}/model/shard-0.bin")
    before = _stat(first)
    assert _stat(tier_loop.compose_map(
        queue, CONSUMER, ram_tiers=tiers)) == before

    # A remount (new epoch) lays the same fragments under a different header.
    tiers2 = {RAM_TIER: {"tier": "ram", "tier_id": RAM_TIER,
                         "mountpoint": ram_root, "epoch": "new-epoch"}}
    second = tier_loop.compose_map(queue, CONSUMER, ram_tiers=tiers2)
    assert _stat(second) != before
    assert "ram_path" not in residency_map.read_map(second)["entries"][
        entry_key]
