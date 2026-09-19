"""Ram chunk fragments compose to the same overlay the whole-phase fragment did.

``compose_map`` already merges per-entry: a promotion files one fragment per
movement node, and the overlay lays ram paths over the stage map entry by
entry.  Chunking changes how many fragments vouch for a phase, not what they
vouch for -- four chunk fragments of one phase must lay exactly the overlay
one whole-phase fragment laid, or the consumer's map would depend on how the
sealer cut the range rather than on what landed.
"""
from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import residency_map  # noqa: E402

CONSUMER = "c" * 64
MANIFEST = "9" * 64
STAGE_TIER = "prismabuild-stage:dl380g10"
RAM_TIER = "ram:dl380g10"
STAGE_ROOT = "/stage/prewarm"
RAM_ROOT = "/ram/prewarm"
EPOCH = "1695052800-1a2b3c4d5e6f7a8b"
ENTRY_BYTES = 4096


def _hexkey(seed: str) -> str:
    return (seed.encode().hex() * 64)[:64]


def _stage_map() -> dict[str, object]:
    """Four staged entries, one per future chunk, vouched by the stage."""

    entries = {}
    for index in range(4):
        key = residency_map.residency_map_key(
            "/mnt/shared/model/shard-0.bin", index * ENTRY_BYTES)
        entries[key] = {
            "stage_path": f"{STAGE_ROOT}/model/shard-0.bin.{index}",
            "bytes": ENTRY_BYTES, "offset": index * ENTRY_BYTES,
            "sha256": _hexkey(f"entry{index}"),
        }
    return residency_map.validate_map({
        "schema": residency_map.RESIDENCY_MAP_SCHEMA_V1,
        "tier_id": STAGE_TIER, "stage_root": STAGE_ROOT,
        "manifest_sha256": MANIFEST, "leads": [_hexkey("mover0")],
        "generation": 1, "entries": entries})


def _ram_fragment(mover: str, indexes: list[int]) -> dict[str, object]:
    entries = {}
    for index in indexes:
        key = residency_map.residency_map_key(
            "/mnt/shared/model/shard-0.bin", index * ENTRY_BYTES)
        entries[key] = {
            "stage_path": f"{RAM_ROOT}/model/shard-0.bin.{index}",
            "bytes": ENTRY_BYTES, "offset": index * ENTRY_BYTES,
            "sha256": _hexkey(f"entry{index}"),
        }
    return {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": CONSUMER, "mover_action_key": mover,
        "tier_id": RAM_TIER, "stage_root": RAM_ROOT,
        "manifest_sha256": MANIFEST, "entries": entries, "epoch": EPOCH,
    }


def _overlay(fragments) -> dict[str, object]:
    return residency_map.overlay_ram(
        _stage_map(), fragments, ram_tier_id=RAM_TIER, ram_root=RAM_ROOT,
        ram_epoch=EPOCH)


def test_four_chunk_fragments_lay_the_whole_phases_overlay() -> None:
    whole = _overlay([_ram_fragment(_hexkey("rampromote0"), [0, 1, 2, 3])])
    chunked = _overlay([_ram_fragment(_hexkey(f"rampromote0c{index}"), [index])
                        for index in range(4)])

    assert chunked == whole


def test_the_overlay_covers_every_entry_contiguously() -> None:
    overlaid = _overlay([_ram_fragment(_hexkey(f"rampromote0c{index}"), [index])
                         for index in range(4)])

    assert overlaid["ram_tier_id"] == RAM_TIER
    assert overlaid["ram_epoch"] == EPOCH
    entries = overlaid["entries"]
    assert isinstance(entries, dict) and len(entries) == 4
    for index in range(4):
        key = residency_map.residency_map_key(
            "/mnt/shared/model/shard-0.bin", index * ENTRY_BYTES)
        assert entries[key]["ram_path"] == (
            f"{RAM_ROOT}/model/shard-0.bin.{index}")
        assert entries[key]["stage_path"] == (
            f"{STAGE_ROOT}/model/shard-0.bin.{index}")


def test_a_missing_chunk_is_a_stage_fallback_not_a_refusal() -> None:
    """A phase whose tail chunk has not promoted yet still serves its head
    from ram: the unlaid entry keeps the stage path the map vouched for."""

    overlaid = _overlay([_ram_fragment(_hexkey("rampromote0c0"), [0])])

    entries = overlaid["entries"]
    assert isinstance(entries, dict)
    head = entries[residency_map.residency_map_key(
        "/mnt/shared/model/shard-0.bin", 0)]
    assert head["ram_path"] == f"{RAM_ROOT}/model/shard-0.bin.0"
    tail = entries[residency_map.residency_map_key(
        "/mnt/shared/model/shard-0.bin", 3 * ENTRY_BYTES)]
    assert "ram_path" not in tail
