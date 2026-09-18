"""A ram-resident entry names the ram path beside the stage path it vouches for.

The composed map is one document per consumer, and ``compose`` refuses
fragments that disagree about the tier -- so a ram fragment is not composed
*into* the stage map; it is laid over it.  The entry keeps the stage path it
already had and gains ``ram_path``: the same ``(path, offset)`` identity, the
same digest, the bytes on the tmpfs under the same content-addressed name.
A consumer prefers the ram copy and falls back to the staged copy the map
already vouched for, which is what makes a stale ram entry a cache miss
rather than an ENOENT.

The ram fragment carries the epoch it landed under, and a fragment of the ram
tier without one does not validate at all -- an unepoched ram range is a
range nobody can place in time (#640).
"""
from __future__ import annotations

from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import residency_map  # noqa: E402

CONSUMER = "c" * 64
STAGE_MOVER = "1" * 64
RAM_MOVER = "2" * 64
MANIFEST = "9" * 64
STAGE_TIER = "prismabuild-stage:dl380g10"
RAM_TIER = "ram:dl380g10"
EPOCH = "1695052800-1a2b3c4d5e6f7a8b"
STAGE_ROOT = "/stage/prewarm"
RAM_ROOT = "/ram/prewarm"
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


def _stage_map() -> dict[str, object]:
    return residency_map.compose([
        _fragment(tier_id=STAGE_TIER, root=STAGE_ROOT, mover=STAGE_MOVER)])


def test_the_overlay_names_the_ram_path_beside_the_stage_path() -> None:
    overlaid = residency_map.overlay_ram(
        _stage_map(),
        [_fragment(tier_id=RAM_TIER, root=RAM_ROOT, mover=RAM_MOVER,
                   epoch=EPOCH)],
        ram_tier_id=RAM_TIER, ram_root=RAM_ROOT, ram_epoch=EPOCH)

    entry = overlaid["entries"][ENTRY_KEY]
    assert entry["stage_path"] == f"{STAGE_ROOT}/model/shard-0.bin"
    assert entry["ram_path"] == f"{RAM_ROOT}/model/shard-0.bin"
    assert entry["sha256"] == "b" * 64
    assert overlaid["ram_tier_id"] == RAM_TIER
    assert overlaid["ram_root"] == RAM_ROOT
    assert overlaid["ram_epoch"] == EPOCH
    # The identity is the map's own key, so a consumer looks up the entry it
    # is about to read and finds both servants of it.
    assert residency_map.lookup(overlaid, "/mnt/shared/model/shard-0.bin")[
        "ram_path"] == f"{RAM_ROOT}/model/shard-0.bin"


def test_a_ram_path_outside_the_announced_root_refuses() -> None:
    stray = _fragment(tier_id=RAM_TIER, root="/elsewhere", mover=RAM_MOVER,
                      epoch=EPOCH)

    with pytest.raises(residency_map.ResidencyMapError):
        residency_map.overlay_ram(
            _stage_map(), [stray],
            ram_tier_id=RAM_TIER, ram_root=RAM_ROOT, ram_epoch=EPOCH)


def test_a_ram_entry_the_stage_map_does_not_name_is_skipped() -> None:
    """The two windows may disagree for a cycle; the map degrades, not fails.

    A ram fragment whose key the stage map does not carry is the shape of a
    crash between the two egresses.  Refusing the whole compose would send the
    consumer to the pool for every entry; skipping it leaves the map exactly
    as wide as the stage's own vouching, and the ram egress drops the
    fragment.
    """

    elsewhere = dict(_fragment(tier_id=RAM_TIER, root=RAM_ROOT,
                               mover=RAM_MOVER, epoch=EPOCH))
    other_key = residency_map.residency_map_key(
        "/mnt/shared/model/shard-9.bin", 0)
    elsewhere["entries"] = {other_key: {
        "stage_path": f"{RAM_ROOT}/model/shard-9.bin", "bytes": 4096,
        "offset": 0, "sha256": "b" * 64}}

    overlaid = residency_map.overlay_ram(
        _stage_map(), [elsewhere],
        ram_tier_id=RAM_TIER, ram_root=RAM_ROOT, ram_epoch=EPOCH)

    assert other_key not in overlaid["entries"]
    assert "ram_path" not in overlaid["entries"][ENTRY_KEY]


def test_a_ram_fragment_without_an_epoch_does_not_validate() -> None:
    with pytest.raises(residency_map.ResidencyMapError):
        residency_map.validate_fragment(
            _fragment(tier_id=RAM_TIER, root=RAM_ROOT, mover=RAM_MOVER))


def test_a_stage_fragment_still_needs_no_epoch() -> None:
    """The new field is the ram tier's price, not a tax on the stage's own."""

    checked = residency_map.validate_fragment(
        _fragment(tier_id=STAGE_TIER, root=STAGE_ROOT, mover=STAGE_MOVER))

    assert "epoch" not in checked
