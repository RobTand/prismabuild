"""PB #893: a repeat cover lookup must not re-read unchanged documents.

``covers_for_keys`` is the stage-fed reader's selection step. PrismaQuant
calls it two or three times for every staged entry, each time with a fresh
``context`` (PQ ``staged_lease._call_context``), so the only cache it had
never hit: every call re-read and re-validated every mover's material
sidecar and fragment. On R11 that is 27 movers, 19 MB of JSON and 13,956
fragment entries per call, and the GPU idled about 75% of the time.

The documents did not change between those calls. A lookup that finds a
file with the same identity as the one it validated last time must reuse
the validated document, not validate it again. Run via published pbtest at
-10; fixtures use tmp_path roots only.
"""
from __future__ import annotations

from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import residency_map, reader_lease  # noqa: E402

CONSUMER = "c" * 64
TIER = "prismabuild-stage:dl380g10"
MANIFEST = "a" * 64


def _mover(index: int) -> str:
    return f"{index:064x}"


def _publish(root: Path, stage: Path, mover: str, keys: dict[str, str],
             generation: str) -> None:
    """One publication: fragment first, then the sidecar that dates it."""

    staged = {}
    dated = {}
    for key, digest in keys.items():
        path = stage / mover[-8:] / key.split("/")[-1]
        staged[key] = {"stage_path": str(path), "bytes": 512,
                       "sha256": digest, "offset": 0}
        dated[key] = {"stage_path": str(path), "bytes": 512,
                      "sha256": digest,
                      "file_id": {"ino": 7, "size": 512, "mtime_ns": 1,
                                  "ctime_ns": 1}}
    residency_map.write_fragment(root, {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": CONSUMER, "mover_action_key": mover,
        "tier_id": TIER, "stage_root": str(stage),
        "manifest_sha256": MANIFEST, "entries": staged})
    reader_lease.write_material(
        root, consumer_action_key=CONSUMER, mover_action_key=mover,
        tier_id=TIER, stage_root=str(stage), manifest_sha256=MANIFEST,
        generation=generation, entries=dated)


@pytest.fixture()
def counted(monkeypatch):
    """Count every material and fragment validation the lookup performs."""

    counts = {"material": 0, "fragment": 0}
    real_material = reader_lease.validate_material
    real_fragment = residency_map.validate_fragment

    def material(value):
        counts["material"] += 1
        return real_material(value)

    def fragment(value):
        counts["fragment"] += 1
        return real_fragment(value)

    monkeypatch.setattr(reader_lease, "validate_material", material)
    monkeypatch.setattr(residency_map, "validate_fragment", fragment)
    return counts


def test_a_repeat_lookup_with_a_fresh_context_revalidates_nothing(
        tmp_path: Path, counted) -> None:
    """PQ's call pattern: fresh context every call, documents unchanged."""

    root = tmp_path / "residency"
    stage = tmp_path / "stage"
    keys = {f"0:/mnt/shared/pb893/shard-{index:05d}.bin": f"{index + 1:064x}"
            for index in range(6)}
    movers = [_mover(index + 1) for index in range(3)]
    for number, mover in enumerate(movers):
        owned = dict(list(keys.items())[number * 2:number * 2 + 2])
        _publish(root, stage, mover, owned, reader_lease.mint_generation())

    def lookup(key: str) -> dict:
        return reader_lease.covers_for_keys(
            root, CONSUMER, [key], tier_id=TIER, manifest_sha256=MANIFEST,
            epoch="", context={})

    # Publishing validates too; count only what the lookups do.
    counted.update(material=0, fragment=0)
    first_key = next(iter(keys))
    first = lookup(first_key)
    assert first["ok"], first
    assert first["covers"] == [{"mover_action_key": movers[0],
                                "manifest_sha256": MANIFEST}]
    seen = dict(counted)
    assert seen == {"material": 3, "fragment": 3}

    # The same documents, asked again and for every other key: nothing on
    # disk changed, so nothing may be read and validated again.
    for key in keys:
        answer = lookup(key)
        assert answer["ok"], answer
        assert answer["expected"] == {key: {"bytes": 512,
                                            "sha256": keys[key]}}
    assert counted == seen
