"""PB #823: same-generation incremental republish must invalidate reader caches.

``stage_move.publish`` rewrites the fragment and the material sidecar as
entries land while ``begin_material`` keeps ONE generation for the whole
mover run. Both reader-side caches keyed on the generation alone handed
back the pre-republish pair to a caller reusing a persistent ``context``:

- ``_cached_cover_docs`` (``covers_for_keys``) answered ``unpublished``
  for bytes the mover had already published (production Stage A r2);
- ``acquire``'s ``cached_fragment``/``cached_material`` answered
  ``source-coverage-gap`` for the same reason.

Cache reuse is allowed only while the freshly read sidecar equals the one
that was cached; a republished sidecar re-reads the fragment and replaces
the entry. Run via published pbtest at -10.
"""
from __future__ import annotations

from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
from prismabuild import pool, residency_map, reader_lease  # noqa: E402
import stage_release  # noqa: E402

CONSUMER = "c" * 64
MOVER = "e" * 64
TIER = "prismabuild-stage:dl380g10"
ATTEMPT = {"nonce": "n1", "scope_id": "s1"}
HOLDER = {"host": "test-host", "pid": 4242}
SOURCE_A = "/mnt/shared/pb823-a.bin"
SOURCE_B = "/mnt/shared/pb823-b.bin"
DIGEST_A = "b" * 64
DIGEST_B = "c" * 64


@pytest.fixture()
def fleet(tmp_path: Path):
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    stage = tmp_path / "stage"
    stage.mkdir()
    stage_release.register_stage_root(queue, tier_id=TIER, stage_root=stage)
    return queue, stage


def _republish(root: Path, stage: Path, generation: str,
               files: dict[str, tuple[Path, str]]) -> None:
    """One incremental publication: fragment first, then the dating sidecar.

    Mirrors ``stage_move.publish`` mid-run: both documents rewritten, the
    mover run's single generation kept.
    """

    staged_entries = {}
    dated_entries = {}
    for key, (path, digest) in files.items():
        staged_entries[key] = {
            "stage_path": str(path), "bytes": 512,
            "sha256": digest, "offset": 0}
        identity = reader_lease.stat_identity(str(path))
        assert identity is not None
        dated_entries[key] = {
            "stage_path": str(path), "bytes": 512,
            "sha256": digest, "file_id": identity}
    residency_map.write_fragment(root, {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": CONSUMER, "mover_action_key": MOVER,
        "tier_id": TIER, "stage_root": str(stage),
        "manifest_sha256": "a" * 64, "entries": staged_entries})
    reader_lease.write_material(
        root, consumer_action_key=CONSUMER, mover_action_key=MOVER,
        tier_id=TIER, stage_root=str(stage), manifest_sha256="a" * 64,
        generation=generation, entries=dated_entries)


def _stage_files(stage: Path) -> tuple[Path, Path]:
    first = stage / "model" / "pb823-a.bin"
    second = stage / "model" / "pb823-b.bin"
    first.parent.mkdir(parents=True)
    first.write_bytes(b"\x71" * 512)
    second.write_bytes(b"\x72" * 512)
    return first, second


def test_covers_see_same_generation_republish(fleet) -> None:
    """covers_for_keys sees entry B published under the cached generation."""

    queue, stage = fleet
    first, second = _stage_files(stage)
    root = queue.root / pool.RESIDENCY
    key_a = residency_map.residency_map_key(SOURCE_A, 0)
    key_b = residency_map.residency_map_key(SOURCE_B, 0)
    generation = reader_lease.mint_generation()
    context: dict = {}
    cover_key = f"cover:{CONSUMER}:{MOVER}"

    _republish(root, stage, generation, {key_a: (first, DIGEST_A)})
    before = reader_lease.covers_for_keys(
        root, CONSUMER, [key_a], tier_id=TIER, manifest_sha256="a" * 64,
        epoch="", context=context)
    assert before["ok"], before
    cached = context[cover_key]["fragment"]
    assert reader_lease.covers_for_keys(
        root, CONSUMER, [key_a], tier_id=TIER, manifest_sha256="a" * 64,
        epoch="", context=context) == before
    assert context[cover_key]["fragment"] is cached

    _republish(root, stage, generation,
               {key_a: (first, DIGEST_A), key_b: (second, DIGEST_B)})
    after = reader_lease.covers_for_keys(
        root, CONSUMER, [key_b], tier_id=TIER, manifest_sha256="a" * 64,
        epoch="", context=context)
    assert after["ok"], after
    assert after["expected"] == {key_b: {"bytes": 512, "sha256": DIGEST_B}}
    assert context[cover_key]["fragment"] is not cached


def test_acquire_sees_same_generation_republish(fleet) -> None:
    """acquire pins entry B published under the cached generation."""

    queue, stage = fleet
    first, second = _stage_files(stage)
    root = queue.root / pool.RESIDENCY
    key_a = residency_map.residency_map_key(SOURCE_A, 0)
    key_b = residency_map.residency_map_key(SOURCE_B, 0)
    generation = reader_lease.mint_generation()
    context: dict = {}

    def acquire(key: str, digest: str, token: str) -> dict:
        return reader_lease.acquire(
            queue, consumer_action_key=CONSUMER, attempt=ATTEMPT,
            tier_id=TIER, epoch="", span={"start_bytes": 0, "end_bytes": 512},
            holder=HOLDER, acquire_token=token,
            covers=[{"mover_action_key": MOVER, "manifest_sha256": "a" * 64}],
            expected={key: {"bytes": 512, "sha256": digest}},
            context=context)

    _republish(root, stage, generation, {key_a: (first, DIGEST_A)})
    first_pin = acquire(key_a, DIGEST_A, "pb823-token-a")
    assert first_pin["ok"], first_pin

    _republish(root, stage, generation,
               {key_a: (first, DIGEST_A), key_b: (second, DIGEST_B)})
    second_pin = acquire(key_b, DIGEST_B, "pb823-token-b")
    assert second_pin["ok"], second_pin
