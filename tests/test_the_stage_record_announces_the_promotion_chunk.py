"""The stage record announces the promotion chunk the submitter cuts with.

The sealer reads the chunk off the tier record, never off its own box --
``movement_tools`` seals argv off the record for the same reason: the box
that seals is very often not the box that runs.  The ram record announces
the effective chunk since #673 (the policy pin, or a window quarter); the
stage record carries the same value, announced beside it by the same cycle,
because there is one chunk family across tiers.  No ram tier on the stage's
host -- or a ram record predating the announcement -- seals the whole-phase
pair, exactly as before: chunking is a sealing-time property, and an
unchunkable phase keeps the shape it always had.
"""
from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
from prismabuild import pool, storage_tiers  # noqa: E402
import tier_loop  # noqa: E402

STAGE_TIER = "prismabuild-stage:dl380g10"
RAM_TIER = "ram:dl380g10"
GIB = storage_tiers.GIB
WINDOW_GIB = 160
CHUNK_GIB = 40


def _discover(stage_extra: dict | None = None,
              ram_extra: dict | None = None):
    def discover(**_kwargs):
        tiers = {
            STAGE_TIER: {
                "schema": storage_tiers.TIER_RECORD_SCHEMA_V1,
                "tier_id": STAGE_TIER, "host": "dl380g10", "tier": "stage",
                "mountpoint": "/stage/prewarm",
                "capacity_bytes": 512 * GIB,
                **(stage_extra or {})},
        }
        if ram_extra is not None:
            tiers[RAM_TIER] = {
                "schema": storage_tiers.TIER_RECORD_SCHEMA_V1,
                "tier_id": RAM_TIER, "host": "dl380g10", "tier": "ram",
                "mountpoint": "/ram/prewarm",
                "capacity_bytes": WINDOW_GIB * GIB,
                **ram_extra}
        return tiers

    return discover


def _announced(tmp_path: Path, discover) -> dict[str, dict]:
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    records = tier_loop.cycle(
        queue, host="dl380g10", source_pool="storage_pool",
        receipts=tier_loop.ReceiptCache(), discover=discover)
    return {str(record["tier_id"]): record for record in records}


def test_the_stage_record_carries_the_ram_tiers_effective_chunk(
        tmp_path) -> None:
    """One chunk family across tiers: the stage announces what the ram
    record announces, so both legs cut the same phases at the same size."""

    announced = _announced(tmp_path, _discover(ram_extra={
        "window_gib": WINDOW_GIB, "promotion_chunk_gib": CHUNK_GIB}))

    assert announced[STAGE_TIER]["promotion_chunk_gib"] == CHUNK_GIB


def test_a_derived_ram_chunk_reaches_the_stage_record_too(tmp_path) -> None:
    """The pin is not the only sizing: an unpinned policy derives a window
    quarter on the ram record, and the stage announces that derivation."""

    announced = _announced(tmp_path, _discover(ram_extra={
        "window_gib": WINDOW_GIB,
        "promotion_chunk_gib": storage_tiers.promotion_chunk_gib_for_window(
            WINDOW_GIB)}))

    assert announced[STAGE_TIER]["promotion_chunk_gib"] == CHUNK_GIB


def test_no_ram_tier_leaves_the_stage_whole_phase(tmp_path) -> None:
    """No tmpfs on the stage's host is nobody's chunk family: the sealer
    must seal the pair the window has published since #583."""

    announced = _announced(tmp_path, _discover())

    assert "promotion_chunk_gib" not in announced[STAGE_TIER]


def test_a_ram_record_predating_the_chunk_announces_nothing(tmp_path) -> None:
    """Tonight's running campaign announces ram records without the key;
    the stage stays whole-phase beside them rather than guessing a size."""

    announced = _announced(tmp_path, _discover(ram_extra={
        "window_gib": WINDOW_GIB}))

    assert "promotion_chunk_gib" not in announced[STAGE_TIER]


def test_a_non_positive_ram_chunk_announces_nothing(tmp_path) -> None:
    """A sizing that is not a positive whole GiB is not a sizing the
    submitter can cut with, whatever record carries it."""

    for bad in (0, -40, True):
        announced = _announced(
            tmp_path, _discover(ram_extra={
                "window_gib": WINDOW_GIB, "promotion_chunk_gib": bad}))

        assert "promotion_chunk_gib" not in announced[STAGE_TIER], bad
