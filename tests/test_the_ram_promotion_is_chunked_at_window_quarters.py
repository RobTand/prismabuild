"""A ram promotion is chunked at window quarters, pinned or derived.

Phase-granular promotion sawtooths the tmpfs 0->123->0 GiB: a 123 GiB phase
promotes only after accepted progress passes the previous phase, and the
egress frees it all at the next boundary, while the GPU idles through each
full-phase copy.  Rob's directive (2026-09-19): refill the tmpfs as it frees
up from SSD -- chunk both directions.  The chunk size is a module constant
with a named rationale: a quarter of the window, so that at window 160 a
123 GiB phase seals 4 chunks (40, 40, 40, 3 GiB) and the window always holds
the chunk being read, the chunk promoting, and run-ahead behind and ahead.
Fewer chunks reintroduce the phase-sized sawtooth; more chunks multiply queue
rows and fragments per phase without buying overlap, because a promotion at
SSD->tmpfs rates fills a 40 GiB chunk in under a minute -- already finer
than the reader's phase dwell.  The policy may pin the size instead; null is
the derivation.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import storage_tiers  # noqa: E402

GIB = storage_tiers.GIB


def test_four_chunks_per_window_is_the_named_constant() -> None:
    assert storage_tiers.PROMOTION_CHUNKS_PER_WINDOW == 4


def test_the_chunk_derives_as_a_window_quarter() -> None:
    """At window 160 the chunk is 40 GiB -- the arithmetic the direction names."""

    assert storage_tiers.promotion_chunk_gib_for_window(160) == 40


def test_a_pinned_chunk_beats_the_derivation() -> None:
    assert storage_tiers.promotion_chunk_gib_for_window(160, pinned=32) == 32


def test_a_tiny_window_still_cuts_a_positive_chunk() -> None:
    """A window under four GiB would derive zero; the floor keeps it sealable."""

    assert storage_tiers.promotion_chunk_gib_for_window(3) == 1


def test_a_phase_splits_into_quarters_with_a_short_last_chunk() -> None:
    """123 GiB at a 40 GiB chunk is 4 chunks, the last one short.

    The phase is 123 one-GiB entries, so every quarter edge is also an
    entry boundary: the splitter cuts only there (#965), and here that is
    exactly the window-quarter arithmetic.
    """

    chunks = storage_tiers.split_range_into_chunks(
        0, 123 * GIB, 40 * GIB, entry_bytes=[GIB] * 123)

    assert chunks == [(0, 40 * GIB), (40 * GIB, 80 * GIB),
                      (80 * GIB, 120 * GIB), (120 * GIB, 123 * GIB)]


def test_an_exact_multiple_needs_no_short_chunk() -> None:
    assert storage_tiers.split_range_into_chunks(
        0, 80 * GIB, 40 * GIB, entry_bytes=[GIB] * 80) == [
        (0, 40 * GIB), (40 * GIB, 80 * GIB)]


def test_a_phase_that_fits_is_one_chunk_whole() -> None:
    """A range at or under the chunk seals one node over the whole phase."""

    assert storage_tiers.split_range_into_chunks(
        0, 40 * GIB, 40 * GIB, entry_bytes=[GIB] * 40) == [(0, 40 * GIB)]
    assert storage_tiers.split_range_into_chunks(
        0, 3 * GIB, 40 * GIB, entry_bytes=[3 * GIB]) == [(0, 3 * GIB)]


def test_chunks_cover_without_gap_or_overlap() -> None:
    """Contiguity is the property the plan validator will recheck at freeze."""

    chunks = storage_tiers.split_range_into_chunks(
        7, 100 * GIB + 7, 30 * GIB, entry_bytes=[7] + [GIB] * 100)

    assert chunks[0][0] == 7
    assert chunks[-1][1] == 100 * GIB + 7
    for first, second in zip(chunks, chunks[1:]):
        assert first[1] == second[0]
    assert all(end > start for start, end in chunks)


def test_an_empty_range_and_a_non_positive_chunk_refuse() -> None:
    import pytest

    for start, end, chunk in ((5, 5, 40 * GIB), (9, 5, 40 * GIB),
                              (0, 40 * GIB, 0), (0, 40 * GIB, -1)):
        with pytest.raises(ValueError):
            storage_tiers.split_range_into_chunks(
                start, end, chunk, entry_bytes=[GIB] * 40)


def _policy(extra: dict) -> Path:
    import tempfile

    base = {"schema": storage_tiers.RAM_TIER_POLICY_SCHEMA_V1,
            "mountpoint": "/ram/prewarm", "ceiling_gib_max": 256,
            "window_gib_default": 160, "arc_floor_gib": 20,
            "system_reserve_gib": 16, "prefill_depth": None}
    path = Path(tempfile.mkdtemp()) / "policy.json"
    path.write_text(json.dumps({**base, **extra}))
    return path


def test_the_policy_pins_the_chunk_or_defaults_to_the_derivation() -> None:
    assert (storage_tiers.read_ram_policy(
        _policy({"promotion_chunk_gib": 32}))["promotion_chunk_gib"] == 32)
    assert (storage_tiers.read_ram_policy(
        _policy({"promotion_chunk_gib": None}))["promotion_chunk_gib"] is None)


def test_a_policy_predating_the_pin_still_validates_as_unpinned(tmp_path) -> None:
    """Tonight's running campaign sealed under a policy without the key."""

    base = {"schema": storage_tiers.RAM_TIER_POLICY_SCHEMA_V1,
            "mountpoint": "/ram/prewarm", "ceiling_gib_max": 256,
            "window_gib_default": 160, "arc_floor_gib": 20,
            "system_reserve_gib": 16, "prefill_depth": None}
    path = tmp_path / "policy.json"
    path.write_text(json.dumps(base))

    policy = storage_tiers.read_ram_policy(path)

    assert policy is not None and policy.get("promotion_chunk_gib") is None


def test_a_bad_pin_refuses_the_policy() -> None:
    for bad in (0, -32, True, "32"):
        assert storage_tiers.read_ram_policy(
            _policy({"promotion_chunk_gib": bad})) is None, bad


def test_the_committed_policy_carries_an_unpinned_chunk() -> None:
    policy = storage_tiers.read_ram_policy(
        Path(__file__).resolve().parents[1] / "tools" / "fleet"
        / "ram_tier_policy.json")

    assert policy is not None and policy["promotion_chunk_gib"] is None


def test_the_tier_record_announces_the_effective_chunk(tmp_path) -> None:
    """The sealer reads the chunk off the tier record, never off its own box.

    ``movement_tools`` seals argv off the record for the same reason: the box
    that seals is very often not the box that runs.  Derived here (160 -> 40
    on the announced window), pinned when the policy pins.
    """

    import os

    mount = tmp_path / "ram"
    mount.mkdir()
    proc = tmp_path / "proc"
    proc.mkdir()
    (proc / "mounts").write_text(f"tmpfs {mount} tmpfs rw,noswap,size=256G 0 0\n")
    (proc / "meminfo").write_text(
        f"MemTotal:  {(294 * GIB) // 1024} kB\n")
    (proc / "arcstats").write_text(
        f"c_max 4 {22 * GIB}\nsize 4 {11 * GIB}\narc_meta_used 4 {5 * GIB}\n")
    block = GIB // 4096

    def read(path: str):
        if path != str(mount):
            raise OSError(f"no tmpfs at {path}")
        return os.statvfs_result(
            (4096, 4096, 256 * block, 200 * block, 200 * block,
             1_000_000, 900_000, 900_000, 0, 255))

    def policy(**over):
        base = {"schema": storage_tiers.RAM_TIER_POLICY_SCHEMA_V1,
                "mountpoint": str(mount), "ceiling_gib_max": 256,
                "window_gib_default": 160, "arc_floor_gib": 20,
                "system_reserve_gib": 16, "prefill_depth": None,
                "promotion_chunk_gib": None}
        return {**base, **over}

    def record(**over):
        tiers = storage_tiers.discover_tiers(
            host="dl380g10", runner=lambda argv: (_ for _ in ()).throw(
                OSError(f"no {argv[0]} on this box")),
            arcstats_path=str(proc / "arcstats"),
            ram_policy=policy(**over), statvfs=read,
            proc_mounts=str(proc / "mounts"),
            meminfo_path=str(proc / "meminfo"), worker_mem_gb=0)
        return tiers.get(storage_tiers.tier_id("ram", "dl380g10"))

    assert record()["promotion_chunk_gib"] == 40
    assert record(promotion_chunk_gib=32)["promotion_chunk_gib"] == 32
