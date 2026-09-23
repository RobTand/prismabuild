"""The chunk splitter sees the manifest's entries and cuts only between them (#965).

A mover stages whole entries, so a chunk is only honest when both of its
edges are entry boundaries in the manifest's read order: then the entries
it covers weigh exactly its range, and its reservation is those bytes.
``split_range_into_chunks`` therefore takes the read order's entry sizes as
a required argument -- there is no way left to call it blind -- and packs
whole entries greedily up to the nominal chunk.  An entry larger than the
chunk is a chunk of its own; an entry larger than the tier's window can
never be admitted, and refuses by name.

The read order is the manifest's own: ``storage_tiers.manifest_read_entries``
and ``prewarm_loop.manifest_read_entries`` (which both movers call) must
agree on it for either schema, or the seal's cuts and the movers' walk
would describe two different byte orders.
"""
from __future__ import annotations

from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
from prismabuild import storage_tiers  # noqa: E402
import prewarm_loop  # noqa: E402

GIB = storage_tiers.GIB
V1 = "prismaquant.prismabuild.data_manifest.v1"
V2 = "prismaquant.prismabuild.data_manifest.v2"


def split(start: int, end: int, chunk: int, sizes: list[int],
          window: int | None = None) -> list[tuple[int, int]]:
    return storage_tiers.split_range_into_chunks(
        start, end, chunk, entry_bytes=sizes, window_bytes=window)


def test_the_splitter_cannot_be_called_without_the_entries() -> None:
    with pytest.raises(TypeError):
        storage_tiers.split_range_into_chunks(  # type: ignore[call-arg]
            0, 80 * GIB, 40 * GIB)


def test_whole_entries_pack_greedily_up_to_the_chunk() -> None:
    """15 + 15 fits in 40, a third does not: cut before it, never inside it."""

    entry = 15 * GIB + 7
    sizes = [entry] * 8 + [3 * GIB]

    chunks = split(0, sum(sizes), 40 * GIB, sizes)

    assert chunks == [(0, 2 * entry), (2 * entry, 4 * entry),
                      (4 * entry, 6 * entry), (6 * entry, 8 * entry + 3 * GIB)]


def test_an_entry_exactly_the_chunk_fills_it() -> None:
    sizes = [40 * GIB, 40 * GIB, 3 * GIB]

    assert split(0, 83 * GIB, 40 * GIB, sizes) == [
        (0, 40 * GIB), (40 * GIB, 80 * GIB), (80 * GIB, 83 * GIB)]


def test_an_entry_larger_than_the_chunk_is_a_chunk_of_its_own() -> None:
    sizes = [10 * GIB, 41 * GIB, 10 * GIB, 10 * GIB]

    assert split(0, 71 * GIB, 40 * GIB, sizes, window=160 * GIB) == [
        (0, 10 * GIB), (10 * GIB, 51 * GIB), (51 * GIB, 71 * GIB)]


def test_an_entry_larger_than_the_window_refuses() -> None:
    sizes = [10 * GIB, 161 * GIB, 10 * GIB]

    with pytest.raises(storage_tiers.EntryExceedsWindow) as caught:
        split(0, 181 * GIB, 40 * GIB, sizes, window=160 * GIB)

    refusal = caught.value
    assert isinstance(refusal, ValueError)
    assert (refusal.entry_index, refusal.start_bytes, refusal.end_bytes,
            refusal.window_bytes) == (1, 10 * GIB, 171 * GIB, 160 * GIB)


def test_an_entry_the_size_of_the_window_is_admissible() -> None:
    sizes = [160 * GIB]

    assert split(0, 160 * GIB, 40 * GIB, sizes, window=160 * GIB) == [
        (0, 160 * GIB)]


def test_the_window_is_checked_only_inside_the_range() -> None:
    """Another phase's oversized entry is that phase's refusal, not this one's."""

    sizes = [161 * GIB, 10 * GIB, 10 * GIB]

    assert split(161 * GIB, 181 * GIB, 40 * GIB, sizes, window=160 * GIB) == [
        (161 * GIB, 181 * GIB)]


def test_a_phase_that_fits_is_one_chunk_whole() -> None:
    assert split(0, 40 * GIB, 40 * GIB, [GIB] * 40) == [(0, 40 * GIB)]
    assert split(0, 3 * GIB, 40 * GIB, [3 * GIB]) == [(0, 3 * GIB)]


def test_a_range_inside_the_read_order_is_cut_from_its_own_start() -> None:
    sizes = [7] + [GIB] * 100

    chunks = split(7, 100 * GIB + 7, 30 * GIB, sizes)

    assert chunks == [(7, 30 * GIB + 7), (30 * GIB + 7, 60 * GIB + 7),
                      (60 * GIB + 7, 90 * GIB + 7), (90 * GIB + 7, 100 * GIB + 7)]


def test_zero_byte_entries_never_make_an_empty_chunk() -> None:
    sizes = [0, 30 * GIB, 0, 0, 30 * GIB, 0]

    chunks = split(0, 60 * GIB, 40 * GIB, sizes)

    assert chunks == [(0, 30 * GIB), (30 * GIB, 60 * GIB)]
    assert all(end > start for start, end in chunks)


@pytest.mark.parametrize("sizes", [
    [1] * 17,
    [5, 1, 9, 2, 2, 40, 3, 3, 3, 30, 1],
    [11, 11, 11, 11],
    [100, 1, 100, 1],
])
@pytest.mark.parametrize("chunk", [1, 4, 10, 25, 1000])
def test_chunks_tile_the_range_at_entry_boundaries(sizes, chunk) -> None:
    """Contiguous, entry-aligned, summing to the range: nothing moved twice."""

    boundaries, running = {0}, 0
    for size in sizes:
        running += size
        boundaries.add(running)

    chunks = split(0, running, chunk, sizes)

    assert chunks[0][0] == 0 and chunks[-1][1] == running
    for first, second in zip(chunks, chunks[1:]):
        assert first[1] == second[0]
    for start, end in chunks:
        assert start in boundaries and end in boundaries and end > start
        # Over the nominal size only when the chunk is a single entry.
        if end - start > chunk:
            assert end - start in sizes
    assert sum(end - start for start, end in chunks) == running


@pytest.mark.parametrize("start, end", [(1, 30), (0, 29), (5, 25)])
def test_a_range_that_cuts_an_entry_refuses(start, end) -> None:
    """The phase itself must be entry-aligned; the splitter will not
    start or stop a chunk inside an entry on a caller's say-so."""

    with pytest.raises(ValueError, match="entry boundary"):
        split(start, end, 10, [10, 10, 10])


def test_an_empty_range_and_a_non_positive_chunk_refuse() -> None:
    for start, end, chunk in ((5, 5, 40), (9, 5, 40), (0, 40, 0), (0, 40, -1)):
        with pytest.raises(ValueError):
            split(start, end, chunk, [40])


def _entries(sizes: list[int]) -> list[dict[str, object]]:
    return [{"path": f"/mnt/shared/part-{index}", "offset": 0, "bytes": size,
             "sha256": None} for index, size in enumerate(sizes)]


def _v1(sizes: list[int], cuts: list[int]) -> dict[str, object]:
    phases, running, position = [], 0, 0
    for ordinal, count in enumerate(cuts):
        running += sum(sizes[position:position + count])
        position += count
        phases.append({"name": f"phase-{ordinal}", "cumulative_bytes": running})
    return {"schema": V1, "produced_by": {}, "annotations": {"phases": phases},
            "mount_prefix": "/mnt/shared", "entries": _entries(sizes),
            "entry_count": len(sizes), "total_bytes": sum(sizes)}


def _v2(sizes: list[int], order: list[list[int]]) -> dict[str, object]:
    phases, running = [], 0
    for ordinal, indices in enumerate(order):
        size = sum(sizes[index] for index in indices)
        running += size
        phases.append({"name": f"phase-{ordinal}", "entry_indices": indices,
                       "bytes": size, "cumulative_bytes": running})
    return {"schema": V2, "produced_by": {}, "annotations": {},
            "mount_prefix": "/mnt/shared", "entries": _entries(sizes),
            "entry_count": len(sizes), "total_bytes": sum(sizes),
            "read_plan": {"phases": phases, "read_bytes": running}}


@pytest.mark.parametrize("manifest", [
    _v1([3 * GIB, 5, 7 * GIB, 11], [2, 2]),
    _v2([3 * GIB, 5, 7 * GIB, 11], [[2, 0], [3, 1]]),
    _v2([3 * GIB, 5, 7 * GIB, 11], [[0], [1, 2, 3]]),
])
def test_the_seal_and_the_movers_read_one_order(manifest) -> None:
    assert (storage_tiers.manifest_read_entries(manifest)
            == prewarm_loop.manifest_read_entries(manifest))


def test_a_manifest_whose_phases_are_refused_has_no_read_order() -> None:
    """The same refusal ``manifest_phase_ranges`` makes: a phase table that
    cuts an entry describes no read order a chunk could be cut against."""

    manifest = _v1([3 * GIB, 5, 7 * GIB, 11], [2, 2])
    manifest["annotations"]["phases"][0]["cumulative_bytes"] = GIB  # type: ignore[index]

    assert storage_tiers.manifest_phase_ranges(manifest) == []
    assert storage_tiers.manifest_read_entries(manifest) == []
