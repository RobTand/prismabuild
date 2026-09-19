"""The v2 phase table is held to the same refusal rule as v1 (#594).

``storage_tiers.manifest_phase_ranges`` turns either manifest schema into the
ranges a movement node names, and the "demand is a quotation from the
manifest" claim rests on it refusing a phase table that does not describe its
own manifest.  The v1 branch refuses -- raw subscripts, bools, floats,
non-monotonic tables, overruns, off-entry boundaries, short tables -- and the
v2 branch must refuse the same inputs, because both branches read the same
logical table.  A boundary is read against the manifest's own read order
(``entries`` order for v1, ``read_plan`` order for v2), never file offsets.

Nothing here touches the live queue, a real pool or a real device.
"""

from __future__ import annotations

from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from prismabuild import storage_tiers  # noqa: E402

GIB = storage_tiers.GIB
V2 = "prismaquant.prismabuild.data_manifest.v2"


def _entries(sizes: list[int]) -> list[dict[str, object]]:
    return [{"path": f"/mnt/shared/part-{index}", "offset": 0, "bytes": size,
             "sha256": None} for index, size in enumerate(sizes)]


def _manifest_v1(sizes: list[int]) -> dict[str, object]:
    """A v1 manifest of one entry per phase, with the running sum v1 declares."""

    phases, running = [], 0
    for index, size in enumerate(sizes):
        running += size
        phases.append({"name": f"phase-{index}", "bytes": size,
                       "cumulative_bytes": running})
    return {
        "schema": "prismaquant.prismabuild.data_manifest.v1",
        "produced_by": {}, "annotations": {"phases": phases},
        "mount_prefix": "/mnt/shared", "entries": _entries(sizes),
        "entry_count": len(sizes), "total_bytes": running,
    }


def _manifest_v2(sizes: list[int], order: list[int] | None = None) -> dict[str, object]:
    """A v2 manifest whose read plan walks ``order``, defaulting to entry order.

    Core-shaped: every phase carries the ``entry_indices`` and ``bytes`` the
    core validator requires, and ``read_bytes`` states the read-order total.
    """

    if order is None:
        order = list(range(len(sizes)))
    phases, running = [], 0
    for position, index in enumerate(order):
        running += sizes[index]
        phases.append({"name": f"phase-{position}",
                       "entry_indices": [index], "bytes": sizes[index],
                       "cumulative_bytes": running})
    return {
        "schema": V2,
        "produced_by": {}, "annotations": {},
        "mount_prefix": "/mnt/shared", "entries": _entries(sizes),
        "entry_count": len(sizes), "total_bytes": sum(sizes),
        "read_plan": {"phases": phases, "read_bytes": running},
    }


def _phases(manifest: dict[str, object]) -> list[dict[str, object]]:
    return manifest["read_plan"]["phases"]  # type: ignore[index]


# -- the valid table, in both schemas ----------------------------------------


def test_v2_ranges_follow_the_manifests_own_read_order() -> None:
    manifest = _manifest_v2([3 * GIB, 7 * GIB])
    assert storage_tiers.manifest_phase_ranges(manifest) == [
        {"name": "phase-0", "start_bytes": 0, "end_bytes": 3 * GIB},
        {"name": "phase-1", "start_bytes": 3 * GIB, "end_bytes": 10 * GIB},
    ]


def test_v2_ranges_follow_a_reordered_read_plan() -> None:
    """The read order is the plan's, not the entries' list order.

    ``read_plan`` exists to express a different consumption order, and the
    prewarm loop already expands it that way (``manifest_read_entries``).  A
    validator that checked v2 boundaries against entry-list prefix sums would
    refuse a table the core validator accepted -- the same two-readers
    disagreement this issue removes.
    """

    manifest = _manifest_v2([3 * GIB, 7 * GIB], order=[1, 0])
    assert storage_tiers.manifest_phase_ranges(manifest) == [
        {"name": "phase-0", "start_bytes": 0, "end_bytes": 7 * GIB},
        {"name": "phase-1", "start_bytes": 7 * GIB, "end_bytes": 10 * GIB},
    ]


def test_v1_and_v2_agree_on_the_same_logical_manifest() -> None:
    assert (storage_tiers.manifest_phase_ranges(_manifest_v2([3 * GIB, 7 * GIB]))
            == storage_tiers.manifest_phase_ranges(_manifest_v1([3 * GIB, 7 * GIB])))


# -- the v1 refusals, against v2 inputs ---------------------------------------


def test_v2_missing_phase_key_is_a_refusal_not_a_keyerror() -> None:
    manifest = _manifest_v2([GIB, GIB])
    del _phases(manifest)[0]["cumulative_bytes"]
    assert storage_tiers.manifest_phase_ranges(manifest) == []
    manifest = _manifest_v2([GIB, GIB])
    del _phases(manifest)[1]["name"]
    assert storage_tiers.manifest_phase_ranges(manifest) == []


def test_v2_non_mapping_phase_is_a_refusal() -> None:
    manifest = _manifest_v2([GIB, GIB])
    _phases(manifest)[0] = "phase-0"  # type: ignore[assignment]
    assert storage_tiers.manifest_phase_ranges(manifest) == []


@pytest.mark.parametrize("cumulative", [True, False, 1.5, "10", None])
def test_v2_non_integer_cumulative_is_refused(cumulative: object) -> None:
    """``int()`` accepts ``True`` and truncates floats; v1 rejects both."""

    manifest = _manifest_v2([GIB, GIB])
    _phases(manifest)[0]["cumulative_bytes"] = cumulative
    assert storage_tiers.manifest_phase_ranges(manifest) == []


@pytest.mark.parametrize("name", ["", 7, None, "phase-1"])
def test_v2_bad_or_duplicate_name_is_refused(name: object) -> None:
    """Empty and non-string names, and a name used twice, describe nothing."""

    manifest = _manifest_v2([GIB, GIB])
    _phases(manifest)[0]["name"] = name
    assert storage_tiers.manifest_phase_ranges(manifest) == []


def test_v2_non_monotonic_table_is_refused() -> None:
    manifest = _manifest_v2([3 * GIB, 7 * GIB])
    _phases(manifest)[0]["cumulative_bytes"] = 10 * GIB
    _phases(manifest)[1]["cumulative_bytes"] = 7 * GIB
    assert storage_tiers.manifest_phase_ranges(manifest) == []


def test_v2_cumulative_past_the_total_is_refused() -> None:
    manifest = _manifest_v2([GIB, GIB])
    _phases(manifest)[1]["cumulative_bytes"] = 3 * GIB
    assert storage_tiers.manifest_phase_ranges(manifest) == []


def test_v2_boundary_off_an_entry_is_refused() -> None:
    """A cut through an entry hands a mover more bytes than its tokens reserve."""

    manifest = _manifest_v2([GIB, GIB])
    _phases(manifest)[0]["cumulative_bytes"] = GIB // 2
    _phases(manifest)[1]["cumulative_bytes"] = 2 * GIB
    assert storage_tiers.manifest_phase_ranges(manifest) == []


def test_v2_short_table_is_refused() -> None:
    """A table that ends before the read order ends leaves bytes unstaged."""

    manifest = _manifest_v2([GIB, GIB])
    _phases(manifest)[1]["cumulative_bytes"] = GIB
    assert storage_tiers.manifest_phase_ranges(manifest) == []


def test_v2_entries_that_do_not_sum_to_the_total_are_refused() -> None:
    manifest = _manifest_v2([GIB, GIB])
    manifest["entries"][0]["bytes"] = GIB + 1  # type: ignore[index]
    assert storage_tiers.manifest_phase_ranges(manifest) == []


def test_v2_read_bytes_disagreement_is_refused() -> None:
    manifest = _manifest_v2([GIB, GIB])
    manifest["read_plan"]["read_bytes"] = 3 * GIB  # type: ignore[index]
    assert storage_tiers.manifest_phase_ranges(manifest) == []


def test_v2_missing_read_plan_is_a_refusal() -> None:
    manifest = _manifest_v2([GIB, GIB])
    del manifest["read_plan"]
    assert storage_tiers.manifest_phase_ranges(manifest) == []
    manifest = _manifest_v2([GIB, GIB])
    manifest["read_plan"] = {}  # type: ignore[assignment]
    assert storage_tiers.manifest_phase_ranges(manifest) == []


def test_v2_entry_index_outside_the_entries_is_refused() -> None:
    manifest = _manifest_v2([GIB, GIB])
    _phases(manifest)[0]["entry_indices"] = [7]
    assert storage_tiers.manifest_phase_ranges(manifest) == []
    manifest = _manifest_v2([GIB, GIB])
    _phases(manifest)[0]["entry_indices"] = [True]
    assert storage_tiers.manifest_phase_ranges(manifest) == []
