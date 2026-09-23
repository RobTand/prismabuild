"""The pre-publish shape gate's harness, on a small shape of the same kind.

``tools/fleet/shape_gate.py`` stages a campaign's manifest shape through the
stage and RAM tiers on a queue of its own and reads every entry back through
a reader-lease pin on RAM.  The reference run is
``tests/gate_campaign_shape.py``, over the committed table of a real
manifest; it is run explicitly, not collected.  These cases run the same
harness over a shape small enough for the ordinary suite:

*   a shape with what the reference has -- a phase larger than a chunk with a
    byte cut inside an entry, a file read at two overlapping ranges,
    digest-less entries, and more bytes than the RAM window -- passes, and
    says what it exercised;
*   the same shape through a splitter that cuts at byte offsets, as the one
    before #965 did, fails by name: the mover refuses
    ``residency_overran_reservation``.  This is the gate catching the defect
    it exists for, on the driver rather than on a fixture;
*   a shape with nothing to cut fails as ``did_not_test`` before any byte is
    written;
*   the committed reference table has the properties the gate claims for it.

Every root is under ``tmp_path``; ``tests/conftest.py`` repoints ``pbrun.SH``
there, and the harness refuses any other.
"""
from __future__ import annotations

from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
from prismabuild import storage_tiers  # noqa: E402
import pbrun  # noqa: E402
import shape_gate  # noqa: E402
import tier_loop  # noqa: E402

GIB = 1 << 30
#: 4 KiB stands in for a GiB, so every mover copies real bytes quickly.
SMALL_UNIT = 1 << 12
SMALL_SCALE = GIB // SMALL_UNIT
#: The live overrun of #965, as the odd tail that keeps entries off whole
#: units, the way real shards sit.
ODD = 16_308_005
REFERENCE = shape_gate.TABLE_ROOT / "682e7b0a859f.json.gz"


def _table(phases: list[tuple[str, list[tuple[int, int, int, int]]]]) -> dict:
    """A table from ``(name, [(file, offset, bytes, declared), ...])`` phases."""

    rows: list[list[int]] = []
    out = []
    for name, entries in phases:
        indices = []
        for row in entries:
            indices.append(len(rows))
            rows.append(list(row))
        out.append({"name": name, "entry_indices": indices})
    return {
        "schema": shape_gate.TABLE_SCHEMA_V1,
        "source": {"manifest_sha256": "0" * 64,
                   "manifest_schema": "prismaquant.prismabuild.data_manifest.v2",
                   "entry_count": len(rows),
                   "total_bytes": sum(row[2] for row in rows),
                   "file_count": len({row[0] for row in rows}),
                   "phase_count": len(out)},
        "entries": rows,
        "phases": out,
    }


def _campaign_like() -> dict:
    """The reference's features at 167 units, 7 more than the RAM window.

    ``chain-1`` is eight entries of 15 units plus the odd tail and a 3-unit
    file: a 40-unit byte cut lands inside its third entry.  File 0 is read at
    two overlapping ranges, and the header-sized entries are digest-less.
    """

    big = 15 * GIB + ODD
    return _table([
        ("head", [(0, 0, 2 * GIB, 1), (0, GIB, 2 * GIB, 1),
                  (1, 0, 138, 0), (2, 0, 4, 0)]),
        ("chain-1", [(3 + index, 0, big, index % 2) for index in range(8)]
         + [(11, 0, 3 * GIB, 1)]),
        ("chain-0", [(12, 0, 5 * GIB, 1), (13, 0, 5 * GIB, 0)]),
        ("tail", [(14, 0, 15 * GIB, 1), (15, 0, 15 * GIB, 1)]),
    ])


@pytest.fixture
def small_units(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(storage_tiers, "GIB", SMALL_UNIT)


def _run(tmp_path: Path, table: dict) -> dict:
    return shape_gate.run_gate(table, root=tmp_path / "gate",
                               shared_root=Path(pbrun.SH), scale=SMALL_SCALE)


def test_a_campaign_shaped_window_stages_and_reads_back_strictly(
        tmp_path: Path, small_units) -> None:
    """Every entry lands once per tier and is read back from RAM, pool untouched."""

    result = _run(tmp_path, _campaign_like())

    total = result["manifest_bytes"]
    assert result["coverage"]["phases_over_chunk"] == ["chain-1"]
    assert result["coverage"]["byte_cuts_inside_entries"] >= 1
    assert result["chunks"]["stage"] > result["phases"]
    assert result["chunks"]["ram"] > result["phases"]
    assert result["overlapping_files"] == 1
    assert result["digest_less_entries"] == 7
    assert (result["stage_bytes_moved"], result["ram_bytes_moved"],
            result["bytes_read_strictly"]) == (total, total, total)
    assert result["entries_read_strictly"] == result["entries"]
    assert result["pool_opens"] == {"promote": 0, "read": 0, "egress": 0}
    assert total > result["ram_window_bytes"] and result["ram_egresses"] >= 1
    assert result["ram_peak_bytes"] <= result["ram_window_bytes"]


def test_a_byte_cut_splitter_is_refused_by_name(
        tmp_path: Path, small_units, monkeypatch: pytest.MonkeyPatch) -> None:
    """The defect #965 fixed, put back in the driver, fails the gate by name."""

    def byte_cuts(start: int, end: int, chunk_bytes: int, **_entries):
        return [(position, min(position + chunk_bytes, end))
                for position in range(start, end, chunk_bytes)]

    monkeypatch.setattr(storage_tiers, "split_range_into_chunks", byte_cuts)
    with pytest.raises(shape_gate.ShapeGateFailure) as caught:
        _run(tmp_path, _campaign_like())
    assert caught.value.reason == "movement_refused", caught.value
    assert "residency_overran_reservation" in caught.value.detail


def test_a_shape_with_nothing_to_cut_did_not_test(
        tmp_path: Path, small_units) -> None:
    """No phase over a chunk: the gate says it tested nothing, and writes nothing."""

    table = _table([("head", [(0, 0, 5 * GIB, 1)]),
                    ("chain-0", [(1, 0, 5 * GIB, 1), (2, 0, 5 * GIB, 1)])])
    with pytest.raises(shape_gate.ShapeGateFailure) as caught:
        _run(tmp_path, table)
    assert caught.value.reason == "did_not_test"
    assert not (tmp_path / "gate" / "pool").exists()


def test_the_reference_table_exercises_what_the_gate_claims() -> None:
    """The committed table: the real entry count, a cut inside an entry, shape kept.

    The chunk is the checkout's own policy, in real GiB: the cut is counted
    where production would make it.
    """

    table = shape_gate.read_table(REFERENCE)
    source = table["source"]
    assert (source["entry_count"], source["phase_count"],
            source["total_bytes"]) == (9255, 6, 188669721544)
    policy = tier_loop.load_ram_policy()
    assert policy is not None
    chunk = storage_tiers.promotion_chunk_gib_for_window(
        int(policy["window_gib_default"]), policy.get("promotion_chunk_gib")) * GIB
    shape = shape_gate.scaled_shape(table, scale=1)
    coverage = shape_gate.shape_coverage(shape, chunk_bytes=chunk)
    assert coverage["phases_over_chunk"] == ["chain-044", "chain-040"]
    assert coverage["byte_cuts_inside_entries"] >= 1
    scaled = shape_gate.scaled_shape(table, scale=shape_gate.SCALE)
    assert (scaled["multi_range_files"], scaled["overlapping_files"]) == (6, 4)
