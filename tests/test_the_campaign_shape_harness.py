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
*   the committed reference tables have the properties the gate claims for
    them, and the registry names exactly the tables on disk;
*   a gate receipt is judged on its outcomes, its tree and its terminal
    record, and each way one can be wrong refuses by name.

Every root is under ``tmp_path``; ``tests/conftest.py`` repoints ``pbrun.SH``
there, and the harness refuses any other.
"""
from __future__ import annotations

import hashlib
import json
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
LAYER_44 = shape_gate.TABLE_ROOT / "bc2a3bc8ad11.json.gz"


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
        tmp_path: Path, small_units, capsys: pytest.CaptureFixture[str]) -> None:
    """Every entry lands once per tier and is read back from RAM, pool untouched."""

    result = _run(tmp_path, _campaign_like())
    with capsys.disabled():
        print(json.dumps({"shape_gate": "campaign-like", "result": result},
                         sort_keys=True))

    total = result["manifest_bytes"]
    assert result["coverage"]["phases_over_chunk"] == ["chain-1"]
    assert result["coverage"]["byte_cuts_inside_entries"] >= 1
    assert result["chunks"]["stage"] > result["phases"]
    assert result["chunks"]["ram"] > result["phases"]
    assert result["overlapping_files"] == 1
    assert result["digest_less_entries"] == 7
    assert result["read_bytes"] == total
    assert (result["stage_bytes_moved"], result["ram_bytes_moved"],
            result["bytes_read_strictly"]) == (total, total, total)
    assert result["entries_read_strictly"] == result["entries"]
    assert result["unique_entries_read"] == result["entries"]
    assert result["pool_opens"] == {"promote": 0, "read": 0, "egress": 0}
    assert total > result["ram_window_bytes"] and result["ram_egresses"] >= 1
    assert result["ram_slide"] is True
    assert result["ram_peak_bytes"] <= result["ram_window_bytes"]
    assert result["coverage"]["chunk_edge_inside_entry"] is True


def test_a_plan_that_reads_an_entry_again_moves_and_reads_it_again(
        tmp_path: Path, small_units) -> None:
    """A v2 revisit is staged again: each tier's frontier is linear in read bytes.

    The GLM layer-44 readset reads one 8 GiB spill plane in four phases.  A
    revisit is moved and read again, so the bytes each tier moves are the
    plan's read bytes, not the manifest's unique bytes, and still no more.
    """

    table = _campaign_like()
    head = table["phases"][0]["entry_indices"]
    table["phases"].append({"name": "revisit-head", "entry_indices": list(head)})
    table["source"]["phase_count"] += 1

    result = _run(tmp_path, table)

    total, reads = result["manifest_bytes"], result["read_bytes"]
    assert reads == total + sum(max(1, table["entries"][index][2] // SMALL_SCALE)
                                for index in head)
    assert (result["stage_bytes_moved"], result["ram_bytes_moved"],
            result["bytes_read_strictly"]) == (reads, reads, reads)
    assert result["entry_reads"] == result["entries"] + len(head)
    assert result["entries_read_strictly"] == result["entry_reads"]
    assert result["unique_entries_read"] == result["entries"]


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


def _real_chunk_bytes() -> int:
    policy = tier_loop.load_ram_policy()
    assert policy is not None
    return storage_tiers.promotion_chunk_gib_for_window(
        int(policy["window_gib_default"]), policy.get("promotion_chunk_gib")) * GIB


def test_the_layer_44_table_is_the_breadth_table() -> None:
    """GLM layer-44 Stage B: many phases, no phase over a chunk, under the window."""

    table = shape_gate.read_table(LAYER_44)
    source = table["source"]
    assert (source["entry_count"], source["phase_count"],
            source["total_bytes"]) == (10344, 22, 148264339117)
    coverage = shape_gate.shape_coverage(
        shape_gate.scaled_shape(table, scale=1), chunk_bytes=_real_chunk_bytes())
    assert coverage["phases_over_chunk"] == []
    assert coverage["byte_cuts_inside_entries"] == 0


def test_the_registry_names_exactly_the_committed_tables() -> None:
    """Every table on disk is gated, each by its bytes, and one cuts an entry.

    The flag each table carries is the table's real-GiB coverage under the
    checkout's own policy, so a registry cannot claim a chunk edge its table
    does not have -- and the gate set as a whole must exercise one.
    """

    on_disk = {path.name.removesuffix(".json.gz")
               for path in shape_gate.TABLE_ROOT.glob("*.json.gz")}
    assert set(shape_gate.REFERENCE_TABLES) == on_disk
    chunk = _real_chunk_bytes()
    for name, spec in shape_gate.REFERENCE_TABLES.items():
        path = shape_gate.TABLE_ROOT / f"{name}.json.gz"
        assert hashlib.sha256(path.read_bytes()).hexdigest() == spec["sha256"], name
        coverage = shape_gate.shape_coverage(
            shape_gate.scaled_shape(shape_gate.read_table(path), scale=1),
            chunk_bytes=chunk)
        assert bool(coverage["byte_cuts_inside_entries"]) == spec["chunk_edge_inside_entry"], name
    assert any(spec["chunk_edge_inside_entry"]
               for spec in shape_gate.REFERENCE_TABLES.values())
    gate = Path(__file__).with_name("gate_campaign_shape.py").read_text()
    test = shape_gate.REFERENCE_TEST.split("::")[1]
    assert shape_gate.REFERENCE_TEST.startswith("tests/gate_campaign_shape.py::")
    assert f"def {test}(" in gate
    assert shape_gate.reference_nodes() == [
        f"{shape_gate.REFERENCE_TEST}[{name}]"
        for name in sorted(shape_gate.REFERENCE_TABLES)]


def _outcomes(**changes) -> dict:
    nodes = shape_gate.reference_nodes()
    record = {"schema": "prismabuild.pbtest_outcomes.v1", "collect_only": False,
              "collected": list(nodes),
              "reports": [[node, "call", "passed", None, None] for node in nodes],
              "uncounted": []}
    record.update(changes)
    return record


def test_a_gate_run_that_passed_every_table_is_accepted() -> None:
    shape_gate.judge_outcomes(_outcomes(), where="k")


@pytest.mark.parametrize("record", [
    None,
    _outcomes(collect_only=True),
    # Filtered to one table.
    _outcomes(collected=shape_gate.reference_nodes()[:1],
              reports=[[shape_gate.reference_nodes()[0], "call", "passed", None, None]]),
    # Ran something else as well.
    _outcomes(collected=[*shape_gate.reference_nodes(), "tests/test_x.py::test_y"]),
    # One table skipped, one failed, one passed but errored in teardown.
    _outcomes(reports=[[node, "call", "skipped", "no", None]
                       for node in shape_gate.reference_nodes()]),
    _outcomes(reports=[[node, "call", "failed", None, None]
                       for node in shape_gate.reference_nodes()]),
    _outcomes(reports=[row for node in shape_gate.reference_nodes()
                       for row in ([node, "call", "passed", None, None],
                                   [node, "teardown", "error", None, None])]),
    _outcomes(uncounted=[[shape_gate.reference_nodes()[0], "call", "passed"]]),
], ids=["no-record", "collect-only", "one-table", "extra-node", "skipped",
        "failed", "teardown-error", "uncounted"])
def test_a_gate_run_that_is_not_every_table_passing_is_refused(record) -> None:
    with pytest.raises(shape_gate.ShapeGateFailure) as caught:
        shape_gate.judge_outcomes(record, where="k")
    assert caught.value.reason == "receipt_refused"


def test_only_pbruns_closure_stamp_may_differ_from_the_commit() -> None:
    stamp = ".pbrun-closure." + "0" * 12 + ".json"
    assert shape_gate.closure_stamp_only([stamp], ".") == []
    assert shape_gate.closure_stamp_only([f"sub/{stamp}"], "sub") == []
    assert shape_gate.closure_stamp_only([f"sub/{stamp}"], ".") == [f"sub/{stamp}"]
    assert shape_gate.closure_stamp_only(
        [stamp, "tools/fleet/stage_move.py"], ".") == ["tools/fleet/stage_move.py"]


def test_an_action_key_resolves_by_a_unique_prefix_only(tmp_path: Path) -> None:
    queue = tmp_path / "pb-queue"
    (queue / "done").mkdir(parents=True)
    (queue / "failed").mkdir()
    one, two = "ab" * 32, "abcdef" + "0" * 58
    (queue / "done" / f"{one}.json").write_text("{}")
    (queue / "failed" / f"{two}.json").write_text("{}")
    assert shape_gate.resolve_action_key(queue, one[:12]) == (
        one, queue / "done" / f"{one}.json")
    assert shape_gate.resolve_action_key(queue, two.upper()[:12]) == (
        two, queue / "failed" / f"{two}.json")
    assert shape_gate.resolve_action_key(queue, one)[0] == one
    for bad in ("abab", "ab" * 3 + "zz" * 3, "abababababab"[:11], "0" * 12):
        with pytest.raises(shape_gate.ShapeGateFailure):
            shape_gate.resolve_action_key(queue, bad)
    (queue / "done" / f"{one[:12]}{'1' * 52}.json").write_text("{}")
    with pytest.raises(shape_gate.ShapeGateFailure, match="names 2 finished actions"):
        shape_gate.resolve_action_key(queue, one[:12])


@pytest.mark.parametrize("state, done", [
    ("failed", {"status": "failed", "detail": {"returncode": 1}}),
    ("done", {"status": "executed", "detail": {"returncode": 1}}),
    ("done", {"status": "executed"}),
])
def test_a_gate_run_that_did_not_exit_zero_is_refused(tmp_path: Path, state, done) -> None:
    queue = tmp_path / "pb-queue"
    (queue / state).mkdir(parents=True)
    key = "cd" * 32
    (queue / state / f"{key}.json").write_text(json.dumps(done))
    with pytest.raises(shape_gate.ShapeGateFailure,
                       match=f"did not finish executed.*{done['status']!r}"):
        shape_gate.verify_gate_receipt(
            action_key=key[:12], commit="a" * 40, checkout=tmp_path,
            queue_root=queue, cas_root=tmp_path / "cas")


def test_an_unreadable_terminal_record_is_refused_by_name(tmp_path: Path) -> None:
    queue = tmp_path / "pb-queue"
    (queue / "done").mkdir(parents=True)
    key = "ef" * 32
    (queue / "done" / f"{key}.json").write_text("not json")
    with pytest.raises(shape_gate.ShapeGateFailure, match="cannot be verified") as caught:
        shape_gate.verify_gate_receipt(
            action_key=key[:12], commit="a" * 40, checkout=tmp_path,
            queue_root=queue, cas_root=tmp_path / "cas")
    assert caught.value.reason == "receipt_refused"
