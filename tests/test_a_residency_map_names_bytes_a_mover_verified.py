"""A residency map is a claim that named bytes are on another device.

Everything here follows from that one sentence.  The claim has to be about an
*entry*, and a data manifest's entry is ``(path, offset)`` -- ``validate_data_
manifest`` refuses a repeated pair and permits one path at several offsets --
so a map keyed by path alone could not tell one range of a shard from another.
The claim has to be verified, so ``sha256`` is required on an entry even though
a manifest may carry ``null`` on every one of its own.  And the claim is made
by several movers at once, so it is composed from one file per mover rather
than read-modify-written into one file they would race on: rename is the only
concurrency primitive this fleet trusts on NFS, and a rename cannot merge.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import prismabuild.residency_map as rm  # noqa: E402

CONSUMER = "c" * 64
MOVER_A = "a" * 64
MOVER_B = "b" * 64
MANIFEST = "9" * 64
DIGEST_1 = "1" * 64
DIGEST_2 = "2" * 64
STAGE = "/stage/prewarm"


def _entry(name: str, offset: int = 0, *, sha256: str = DIGEST_1, size: int = 1024) -> dict:
    return {
        "stage_path": f"{STAGE}/{name}",
        "bytes": size,
        "offset": offset,
        "sha256": sha256,
    }


def _fragment(mover: str = MOVER_A, **overrides) -> dict:
    fragment = {
        "schema": rm.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": CONSUMER,
        "mover_action_key": mover,
        "tier_id": "prismabuild-stage:dl380g10",
        "stage_root": STAGE,
        "manifest_sha256": MANIFEST,
        "entries": {rm.residency_map_key("/mnt/shared/m/shard-1", 0): _entry("shard-1")},
    }
    fragment.update(overrides)
    return fragment


def test_one_path_at_two_offsets_is_two_entries() -> None:
    """The key is the manifest's own entry identity, not its path."""

    whole = rm.residency_map_key("/mnt/shared/m/shard-1", 0)
    tail = rm.residency_map_key("/mnt/shared/m/shard-1", 1048576)
    assert whole != tail
    assert rm.parse_residency_map_key(whole) == ("/mnt/shared/m/shard-1", 0)
    assert rm.parse_residency_map_key(tail) == ("/mnt/shared/m/shard-1", 1048576)


def test_a_colon_in_a_path_cannot_be_read_as_an_offset() -> None:
    """The split is on the first colon and the offset is digits, so it is injective."""

    key = rm.residency_map_key("/mnt/shared/odd:name:1", 42)
    assert rm.parse_residency_map_key(key) == ("/mnt/shared/odd:name:1", 42)


@pytest.mark.parametrize("key", ["", "/no/offset", "abc:/mnt/x", "12:", "12"])
def test_a_malformed_key_is_refused(key: str) -> None:
    with pytest.raises(rm.ResidencyMapError):
        rm.parse_residency_map_key(key)


def test_a_map_entry_must_carry_a_digest_even_though_a_manifest_need_not() -> None:
    """A copy nobody hashed cannot say the bytes are the bytes the manifest named."""

    entry = _entry("shard-1")
    del entry["sha256"]
    with pytest.raises(rm.ResidencyMapError, match="sha256"):
        rm.validate_entry(rm.residency_map_key("/m/s", 0), entry, stage_root=STAGE)
    with pytest.raises(rm.ResidencyMapError, match="sha256"):
        rm.validate_entry(
            rm.residency_map_key("/m/s", 0), _entry("shard-1", sha256="NOTHEX" * 10 + "abcd"),
            stage_root=STAGE)


def test_an_entry_cannot_point_outside_the_stage() -> None:
    """A map that could redirect a read anywhere is not a cache."""

    with pytest.raises(rm.ResidencyMapError, match="stage_path must live under"):
        rm.validate_entry(
            rm.residency_map_key("/m/s", 0),
            {"stage_path": "/etc/passwd", "bytes": 1, "offset": 0, "sha256": DIGEST_1},
            stage_root=STAGE)
    # A sibling dataset whose name merely starts with the stage root's text is
    # outside it too.
    with pytest.raises(rm.ResidencyMapError, match="stage_path must live under"):
        rm.validate_entry(
            rm.residency_map_key("/m/s", 0),
            {"stage_path": "/stage/prewarm-other/x", "bytes": 1, "offset": 0,
             "sha256": DIGEST_1},
            stage_root=STAGE)


def test_an_entry_offset_must_agree_with_its_key() -> None:
    with pytest.raises(rm.ResidencyMapError, match="disagrees with its key"):
        rm.validate_entry(
            rm.residency_map_key("/m/s", 0), _entry("shard-1", offset=4096),
            stage_root=STAGE)


@pytest.mark.parametrize("bad", [0, -1, "1024", 1024.0, True])
def test_an_entry_of_no_bytes_is_refused(bad: object) -> None:
    entry = _entry("shard-1")
    entry["bytes"] = bad
    with pytest.raises(rm.ResidencyMapError, match="bytes"):
        rm.validate_entry(rm.residency_map_key("/m/s", 0), entry, stage_root=STAGE)


def test_an_unknown_field_is_refused_rather_than_ignored() -> None:
    """A reader that ignores a field it does not know cannot be told anything new safely."""

    with pytest.raises(rm.ResidencyMapError, match="unknown residency map fragment fields"):
        rm.validate_fragment(_fragment(evicted=["/m/s"]))
    entry = _entry("shard-1") | {"warm": True}
    with pytest.raises(rm.ResidencyMapError, match="unknown residency map entry fields"):
        rm.validate_entry(rm.residency_map_key("/m/s", 0), entry, stage_root=STAGE)


def test_a_fragment_of_another_schema_is_refused() -> None:
    with pytest.raises(rm.ResidencyMapError, match="fragment schema"):
        rm.validate_fragment(_fragment(schema=rm.RESIDENCY_MAP_SCHEMA_V1))


def test_composing_two_movers_gives_the_consumer_both_ranges() -> None:
    first = _fragment(MOVER_A)
    second = _fragment(
        MOVER_B,
        entries={rm.residency_map_key("/mnt/shared/m/shard-1", 1024): _entry(
            "shard-1.range/1024-2048", offset=1024, sha256=DIGEST_2, size=2048)})
    composed = rm.compose([first, second])
    assert composed["schema"] == rm.RESIDENCY_MAP_SCHEMA_V1
    assert composed["leads"] == sorted([MOVER_A, MOVER_B])
    assert composed["generation"] == 2
    assert set(composed["entries"]) == {
        rm.residency_map_key("/mnt/shared/m/shard-1", 0),
        rm.residency_map_key("/mnt/shared/m/shard-1", 1024),
    }


def test_two_movers_that_staged_one_range_differently_refuse() -> None:
    """Picking either copy would make the map a guess about what is on the device."""

    first = _fragment(MOVER_A)
    second = _fragment(
        MOVER_B,
        entries={rm.residency_map_key("/mnt/shared/m/shard-1", 0): _entry(
            "shard-1", sha256=DIGEST_2)})
    with pytest.raises(rm.ResidencyMapError, match="staged .* differently"):
        rm.compose([first, second])
    # The same entry from two movers is not a conflict: a retry may restage it.
    assert rm.compose([first, _fragment(MOVER_B)])["generation"] == 2


@pytest.mark.parametrize(
    "field,value",
    [("consumer_action_key", "d" * 64), ("tier_id", "prismabuild-stage:sparky"),
     ("stage_root", "/stage/other"), ("manifest_sha256", "8" * 64)],
)
def test_fragments_about_different_work_do_not_compose(field: str, value: str) -> None:
    other = _fragment(MOVER_B, **{field: value})
    if field == "stage_root":
        # An entry has to live under its own fragment's stage root, so move it
        # too: what is under test here is the disagreement between fragments.
        other["entries"] = {
            rm.residency_map_key("/mnt/shared/m/shard-1", 0):
                _entry("shard-1") | {"stage_path": f"{value}/shard-1"}}
    with pytest.raises(rm.ResidencyMapError, match=f"disagree about {field}"):
        rm.compose([_fragment(MOVER_A), other])


def test_a_map_needs_at_least_one_fragment() -> None:
    with pytest.raises(rm.ResidencyMapError, match="at least one fragment"):
        rm.compose([])


def test_a_fragment_is_filed_under_one_writer_and_read_back(tmp_path: Path) -> None:
    """One file per mover is what makes the write a rename rather than a merge."""

    root = tmp_path / "residency"
    path_a = rm.write_fragment(root, _fragment(MOVER_A))
    path_b = rm.write_fragment(root, _fragment(MOVER_B, entries={
        rm.residency_map_key("/mnt/shared/m/shard-2", 0): _entry("shard-2")}))
    assert path_a == rm.fragment_path(root, CONSUMER, MOVER_A)
    assert path_a != path_b
    assert path_a.parent == path_b.parent
    read = rm.read_fragments(root, CONSUMER)
    assert [fragment["mover_action_key"] for fragment in read] == [MOVER_A, MOVER_B]


def test_an_unreadable_fragment_does_not_hide_the_copies_that_were_made(
        tmp_path: Path) -> None:
    """A half-written or foreign file must not cost a consumer the bytes it has."""

    root = tmp_path / "residency"
    rm.write_fragment(root, _fragment(MOVER_A))
    (root / CONSUMER / ("e" * 64 + ".json")).write_text("{not json")
    (root / CONSUMER / ("f" * 64 + ".json")).write_text(
        json.dumps(_fragment(MOVER_B, schema="something.else.v1")))
    read = rm.read_fragments(root, CONSUMER)
    assert [fragment["mover_action_key"] for fragment in read] == [MOVER_A]


def test_a_consumer_with_no_fragments_reads_an_empty_list(tmp_path: Path) -> None:
    assert rm.read_fragments(tmp_path / "residency", CONSUMER) == []


def test_the_map_round_trips_through_the_file_the_launcher_names(tmp_path: Path) -> None:
    path = tmp_path / "map.json"
    composed = rm.compose([_fragment(MOVER_A)])
    rm.write_map(path, composed)
    assert rm.read_map(path) == composed
    # The variable's name is the contract with the consumer.
    assert rm.RESIDENCY_MAP_ENV == "PRISMABUILD_RESIDENCY_MAP"


def test_a_written_map_is_renamed_into_place(tmp_path: Path) -> None:
    """A reader never sees half a document, and no temporary is left behind."""

    directory = tmp_path / "residency"
    path = directory / "map.json"
    rm.write_map(path, rm.compose([_fragment(MOVER_A)]))
    assert sorted(entry.name for entry in directory.iterdir()) == ["map.json"]


def test_an_invalid_map_is_refused_on_the_way_out_as_well_as_in(tmp_path: Path) -> None:
    composed = rm.compose([_fragment(MOVER_A)])
    composed["leads"] = [MOVER_A, MOVER_A]
    with pytest.raises(rm.ResidencyMapError, match="must not repeat a key"):
        rm.write_map(tmp_path / "map.json", composed)
    assert not (tmp_path / "map.json").exists()


def test_an_entry_the_map_does_not_name_falls_back_to_the_pool() -> None:
    """The fallback is the point: a map names copies, it does not forbid reads."""

    composed = rm.compose([_fragment(MOVER_A)])
    found = rm.lookup(composed, "/mnt/shared/m/shard-1", 0)
    assert found is not None and found["stage_path"] == f"{STAGE}/shard-1"
    assert rm.lookup(composed, "/mnt/shared/m/shard-1", 1024) is None
    assert rm.lookup(composed, "/mnt/shared/m/shard-9", 0) is None
    assert rm.lookup({}, "/mnt/shared/m/shard-1", 0) is None
