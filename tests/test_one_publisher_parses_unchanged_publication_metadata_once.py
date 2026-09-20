"""One publisher reads unchanged publication metadata once, not once per file (#761).

The measured defect: ``_StagedPublisher._proof_search`` runs per destination --
tens of thousands of times for one mover, and again on every
``_PUBLISH_GRACE_S`` poll -- and each run re-opens and re-parses *every*
consumer fragment under the residency root plus the material sidecar of every
fragment that names the path.  Nothing parsed is reused, even when no file
changed.  A read-only profile of one such lookup against the live residency
root (PB action ``cf90fe6f6e42``, py-spy blob ``f171dbcb8499``, 350 samples at
100 Hz) put 2.76 of 3.50 sampled seconds under ``_proof_search``, 2.19 of them
in ``json.load`` inside ``_proof_candidate``; the root held 118 candidate
fragment files totalling 334.6 MB at the time.  That parse also runs inside
``queue.stage_ownership_lock``, so it serializes every copy worker.

These cases are about *work*, not wall clock.  They count the metadata files
the publisher opens, which is the quantity the fix removes and the one a timer
cannot confound:

* ``test_a_second_lookup_parses_no_metadata_file_again`` -- the RED case.  Five
  distinct destinations under one publisher, then the same five again.  Before
  the fix every lookup re-reads and re-parses every fragment and sidecar; after
  it, lookups past the first parse nothing while returning the same verdicts.
* the invalidation cases -- a rewritten, added, removed, corrupted or
  unreadable record must be seen *before* the next decision, and an unreadable
  one must never cache as success.
* the ceiling cases -- churn under a ceiling small enough to reach must stay
  bounded *and* leave reuse working, because an accounting that forgets to
  discount a dropped record degrades silently back to re-parsing everything.
* ``test_a_shared_source_and_a_stale_donor_still_decide_as_before`` -- the two
  scopes the brief names, exercised through the same publisher instance that
  did the reuse, so reuse cannot quietly hold a verdict the forest no longer
  supports.

The count that decides these cases is **JSON parses**, not opens.  A syscall
counter cannot tell reuse from a reader rewritten onto another API --
``builtins.open``, ``io.open`` (what ``Path.open`` reaches) and ``os.open`` are
three doors to the same work, and a reader that moved between them would turn
an open-counting test green while eliminating nothing.  Parsing is the work
#761 removes (2.19 of 3.50 sampled seconds inside ``json.load``), and any
reader that reaches a verdict about a fragment must decode it, so the parse
count survives any rewrite of the reader.  Opens are counted through all three
doors as well, but only as corroboration.  ``_Work.assert_live`` fails when an
instrument observed nothing at all, so a counter that silently stopped counting
cannot pass a case by measuring zero work.

Nothing here rehashes a payload, writes a cache to disk or changes a schema.
"""
from __future__ import annotations

import builtins
import hashlib
import io
import json
import os
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
from prismabuild import pool, reader_lease, residency_map  # noqa: E402
import stage_move  # noqa: E402

TIER = "prismabuild-stage:dl380g10"
MANIFEST = "4" * 64

#: Five destinations one mover publishes, plus noise consumers that name none
#: of them -- the live shape, where most fragments are irrelevant to any one
#: lookup and are re-parsed for it anyway.
DESTINATIONS = 5
NOISE_CONSUMERS = 3
NOISE_ENTRIES = 40


def _key(seed: str) -> str:
    """A distinct 64-hex action key per name, without hashing a real body."""

    return (seed.encode().hex() * 64)[:64]


CONSUMER = _key("consumer0")
MOVER = _key("mover0")
OTHER_CONSUMER = _key("otherconsumer0")
OTHER_MOVER = _key("othermover0")


class _Work:
    """The work one publisher does on publication metadata, counted.

    ``parses`` counts JSON decodes and is the load-bearing count.  It is
    implementation-independent: whichever door a reader uses for the bytes,
    a verdict about a fragment requires decoding it, so a reader rewritten
    onto another IO API cannot make this number fall without actually
    eliminating the parse.  Only ``json.loads`` is wrapped, because
    ``json.load`` reaches it through the module global and wrapping both
    would score one decode twice.

    ``opens`` corroborates, through all three doors that reach a file:
    ``builtins.open``, ``io.open`` (which is the same object in CPython and
    is what ``Path.open``/``Path.read_bytes`` call) and ``os.open``.  The
    wrapper is installed under both ``open`` names, so one call is counted
    once, not twice.

    ``assert_live`` refuses a window in which an instrument saw nothing at
    all: a counter that silently stopped counting must fail a case, never
    pass one by measuring zero work.
    """

    def __init__(self, monkeypatch, root: Path) -> None:
        self.root = str(Path(root).resolve())
        self.paths: list[str] = []
        self.parses = 0
        real_open, real_os_open = builtins.open, os.open
        real_loads = json.loads

        def note(candidate) -> None:
            try:
                resolved = os.fspath(candidate)
            except TypeError:
                return
            if isinstance(resolved, bytes):
                resolved = resolved.decode(errors="replace")
            if isinstance(resolved, str) and resolved.startswith(self.root):
                self.paths.append(resolved)

        def counted_open(file, *args, **kwargs):
            note(file)
            return real_open(file, *args, **kwargs)

        def counted_os_open(path, *args, **kwargs):
            note(path)
            return real_os_open(path, *args, **kwargs)

        def counted_loads(raw, **kwargs):
            self.parses += 1
            return real_loads(raw, **kwargs)

        monkeypatch.setattr(builtins, "open", counted_open)
        monkeypatch.setattr(io, "open", counted_open)
        monkeypatch.setattr(os, "open", counted_os_open)
        monkeypatch.setattr(json, "loads", counted_loads)

    def reset(self) -> None:
        self.paths.clear()
        self.parses = 0

    @property
    def opens(self) -> int:
        return len(self.paths)

    def assert_live(self, where: str) -> None:
        assert self.parses > 0, (
            f"the parse counter saw nothing during {where}; an instrument "
            f"that stopped counting must fail, not pass")
        assert self.opens > 0, (
            f"the open counter saw nothing during {where}; an instrument "
            f"that stopped counting must fail, not pass")


@pytest.fixture()
def forest(tmp_path, monkeypatch):
    """A stage root, a residency forest that proves it, and a publisher."""

    stage_root = tmp_path / "stage"
    residency_root = tmp_path / "residency"
    (stage_root / "payload").mkdir(parents=True)
    residency_root.mkdir()

    destinations: list[dict[str, object]] = []
    fragment_entries: dict[str, dict[str, object]] = {}
    material_entries: dict[str, dict[str, object]] = {}
    for index in range(DESTINATIONS):
        body = f"entry-{index}".encode() * (index + 3)
        path = stage_root / "payload" / f"object-{index}.bin"
        path.write_bytes(body)
        source = f"/mnt/shared/pool/object-{index}.bin"
        norm = os.path.normpath(str(path))
        digest = hashlib.sha256(body).hexdigest()
        key = residency_map.residency_map_key(source, 0)
        fragment_entries[key] = {
            "stage_path": norm, "bytes": len(body), "offset": 0,
            "sha256": digest,
        }
        material_entries[key] = {
            "stage_path": norm, "bytes": len(body), "sha256": digest,
            "file_id": reader_lease.stat_identity(norm),
        }
        destinations.append(
            {"norm": norm, "bytes": len(body), "sha256": digest, "key": key})

    def write_fragment(consumer: str, mover: str,
                       entries: dict[str, dict[str, object]]) -> Path:
        return residency_map.write_fragment(residency_root, {
            "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
            "consumer_action_key": consumer, "mover_action_key": mover,
            "tier_id": TIER, "stage_root": str(stage_root),
            "manifest_sha256": MANIFEST, "entries": entries,
        })

    def write_material(consumer: str, mover: str,
                       entries: dict[str, dict[str, object]]) -> Path:
        return reader_lease.write_material(
            residency_root, consumer_action_key=consumer,
            mover_action_key=mover, tier_id=TIER, stage_root=str(stage_root),
            manifest_sha256=MANIFEST, generation=reader_lease.mint_generation(),
            entries=entries)

    write_fragment(CONSUMER, MOVER, fragment_entries)
    write_material(CONSUMER, MOVER, material_entries)

    # Consumers that name nothing this mover publishes: every lookup pays for
    # them today, which is most of the live 334.6 MB.
    for noise in range(NOISE_CONSUMERS):
        consumer, mover = _key(f"noise{noise}c"), _key(f"noise{noise}m")
        entries = {}
        for item in range(NOISE_ENTRIES):
            source = f"/mnt/shared/pool/noise-{noise}-{item}.bin"
            entries[residency_map.residency_map_key(source, 0)] = {
                "stage_path": os.path.normpath(
                    str(stage_root / "payload" / f"noise-{noise}-{item}.bin")),
                "bytes": 64, "offset": 0, "sha256": "e" * 64,
            }
        write_fragment(consumer, mover, entries)

    publisher = stage_move._StagedPublisher(
        queue=pool.PoolQueue(tmp_path / "pool"), stage_root=stage_root,
        residency_root=residency_root, mover_action_key=_key("thismover0"),
        manifest_sha256=MANIFEST, tier_id=TIER,
        cas_root=str(tmp_path / "pool" / "cas"))

    return {
        "stage_root": stage_root, "residency_root": residency_root,
        "publisher": publisher, "destinations": destinations,
        "fragment_entries": fragment_entries,
        "material_entries": material_entries,
        "write_fragment": write_fragment, "write_material": write_material,
        "work": _Work(monkeypatch, residency_root),
    }


def _sweep(forest) -> list[tuple]:
    """One pass over every destination, as a mover's workers would."""

    out = []
    for item in forest["destinations"]:
        proof, standing, detail = forest["publisher"]._proof_search(
            str(item["norm"]), int(item["bytes"]), item["sha256"])
        out.append((proof is not None, standing, detail))
    return out


def _assert_cardinality_is_bounded(publisher, where: str) -> None:
    """Every retained row costs at least the per-record overhead.

    This is what bounds *cardinality*: the ceiling is bytes, so it bounds
    the number of rows only if no row is free.  Residue is allowed between
    reclaims -- a fragment that leaves the forest is never visited again, so
    its entry waits for the reclaim that pressure triggers -- but it is
    always paid for, so it can never exceed the ceiling however long it
    waits.
    """

    floor = len(publisher._fragments) * stage_move._RECORD_OVERHEAD_BYTES
    assert floor <= publisher._index_bytes(), (
        f"{where}: {len(publisher._fragments)} retained rows are charged "
        f"{publisher._index_bytes()} bytes in total, less than the "
        f"{stage_move._RECORD_OVERHEAD_BYTES} bytes a single row costs; a "
        f"row priced below its overhead lets cardinality escape a byte "
        f"ceiling entirely")
    assert (publisher._index_bytes()
            <= stage_move._INDEX_BUDGET_BYTES), f"{where}: past the ceiling"


# --------------------------------------------------------------------------
# The instrument itself
# --------------------------------------------------------------------------

def test_the_instrument_sees_every_door_a_reader_can_use(forest):
    """A counter that misses a reader's API would pass a false green.

    An implementation that re-read every file through ``os.open`` or
    ``Path.open`` while parsing it every time has eliminated no work, and a
    counter blind to that door would call it reuse.  This pins all three
    doors and the parse counter against known calls, so the cases below
    cannot pass by measuring work the instrument cannot see.
    """

    work = forest["work"]
    fragment = residency_map.fragment_path(
        forest["residency_root"], CONSUMER, MOVER)

    work.reset()
    json.loads(fragment.read_bytes())            # Path.open -> io.open
    assert (work.parses, work.opens) == (1, 1), "io.open door unseen"

    work.reset()
    with open(fragment) as stream:               # builtins.open
        json.load(stream)                     # delegates to json.loads
    assert (work.parses, work.opens) == (1, 1), "builtins.open door unseen"

    work.reset()
    handle = os.open(fragment, os.O_RDONLY)      # os.open
    try:
        assert (work.parses, work.opens) == (0, 1), "os.open door unseen"
    finally:
        os.close(handle)


# --------------------------------------------------------------------------
# The RED case
# --------------------------------------------------------------------------

def test_a_second_lookup_parses_no_metadata_file_again(forest):
    """RED before #761: every destination re-parses the whole forest.

    Five distinct destinations, then the same five again, against a residency
    root nothing touched in between.  The verdicts must be identical both
    passes -- reuse that changed an answer would be a defect, not a saving --
    and the second pass must parse no metadata file at all, because every one
    of them was already decoded at a version that has not moved.

    Parses, not opens, decide this: a reader moved onto another IO API would
    still have to decode what it read, so this number cannot fall without the
    work actually going away.  On the pre-fix tree the second pass parses
    exactly what the first did and this fails.
    """

    work = forest["work"]
    work.reset()
    first = _sweep(forest)
    first_parses, first_opens = work.parses, work.opens
    work.assert_live("the first pass")

    work.reset()
    second = _sweep(forest)
    second_parses, second_opens = work.parses, work.opens

    assert [row[1] for row in first] == ["proof"] * DESTINATIONS, first
    assert second == first, "reuse changed a verdict"
    assert second_parses == 0, (
        f"the second pass re-parsed {second_parses} metadata files under an "
        f"unchanged residency root (the first pass parsed {first_parses}); "
        f"unchanged publication metadata must be parsed once per version")
    assert second_opens == 0, (
        f"the second pass re-opened {second_opens} metadata files under an "
        f"unchanged residency root (the first pass opened {first_opens})")


def test_one_pass_parses_each_metadata_file_at_most_once(forest):
    """Even inside a single pass, one file is not re-parsed per destination.

    The five destinations live in one fragment with one sidecar, beside three
    noise fragments.  Today every one of those five files is parsed once per
    destination; the bound is one parse per distinct file, whatever the
    destination count, and no file opened twice.
    """

    work = forest["work"]
    work.reset()
    _sweep(forest)
    work.assert_live("one pass")

    distinct = len(set(work.paths))
    assert work.parses <= distinct, (
        f"{work.parses} parses for {distinct} distinct metadata files over "
        f"{DESTINATIONS} destinations; unchanged metadata must be parsed "
        f"once per version, not once per destination")
    duplicates = {path for path in work.paths if work.paths.count(path) > 1}
    assert not duplicates, (
        f"{len(duplicates)} metadata file(s) were opened more than once for "
        f"{DESTINATIONS} destinations: {sorted(duplicates)[:3]}")


# --------------------------------------------------------------------------
# The ceiling: bounded, and never permanently in the way
# --------------------------------------------------------------------------

@pytest.mark.skipif(os.geteuid() == 0, reason="root reads an unreadable file")
def test_repeatedly_unreadable_metadata_does_not_drift_the_accounting(forest):
    """The reachable leak: a drop path that forgets what it dropped (#761 review).

    A cached fragment that becomes unreadable is dropped on the very next
    lookup -- that is the fail-closed behaviour, and it is the one drop path
    a running mover actually reaches, because a file that *vanishes* is
    never visited again.  If that drop does not discount what it held, the
    index's accounting climbs by the record's whole size every time the file
    flickers, while the index holds no more than before.  Nothing is wrong
    with any verdict, so only the accounting shows it.

    Ten flickers here, and the index must cost exactly what it cost before
    them.  A drop that forgets would report ten times the content.
    """

    publisher = forest["publisher"]
    fragment = residency_map.fragment_path(
        forest["residency_root"], CONSUMER, MOVER)
    mode = fragment.stat().st_mode

    _sweep(forest)
    settled = publisher._index_bytes()
    assert settled > 0, "nothing was retained, so nothing is being measured"

    for flicker in range(10):
        fragment.chmod(0o000)
        try:
            assert _sweep(forest)[0][1] == "unknown"
        finally:
            fragment.chmod(mode)
        assert [row[1] for row in _sweep(forest)] == ["proof"] * DESTINATIONS
        assert publisher._index_bytes() == settled, (
            f"after {flicker + 1} unreadable/readable cycles the index "
            f"reports {publisher._index_bytes()} bytes against {settled} "
            f"before, holding the same content; a drop that does not "
            f"discount what it dropped drifts upward until the ceiling "
            f"rejects everything and reuse is dead for the process")


def test_churn_stays_bounded_and_leaves_reuse_working(forest, monkeypatch):
    """Churn under a reachable ceiling must bound memory *and* keep reuse.

    Fragments come and go over a mover's life.  One that disappears is never
    visited again, so nothing on the per-lookup path can drop its entry --
    only a reclaim under pressure can, and if none happens the index grows
    with every fragment ever seen.  This adds and deletes under a ceiling
    small enough to reach, then restores room and checks reuse still works.

    The size half alone would pass while reuse was dead; the reuse half
    alone would pass while memory grew.  Both are asserted.
    """

    publisher, work = forest["publisher"], forest["work"]
    real = stage_move._INDEX_BUDGET_BYTES
    _sweep(forest)
    tiny = publisher._index_bytes() * 2
    monkeypatch.setattr(stage_move, "_INDEX_BUDGET_BYTES", tiny)

    for round_ in range(12):
        consumer, mover = _key(f"churn{round_}c"), _key(f"churn{round_}m")
        forest["write_fragment"](consumer, mover, forest["fragment_entries"])
        assert [row[1] for row in _sweep(forest)] == ["proof"] * DESTINATIONS
        assert publisher._index_bytes() <= tiny, (
            f"round {round_}: the index passed its ceiling")

        residency_map.fragment_path(
            forest["residency_root"], consumer, mover).unlink()
        _sweep(forest)
        assert publisher._index_bytes() <= tiny, (
            f"round {round_}: the index passed its ceiling after a delete")
        _assert_cardinality_is_bounded(publisher, f"round {round_}")

        # Replacement churn too: same content, new version every round.
        forest["write_fragment"](CONSUMER, MOVER, forest["fragment_entries"])
        forest["write_material"](CONSUMER, MOVER, forest["material_entries"])
        _sweep(forest)
        assert publisher._index_bytes() <= tiny, (
            f"round {round_}: the index passed its ceiling on replacement")

    # The ceiling was a ceiling, not a one-way door: with room again, reuse
    # works.  A drifted accounting would still be rejecting everything here.
    monkeypatch.setattr(stage_move, "_INDEX_BUDGET_BYTES", real)
    first = _sweep(forest)
    work.reset()
    again = _sweep(forest)
    assert again == first, "reuse changed a verdict after churn"
    assert work.parses == 0, (
        f"after churn the second pass re-parsed {work.parses} files: the "
        f"ceiling disabled reuse permanently instead of bounding it")


def test_records_that_name_no_path_still_cost_and_are_bounded(forest,
                                                               monkeypatch):
    """A row priced at zero is a row that can be added without limit.

    Fragments that parse but are not residency fragments -- and fragments
    that name nothing -- retain no paths, so an accounting that prices only
    paths prices them at nothing and admits them without end.  That is the
    same failure class as an accounting that forgets what it dropped:
    something grows while nothing counts it.

    Three hundred such files against a nearly full index.  The index must
    stay inside its ceiling *and* stop taking rows; size alone would pass
    while cardinality ran away.
    """

    publisher = forest["publisher"]
    _sweep(forest)
    settled = len(publisher._fragments)
    monkeypatch.setattr(stage_move, "_INDEX_BUDGET_BYTES",
                        publisher._index_bytes() + 4096)

    for index in range(300):
        directory = forest["residency_root"] / _key(f"empty{index}c")
        directory.mkdir(exist_ok=True)
        (directory / f"{_key(f'empty{index}m')}.json").write_text(
            '{"schema": "not-a-residency-fragment"}')

    assert [row[1] for row in _sweep(forest)] == ["proof"] * DESTINATIONS
    assert publisher._index_bytes() <= stage_move._INDEX_BUDGET_BYTES, (
        "the index passed its ceiling on rows that name no path")
    assert len(publisher._fragments) < settled + 300, (
        f"the index took {len(publisher._fragments) - settled} of 300 rows "
        f"that name no path while reporting "
        f"{publisher._index_bytes()} bytes; a row charged nothing evades "
        f"the ceiling however large the ceiling is")
    _assert_cardinality_is_bounded(publisher, "rows that name no path")


def test_a_long_path_is_charged_its_length_not_a_flat_figure(tmp_path):
    """A flat per-path price under-charges the input most likely to blow it.

    Path lengths are not a constant and a unicode path is not one byte per
    character, so a flat figure is a guess standing in for something the
    code can read exactly.  This builds a forest of one fragment naming one
    very long path and asserts the index charges at least what that path
    measures -- which a flat figure, by construction, does not.
    """

    stage_root, residency_root = tmp_path / "stage", tmp_path / "residency"
    residency_root.mkdir()
    deep = stage_root / ("directory" * 14) / ("segment" * 16)
    deep.mkdir(parents=True)
    body = b"long-path-payload"
    destination = deep / (("name" * 24) + ".bin")
    destination.write_bytes(body)
    norm = os.path.normpath(str(destination))
    digest = hashlib.sha256(body).hexdigest()
    key = residency_map.residency_map_key("/mnt/shared/pool/long.bin", 0)

    residency_map.write_fragment(residency_root, {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": CONSUMER, "mover_action_key": MOVER,
        "tier_id": TIER, "stage_root": str(stage_root),
        "manifest_sha256": MANIFEST,
        "entries": {key: {"stage_path": norm, "bytes": len(body),
                          "offset": 0, "sha256": digest}}})

    publisher = stage_move._StagedPublisher(
        queue=pool.PoolQueue(tmp_path / "pool"), stage_root=stage_root,
        residency_root=residency_root, mover_action_key=_key("thismover0"),
        manifest_sha256=MANIFEST, tier_id=TIER,
        cas_root=str(tmp_path / "pool" / "cas"))

    assert publisher._proof_search(norm, len(body), digest)[1] == "owned"
    assert len(norm) > 400, "the fixture must actually be a long path"
    assert publisher._index_bytes() >= len(norm), (
        f"the index charges {publisher._index_bytes()} bytes for a record "
        f"whose single retained path is {len(norm)} characters long; a flat "
        f"per-path price under-counts exactly the inputs most likely to "
        f"exhaust the budget")


def test_a_record_too_big_for_the_ceiling_still_decides_correctly(forest,
                                                                  monkeypatch):
    """Over the ceiling is uncached, never wrong and never a held verdict."""

    monkeypatch.setattr(stage_move, "_INDEX_BUDGET_BYTES", 1)
    assert [row[1] for row in _sweep(forest)] == ["proof"] * DESTINATIONS
    assert forest["publisher"]._index_bytes() == 0, (
        "nothing may be retained under a ceiling nothing fits in")

    # And the interned table is not a back door: a record that was priced
    # and refused must leave nothing behind in it.
    assert forest["publisher"]._interned == {}


# --------------------------------------------------------------------------
# Invalidation: a decision is never made on evidence that moved
# --------------------------------------------------------------------------

def test_a_rewritten_sidecar_is_seen_before_the_next_decision(forest):
    """A sidecar that stops dating the current incarnation stops proving it."""

    target = forest["destinations"][0]
    assert _sweep(forest)[0][1] == "proof"

    stale = dict(forest["material_entries"])
    stale[str(target["key"])] = {
        **stale[str(target["key"])],
        "file_id": {"ino": 1, "size": 1, "mtime_ns": 1, "ctime_ns": 1},
    }
    forest["write_material"](CONSUMER, MOVER, stale)

    proof, standing, _ = forest["publisher"]._proof_search(
        str(target["norm"]), int(target["bytes"]), target["sha256"])
    assert proof is None and standing == "owned", (
        "a record dating a superseded incarnation must defer, not adopt")


def test_a_rewritten_fragment_digest_is_seen_before_the_next_decision(forest):
    """A sidecar digest that contradicts the manifest refuses, after reuse."""

    target = forest["destinations"][1]
    assert _sweep(forest)[1][1] == "proof"

    wrong = dict(forest["material_entries"])
    wrong[str(target["key"])] = {
        **wrong[str(target["key"])], "sha256": "b" * 64}
    forest["write_material"](CONSUMER, MOVER, wrong)

    proof, standing, _ = forest["publisher"]._proof_search(
        str(target["norm"]), int(target["bytes"]), target["sha256"])
    assert proof is None and standing == "divergent"


def test_an_added_fragment_is_seen_before_the_next_decision(forest):
    """A publication that lands after the first lookup is not missed."""

    stage_root = forest["stage_root"]
    body = b"late-arrival" * 4
    path = stage_root / "payload" / "late.bin"
    path.write_bytes(body)
    norm = os.path.normpath(str(path))
    digest = hashlib.sha256(body).hexdigest()

    proof, standing, _ = forest["publisher"]._proof_search(
        norm, len(body), digest)
    assert proof is None and standing == "clean"

    source = "/mnt/shared/pool/late.bin"
    key = residency_map.residency_map_key(source, 0)
    entries = {key: {"stage_path": norm, "bytes": len(body), "offset": 0,
                     "sha256": digest}}
    forest["write_fragment"](OTHER_CONSUMER, OTHER_MOVER, entries)
    forest["write_material"](OTHER_CONSUMER, OTHER_MOVER, {
        key: {"stage_path": norm, "bytes": len(body), "sha256": digest,
              "file_id": reader_lease.stat_identity(norm)}})

    proof, standing, _ = forest["publisher"]._proof_search(
        norm, len(body), digest)
    assert standing == "proof" and proof is not None


def test_a_removed_fragment_is_seen_before_the_next_decision(forest):
    """A fragment that goes away stops vouching on the very next lookup."""

    target = forest["destinations"][2]
    assert _sweep(forest)[2][1] == "proof"

    residency_map.fragment_path(
        forest["residency_root"], CONSUMER, MOVER).unlink()

    proof, standing, _ = forest["publisher"]._proof_search(
        str(target["norm"]), int(target["bytes"]), target["sha256"])
    assert proof is None and standing == "clean"


def test_a_corrupted_fragment_taints_and_heals_without_a_cached_verdict(forest):
    """Corruption is unknown, and a repair is seen on the next lookup."""

    target = forest["destinations"][3]
    assert _sweep(forest)[3][1] == "proof"

    fragment = residency_map.fragment_path(
        forest["residency_root"], CONSUMER, MOVER)
    good = fragment.read_bytes()
    fragment.write_bytes(b"{not json")

    proof, standing, detail = forest["publisher"]._proof_search(
        str(target["norm"]), int(target["bytes"]), target["sha256"])
    assert proof is None and standing == "unknown" and detail

    fragment.write_bytes(good)
    proof, standing, _ = forest["publisher"]._proof_search(
        str(target["norm"]), int(target["bytes"]), target["sha256"])
    assert standing == "proof" and proof is not None


@pytest.mark.skipif(os.geteuid() == 0, reason="root reads an unreadable file")
def test_an_unreadable_fragment_is_unknown_and_never_a_cached_success(forest):
    """A fresh read that fails is unknown -- not a verdict held from before."""

    target = forest["destinations"][4]
    assert _sweep(forest)[4][1] == "proof"

    fragment = residency_map.fragment_path(
        forest["residency_root"], CONSUMER, MOVER)
    mode = fragment.stat().st_mode
    fragment.chmod(0o000)
    try:
        proof, standing, detail = forest["publisher"]._proof_search(
            str(target["norm"]), int(target["bytes"]), target["sha256"])
        assert proof is None and standing == "unknown" and detail, (
            "an unreadable fragment must fail closed, never reuse the "
            "success it last parsed")
    finally:
        fragment.chmod(mode)

    proof, standing, _ = forest["publisher"]._proof_search(
        str(target["norm"]), int(target["bytes"]), target["sha256"])
    assert standing == "proof" and proof is not None


# --------------------------------------------------------------------------
# The two scopes the brief names, through the reusing publisher
# --------------------------------------------------------------------------

def test_a_shared_source_and_a_stale_donor_still_decide_as_before(forest):
    """Two consumers name one staged path; the stale one must not decide it.

    The #755 shape, run through a publisher that has already reused this
    forest: a donor whose sidecar dates a superseded inode is ``stale`` --
    skipped, never divergent -- and the current incarnation's own record still
    proves the bytes.  Order matters (the stale donor sorts first), so this
    also pins that reuse did not reorder the search.
    """

    target = forest["destinations"][0]
    norm, want, digest = (str(target["norm"]), int(target["bytes"]),
                          target["sha256"])
    assert _sweep(forest)[0][1] == "proof"

    donor_consumer, donor_mover = _key("adonor0"), _key("adonormover0")
    entries = {str(target["key"]): {
        "stage_path": norm, "bytes": want, "offset": 0, "sha256": digest}}
    forest["write_fragment"](donor_consumer, donor_mover, entries)
    forest["write_material"](donor_consumer, donor_mover, {
        str(target["key"]): {
            "stage_path": norm, "bytes": want, "sha256": digest,
            "file_id": {"ino": 99, "size": want, "mtime_ns": 7,
                        "ctime_ns": 7}}})

    proof, standing, _ = forest["publisher"]._proof_search(norm, want, digest)
    assert standing == "proof" and proof is not None, (
        "a stale donor record must be skipped, not treated as divergence")

    # And the shared name is still proven when the *current* record is the
    # only one that dates it: drop this mover's own proof and the stale donor
    # alone must not adopt.
    residency_map.fragment_path(
        forest["residency_root"], CONSUMER, MOVER).unlink()
    proof, standing, _ = forest["publisher"]._proof_search(norm, want, digest)
    assert proof is None and standing == "owned", (
        "a stale donor alone proves nothing and permits no replacement")
