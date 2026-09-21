"""Bounded work for RAM-destination adoption: a stable document decides once.

Derived from the PR #779 reproduction.  The live Stage A 8ca8952c incident
(the RAM head promotion ``a201e161`` held the destination-root ownership lock
with ~29 GB rchar over ~3,020 large reads, zero writes, 16 workers and ~527
CPU-s, and was auto-withdrawn) was traced to ``_Copier._copy_one`` ->
``_StagedPublisher.try_adopt`` -> ``_proof_search`` per entry: with the #761
index unable to retain an owner's records, every attempted adoption re-read
and re-decoded every fragment and sidecar, and each refusal first ran
``_reclaim``.  At a 64 KiB scaled budget the reproduction measured 421 decodes
and 420 reclaims for 60 destinations over an 8-document fixture, against 8
decodes and 0 reclaims with room.

That file was reproduction-only.  This is the regression the fix must satisfy,
and it is a work-count gate, never a wall-clock gate:

* a budget that fits the compact projections must decode each stable document
  once and reclaim zero times, with every destination still adopting -- at
  the fixture scale and as fixture and cap shrink together;
* a budget nothing fits in must still decide correctly and retain nothing;
* real ``_Copier`` threads and a contending reader must complete with no lost
  or duplicated work and no extra decodes, synchronized by events rather than
  by an unisolated timing maximum;
* compact-retained verdicts must equal the same documents' uncached verdicts,
  and mutation, taint/heal and duplicate-sidecar-mention answers must be
  preserved.

The fixture is real: real writers, real identities, the real publisher and
copier, adoption through the real ``try_adopt``/``_proof_search`` path under
the bucket's stage ownership lock.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import sys
import threading

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

from prismabuild import pool, reader_lease, residency_map  # noqa: E402
import prewarm_loop  # noqa: E402
import stage_move  # noqa: E402

TIER = "ram:fixture"
EPOCH = "1789957191-repro"
MANIFEST = "5" * 64
N_DESTS = 60
OWNERS = 4           # prior consumers whose fragments+sidecars name every dest
PAYLOAD = b"repro-payload\x00\x01\x02" * 64

#: The scaled fixture-size lever PR #779 used: far below the production 192
#: MiB ceiling, and small enough that the packed projections of the whole
#: 60-destination/two-per-owner forest fit, while the pre-fix object graph
#: does not.
CONSTRAINED_BUDGET = 64 << 10
SMALL_DESTS = 15
SMALL_OWNERS = 2
SMALL_BUDGET = 16 << 10

#: The production default captured before any test patches it, so an arm that
#: asks for "no lever" is not silently handed a previous arm's budget.
DEFAULT_BUDGET = int(stage_move._INDEX_BUDGET_BYTES)


def _hex64(seed: str) -> str:
    return hashlib.sha256(seed.encode()).hexdigest()


class _Counters:
    """Read/decode/reclaim counters around the publisher's metadata I/O.

    Two independent views of the same work: ``_read_metadata`` byte reads
    (the reader the publisher uses) and every ``json.loads`` decode.  A
    reader rewritten onto another IO door would keep the second honest, and a
    representation that stopped decoding documents would show in both.
    """

    def __init__(self, monkeypatch):
        self.decodes: dict[str, int] = {}
        self.decode_bytes = 0
        self.reclaims = 0
        self.parses = 0
        real_read = stage_move._read_metadata
        real_reclaim = stage_move._StagedPublisher._reclaim
        real_loads = json.loads

        def counted_read(path):
            version, raw = real_read(path)
            self.decodes[str(path)] = self.decodes.get(str(path), 0) + 1
            self.decode_bytes += len(raw)
            return version, raw

        def counted_reclaim(self_pub):
            self.reclaims += 1
            return real_reclaim(self_pub)

        def counted_loads(raw, **kwargs):
            self.parses += 1
            return real_loads(raw, **kwargs)

        monkeypatch.setattr(stage_move, "_read_metadata", counted_read)
        monkeypatch.setattr(stage_move._StagedPublisher, "_reclaim",
                            counted_reclaim)
        monkeypatch.setattr(json, "loads", counted_loads)

    def total_decodes(self) -> int:
        return sum(self.decodes.values())

    def documents(self) -> int:
        return len(self.decodes)


def _fixture(tmp_path: Path, *, destinations: int = N_DESTS,
             owners: int = OWNERS) -> dict[str, object]:
    """A real RAM-destination forest: ``owners`` prior consumers.

    Every owner's fragment and sidecar legitimately vouch for every
    destination (identical bytes, identical identities), exactly the live
    shape where several prior campaigns' RAM proofs all name the head files.
    The promoting mover itself has no fragment yet -- mid-run.
    """
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    residence = tmp_path / "queue" / pool.RESIDENCY
    ram_root = tmp_path / "ram" / "prewarm"
    ram_root.mkdir(parents=True)
    src_root = tmp_path / "src"
    src_root.mkdir()
    entries = []
    digest = hashlib.sha256(PAYLOAD).hexdigest()
    for index in range(destinations):
        name = f"model-{index:05d}-of-{destinations:05d}.safetensors"
        source = src_root / name
        source.write_bytes(PAYLOAD)
        (ram_root / name).write_bytes(PAYLOAD)
        entries.append({"path": str(source), "offset": 0,
                        "bytes": len(PAYLOAD), "sha256": digest})

    fragment_entries: dict[str, dict[str, object]] = {}
    material_entries: dict[str, dict[str, object]] = {}
    for entry in entries:
        key = residency_map.residency_map_key(entry["path"], 0)
        destination = str(ram_root / Path(entry["path"]).name)
        identity = reader_lease.stat_identity(destination)
        assert identity is not None
        fragment_entries[key] = {
            "stage_path": destination, "bytes": entry["bytes"],
            "offset": 0, "sha256": entry["sha256"]}
        material_entries[key] = {
            "stage_path": destination, "bytes": entry["bytes"],
            "sha256": entry["sha256"], "file_id": identity}

    owner_keys: list[tuple[str, str]] = []
    for owner_index in range(owners):
        consumer = _hex64(f"prior-consumer-{owner_index}")
        mover = _hex64(f"prior-mover-{owner_index}")
        owner_keys.append((consumer, mover))
        _write_fragment(residence, consumer, mover, fragment_entries)
        _write_material(residence, consumer, mover, material_entries)

    return {
        "queue": queue, "residence": residence, "ram_root": ram_root,
        "src_root": src_root, "entries": entries,
        "fragment_entries": fragment_entries,
        "material_entries": material_entries,
        "owners": owner_keys,
    }


def _write_fragment(residence: Path, consumer: str, mover: str,
                    entries: dict[str, dict[str, object]]) -> Path:
    return residency_map.write_fragment(residence, {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": consumer, "mover_action_key": mover,
        "tier_id": TIER, "stage_root": str(residence.parent.parent /
                                           "ram" / "prewarm"),
        "epoch": EPOCH,
        "manifest_sha256": MANIFEST, "entries": entries})


def _write_material(residence: Path, consumer: str, mover: str,
                    entries: dict[str, dict[str, object]]) -> Path:
    return reader_lease.write_material(
        residence, consumer_action_key=consumer, mover_action_key=mover,
        tier_id=TIER, stage_root=str(residence.parent.parent / "ram" /
                                     "prewarm"),
        manifest_sha256=MANIFEST,
        generation=reader_lease.mint_generation(),
        entries=entries, epoch=EPOCH)


def _publisher(forest: dict[str, object], tmp_path: Path):
    return stage_move._StagedPublisher(
        queue=forest["queue"], stage_root=forest["ram_root"],
        residency_root=forest["residence"],
        mover_action_key=_hex64("promoting-mover"), manifest_sha256=MANIFEST,
        tier_id=TIER, cas_root=str(tmp_path / "cas"))


def _sweep(tmp_path: Path, monkeypatch, *, budget: int,
           destinations: int = N_DESTS, owners: int = OWNERS
           ) -> dict[str, object]:
    """One promotion-shaped adoption sweep, under an explicit budget lever.

    Every destination is adopted through the real ``try_adopt`` path a RAM
    promotion drives, with counters around the metadata reads and reclaims.
    """
    forest = _fixture(tmp_path, destinations=destinations, owners=owners)
    monkeypatch.setattr(stage_move, "_INDEX_BUDGET_BYTES", budget)
    counters = _Counters(monkeypatch)
    publisher = _publisher(forest, tmp_path)
    adopted = []
    for entry in forest["entries"]:
        destination = forest["ram_root"] / Path(entry["path"]).name
        result = publisher.try_adopt(
            entry, destination, stage_move._origin_id_of(entry["path"]))
        adopted.append(result is not None)
    return {
        "entries": len(forest["entries"]),
        "adopted": adopted,
        "total_decodes": counters.total_decodes(),
        "parses": counters.parses,
        "documents": counters.documents(),
        "metadata_bytes_read": counters.decode_bytes,
        "reclaims": counters.reclaims,
        "publisher": publisher,
        "forest": forest,
    }


def _verdicts(publisher, forest: dict[str, object]) -> list[tuple]:
    """Every destination's full ``_proof_search`` verdict, in order."""

    out = []
    for entry in forest["entries"]:
        norm = os.path.normpath(
            str(forest["ram_root"] / Path(entry["path"]).name))
        proof, standing, detail = publisher._proof_search(
            norm, int(entry["bytes"]), entry.get("sha256"))
        out.append((proof is not None, standing, detail))
    return out


# --------------------------------------------------------------------------
# Bounded work: one decode per stable document, none per destination
# --------------------------------------------------------------------------

def test_each_stable_document_decides_once_under_a_constrained_budget(
        tmp_path, monkeypatch):
    """The RED case: a refusal may cost one parse, never one per adoption.

    Ample budget: the whole forest is retained and every document is decoded
    once.  The constrained budget is far below the 192 MiB production ceiling
    but fits the packed projections, so it must behave the same way.  Before
    the fix the constrained arm re-decoded the whole forest per destination
    (421 decodes / 420 reclaims measured for this fixture); the fix retains a
    compact projection instead of declining retention.
    """
    ample = _sweep(tmp_path / "ample", monkeypatch, budget=DEFAULT_BUDGET)
    assert ample["adopted"] == [True] * ample["entries"], ample
    assert ample["total_decodes"] <= 2 * OWNERS, (
        f"ample budget still re-decodes: {ample['total_decodes']}")
    assert ample["parses"] <= 2 * OWNERS, ample
    assert ample["reclaims"] == 0, ample

    constrained = _sweep(tmp_path / "capped", monkeypatch,
                         budget=CONSTRAINED_BUDGET)
    assert constrained["adopted"] == [True] * constrained["entries"], (
        "a constrained budget changed an adoption answer")
    assert constrained["total_decodes"] <= 2 * OWNERS, (
        f"60 destinations decoded {constrained['total_decodes']} documents "
        f"under a {CONSTRAINED_BUDGET} byte budget; each of the "
        f"{2 * OWNERS} stable documents must decode at most once, not once "
        f"per adoption ({constrained['metadata_bytes_read']} metadata bytes)")
    assert constrained["parses"] <= 2 * OWNERS, (
        f"{constrained['parses']} JSON decodes for {2 * OWNERS} stable "
        f"documents: the reader is re-decoding documents")
    assert constrained["reclaims"] == 0, (
        f"{constrained['reclaims']} reclaims under a budget the compact "
        f"projections fit: retention was declined")


def test_work_stays_bounded_as_the_fixture_and_cap_shrink(tmp_path,
                                                          monkeypatch):
    """The same bound holds when records and cap scale down together.

    A smaller forest under a smaller cap is the other side of the lever: the
    budget -- not the fixture -- must decide whether the projection is
    retained, and when it fits, duplicate work stays at one decode per
    stable document.
    """
    arm = _sweep(tmp_path / "small", monkeypatch, budget=SMALL_BUDGET,
                 destinations=SMALL_DESTS, owners=SMALL_OWNERS)
    assert arm["adopted"] == [True] * arm["entries"], arm
    assert arm["total_decodes"] <= 2 * SMALL_OWNERS, (
        f"{arm['entries']} destinations decoded {arm['total_decodes']} "
        f"documents under a {SMALL_BUDGET} byte budget with "
        f"{SMALL_DESTS} paths per record")
    assert arm["parses"] <= 2 * SMALL_OWNERS, arm
    assert arm["reclaims"] == 0, arm


def test_a_budget_nothing_fits_decides_correctly_and_retains_nothing(
        tmp_path, monkeypatch):
    """Cache inability is safe: uncached answers, nothing retained."""

    arm = _sweep(tmp_path / "tiny", monkeypatch, budget=1)
    assert arm["adopted"] == [True] * arm["entries"], (
        "a budget nothing fits in changed an adoption answer")
    publisher = arm["publisher"]
    assert publisher._index_bytes() == 0, (
        "a record survived a one-byte ceiling")
    assert publisher._interned == {}, (
        "a table survived a one-byte ceiling as a back door")


# --------------------------------------------------------------------------
# The compact representation must answer exactly as the object path did
# --------------------------------------------------------------------------

def test_compact_retained_verdicts_match_uncached_verdicts(tmp_path,
                                                           monkeypatch):
    """Differential: same forest, one budget that retains and one that cannot.

    The compact projection is a representation of the parsed document, not a
    different reading of it.  Every destination's full verdict -- adoption,
    standing and refusal detail -- must be identical whichever way the record
    was held.
    """
    forest = _fixture(tmp_path)
    answers = {}
    for label, budget in (("retained", CONSTRAINED_BUDGET), ("uncached", 1)):
        monkeypatch.setattr(stage_move, "_INDEX_BUDGET_BYTES", budget)
        answers[label] = _verdicts(_publisher(forest, tmp_path / label),
                                   forest)
    assert answers["retained"] == answers["uncached"], (
        "the compact projection changed a verdict")
    assert all(row[1] == "proof" for row in answers["retained"])


def test_a_mutated_sidecar_and_a_corrupt_fragment_are_seen_after_retention(
        tmp_path, monkeypatch):
    """Invalidation survives compaction: a decision never uses moved evidence.

    One prior owner, so a rewrite is decisive.  After the compact records are
    retained: a sidecar that stops dating the current incarnation defers as
    ``owned``; a sidecar whose digest contradicts the manifest refuses as
    ``divergent``; a fragment that becomes corrupt reads ``unknown`` and a
    repaired fragment proves again -- all on the very next lookup.
    """
    forest = _fixture(tmp_path, destinations=SMALL_DESTS, owners=1)
    monkeypatch.setattr(stage_move, "_INDEX_BUDGET_BYTES", SMALL_BUDGET)
    counters = _Counters(monkeypatch)
    publisher = _publisher(forest, tmp_path)
    consumer, mover = forest["owners"][0]
    entries = forest["entries"]
    target = entries[0]
    norm = os.path.normpath(str(forest["ram_root"] /
                                 Path(target["path"]).name))

    assert _verdicts(publisher, forest) == [
        (True, "proof", None)] * len(entries)
    assert counters.total_decodes() == 2, (
        "the fixture did not retain its two stable documents compactly")

    stale = {key: dict(record)
             for key, record in forest["material_entries"].items()}
    stale_key = str(residency_map.residency_map_key(target["path"], 0))
    stale[stale_key] = {
        **stale[stale_key],
        "file_id": {"ino": 1, "size": 1, "mtime_ns": 1, "ctime_ns": 1},
    }
    _write_material(forest["residence"], consumer, mover, stale)
    _, standing, _ = publisher._proof_search(
        norm, int(target["bytes"]), target["sha256"])
    assert standing == "owned", (
        "a record dating a superseded incarnation must defer, not adopt")

    wrong = {key: dict(record)
             for key, record in forest["material_entries"].items()}
    wrong[stale_key] = {**wrong[stale_key], "sha256": "b" * 64}
    _write_material(forest["residence"], consumer, mover, wrong)
    _, standing, _ = publisher._proof_search(
        norm, int(target["bytes"]), target["sha256"])
    assert standing == "divergent", (
        "a digest that contradicts the manifest must refuse")

    # Restore the dated record before the taint case, so the healed fragment
    # is judged on its own evidence.
    _write_material(forest["residence"], consumer, mover,
                    forest["material_entries"])
    fragment = residency_map.fragment_path(forest["residence"], consumer,
                                           mover)
    good = fragment.read_bytes()
    fragment.write_bytes(b"{not json")
    _, standing, detail = publisher._proof_search(
        norm, int(target["bytes"]), target["sha256"])
    assert standing == "unknown" and detail, (
        "a corrupt fragment must read unknown, never skip into success")

    fragment.write_bytes(good)
    _, standing, _ = publisher._proof_search(
        norm, int(target["bytes"]), target["sha256"])
    assert standing == "proof", "a repaired fragment must prove again"


def test_duplicate_sidecar_mentions_keep_their_order(tmp_path, monkeypatch):
    """Two mentions of one path: order selects the identity and the digest.

    A sidecar can name one staged path under two map keys (two extents of one
    file), and ``_proof_candidate`` legitimately reads the *first* size-
    matching mention for the digest and the *first* mention with an identity
    for the incarnation.  The packed projection must keep that order.  The
    uncached and compact-retained publishers must answer the same, and the
    answers must be the documented ones -- not merely self-consistent.
    """
    forest = _fixture(tmp_path, destinations=1, owners=1)
    consumer, mover = forest["owners"][0]
    entry = forest["entries"][0]
    norm = os.path.normpath(str(forest["ram_root"] / Path(entry["path"]).name))
    live = forest["material_entries"][
        str(residency_map.residency_map_key(entry["path"], 0))]["file_id"]
    stale = {"ino": 1, "size": 1, "mtime_ns": 1, "ctime_ns": 1}
    digest = entry["sha256"]

    def sidecar(first_identity, first_digest, second_identity, second_digest):
        return {
            "a" * 64: {"stage_path": norm, "bytes": int(entry["bytes"]),
                       "sha256": first_digest, "file_id": first_identity},
            "b" * 64: {"stage_path": norm, "bytes": int(entry["bytes"]),
                       "sha256": second_digest, "file_id": second_identity},
        }

    cases = [
        # The first mention's stale identity decides ("stale" -> owned), even
        # though the second mention dates the live incarnation.
        ("owned", sidecar(stale, digest, live, "c" * 64)),
        # Reversed: the live identity is first and its digest matches.
        ("proof", sidecar(live, digest, stale, "c" * 64)),
    ]
    for expected, material in cases:
        _write_material(forest["residence"], consumer, mover, material)
        answers = {}
        for label, budget in (("retained", CONSTRAINED_BUDGET),
                              ("uncached", 1)):
            monkeypatch.setattr(stage_move, "_INDEX_BUDGET_BYTES", budget)
            publisher = _publisher(forest, tmp_path / label)
            answers[label] = publisher._proof_search(
                norm, int(entry["bytes"]), digest)[1]
        assert answers["retained"] == answers["uncached"], (
            "the compact projection reordered the mentions")
        assert answers["retained"] == expected, (
            f"duplicate-mention order changed the answer: {answers}")


# --------------------------------------------------------------------------
# Real copier threads and a reader, synchronized by events
# --------------------------------------------------------------------------

def test_copier_threads_and_a_reader_contend_without_losing_work(
        tmp_path, monkeypatch):
    """The production publication path under real threads, not a timer.

    Four ``_Copier`` workers adopt all destinations while a reader thread
    walks the same forest.  The first metadata decode is gated: it holds the
    ownership lock until the reader has begun its own lock acquisition, so
    the overlap is a happened-before relation rather than a race won by a
    scheduler.  Every adoption must land exactly once, no thread may see a
    wrong answer, and the cache must keep the work at one decode per stable
    document.
    """
    forest = _fixture(tmp_path)
    monkeypatch.setattr(stage_move, "_INDEX_BUDGET_BYTES", DEFAULT_BUDGET)
    counters = _Counters(monkeypatch)
    publisher = _publisher(forest, tmp_path)
    copier = stage_move._Copier(
        mounts=prewarm_loop.MountMap([f"{forest['src_root']}="
                                      f"{forest['src_root']}"]),
        pacer=None, stage_root=forest["ram_root"],
        mount_prefix=str(forest["src_root"]), block=1 << 16, workers=4,
        owner=_hex64("promoting-mover"), publisher=publisher)
    whole = {entry["path"] for entry in forest["entries"]}

    first_decode = threading.Event()
    reader_attempted = threading.Event()
    gate = threading.Lock()
    counted_read = stage_move._read_metadata

    def gated_read(path):
        with gate:
            if not first_decode.is_set():
                first_decode.set()
                # Hold the decode (and with it the ownership lock) until the
                # reader has actually started its own acquisition.
                assert reader_attempted.wait(30), (
                    "the reader never reached the publisher")
        return counted_read(path)

    monkeypatch.setattr(stage_move, "_read_metadata", gated_read)

    reader_results: list[bool] = []
    reader_errors: list[BaseException] = []

    def reader() -> None:
        try:
            assert first_decode.wait(30), "no copier decode was observed"
            reader_attempted.set()
            for entry in forest["entries"]:
                adopted = publisher.try_adopt(
                    entry, forest["ram_root"] / Path(entry["path"]).name,
                    stage_move._origin_id_of(entry["path"]))
                reader_results.append(adopted is not None)
        except BaseException as exc:            # surfaced, never swallowed
            reader_errors.append(exc)

    thread = threading.Thread(target=reader, name="reader", daemon=True)
    thread.start()
    try:
        copier.run(forest["entries"], whole=whole, stop=threading.Event())
    finally:
        thread.join(60)

    assert not thread.is_alive(), "the reader never finished"
    assert reader_errors == [], reader_errors
    assert reader_results == [True] * len(forest["entries"]), (
        "the contending reader lost an adoption")
    assert copier.errors == [], copier.errors
    assert len(copier.staged) == len(forest["entries"]), (
        f"{len(copier.staged)} of {len(forest['entries'])} entries landed")
    assert copier.bytes_staged == sum(int(entry["bytes"])
                                      for entry in forest["entries"])
    assert counters.total_decodes() <= 2 * OWNERS, (
        f"concurrent adoption decoded {counters.total_decodes()} documents "
        f"for {2 * OWNERS} stable documents")
    assert counters.reclaims == 0, counters.reclaims


# --------------------------------------------------------------------------
# Fail-closed beside the counters
# --------------------------------------------------------------------------

def test_a_late_tainted_fragment_defeats_an_earlier_proof(tmp_path,
                                                          monkeypatch):
    """The taint contract: unknown wins even after a valid proof is found.

    The corrupt file lives in a consumer directory the scan visits *after*
    every proving one, so the lookup meets proof first and must still refuse:
    an unreadable record might name this very path, and the safe answer is
    unknown, never a silent skip into adoption.
    """

    forest = _fixture(tmp_path)
    monkeypatch.setattr(stage_move, "_INDEX_BUDGET_BYTES", CONSTRAINED_BUDGET)
    publisher = _publisher(forest, tmp_path)
    entry = forest["entries"][0]
    destination = forest["ram_root"] / Path(entry["path"]).name
    assert publisher.try_adopt(
        entry, destination,
        stage_move._origin_id_of(entry["path"])) is not None

    last = max(path.name for path in forest["residence"].iterdir()
               if path.is_dir() and path.name not in ("leases", "material"))
    late = forest["residence"] / (last + "z")
    late.mkdir()
    (late / (("e" * 64) + ".json")).write_text("{malformed")

    proof, standing, detail = publisher._proof_search(
        os.path.normpath(str(destination)), entry["bytes"], entry["sha256"])
    assert proof is None and standing == "unknown" and detail, (
        f"a late corrupt fragment must defeat the found proof, saw "
        f"{standing!r}: {detail!r}")
    assert publisher.try_adopt(
        entry, destination,
        stage_move._origin_id_of(entry["path"])) is None, (
        "a late corrupt fragment must not adopt")


def test_a_late_divergent_sidecar_defeats_an_earlier_proof(tmp_path,
                                                           monkeypatch):
    """A later record dating the live inode with other bytes refuses at once.

    The rewritten sidecar is the last one the scan visits, so earlier owners
    have already proved the destination; divergence is still the verdict.
    """

    forest = _fixture(tmp_path, destinations=SMALL_DESTS)
    monkeypatch.setattr(stage_move, "_INDEX_BUDGET_BYTES", CONSTRAINED_BUDGET)
    publisher = _publisher(forest, tmp_path)
    entry = forest["entries"][0]
    norm = os.path.normpath(str(forest["ram_root"] /
                                 Path(entry["path"]).name))
    assert publisher._proof_search(
        norm, entry["bytes"], entry["sha256"])[1] == "proof"

    consumer, mover = max(forest["owners"], key=lambda pair: pair[0])
    entries = {key: dict(record) for key, record in
               forest["material_entries"].items()}
    key = str(residency_map.residency_map_key(entry["path"], 0))
    entries[key] = {
        **entries[key], "sha256": "b" * 64,
        "file_id": reader_lease.stat_identity(norm),
    }
    _write_material(forest["residence"], consumer, mover, entries)

    proof, standing, detail = publisher._proof_search(
        norm, entry["bytes"], entry["sha256"])
    assert proof is None and standing == "divergent" and detail, (
        f"a late record rejecting the live inode's digest must refuse, saw "
        f"{standing!r}: {detail!r}")
    with pytest.raises(OSError):
        publisher.try_adopt(
            entry, forest["ram_root"] / Path(entry["path"]).name,
            stage_move._origin_id_of(entry["path"]))


def test_a_replaced_incarnation_is_not_adopted_until_a_record_dates_it(
        tmp_path, monkeypatch):
    """#755 beside the compact index: a new inode needs a new date.

    No prior record of the old incarnation may adopt the replacement; once
    one legitimate owner is re-dated to the new identity (with the
    manifest's bytes), adoption resumes through the ordinary gate.
    """

    forest = _fixture(tmp_path, destinations=1, owners=1)
    monkeypatch.setattr(stage_move, "_INDEX_BUDGET_BYTES", CONSTRAINED_BUDGET)
    publisher = _publisher(forest, tmp_path)
    entry = forest["entries"][0]
    destination = forest["ram_root"] / Path(entry["path"]).name
    norm = os.path.normpath(str(destination))
    assert publisher._proof_search(
        norm, entry["bytes"], entry["sha256"])[1] == "proof"

    replacement = destination.with_name(destination.name + ".new")
    replacement.write_bytes(PAYLOAD)
    os.replace(replacement, destination)
    proof, standing, _ = publisher._proof_search(
        norm, entry["bytes"], entry["sha256"])
    assert proof is None and standing == "owned", (
        f"a superseded incarnation must defer, saw {standing!r}")
    assert publisher.try_adopt(
        entry, destination,
        stage_move._origin_id_of(entry["path"])) is None

    consumer, mover = forest["owners"][0]
    dated = {key: dict(record) for key, record in
             forest["material_entries"].items()}
    key = str(residency_map.residency_map_key(entry["path"], 0))
    dated[key] = {**dated[key], "file_id": reader_lease.stat_identity(norm)}
    _write_material(forest["residence"], consumer, mover, dated)

    proof, standing, _ = publisher._proof_search(
        norm, entry["bytes"], entry["sha256"])
    assert standing == "proof" and proof is not None, (
        "a record dating the new incarnation must prove it again")
    assert publisher.try_adopt(
        entry, destination,
        stage_move._origin_id_of(entry["path"])) is not None


def test_a_same_size_rewrite_with_restored_mtime_is_seen(tmp_path,
                                                         monkeypatch):
    """Size and mtime restored, ctime not: the version fence catches it.

    A writer that restores the visible size and mtime still cannot restore
    ctime, so the per-file version differs and the new bytes are parsed
    before the next decision -- the compact record is never a held verdict.
    """

    forest = _fixture(tmp_path, destinations=1, owners=1)
    monkeypatch.setattr(stage_move, "_INDEX_BUDGET_BYTES", CONSTRAINED_BUDGET)
    publisher = _publisher(forest, tmp_path)
    entry = forest["entries"][0]
    norm = os.path.normpath(str(forest["ram_root"] /
                                 Path(entry["path"]).name))
    assert publisher._proof_search(
        norm, entry["bytes"], entry["sha256"])[1] == "proof"

    consumer, mover = forest["owners"][0]
    material = reader_lease.material_path(forest["residence"], consumer, mover)
    before = material.stat()
    old = material.read_bytes()
    assert entry["sha256"].encode() in old
    material.write_bytes(old.replace(entry["sha256"].encode(), b"b" * 64))
    os.utime(material, ns=(before.st_atime_ns, before.st_mtime_ns))
    after = material.stat()
    assert after.st_size == before.st_size, "the rewrite must be same-size"
    assert after.st_mtime_ns == before.st_mtime_ns, "mtime must be restored"
    assert after.st_ctime_ns != before.st_ctime_ns, "ctime must have moved"

    proof, standing, _ = publisher._proof_search(
        norm, entry["bytes"], entry["sha256"])
    assert proof is None and standing == "divergent", (
        f"a same-size restored-mtime rewrite must still be seen, saw "
        f"{standing!r}")


def test_the_instrument_counts_real_decodes(tmp_path, monkeypatch):
    """A counter that stopped seeing the reader must fail, not pass.

    One uncached sweep must show at least one decode per document, so a green
    bounded-work case cannot be green because instrumentation silently went
    dark.
    """
    arm = _sweep(tmp_path / "uncached", monkeypatch, budget=1)
    assert arm["total_decodes"] >= 2 * OWNERS, (
        f"the decode counter saw {arm['total_decodes']} reads over an "
        f"uncached sweep; the instrument is not measuring the reader")
    assert arm["parses"] >= 2 * OWNERS, (
        f"the JSON decode counter saw {arm['parses']} decodes over an "
        f"uncached sweep")
    assert arm["adopted"] == [True] * arm["entries"]
