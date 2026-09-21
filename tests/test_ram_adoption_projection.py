"""Destination-root projection of the publisher's metadata index.

Root-authorized follow-up to the amplification repro (PR779, issue #778):
the repro measured, at a scaled budget, that a publisher whose proof
forest contains another root's huge documents re-decodes them per
destination once their unprojected retention cannot fit the #761 index.
Whether the live Stage A forest actually overflowed the production
ceiling was NOT measured -- the live tree's shape (163 MB / 347 documents,
several 22-26 MB proofs) makes it plausible, and this file demonstrates
the same mechanism at the PRODUCTION 192 MiB ceiling with a foreign proof
sized past it. The architecture direction is a projection, not a bigger
cache: a publisher only ever asks about destinations under ITS OWN stage
root, so a fragment or sidecar whose entries name another root's paths
can be retained as the (near-empty) set of paths it holds under THIS
root -- whole documents are still read and parsed, malformed JSON still
taints, sidecars still run full ``validate_material``, and the honest
192 MiB measured ceiling stays.

These cases pin the contract at the PRODUCTION budget (192 MiB), on a
mixed forest shaped like the live one: foreign SSD-root documents large
enough that their unprojected retention cannot fit, foreign RAM co-owners
that must adopt, and the version/taint/escape fences. The all-RAM limit
stays explicit: a working set of same-root metadata larger than the
ceiling still refuses retention and re-decodes -- the last test says so
in numbers.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import sys
import time

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

from prismabuild import pool, reader_lease, residency_map  # noqa: E402
import prewarm_loop  # noqa: E402
import stage_move  # noqa: E402

RAM_TIER = "ram:fixture"
EPOCH = "1789957191-projection"
MANIFEST = "6" * 64
N_DESTS = 12
N_FOREIGN_PATHS = 340_000     # one SSD proof whose unprojected charge > 192 MiB
ADOPTED_IN_SWEEP = 6
PAYLOAD = b"projection-payload\x00\x01" * 32


def _hex64(seed: str) -> str:
    return hashlib.sha256(seed.encode()).hexdigest()


class _Counters:
    """Decode/byte/reclaim counters around the publisher's metadata I/O."""

    def __init__(self, monkeypatch):
        self.decodes: dict[str, int] = {}
        self.decode_bytes = 0
        self.reclaims = 0
        real_read = stage_move._read_metadata
        real_reclaim = stage_move._StagedPublisher._reclaim

        def counted_read(path):
            version, raw = real_read(path)
            self.decodes[str(path)] = self.decodes.get(str(path), 0) + 1
            self.decode_bytes += len(raw)
            return version, raw

        def counted_reclaim(self_pub):
            self.reclaims += 1
            return real_reclaim(self_pub)

        monkeypatch.setattr(stage_move, "_read_metadata", counted_read)
        monkeypatch.setattr(stage_move._StagedPublisher, "_reclaim",
                            counted_reclaim)

    @property
    def total(self) -> int:
        return sum(self.decodes.values())


def _write_proof(residence, *, consumer, mover, tier, root, epoch, mentions,
                 digest):
    """One owner's fragment+sidecar through the real writers.

    ``mentions``: {map key: (path, file_id | None)}; ``file_id`` must be
    the live stat identity for paths this publisher will query (the RAM
    co-owners), and may be a constant for foreign-root paths that are
    never queried. ``digest`` is the payload digest every row vouches.
    """
    frag_entries, mat_entries = {}, {}
    for key, (path, file_id) in mentions.items():
        identity = file_id or {"ino": 1, "size": PAYLOAD_BYTES,
                               "mtime_ns": 1, "ctime_ns": 1}
        frag_entries[key] = {"stage_path": str(path), "bytes": PAYLOAD_BYTES,
                             "offset": 0, "sha256": digest}
        mat_entries[key] = {"stage_path": str(path),
                            "bytes": PAYLOAD_BYTES, "sha256": digest,
                            "file_id": identity}
    body = {"schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
            "consumer_action_key": consumer, "mover_action_key": mover,
            "tier_id": tier, "stage_root": str(root),
            "manifest_sha256": MANIFEST, "entries": frag_entries}
    if epoch is not None:
        body["epoch"] = epoch
    residency_map.write_fragment(residence, body)
    material = {"consumer_action_key": consumer, "mover_action_key": mover,
                "tier_id": tier, "stage_root": str(root),
                "manifest_sha256": MANIFEST,
                "generation": reader_lease.mint_generation(),
                "entries": mat_entries}
    if epoch is not None:
        material["epoch"] = epoch
    reader_lease.write_material(residence, **material)


PAYLOAD_BYTES = len(PAYLOAD)


def _mixed_forest(tmp_path: Path, *, foreign_paths: int = N_FOREIGN_PATHS,
                  dests: int = N_DESTS):
    """Live-shaped forest: huge foreign SSD proofs + RAM co-owners.

    Relation to the live tree (163 MB / 347 docs, several 22-26 MB proofs):
    one foreign SSD fragment/sidecar pair is scaled past the live size so
    its UNPROJECTED retention cannot fit the production 192 MiB ceiling.
    That the live forest reached this condition is plausible but unmeasured
    -- this fixture demonstrates the mechanism, not a measured live
    overflow. Every payload here stays tiny and no live artifact is
    touched.
    """
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    residence = tmp_path / "queue" / pool.RESIDENCY
    ram_root = tmp_path / "ram" / "prewarm"
    ram_root.mkdir(parents=True)
    ssd_root = tmp_path / "stage" / "prewarm"
    ssd_root.mkdir(parents=True)
    src_root = tmp_path / "src"
    src_root.mkdir()
    digest = hashlib.sha256(PAYLOAD).hexdigest()
    entries = []
    for index in range(dests):
        name = f"model-{index:05d}-of-{dests:05d}.safetensors"
        (src_root / name).write_bytes(PAYLOAD)
        (ram_root / name).write_bytes(PAYLOAD)
        entries.append({"path": str(src_root / name), "offset": 0,
                        "bytes": PAYLOAD_BYTES, "sha256": digest})
    # Foreign RAM co-owners: two prior consumers vouching every destination.
    payload_digest = hashlib.sha256(PAYLOAD).hexdigest()
    for owner in range(2):
        _write_proof(
            residence, consumer=_hex64(f"ram-consumer-{owner}"),
            mover=_hex64(f"ram-mover-{owner}"), tier=RAM_TIER,
            root=ram_root, epoch=EPOCH, digest=payload_digest,
            mentions={residency_map.residency_map_key(e["path"], 0): (
                ram_root / Path(e["path"]).name,
                reader_lease.stat_identity(
                    str(ram_root / Path(e["path"]).name)))
                for e in entries})
    # One foreign SSD proof pair, sized past the ceiling.
    foreign = {}
    for index in range(foreign_paths):
        path = ssd_root / f"shard-{index:07d}.bin"
        foreign[residency_map.residency_map_key(str(path), 0)] = (path, None)
    _write_proof(
        residence, consumer=_hex64("ssd-consumer"),
        mover=_hex64("ssd-mover"), tier="prismabuild-stage:fixture",
        root=ssd_root, epoch=None, digest=payload_digest, mentions=foreign)
    return queue, residence, ram_root, src_root, entries


def _publisher(queue, residence, ram_root, tmp_path):
    return stage_move._StagedPublisher(
        queue=queue, stage_root=ram_root, residency_root=residence,
        mover_action_key=_hex64("promoting-mover"), manifest_sha256=MANIFEST,
        tier_id=RAM_TIER, cas_root=str(tmp_path / "cas"))


def _adopt_sweep(publisher, ram_root, src_root, entries, count):
    """The promotion's per-entry question, ``count`` destinations deep."""
    out = []
    for entry in entries[:count]:
        destination = ram_root / Path(entry["path"]).name
        began = time.monotonic()
        adopted = publisher.try_adopt(
            entry, destination, stage_move._origin_id_of(entry["path"]))
        out.append(time.monotonic() - began)
        assert adopted is not None, f"destination lost proof: {destination}"
    return out


def test_foreign_root_proofs_project_to_nothing_and_stop_redecoding(
        tmp_path, monkeypatch):
    """RED before the fix, GREEN after, at the PRODUCTION budget.

    Before: the SSD proof's unprojected retention cannot fit 192 MiB, so
    every adoption re-decodes it (and reclaims). After: it is retained as
    the empty set of paths it holds under THIS root, each document decodes
    exactly once, nothing is reclaimed, and adoption still proves every
    destination from the foreign RAM co-owners.
    """
    assert stage_move._INDEX_BUDGET_BYTES == 192 << 20, (
        "this case is written against the production ceiling")
    queue, residence, ram_root, src_root, entries = _mixed_forest(tmp_path)
    counters = _Counters(monkeypatch)
    publisher = _publisher(queue, residence, ram_root, tmp_path)
    sweep = _adopt_sweep(publisher, ram_root, src_root, entries,
                         ADOPTED_IN_SWEEP)
    print("SWEEP", json.dumps({
        "documents": counters.total and len(counters.decodes),
        "total_decodes": counters.total,
        "metadata_bytes": counters.decode_bytes,
        "reclaims": counters.reclaims,
        "retained_index_bytes": publisher._index_bytes(),
        "adopt_mean_ms": round(1000 * sum(sweep) / len(sweep), 3)}, sort_keys=True))
    # The bound: one decode per document for the sweep, no reclaim churn,
    # and the retained index is the RAM projection, not the SSD forest.
    documents = 2 * 2 + 2           # ram co-owners' frag+mat, ssd pair
    assert counters.total <= documents, (
        f"redundant decodes at the production budget: {counters.decodes}")
    assert counters.reclaims == 0
    assert publisher._index_bytes() < (16 << 20), (
        "the SSD path set is being retained for a RAM publisher")


def test_corrupt_other_root_fragment_still_taints(tmp_path, monkeypatch):
    """Fail-closed is not projected away: an unreadable foreign fragment
    still reads unknown and refuses adoption."""
    queue, residence, ram_root, _src, entries = _mixed_forest(
        tmp_path, foreign_paths=64)
    ssd_dir = residence / _hex64("ssd-consumer")
    fragment = next(name for name in sorted(os.listdir(ssd_dir))
                    if name.endswith(".json"))
    (ssd_dir / fragment).write_text("{not json")
    publisher = _publisher(queue, residence, ram_root, tmp_path)
    destination = ram_root / Path(entries[0]["path"]).name
    assert publisher.try_adopt(
        entries[0], destination,
        stage_move._origin_id_of(entries[0]["path"])) is None
    proof, standing, _detail = publisher._proof_search(
        os.path.normpath(str(destination)), entries[0]["bytes"],
        entries[0]["sha256"])
    assert standing == "unknown"


@pytest.mark.parametrize("mutation", ["changed", "removed"])
def test_document_versions_are_still_seen(tmp_path, monkeypatch, mutation):
    """Metadata-version invalidation survives the projection: a changed or
    removed proof document is re-read / dropped exactly as before."""
    queue, residence, ram_root, _src, entries = _mixed_forest(
        tmp_path, foreign_paths=64)
    counters = _Counters(monkeypatch)
    publisher = _publisher(queue, residence, ram_root, tmp_path)
    _adopt_sweep(publisher, ram_root, _src, entries, 1)
    baseline = dict(counters.decodes)
    ram_dir = residence / _hex64("ram-consumer-0")
    fragment = next(name for name in sorted(os.listdir(ram_dir))
                    if name.endswith(".json"))
    if mutation == "changed":
        # Same content re-written is a new ctime: a new version.
        time.sleep(0.01)
        (ram_dir / fragment).write_text((ram_dir / fragment).read_text())
    else:
        (ram_dir / fragment).unlink()
    _adopt_sweep(publisher, ram_root, _src, entries, 1)
    if mutation == "changed":
        assert counters.decodes[str(ram_dir / fragment)] \
            == baseline.get(str(ram_dir / fragment), 0) + 1, (
            "a changed document version was not re-read")
    else:
        # A removed document's verdict is re-derived on the next lookup:
        # the file is gone, the record is dropped, no stale proof answers.
        assert not (ram_dir / fragment).exists()
        destination = ram_root / Path(entries[0]["path"]).name
        assert publisher.try_adopt(
            entries[0], destination,
            stage_move._origin_id_of(entries[0]["path"])) is not None, (
            "one co-owner's removal must not lose the other's proof")


def test_projection_filters_by_entry_path_not_header():
    """The fence root named: a foreign HEADER never widens or narrows the
    projection -- only each entry's own path decides."""
    ram = "/ram/prewarm"
    names = {f"{ram}/a.bin", "/stage/prewarm/b.bin", "/other/root/c.bin",
             f"{ram}/nested/d.bin"}
    projected = stage_move._projected_paths(names, ram)
    assert projected == frozenset({f"{ram}/a.bin", f"{ram}/nested/d.bin"})


def test_own_root_entries_survive_a_foreign_header(tmp_path, monkeypatch):
    """A fragment whose header declares another root but whose entries name
    THIS root still proves and adopts, and its names are still retained.

    The writer refuses such a document (``validate_entry`` ties entries to
    the declared root), so this document is written raw, over an honest
    fragment in place: the reader cannot assume every document it meets
    was written by this writer, and it never held fragments to header-root
    containment before the projection either. The sidecar gets the same
    foreign header. Filtering by entry path -- not by the header -- is the
    only rule that keeps these destinations provable, and the whole point
    of the projection is that it changes no answer a publisher can ask.
    """
    queue, residence, ram_root, src_root, entries = _mixed_forest(
        tmp_path, foreign_paths=64)
    foreign_root = str(tmp_path / "stage" / "prewarm")
    ram_dir = residence / _hex64("ram-consumer-0")
    fragment = next(name for name in sorted(os.listdir(ram_dir))
                    if name.endswith(".json"))
    body = json.loads((ram_dir / fragment).read_text())
    body["stage_root"] = foreign_root
    (ram_dir / fragment).write_text(json.dumps(body))
    material = json.loads(
        (residence / "material" / _hex64("ram-consumer-0")
         / fragment).read_text())
    reader_lease.write_material(
        residence,
        consumer_action_key=material["consumer_action_key"],
        mover_action_key=material["mover_action_key"],
        tier_id=material["tier_id"], stage_root=foreign_root,
        manifest_sha256=material["manifest_sha256"],
        generation=material["generation"],
        entries=material["entries"],
        **({"epoch": material["epoch"]} if "epoch" in material else {}))
    publisher = _publisher(queue, residence, ram_root, tmp_path)
    adopted = _adopt_sweep(publisher, ram_root, src_root, entries,
                           len(entries))
    assert len(adopted) == len(entries)
    # And the retained record still carries the own-root names, not an
    # empty projection taken from the foreign header.
    record = publisher._fragments[str(ram_dir / fragment)][1]
    destination = os.path.normpath(
        str(ram_root / Path(entries[0]["path"]).name))
    assert destination in record[1]


def test_all_ram_working_set_over_the_ceiling_still_refuses(tmp_path,
                                                             monkeypatch):
    """The explicit limit: projection cannot shrink a same-root working
    set -- every mention IS one this publisher can query.  A ceiling the
    honest retained set cannot meet still refuses retention, and lookups
    still re-decode rather than retain beyond the budget.  (The foreign
    SSD pair in the same forest DOES stay cached at that ceiling: its
    projection is empty, which is the whole fix.)"""
    monkeypatch.setattr(stage_move, "_INDEX_BUDGET_BYTES", 6 << 10)
    queue, residence, ram_root, src_root, entries = _mixed_forest(
        tmp_path, foreign_paths=8, dests=200)
    counters = _Counters(monkeypatch)
    publisher = _publisher(queue, residence, ram_root, tmp_path)
    _adopt_sweep(publisher, ram_root, src_root, entries, 4)
    print("OVER_CEILING", json.dumps({
        "total_decodes": counters.total,
        "distinct_documents": len(counters.decodes),
        "reclaims": counters.reclaims,
        "retained_index_bytes": publisher._index_bytes()}, sort_keys=True))
    # Re-decode rather than retain beyond the budget.
    assert counters.total > len(counters.decodes), (
        "an all-RAM working set over the ceiling was retained anyway")
