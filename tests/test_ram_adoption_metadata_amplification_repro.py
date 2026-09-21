"""Measure metadata amplification in RAM-destination adoption (repro, no fix).

The live Stage A 8ca8952c incident: the RAM head promotion (a201e161) held
the /ram/prewarm ownership lock while showing ~29 GB rchar over ~3,020
large reads with zero writes, 16 worker threads and ~527 CPU-s, and was
auto-withdrawn after the consumer failed. Native audit
(stage-ownership-lock-liveness-audit-astra.md) traced the source path --
``_Copier._copy_one`` -> ``_StagedPublisher.try_adopt`` -> ``_proof_search``
per entry under the destination-root ownership lock -- and named the
publisher's bounded metadata index (192 MiB, #761) as the concrete possible
amplification path: on a retention miss every lookup re-reads and re-decodes
an entire fragment or sidecar, and each miss first runs ``_reclaim``. That
audit is source-only; this file makes the mechanism measurable on a tiny
real fixture.

No production behavior changes here. The test pins today's threshold
behavior from both sides so the number, not the narrative, is the artifact:

* with an ample budget, each document is decoded once for the whole sweep
  and nothing is reclaimed -- the cache does its job;
* with a budget that cannot retain even one owner's records (the live
  forest shape: several prior consumers whose fragments/sidecars each name
  every destination), decodes and reclaims grow per attempted adoption --
  deterministic, bounded-payload proof of the amplification, scaled far
  below the live 192 MiB ceiling by the only lever that is pure fixture
  size (the budget constant), with real writers, real documents, real
  identities and the real copier and publisher.

The fail-closed contract is asserted alongside: cross-consumer owners
adopt legitimately (the ownership contract), and a corrupt fragment makes
every affected lookup refuse unknown -- never a silent skip into success.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import sys
import threading
import time

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


def _hex64(seed: str) -> str:
    return hashlib.sha256(seed.encode()).hexdigest()


class _Counters:
    """Read/decode/reclaim counters around the publisher's metadata I/O."""

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

    def total_decodes(self) -> int:
        return sum(self.decodes.values())

    def documents(self) -> int:
        return len(self.decodes)


def _fixture(tmp_path: Path):
    """A real RAM-destination forest: OWNERS prior consumers, N_DESTS files.

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
    for index in range(N_DESTS):
        name = f"model-{index:05d}-of-{N_DESTS:05d}.safetensors"
        source = src_root / name
        source.write_bytes(PAYLOAD)
        dest = ram_root / name
        dest.write_bytes(PAYLOAD)
        entries.append({"path": str(source), "offset": 0,
                        "bytes": len(PAYLOAD), "sha256": digest})
    for owner_index in range(OWNERS):
        consumer = _hex64(f"prior-consumer-{owner_index}")
        mover = _hex64(f"prior-mover-{owner_index}")
        frag_entries, mat_entries = {}, {}
        for entry, source in zip(entries, sorted(src_root.iterdir())):
            key = residency_map.residency_map_key(entry["path"], 0)
            identity = reader_lease.stat_identity(str(ram_root / source.name))
            assert identity is not None
            frag_entries[key] = {
                "stage_path": str(ram_root / source.name),
                "bytes": entry["bytes"], "offset": 0,
                "sha256": entry["sha256"]}
            mat_entries[key] = {
                "stage_path": str(ram_root / source.name),
                "bytes": entry["bytes"], "sha256": entry["sha256"],
                "file_id": identity}
        residency_map.write_fragment(residence, {
            "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
            "consumer_action_key": consumer, "mover_action_key": mover,
            "tier_id": TIER, "stage_root": str(ram_root), "epoch": EPOCH,
            "manifest_sha256": MANIFEST, "entries": frag_entries})
        reader_lease.write_material(
            residence, consumer_action_key=consumer,
            mover_action_key=mover, tier_id=TIER, stage_root=str(ram_root),
            manifest_sha256=MANIFEST,
            generation=reader_lease.mint_generation(),
            entries=mat_entries, epoch=EPOCH)
    return queue, residence, ram_root, src_root, entries


def _adopt_all(tmp_path, monkeypatch, *, budget: int | None):
    """One promotion-shaped sweep: try_adopt every destination, no copying.

    Returns (counters, wall_seconds, per-entry adopt timings, copier). With
    ``budget`` the #761 ceiling is scaled (fixture-size lever only); with
    ``None`` the production 192 MiB default governs.
    """
    queue, residence, ram_root, src_root, entries = _fixture(tmp_path)
    if budget is not None:
        monkeypatch.setattr(stage_move, "_INDEX_BUDGET_BYTES", budget)
    counters = _Counters(monkeypatch)
    publisher = stage_move._StagedPublisher(
        queue=queue, stage_root=ram_root, residency_root=residence,
        mover_action_key=_hex64("promoting-mover"), manifest_sha256=MANIFEST,
        tier_id=TIER, cas_root=str(tmp_path / "cas"))
    copier = stage_move._Copier(
        mounts=prewarm_loop.MountMap([f"{src_root}={src_root}"]),
        pacer=None, stage_root=ram_root, mount_prefix=str(src_root),
        block=1 << 16, workers=1, owner=_hex64("promoting-mover"),
        publisher=publisher)
    whole = {entry["path"] for entry in entries}
    adopt_seconds = []
    started = time.monotonic()
    for entry in entries:
        destination = ram_root / Path(entry["path"]).name
        before = time.monotonic()
        adopted = publisher.try_adopt(
            entry, destination, stage_move._origin_id_of(entry["path"]))
        adopt_seconds.append(time.monotonic() - before)
        assert adopted is not None, f"destination lost proof: {destination}"
    wall = time.monotonic() - started
    return {
        "budget": budget if budget is not None else stage_move._INDEX_BUDGET_BYTES,
        "entries": len(entries),
        "documents_decoded_once_each": counters.documents(),
        "total_decodes": counters.total_decodes(),
        "metadata_bytes_read": counters.decode_bytes,
        "reclaims": counters.reclaims,
        "adopt_mean_ms": round(1000 * sum(adopt_seconds) / len(adopt_seconds), 3),
        "adopt_max_ms": round(1000 * max(adopt_seconds), 3),
        "wall_s": round(wall, 3),
    }


def test_adoption_decode_amplification_under_a_retention_refusing_budget(
        tmp_path, monkeypatch):
    """Today's threshold behavior, measured from both sides of the ceiling.

    Ample budget: one decode per document for the whole sweep, no reclaim.
    A budget below one owner's retained records: every lookup re-decodes the
    whole forest and reclaims -- the amplification the live run showed at
    the 192 MiB ceiling, demonstrated deterministically at fixture scale.
    """
    ample = _adopt_all(tmp_path / "ample", monkeypatch, budget=None)
    print("AMPLE", json.dumps(ample, sort_keys=True))
    # 4 owners x (fragment + sidecar) documents, decoded once each.
    assert ample["total_decodes"] <= 2 * OWNERS, (
        f"ample budget still re-decodes: {ample}")
    assert ample["reclaims"] == 0

    # One owner's fragment+sidecar retain cost is ~150 KB at this fixture
    # size (see stage_move charge constants); 64 KiB refuses every record.
    constrained = _adopt_all(tmp_path / "capped", monkeypatch, budget=64 << 10)
    print("CONSTRAINED", json.dumps(constrained, sort_keys=True))
    assert constrained["total_decodes"] >= 3 * ample["total_decodes"], (
        f"no amplification demonstrated: {constrained} vs {ample}")
    assert constrained["reclaims"] >= 1
    # The proof answers stay correct in both arms: every destination
    # adopted, byte counts exact, nothing copied (adoption is metadata).
    for arm in (ample, constrained):
        assert arm["entries"] == N_DESTS


def test_a_corrupt_foreign_fragment_refuses_unknown_not_silent(
        tmp_path, monkeypatch):
    """The fail-closed contract beside the counters: taint, never skip."""

    queue, residence, ram_root, _src, entries = _fixture(tmp_path)
    consumer_dirs = sorted(
        path for path in residence.iterdir()
        if path.is_dir() and path.name not in ("leases", "material"))
    fragment = next(name for name in sorted(os.listdir(consumer_dirs[0]))
                    if name.endswith(".json"))
    (consumer_dirs[0] / fragment).write_text("{not json")
    publisher = stage_move._StagedPublisher(
        queue=queue, stage_root=ram_root, residency_root=residence,
        mover_action_key=_hex64("promoting-mover"), manifest_sha256=MANIFEST,
        tier_id=TIER, cas_root=str(tmp_path / "cas"))
    destination = ram_root / Path(entries[0]["path"]).name
    adopted = publisher.try_adopt(
        entries[0], destination,
        stage_move._origin_id_of(entries[0]["path"]))
    assert adopted is None, "a corrupt proof must not adopt"
    norm = os.path.normpath(str(destination))
    proof, standing, detail = publisher._proof_search(
        norm, entries[0]["bytes"], entries[0]["sha256"])
    assert standing == "unknown", (
        f"corrupt fragment must read unknown, saw {standing}: {detail}")


def test_adoption_lock_hold_is_the_metadata_scan(tmp_path, monkeypatch):
    """Per-entry lock-held time scales with the metadata work, not bytes.

    try_adopt holds the destination-root ownership lock across the whole
    _proof_search. Under the ample budget the per-entry hold is the cached
    path-set lookups; under a refusing budget it includes the full forest
    decode. The ratio between the arms is the reader-visible contention a
    same-root reader would queue behind, at fixture scale.
    """
    ample = _adopt_all(tmp_path / "ample", monkeypatch, budget=None)
    constrained = _adopt_all(tmp_path / "capped", monkeypatch, budget=64 << 10)
    print("LOCK_RATIO", json.dumps({
        "ample_adopt_max_ms": ample["adopt_max_ms"],
        "constrained_adopt_max_ms": constrained["adopt_max_ms"],
        "decode_ratio": round(constrained["total_decodes"]
                              / max(1, ample["total_decodes"]), 1)}, sort_keys=True))
    assert constrained["adopt_max_ms"] >= ample["adopt_max_ms"]
