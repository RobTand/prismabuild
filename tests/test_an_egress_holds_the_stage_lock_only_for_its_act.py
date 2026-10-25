"""Egress preparation permits publication; only the validated act owns the stage.

Small real files replace the old performance workload. Spawned lock probes do
not inherit the parent's reentrant ownership state. Every probe finishes by
protocol, never by expiry. Synthetic phase costs test receipt accounting, not
machine speed; module-local clock bindings share the synthetic grant clock.
"""
from __future__ import annotations

from contextlib import contextmanager
import hashlib
import multiprocessing
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import pool, reader_lease, residency_map, storage_tiers
from prismabuild import staged_path, kept_reads
from record_store_trust import ObservedTrust

TIER = "prismabuild-stage:dl380g10"
STAGE_KIND = f"stage_gib@{TIER}"
ENTRY_BYTES = 1024
RANGE_ENTRIES = 4
DIGEST = "a" * 64


def _key(label: str) -> str:
    return hashlib.sha256(f"test-988:{label}".encode()).hexdigest()


LANDED_CONSUMER = _key("landed-consumer")
LANDED_MOVER = _key("landed-mover")
LANDED_MANIFEST = _key("landed-manifest")


def _land_range(queue: pool.PoolQueue, stage: Path, root: Path) -> None:
    """A complete mover's range: files, fragment, material and its tier token."""

    entries: dict[str, dict[str, object]] = {}
    material: dict[str, dict[str, object]] = {}
    for number in range(RANGE_ENTRIES):
        declared = f"/pool/landed/part-{number}.bin"
        key = residency_map.residency_map_key(declared, 0)
        path = stage / "landed" / f"d{number // 500:03d}" / f"part-{number}.bin"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"\0" * ENTRY_BYTES)
        entries[key] = {"stage_path": str(path), "bytes": ENTRY_BYTES,
                        "offset": 0, "sha256": DIGEST}
        material[key] = {"stage_path": str(path), "bytes": ENTRY_BYTES,
                         "sha256": DIGEST,
                         "file_id": reader_lease.stat_identity(str(path))}
    staged_path.publish_vouch(
        root, {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": LANDED_CONSUMER,
        "mover_action_key": LANDED_MOVER,
        "tier_id": TIER, "stage_root": str(stage),
        "manifest_sha256": LANDED_MANIFEST, "entries": entries},
        generation=reader_lease.mint_generation(),
        material_entries=material)
    # The mover that staged it holds its tier token until an egress frees it.
    queue.publish(action_key=LANDED_MOVER, cas_root=queue.root / "cas",
                  checkout_root=queue.root / "co",
                  worker_script=queue.root / "worker.py",
                  resources={"cpu": 1, "mem_gb": 1, STAGE_KIND: 1},
                  residency={"schema": pool.RESIDENCY_SCHEMA_V1,
                             "tier_id": TIER,
                             "manifest_sha256": LANDED_MANIFEST,
                             "manifest_bytes": 1 << 40,
                             "range_start_bytes": 0,
                             "range_end_bytes": storage_tiers.GIB},
                  max_attempts=1, retry_safe=False)
    claimed = queue.claim(capacity={"cpu": 4, "mem_gb": 8}, tags=["dl380g10"])
    assert claimed is not None and claimed["action_key"] == LANDED_MOVER
    queue.record_move(LANDED_MOVER, {
        "consumer_action_key": LANDED_CONSUMER, "tier_id": TIER,
        "stage_root": str(stage), "manifest_sha256": LANDED_MANIFEST,
        "range_start_bytes": 0, "range_end_bytes": storage_tiers.GIB,
        "bytes_staged": storage_tiers.GIB, "complete": True})
    queue.finish(LANDED_MOVER, status="executed")
    assert queue.tier(TIER).holdings(LANDED_MOVER) == {'stage_gib': 1}


def _publication_probe(pool_root, stage, index, publish):
    """Fresh process: test real exclusion, publishing only when granted."""
    queue = pool.PoolQueue(Path(pool_root))
    with queue.stage_ownership_lock(stage, blocking=False) as grant:
        available = bool(grant)
    if publish:
        assert available, "preparation excluded a publisher"
        publisher = staged_path.StagedPublisher(
            queue=queue, stage_root=stage,
            residency_root=queue.root / pool.record_store.RESIDENCY,
            mover_action_key=_key(f"publisher-{index}"),
            manifest_sha256=_key(f"manifest-{index}"), tier_id=TIER,
            cas_root=queue.root.parent / "cas",
            consumer_action_key=_key(f"consumer-{index}"))
        publisher.begin_material()
        destination = Path(stage) / f"publisher-{index}" / "part.bin"
        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary = destination.with_suffix(".partial")
        temporary.write_bytes(b"1" * ENTRY_BYTES)
        publisher.publish(
            {"bytes": ENTRY_BYTES, "sha256": DIGEST, "offset": 0,
             "path": str(destination)}, destination, temporary, DIGEST)
        assert destination.read_bytes() == b"1" * ENTRY_BYTES
        assert not temporary.exists()
    else:
        assert not available, "the eviction act lost inter-process exclusion"


def test_a_whole_range_egress_never_parks_the_publishers_for_its_range(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.records.ensure_layout()
    queue.tier(TIER).mint({'stage_gib': 8})
    stage = (tmp_path / "stage").resolve()
    stage.mkdir()
    assert staged_path.register_stage_root(
        queue, tier_id=TIER, stage_root=stage) == "registered"
    root = queue.root / pool.record_store.RESIDENCY
    _land_range(queue, stage, root)
    monkeypatch.setattr(kept_reads, "filesystem_trust", ObservedTrust())

    def probe(index, publish):
        worker = multiprocessing.get_context("spawn").Process(
            target=_publication_probe,
            args=(str(queue.root), str(stage), index, publish))
        worker.start()
        try:
            worker.join()
            assert worker.exitcode == 0
        finally:
            if worker.is_alive():
                worker.terminate()
                worker.join()
            worker.close()

    holding = False
    phases = []
    clock = SimpleNamespace(value=0)
    # Do not mutate the shared system time module (pool and children use it).
    monkeypatch.setattr(staged_path, "time", SimpleNamespace(
        perf_counter=lambda: clock.value, time=lambda: 0))
    # PoolQueue supplies the grant timestamp; share the synthetic clock there.
    monkeypatch.setattr(pool, "time", SimpleNamespace(
        **(vars(pool.time) | {"perf_counter": lambda: clock.value})))
    parsed_under_lock = {"fragment": 0, "pin": 0, "material": 0}

    def counted(kind, validate):
        def wrapper(*args, **kwargs):
            if holding:
                parsed_under_lock[kind] += 1
            return validate(*args, **kwargs)
        return wrapper

    for module, name, kind in (
            (residency_map, "validate_fragment", "fragment"),
            (reader_lease, "validate_pin", "pin"),
            (reader_lease, "validate_material", "material")):
        monkeypatch.setattr(module, name, counted(kind, getattr(module, name)))

    original_lock = queue.stage_ownership_lock
    holds = []

    @contextmanager
    def observed_lock(stage_root, *, blocking=True):
        nonlocal holding
        assert not holding
        with original_lock(stage_root, blocking=blocking) as grant:
            assert grant is True
            clock.value += 2  # synthetic acquisition cost
            granted = clock.value
            holding = True
            phases.append("grant")
            try:
                yield grant  # preserve the real lock's boolean protocol
            finally:
                holds.append(clock.value - granted)
                holding = False
                phases.append("release")

    monkeypatch.setattr(queue, "stage_ownership_lock", observed_lock)
    census = staged_path.ownership_census

    def observed_census(*args, **kwargs):
        if holding:
            phases.append("locked-census")
            probe(0, False)
            clock.value += 3
        else:
            phases.append("hint")
            # Publication completes *inside* preparation, not just before evict.
            probe(0, True)
            clock.value += 11
        return census(*args, **kwargs)

    monkeypatch.setattr(staged_path, "ownership_census", observed_census)
    own_material = staged_path._read_own_material

    def observed_material(*args, **kwargs):
        # Preparation parses it; the protected act rechecks the file version.
        # The independent validator counters below require that recheck to reuse.
        phases.append("material-recheck" if holding else "material")
        return own_material(*args, **kwargs)

    monkeypatch.setattr(staged_path, "_read_own_material", observed_material)
    fences = staged_path._entry_fences

    def observed_fences(*args, **kwargs):
        assert not holding
        phases.append("fences")
        return fences(*args, **kwargs)

    monkeypatch.setattr(staged_path, "_entry_fences", observed_fences)
    owned = staged_path._evict_owned

    def observed_act(*args, **kwargs):
        assert holding
        phases.append("act")
        return owned(*args, **kwargs)

    monkeypatch.setattr(staged_path, "_evict_owned", observed_act)
    unlink = staged_path.os.unlink
    deleted = []

    def observed_unlink(path, *args, **kwargs):
        if stage / "landed" in Path(path).parents:
            assert holding
            deleted.append(Path(path))
            clock.value += 1
        return unlink(path, *args, **kwargs)

    monkeypatch.setattr(staged_path, "os", SimpleNamespace(
        **(vars(staged_path.os) | {"unlink": observed_unlink})))
    prune = staged_path.prune_empty

    def observed_prune(*args, **kwargs):
        assert not holding
        phases.append("prune")
        clock.value += 7
        return prune(*args, **kwargs)

    monkeypatch.setattr(staged_path, "prune_empty", observed_prune)
    receipt = staged_path.evict(
        queue, LANDED_MOVER, consumer_action_key=LANDED_CONSUMER,
        stage_root=str(stage), reason="beyond-horizon", whole=True)
    probe(1, True)

    assert phases == ["hint", "material", "fences", "grant", "act",
                      "locked-census", "material-recheck", "release", "prune"]
    assert holds == [3 + RANGE_ENTRIES]
    assert len(deleted) == RANGE_ENTRIES
    assert receipt["complete"] is True, receipt
    assert receipt["entries_deleted"] == RANGE_ENTRIES
    assert receipt["entries_judged"] == RANGE_ENTRIES
    assert receipt["tokens_released"] == 1
    assert queue.tier(TIER).holdings(LANDED_MOVER) == {}
    assert not residency_map.fragment_path(root, LANDED_CONSUMER, LANDED_MOVER).exists()
    assert not list((stage / "landed").rglob("*.bin"))
    for index in range(2):
        assert (stage / f"publisher-{index}" / "part.bin").read_bytes() == b"1" * ENTRY_BYTES
    assert parsed_under_lock == {"fragment": 0, "pin": 0, "material": 0}
    assert receipt["locked_parses"] == 0
    assert receipt["locked_reuses"] >= 2
    assert receipt["lock_wait_s"] == 2
    assert receipt["lock_held_s"] == holds[0]
    assert receipt["census_s"] == 11
    assert receipt["census_validate_s"] == 3
    assert receipt["unlink_s"] == RANGE_ENTRIES
    assert receipt["prune_s"] == 7
