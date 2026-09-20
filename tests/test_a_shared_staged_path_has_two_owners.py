"""One staged file can have two owners; an egress must not orphan either.

Two shapes of the same defect in the shared copier/fragment mechanism:

1. Forward and reverse passes stage the same source extent -- the same
   ``(path, offset, bytes)`` triple -- through different movers (different
   manifests, different consumers, or two read phases of one v2 plan), and
   ``stage_relative`` derives one staged name for both.  The first owner's
   egress unlinks the file while the second owner's fragment still vouches
   for it and its tokens stay held: a hole behind a live map.
2. A split range lives on the stage as ``<rel>.pbrange/<offset>-<size>`` from
   byte zero, but a promotion resolves its source as the original pool path
   at the manifest offset.  Whole-file ranges promote; anything split fails
   to even open its source.
"""
from __future__ import annotations

import json
import threading
from pathlib import Path
import sys
import types

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
from prismabuild import pool, residency_map, storage_tiers  # noqa: E402
import prewarm_loop  # noqa: E402
import stage_release  # noqa: E402
from stage_move import _Copier, whole_file_paths  # noqa: E402
import ram_promote  # noqa: E402

CONSUMER_A = "a" * 64
CONSUMER_B = "b" * 64
MOVER_A = "1" * 64
MOVER_B = "2" * 64
TIER = "prismabuild-stage:dl380g10"
RAM_TIER = "ram:dl380g10"
GIB = storage_tiers.GIB
MIB = 1024 * 1024


def _fragment(root: Path, stage: Path, consumer: str, mover: str,
              source: str, staged: Path, size: int, tier: str = TIER,
              epoch: str | None = None) -> None:
    body: dict[str, object] = {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": consumer, "mover_action_key": mover,
        "tier_id": tier, "stage_root": str(stage),
        "manifest_sha256": "a" * 64,
        "entries": {
            residency_map.residency_map_key(source, 0): {
                "stage_path": str(staged), "bytes": size,
                "sha256": "b" * 64, "offset": 0,
            },
        },
    }
    if epoch is not None:
        body["epoch"] = epoch
    residency_map.write_fragment(root, body)


@pytest.fixture()
def fleet(tmp_path: Path):
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    stage = tmp_path / "stage"
    stage.mkdir()
    stage_release.register_stage_root(queue, tier_id=TIER, stage_root=stage)
    return queue, stage


def test_one_owners_egress_leaves_the_other_owners_bytes(fleet) -> None:
    """The forward/reverse same-extent shape: two movers, one staged file."""

    queue, stage = fleet
    shared = stage / "model" / "layer.safetensors"
    shared.parent.mkdir(parents=True)
    shared.write_bytes(b"\0" * 4096)
    root = queue.root / pool.RESIDENCY
    _fragment(root, stage, CONSUMER_A, MOVER_A,
              "/mnt/shared/model/layer.safetensors", shared, 4096)
    _fragment(root, stage, CONSUMER_B, MOVER_B,
              "/mnt/shared/model/layer.safetensors", shared, 4096)

    first = stage_release.evict(queue, MOVER_A, consumer_action_key=CONSUMER_A,
                                stage_root=str(stage))
    assert shared.exists(), (
        "mover B still vouches for this file; A's egress must not unlink it")
    assert first["complete"] is True
    assert first["entries_shared"] == 1
    assert first["entries_deleted"] == 0
    assert MOVER_B[:12] in first["shared_with"][0]

    second = stage_release.evict(queue, MOVER_B, consumer_action_key=CONSUMER_B,
                                 stage_root=str(stage))
    assert not shared.exists()
    assert second["complete"] is True
    assert second["entries_shared"] == 0
    assert second["entries_deleted"] == 1


def _split_manifest(pool_dir: Path) -> dict[str, object]:
    entries = [
        {"path": "/mnt/shared/shard.bin", "offset": 0, "bytes": MIB,
         "sha256": None},
        {"path": "/mnt/shared/shard.bin", "offset": MIB, "bytes": MIB,
         "sha256": None},
    ]
    return {
        "schema": "prismaquant.prismabuild.data_manifest.v1",
        "produced_by": {}, "annotations": {},
        "mount_prefix": "/mnt/shared",
        "entries": entries,
        "entry_count": 2,
        "total_bytes": 2 * MIB,
    }


def test_a_split_range_promotes_from_its_staged_shard_path(tmp_path: Path) -> None:
    """A nonzero-offset split range must promote, not fail to open its source."""

    pool_dir = tmp_path / "pool"
    pool_dir.mkdir()
    (pool_dir / "shard.bin").write_bytes(b"\x01" * MIB + b"\x02" * MIB)
    stage = tmp_path / "stage"
    stage.mkdir()
    ram = tmp_path / "ram"
    ram.mkdir()
    assert storage_tiers.ensure_ram_epoch(ram, host="test") is not None
    manifest_path = tmp_path / "manifest.json"
    manifest = _split_manifest(pool_dir)
    manifest_path.write_text(json.dumps(manifest))

    entries = prewarm_loop.manifest_read_entries(manifest)
    assert whole_file_paths(entries) == set()
    stage_copier = _Copier(
        mounts=prewarm_loop.MountMap([f"/mnt/shared={pool_dir}"]),
        pacer=None, stage_root=stage, mount_prefix="/mnt/shared",
        block=MIB, workers=2, owner="stage-mover")
    stage_copier.run(entries, whole=set(), stop=threading.Event())
    assert stage_copier.errors == []
    assert stage_copier.bytes_staged == 2 * MIB

    args = ram_promote.build_parser().parse_args([
        "--pool-root", str(tmp_path / "pool-root"),
        "--manifest", str(manifest_path),
        "--consumer-action-key", CONSUMER_A,
        "--tier-id", RAM_TIER,
        "--ram-root", str(ram),
        "--source-stage-root", str(stage),
        "--manifest-sha256", "0" * 64,
        "--range-start-bytes", "0",
        "--range-end-bytes", str(2 * MIB),
        "--action-key", MOVER_A,
        "--residency-root", str(tmp_path / "residency"),
        "--block", str(MIB),
        "--readers", "2",
    ])
    receipt = ram_promote.promote(args)
    assert receipt["complete"] is True, receipt
    assert int(receipt["bytes_staged"]) == 2 * MIB
    first = (ram / "shard.bin.pbrange" / f"0-{MIB}").read_bytes()
    second = (ram / "shard.bin.pbrange" / f"{MIB}-{MIB}").read_bytes()
    assert first == b"\x01" * MIB
    assert second == b"\x02" * MIB


def _shared_pair(tmp_path: Path):
    """Two consumers, two movers, one staged file; returns (queue, stage, file)."""

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    stage = tmp_path / "stage"
    stage.mkdir()
    stage_release.register_stage_root(queue, tier_id=TIER, stage_root=stage)
    shared = stage / "model" / "layer.safetensors"
    shared.parent.mkdir(parents=True)
    shared.write_bytes(b"\0" * 4096)
    root = queue.root / pool.RESIDENCY
    _fragment(root, stage, CONSUMER_A, MOVER_A,
              "/mnt/shared/model/layer.safetensors", shared, 4096)
    _fragment(root, stage, CONSUMER_B, MOVER_B,
              "/mnt/shared/model/layer.safetensors", shared, 4096)
    return queue, stage, shared


def test_dual_egress_deletes_exactly_once(tmp_path: Path) -> None:
    """Concurrent egresses order through the ownership lock: last one deletes.

    Same-process threads serialize on the lock registry, exercising the same
    scan-then-act path cross-process fcntl serializes.  Every schedule must
    end here: one deletion total, both receipts complete, both fragments
    dropped, the file gone.
    """

    import threading

    for _ in range(20):
        queue, stage, shared = _shared_pair(tmp_path / f"run-{_}")
        order = []
        barrier = threading.Barrier(3)

        def run(which, mover, consumer):
            barrier.wait(timeout=60)
            receipt = stage_release.evict(
                queue, mover, consumer_action_key=consumer,
                stage_root=str(stage))
            order.append((which, receipt))

        threads = [
            threading.Thread(target=run, args=("a", MOVER_A, CONSUMER_A)),
            threading.Thread(target=run, args=("b", MOVER_B, CONSUMER_B)),
        ]
        for thread in threads:
            thread.start()
        barrier.wait(timeout=60)
        for thread in threads:
            thread.join(timeout=120)
            assert not thread.is_alive()
        assert not shared.exists()
        by_which = dict(order)
        assert by_which["a"]["complete"] is True
        assert by_which["b"]["complete"] is True
        assert (by_which["a"]["entries_deleted"]
                + by_which["b"]["entries_deleted"]) == 1
        root = queue.root / pool.RESIDENCY
        assert list((root / CONSUMER_A).glob("*.json")) == []
        assert list((root / CONSUMER_B).glob("*.json")) == []


def _write_claimed_copy_shape(queue: pool.PoolQueue, cas: Path, key: str,
                              manifest: dict, start: int, end: int) -> None:
    """A claimed mover row plus its sealed CAS request and manifest blob."""

    blob = json.dumps(manifest).encode("utf-8")
    digest = __import__("hashlib").sha256(blob).hexdigest()
    shard = cas / "blobs" / digest[:2]
    shard.mkdir(parents=True, exist_ok=True)
    (shard / digest).write_bytes(blob)
    request = {
        "action_key": key,
        "params": {
            "command": ["python3", "stage_move.py",
                        "--range-start-bytes", str(start),
                        "--range-end-bytes", str(end)],
        },
        "inputs": [{"id": "pbcampaign.data-manifest",
                    "sha256": digest, "bytes": len(blob)}],
    }
    shard = cas / "requests" / key[:2]
    shard.mkdir(parents=True, exist_ok=True)
    (shard / f"{key}.json").write_text(json.dumps(request))
    claimed = queue.dir(pool.CLAIMED)
    claimed.mkdir(parents=True, exist_ok=True)
    (claimed / f"{key}.json").write_text(json.dumps({
        "action_key": key,
        # The sealed claim names its own CAS root, the way publication_row
        # seals it; the egress reads it off the record, never assumes it.
        "cas_root": str(cas),
        "resources": {"cpu": 2, "mem_gb": 1, f"stage_gib@{TIER}": 1},
    }))


def _one_entry_manifest() -> dict[str, object]:
    return {
        "schema": "prismaquant.prismabuild.data_manifest.v1",
        "produced_by": {}, "annotations": {},
        "mount_prefix": "/mnt/shared",
        "entries": [{"path": "/mnt/shared/model/layer.safetensors", "offset": 0,
                     "bytes": 4096, "sha256": None}],
        "entry_count": 1,
        "total_bytes": 4096,
    }


def test_a_claimed_copy_without_a_fragment_still_owns_its_paths(fleet) -> None:
    """The claim-to-fragment transition, decided deterministically.

    A claimed mover has no fragment yet, but its sealed request already names
    its range; an egress that unlinked those bytes would leave the publisher's
    fragment dangling.  The egress must skip on the claim alone.
    """

    import hashlib

    queue, stage = fleet
    shared = stage / "model" / "layer.safetensors"
    shared.parent.mkdir(parents=True)
    shared.write_bytes(b"\0" * 4096)
    root = queue.root / pool.RESIDENCY
    _fragment(root, stage, CONSUMER_A, MOVER_A,
              "/mnt/shared/model/layer.safetensors", shared, 4096)
    cas = queue.root.parent / "cas"
    manifest = _one_entry_manifest()
    _write_claimed_copy_shape(queue, cas, MOVER_B, manifest, 0, 4096)

    receipt = stage_release.evict(queue, MOVER_A,
                                  consumer_action_key=CONSUMER_A,
                                  stage_root=str(stage))
    assert shared.exists()
    assert receipt["complete"] is True
    assert receipt["entries_shared"] == 1
    assert receipt["shared_with"] == ["in-flight-copy"]
    assert receipt["entries_deleted"] == 0


def test_a_copy_start_waits_out_an_in_progress_egress_snapshot(tmp_path: Path) -> None:
    """The start gate orders a new claim after a snapshotting egress.

    Event-driven, no sleeps: the main thread holds the ownership lock (the
    egress), the gate thread must not pass until it is released.  Timeouts are
    deadlock tripwires only.
    """

    import threading

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    stage = tmp_path / "stage"
    stage.mkdir()
    order: list[str] = []
    attempting = threading.Event()
    done = threading.Event()

    def gate() -> None:
        attempting.set()
        queue.ownership_start_gate(str(stage))
        order.append("gate")
        done.set()

    with queue.stage_ownership_lock(str(stage)):
        thread = threading.Thread(target=gate)
        thread.start()
        assert attempting.wait(timeout=30)
        assert order == [], "the gate passed while the egress held the lock"
    assert done.wait(timeout=30)
    thread.join(timeout=30)
    assert not thread.is_alive()
    assert order == ["gate"]


def test_the_ownership_lock_excludes_another_process(tmp_path: Path) -> None:
    """The load-bearing primitive, proved across fork: exclusion then release."""

    import os

    if os.name != "posix":
        pytest.skip("POSIX locks need fork")
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    stage = tmp_path / "stage"
    stage.mkdir()

    def try_acquire() -> int:
        pid = os.fork()
        if pid != 0:
            _, status = os.waitpid(pid, 0)
            return os.waitstatus_to_exitcode(status)
        try:
            with queue.stage_ownership_lock(str(stage), blocking=False) as ok:
                os._exit(0 if not ok else 1)
        except BaseException:
            os._exit(2)

    with queue.stage_ownership_lock(str(stage)):
        assert try_acquire() == 0, "another process acquired a held lock"
    assert try_acquire() == 1, "release did not free the lock"


def test_ownership_scan_scales_with_fragments_not_entries(tmp_path: Path) -> None:
    """One walk, intersected: corpus-sized metadata in well under a bound."""

    import time

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    stage = tmp_path / "stage"
    stage.mkdir()
    stage_release.register_stage_root(queue, tier_id=TIER, stage_root=stage)
    root = queue.root / pool.RESIDENCY
    target = stage / "wanted.bin"
    target.write_bytes(b"\0" * 1024)
    for consumer in range(40):
        for mover in range(5):
            residency_map.write_fragment(root, {
                "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
                "consumer_action_key": f"{consumer:064d}"[-64:].replace(" ", "0"),
                "mover_action_key": f"{mover:064d}"[-64:].replace(" ", "0"),
                "tier_id": TIER, "stage_root": str(stage),
                "manifest_sha256": "a" * 64,
                "entries": {
                    f"0:/mnt/shared/noise-{consumer}-{mover}-{i}.bin": {
                        "stage_path": str(stage / f"n-{consumer}-{mover}-{i}.bin"),
                        "bytes": 1024, "sha256": "b" * 64, "offset": 0,
                    } for i in range(3)
                },
            })
    _fragment(root, stage, CONSUMER_A, MOVER_A,
              "/mnt/shared/wanted.bin", target, 1024)
    wanted = {str(target)}
    started = time.monotonic()
    owners, tainted = stage_release._fragment_owners(
        root, wanted, except_consumer=CONSUMER_A, except_mover=MOVER_A)
    elapsed = time.monotonic() - started
    assert tainted == []
    assert owners == {}
    # A second consumer vouches for the wanted file: still cheap, and found.
    _fragment(root, stage, CONSUMER_B, MOVER_B,
              "/mnt/shared/wanted.bin", target, 1024)
    started = time.monotonic()
    owners, tainted = stage_release._fragment_owners(
        root, wanted, except_consumer=CONSUMER_A, except_mover=MOVER_A)
    elapsed = time.monotonic() - started
    assert tainted == []
    assert owners == {str(target): {(CONSUMER_B, MOVER_B)}}
    assert elapsed < 5.0, f"owners scan took {elapsed:.2f}s over 201 fragments"


def test_a_claim_to_fragment_handoff_between_the_two_reads_is_covered(fleet,
                                                                      monkeypatch) -> None:
    """The snapshot order, proved by forcing the transition inside it.

    A publisher that releases its claim and lands its fragment exactly
    between the egress's two readers is covered if and only if claims are
    read first: the first read already attributed the copy.  Fragments-first
    would miss it on both reads and unlink a file whose fragment lands a
    moment later.  The wrapper rendezvouses the flip between the real first
    and second reads -- no sleeps, timeouts are deadlock tripwires only.
    """

    import threading

    queue, stage = fleet
    shared = stage / "model" / "layer.safetensors"
    shared.parent.mkdir(parents=True)
    shared.write_bytes(b"\0" * 4096)
    root = queue.root / pool.RESIDENCY
    _fragment(root, stage, CONSUMER_A, MOVER_A,
              "/mnt/shared/model/layer.safetensors", shared, 4096)
    cas = queue.root.parent / "cas"
    manifest = _one_entry_manifest()
    mover_c = "3" * 64
    consumer_c = "d" * 64
    _write_claimed_copy_shape(queue, cas, mover_c, manifest, 0, 4096)

    real_claimed_paths = stage_release._claimed_paths
    read_first = threading.Event()
    flipped = threading.Event()

    def rendezvous(queue_arg, tier_id):
        result = real_claimed_paths(queue_arg, tier_id)
        read_first.set()
        assert flipped.wait(timeout=60), "flip never ran: deadlock"
        return result

    monkeypatch.setattr(stage_release, "_claimed_paths", rendezvous)
    outcome: list[dict] = []

    def egress() -> None:
        outcome.append(stage_release.evict(
            queue, MOVER_A, consumer_action_key=CONSUMER_A,
            stage_root=str(stage)))

    thread = threading.Thread(target=egress)
    thread.start()
    assert read_first.wait(timeout=60)
    # The handoff, in publication order: fragment lands before the claim
    # record is gone, exactly as a finishing worker does it.
    _fragment(root, stage, consumer_c, mover_c,
              "/mnt/shared/model/layer.safetensors", shared, 4096)
    (queue.dir(pool.CLAIMED) / f"{mover_c}.json").unlink()
    flipped.set()
    thread.join(timeout=120)
    assert not thread.is_alive()
    assert shared.exists(), (
        "the claim-to-fragment handoff landed between the two readers and "
        "the file was still unlinked: snapshot order is wrong")
    assert outcome[0]["complete"] is True
    assert outcome[0]["entries_shared"] == 1
    assert outcome[0]["entries_deleted"] == 0
