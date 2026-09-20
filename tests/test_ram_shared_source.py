"""Shared staged-source identity: overlapping copies must converge.

Live evidence (PQ869): two consumers' stage movers wrote the same
content-addressed staged pathname; each `os.replace` minted a new
inode, invalidating the other's published material identity, and the
RAM promotion's cover proof refused `file-identity-changed` on all 3
attempts -- same bytes, new inode.

Proved here on real `stage_move.move` + `reader_lease.acquire` +
`ram_promote.promote` at tiny sizes: a second overlapping copy of
identical bytes must preserve the published incarnation (no replace),
a live pin/copy handoff must survive it, simultaneous first
publications converge, concurrent copies stay correct, and
crash-partial temps still recover. A genuinely different content
under a shared name, a digest-less cross-consumer overlap, or
unreadable proof state refuses instead of invalidating.
"""
from __future__ import annotations

import hashlib
import json
import os
import socket
from pathlib import Path
import sys
import threading

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

from prismabuild import pool, reader_lease, residency_map  # noqa: E402
from prismabuild import storage_tiers  # noqa: E402
import stage_move  # noqa: E402
import ram_promote  # noqa: E402

STAGE_TIER = "prismabuild-stage:testbox"
RAM_TIER = "ram:testbox"
CONSUMER_A = "a" * 64
CONSUMER_B = "b" * 64
CONSUMER_C = "c" * 64
MOVER_A = "1" * 64
MOVER_B = "2" * 64
MOVER_C = "3" * 64
RAM_MOVER = "e" * 64
SIZE = 16 * 1024


def _payload() -> bytes:
    return bytes([i % 251 + 1 for i in range(SIZE)])


def _queue(tmp_path: Path) -> pool.PoolQueue:
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    return queue


def _manifest(tmp_path: Path, *, name: str = "manifest.json",
              null_digest: bool = False) -> tuple[Path, str, dict]:
    origin = tmp_path / "origin"
    origin.mkdir(parents=True, exist_ok=True)
    payload = _payload()
    (origin / "calib.bin").write_bytes(payload)
    digest: str | None = None if null_digest else hashlib.sha256(
        payload).hexdigest()
    body = {
        "schema": "prismaquant.prismabuild.data_manifest.v1",
        "produced_by": {"tool": "ram-shared-source-fixture"},
        "mount_prefix": str(origin),
        "entries": [{"path": str(origin / "calib.bin"), "offset": 0,
                     "bytes": SIZE, "sha256": digest}],
        "entry_count": 1,
        "total_bytes": SIZE,
        "annotations": {},
    }
    path = tmp_path / name
    path.write_text(json.dumps(body))
    manifest_sha = hashlib.sha256(path.read_bytes()).hexdigest()
    return path, manifest_sha, body


def _run_stage(queue: pool.PoolQueue, tmp_path: Path, manifest: Path,
               manifest_sha: str, consumer: str, mover: str) -> dict:
    stage = tmp_path / "stage"
    args = stage_move.build_parser().parse_args([
        "--pool-root", str(queue.root),
        "--cas-root", str(tmp_path / "cas"),
        "--action-key", mover,
        "--consumer-action-key", consumer,
        "--tier-id", STAGE_TIER,
        "--stage-root", str(stage),
        "--manifest-sha256", manifest_sha,
        "--range-start-bytes", "0",
        "--range-end-bytes", str(SIZE),
        "--manifest", str(manifest),
        "--residency-root", str(queue.root / pool.RESIDENCY),
        "--block", "4096",
        "--readers", "2",
        "--max-readers", "2",
        "--unpaced",
    ])
    return stage_move.move(args)


def _stage(queue: pool.PoolQueue, tmp_path: Path, manifest: Path,
           manifest_sha: str, consumer: str, mover: str) -> dict:
    receipt = _run_stage(queue, tmp_path, manifest, manifest_sha,
                         consumer, mover)
    assert receipt["complete"] is True, receipt
    return receipt


def _staged_path(queue: pool.PoolQueue, consumer: str) -> Path:
    fragments = residency_map.read_fragments(
        queue.root / pool.RESIDENCY, consumer)
    composed = residency_map.compose(fragments)
    entries = composed["entries"]
    assert isinstance(entries, dict) and len(entries) == 1
    return Path(str(next(iter(entries.values()))["stage_path"]))


def _sidecar_identity(queue: pool.PoolQueue, consumer: str,
                      mover: str) -> dict:
    sidecar = reader_lease.read_material(
        queue.root / pool.RESIDENCY, consumer, mover)
    assert isinstance(sidecar, dict)
    entries = sidecar["entries"]
    assert isinstance(entries, dict) and len(entries) == 1
    return dict(next(iter(entries.values()))["file_id"])


def test_shared_stage_converges_without_replace(tmp_path: Path) -> None:
    queue = _queue(tmp_path)
    manifest, manifest_sha, body = _manifest(tmp_path)
    digest = str(body["entries"][0]["sha256"])

    _stage(queue, tmp_path, manifest, manifest_sha, CONSUMER_A, MOVER_A)
    first = _staged_path(queue, CONSUMER_A)
    before = os.stat(first)
    sidecar_a = reader_lease.read_material(
        queue.root / pool.RESIDENCY, CONSUMER_A, MOVER_A)
    assert isinstance(sidecar_a, dict)

    # A second independent consumer stages the identical bytes.
    _stage(queue, tmp_path, manifest, manifest_sha, CONSUMER_B, MOVER_B)
    after = os.stat(first)
    # The published incarnation survives: same inode and times, and A's
    # sidecar still names the live file.
    assert (after.st_ino, after.st_mtime_ns, after.st_ctime_ns) == (
        before.st_ino, before.st_mtime_ns, before.st_ctime_ns)
    live = reader_lease.stat_identity(str(first))
    assert live is not None
    published = sidecar_a["entries"][
        next(iter(sidecar_a["entries"]))]["file_id"]
    assert live == published


def test_ram_promotion_survives_shared_source(tmp_path: Path) -> None:
    queue = _queue(tmp_path)
    manifest, manifest_sha, body = _manifest(tmp_path)
    digest = str(body["entries"][0]["sha256"])
    source = str(body["entries"][0]["path"])

    _stage(queue, tmp_path, manifest, manifest_sha, CONSUMER_A, MOVER_A)
    _stage(queue, tmp_path, manifest, manifest_sha, CONSUMER_B, MOVER_B)

    key = residency_map.residency_map_key(source, 0)
    proof = reader_lease.acquire(
        queue, consumer_action_key=CONSUMER_A,
        attempt={"nonce": "f" * 32, "scope_id": "fixture-scope"},
        tier_id=STAGE_TIER, epoch="",
        span={"start_bytes": 0, "end_bytes": SIZE},
        holder={"host": socket.gethostname(), "pid": os.getpid()},
        acquire_token="fixture:cover",
        covers=[{"mover_action_key": MOVER_A,
                 "manifest_sha256": manifest_sha}],
        expected={key: {"bytes": SIZE, "sha256": digest}},
        file_pin=False)
    assert proof.get("ok"), proof

    ram = tmp_path / "ram"
    ram.mkdir(exist_ok=True)
    epoch = storage_tiers.ensure_ram_epoch(ram, host="testbox")
    assert epoch is not None
    args = ram_promote.build_parser().parse_args([
        "--pool-root", str(queue.root),
        "--cas-root", str(tmp_path / "cas"),
        "--action-key", RAM_MOVER,
        "--consumer-action-key", CONSUMER_A,
        "--tier-id", RAM_TIER,
        "--ram-root", str(ram),
        "--source-stage-root", str(tmp_path / "stage"),
        "--manifest-sha256", manifest_sha,
        "--range-start-bytes", "0",
        "--range-end-bytes", str(SIZE),
        "--manifest", str(manifest),
        "--residency-root", str(queue.root / pool.RESIDENCY),
        "--block", "4096",
        "--readers", "2",
        "--max-readers", "2",
    ])
    receipt = ram_promote.promote(args)
    assert receipt.get("complete") is True, receipt
    assert receipt.get("refusal") is None, receipt


def test_live_pin_survives_overlapping_copy(tmp_path: Path) -> None:
    queue = _queue(tmp_path)
    manifest, manifest_sha, body = _manifest(tmp_path)
    digest = str(body["entries"][0]["sha256"])
    source = str(body["entries"][0]["path"])

    _stage(queue, tmp_path, manifest, manifest_sha, CONSUMER_A, MOVER_A)
    key = residency_map.residency_map_key(source, 0)
    acquired = reader_lease.acquire(
        queue, consumer_action_key=CONSUMER_A,
        attempt={"nonce": "f" * 32, "scope_id": "fixture-scope"},
        tier_id=STAGE_TIER, epoch="",
        span={"start_bytes": 0, "end_bytes": SIZE},
        holder={"host": socket.gethostname(), "pid": os.getpid()},
        acquire_token="fixture:pin",
        covers=[{"mover_action_key": MOVER_A,
                 "manifest_sha256": manifest_sha}],
        expected={key: {"bytes": SIZE, "sha256": digest}})
    assert acquired.get("ok"), acquired

    # Overlapping copy lands while the read is live; the open must still
    # fence the exact published bytes.
    _stage(queue, tmp_path, manifest, manifest_sha, CONSUMER_B, MOVER_B)
    key = residency_map.residency_map_key(source, 0)
    fd, serving = reader_lease.open_pinned(
        queue, acquired["pin"], acquired["ref_id"], key)
    try:
        assert os.read(fd, SIZE) == _payload()
    finally:
        os.close(fd)
    assert serving["tier_id"] == STAGE_TIER
    assert reader_lease.release(
        queue, acquired["pin_id"], acquired["ref_id"],
        consumer_action_key=CONSUMER_A) is True


def test_simultaneous_first_publication_converges(tmp_path: Path) -> None:
    """Two movers observing initial absence serialize at publication.

    Pre-fix both copied concurrently and replaced each other; the loser
    invalidated the winner's sidecar. Post-fix the final gate adopts:
    both receipts complete and both sidecars name one incarnation.
    """

    queue = _queue(tmp_path)
    manifest, manifest_sha, body = _manifest(tmp_path)
    outcomes: dict[str, object] = {}
    barrier = threading.Barrier(2)

    def _run(which: str, consumer: str, mover: str) -> None:
        barrier.wait(timeout=60)
        try:
            outcomes[which] = _run_stage(
                queue, tmp_path, manifest, manifest_sha, consumer, mover)
        except BaseException as exc:  # noqa: BLE001
            outcomes[which] = exc

    threads = [threading.Thread(target=_run, args=("a", CONSUMER_A, MOVER_A)),
               threading.Thread(target=_run, args=("b", CONSUMER_B, MOVER_B))]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=120)
    for which, outcome in outcomes.items():
        assert not isinstance(outcome, BaseException), (which, outcome)
        assert outcome["complete"] is True, (which, outcome)
    staged_a = _staged_path(queue, CONSUMER_A)
    staged_b = _staged_path(queue, CONSUMER_B)
    assert str(staged_a) == str(staged_b)
    assert staged_a.read_bytes() == _payload()
    assert _sidecar_identity(queue, CONSUMER_A, MOVER_A) == (
        _sidecar_identity(queue, CONSUMER_B, MOVER_B))


def test_concurrent_overlapping_copies_stay_correct(tmp_path: Path) -> None:
    queue = _queue(tmp_path)
    manifest, manifest_sha, body = _manifest(tmp_path)
    outcomes: dict[str, object] = {}
    barrier = threading.Barrier(2)

    def _run(which: str, consumer: str, mover: str) -> None:
        barrier.wait(timeout=60)
        try:
            outcomes[which] = _stage(
                queue, tmp_path, manifest, manifest_sha, consumer, mover)
        except BaseException as exc:  # noqa: BLE001
            outcomes[which] = exc

    threads = [threading.Thread(target=_run, args=("a", CONSUMER_A, MOVER_A)),
               threading.Thread(target=_run, args=("b", CONSUMER_B, MOVER_B))]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=120)
    for which, outcome in outcomes.items():
        assert not isinstance(outcome, BaseException), (which, outcome)
        assert outcome["complete"] is True, (which, outcome)
    staged = _staged_path(queue, CONSUMER_A)
    assert staged.read_bytes() == _payload()
    assert hashlib.sha256(staged.read_bytes()).hexdigest() == str(
        body["entries"][0]["sha256"])


def test_crash_partial_temp_recovered(tmp_path: Path) -> None:
    queue = _queue(tmp_path)
    manifest, manifest_sha, body = _manifest(tmp_path)
    _stage(queue, tmp_path, manifest, manifest_sha, CONSUMER_A, MOVER_A)
    staged = _staged_path(queue, CONSUMER_A)
    # A crashed predecessor's owner-keyed temp with garbage must not
    # leak into the publish; adoption discards it, and a fresh copy
    # would truncate and verify.
    temp = staged.with_name(f".{staged.name}.{MOVER_B[:16]}.partial")
    temp.write_bytes(b"garbage-prefix")
    _stage(queue, tmp_path, manifest, manifest_sha, CONSUMER_B, MOVER_B)
    assert staged.read_bytes() == _payload()
    assert not temp.exists()


def test_different_bytes_under_shared_name_refuse(tmp_path: Path) -> None:
    queue = _queue(tmp_path)
    manifest, manifest_sha, body = _manifest(tmp_path)
    _stage(queue, tmp_path, manifest, manifest_sha, CONSUMER_A, MOVER_A)
    staged = _staged_path(queue, CONSUMER_A)
    # White-box foreign write (documented as such): same size, different
    # bytes under the shared staged name. The next copy refuses with an
    # entry error rather than silently invalidating whoever the bytes
    # belong to; the conflicting claim needs an owner
    # (egress/reconcile), not a blind overwrite.
    foreign = bytes([255 - (i % 251) for i in range(SIZE)])
    assert foreign != _payload()
    staged.write_bytes(foreign)
    before = os.stat(staged)
    receipt = _run_stage(queue, tmp_path, manifest, manifest_sha,
                         CONSUMER_B, MOVER_B)
    assert receipt["complete"] is False, receipt
    assert receipt["entries_staged"] == 0, receipt
    assert any("not replacing" in str(err) or "different bytes" in str(err)
               for err in receipt["errors"]), receipt
    after = os.stat(staged)
    assert (after.st_ino, after.st_mtime_ns) == (before.st_ino,
                                                 before.st_mtime_ns)
    assert staged.read_bytes() == foreign


def test_null_digest_overlap_converges(tmp_path: Path) -> None:
    """Digest-less dev manifests share one staged incarnation.

    Both-Spark sharing is the requirement, so a second consumer's
    null-digest overlap must converge -- never replace the live
    incarnation.  The origin fast path adopts without copying where the
    published file still carries this source's identity; otherwise the
    necessary private copy's digest is compared against the stored
    material proof, without rehashing the existing file.  A source that
    actually changed refuses instead of invalidating.
    """

    queue = _queue(tmp_path)
    manifest, manifest_sha, body = _manifest(
        tmp_path, name="manifest-null.json", null_digest=True)
    # Manifest entries carry an explicit null digest.
    raw = json.loads(manifest.read_text())
    assert raw["entries"][0]["sha256"] is None
    manifest_sha = hashlib.sha256(manifest.read_bytes()).hexdigest()

    _stage(queue, tmp_path, manifest, manifest_sha, CONSUMER_A, MOVER_A)
    staged = _staged_path(queue, CONSUMER_A)
    before = os.stat(staged)

    receipt = _run_stage(queue, tmp_path, manifest, manifest_sha,
                         CONSUMER_B, MOVER_B)
    assert receipt["complete"] is True, receipt
    after = os.stat(staged)
    assert (after.st_ino, after.st_mtime_ns, after.st_ctime_ns) == (
        before.st_ino, before.st_mtime_ns, before.st_ctime_ns)
    assert staged.read_bytes() == _payload()
    live = reader_lease.stat_identity(str(staged))
    assert live == _sidecar_identity(queue, CONSUMER_A, MOVER_A)
    assert live == _sidecar_identity(queue, CONSUMER_B, MOVER_B)


def test_delayed_fragment_adoption_never_replaces(tmp_path: Path) -> None:
    """A publisher slower than the old 3 s wait still converges.

    The first publication's bytes land but its fragment is delayed past
    the old bounded-recheck interval; the second mover must wait out the
    grace and adopt -- never convert the timeout into permission to
    replace the live incarnation.
    """

    import time

    queue = _queue(tmp_path)
    manifest, manifest_sha, body = _manifest(tmp_path)
    digest = str(body["entries"][0]["sha256"])
    source = str(body["entries"][0]["path"])

    _stage(queue, tmp_path, manifest, manifest_sha, CONSUMER_A, MOVER_A)
    staged = _staged_path(queue, CONSUMER_A)
    before = os.stat(staged)
    key = residency_map.residency_map_key(source, 0)

    # Hold back the proof: the bytes stay, the vouch goes away, and a
    # background thread re-files it after 4 s -- past the old interval.
    residency = queue.root / pool.RESIDENCY
    saved_entries = dict(
        residency_map.compose(
            residency_map.read_fragments(residency, CONSUMER_A))["entries"])
    assert len(saved_entries) == 1
    (residency_map.fragment_path(residency, CONSUMER_A, MOVER_A)
     .unlink(missing_ok=True))
    (reader_lease.material_path(residency, CONSUMER_A, MOVER_A)
     .unlink(missing_ok=True))

    def _refile() -> None:
        time.sleep(4.0)
        file_id = reader_lease.stat_identity(str(staged))
        assert file_id is not None
        residency_map.write_fragment(residency, {
            "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
            "consumer_action_key": CONSUMER_A,
            "mover_action_key": MOVER_A,
            "tier_id": STAGE_TIER,
            "stage_root": str(tmp_path / "stage"),
            "manifest_sha256": manifest_sha,
            "entries": saved_entries,
        })
        reader_lease.write_material(
            residency, consumer_action_key=CONSUMER_A,
            mover_action_key=MOVER_A, tier_id=STAGE_TIER,
            stage_root=str(tmp_path / "stage"),
            manifest_sha256=manifest_sha,
            generation=reader_lease.mint_generation(),
            entries={key: {
                "stage_path": str(staged), "bytes": SIZE,
                "sha256": digest, "file_id": file_id}})

    thread = threading.Thread(target=_refile, daemon=True)
    started = time.monotonic()
    thread.start()
    try:
        receipt = _run_stage(queue, tmp_path, manifest, manifest_sha,
                             CONSUMER_B, MOVER_B)
    finally:
        thread.join(timeout=60)
    elapsed = time.monotonic() - started
    assert receipt["complete"] is True, receipt
    # The wait outlasted the old interval; the incarnation survived it.
    assert elapsed >= 3.0, elapsed
    after = os.stat(staged)
    assert (after.st_ino, after.st_mtime_ns, after.st_ctime_ns) == (
        before.st_ino, before.st_mtime_ns, before.st_ctime_ns)
    assert _sidecar_identity(queue, CONSUMER_B, MOVER_B) == (
        _sidecar_identity(queue, CONSUMER_A, MOVER_A))


def test_inflight_copy_defers_without_replacing(tmp_path: Path,
                                                monkeypatch) -> None:
    """An in-flight publisher past a short grace still never loses bytes.

    A sibling copy temporary with no proof yet means a publisher may be
    alive; even after the (test-shortened) grace expires, the gate
    defers to the stall policy's retry instead of replacing the
    unattributed live incarnation.
    """

    monkeypatch.setattr(stage_move, "_PUBLISH_GRACE_S", 1.0)
    queue = _queue(tmp_path)
    manifest, manifest_sha, body = _manifest(tmp_path)

    _stage(queue, tmp_path, manifest, manifest_sha, CONSUMER_A, MOVER_A)
    staged = _staged_path(queue, CONSUMER_A)
    before = os.stat(staged)
    residency = queue.root / pool.RESIDENCY
    # Unpublish the bytes without touching them, then fake an in-flight
    # sibling copy that never finishes.
    (residency_map.fragment_path(residency, CONSUMER_A, MOVER_A)
     .unlink(missing_ok=True))
    (reader_lease.material_path(residency, CONSUMER_A, MOVER_A)
     .unlink(missing_ok=True))
    sibling = staged.with_name(f".{staged.name}.{'f' * 16}.partial")
    sibling.write_bytes(b"partial-prefix")

    receipt = _run_stage(queue, tmp_path, manifest, manifest_sha,
                         CONSUMER_B, MOVER_B)
    assert receipt["complete"] is False, receipt
    assert receipt["entries_staged"] == 0, receipt
    assert any("deferring" in str(err) for err in receipt["errors"]), receipt
    after = os.stat(staged)
    assert (after.st_ino, after.st_mtime_ns) == (before.st_ino,
                                                 before.st_mtime_ns)
    assert staged.read_bytes() == _payload()


def test_unreadable_proof_state_refuses(tmp_path: Path) -> None:
    """Tainted proof state fails closed without replacing.

    A corrupt fragment file makes ownership unknowable; the gate
    refuses with a named error and keeps the published bytes.
    """

    queue = _queue(tmp_path)
    manifest, manifest_sha, body = _manifest(tmp_path)
    _stage(queue, tmp_path, manifest, manifest_sha, CONSUMER_A, MOVER_A)
    staged = _staged_path(queue, CONSUMER_A)
    before = os.stat(staged)

    residency = queue.root / pool.RESIDENCY
    tainted = residency / CONSUMER_C / f"{MOVER_C}.json"
    tainted.parent.mkdir(parents=True, exist_ok=True)
    tainted.write_text("{not-json")

    receipt = _run_stage(queue, tmp_path, manifest, manifest_sha,
                         CONSUMER_B, MOVER_B)
    assert receipt["complete"] is False, receipt
    assert receipt["entries_staged"] == 0, receipt
    assert any("unreadable" in str(err) for err in receipt["errors"]), receipt
    after = os.stat(staged)
    assert (after.st_ino, after.st_mtime_ns) == (before.st_ino,
                                                 before.st_mtime_ns)
    assert staged.read_bytes() == _payload()
