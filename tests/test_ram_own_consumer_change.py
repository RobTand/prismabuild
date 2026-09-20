"""R3: same-consumer dev reuse must honor origin changes.

Narrow regression on the real mover path. A digest-less (dev/null)
manifest adopted this consumer's earlier published copy blindly --
``consumer == self.consumer`` returned the stored digest before
testing the origin signature or the copy's computed digest -- so a
changed origin silently re-served stale bytes as current. Stable
destination identity alone never proves the bytes still represent the
dev origin.

RED driver: publish null-digest as consumer A, mutate the origin to
different equal-length bytes, re-run the SAME consumer; pre-fix the
rerun completes by adopting the stale incarnation. Post-fix it
refuses as divergent while the old pins/material stay valid, and
unchanged same-consumer reuse plus cross-consumer convergence still
hold without rehashing the existing payload.
"""
from __future__ import annotations

import hashlib
import json
import os
import socket
from pathlib import Path
import sys

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
MOVER_A = "1" * 64
MOVER_A2 = "4" * 64
MOVER_B = "2" * 64
RAM_MOVER = "e" * 64
SIZE = 16 * 1024


def _payload() -> bytes:
    return bytes([i % 251 + 1 for i in range(SIZE)])


def _changed() -> bytes:
    other = bytes([255 - (i % 251) for i in range(SIZE)])
    assert other != _payload()
    return other


def _queue(tmp_path: Path) -> pool.PoolQueue:
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    return queue


def _manifest(tmp_path: Path) -> tuple[Path, str, dict]:
    origin = tmp_path / "origin"
    origin.mkdir(parents=True, exist_ok=True)
    (origin / "calib.bin").write_bytes(_payload())
    body = {
        "schema": "prismaquant.prismabuild.data_manifest.v1",
        "produced_by": {"tool": "ram-own-consumer-change-fixture"},
        "mount_prefix": str(origin),
        "entries": [{"path": str(origin / "calib.bin"), "offset": 0,
                     "bytes": SIZE, "sha256": None}],
        "entry_count": 1,
        "total_bytes": SIZE,
        "annotations": {},
    }
    path = tmp_path / "manifest-null.json"
    path.write_text(json.dumps(body))
    assert json.loads(path.read_text())["entries"][0]["sha256"] is None
    manifest_sha = hashlib.sha256(path.read_bytes()).hexdigest()
    return path, manifest_sha, body


def _run_stage(queue: pool.PoolQueue, tmp_path: Path, manifest: Path,
               manifest_sha: str, consumer: str, mover: str) -> dict:
    args = stage_move.build_parser().parse_args([
        "--pool-root", str(queue.root),
        "--cas-root", str(tmp_path / "cas"),
        "--action-key", mover,
        "--consumer-action-key", consumer,
        "--tier-id", STAGE_TIER,
        "--stage-root", str(tmp_path / "stage"),
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


def _staged_path(queue: pool.PoolQueue, consumer: str) -> Path:
    fragments = residency_map.read_fragments(
        queue.root / pool.RESIDENCY, consumer)
    composed = residency_map.compose(fragments)
    entries = composed["entries"]
    assert isinstance(entries, dict) and len(entries) == 1
    return Path(str(next(iter(entries.values()))["stage_path"]))


def test_same_consumer_changed_origin_refuses(tmp_path: Path) -> None:
    queue = _queue(tmp_path)
    manifest, manifest_sha, body = _manifest(tmp_path)
    source = str(body["entries"][0]["path"])
    digest = hashlib.sha256(_payload()).hexdigest()

    first = _run_stage(queue, tmp_path, manifest, manifest_sha,
                       CONSUMER_A, MOVER_A)
    assert first["complete"] is True, first
    staged = _staged_path(queue, CONSUMER_A)
    before = os.stat(staged)

    # Pin the old bytes while live, then change the origin underneath.
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
    Path(source).write_bytes(_changed())

    # Same-consumer retry must refuse: the published bytes no longer
    # represent this origin. No replace, no new fragment.
    rerun = _run_stage(queue, tmp_path, manifest, manifest_sha,
                       CONSUMER_A, MOVER_A2)
    assert rerun["complete"] is False, rerun
    assert rerun["entries_staged"] == 0, rerun
    assert any("different bytes" in str(err) or "divergent" in str(err)
               or "not replacing" in str(err)
               for err in rerun["errors"]), rerun
    after = os.stat(staged)
    assert (after.st_ino, after.st_mtime_ns, after.st_ctime_ns) == (
        before.st_ino, before.st_mtime_ns, before.st_ctime_ns)
    assert staged.read_bytes() == _payload()

    # The old pin still fences the old bytes: prior material stays valid.
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


def test_same_consumer_unchanged_origin_reuses(tmp_path: Path) -> None:
    queue = _queue(tmp_path)
    manifest, manifest_sha, body = _manifest(tmp_path)

    first = _run_stage(queue, tmp_path, manifest, manifest_sha,
                       CONSUMER_A, MOVER_A)
    assert first["complete"] is True, first
    staged = _staged_path(queue, CONSUMER_A)
    before = os.stat(staged)

    # Unchanged origin, same consumer: converges onto the live
    # incarnation without replacing it.
    rerun = _run_stage(queue, tmp_path, manifest, manifest_sha,
                       CONSUMER_A, MOVER_A2)
    assert rerun["complete"] is True, rerun
    after = os.stat(staged)
    assert (after.st_ino, after.st_mtime_ns, after.st_ctime_ns) == (
        before.st_ino, before.st_mtime_ns, before.st_ctime_ns)
    assert staged.read_bytes() == _payload()


def test_cross_consumer_changed_origin_refuses(tmp_path: Path) -> None:
    queue = _queue(tmp_path)
    manifest, manifest_sha, body = _manifest(tmp_path)
    source = str(body["entries"][0]["path"])

    first = _run_stage(queue, tmp_path, manifest, manifest_sha,
                       CONSUMER_A, MOVER_A)
    assert first["complete"] is True, first
    staged = _staged_path(queue, CONSUMER_A)
    before = os.stat(staged)
    Path(source).write_bytes(_changed())

    # Another consumer's changed origin must not overwrite A's bytes.
    rerun = _run_stage(queue, tmp_path, manifest, manifest_sha,
                       CONSUMER_B, MOVER_B)
    assert rerun["complete"] is False, rerun
    assert rerun["entries_staged"] == 0, rerun
    after = os.stat(staged)
    assert (after.st_ino, after.st_mtime_ns) == (before.st_ino,
                                                 before.st_mtime_ns)
    assert staged.read_bytes() == _payload()


def test_dev_shared_source_end_to_end_reader(tmp_path: Path) -> None:
    """Minimal DEV positive path: HDD origin → stage → RAM → actual read.

    Two consumers share one null-digest dev source; both stages
    converge on one incarnation, the promotion lands under the RAM
    epoch, and both consumers resolve the bytes through overlay_ram
    and read them back. Locks in the working path; not a RED test.
    """

    queue = _queue(tmp_path)
    manifest, manifest_sha, body = _manifest(tmp_path)
    source = str(body["entries"][0]["path"])

    first = _run_stage(queue, tmp_path, manifest, manifest_sha,
                       CONSUMER_A, MOVER_A)
    assert first["complete"] is True, first
    second = _run_stage(queue, tmp_path, manifest, manifest_sha,
                        CONSUMER_B, MOVER_B)
    assert second["complete"] is True, second
    staged_a = _staged_path(queue, CONSUMER_A)
    staged_b = _staged_path(queue, CONSUMER_B)
    assert str(staged_a) == str(staged_b)
    assert staged_a.read_bytes() == _payload()

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

    residency = queue.root / pool.RESIDENCY
    ram_frags = [f for f in residency_map.read_fragments(residency, CONSUMER_A)
                 if str(f.get("tier_id") or "") == RAM_TIER]
    assert ram_frags
    for consumer in (CONSUMER_A, CONSUMER_B):
        stage_frags = [f for f in residency_map.read_fragments(
            residency, consumer)
            if str(f.get("tier_id") or "") == STAGE_TIER]
        mapping = residency_map.compose(stage_frags)
        overlaid = residency_map.overlay_ram(
            mapping, ram_frags, ram_tier_id=RAM_TIER,
            ram_root=str(ram), ram_epoch=str(epoch["epoch"]))
        entry = residency_map.lookup(overlaid, source, 0)
        assert entry is not None
        assert entry.get("ram_path") is not None
        assert Path(str(entry["ram_path"])).read_bytes() == _payload()
