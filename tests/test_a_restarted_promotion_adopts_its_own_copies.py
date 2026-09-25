"""A restarted promotion adopts the correct copies its last attempt left (#1081).

``ram_promote`` files its fragment and material sidecar once, at the end of
its range.  A promotion that is killed before that point -- every PB publish
restarts the tier role, and the role's promotions with it -- leaves complete,
digest-verified files under their final names that no record names.  Its
retry runs under the same action key.  Before #1081 the publication gate read
each such name as positive absence: it copied the entry again, waited the
whole ``_PUBLISH_GRACE_S`` for a fragment that would never come, and then
replaced the name with a new inode.  Sixteen readers, 16 MiB entries and a
30 s grace make 16 x 16 MiB per 30 s, about 9 MB/s: the refill rate the
issue measured on the live RAM tier.

With the fix, a name under positive absence (no record, no pin, no live
claim, no copy in flight) is proven by its content: its bytes are hashed and
compared with the declared digest, or, for a digest-less manifest, with the
digest of the copy.  A match is adopted with no grace and no rename, and a
restarted promotion adopts before it copies anything.  Every refusal that
protects someone else stays: a copy in flight or a live claim waits and then
refuses, a live pin refuses, a record that dates other bytes refuses at once,
and unattributed wrong bytes wait out the grace and are healed by
replacement, as before.

The fixtures drive the real ``stage_move.move`` and ``ram_promote.promote``
over tiny real files.  The grace is shortened; the red assertions are about
identity, copies and polls, and the elapsed bound is the grace itself.
Runs under pbtest at priority -10; never executed locally.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import socket
import sys
import time

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

from prismabuild import pool, reader_lease, residency_map  # noqa: E402
from prismabuild import storage_tiers  # noqa: E402
import ram_promote  # noqa: E402
import stage_move  # noqa: E402

STAGE_TIER = "prismabuild-stage:testbox"
RAM_TIER = "ram:testbox"
CONSUMER = "a" * 64
FOREIGN_CONSUMER = "b" * 64
STAGE_MOVER = "1" * 64
FOREIGN_STAGE_MOVER = "2" * 64
RAM_MOVER = "e" * 64
FOREIGN_RAM_MOVER = "d" * 64
N = 3
SIZE = 16 * 1024
TOTAL = N * SIZE
#: Shortened for the test.  One reader copies in order, so a gate that waits
#: out the grace per entry costs at least ``N * GRACE`` seconds.
GRACE = 2.0


def _payload(index: int) -> bytes:
    return bytes((position * 7 + index * 13) % 251 + 1
                 for position in range(SIZE))


def _manifest(tmp_path: Path, *, null_digest: bool = False
              ) -> tuple[Path, str, dict]:
    origin = tmp_path / "origin"
    origin.mkdir(parents=True, exist_ok=True)
    entries = []
    for index in range(N):
        path = origin / f"shard-{index}.bin"
        path.write_bytes(_payload(index))
        entries.append({
            "path": str(path), "offset": 0, "bytes": SIZE,
            "sha256": (None if null_digest
                       else hashlib.sha256(_payload(index)).hexdigest())})
    body = {
        "schema": "prismaquant.prismabuild.data_manifest.v1",
        "produced_by": {"tool": "restarted-promotion-fixture"},
        "mount_prefix": str(origin),
        "entries": entries,
        "entry_count": N,
        "total_bytes": TOTAL,
        "annotations": {},
    }
    path = tmp_path / ("manifest-null.json" if null_digest
                       else "manifest.json")
    path.write_text(json.dumps(body))
    return path, hashlib.sha256(path.read_bytes()).hexdigest(), body


def _stage_args(tmp_path: Path, queue: pool.PoolQueue, manifest: Path,
                manifest_sha: str, consumer: str, mover: str):
    return stage_move.build_parser().parse_args([
        "--pool-root", str(queue.root),
        "--cas-root", str(tmp_path / "cas"),
        "--action-key", mover,
        "--consumer-action-key", consumer,
        "--tier-id", STAGE_TIER,
        "--stage-root", str(tmp_path / "stage"),
        "--manifest-sha256", manifest_sha,
        "--range-start-bytes", "0",
        "--range-end-bytes", str(TOTAL),
        "--manifest", str(manifest),
        "--residency-root", str(queue.root / pool.RESIDENCY),
        "--block", "4096",
        "--readers", "1",
        "--max-readers", "1",
        "--unpaced",
    ])


def _promote_args(tmp_path: Path, queue: pool.PoolQueue, manifest: Path,
                  manifest_sha: str):
    return ram_promote.build_parser().parse_args([
        "--pool-root", str(queue.root),
        "--cas-root", str(tmp_path / "cas"),
        "--action-key", RAM_MOVER,
        "--consumer-action-key", CONSUMER,
        "--tier-id", RAM_TIER,
        "--ram-root", str(tmp_path / "ram"),
        "--source-stage-root", str(tmp_path / "stage"),
        "--manifest-sha256", manifest_sha,
        "--range-start-bytes", "0",
        "--range-end-bytes", str(TOTAL),
        "--manifest", str(manifest),
        "--residency-root", str(queue.root / pool.RESIDENCY),
        "--block", "4096",
        "--readers", "1",
        "--max-readers", "1",
    ])


def _ram_paths(tmp_path: Path, body: dict) -> list[Path]:
    return [tmp_path / "ram" / stage_move.stage_relative(
                str(entry["path"]), 0, SIZE,
                mount_prefix=str(body["mount_prefix"]))
            for entry in body["entries"]]


def _identities(paths: list[Path]) -> list[dict[str, int] | None]:
    return [reader_lease.stat_identity(str(path)) for path in paths]


def _promoted(tmp_path: Path):
    """Stage the range, promote it once, and return the fixture."""

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    manifest, manifest_sha, body = _manifest(tmp_path)
    staged = stage_move.move(_stage_args(
        tmp_path, queue, manifest, manifest_sha, CONSUMER, STAGE_MOVER))
    assert staged["complete"] is True, staged
    ram = tmp_path / "ram"
    ram.mkdir()
    epoch = storage_tiers.ensure_ram_epoch(ram, host="testbox")
    assert epoch is not None
    args = _promote_args(tmp_path, queue, manifest, manifest_sha)
    first = ram_promote.promote(args)
    assert first["complete"] is True, first
    paths = _ram_paths(tmp_path, body)
    for index, path in enumerate(paths):
        assert path.read_bytes() == _payload(index)
    return queue, args, body, paths, str(epoch["epoch"]), manifest_sha


def _forget(queue: pool.PoolQueue, consumer: str = CONSUMER,
            mover: str = RAM_MOVER) -> None:
    """What a kill before the end-of-range records leaves: no records."""

    residence = queue.root / pool.RESIDENCY
    residency_map.fragment_path(residence, consumer, mover).unlink()
    reader_lease.material_path(residence, consumer, mover).unlink()


def _timings(receipt: dict) -> dict:
    timings = receipt.get("phase_timings")
    assert isinstance(timings, dict), (
        f"the receipt records no phase timings: {sorted(receipt)}")
    return timings


def _refused(receipt: dict, needle: str) -> None:
    assert receipt["complete"] is False, receipt
    assert any(needle in str(error) for error in receipt["errors"]), receipt


# --- red on main: a restart pays a copy, a grace and a rename per entry ------

def test_a_restarted_promotion_adopts_without_a_copy_or_a_grace(
        tmp_path: Path, monkeypatch) -> None:
    queue, args, _body, paths, _epoch, _sha = _promoted(tmp_path)
    before = _identities(paths)
    _forget(queue)
    monkeypatch.setattr(stage_move, "_PUBLISH_GRACE_S", GRACE)

    started = time.monotonic()
    receipt = ram_promote.promote(args)
    elapsed = time.monotonic() - started

    assert receipt["complete"] is True, receipt
    assert _identities(paths) == before, (
        f"the restart replaced its own correct copies: {N} entries, one "
        f"reader, {elapsed:.1f} s against a {GRACE} s grace")
    assert elapsed < GRACE, (
        f"the restart waited out the grace: {elapsed:.1f} s for {N} "
        f"entries already in place")
    timings = _timings(receipt)
    assert timings["outcomes"] == {"adopted_by_content": N}, timings
    phases = timings["thread_seconds"]
    assert "copy_read" not in phases, f"an entry was copied again: {phases}"
    assert "publish_poll_sleep" not in phases, phases
    assert phases["content_proof"]["calls"] == N, phases
    # The restart files its records again, dating the adopted inodes.
    material = reader_lease.read_material(
        queue.root / pool.RESIDENCY, CONSUMER, RAM_MOVER)
    assert isinstance(material, dict), material
    dated = {os.path.normpath(str(entry["stage_path"])): dict(entry["file_id"])
             for entry in material["entries"].values()}
    assert dated == {os.path.normpath(str(path)): identity
                     for path, identity in zip(paths, before)}, material


def test_copies_another_key_left_are_adopted_too(
        tmp_path: Path, monkeypatch) -> None:
    """Content, not the key, proves the name: a new key adopts as well."""

    queue, args, _body, paths, _epoch, _sha = _promoted(tmp_path)
    before = _identities(paths)
    _forget(queue)
    monkeypatch.setattr(stage_move, "_PUBLISH_GRACE_S", GRACE)
    args.action_key = FOREIGN_RAM_MOVER

    receipt = ram_promote.promote(args)

    assert receipt["complete"] is True, receipt
    assert _identities(paths) == before, receipt
    assert _timings(receipt)["outcomes"] == {"adopted_by_content": N}


def test_a_digest_less_stage_copy_adopts_at_publication(
        tmp_path: Path, monkeypatch) -> None:
    """No declared digest: the copy's own digest proves the name.

    A stage mover over a digest-less manifest cannot prove the name before
    it copies, so it copies and then compares its digest with the bytes that
    are there.  Before #1081 that publication waited out the grace and
    replaced the name.
    """

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    manifest, manifest_sha, body = _manifest(tmp_path, null_digest=True)
    first = stage_move.move(_stage_args(
        tmp_path, queue, manifest, manifest_sha, CONSUMER, STAGE_MOVER))
    assert first["complete"] is True, first
    paths = [tmp_path / "stage" / stage_move.stage_relative(
                 str(entry["path"]), 0, SIZE,
                 mount_prefix=str(body["mount_prefix"]))
             for entry in body["entries"]]
    before = _identities(paths)
    _forget(queue, CONSUMER, STAGE_MOVER)
    monkeypatch.setattr(stage_move, "_PUBLISH_GRACE_S", GRACE)

    receipt = stage_move.move(_stage_args(
        tmp_path, queue, manifest, manifest_sha, FOREIGN_CONSUMER,
        FOREIGN_STAGE_MOVER))

    assert receipt["complete"] is True, receipt
    assert _identities(paths) == before, (
        "a digest-less copy replaced identical bytes")
    timings = receipt["phase_timings"]
    assert timings["outcomes"] == {
        "adopted_by_content_at_publication": N}, timings
    assert "publish_poll_sleep" not in timings["thread_seconds"], timings


# --- every refusal that protects someone else stays ------------------------

def test_a_copy_in_flight_still_defers_and_never_replaces(
        tmp_path: Path, monkeypatch) -> None:
    queue, args, _body, paths, _epoch, _sha = _promoted(tmp_path)
    before = _identities(paths)
    _forget(queue)
    sibling = paths[0].with_name(f".{paths[0].name}.{'f' * 16}.partial")
    sibling.write_bytes(b"partial-prefix")
    monkeypatch.setattr(stage_move, "_PUBLISH_GRACE_S", GRACE)

    started = time.monotonic()
    receipt = ram_promote.promote(args)
    elapsed = time.monotonic() - started

    _refused(receipt, "copy in flight")
    assert elapsed >= GRACE, "a copy in flight must be waited for"
    assert _identities(paths)[0] == before[0]
    assert paths[0].read_bytes() == _payload(0)


def test_a_live_claim_still_defers_and_never_replaces(
        tmp_path: Path, monkeypatch) -> None:
    """The claim census's answer, not the census itself, is under test.

    ``_live_claim_cover`` is covered against real sealed claims elsewhere;
    here it reports another live publisher for the first name, so the test
    shows that the content proof never outranks it.
    """

    queue, args, _body, paths, _epoch, _sha = _promoted(tmp_path)
    before = _identities(paths)
    _forget(queue)
    covered = os.path.normpath(str(paths[0]))
    real = stage_move._StagedPublisher._live_claim_cover

    def cover(self, norm):
        if norm == covered:
            return True, "live mover claim"
        return real(self, norm)

    monkeypatch.setattr(stage_move._StagedPublisher, "_live_claim_cover",
                        cover)
    monkeypatch.setattr(stage_move, "_PUBLISH_GRACE_S", GRACE)

    started = time.monotonic()
    receipt = ram_promote.promote(args)
    elapsed = time.monotonic() - started

    _refused(receipt, "live publisher")
    assert elapsed >= GRACE, "a live publisher must be waited for"
    assert _identities(paths)[0] == before[0]


def test_a_live_pin_still_refuses(tmp_path: Path, monkeypatch) -> None:
    queue, args, body, paths, epoch, manifest_sha = _promoted(tmp_path)
    queue.announce_tier({
        "schema": storage_tiers.TIER_RECORD_SCHEMA_V1,
        "tier_id": RAM_TIER, "host": "testbox", "tier": "ram",
        "mountpoint": str(tmp_path / "ram"), "epoch": epoch,
    })
    pinned = reader_lease.acquire(
        queue, consumer_action_key=CONSUMER,
        attempt={"nonce": "f" * 32, "scope_id": "fixture-scope"},
        tier_id=RAM_TIER, epoch=epoch,
        span={"start_bytes": 0, "end_bytes": TOTAL},
        holder={"host": socket.gethostname(), "pid": os.getpid()},
        acquire_token="fixture:ram-pin",
        covers=[{"mover_action_key": RAM_MOVER,
                 "manifest_sha256": manifest_sha}],
        expected={residency_map.residency_map_key(str(entry["path"]), 0): {
            "bytes": SIZE, "sha256": entry["sha256"]}
            for entry in body["entries"]})
    assert pinned.get("ok"), pinned
    before = _identities(paths)
    _forget(queue)
    monkeypatch.setattr(stage_move, "_PUBLISH_GRACE_S", GRACE)

    receipt = ram_promote.promote(args)

    _refused(receipt, "live-pinned")
    assert _identities(paths) == before


def test_a_record_dating_other_bytes_still_refuses_at_once(
        tmp_path: Path, monkeypatch) -> None:
    queue, args, _body, paths, epoch, manifest_sha = _promoted(tmp_path)
    residence = queue.root / pool.RESIDENCY
    fragment = json.loads(residency_map.fragment_path(
        residence, CONSUMER, RAM_MOVER).read_bytes())
    material = reader_lease.read_material(residence, CONSUMER, RAM_MOVER)
    assert isinstance(material, dict), material
    before = _identities(paths)
    _forget(queue)
    # Another promotion's records date the very inode that is there, with
    # a digest that is not the manifest's.
    norm = os.path.normpath(str(paths[0]))
    entries = {key: dict(entry) for key, entry in
               material["entries"].items()}
    for entry in entries.values():
        if os.path.normpath(str(entry["stage_path"])) == norm:
            entry["sha256"] = "0" * 64
    residency_map.write_fragment(
        residence, dict(fragment, mover_action_key=FOREIGN_RAM_MOVER))
    reader_lease.write_material(
        residence, consumer_action_key=CONSUMER,
        mover_action_key=FOREIGN_RAM_MOVER, tier_id=RAM_TIER,
        stage_root=str(tmp_path / "ram"), manifest_sha256=manifest_sha,
        generation=reader_lease.mint_generation(), entries=entries,
        epoch=epoch)
    # ... and that promotion is still queued, so it protects someone.  Since
    # #1004 item 1 a promotion arbitrates a divergent name by its owners'
    # states: an owner provably ended would be replaced (covered in
    # ``test_a_divergent_ram_name_is_arbitrated_by_its_owners``); a queued
    # mover's ending is unproven, which keeps the immediate refusal.
    queue.publish(action_key=FOREIGN_RAM_MOVER, cas_root="/cas",
                  checkout_root="/co", worker_script="/w.py",
                  resources={"cpu": 1}, max_attempts=1)
    monkeypatch.setattr(stage_move, "_PUBLISH_GRACE_S", GRACE)

    started = time.monotonic()
    receipt = ram_promote.promote(args)
    elapsed = time.monotonic() - started

    _refused(receipt, "different bytes")
    assert elapsed < GRACE, "a dated divergence refuses without waiting"
    assert _identities(paths)[0] == before[0]


def test_unattributed_wrong_bytes_wait_and_heal_never_adopt(
        tmp_path: Path, monkeypatch) -> None:
    queue, args, _body, paths, _epoch, _sha = _promoted(tmp_path)
    _forget(queue)
    wrong = paths[0].with_name(paths[0].name + ".wrong")
    wrong.write_bytes(bytes(SIZE))
    os.replace(wrong, paths[0])
    planted = reader_lease.stat_identity(str(paths[0]))
    monkeypatch.setattr(stage_move, "_PUBLISH_GRACE_S", GRACE)

    started = time.monotonic()
    receipt = ram_promote.promote(args)
    elapsed = time.monotonic() - started

    assert receipt["complete"] is True, receipt
    assert paths[0].read_bytes() == _payload(0), "wrong bytes were adopted"
    assert reader_lease.stat_identity(str(paths[0])) != planted
    assert elapsed >= GRACE, "unattributed bytes still wait for a record"
