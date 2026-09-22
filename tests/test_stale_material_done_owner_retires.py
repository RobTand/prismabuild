"""A stale-material DONE owner blocks a successor until housekeeping retires it (#853).

Production shape (full512 Stage A R6, 2026-09-22): a FAILED consumer's DONE
executed mover still held its tokens and carried a material sidecar, while a
later publication (the prewarm loop, another consumer's copy) had replaced a
destination under a fresh inode.  The sidecar can no longer prove the bytes
that are there, so the shared publisher refuses to adopt it and refuses to
overwrite the live/unknown incarnation -- while the unconditional dead-owner
pass excludes it (material present, charge held) and the held-key orphan pass
breaks before eviction whenever no tier needs room.  The block is permanent
until something routes that exact owner through ``evict``.

This file is the base-source reproduction, not a fix.  It asserts the desired
end state through the real machinery -- ordinary `sweep`, a real
`stage_move.move`, and a real `reader_lease.acquire`/`open_pinned` exact-byte
read -- so it fails at the unmodified source exactly where the incident
failed, and it includes the mixed stale+coherent fragment the live report
cannot rule out.  Every fixture is a temp stage root registered to a fake
queue (never real /stage or /ram); no payload beyond the small fixture bytes
is hashed.
"""
from __future__ import annotations

import hashlib
import os
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

import test_dead_owner_fragment_blocks_then_retires as base  # noqa: E402
from test_dead_owner_fragment_blocks_then_retires import fleet  # noqa: E402
import prismabuild.core as pb  # noqa: E402
from prismabuild import pool, reader_lease, residency_map  # noqa: E402
import stage_move  # noqa: E402
import stage_release  # noqa: E402

SIZE = base.SIZE
NAMES = base.NAMES
TIER = base.TIER
OLD_PAYLOAD = b"o" * SIZE
OLD_DIGEST = hashlib.sha256(OLD_PAYLOAD).hexdigest()
NEW_PAYLOAD = b"n" * SIZE
#: One publication grace for the whole reproduction, so a blocked successor
#: fails visibly in seconds instead of minutes; the multiplication is what the
#: bound test measures, and it is spelled in that test.
GRACE = 0.5


def staged_name(name: str) -> str:
    """Where the stage mover stages the whole ``SIZE``-byte source ``name``."""

    return stage_move.stage_relative(f"/m/{name}", 0, SIZE, mount_prefix="/m")


def _identity(path: Path) -> dict[str, int]:
    info = os.stat(path)
    return {"ino": int(info.st_ino), "size": int(info.st_size),
            "mtime_ns": int(info.st_mtime_ns),
            "ctime_ns": int(info.st_ctime_ns)}


def _entries(stage: Path) -> dict[str, dict[str, object]]:
    """The sidecar the owner would have written: every entry's real identity.

    Taken before any replacement, so each mention dates the actual first
    incarnation of the bytes the mover staged.  A later ``os.replace`` of a
    selected destination is what makes that one a real stale witness; the
    untouched destinations stay coherent.  The mention digest is the staged
    payload's own digest, the same one the fragment carries.
    """

    out: dict[str, dict[str, object]] = {}
    for name in NAMES:
        path = stage / staged_name(name)
        key = residency_map.residency_map_key(str(path), 0)
        out[key] = {
            "stage_path": str(path), "bytes": SIZE, "sha256": OLD_DIGEST,
            "file_id": _identity(path),
        }
    return out


def _fragment(queue, stage, consumer, mover) -> None:
    """One real fragment through the supported writer, matching the sidecar."""

    residency_map.write_fragment(queue.root / pool.RESIDENCY, {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": consumer, "mover_action_key": mover,
        "tier_id": TIER, "stage_root": str(stage),
        "manifest_sha256": "a" * 64,
        "entries": {
            residency_map.residency_map_key(str(stage / staged_name(name)), 0): {
                "stage_path": str(stage / staged_name(name)), "bytes": SIZE,
                "sha256": OLD_DIGEST, "offset": 0,
            } for name in NAMES
        },
    })


def _write_sidecar(queue, stage, consumer, mover, entries, *,
                   tier_id: str = TIER, stage_root: str | None = None,
                   manifest_sha256: str = "a" * 64,
                   generation: str = "a" * 32) -> None:
    reader_lease.write_material(
        queue.root / pool.RESIDENCY, consumer_action_key=consumer,
        mover_action_key=mover, tier_id=tier_id,
        stage_root=str(stage) if stage_root is None else stage_root,
        manifest_sha256=manifest_sha256, generation=generation,
        entries=entries)


def _charge(queue, mover: str, tokens: int = 1) -> None:
    queue.mint_tier_capacity(TIER, {"stage_gib": tokens})
    assert queue.tier_ledger(TIER).acquire(mover, {"stage_gib": tokens}) is True


def _stage(stage: Path, name: str, payload: bytes) -> Path:
    """A staged destination carrying the prewarm loop's own source mark.

    The live destinations carried ``user.pbstage.source`` -- every stage
    publication sets it -- and that mark is why routine reconciliation keeps
    them as prewarm-owned instead of deleting them.  Reproduce that: an
    unmarked file would let reconcile remove the bytes and hide the block.
    """

    # ``<name>.later`` is a replacement written beside its target's staged
    # name, to be renamed over it; everything else is the staged name.
    later = ".later"
    path = (stage / (staged_name(name[:-len(later)]) + later)
            if name.endswith(later) else stage / staged_name(name))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    os.setxattr(path, base.prewarm_loop.STAGE_SOURCE_XATTR,
                f"/originals/{name}@0".encode())
    return path


def stale_owner(fleet, *, coherent_names=(), charged: bool = True,
                material: bool = True, move_receipt: bool = True,
                replace: bool = True, mover_status: str | None = "executed",
                consumer_status: str = "failed") -> tuple[str, str]:
    """FAILED consumer + DONE executed mover with a dated material sidecar.

    The first incarnation is written and dated; a later publication really
    replaces every non-coherent destination with a fresh inode, then the
    fragment is filed, a complete move receipt recorded, the mover finished
    executed and (by default) its tokens acquired.  The incident's charge is
    present: the owner holds the tier tokens for the bytes its fragment names.
    """

    queue, stage, _ = fleet
    if consumer_status == "failed":
        consumer, _generation = base._fail_consumer(queue)
    else:
        consumer = base._key()
        base._publish(queue, consumer, max_attempts=1)
        queue.finish(consumer, status=consumer_status,
                     detail={"returncode": 0})
    mover = base._key()
    base._publish(queue, mover, max_attempts=1)
    for name in NAMES:
        _stage(stage, name, OLD_PAYLOAD)
    if material:
        _write_sidecar(queue, stage, consumer, mover, _entries(stage))
    if replace:
        for name in NAMES:
            if name in coherent_names:
                continue
            replacement = _stage(stage, f"{name}.later", NEW_PAYLOAD)
            os.replace(replacement, stage / staged_name(name))
    _fragment(queue, stage, consumer, mover)
    if move_receipt:
        queue.record_move(mover, {
            "consumer_action_key": consumer, "tier_id": TIER,
            "stage_root": str(stage), "manifest_sha256": "a" * 64,
            "complete": True, "entries_declared": len(NAMES),
            "entries_staged": len(NAMES), "bytes_staged": len(NAMES) * SIZE,
            "range_bytes": len(NAMES) * SIZE, "range_start_bytes": 0,
            "range_end_bytes": len(NAMES) * SIZE, "errors": []})
    if mover_status is not None:
        queue.finish(mover, status=mover_status,
                     detail={"returncode": 1 if mover_status == "failed" else 0})
    if charged:
        _charge(queue, mover)
    return consumer, mover


def _retired(receipts, mover: str) -> bool:
    return any(entry.get("action_key") == mover
               and entry.get("complete") is True for entry in receipts)


def _sweep(queue, stage):
    return stage_release.sweep(
        queue, stage_roots={TIER: str(stage)}, pressure={})


def _kept(queue, stage, consumer, mover) -> None:
    """The whole ownership survives: files, fragment, material and charge."""

    assert all((stage / staged_name(name)).exists() for name in NAMES)
    assert residency_map.fragment_path(
        queue.residency_fragment_root(), consumer, mover).exists()
    assert reader_lease.material_path(
        queue.residency_fragment_root(), consumer, mover).exists()
    assert mover in queue.tier_ledger(TIER).held_keys()


def _successor_manifest(tmp_path: Path, payloads: dict[str, bytes]):
    """One real manifest whose per-name payloads are the caller's choice."""

    mount = tmp_path / "sources"
    mount.mkdir(exist_ok=True)
    entries = []
    digests = {}
    for name in NAMES:
        payload = payloads[name]
        source = mount / name
        source.write_bytes(payload)
        digest = hashlib.sha256(payload).hexdigest()
        digests[name] = digest
        entries.append({"path": str(source), "offset": 0, "bytes": SIZE,
                        "sha256": digest})
    manifest = {"schema": pb.DATA_MANIFEST_SCHEMA_V1, "produced_by": {},
                "annotations": {}, "mount_prefix": str(mount),
                "entries": entries, "entry_count": len(entries),
                "total_bytes": len(entries) * SIZE}
    raw = pb._canonical_file_bytes(pb.validate_data_manifest(manifest))
    manifest_digest = hashlib.sha256(raw).hexdigest()
    path = tmp_path / "manifest.json"
    path.write_bytes(raw)
    return path, manifest_digest, entries, digests


def _successor_args(queue, stage, cas, copier, successor, path, digest,
                    entries):
    return stage_move.build_parser().parse_args([
        "--pool-root", str(queue.root), "--cas-root", str(cas),
        "--action-key", copier, "--consumer-action-key", successor,
        "--tier-id", TIER, "--stage-root", str(stage),
        "--manifest", str(path), "--manifest-sha256", digest,
        "--range-start-bytes", "0",
        "--range-end-bytes", str(len(entries) * SIZE),
        "--residency-root", str(queue.residency_fragment_root()),
        "--readers", "1", "--max-readers", "1", "--unpaced"])


def _successor_reads(queue, successor, copier, entries, digests, payloads,
                     manifest_digest):
    """The real leased staged read a recovered successor must be able to do."""

    key = residency_map.residency_map_key(entries[0]["path"], 0)
    pin = reader_lease.acquire(
        queue, consumer_action_key=successor,
        attempt={"nonce": "n1", "scope_id": "s1"}, tier_id=TIER, epoch="",
        span={"start_bytes": 0, "end_bytes": SIZE},
        holder={"host": "fixture", "pid": os.getpid()},
        acquire_token="successor-proof",
        covers=[{"mover_action_key": copier,
                 "manifest_sha256": manifest_digest}],
        expected={key: {"bytes": SIZE, "sha256": digests[NAMES[0]]}})
    assert pin["ok"], pin
    fd, _ = reader_lease.open_pinned(queue, pin["pin"], pin["ref_id"], key)
    try:
        assert os.read(fd, SIZE) == payloads[NAMES[0]]
    finally:
        os.close(fd)
        reader_lease.release(queue, pin["pin_id"], pin["ref_id"],
                             consumer_action_key=successor)


def test_housekeeping_retires_the_stale_owner_and_the_successor_reads(
        fleet, tmp_path, monkeypatch):
    """The incident shape: ordinary sweep must retire, then the copy lands."""

    queue, stage, cas = fleet
    consumer, mover = stale_owner(fleet)
    successor, copier = base._key(), base._key()
    publisher = base._publisher(fleet, copier, successor)
    assert publisher._decide(stage / staged_name(NAMES[0]), SIZE, "c" * 64,
                             computed=None, source_id=None,
                             heal=True)[0] == "refuse", (
        "the stale owner must block the shared publisher")
    assert mover in queue.tier_ledger(TIER).held_keys()

    receipts = _sweep(queue, stage)
    assert _retired(receipts, mover), (
        f"stale-material DONE owner {mover[:12]} with {consumer[:12]} was "
        f"not retired by ordinary housekeeping: {receipts}")
    assert not residency_map.fragment_path(
        queue.residency_fragment_root(), consumer, mover).exists()
    assert not reader_lease.material_path(
        queue.residency_fragment_root(), consumer, mover).exists()
    assert queue.tier_ledger(TIER).holder_tokens(mover) == {}

    monkeypatch.setattr(stage_move, "_PUBLISH_GRACE_S", GRACE)
    monkeypatch.setattr(stage_move, "_PUBLISH_POLL_S", 0.02)
    payloads = {name: b"x" * SIZE for name in NAMES}
    path, manifest_digest, entries, digests = _successor_manifest(
        tmp_path, payloads)
    base._publish(queue, successor, max_attempts=1)
    args = _successor_args(queue, stage, cas, copier, successor, path,
                           manifest_digest, entries)
    result = stage_move.move(args)
    assert result["complete"] and result["entries_staged"] == len(entries), (
        result["errors"])
    queue.record_move(copier, result)
    _successor_reads(queue, successor, copier, entries, digests, payloads,
                     manifest_digest)


def test_a_mixed_fragment_lets_the_successor_publish_the_stale_path(
        fleet, tmp_path, monkeypatch):
    """One coherent mention plus one stale path: the stale path must unblock.

    The live report established one stale witness, not that every material
    entry was obsolete, so this shape is the reproduction's second half: the
    coherent destination must keep serving the successor (adopted or
    identically republished, never left blocked), while the stale destination
    must become publishable through ordinary housekeeping.
    """

    queue, stage, cas = fleet
    coherent = {NAMES[0]}
    consumer, mover = stale_owner(fleet, coherent_names=coherent)
    successor, copier = base._key(), base._key()
    publisher = base._publisher(fleet, copier, successor)
    blocked = publisher._decide(stage / staged_name(NAMES[1]), SIZE, OLD_DIGEST,
                                computed=None, source_id=None, heal=False)
    assert blocked[0] in ("wait", "refuse"), (
        f"the stale path must not adopt or replace, saw {blocked}")
    assert publisher._decide(stage / staged_name(NAMES[0]), SIZE, OLD_DIGEST,
                             computed=None, source_id=None,
                             heal=True)[0] == "adopt", (
        "the coherent entry must still prove its path")
    receipts = _sweep(queue, stage)
    # The mechanism is the implementation's choice -- whole-owner retirement,
    # a per-path recovery, ... -- so none is prescribed here.  What the
    # reproduction requires is that ordinary housekeeping leaves the coherent
    # destination's bytes intact and does not leave the stale path blocked
    # forever; the successor move below is that second half.
    assert (stage / staged_name(NAMES[0])).read_bytes() == OLD_PAYLOAD, (
        f"the coherent destination's bytes did not survive ordinary "
        f"housekeeping: {receipts}")

    monkeypatch.setattr(stage_move, "_PUBLISH_GRACE_S", GRACE)
    monkeypatch.setattr(stage_move, "_PUBLISH_POLL_S", 0.02)
    payloads = {NAMES[0]: OLD_PAYLOAD, NAMES[1]: b"x" * SIZE}
    path, manifest_digest, entries, digests = _successor_manifest(
        tmp_path, payloads)
    base._publish(queue, successor, max_attempts=1)
    args = _successor_args(queue, stage, cas, copier, successor, path,
                           manifest_digest, entries)
    result = stage_move.move(args)
    assert result["complete"] and result["entries_staged"] == len(entries), (
        result["errors"])
    assert (stage / staged_name(NAMES[0])).read_bytes() == OLD_PAYLOAD, (
        "the coherent destination must serve the successor, adopted or "
        "republished -- never be left blocked")
    queue.record_move(copier, result)
    _successor_reads(queue, successor, copier, entries, digests, payloads,
                     manifest_digest)


def test_a_coherent_current_incarnation_material_is_retained(fleet):
    """A sidecar that still dates the live file is a usable proof: keep it."""

    queue, stage, _ = fleet
    consumer, mover = stale_owner(fleet, coherent_names=set(NAMES),
                                  replace=False)
    receipts = _sweep(queue, stage)
    assert not _retired(receipts, mover), receipts
    _kept(queue, stage, consumer, mover)
    publisher = base._publisher(fleet, base._key(), base._key())
    assert publisher._decide(stage / staged_name(NAMES[0]), SIZE, OLD_DIGEST,
                             computed=None, source_id=None,
                             heal=True)[0] == "adopt", (
        "a coherent current-incarnation mention must still prove its path")


@pytest.mark.parametrize("damage", ["corrupt", "wrong-tier", "wrong-root",
                                    "wrong-manifest", "no-mention"])
def test_unknown_or_foreign_material_retains(fleet, damage):
    """Unreadable, invalid or differently-bound evidence must never authorize."""

    queue, stage, _ = fleet
    consumer, mover = stale_owner(fleet, material=False, charged=True)
    if damage == "corrupt":
        path = reader_lease.material_path(
            queue.residency_fragment_root(), consumer, mover)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("{not json")
    elif damage == "no-mention":
        _write_sidecar(queue, stage, consumer, mover, {
            residency_map.residency_map_key("/elsewhere/other.bin", 0): {
                "stage_path": "/elsewhere/other.bin", "bytes": SIZE,
                "sha256": OLD_DIGEST,
                "file_id": {"ino": 7, "size": SIZE, "mtime_ns": 7,
                            "ctime_ns": 7}}})
    else:
        kwargs = {"wrong-tier": {"tier_id": "prismabuild-stage:other"},
                  "wrong-root": {"stage_root": "/somewhere/else"},
                  "wrong-manifest": {"manifest_sha256": "f" * 64}}[damage]
        _write_sidecar(queue, stage, consumer, mover, _entries(stage), **kwargs)
    receipts = _sweep(queue, stage)
    assert not _retired(receipts, mover), receipts
    _kept(queue, stage, consumer, mover)


@pytest.mark.parametrize("shape", ["mover-failed", "consumer-executed",
                                   "mover-absent"])
def test_terminal_shapes_outside_the_named_form_retain(fleet, shape):
    """Only a FAILED consumer plus a DONE executed or WITHDRAWN mover counts."""

    queue, stage, _ = fleet
    if shape == "mover-failed":
        consumer, mover = stale_owner(fleet, mover_status="failed")
    elif shape == "consumer-executed":
        consumer, mover = stale_owner(fleet, consumer_status="executed")
    else:
        consumer, mover = stale_owner(fleet, mover_status=None,
                                      move_receipt=False)
    receipts = _sweep(queue, stage)
    assert not _retired(receipts, mover), receipts
    _kept(queue, stage, consumer, mover)


def test_a_live_reader_pin_keeps_the_owner_and_its_bytes(fleet):
    """A live pin outranks any recovery: the whole ownership stays."""

    queue, stage, _ = fleet
    consumer, _ = base._fail_consumer(queue)
    mover = base._key()
    base._publish(queue, mover, max_attempts=1)
    for name in NAMES:
        _stage(stage, name, OLD_PAYLOAD)
    _write_sidecar(queue, stage, consumer, mover, _entries(stage))
    _fragment(queue, stage, consumer, mover)
    acquired = reader_lease.acquire(
        queue, consumer_action_key=consumer,
        attempt={"nonce": "n1", "scope_id": "s1"}, tier_id=TIER, epoch="",
        span={"start_bytes": 0, "end_bytes": SIZE},
        holder={"host": "fixture", "pid": os.getpid()}, acquire_token="p1",
        covers=[{"mover_action_key": mover, "manifest_sha256": "a" * 64}])
    assert acquired.get("ok"), acquired
    for name in NAMES:
        replacement = _stage(stage, f"{name}.later", NEW_PAYLOAD)
        os.replace(replacement, stage / staged_name(name))
    queue.finish(mover, status="executed", detail={"returncode": 0})
    _charge(queue, mover)

    receipts = _sweep(queue, stage)
    assert not _retired(receipts, mover), receipts
    _kept(queue, stage, consumer, mover)


def test_a_live_promotion_handoff_keeps_the_owner_and_its_bytes(tmp_path):
    """A live ram promotion's source leg must never be retired under it."""

    import test_a_promotion_handoff_defers_the_source_retirement as handoff

    queue, stage, staged, manifest_sha, manifest = handoff._world(tmp_path)
    handoff._claim_promotion(queue, tmp_path, manifest, manifest_sha)
    replacement = staged.with_name(staged.name + ".later")
    replacement.write_bytes(NEW_PAYLOAD)
    os.setxattr(replacement, base.prewarm_loop.STAGE_SOURCE_XATTR,
                b"/originals/calib.bin@0")
    os.replace(replacement, staged)
    for key, status in ((handoff.CONSUMER_A, "failed"),
                        (handoff.MOVER_A, "executed")):
        queue.publish(action_key=key, cas_root="/cas", checkout_root="/co",
                      worker_script="/w.py", resources={"cpu": 1},
                      max_attempts=1)
        claimed = queue.claim(capacity={"cpu": 4})
        assert claimed and claimed["action_key"] == key
        queue.finish(key, status=status,
                     detail={"returncode": 1 if status == "failed" else 0})
    # Re-acquire after the terminal transitions: a synthetic row without the
    # production residency block does not keep its tier tokens across
    # ``finish``, and this negative is about a *charged* owner whose source
    # leg a live promotion is reading.
    handoff._charged(queue)
    receipts = stage_release.sweep(
        queue, stage_roots={handoff.STAGE_TIER: str(stage)}, pressure={})
    retired = [entry for entry in receipts
               if entry.get("action_key") == handoff.MOVER_A
               and entry.get("complete") is True]
    assert not retired, receipts
    assert staged.exists()
    assert residency_map.fragment_path(
        queue.residency_fragment_root(), handoff.CONSUMER_A,
        handoff.MOVER_A).exists()
    assert reader_lease.material_path(
        queue.residency_fragment_root(), handoff.CONSUMER_A,
        handoff.MOVER_A).exists()
    assert handoff.MOVER_A in queue.tier_ledger(
        handoff.STAGE_TIER).held_keys()
