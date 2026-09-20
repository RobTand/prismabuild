"""A stale donor is declined; the current incarnation is adopted instead (#755).

The live dev-reader failure this file reproduces, on the deployed b8aa
runtime (00ff716f4151-1789915735-d8182a4286a9): a finished donor staged
``calib-8x16.safetensors`` (1296 bytes) and dated its material; a later
publication replaced the shared destination with identical bytes under a new
inode, exactly what the pre-fix unconditional rename did; a later mover
published valid material for that current incarnation.  Then a third
consumer arrived and both reuse paths failed it:

* ``tier_loop.adopt`` copied the first same-descriptor donor's material
  entries unchanged -- dating the successor's vouch with the superseded
  inode -- so the consumer's strict pinned read refused
  ``file-identity-changed`` while a receipt said its range was resident
  (``complete: true, bytes_copied: 0``) even though its own mover had
  failed three attempts at zero bytes;
* ``_StagedPublisher._proof_search`` returned ``divergent`` on the first
  stale ``file_id`` it met, so the consumer's own zero-copy mover path
  refused the destination that another record proved was the current,
  unchanged incarnation.

A stale record is not evidence that the current bytes are different.  The
corrected paths adopt the current incarnation without replacing or
rehashing it, decline a donor whose material no longer dates the files that
are there (publishing no successor at all), and never convert missing proof
or a live pin into permission to overwrite.

Every case uses real small files and the real production classes -- the
tier loop's adoption, the copier's publication gate, and the strict
``reader_lease.acquire``/``open_pinned`` a consumer actually reads through.
"""
from __future__ import annotations

import hashlib
import os
from pathlib import Path
import sys
import threading

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
from prismabuild import pool, reader_lease, residency_map, residency_plan, storage_tiers  # noqa: E402
import prewarm_loop  # noqa: E402
import stage_move  # noqa: E402
import stage_release  # noqa: E402
import tier_loop  # noqa: E402

TIER = "prismabuild-stage:dl380g10"
STAGE_KIND = f"stage_gib@{TIER}"
MANIFEST = "4" * 64
GIB = storage_tiers.GIB
PHASE_GIB = 2
STAGE_GIB = 5

#: The live file: 1,296 bytes of calibration payload, one whole-file entry.
SIZE = 1296
PAYLOAD = bytes(range(256)) * 5 + bytes(range(16))
DIGEST = hashlib.sha256(PAYLOAD).hexdigest()
SOURCE = "/mnt/shared/pq-live-reader-20260920/calib-8x16.safetensors"
RELATIVE = "pq-live-reader-20260920/calib-8x16.safetensors"
MAP_KEY = residency_map.residency_map_key(SOURCE, 0)
ENTRIES = [{"path": SOURCE, "offset": 0, "bytes": SIZE, "sha256": DIGEST}]

FIRST = "1" * 64        # the donor consumer whose material went stale
SECOND = "2" * 64      # the consumer that published the current incarnation
THIRD = "3" * 64       # the live consumer that wants the bytes now
FOURTH = "4" * 64      # a second reader (the mover-path cases)


def _hexkey(seed: str) -> str:
    """A distinct 64-hex action key per name, without hashing a real body."""

    return (seed.encode().hex() * 64)[:64]


#: The stale donor sorts before the current one, so the first-key-wins index
#: of the pre-fix code meets the stale record first -- the live ordering too.
DONOR_A = _hexkey("adonor0")
DONOR_B = _hexkey("bcurrent0")


def _row(queue: pool.PoolQueue, key: str,
         resources: dict[str, int]) -> dict[str, object]:
    return {"action_key": key, "cas_root": str(queue.root / "cas"),
            "checkout_root": str(queue.root / "co"),
            "worker_script": str(queue.root / "worker.py"),
            "tags": ["dl380g10"], "resources": resources}


def _plan(queue: pool.PoolQueue, consumer: str, *,
          label: str = "") -> dict[str, object]:
    """One consumer's frozen plan over the shared manifest, as #598 tests seal."""

    start, end = 0, PHASE_GIB * GIB
    built = [{
        "name": "phase-0",
        "start_bytes": start, "end_bytes": end, "stage_gib": PHASE_GIB,
        "mover_row": {
            **_row(queue, _hexkey(f"{label}mover0"),
                   {STAGE_KIND: PHASE_GIB, "cpu": 1, "mem_gb": 1}),
            "residency": {
                "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                "manifest_sha256": MANIFEST, "manifest_bytes": 1 << 30,
                "range_start_bytes": start, "range_end_bytes": end},
        },
        "egress_row": _row(queue, _hexkey(f"{label}egress0"), {"mem_gb": 1}),
    }]
    return residency_plan.build_plan(
        consumer_action_key=consumer, tier_id=TIER, stage_root="/stage/prewarm",
        manifest_sha256=MANIFEST, manifest_bytes=1 << 30, phases=built)


def _publish_consumer(queue: pool.PoolQueue, consumer: str,
                      plan: dict[str, object]) -> None:
    residency_plan.freeze(queue, plan)
    queue.publish(
        action_key=consumer, cas_root=queue.root / "cas",
        checkout_root=queue.root / "co", worker_script=queue.root / "worker.py",
        resources={"cpu": 1, "mem_gb": 1},
        residency={"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                   "manifest_sha256": MANIFEST, "manifest_bytes": 1 << 30,
                   "leads": residency_plan.leads_for(plan)})


def _write_destination(stage: Path) -> dict[str, int]:
    """A first publication: the file arrives on the stage."""

    destination = stage / RELATIVE
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(PAYLOAD)
    identity = reader_lease.stat_identity(str(destination))
    assert identity is not None
    return identity


def _replace_destination_with_identical_bytes(stage: Path) -> dict[str, int]:
    """What the pre-fix publisher did: an unconditional rename per copy.

    Identical bytes under a fresh inode/mtime/ctime -- the history the live
    tree carries, reproduced with the same operation that produced it.
    """

    destination = stage / RELATIVE
    temporary = destination.with_name(f".{destination.name}.history.tmp")
    temporary.write_bytes(PAYLOAD)
    os.replace(temporary, destination)
    identity = reader_lease.stat_identity(str(destination))
    assert identity is not None
    return identity


def _publish_donor_record(queue: pool.PoolQueue, *, mover: str, consumer: str,
                          stage: Path, ordinal: int = 0) -> dict[str, int]:
    """A finished mover's full record around the file already on the stage.

    Tokens held, a fragment naming the file, a material sidecar dating the
    incarnation that is there now (``stat_identity`` at publish, exactly
    what ``stage_move`` files), and a completed receipt -- the four things
    every reader downstream reads.  The file itself is not touched: which
    incarnation a donor dates is decided by what happened to the name
    before this runs, which is what these tests arrange.
    """

    destination = stage / RELATIVE
    identity = reader_lease.stat_identity(str(destination))
    assert identity is not None
    root = queue.residency_fragment_root()
    residency_map.write_fragment(root, {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": consumer, "mover_action_key": mover,
        "tier_id": TIER, "stage_root": str(stage),
        "manifest_sha256": MANIFEST,
        "entries": {MAP_KEY: {
            "stage_path": str(destination), "bytes": SIZE, "offset": 0,
            "sha256": DIGEST}}})
    reader_lease.write_material(
        root, consumer_action_key=consumer, mover_action_key=mover,
        tier_id=TIER, stage_root=str(stage), manifest_sha256=MANIFEST,
        generation=reader_lease.mint_generation(),
        entries={MAP_KEY: {"stage_path": str(destination), "bytes": SIZE,
                           "sha256": DIGEST, "file_id": identity}})
    assert queue.tier_ledger(TIER).acquire(mover, {"stage_gib": PHASE_GIB})
    start = ordinal * PHASE_GIB * GIB
    end = (ordinal + 1) * PHASE_GIB * GIB
    queue.record_move(mover, {
        "consumer_action_key": consumer, "tier_id": TIER,
        "stage_root": str(stage), "manifest_sha256": MANIFEST,
        "range_start_bytes": start, "range_end_bytes": end,
        "range_bytes": end - start, "bytes_staged": end - start,
        "entries_declared": 1, "entries_staged": 1,
        "complete": True, "seconds": 0.25, "unix": 1000.0 + ordinal})
    return identity


def _tier_record(stage: Path) -> dict[str, object]:
    return {"schema": storage_tiers.TIER_RECORD_SCHEMA_V1, "tier_id": TIER,
            "host": "dl380g10", "tier": "stage", "mountpoint": str(stage),
            "capacity_bytes": STAGE_GIB * GIB}


def _cycle(queue: pool.PoolQueue, stage: Path) -> None:
    tier_loop.cycle(queue, host="dl380g10", source_pool="storage_pool",
                    receipts=tier_loop.ReceiptCache(),
                    discover=lambda **_kwargs: {TIER: _tier_record(stage)})


def assert_ledger_matches_the_stage(queue: pool.PoolQueue) -> None:
    """Held tier tokens equal the ranges the stage's fragments account for."""

    ledger = queue.tier_ledger(TIER)
    held = {key: gib for key, gib in
            ((key, ledger.holder_tokens(key).get("stage_gib", 0))
             for key in ledger.held_keys()) if gib}
    root = queue.residency_fragment_root()
    accounted: dict[str, int] = {}
    hexdigits = set("0123456789abcdef")
    # Only consumer directories -- the material sidecars and pin files live
    # in sibling directories of this root that name no consumer.
    consumers = sorted(e.name for e in root.iterdir()
                       if e.is_dir() and len(e.name) == 64
                       and set(e.name) <= hexdigits)
    for consumer in consumers:
        for fragment in residency_map.read_fragments(root, consumer):
            mover = str(fragment["mover_action_key"])
            for entry in dict(fragment["entries"]).values():
                assert Path(str(entry["stage_path"])).exists(), (
                    f"{mover[:8]} vouches for {entry['stage_path']}, gone")
            receipt = queue.move_record(mover)
            assert receipt is not None, f"{mover[:8]}: fragment, no receipt"
            accounted[mover] = storage_tiers.stage_tokens_for_bytes(
                int(receipt["range_end_bytes"])
                - int(receipt["range_start_bytes"]))
    assert held == accounted, f"held={held} accounted={accounted}"


def _read_like_the_consumer(queue: pool.PoolQueue, *, consumer: str,
                            mover: str) -> bytes:
    """The strict pinned read a consumer's calibration actually performs.

    Deliberately not released in the pin-refusing cases: a reader that is
    still reading is exactly what a live pin is.
    """

    acquired = reader_lease.acquire(
        queue, consumer_action_key=consumer,
        attempt={"nonce": "n1", "scope_id": "s1"}, tier_id=TIER, epoch="",
        span={"start_bytes": 0, "end_bytes": SIZE},
        holder={"host": "test-host", "pid": os.getpid()},
        acquire_token=f"{consumer[:8]}-token",
        covers=[{"mover_action_key": mover, "manifest_sha256": MANIFEST}],
        expected={MAP_KEY: {"bytes": SIZE, "sha256": DIGEST}})
    assert acquired["ok"], acquired
    fd, _serving = reader_lease.open_pinned(
        queue, acquired["pin"], acquired["ref_id"], MAP_KEY)
    try:
        return os.read(fd, SIZE)
    finally:
        os.close(fd)


@pytest.fixture()
def queue(tmp_path: Path) -> pool.PoolQueue:
    q = pool.PoolQueue(tmp_path / "pb-queue")
    q.ensure_layout()
    q.mint_tier_capacity(TIER, {"stage_gib": STAGE_GIB})
    return q


@pytest.fixture()
def stage(tmp_path: Path, queue: pool.PoolQueue) -> Path:
    path = tmp_path / "stage"
    path.mkdir()
    stage_release.register_stage_root(queue, tier_id=TIER, stage_root=path)
    return path


@pytest.fixture()
def history(queue: pool.PoolQueue, stage: Path) -> dict[str, object]:
    """The live tree's history: stale donor A, current donor B, both held.

    A stages the file and dates that incarnation; the destination is then
    replaced with identical bytes (the pre-fix unconditional rename); B --
    the mover that published under that old behavior -- records the same
    descriptor against the incarnation now on the stage.  Both consumers
    are terminal, both ranges still hold tokens, and the file is B's.
    """

    stale_identity = _write_destination(stage)
    _publish_donor_record(queue, mover=DONOR_A, consumer=FIRST, stage=stage)
    current_identity = _replace_destination_with_identical_bytes(stage)
    assert current_identity["ino"] != stale_identity["ino"]
    assert current_identity["size"] == stale_identity["size"]
    _publish_donor_record(queue, mover=DONOR_B, consumer=SECOND, stage=stage)
    live = reader_lease.stat_identity(str(stage / RELATIVE))
    assert live is not None
    assert live == current_identity
    assert_ledger_matches_the_stage(queue)
    return {"stale": stale_identity, "current": current_identity, "live": live}


# ---------------------------------------------- the tier loop's adoption half


def test_the_current_incarnation_is_adopted_and_read(
        queue, stage, history) -> None:
    """The positive path: C takes over B's range and reads B's bytes.

    RED on the pre-fix tree: the first-key-wins index meets donor A first,
    copies its stale entries into the successor's material, and the
    consumer's strict read then refuses ``file-identity-changed`` -- the
    exact live refusal.  GREEN: A is declined ``donor_file_changed``, B is
    adopted, C's material dates the file that is actually there, and the
    real pinned reader reads the payload with the inode preserved.
    """

    _publish_consumer(queue, THIRD, _plan(queue, THIRD, label="third"))
    third_mover = _hexkey("thirdmover0")

    _cycle(queue, stage)

    assert_ledger_matches_the_stage(queue)
    ledger = queue.tier_ledger(TIER)
    # Credit moved through the existing transfer, from the current donor.
    assert ledger.holder_tokens(third_mover) == {"stage_gib": PHASE_GIB}
    assert ledger.holder_tokens(DONOR_B) == {}
    # The stale donor was not touched: it still vouches and still holds.
    assert ledger.holder_tokens(DONOR_A) == {"stage_gib": PHASE_GIB}
    receipt = queue.move_record(third_mover)
    assert receipt is not None
    assert receipt[pool.MOVE_ADOPTED_FROM_FIELD] == DONOR_B
    assert receipt["bytes_copied"] == 0
    # No copy was queued for the successor.
    assert not queue.item_path(pool.READY, third_mover).exists()
    # The successor's material dates the incarnation that is there now...
    root = queue.residency_fragment_root()
    material = reader_lease.read_material(root, THIRD, third_mover)
    assert isinstance(material, dict)
    assert material["entries"][MAP_KEY]["file_id"] == history["live"], (
        "the successor's material must date the current file, not the "
        f"donor history: {material['entries'][MAP_KEY]['file_id']} "
        f"vs live {history['live']}")
    # ...the file itself was not replaced...
    assert reader_lease.stat_identity(str(stage / RELATIVE)) == history["live"]
    # ...and the consumer's real pinned read returns the actual bytes.
    assert _read_like_the_consumer(
        queue, consumer=THIRD, mover=third_mover) == PAYLOAD


def test_a_donor_whose_file_went_stale_publishes_no_successor(
        queue, stage) -> None:
    """No current proof anywhere: refusal, with zero successor publication.

    A stages, the destination is replaced underneath it, and nobody dates
    the new incarnation.  The adoption declines ``donor_file_changed`` and
    publishes nothing: no successor fragment, no successor material, no
    token movement.  The mover half meets the same world in
    :func:`test_without_a_current_proof_the_publisher_refuses_and_keeps_the_file`.
    """

    _write_destination(stage)
    _publish_donor_record(queue, mover=DONOR_A, consumer=FIRST, stage=stage)
    _replace_destination_with_identical_bytes(stage)
    _publish_consumer(queue, THIRD, _plan(queue, THIRD, label="third"))
    third_mover = _hexkey("thirdmover0")
    root = queue.residency_fragment_root()

    _cycle(queue, stage)

    assert_ledger_matches_the_stage(queue)
    ledger = queue.tier_ledger(TIER)
    assert ledger.holder_tokens(DONOR_A) == {"stage_gib": PHASE_GIB}
    assert ledger.holder_tokens(third_mover) == {}
    assert not residency_map.fragment_path(root, THIRD, third_mover).exists(), (
        "a declined adoption must publish no successor fragment")
    assert not reader_lease.material_path(
        root, THIRD, third_mover).exists(), (
        "a declined adoption must publish no successor material")
    assert queue.move_record(third_mover) is None
    # ...and the stale donor's own records are still exactly what they were.
    assert isinstance(reader_lease.read_material(root, FIRST, DONOR_A), dict)


# --------------------------------------------- the mover's publication half


def _publisher(queue: pool.PoolQueue, stage: Path, mover: str
               ) -> stage_move._StagedPublisher:
    return stage_move._StagedPublisher(
        queue=queue, stage_root=stage,
        residency_root=queue.residency_fragment_root(),
        mover_action_key=mover, manifest_sha256=MANIFEST,
        tier_id=TIER, cas_root=str(queue.root / "cas"))


def _copier(tmp_path: Path, stage: Path, mover: str,
            queue: pool.PoolQueue) -> stage_move._Copier:
    pool_dir = tmp_path / "shared"
    (pool_dir / "pq-live-reader-20260920").mkdir(parents=True, exist_ok=True)
    (pool_dir / "pq-live-reader-20260920" / "calib-8x16.safetensors"
     ).write_bytes(PAYLOAD)
    return stage_move._Copier(
        mounts=prewarm_loop.MountMap([f"/mnt/shared={pool_dir}"]),
        pacer=None, stage_root=stage, mount_prefix="/mnt/shared",
        block=65536, workers=1, owner=mover,
        publisher=_publisher(queue, stage, mover))


def test_the_mover_adopts_the_current_incarnation_despite_a_stale_record(
        queue, stage, tmp_path, history) -> None:
    """The consumer's own zero-copy path, on the same shared history.

    RED on the pre-fix tree: ``_proof_search`` meets A's stale ``file_id``
    first, answers ``divergent``, and the mover fails its entry with
    ``staged destination holds different bytes...`` at zero bytes staged --
    the three failed attempts of the live incident.  GREEN: the stale
    record is skipped, B's proof of the current incarnation adopts without
    copying, the mover's own material dates the live inode, and a real
    pinned reader reads the bytes through the mover's fresh cover.
    """

    fourth_mover = _hexkey("fourthmover0")
    before = reader_lease.stat_identity(str(stage / RELATIVE))
    copier = _copier(tmp_path, stage, fourth_mover, queue)

    copier.run(ENTRIES, whole={SOURCE}, stop=threading.Event())

    assert copier.errors == [], (
        f"the mover must adopt the current incarnation, not refuse it: "
        f"{copier.errors}")
    assert copier.bytes_staged == SIZE
    destination = stage / RELATIVE
    # No replacement, no rehash: the same incarnation, now vouched by this
    # mover's own dated material.
    assert reader_lease.stat_identity(str(destination)) == before
    assert copier.sidecar[MAP_KEY]["file_id"] == before
    root = queue.residency_fragment_root()
    residency_map.write_fragment(root, {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": FOURTH, "mover_action_key": fourth_mover,
        "tier_id": TIER, "stage_root": str(stage),
        "manifest_sha256": MANIFEST, "entries": copier.staged})
    reader_lease.write_material(
        root, consumer_action_key=FOURTH, mover_action_key=fourth_mover,
        tier_id=TIER, stage_root=str(stage), manifest_sha256=MANIFEST,
        generation=reader_lease.mint_generation(), entries=copier.sidecar)
    assert _read_like_the_consumer(
        queue, consumer=FOURTH, mover=fourth_mover) == PAYLOAD


def test_without_a_current_proof_the_publisher_refuses_and_keeps_the_file(
        queue, stage, monkeypatch: pytest.MonkeyPatch) -> None:
    """Stale-only history is not permission to overwrite the destination.

    The copy is already verified in a temporary beside the name, as
    ``publish`` always receives it; with no record dating the incarnation
    that is there, the gate refuses after the grace and defers to the
    action stall policy's retry.  The destination's inode is untouched.
    """

    _write_destination(stage)
    _publish_donor_record(queue, mover=DONOR_A, consumer=FIRST, stage=stage)
    live = _replace_destination_with_identical_bytes(stage)
    destination = stage / RELATIVE
    monkeypatch.setattr(stage_move, "_PUBLISH_GRACE_S", 0.05)
    monkeypatch.setattr(stage_move, "_PUBLISH_POLL_S", 0.01)
    mover = _hexkey("fourthmover0")
    publisher = _publisher(queue, stage, mover)
    temporary = destination.with_name(
        f".{destination.name}.{mover[:16]}.partial")
    temporary.write_bytes(PAYLOAD)

    with pytest.raises(OSError, match="published elsewhere"):
        publisher.publish(dict(ENTRIES[0]), destination, temporary, DIGEST,
                          threading.Event())

    assert not temporary.exists()
    assert reader_lease.stat_identity(str(destination)) == live


def test_a_live_pinned_file_with_only_stale_records_is_never_replaced(
        queue, stage) -> None:
    """A live pin outranks every missing-proof story, immediately.

    A reader pinned the incarnation B published; the destination was later
    replaced again (B's material went stale with it), and the pin is still
    live.  A new mover's publication gate refuses at once, naming the pin,
    and the file that is there now is not replaced.
    """

    _write_destination(stage)
    _publish_donor_record(queue, mover=DONOR_A, consumer=FIRST, stage=stage)
    current = _replace_destination_with_identical_bytes(stage)
    _publish_donor_record(queue, mover=DONOR_B, consumer=SECOND, stage=stage)
    pinned = _read_like_the_consumer(queue, consumer=SECOND, mover=DONOR_B)
    assert pinned == PAYLOAD
    replaced = _replace_destination_with_identical_bytes(stage)
    assert replaced["ino"] != current["ino"]

    destination = stage / RELATIVE
    mover = _hexkey("fourthmover0")
    publisher = _publisher(queue, stage, mover)
    temporary = destination.with_name(
        f".{destination.name}.{mover[:16]}.partial")
    temporary.write_bytes(PAYLOAD)

    with pytest.raises(OSError, match="live-pinned"):
        publisher.publish(dict(ENTRIES[0]), destination, temporary, DIGEST,
                          threading.Event())

    assert reader_lease.stat_identity(str(destination)) == replaced
