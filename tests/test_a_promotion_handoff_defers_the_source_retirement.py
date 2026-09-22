"""A live promotion handoff defers its source's retirement, proof and charge.

Observed on deployed PB main184da/source1d487 (2026-09-20): head
``a52860ef3084`` staged 36,439 entries / 10,895,318,814 logical bytes; its
egress ``16f979eed929`` completed with ``bytes_deleted 0`` and
``entries_shared 36,439``, naming ``promotion-handoff`` among its
``shared_with``; both the SSD fragment and the material sidecar for that head
are now absent, and the next head over the same names ran at roughly 0.5
files/s and timed out after 3600s.

The mechanism, in ``stage_release._evict_owned``: a path a live RAM promotion
holds as its **source** was treated as a shared skip -- the file survived, but
the pass then had no errors and no deferred entries, so it unlinked this
mover's fragment and material and settled its tokens.  Two consequences, both
asserted below as the behaviour that must hold:

* The surviving bytes lose their only proof.  The promotion's RAM fragment
  names a different tier and a different path, so it can never vouch for the
  SSD incarnation, and ``_StagedPublisher._proof_search`` needs a same-path
  fragment **plus** its material sidecar **plus** current file identity before
  it will adopt.  Without them the next publisher recopies and pays the
  publish grace per entry.
* The charge leaves early.  The shared branch added no ``bytes_shared``, so
  ``shared_part = min(count - freed, _tokens_for_egressed_bytes(0)) = 0`` and
  ``free[kind] = count``: every token came back as free while every byte was
  still on the stage.

The repair is the deferral the pinned branch already implements -- keep the
file, the fragment, the material and the charge until the handoff ends -- with
one deliberate difference proved by
``test_a_handoff_deferral_leaves_the_promotion_able_to_prove_its_source``: a
handoff-only pass files **no** retiring mark, because ``reader_lease.acquire``
refuses a generation that carries one and ``ram_promote`` acquires its
proof-only cover *after* its claim row exists.

Every promotion claim here is a real queue record plus its sealed CAS request,
read through ``stage_release._claimed_source_paths`` itself; no source-path set
is mocked.  Files are tiny (16 KiB).
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

import prismabuild.core as pb  # noqa: E402
from prismabuild import pool, reader_lease, residency_map  # noqa: E402
from prismabuild import storage_tiers  # noqa: E402
import ram_promote  # noqa: E402
import stage_move  # noqa: E402
import stage_release  # noqa: E402

STAGE_TIER = "prismabuild-stage:testbox"
RAM_TIER = "ram:testbox"
CONSUMER_A = "a" * 64
CONSUMER_B = "b" * 64
MOVER_A = "1" * 64
MOVER_B = "2" * 64
RAM_MOVER = "e" * 64
SIZE = 16 * 1024


# ---------------------------------------------------------------------------
# Fixtures: a real staged range, and a real claimed promotion over its source.


def _payload() -> bytes:
    return bytes([index % 251 + 1 for index in range(SIZE)])


def _queue(tmp_path: Path) -> pool.PoolQueue:
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    return queue


def _manifest(tmp_path: Path) -> tuple[Path, str, dict]:
    origin = tmp_path / "origin"
    origin.mkdir(parents=True, exist_ok=True)
    payload = _payload()
    (origin / "calib.bin").write_bytes(payload)
    body = {
        "schema": pb.DATA_MANIFEST_SCHEMA_V1,
        "produced_by": {"tool": "promotion-handoff-fixture"},
        "mount_prefix": str(origin),
        "entries": [{"path": str(origin / "calib.bin"), "offset": 0,
                     "bytes": SIZE,
                     "sha256": hashlib.sha256(payload).hexdigest()}],
        "entry_count": 1,
        "total_bytes": SIZE,
        "annotations": {},
    }
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(body))
    return path, hashlib.sha256(path.read_bytes()).hexdigest(), body


def _stage_root(tmp_path: Path) -> Path:
    return tmp_path / "stage"


def _stage(queue: pool.PoolQueue, tmp_path: Path, manifest: Path,
           manifest_sha: str, consumer: str, mover: str) -> dict:
    """One real ``stage_move.move``: copy, fragment and material sidecar."""

    args = stage_move.build_parser().parse_args([
        "--pool-root", str(queue.root),
        "--cas-root", str(tmp_path / "cas"),
        "--action-key", mover,
        "--consumer-action-key", consumer,
        "--tier-id", STAGE_TIER,
        "--stage-root", str(_stage_root(tmp_path)),
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
    receipt = stage_move.move(args)
    assert receipt["complete"] is True, receipt
    return receipt


def _staged_path(queue: pool.PoolQueue, consumer: str) -> Path:
    fragments = residency_map.read_fragments(
        queue.root / pool.RESIDENCY, consumer)
    entries = residency_map.compose(fragments)["entries"]
    assert isinstance(entries, dict) and len(entries) == 1
    return Path(str(next(iter(entries.values()))["stage_path"]))


def _claim_promotion(queue: pool.PoolQueue, tmp_path: Path, manifest: Path,
                     manifest_sha: str, *, key: str = RAM_MOVER) -> Path:
    """A live RAM promotion claim over this stage's source leg.

    The shape ``ram_promote`` runs under: a claimed row demanding ram tokens,
    naming its own CAS root, plus the sealed request whose command carries
    ``--source-stage-root`` and the range, and the manifest blob that request's
    input names.  ``_claimed_source_paths`` reads exactly this.
    """

    cas = tmp_path / "cas"
    blob = pb.PrismaBuildCAS(cas).blob_path(manifest_sha)
    blob.parent.mkdir(parents=True, exist_ok=True)
    blob.write_bytes(manifest.read_bytes())
    request = {
        "action_key": key,
        "params": {"command": [
            "python3", "ram_promote.py",
            "--consumer-action-key", CONSUMER_A,
            "--tier-id", RAM_TIER,
            "--ram-root", str(tmp_path / "ram"),
            "--source-stage-root", str(_stage_root(tmp_path)),
            "--range-start-bytes", "0",
            "--range-end-bytes", str(SIZE),
        ]},
        "inputs": [{"id": pb.PBCAMPAIGN_DATA_MANIFEST_INPUT_ID,
                    "sha256": manifest_sha, "bytes": blob.stat().st_size}],
    }
    sealed = cas / "requests" / key[:2] / f"{key}.json"
    sealed.parent.mkdir(parents=True, exist_ok=True)
    sealed.write_text(json.dumps(request))
    claimed = queue.dir(pool.CLAIMED)
    claimed.mkdir(parents=True, exist_ok=True)
    row = claimed / f"{key}.json"
    row.write_text(json.dumps({
        "action_key": key,
        "cas_root": str(cas),
        "resources": {"cpu": 2, "mem_gb": 1,
                      f"{storage_tiers.RAM_CAPACITY_KIND}@{RAM_TIER}": 1},
    }))
    return row


def _charged(queue: pool.PoolQueue, mover: str = MOVER_A) -> None:
    """The stage tokens a mover holds for its landed bytes."""

    queue.mint_tier_capacity(STAGE_TIER, {storage_tiers.STAGE_CAPACITY_KIND: 8})
    assert queue.tier_ledger(STAGE_TIER).acquire(
        mover, {storage_tiers.STAGE_CAPACITY_KIND: 1}) is True


def _world(tmp_path: Path) -> tuple[pool.PoolQueue, Path, Path, str, Path]:
    """Queue, stage root, staged file, manifest sha, manifest path."""

    queue = _queue(tmp_path)
    manifest, manifest_sha, _body = _manifest(tmp_path)
    stage = _stage_root(tmp_path)
    _stage(queue, tmp_path, manifest, manifest_sha, CONSUMER_A, MOVER_A)
    assert stage_release.register_stage_root(
        queue, tier_id=STAGE_TIER, stage_root=stage) == "registered"
    staged = _staged_path(queue, CONSUMER_A)
    assert staged.exists()
    _charged(queue)
    return queue, stage, staged, manifest_sha, manifest


def _fragment_path(queue: pool.PoolQueue, consumer: str, mover: str) -> Path:
    return residency_map.fragment_path(
        queue.root / pool.RESIDENCY, consumer, mover)


def _material_path(queue: pool.PoolQueue, consumer: str, mover: str) -> Path:
    return reader_lease.material_path(
        queue.root / pool.RESIDENCY, consumer, mover)


def _evict(queue: pool.PoolQueue, stage: Path, *, mover: str = MOVER_A,
           consumer: str = CONSUMER_A) -> dict:
    return stage_release.evict(queue, mover, consumer_action_key=consumer,
                               stage_root=str(stage))


# ---------------------------------------------------------------------------
# The claim is recognized through the real helper, not a mocked path set.


def test_the_promotion_claim_is_recognized_through_the_real_helper(
        tmp_path: Path) -> None:
    queue, stage, staged, manifest_sha, manifest = _world(tmp_path)

    paths, tainted = stage_release._claimed_source_paths(queue, stage)
    assert (paths, tainted) == (set(), []), (
        "no claim is filed yet, so no source leg is held")

    _claim_promotion(queue, tmp_path, manifest, manifest_sha)

    paths, tainted = stage_release._claimed_source_paths(queue, stage)
    assert tainted == []
    # The staged source leg under its range name, and the bare name a stage
    # leg sealed before range-only naming may still hold (over-retained).
    bare = staged.parent.parent / "calib.bin"
    assert paths == {os.path.normpath(str(staged)),
                     os.path.normpath(str(bare))}, (
        "the sealed promotion claim must name this staged source leg")


# ---------------------------------------------------------------------------
# Egress during the handoff: defer with proof and charge held.


def test_an_egress_during_a_live_promotion_keeps_the_sources_proof(
        tmp_path: Path) -> None:
    """The bytes stay -- and so must the only records that prove them.

    Before the repair this pass counted the entry as *shared* and reported
    ``complete``, so it unlinked this mover's fragment and material sidecar
    while the file itself survived: exactly the proofless SSD copy the
    deployed head left behind.
    """

    queue, stage, staged, manifest_sha, manifest = _world(tmp_path)
    _claim_promotion(queue, tmp_path, manifest, manifest_sha)
    identity = reader_lease.stat_identity(str(staged))

    receipt = _evict(queue, stage)

    assert staged.exists(), "a live promotion source must not be unlinked"
    assert reader_lease.stat_identity(str(staged)) == identity
    # The proof the surviving bytes need: same-path fragment plus sidecar.
    assert _fragment_path(queue, CONSUMER_A, MOVER_A).exists(), (
        "the surviving bytes lost their fragment: nothing can prove them")
    assert _material_path(queue, CONSUMER_A, MOVER_A).exists(), (
        "the surviving bytes lost their material sidecar")
    composed = residency_map.compose(residency_map.read_fragments(
        queue.root / pool.RESIDENCY, CONSUMER_A))["entries"]
    assert [str(entry["stage_path"]) for entry in composed.values()] == [
        str(staged)], "the consumer's map must still name the staged range"

    assert receipt["entries_deferred"] == 1, receipt
    assert receipt["entries_deleted"] == 0
    assert receipt["entries_shared"] == 0, (
        "a handoff is a deferral, not a co-owner's shared skip")
    assert receipt["deferred_handoffs"] == ["promotion-handoff"], receipt
    assert receipt["complete"] is False, (
        "bytes are still on the stage: the sweep must retry")


def test_an_egress_during_a_live_promotion_keeps_the_sources_charge(
        tmp_path: Path) -> None:
    """Occupancy stays with the occupied bytes.

    The shared branch added no ``bytes_shared``, so the settle computed
    ``shared_part = min(count - freed, _tokens_for_egressed_bytes(0)) = 0``
    and ``free[kind] = count``: every token came back as free while every
    byte was still on the stage.
    """

    queue, stage, staged, manifest_sha, manifest = _world(tmp_path)
    _claim_promotion(queue, tmp_path, manifest, manifest_sha)

    receipt = _evict(queue, stage)

    assert staged.exists()
    assert queue.tier_ledger(STAGE_TIER).holder_tokens(MOVER_A) == {
        storage_tiers.STAGE_CAPACITY_KIND: 1}, (
            "tokens came back free for bytes that never left the stage")
    assert receipt["tokens_released"] == 0
    assert receipt["tokens_decharged"] == 0


def test_a_handoff_only_deferral_files_no_retiring_mark(
        tmp_path: Path) -> None:
    """A mark would close the generation the promotion still has to prove.

    ``reader_lease.acquire`` refuses a covering mover whose live material
    generation carries a retiring mark, and ``ram_promote`` takes its
    proof-only cover *after* its claim row exists -- so a mark filed by this
    deferral would refuse the very handoff the deferral protects.  The
    negative control at the end of this test is that refusal, shown directly.
    """

    queue, stage, staged, manifest_sha, manifest = _world(tmp_path)
    _claim_promotion(queue, tmp_path, manifest, manifest_sha)

    receipt = _evict(queue, stage)
    assert receipt["entries_deferred"] == 1
    assert receipt["retiring"] is False
    leases = reader_lease.leases_root(queue, queue.root / pool.RESIDENCY)
    marks, tainted = reader_lease.retiring_for(leases, MOVER_A)
    assert (marks, tainted) == ([], []), (
        "a handoff-only deferral must not close this generation")

    # The promotion still runs: it proves its source and copies to RAM.
    ram = tmp_path / "ram"
    ram.mkdir(exist_ok=True)
    assert storage_tiers.ensure_ram_epoch(ram, host="testbox") is not None
    promotion = ram_promote.promote(ram_promote.build_parser().parse_args([
        "--pool-root", str(queue.root),
        "--cas-root", str(tmp_path / "cas"),
        "--action-key", RAM_MOVER,
        "--consumer-action-key", CONSUMER_A,
        "--tier-id", RAM_TIER,
        "--ram-root", str(ram),
        "--source-stage-root", str(stage),
        "--manifest-sha256", manifest_sha,
        "--range-start-bytes", "0",
        "--range-end-bytes", str(SIZE),
        "--manifest", str(manifest),
        "--residency-root", str(queue.root / pool.RESIDENCY),
        "--block", "4096",
        "--readers", "2",
        "--max-readers", "2",
    ]))
    assert promotion.get("refusal") is None, promotion
    assert promotion["complete"] is True
    assert (ram / stage_move.stage_relative(
        "/m/calib.bin", 0, SIZE, mount_prefix="/m")).read_bytes() == _payload()

    # The RAM fragment names the ram tier and the ram path: it can never
    # vouch for the surviving SSD incarnation, which is why the SSD
    # fragment had to stay.
    ram_fragment = residency_map.validate_fragment(json.loads(
        _fragment_path(queue, CONSUMER_A, RAM_MOVER).read_text()))
    assert ram_fragment["tier_id"] == RAM_TIER
    assert ram_fragment["stage_root"] == str(ram)
    assert str(staged) not in [
        str(entry["stage_path"])
        for entry in ram_fragment["entries"].values()]

    # Negative control: the mark this deferral deliberately did not write
    # would have refused that promotion.
    material = reader_lease.read_material(
        queue.root / pool.RESIDENCY, CONSUMER_A, MOVER_A)
    assert isinstance(material, dict)
    reader_lease.write_retiring(
        leases, consumer_action_key=CONSUMER_A, mover_action_key=MOVER_A,
        generation=str(material["generation"]))
    second_ram = tmp_path / "ram-two"
    second_ram.mkdir()
    assert storage_tiers.ensure_ram_epoch(second_ram, host="testbox") is not None
    refused = ram_promote.promote(ram_promote.build_parser().parse_args([
        "--pool-root", str(queue.root),
        "--cas-root", str(tmp_path / "cas"),
        "--action-key", "f" * 64,
        "--consumer-action-key", CONSUMER_A,
        "--tier-id", RAM_TIER,
        "--ram-root", str(tmp_path / "ram-two"),
        "--source-stage-root", str(stage),
        "--manifest-sha256", manifest_sha,
        "--range-start-bytes", "0",
        "--range-end-bytes", str(SIZE),
        "--manifest", str(manifest),
        "--residency-root", str(queue.root / pool.RESIDENCY),
        "--block", "4096",
        "--readers", "2",
        "--max-readers", "2",
    ]))
    assert refused["refusal"] == "retiring", refused


def test_the_deferred_proof_still_adopts_for_a_current_publisher(
        tmp_path: Path) -> None:
    """The point of keeping the proof: no recopy, no rehash, no grace.

    The original is overwritten with different bytes of the same length
    before the second consumer stages the same range.  A recopy would read
    and hash those bytes and refuse on the manifest digest; only an adoption
    of the surviving published incarnation can complete -- and the staged
    file's identity proves nothing was replaced.
    """

    queue, stage, staged, manifest_sha, manifest = _world(tmp_path)
    _claim_promotion(queue, tmp_path, manifest, manifest_sha)
    assert _evict(queue, stage)["entries_deferred"] == 1

    identity = reader_lease.stat_identity(str(staged))
    original = tmp_path / "origin" / "calib.bin"
    original.write_bytes(b"\xff" * SIZE)

    receipt = _stage(queue, tmp_path, manifest, manifest_sha,
                     CONSUMER_B, MOVER_B)

    assert receipt["complete"] is True, receipt
    assert staged.read_bytes() == _payload(), "the published bytes stand"
    assert reader_lease.stat_identity(str(staged)) == identity, (
        "adoption publishes no new incarnation")
    assert _staged_path(queue, CONSUMER_B) == staged


# ---------------------------------------------------------------------------
# After the handoff: delete or retain, and release exactly once.


def test_the_retry_after_the_handoff_deletes_and_releases_exactly_once(
        tmp_path: Path) -> None:
    queue, stage, staged, manifest_sha, manifest = _world(tmp_path)
    row = _claim_promotion(queue, tmp_path, manifest, manifest_sha)
    assert _evict(queue, stage)["entries_deferred"] == 1

    row.unlink()                      # the promotion ends; the claim is gone
    assert stage_release._claimed_source_paths(queue, stage) == (set(), [])

    retry = _evict(queue, stage)
    assert retry["complete"] is True, retry
    assert retry["entries_deleted"] == 1
    assert retry["entries_deferred"] == 0
    assert retry["deferred_handoffs"] == []
    assert retry["tokens_released"] == 1
    assert retry["tokens_decharged"] == 0
    assert not staged.exists()
    assert not _fragment_path(queue, CONSUMER_A, MOVER_A).exists()
    assert not _material_path(queue, CONSUMER_A, MOVER_A).exists()
    assert queue.tier_ledger(STAGE_TIER).holder_tokens(MOVER_A) == {}

    again = _evict(queue, stage)      # no fragment left: a no-op, not a free
    assert again["complete"] is True
    assert again["entries_deleted"] == 0
    assert again["tokens_released"] == 0
    assert queue.tier_ledger(STAGE_TIER).holder_tokens(MOVER_A) == {}


def test_the_retry_after_the_handoff_retains_for_a_real_co_owner(
        tmp_path: Path) -> None:
    """A real same-path owner keeps the bytes; the duplicate is decharged."""

    queue, stage, staged, manifest_sha, manifest = _world(tmp_path)
    row = _claim_promotion(queue, tmp_path, manifest, manifest_sha)
    assert _evict(queue, stage)["entries_deferred"] == 1

    _stage(queue, tmp_path, manifest, manifest_sha, CONSUMER_B, MOVER_B)
    row.unlink()

    retry = _evict(queue, stage)
    assert retry["complete"] is True, retry
    assert retry["entries_shared"] == 1
    assert retry["entries_deleted"] == 0
    assert retry["bytes_shared"] == SIZE, (
        "a co-owner's skip carries the bytes it left behind")
    assert retry["tokens_decharged"] == 1
    assert retry["tokens_released"] == 0
    assert MOVER_B[:12] in retry["shared_with"][0]
    assert staged.exists()
    assert not _fragment_path(queue, CONSUMER_A, MOVER_A).exists()
    assert queue.tier_ledger(STAGE_TIER).holder_tokens(MOVER_A) == {}

    last = _evict(queue, stage, mover=MOVER_B, consumer=CONSUMER_B)
    assert last["complete"] is True
    assert last["entries_deleted"] == 1, "the last owner to leave deletes"
    assert not staged.exists()


def test_a_pin_and_a_handoff_on_one_mover_both_defer(tmp_path: Path) -> None:
    """The mixed case: the pin's mark waits for the handoff to end.

    A retiring mark is per mover, not per entry, so filing one for the pinned
    entry would close the same generation the promotion has to prove.  While
    the handoff is live the pass defers both and files nothing; once the claim
    is gone the ordinary pinned deferral resumes and the mark is written.
    """

    queue, stage, staged, manifest_sha, manifest = _world(tmp_path)
    row = _claim_promotion(queue, tmp_path, manifest, manifest_sha)
    key = residency_map.residency_map_key(
        str(tmp_path / "origin" / "calib.bin"), 0)
    proof = reader_lease.acquire(
        queue, consumer_action_key=CONSUMER_A,
        attempt={"nonce": "f" * 32, "scope_id": "handoff-fixture"},
        tier_id=STAGE_TIER, epoch="",
        span={"start_bytes": 0, "end_bytes": SIZE},
        holder={"host": socket.gethostname(), "pid": os.getpid()},
        acquire_token="fixture:reader",
        covers=[{"mover_action_key": MOVER_A,
                 "manifest_sha256": manifest_sha}],
        expected={key: {"bytes": SIZE,
                        "sha256": hashlib.sha256(_payload()).hexdigest()}})
    assert proof.get("ok"), proof
    live, taint = reader_lease.live_for(
        queue, {os.path.normpath(str(staged))},
        residency_root=queue.root / pool.RESIDENCY)
    assert taint == [] and live.get(os.path.normpath(str(staged))), (
        "the fixture must leave a real live pin, not a proof-only cover")
    leases = reader_lease.leases_root(queue, queue.root / pool.RESIDENCY)

    during = _evict(queue, stage)
    assert during["entries_deferred"] == 1, during
    assert during["deferred_handoffs"] == ["promotion-handoff"]
    assert during["retiring"] is False
    assert reader_lease.retiring_for(leases, MOVER_A) == ([], [])
    assert staged.exists()
    assert _fragment_path(queue, CONSUMER_A, MOVER_A).exists()
    assert queue.tier_ledger(STAGE_TIER).holder_tokens(MOVER_A) == {
        storage_tiers.STAGE_CAPACITY_KIND: 1}

    row.unlink()
    after = _evict(queue, stage)
    assert after["entries_deferred"] == 1, after
    assert after["deferred_handoffs"] == []
    assert after["retiring"] is True, (
        "with the handoff gone the ordinary pinned deferral resumes")
    marks, tainted = reader_lease.retiring_for(leases, MOVER_A)
    assert tainted == [] and len(marks) == 1
    assert after["live_pins"] == [str(proof["pin_id"])]
    assert staged.exists()
    assert queue.tier_ledger(STAGE_TIER).holder_tokens(MOVER_A) == {
        storage_tiers.STAGE_CAPACITY_KIND: 1}
