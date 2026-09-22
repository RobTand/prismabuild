"""A ram promotion holds its tokens until the egress frees them.

The promotion node is the #639 part-3 plumbing aimed at the right actor: it
copies the staged range from the SSD stage into the tmpfs under the *same
content-addressed names* -- same relative path, same digest -- so the map
entry it vouches for is the entry the stage already vouched for, only faster
to serve.  It draws no pool bandwidth and paces nothing: its source is the
stage on the same box, which is why the promotion is never admitted before
the stage range has landed (the window publishes it only then).

Occupancy is the stage's rule, transferred intact: the promotion holds
``ram_gib`` from claim past ``finish`` -- the pin, read off its receipt --
and returns it only when an egress deletes its files, because bytes on a
roof-limited tmpfs that no token stands for are ENOSPC waiting to happen
(#640).
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import prismabuild.core as pb  # noqa: E402
from prismabuild import pool, reader_lease, residency_map, storage_tiers  # noqa: E402

import ram_promote  # noqa: E402
import stage_move  # noqa: E402
import stage_release  # noqa: E402

CONSUMER = "c" * 64
MOVER = "a" * 64
STAGE_MOVER = "b" * 64
MANIFEST_SHA = "9" * 64
RAM_TIER = "ram:dl380g10"
GIB = storage_tiers.GIB
CHUNK = 1 << 20
#: The stage's (and the promotion's) name for the one whole-file entry.
SHARD = stage_move.stage_relative("/mnt/shared/model/shard-0.bin", 0, CHUNK,
                                  mount_prefix="/mnt/shared")


def _hexkey(seed: str) -> str:
    return (seed.encode().hex() * 64)[:64]


def _staged(tmp_path: Path) -> tuple[Path, Path, bytes]:
    """One staged file on the SSD stage, and the manifest that names it."""

    payload = bytes(range(256)) * (CHUNK // 256)
    stage = tmp_path / "stage"
    (stage / SHARD).parent.mkdir(parents=True)
    (stage / SHARD).write_bytes(payload)
    manifest = {
        "schema": pb.DATA_MANIFEST_SCHEMA_V1,
        "produced_by": {"tool": "test"},
        "mount_prefix": "/mnt/shared",
        "entries": [{"path": "/mnt/shared/model/shard-0.bin", "offset": 0,
                     "bytes": len(payload),
                     "sha256": hashlib.sha256(payload).hexdigest()}],
        "entry_count": 1, "total_bytes": len(payload), "annotations": {},
    }
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest))
    return stage, path, payload


def _args(tmp_path: Path, queue: pool.PoolQueue, *, stage: Path,
          manifest: Path, ram: Path, mover: str = MOVER,
          source: Path | None = None):
    return ram_promote.build_parser().parse_args([
        "--pool-root", str(queue.root),
        "--action-key", mover,
        "--consumer-action-key", CONSUMER,
        "--tier-id", RAM_TIER,
        "--ram-root", str(ram),
        "--source-stage-root", str(source if source is not None else stage),
        "--manifest-sha256", MANIFEST_SHA,
        "--range-start-bytes", "0",
        "--range-end-bytes", str(CHUNK),
        "--manifest", str(manifest),
        "--residency-root", str(queue.root / pool.RESIDENCY),
    ])


def _publish_stage(queue: pool.PoolQueue, stage: Path, payload: bytes
                   ) -> None:
    """What the stage mover filed before this promotion was ever published.

    A promotion proves its source window against published stage material;
    bytes with no fragment and no sidecar are not a stage range, so the
    tests file both, the way ``stage_move.move`` does in production.
    """

    root = queue.root / pool.RESIDENCY
    staged = stage / SHARD
    key = residency_map.residency_map_key("/mnt/shared/model/shard-0.bin", 0)
    digest = hashlib.sha256(payload).hexdigest()
    residency_map.write_fragment(root, {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": CONSUMER, "mover_action_key": STAGE_MOVER,
        "tier_id": "prismabuild-stage:dl380g10", "stage_root": str(stage),
        "manifest_sha256": MANIFEST_SHA,
        "entries": {key: {"stage_path": str(staged), "bytes": len(payload),
                          "sha256": digest, "offset": 0}}})
    identity = reader_lease.stat_identity(str(staged))
    assert identity is not None
    reader_lease.write_material(
        root, consumer_action_key=CONSUMER, mover_action_key=STAGE_MOVER,
        tier_id="prismabuild-stage:dl380g10", stage_root=str(stage),
        manifest_sha256=MANIFEST_SHA,
        generation=reader_lease.mint_generation(),
        entries={key: {"stage_path": str(staged), "bytes": len(payload),
                       "sha256": digest, "file_id": identity}})


def _epoch(ram: Path) -> str:
    marker = storage_tiers.ensure_ram_epoch(ram, host="dl380g10")
    assert marker is not None
    return str(marker["epoch"])


def test_the_promotion_copies_the_staged_names_into_the_tmpfs(
        tmp_path: Path) -> None:
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    stage, manifest, payload = _staged(tmp_path)
    ram = tmp_path / "ram"
    ram.mkdir()
    epoch = _epoch(ram)
    _publish_stage(queue, stage, payload)

    receipt = ram_promote.promote(_args(tmp_path, queue, stage=stage,
                                        manifest=manifest, ram=ram))

    assert receipt["complete"] is True
    assert receipt.get("refusal") is None
    assert receipt["tier_id"] == RAM_TIER
    assert receipt["epoch"] == epoch
    assert receipt["bytes_staged"] == CHUNK
    # The same content-addressed identity: the relative name the stage holds.
    staged_copy = ram / SHARD
    assert staged_copy.read_bytes() == payload
    fragment = residency_map.validate_fragment(json.loads(
        residency_map.fragment_path(
            queue.root / pool.RESIDENCY, CONSUMER, MOVER).read_text()))
    assert fragment["tier_id"] == RAM_TIER
    assert fragment["stage_root"] == str(ram)
    assert fragment["epoch"] == epoch
    entry = fragment["entries"][
        residency_map.residency_map_key("/mnt/shared/model/shard-0.bin", 0)]
    assert entry["stage_path"] == str(staged_copy)
    assert entry["sha256"] == hashlib.sha256(payload).hexdigest()


def test_a_promotion_without_an_epoch_refuses_to_stage(tmp_path: Path) -> None:
    """No epoch, no promotion: a range nobody can place in time is not resident."""

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    stage, manifest, _ = _staged(tmp_path)
    ram = tmp_path / "ram"
    ram.mkdir()                      # no marker: the pre-bootstrap shape

    receipt = ram_promote.promote(_args(tmp_path, queue, stage=stage,
                                        manifest=manifest, ram=ram))

    assert receipt["refusal"] == "ram_epoch_absent"
    assert receipt["complete"] is False
    assert not (ram / "model").exists()
    assert not residency_map.fragment_path(
        queue.root / pool.RESIDENCY, CONSUMER, MOVER).exists()


def test_a_promotion_refuses_a_source_that_is_not_the_stage(
        tmp_path: Path) -> None:
    """Pool -> ram directly is the one road this refuses (#640's topology)."""

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    stage, manifest, _ = _staged(tmp_path)
    ram = tmp_path / "ram"
    ram.mkdir()
    _epoch(ram)

    receipt = ram_promote.promote(
        _args(tmp_path, queue, stage=stage, manifest=manifest, ram=ram,
              source=tmp_path / "no-such-stage"))

    assert receipt["refusal"] == "ram_source_stage_absent"
    assert receipt["complete"] is False


def test_the_tokens_are_held_past_finish_and_returned_by_the_egress(
        tmp_path: Path) -> None:
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    stage, manifest, payload = _staged(tmp_path)
    ram = tmp_path / "ram"
    ram.mkdir()
    _epoch(ram)
    _publish_stage(queue, stage, payload)
    receipt = ram_promote.promote(_args(tmp_path, queue, stage=stage,
                                        manifest=manifest, ram=ram))
    queue.record_move(MOVER, receipt)

    # The claim files its tokens; the pin keeps them past finish, read off
    # the receipt exactly the way a stage mover's is.
    queue.mint_tier_capacity(RAM_TIER, {"ram_gib": 8})
    assert queue.tier_ledger(RAM_TIER).acquire(MOVER, {"ram_gib": 1})
    record = {"action_key": MOVER, "status": "executed", "residency": {
        "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": RAM_TIER,
        "manifest_sha256": MANIFEST_SHA, "manifest_bytes": CHUNK,
        "range_start_bytes": 0, "range_end_bytes": CHUNK}}
    assert queue.residency_pin_holds(record, MOVER) is True

    # The egress is the only thing that may return them, because it is the
    # only thing that removes the bytes they stand for.
    assert stage_release.register_stage_root(
        queue, tier_id=RAM_TIER, stage_root=ram) == "registered"
    egress = stage_release.evict(queue, MOVER, consumer_action_key=CONSUMER,
                                 stage_root=str(ram))

    assert egress["complete"] is True
    assert not (ram / SHARD).exists()
    assert queue.tier_ledger(RAM_TIER).holder_tokens(MOVER) == {}
    assert not residency_map.fragment_path(
        queue.root / pool.RESIDENCY, CONSUMER, MOVER).exists()
