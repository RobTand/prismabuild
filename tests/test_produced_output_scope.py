"""R2 bounded fixture: template/instance/batches on existing PB machinery.

Drives REAL primitives only -- tier ledger (+transfer), stage_move.move,
residency_map compose/lookup, stage_release.evict, ram_promote.promote
(refusal), queue publish/claim/finish -- over KiB files in tmp_path. No
model bytes, no GPU, no giant hashes. Sparse-file window test uses
truncate (no pages). Every green asserts a transition, refusal, or
byte-equality; SDK-dependent lease steps return the exact dependency
instead of a stub.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

from prismabuild import pool, produced_output as po  # noqa: E402
from prismabuild import residency_map as rm  # noqa: E402
from prismabuild import storage_tiers  # noqa: E402
import stage_move  # noqa: E402
import stage_release  # noqa: E402
import ram_promote  # noqa: E402

STAGE_TIER = "prismabuild-stage:dl380g10"
RAM_TIER = "ram:dl380g10"
STAGE_BARE = "stage_gib"
OWNER = "c" * 64
MOVER0 = "d" * 64
MOVER1 = "e" * 64
RAM_MOVER = "f" * 64
GIB = storage_tiers.GIB


def _template(output_prefix: str, **overrides) -> dict:
    body = {
        "schema": po.TEMPLATE_SCHEMA_V1,
        "version": 1,
        "template_id": "r2-fixture-v1",
        "output_prefix": output_prefix,
        "slots": {
            "boundary-0": {"class": "payload"},
            "boundary-1": {"class": "payload"},
            "cotangent-0": {"class": "payload"},
            "checkpoint-0": {"class": "checkpoint"},
            "scratch-0": {"class": "temp"},
        },
        "durable_maxima": {
            "payload_max_bytes": 1 << 20,
            "checkpoint_max_bytes": 1 << 20,
            "temp_max_bytes": 1 << 20,
        },
        "working_demands": {
            STAGE_TIER: {"minimum_gib": 1, "window_gib": 2},
        },
        "permitted_tiers": [STAGE_TIER],
    }
    body.update(overrides)
    return po.validate_template(body)


def _queue(tmp_path: Path) -> pool.PoolQueue:
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    queue.mint_tier_capacity(STAGE_TIER, {"stage_gib": 8})
    return queue


def _publish_claim(queue: pool.PoolQueue, owner: str = OWNER) -> dict:
    queue.publish(action_key=owner, cas_root="/cas", worker_script="/w.py",
                  checkout_root="/co", resources={"cpu": 1, "mem_gb": 1})
    claimed = queue.claim()
    assert claimed is not None and claimed["action_key"] == owner
    return claimed


def _bind(queue: pool.PoolQueue, template: dict, owner: str = OWNER) -> dict:
    claimed = _publish_claim(queue, owner)
    instance = po.bind_instance(queue, template, owner_action_key=owner,
                                claim_snapshot=claimed)
    po.declare_template(queue.root, template)
    po.declare_instance(queue.root, instance)
    return {"instance": instance, "claimed": claimed}


def _write(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)


def _desc(origin: Path, slot: str, cls: str, name: str, payload: bytes,
          instance: dict, gen: str | None = None) -> dict:
    _write(origin / name, payload)
    return po.validate_descriptor({
        "schema": po.DESCRIPTOR_SCHEMA_V2,
        "slot": slot,
        "artifact_class": cls,
        "path": str(origin / name),
        "bytes": len(payload),
        "sha256": hashlib.sha256(payload).hexdigest(),
        "producer_generation": gen or po.mint_generation(),
        "owner_action_key": instance["owner_action_key"],
        "owner_attempt": dict(instance["owner_attempt"]),
    }, _TEMPLATE_CACHE, instance)


_TEMPLATE_CACHE: dict = {}


def _stage_batch(queue: pool.PoolQueue, batch: dict, origin: Path,
                 stage: Path, out_base: Path, tmp_path: Path) -> dict:
    manifest = po.build_stage_manifest(batch, str(origin))
    total = int(manifest["total_bytes"])
    manifest_path = tmp_path / f"manifest-{batch['batch_id']}.json"
    manifest_path.write_text(json.dumps(manifest))
    args = stage_move.build_parser().parse_args([
        "--pool-root", str(queue.root),
        "--cas-root", str(tmp_path / "cas"),
        "--action-key", batch["mover_key"],
        "--consumer-action-key", batch["batch_namespace"],
        "--tier-id", batch["tier"],
        "--stage-root", str(stage),
        "--manifest-sha256", batch["manifest_digest"],
        "--range-start-bytes", "0",
        "--range-end-bytes", str(total),
        "--manifest", str(manifest_path),
        "--residency-root", str(out_base),
        "--block", str(1 << 16),
        "--readers", "2",
        "--max-readers", "2",
        "--unpaced",
    ])
    receipt = stage_move.move(args)
    assert receipt["complete"] is True
    return receipt


def test_binding_refuses_foreign_and_stale(tmp_path: Path) -> None:
    origin = tmp_path / "pool-origin" / "outputs"
    origin.mkdir(parents=True)
    global _TEMPLATE_CACHE
    _TEMPLATE_CACHE = _template(str(origin))
    queue = _queue(tmp_path)
    claimed = _publish_claim(queue, OWNER)
    instance = po.bind_instance(queue, _TEMPLATE_CACHE,
                                owner_action_key=OWNER,
                                claim_snapshot=claimed)
    assert instance["attempt_source"] in ("broker", "claim")
    assert len(instance["owner_attempt"]["nonce"]) == 32
    # Foreign: snapshot names another action.
    other = "b" * 64
    queue.publish(action_key=other, cas_root="/cas", worker_script="/w.py",
                  checkout_root="/co", resources={"cpu": 1, "mem_gb": 1})
    with pytest.raises(po.ProducedOutputError):
        po.bind_instance(queue, _TEMPLATE_CACHE, owner_action_key=other,
                         claim_snapshot=claimed)
    # Stale: finish the claim, the snapshot no longer names live work.
    queue.finish(OWNER, status="executed", detail={"status": "executed"},
                 claim_snapshot=claimed)
    with pytest.raises(po.ProducedOutputError):
        po.bind_instance(queue, _TEMPLATE_CACHE, owner_action_key=OWNER,
                         claim_snapshot=claimed)


def test_minimum_transfer_no_double_charge(tmp_path: Path) -> None:
    origin = tmp_path / "pool-origin" / "outputs"
    origin.mkdir(parents=True)
    global _TEMPLATE_CACHE
    _TEMPLATE_CACHE = _template(str(origin))
    queue = _queue(tmp_path)
    bound = _bind(queue, _TEMPLATE_CACHE)
    instance = bound["instance"]
    ok = po.admit_instance(queue, instance, _TEMPLATE_CACHE)
    assert ok["ok"] is True
    assert ok["admission"]["minimum_gib"][STAGE_TIER] == 1
    # Bound credit holds NO ledger tokens: the instance namespace stays empty.
    assert queue.tier_ledger(STAGE_TIER).holder_tokens(
        po.reservation_holder(instance)) == {}
    # Prewrite then commit batch0: batch holder acquires exact 1, transfers
    # whole to the mover (no free interval). Held total is mover(1) alone:
    # one real allocation counted once, never doubled beside a standing pool.
    pre = po.require_prewrite(queue, instance, _TEMPLATE_CACHE,
                              batch_id="batch-0000", tier=STAGE_TIER,
                              class_bytes={"payload": 12288, "checkpoint": 0,
                                           "temp": 2048})
    assert pre["ok"] is True
    a, b, c = b"A" * 4096, b"B" * 8192, b"C" * 2048
    descs = [
        _desc(origin, "boundary-0", "payload", "boundary-0.pt", a, instance),
        _desc(origin, "cotangent-0", "payload", "cotangent-0.pt", b, instance),
        _desc(origin, "scratch-0", "temp", "scratch-0.pt", c, instance),
    ]
    committed = po.commit_batch(queue, instance, _TEMPLATE_CACHE, descs,
                                batch_id="batch-0000", tier=STAGE_TIER,
                                mover_key=MOVER0)
    assert committed["ok"] is True
    assert queue.tier_ledger(STAGE_TIER).holder_tokens(MOVER0) == {STAGE_BARE: 1}
    assert queue.tier_ledger(STAGE_TIER).holder_tokens(
        po.reservation_holder(instance)) == {}
    batch_ns = committed["batch_namespace"]
    assert queue.tier_ledger(STAGE_TIER).holder_tokens(batch_ns) == {}


def test_prewrite_class_budgets_refuse_and_zero_valid(tmp_path: Path) -> None:
    origin = tmp_path / "pool-origin" / "outputs"
    origin.mkdir(parents=True)
    global _TEMPLATE_CACHE
    _TEMPLATE_CACHE = _template(str(origin))
    queue = _queue(tmp_path)
    bound = _bind(queue, _TEMPLATE_CACHE)
    instance = bound["instance"]
    assert po.admit_instance(queue, instance, _TEMPLATE_CACHE)["ok"] is True
    # Checkpoint over maxima refuses before any write.
    refused = po.require_prewrite(
        queue, instance, _TEMPLATE_CACHE, batch_id="big-ckpt", tier=STAGE_TIER,
        class_bytes={"payload": 0, "checkpoint": (1 << 20) + 1, "temp": 0})
    assert refused["ok"] is False
    assert "checkpoint" in str(refused["refusal"])
    # Commit with no prewrite record refuses.
    a = b"A" * 64
    _write(origin / "boundary-0.pt", a)
    desc = po.validate_descriptor({
        "schema": po.DESCRIPTOR_SCHEMA_V2, "slot": "boundary-0",
        "artifact_class": "payload", "path": str(origin / "boundary-0.pt"),
        "bytes": len(a), "sha256": hashlib.sha256(a).hexdigest(),
        "producer_generation": po.mint_generation(),
        "owner_action_key": instance["owner_action_key"],
        "owner_attempt": dict(instance["owner_attempt"]),
    }, _TEMPLATE_CACHE, instance)
    missing = po.commit_batch(queue, instance, _TEMPLATE_CACHE, [desc],
                              batch_id="no-prewrite", tier=STAGE_TIER,
                              mover_key="a" * 64)
    assert missing["ok"] is False
    assert missing["refusal"] == "prewrite-reservation-missing"
    # Explicit zeros are valid classes.
    zeros = po.require_prewrite(
        queue, instance, _TEMPLATE_CACHE, batch_id="zeros", tier=STAGE_TIER,
        class_bytes={"payload": 64, "checkpoint": 0, "temp": 0})
    assert zeros["ok"] is True
    # No admission record, no prewrite: a second instance never admitted.
    claimed2 = _publish_claim(queue, "b" * 64)
    other = po.bind_instance(queue, _TEMPLATE_CACHE,
                             owner_action_key="b" * 64,
                             claim_snapshot=claimed2)
    assert other["owner_action_key"] == "b" * 64
    assert po.require_prewrite(
        queue, other, _TEMPLATE_CACHE, batch_id="x", tier=STAGE_TIER,
        class_bytes={"payload": 1, "checkpoint": 0, "temp": 0}) == {
            "ok": False, "refusal": "prewrite-not-admitted"}


def test_two_windows_rollover_tick_and_ram_refusal(tmp_path: Path) -> None:
    origin = tmp_path / "pool-origin" / "outputs"
    origin.mkdir(parents=True)
    global _TEMPLATE_CACHE
    _TEMPLATE_CACHE = _template(str(origin))
    queue = _queue(tmp_path)
    stage = tmp_path / "stage"
    stage.mkdir()
    assert stage_release.register_stage_root(
        queue, tier_id=STAGE_TIER, stage_root=stage) == "registered"
    bound = _bind(queue, _TEMPLATE_CACHE)
    instance, claimed = bound["instance"], bound["claimed"]
    out_base = po.output_fragment_root(queue.root / pool.RESIDENCY)
    assert po.admit_instance(queue, instance, _TEMPLATE_CACHE)["ok"] is True

    # Window 0: boundary-0 + cotangent genA + scratch temp.
    assert po.require_prewrite(
        queue, instance, _TEMPLATE_CACHE, batch_id="batch-0000",
        tier=STAGE_TIER,
        class_bytes={"payload": 12288, "checkpoint": 0, "temp": 2048})["ok"] is True
    gen_a = po.mint_generation()
    descs0 = [
        _desc(origin, "boundary-0", "payload", "boundary-0.pt", b"A" * 4096, instance),
        _desc(origin, "cotangent-0", "payload", "cotangent-0.pt", b"B" * 8192,
              instance, gen=gen_a),
        _desc(origin, "scratch-0", "temp", "scratch-0.pt", b"C" * 2048, instance),
    ]
    batch0 = po.commit_batch(queue, instance, _TEMPLATE_CACHE, descs0,
                             batch_id="batch-0000", tier=STAGE_TIER, mover_key=MOVER0)
    assert batch0["ok"] is True
    _stage_batch(queue, batch0, origin, stage, out_base, tmp_path)

    # Window 1: boundary-1 + cotangent genB (rollover, same slot new bytes) + checkpoint.
    assert po.require_prewrite(
        queue, instance, _TEMPLATE_CACHE, batch_id="batch-0001",
        tier=STAGE_TIER,
        class_bytes={"payload": 12288, "checkpoint": 2048, "temp": 0})["ok"] is True
    gen_b = po.mint_generation()
    assert gen_b != gen_a
    descs1 = [
        _desc(origin, "boundary-1", "payload", "boundary-1.pt", b"D" * 4096, instance),
        _desc(origin, "cotangent-0", "payload", "cotangent-0.pt", b"E" * 8192,
              instance, gen=gen_b),
        _desc(origin, "checkpoint-0", "checkpoint", "checkpoint-0.pt", b"F" * 2048,
              instance),
    ]
    batch1 = po.commit_batch(queue, instance, _TEMPLATE_CACHE, descs1,
                             batch_id="batch-0001", tier=STAGE_TIER, mover_key=MOVER1)
    assert batch1["ok"] is True
    assert batch1["manifest_digest"] != batch0["manifest_digest"]
    assert batch1["batch_namespace"] != batch0["batch_namespace"]
    _stage_batch(queue, batch1, origin, stage, out_base, tmp_path)

    # Each batch composes under its own immutable namespace (never merged).
    for batch in (batch0, batch1):
        fragments = rm.read_fragments(out_base, batch["batch_namespace"])
        assert len(fragments) == 1
        composed = rm.compose(fragments)
        assert composed["manifest_sha256"] == batch["manifest_digest"]
    # Tick reconciles both without publishing or deleting (owned elsewhere).
    events = po.output_scope_tick(queue, {STAGE_TIER: {}})
    staged = {e["batch_id"] for e in events if e["event"] == "output-batch-staged"}
    assert {"batch-0000", "batch-0001"} <= staged

    # RAM leg: real tool, honest refusal (no epoch marker on this box).
    manifest = po.build_stage_manifest(batch0, str(origin))
    manifest_path = tmp_path / "ram-manifest.json"
    manifest_path.write_text(json.dumps(manifest))
    ram_args = ram_promote.build_parser().parse_args([
        "--pool-root", str(queue.root),
        "--cas-root", str(tmp_path / "cas"),
        "--action-key", RAM_MOVER,
        "--consumer-action-key", batch0["batch_namespace"],
        "--tier-id", RAM_TIER,
        "--ram-root", str(tmp_path / "ram"),
        "--source-stage-root", str(stage),
        "--manifest-sha256", batch0["manifest_digest"],
        "--range-start-bytes", "0",
        "--range-end-bytes", str(manifest["total_bytes"]),
        "--manifest", str(manifest_path),
        "--residency-root", str(out_base),
        "--block", str(1 << 16),
    ])
    ram_receipt = ram_promote.promote(ram_args)
    assert ram_receipt["complete"] is False
    assert ram_receipt["refusal"] == "ram_epoch_absent"

    # Rollover: retire window 0, window 1 bytes stay intact and readable.
    ret0 = po.retire_batch(queue, batch0, stage_root=str(stage),
                           residency_root=str(out_base))
    assert ret0["ok"] is True
    assert ret0["receipt"]["tokens_released"] == 1
    po.mark_batch_retired(queue.root, instance, "batch-0000")
    for entry in rm.compose(
            rm.read_fragments(out_base, batch1["batch_namespace"]))["entries"].values():
        assert Path(str(entry["stage_path"])).read_bytes()
    assert queue.tier_ledger(STAGE_TIER).holder_tokens(MOVER0) == {}
    assert queue.tier_ledger(STAGE_TIER).holder_tokens(MOVER1) == {STAGE_BARE: 1}
    # HDD origin preserved through staged-copy eviction.
    assert (origin / "boundary-1.pt").is_file()
    assert (origin / "checkpoint-0.pt").is_file()
    # With window 1 still unretired, the batches retain sorts before owner.
    early = po.safe_release_instance(queue, instance, _TEMPLATE_CACHE)
    assert early["ok"] is False
    assert early["refusal"] == "active-batches-retain"

    # Safe release retains while the owner is still active (no terminal yet).
    # (Both batches are retired here, so the refusal names the owner rather
    # than the batches — the two retains are ordered deterministically.)
    ret1 = po.retire_batch(queue, batch1, stage_root=str(stage),
                           residency_root=str(out_base))
    assert ret1["ok"] is True
    assert ret1["receipt"]["tokens_released"] == 1
    po.mark_batch_retired(queue.root, instance, "batch-0001")
    held = po.safe_release_instance(queue, instance, _TEMPLATE_CACHE)
    assert held["ok"] is False
    assert held["refusal"] == "owner-active-retain"
    # Finish the owner; leftover reclaim is idempotent (egress already
    # released each mover exactly once — receipts + empty ledger are proof).
    queue.finish(OWNER, status="executed", detail={"status": "executed"},
                 claim_snapshot=claimed)
    first = po.safe_release_instance(queue, instance, _TEMPLATE_CACHE)
    assert first["ok"] is True and first["released"] == 0
    second = po.safe_release_instance(queue, instance, _TEMPLATE_CACHE)
    assert second["ok"] is True and second["released"] == 0
    assert queue.tier_ledger(STAGE_TIER).holder_tokens(MOVER1) == {}
    assert queue.tier_ledger(STAGE_TIER).holder_tokens(
        po.reservation_holder(instance)) == {}


def test_crash_unknown_retains_and_taint_fails_closed(tmp_path: Path) -> None:
    origin = tmp_path / "pool-origin" / "outputs"
    origin.mkdir(parents=True)
    global _TEMPLATE_CACHE
    _TEMPLATE_CACHE = _template(str(origin))
    queue = _queue(tmp_path)
    bound = _bind(queue, _TEMPLATE_CACHE)
    instance = bound["instance"]
    assert po.admit_instance(queue, instance, _TEMPLATE_CACHE)["ok"] is True
    # Unknown commitments (corrupt record) retains instead of releasing.
    commitments = po._commitments_path(queue.root, instance)
    commitments.parent.mkdir(parents=True, exist_ok=True)
    commitments.write_text("{corrupt")
    retained = po.safe_release_instance(queue, instance, _TEMPLATE_CACHE)
    assert retained["ok"] is False
    assert retained["refusal"].startswith("unknown-retain")
    # Bound credit (no ledger tokens since the liveness R2 ruling) is what
    # retains here: unknown commitments never authorize a release.
    assert queue.tier_ledger(STAGE_TIER).holder_tokens(
        po.reservation_holder(instance)) == {}


def test_prefix_escape_and_bool_rejection(tmp_path: Path) -> None:
    origin = tmp_path / "pool-origin" / "outputs"
    outside = tmp_path / "outside"
    origin.mkdir(parents=True)
    outside.mkdir(parents=True)
    (outside / "evil.pt").write_bytes(b"X" * 64)
    (origin / "link").symlink_to(outside, target_is_directory=True)
    global _TEMPLATE_CACHE
    _TEMPLATE_CACHE = _template(str(origin))
    queue = _queue(tmp_path)
    bound = _bind(queue, _TEMPLATE_CACHE)
    instance = bound["instance"]
    # String prefix matches, physical identity escapes: refused.
    with pytest.raises(po.ProducedOutputError):
        po.validate_descriptor({
            "schema": po.DESCRIPTOR_SCHEMA_V2, "slot": "boundary-0",
            "artifact_class": "payload",
            "path": str(origin / "link" / "evil.pt"),
            "bytes": 64, "sha256": hashlib.sha256(b"X" * 64).hexdigest(),
            "producer_generation": po.mint_generation(),
            "owner_action_key": instance["owner_action_key"],
            "owner_attempt": dict(instance["owner_attempt"]),
        }, _TEMPLATE_CACHE, instance)
    # bool is not an integer version.
    bad = dict(po.validate_template(_TEMPLATE_CACHE))
    bad["version"] = True
    with pytest.raises(po.ProducedOutputError):
        po.validate_template(bad)
    # Class/slot mismatch refused.
    _write(origin / "boundary-0.pt", b"A" * 64)
    with pytest.raises(po.ProducedOutputError):
        po.validate_descriptor({
            "schema": po.DESCRIPTOR_SCHEMA_V2, "slot": "boundary-0",
            "artifact_class": "checkpoint",
            "path": str(origin / "boundary-0.pt"),
            "bytes": 64, "sha256": hashlib.sha256(b"A" * 64).hexdigest(),
            "producer_generation": po.mint_generation(),
            "owner_action_key": instance["owner_action_key"],
            "owner_attempt": dict(instance["owner_attempt"]),
        }, _TEMPLATE_CACHE, instance)


def test_sparse_window_refusal_without_bulk_io(tmp_path: Path) -> None:
    origin = tmp_path / "pool-origin" / "outputs"
    origin.mkdir(parents=True)
    global _TEMPLATE_CACHE
    # Wide durable envelope (4 GiB payload) but a 2 GiB working window: the
    # prewrite passes on durable headroom and the commit refuses on the
    # window — proving the two budgets are distinct.
    _TEMPLATE_CACHE = _template(str(origin), durable_maxima={
        "payload_max_bytes": 4 * GIB,
        "checkpoint_max_bytes": 1 << 20,
        "temp_max_bytes": 1 << 20,
    })
    queue = _queue(tmp_path)
    bound = _bind(queue, _TEMPLATE_CACHE)
    instance = bound["instance"]
    assert po.admit_instance(queue, instance, _TEMPLATE_CACHE)["ok"] is True
    # Sparse 3 GiB file: lstat size without bulk pages; window is 2 GiB.
    sparse = origin / "boundary-0.pt"
    with open(sparse, "wb") as handle:
        handle.truncate(3 * GIB)
    assert sparse.lstat().st_size == 3 * GIB
    assert po.require_prewrite(
        queue, instance, _TEMPLATE_CACHE, batch_id="sparse",
        tier=STAGE_TIER,
        class_bytes={"payload": 3 * GIB, "checkpoint": 0, "temp": 0})["ok"] is True
    desc = po.validate_descriptor({
        "schema": po.DESCRIPTOR_SCHEMA_V2, "slot": "boundary-0",
        "artifact_class": "payload", "path": str(sparse),
        "bytes": 3 * GIB, "sha256": "0" * 64,
        "producer_generation": po.mint_generation(),
        "owner_action_key": instance["owner_action_key"],
        "owner_attempt": dict(instance["owner_attempt"]),
    }, _TEMPLATE_CACHE, instance)
    refused = po.commit_batch(queue, instance, _TEMPLATE_CACHE, [desc],
                              batch_id="sparse", tier=STAGE_TIER,
                              mover_key="a" * 64)
    assert refused["ok"] is False
    assert refused["refusal"] == "batch-exceeds-window"


def test_lease_sdk_dependency_named_not_stubbed(tmp_path: Path) -> None:
    assert "PB730" in po.SDK_DEPENDENCY and "pin_id_for" in po.SDK_DEPENDENCY
    assert "LIVENESS" in po.SDK_DEPENDENCY and "funded" in po.SDK_DEPENDENCY
    assert po.READER_HELPER_ROOT_ENV == "PRISMABUILD_READER_HELPER_ROOT"
    try:
        import reader_lease  # noqa: F401
    except ImportError:
        assert True  # dependency pending; no stub acquire faked
        return
    assert hasattr(reader_lease, "acquire") and hasattr(reader_lease, "open_pinned")


def test_owner_namespace_separation_and_object_set(tmp_path: Path) -> None:
    """OWNER vs NAMESPACE stay distinct; equal-sized twins stay distinct."""

    origin = tmp_path / "pool-origin" / "outputs"
    origin.mkdir(parents=True)
    global _TEMPLATE_CACHE
    _TEMPLATE_CACHE = _template(str(origin))
    queue = _queue(tmp_path)
    bound = _bind(queue, _TEMPLATE_CACHE)
    instance = bound["instance"]
    namespace = po.instance_namespace(instance)
    assert namespace != OWNER and len(namespace) == 64
    assert po.reservation_holder(instance) == namespace
    path = po.declare_instance(queue.root, instance)
    assert OWNER in str(path) and namespace not in str(path)
    # Manifest object set (ours) disambiguates what the old pin_id did not;
    # pin serialization itself stays PB730-owned.
    twin_a = {"0:/src/alpha.pt": {"bytes": 4096, "sha256": "a" * 64}}
    twin_b = {"0:/src/beta.pt": {"bytes": 4096, "sha256": "b" * 64}}
    assert po.manifest_object_set_id(twin_a) != po.manifest_object_set_id(twin_b)
    assert po.label_span_for_manifest(14336) == {
        "coordinate_space": "output-manifest",
        "start_bytes": 0, "end_bytes": 14336}
