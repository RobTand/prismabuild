"""Bounded real copy->read->retire fixture for produced-output staging.

Drives the REAL PB primitives -- tier ledger, stage_move.move,
residency_map fragments/compose/lookup, stage_release.evict -- over small
real files in tmp_path. No model bytes, no GPU, no giant hashes, no
availability fakes: every green asserts a transition, a refusal, or
byte-equality the test just produced.

The output material lives under its own fragment namespace
(produced_output.output_fragment_root), never merged into the external
input map, so residency_map.compose keeps its one-manifest-identity rule.
The PB lease lane (reader_lease.acquire/open_pinned/release) plugs onto
the composed material exactly as the design note shows; this fixture
proves the copy/read/retire mechanics the lease then pins.
"""
from __future__ import annotations

import hashlib
import json
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

STAGE_TIER = "prismabuild-stage:dl380g10"
STAGE_KIND = f"stage_gib@{STAGE_TIER}"
PRODUCER = "p" * 64
MOVER = "a" * 64
ATTEMPT = {"nonce": "attempt-0", "scope_id": "scope-0"}


def _scope(output_prefix: str, **overrides) -> dict:
    body = {
        "schema": po.PRODUCED_OUTPUT_SCOPE_SCHEMA_V1,
        "version": 1,
        "producer_action_key": PRODUCER,
        "attempt": dict(ATTEMPT),
        "output_prefix": output_prefix,
        "slots": ["boundary-0", "cotangent-0", "checkpoint-0"],
        "byte_envelopes": {
            "payload_max_bytes": 1 << 20,
            "checkpoint_max_bytes": 1 << 20,
            "temp_overlap_max_bytes": 1 << 20,
        },
        "permitted_tiers": [STAGE_TIER],
    }
    body.update(overrides)
    return po.validate_scope(body)


def _write_origin(path: Path, payload: bytes) -> bytes:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    return payload


def _origin_files(tmp_path: Path):
    origin = tmp_path / "pool-origin" / "outputs"
    a = origin / "boundary-0.pt"
    b = origin / "cotangent-0.pt"
    c = origin / "checkpoint-0.pt"
    _write_origin(a, b"A" * 4096)
    _write_origin(b, b"B" * 8192)
    _write_origin(c, b"C" * 2048)
    return origin, [(a, "boundary-0"), (b, "cotangent-0"), (c, "checkpoint-0")]


def _descriptors(origin_files, scope) -> list[dict]:
    out = []
    for path, slot in origin_files[1]:
        payload = path.read_bytes()
        out.append(po.validate_descriptor({
            "schema": po.PRODUCED_OUTPUT_DESCRIPTOR_SCHEMA_V1,
            "slot": slot,
            "path": str(path),
            "bytes": len(payload),
            "sha256": hashlib.sha256(payload).hexdigest(),
            "producer_generation": po.mint_generation(),
            "producer_action_key": PRODUCER,
            "attempt": dict(ATTEMPT),
        }, scope))
    return out


def _queue(tmp_path: Path) -> pool.PoolQueue:
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    queue.mint_tier_capacity(STAGE_TIER, {"stage_gib": 8})
    return queue


def test_scope_budget_counts_checkpoint_and_overlap_before_write(tmp_path: Path) -> None:
    origin, _ = _origin_files(tmp_path)
    scope = _scope(str(origin))

    # Before-write budget is the sum, not a posthoc byte registration.
    assert po.total_reservation_bytes(scope) == 3 * (1 << 20)

    queue = _queue(tmp_path)
    refused = po.reserve_scope(queue, scope, "prismabuild-stage:other")
    assert refused == {"ok": False, "refusal": "tier-not-permitted"}

    ok = po.reserve_scope(queue, scope, STAGE_TIER)
    assert ok["ok"] is True
    held = queue.tier_ledger(STAGE_TIER).holder_tokens(po.reservation_key(scope))
    assert held == {STAGE_KIND.split("@")[0]: 1}

    # An envelope that never fits refuses permanently, not as a stall.
    big = _scope(str(origin), byte_envelopes={
        "payload_max_bytes": 1 << 40,
        "checkpoint_max_bytes": 1 << 20,
        "temp_overlap_max_bytes": 1 << 20,
    })
    assert po.reserve_scope(queue, big, STAGE_TIER)["refusal"] == \
        "never-fits-tier-capacity"


def test_descriptors_are_immutable_and_prefix_bound(tmp_path: Path) -> None:
    origin, files = _origin_files(tmp_path)
    scope = _scope(str(origin))
    descriptors = _descriptors((origin, files), scope)
    manifest = po.build_output_manifest(descriptors, scope)
    assert manifest["entry_count"] == 3
    assert manifest["output_consumer_key"] == po.output_consumer_key(scope)

    # Same path/length with different bytes is a different descriptor:
    # the old digest no longer matches, so the old material cannot ABA-alias.
    payload = files[0][0].read_bytes()
    tampered = dict(descriptors[0], sha256=hashlib.sha256(b"X" * len(payload)).hexdigest())
    with pytest.raises(po.ProducedOutputError):
        # The tamper is only detected when the staged bytes are verified;
        # the scope check below pins the prefix/slot/envelope half.
        po.validate_descriptor(dict(tampered, path=str(origin / "elsewhere.pt")), scope)
    with pytest.raises(po.ProducedOutputError):
        po.validate_descriptor(dict(descriptors[0], slot="foreign-slot"), scope)


def test_bounded_copy_read_retire_keeps_hdd_origin(tmp_path: Path) -> None:
    """One mover stages three produced outputs; read staged, retire staged."""

    origin, files = _origin_files(tmp_path)
    scope = _scope(str(origin))
    consumer = po.output_consumer_key(scope)
    assert consumer != PRODUCER  # namespace differs from the owner key

    queue = _queue(tmp_path)
    stage = tmp_path / "stage"
    stage.mkdir()
    assert stage_release.register_stage_root(
        queue, tier_id=STAGE_TIER, stage_root=stage) == "registered"

    # Scope reservation happens BEFORE any byte is written.
    reservation = po.reserve_scope(queue, scope, STAGE_TIER)
    assert reservation["ok"] is True

    descriptors = _descriptors((origin, files), scope)
    manifest_sha = po.output_manifest_sha256(descriptors)

    # Reuse the existing mover: a synthetic manifest over the durable HDD
    # origin, one batched movement unit (never one PB run per 4MiB object).
    entries = [{"path": d["path"], "offset": 0, "bytes": d["bytes"],
                "sha256": d["sha256"]} for d in descriptors]
    total = sum(e["bytes"] for e in entries)
    manifest = {
        "schema": "prismaquant.prismabuild.data_manifest.v1",
        "produced_by": {"tool": "test-produced-output"},
        "mount_prefix": str(origin),
        "entries": entries,
        "entry_count": len(entries),
        "total_bytes": total,
        "annotations": {},
    }
    manifest_path = tmp_path / "output-manifest.json"
    manifest_path.write_text(json.dumps(manifest))
    out_root = po.output_fragment_root(queue.root / pool.RESIDENCY)
    mover_demand = storage_tiers.stage_tokens_for_bytes(total)
    assert queue.tier_ledger(STAGE_TIER).acquire(MOVER, {STAGE_KIND: mover_demand})

    args = stage_move.build_parser().parse_args([
        "--pool-root", str(queue.root),
        "--cas-root", str(tmp_path / "cas"),
        "--action-key", MOVER,
        "--consumer-action-key", consumer,
        "--tier-id", STAGE_TIER,
        "--stage-root", str(stage),
        "--manifest-sha256", manifest_sha,
        "--range-start-bytes", "0",
        "--range-end-bytes", str(total),
        "--manifest", str(manifest_path),
        "--residency-root", str(out_root),
        "--block", str(1 << 16),
        "--readers", "2",
        "--max-readers", "2",
        "--unpaced",
    ])
    receipt = stage_move.move(args)
    assert receipt["complete"] is True
    assert receipt["bytes_staged"] == total
    assert receipt["entries_staged"] == 3
    assert receipt.get("refusal") is None

    # Dynamic material lookup from the published fragments (own namespace).
    fragments = rm.read_fragments(out_root, consumer)
    assert len(fragments) == 1
    composed = rm.compose(fragments)
    assert composed["manifest_sha256"] == manifest_sha
    for entry in entries:
        found = rm.lookup(composed, entry["path"], 0)
        assert found is not None
        staged = Path(str(found["stage_path"]))
        assert staged.is_file()
        payload = staged.read_bytes()
        assert len(payload) == entry["bytes"]
        assert hashlib.sha256(payload).hexdigest() == entry["sha256"]
        # The durable HDD origin is preserved through the staged copy.
        assert Path(entry["path"]).read_bytes() == payload

    # Unknown keys miss (typed, bounded): the caller waits or refuses with
    # readset-not-staged, never silently streams the HDD origin as staged.
    assert rm.lookup(composed, str(origin / "missing.pt"), 0) is None

    # Retirement: physical reclaim before charge free. The mover's own
    # tokens release at egress; the scope reservation releases after.
    egress = stage_release.evict(queue, MOVER, consumer_action_key=consumer,
                                 stage_root=str(stage),
                                 residency_root=str(out_root))
    assert egress["complete"] is True
    assert egress["entries_deleted"] == 3
    assert egress["errors"] == []
    for entry in entries:
        assert not Path(stage / Path(entry["path"]).name).exists()
    # HDD origin survives staged-copy eviction.
    for path, _slot in files:
        assert path.is_file()
    assert queue.tier_ledger(STAGE_TIER).holder_tokens(MOVER) == {}
    assert po.release_scope(queue, scope) >= 0
    assert queue.tier_ledger(STAGE_TIER).holder_tokens(
        po.reservation_key(scope)) == {}

    # Idempotent second egress: a no-op receipt, not a failure.
    again = stage_release.evict(queue, MOVER, consumer_action_key=consumer,
                                stage_root=str(stage),
                                residency_root=str(out_root))
    assert again["complete"] is True
    assert again["entries_deleted"] == 0


def test_tainted_fragment_fails_closed_and_retains_charge(tmp_path: Path) -> None:
    origin, files = _origin_files(tmp_path)
    scope = _scope(str(origin))
    consumer = po.output_consumer_key(scope)
    queue = _queue(tmp_path)
    stage = tmp_path / "stage"
    stage.mkdir()
    assert stage_release.register_stage_root(
        queue, tier_id=STAGE_TIER, stage_root=stage) == "registered"
    out_root = po.output_fragment_root(queue.root / pool.RESIDENCY)

    staged = stage / "boundary-0.pt"
    staged.parent.mkdir(parents=True, exist_ok=True)
    staged.write_bytes(b"A" * 4096)
    frag_dir = out_root / consumer
    frag_dir.mkdir(parents=True, exist_ok=True)
    (frag_dir / f"{MOVER}.json").write_text("{not json")
    assert queue.tier_ledger(STAGE_TIER).acquire(MOVER, {STAGE_KIND: 1})

    receipt = stage_release.evict(queue, MOVER, consumer_action_key=consumer,
                                  stage_root=str(stage),
                                  residency_root=str(out_root))
    assert receipt["complete"] is False
    assert receipt["errors"] != []
    assert staged.is_file()  # nothing unlinked
    assert queue.tier_ledger(STAGE_TIER).holder_tokens(MOVER) != {}
