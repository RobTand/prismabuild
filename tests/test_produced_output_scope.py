"""R5 bounded fixture: template/instance/batches + coherent pin lifecycle.

Drives REAL primitives only -- queue publish/claim/finish (+validate),
tier ledger (+transfer), stage_move.move, residency_map compose/lookup,
stage_release.evict, ram_promote.promote (refusal), and -- where the
checkout provides it -- the COHERENT `prismabuild.reader_lease` package
(write_material, covers_for_keys, acquire, open_pinned, release,
injected_context; R7/R8 auto-cleanup paths never touched). On checkouts
without the pin family those tests skip with the exact dependency instead
of a stub. No model bytes, no GPU, no giant hashes. Sparse-file window
tests use truncate (no pages). No capability is announced. Pending
general-window admission returns its exact liveness dependency.
"""
from __future__ import annotations

import hashlib
import json
import os
import secrets
import threading
from pathlib import Path
import sys
import uuid

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

from prismabuild import pool, produced_output as po  # noqa: E402
from prismabuild import residency_map as rm  # noqa: E402
from prismabuild import storage_tiers  # noqa: E402
import stage_move  # noqa: E402
import stage_release  # noqa: E402
import ram_promote  # noqa: E402

try:
    from prismabuild import reader_lease as rlc  # coherent package only
    HAS_PIN = True
except ImportError:
    rlc = None  # type: ignore[assignment]
    HAS_PIN = False

STAGE_TIER = "prismabuild-stage:dl380g10"
RAM_TIER = "ram:dl380g10"
STAGE_BARE = "stage_gib"
OWNER = "c" * 64
MOVER0 = "d" * 64
MOVER1 = "e" * 64
RAM_MOVER = "f" * 64
WORKER = "fixture-worker"
GIB = storage_tiers.GIB

PIN_DEPENDENCY = ("coherent prismabuild.reader_lease (PB730 a9bb83e9, "
                  "R7/R8 auto-cleanup paths excluded)")


def _template(output_prefix: str, **overrides) -> dict:
    body = {
        "schema": po.TEMPLATE_SCHEMA_V1,
        "version": 1,
        "template_id": "r5-fixture-v1",
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


def _publish_claim(queue: pool.PoolQueue, owner: str = OWNER,
                   worker: str = WORKER) -> dict:
    queue.publish(action_key=owner, cas_root="/cas", worker_script="/w.py",
                  checkout_root="/co", resources={"cpu": 1, "mem_gb": 1})
    claimed = queue.claim(owner=worker)
    assert claimed is not None and claimed["action_key"] == owner
    assert claimed.get("claimed_by") == worker
    return claimed


def _file_broker_control(queue: pool.PoolQueue, owner: str) -> dict:
    """File a realistic broker-issued control (test-only constructor).

    Drives the EXISTING `ResourceScope` verification (`_adopt_created_scope`
    over a canned broker response enforcing the broker's own scope formula);
    only the socket round-trip is canned. Production accepts exclusively
    broker-issued controls; this helper exists so fixtures prove the same
    checks instead of forcing a fallback into production.
    """

    from prismabuild import resource_scope

    nonce = secrets.token_hex(16)
    scope = resource_scope.ResourceScope(
        owner, nonce, 1 * 1024 ** 3,
        queue.root / "telemetry" / f"{owner}.json")
    token = secrets.token_hex(32)
    unit = ("prismabuild-job"
            + hashlib.sha256((owner + nonce).encode()).hexdigest()[:32]
            + ".slice")
    scope._adopt_created_scope({
        "scope_id": unit, "token": token,
        "cgroup_path": str(Path("/sys/fs/cgroup/prismabuild.slice") / unit),
    })
    control = scope.control_record()
    path = queue.item_path(pool.CLAIMED, owner)
    live = pool._read_json(path)
    assert live is not None
    live["resource_scope"] = control
    pool._write_json_atomic(path, live)
    return control


def _bind(queue: pool.PoolQueue, template: dict, owner: str = OWNER):
    claimed = _publish_claim(queue, owner)
    control = _file_broker_control(queue, owner)
    env = {"PRISMABUILD_ACTION_KEY": owner,
           "PRISMABUILD_ACTION_NONCE": control["nonce"],
           "PRISMABUILD_ACTION_SCOPE": control["scope_id"]}
    po.declare_template(queue.root, template)
    instance = po.bind_instance(queue, template, owner_action_key=owner,
                                claim_snapshot=claimed, env=env)
    po.declare_instance(queue.root, instance)
    return {"instance": instance, "claimed": claimed, "control": control,
            "env": env}


def _write(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)


def _desc(origin: Path, template: dict, slot: str, cls: str, name: str,
          payload: bytes, instance: dict, gen: str | None = None) -> dict:
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
    }, template, instance)


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


def _file_sidecars(queue: pool.PoolQueue, batch: dict, out_base: Path):
    """Publish-time material sidecars with the coherent pin writer."""

    assert rlc is not None
    composed = rm.compose(rm.read_fragments(out_base, batch["batch_namespace"]))
    generation = rlc.mint_generation()
    entries = {}
    for key, entry in composed["entries"].items():
        staged = str(entry["stage_path"])
        file_id = rlc.stat_identity(staged)
        assert file_id is not None
        entries[str(key)] = {"stage_path": staged,
                             "bytes": int(entry["bytes"]),
                             "sha256": str(entry["sha256"]),
                             "file_id": file_id}
    rlc.write_material(out_base, consumer_action_key=batch["batch_namespace"],
                       mover_action_key=batch["mover_key"], tier_id=batch["tier"],
                       stage_root=str(composed["stage_root"]),
                       manifest_sha256=batch["manifest_digest"],
                       generation=generation, entries=entries)
    return generation, sorted(entries)


def test_binding_both_halves_declared_and_refusals(tmp_path: Path) -> None:
    origin = tmp_path / "pool-origin" / "outputs"
    origin.mkdir(parents=True)
    template = _template(str(origin))
    queue = _queue(tmp_path)
    bound = _bind(queue, template)
    instance = bound["instance"]
    assert instance["attempt_source"] == "broker-launch"
    assert instance["owner_attempt"]["nonce"] == bound["control"]["nonce"]
    claimed = bound["claimed"]
    # Undeclared template binds nothing, even with a real claim+control.
    other_template = _template(str(origin), template_id="undeclared-v1")
    with pytest.raises(po.ProducedOutputError, match="undeclared-template"):
        po.bind_instance(queue, other_template, owner_action_key=OWNER,
                         claim_snapshot=claimed, env=bound["env"])
    with pytest.raises(po.ProducedOutputError, match="no-launch-context"):
        po.bind_instance(queue, template, owner_action_key=OWNER,
                         claim_snapshot=claimed, env={})
    other = "b" * 64
    claimed_other = _publish_claim(queue, other)
    env_other = {"PRISMABUILD_ACTION_KEY": other,
                 "PRISMABUILD_ACTION_NONCE": "0" * 32,
                 "PRISMABUILD_ACTION_SCOPE": "prismabuild-job" + "0" * 32 + ".slice"}
    with pytest.raises(po.ProducedOutputError, match="no-control-context"):
        po.bind_instance(queue, template, owner_action_key=other,
                         claim_snapshot=claimed_other, env=env_other)
    control = bound["control"]
    bad_env = dict(bound["env"], PRISMABUILD_ACTION_NONCE="f" * 32)
    assert bad_env["PRISMABUILD_ACTION_NONCE"] != control["nonce"]
    with pytest.raises(po.ProducedOutputError, match="attempt-superseded"):
        po.bind_instance(queue, template, owner_action_key=OWNER,
                         claim_snapshot=claimed, env=bad_env)
    with pytest.raises(po.ProducedOutputError, match="foreign"):
        po.bind_instance(queue, template, owner_action_key=other,
                         claim_snapshot=claimed, env=env_other)
    queue.finish(OWNER, status="executed", detail={"status": "executed"},
                 claim_snapshot=claimed)
    with pytest.raises(po.ProducedOutputError, match="[Ss]tale"):
        po.bind_instance(queue, template, owner_action_key=OWNER,
                         claim_snapshot=claimed, env=bound["env"])


def test_pin_identity_surface() -> None:
    if rlc is None:
        pytest.skip(PIN_DEPENDENCY)
    assert rlc.READER_LEASE_TAG == "reader-lease-v1"
    for name in ("write_material", "covers_for_keys", "acquire",
                 "open_pinned", "release", "injected_context",
                 "stat_identity", "mint_generation"):
        assert callable(getattr(rlc, name)), name


def test_injected_context_positive_and_mismatch(tmp_path: Path) -> None:
    if rlc is None:
        pytest.skip(PIN_DEPENDENCY)
    origin = tmp_path / "pool-origin" / "outputs"
    origin.mkdir(parents=True)
    template = _template(str(origin))
    queue = _queue(tmp_path)
    bound = _bind(queue, template)
    instance = bound["instance"]
    map_probe = tmp_path / "probe.map.json"
    map_probe.write_text("{}")
    env = dict(bound["env"], PRISMABUILD_RESIDENCY_MAP=str(map_probe))
    ctx = rlc.injected_context(queue, env=env)
    assert ctx["ok"] is True, ctx
    assert ctx["ctx"]["nonce"] == instance["owner_attempt"]["nonce"]
    assert ctx["ctx"]["scope_id"] == instance["owner_attempt"]["scope_id"]
    assert ctx["ctx"]["action_key"] == OWNER
    bad = dict(env, PRISMABUILD_ACTION_NONCE="f" * 32)
    refused = rlc.injected_context(queue, env=bad)
    assert refused["ok"] is False
    assert refused["refusal"] == "attempt-superseded"


def test_admission_record_and_funded_window_gate(tmp_path: Path) -> None:
    origin = tmp_path / "pool-origin" / "outputs"
    origin.mkdir(parents=True)
    template = _template(str(origin))
    queue = _queue(tmp_path)
    bound = _bind(queue, template)
    instance = bound["instance"]
    ok = po.admit_instance(queue, instance, template)
    assert ok["ok"] is True
    assert ok["admission"]["minimum_gib"][STAGE_TIER] == 1
    assert queue.tier_ledger(STAGE_TIER).holder_tokens(
        po.reservation_holder(instance)) == {}
    pending = po.admit_funded_window(queue, instance, template,
                                     need_gib_per_tier={STAGE_TIER: 1})
    assert pending["refusal"] == "funding-primitive-pending"
    assert "liveness" in pending["dependency"].lower()
    assert pending["liveness_draft_present"] == []
    assert po.admit_funded_window(
        queue, instance, template,
        need_gib_per_tier={"prismabuild-stage:other": 1})["refusal"] == \
        "tier-not-permitted"
    assert po.admit_funded_window(
        queue, instance, template,
        need_gib_per_tier={STAGE_TIER: 0})["refusal"] == "bad-window-need"
    assert po.admit_funded_window(
        queue, instance, template,
        need_gib_per_tier={STAGE_TIER: 9})["refusal"] == "window-need-exceeds-demand"


def test_owner_demand_derivation_and_publish_rule(tmp_path: Path) -> None:
    origin = tmp_path / "pool-origin" / "outputs"
    origin.mkdir(parents=True)
    template = _template(str(origin))
    assert po.owner_demand_terms(template) == {f"stage_gib@{STAGE_TIER}": 2}
    queue = _queue(tmp_path)
    # Today's publish rule refuses tier demand without an attributing
    # residency block (#595) — the exact rule the hook proposal extends.
    with pytest.raises(pool.PoolContractError, match="residency block"):
        queue.publish(action_key="d" * 64, cas_root="/cas",
                      worker_script="/w.py", checkout_root="/co",
                      resources={"cpu": 1, "mem_gb": 1,
                                 f"stage_gib@{STAGE_TIER}": 2})


def test_minimum_transfer_no_double_charge(tmp_path: Path) -> None:
    origin = tmp_path / "pool-origin" / "outputs"
    origin.mkdir(parents=True)
    template = _template(str(origin))
    queue = _queue(tmp_path)
    bound = _bind(queue, template)
    instance = bound["instance"]
    assert po.admit_instance(queue, instance, template)["ok"] is True
    files = [str(origin / "boundary-0.pt"), str(origin / "cotangent-0.pt"),
             str(origin / "scratch-0.pt")]
    pre = po.require_prewrite(queue, instance, template,
                              batch_id="batch-0000", tier=STAGE_TIER,
                              class_bytes={"payload": 12288, "checkpoint": 0,
                                           "temp": 2048},
                              paths=files)
    assert pre["ok"] is True
    a, b, c = b"A" * 4096, b"B" * 8192, b"C" * 2048
    descs = [
        _desc(origin, template, "boundary-0", "payload", "boundary-0.pt", a, instance),
        _desc(origin, template, "cotangent-0", "payload", "cotangent-0.pt", b, instance),
        _desc(origin, template, "scratch-0", "temp", "scratch-0.pt", c, instance),
    ]
    committed = po.commit_batch(queue, instance, template, descs,
                                batch_id="batch-0000", tier=STAGE_TIER,
                                mover_key=MOVER0)
    assert committed["ok"] is True
    assert queue.tier_ledger(STAGE_TIER).holder_tokens(MOVER0) == {STAGE_BARE: 1}
    assert queue.tier_ledger(STAGE_TIER).holder_tokens(
        po.reservation_holder(instance)) == {}
    batch_ns = committed["batch_namespace"]
    assert queue.tier_ledger(STAGE_TIER).holder_tokens(batch_ns) == {}


def test_prewrite_counts_outstanding_abort_proves_absence(tmp_path: Path) -> None:
    origin = tmp_path / "pool-origin" / "outputs"
    origin.mkdir(parents=True)
    template = _template(str(origin))
    queue = _queue(tmp_path)
    bound = _bind(queue, template)
    instance = bound["instance"]
    assert po.admit_instance(queue, instance, template)["ok"] is True
    fa = [str(origin / "a.pt")]
    fb = [str(origin / "b.pt")]
    # Two outstanding 768 KiB reservations under a 1 MiB cap: the second
    # refuses while the first is outstanding.
    first = po.require_prewrite(
        queue, instance, template, batch_id="batch-a", tier=STAGE_TIER,
        class_bytes={"payload": 768 << 10, "checkpoint": 0, "temp": 0},
        paths=fa)
    assert first["ok"] is True
    second = po.require_prewrite(
        queue, instance, template, batch_id="batch-b", tier=STAGE_TIER,
        class_bytes={"payload": 768 << 10, "checkpoint": 0, "temp": 0},
        paths=fb)
    assert second["ok"] is False
    assert second["refusal"] == "prewrite-exceeds-payload-maxima"
    assert po.require_prewrite(
        queue, instance, template, batch_id="batch-a", tier=STAGE_TIER,
        class_bytes={"payload": 768 << 10, "checkpoint": 0, "temp": 0},
        paths=fa)["ok"] is True
    # Abort with a present file refuses: uncommitted is not unwritten.
    _write(origin / "a.pt", b"A" * 64)
    refused_abort = po.abort_prewrite(queue, instance, template, batch_id="batch-a")
    assert refused_abort["ok"] is False
    assert refused_abort["refusal"] == "abort-files-present-retain"
    # Safe disposal is the producer's job; absence unblocks the abort.
    (origin / "a.pt").unlink()
    assert po.abort_prewrite(queue, instance, template,
                             batch_id="batch-a") == {
        "ok": True, "batch_id": "batch-a", "aborted": True}
    assert po.require_prewrite(
        queue, instance, template, batch_id="batch-b", tier=STAGE_TIER,
        class_bytes={"payload": 768 << 10, "checkpoint": 0, "temp": 0},
        paths=fb)["ok"] is True
    assert po.abort_prewrite(queue, instance, template,
                             batch_id="missing") == {
        "ok": True, "batch_id": "missing", "aborted": False}
    # Checkpoint over maxima refuses before any write; zeros stay valid.
    refused = po.require_prewrite(
        queue, instance, template, batch_id="big-ckpt", tier=STAGE_TIER,
        class_bytes={"payload": 0, "checkpoint": (1 << 20) + 1, "temp": 0},
        paths=[str(origin / "ckpt.pt")])
    assert refused["ok"] is False
    assert "checkpoint" in str(refused["refusal"])
    zeros = po.require_prewrite(
        queue, instance, template, batch_id="zeros", tier=STAGE_TIER,
        class_bytes={"payload": 64, "checkpoint": 0, "temp": 0},
        paths=[str(origin / "z.pt")])
    assert zeros["ok"] is True
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
    }, template, instance)
    missing = po.commit_batch(queue, instance, template, [desc],
                              batch_id="no-prewrite", tier=STAGE_TIER,
                              mover_key="a" * 64)
    assert missing["ok"] is False
    assert missing["refusal"] == "prewrite-reservation-missing"
    # No admission record, no prewrite.
    claimed2 = _publish_claim(queue, "b" * 64)
    control2 = _file_broker_control(queue, "b" * 64)
    other = po.bind_instance(
        queue, template, owner_action_key="b" * 64,
        claim_snapshot=claimed2,
        env={"PRISMABUILD_ACTION_KEY": "b" * 64,
             "PRISMABUILD_ACTION_NONCE": control2["nonce"],
             "PRISMABUILD_ACTION_SCOPE": control2["scope_id"]})
    assert po.require_prewrite(
        queue, other, template, batch_id="x", tier=STAGE_TIER,
        class_bytes={"payload": 1, "checkpoint": 0, "temp": 0},
        paths=[str(origin / "x.pt")]) == {
            "ok": False, "refusal": "prewrite-not-admitted"}


def test_concurrent_prewrites_admit_exactly_one(tmp_path: Path) -> None:
    origin = tmp_path / "pool-origin" / "outputs"
    origin.mkdir(parents=True)
    template = _template(str(origin))
    queue = _queue(tmp_path)
    bound = _bind(queue, template)
    instance = bound["instance"]
    assert po.admit_instance(queue, instance, template)["ok"] is True
    results: dict[str, bool] = {}
    barrier = threading.Barrier(2)

    def attempt(suffix: str) -> None:
        barrier.wait(timeout=30)
        outcome = po.require_prewrite(
            queue, instance, template, batch_id=f"race-{suffix}",
            tier=STAGE_TIER,
            class_bytes={"payload": 768 << 10, "checkpoint": 0, "temp": 0},
            paths=[str(origin / f"race-{suffix}.pt")])
        results[suffix] = bool(outcome["ok"])

    threads = [threading.Thread(target=attempt, args=(str(i),))
               for i in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=60)
    assert sorted(results.values()) == [False, True]


def test_transfer_short_resumes_from_intent(tmp_path: Path) -> None:
    origin = tmp_path / "pool-origin" / "outputs"
    origin.mkdir(parents=True)
    template = _template(str(origin), durable_maxima={
        "payload_max_bytes": 4 * GIB,
        "checkpoint_max_bytes": 1 << 20,
        "temp_max_bytes": 1 << 20,
    })
    queue = _queue(tmp_path)
    bound = _bind(queue, template)
    instance = bound["instance"]
    assert po.admit_instance(queue, instance, template)["ok"] is True
    payload_bytes = 3 * GIB // 2
    sparse = origin / "boundary-0.pt"
    assert po.require_prewrite(
        queue, instance, template, batch_id="big", tier=STAGE_TIER,
        class_bytes={"payload": payload_bytes, "checkpoint": 0, "temp": 0},
        paths=[str(sparse)])["ok"] is True
    with open(sparse, "wb") as handle:
        handle.truncate(payload_bytes)
    desc = po.validate_descriptor({
        "schema": po.DESCRIPTOR_SCHEMA_V2, "slot": "boundary-0",
        "artifact_class": "payload", "path": str(sparse),
        "bytes": payload_bytes, "sha256": "0" * 64,
        "producer_generation": po.mint_generation(),
        "owner_action_key": instance["owner_action_key"],
        "owner_attempt": dict(instance["owner_attempt"]),
    }, template, instance)
    # Crash injection between acquire and transfer: exact tokens under the
    # batch holder, one share moved with the same rename primitive, intent
    # filed exactly as commit would file it (white-box crash instant;
    # every transition below it runs through the real APIs).
    mover = "a" * 64
    manifest = po.output_manifest_sha256([desc])
    batch_ns = po.batch_namespace(instance, "big", manifest)
    ledger = queue.tier_ledger(STAGE_TIER)
    assert ledger.acquire(batch_ns, {STAGE_BARE: 2}) is True
    po._write_funding(po._funding_dir(queue.root, instance) / "big.funding.json", {
        "batch_id": "big", "tier": STAGE_TIER, "mover_key": mover,
        "batch_gib": 2, "manifest_digest": manifest})
    held = ledger.held_dir
    names = sorted(p.name for p in (held / batch_ns).iterdir())
    assert len(names) == 2
    (held / mover).mkdir(parents=True, exist_ok=True)
    os.rename(held / batch_ns / names[0], held / mover / names[0])
    # Resume through the real commit path completes without re-acquiring.
    resumed = po.commit_batch(queue, instance, template, [desc],
                              batch_id="big", tier=STAGE_TIER, mover_key=mover)
    assert resumed["ok"] is True, resumed
    assert ledger.holder_tokens(mover).get(STAGE_BARE) == 2
    assert ledger.holder_tokens(batch_ns) == {}


def test_two_windows_rollover_tick_and_ram_refusal(tmp_path: Path) -> None:
    origin = tmp_path / "pool-origin" / "outputs"
    origin.mkdir(parents=True)
    template = _template(str(origin))
    queue = _queue(tmp_path)
    stage = tmp_path / "stage"
    stage.mkdir()
    assert stage_release.register_stage_root(
        queue, tier_id=STAGE_TIER, stage_root=stage) == "registered"
    bound = _bind(queue, template)
    instance, claimed = bound["instance"], bound["claimed"]
    out_base = po.output_fragment_root(queue.root / pool.RESIDENCY)
    assert po.admit_instance(queue, instance, template)["ok"] is True

    # Window 0: boundary-0 + cotangent genA + scratch temp.
    files0 = [str(origin / "boundary-0.pt"), str(origin / "cotangent-0.pt"),
              str(origin / "scratch-0.pt")]
    assert po.require_prewrite(
        queue, instance, template, batch_id="batch-0000",
        tier=STAGE_TIER,
        class_bytes={"payload": 12288, "checkpoint": 0, "temp": 2048},
        paths=files0)["ok"] is True
    gen_a = po.mint_generation()
    descs0 = [
        _desc(origin, template, "boundary-0", "payload", "boundary-0.pt", b"A" * 4096, instance),
        _desc(origin, template, "cotangent-0", "payload", "cotangent-0.pt", b"B" * 8192,
              instance, gen=gen_a),
        _desc(origin, template, "scratch-0", "temp", "scratch-0.pt", b"C" * 2048, instance),
    ]
    batch0 = po.commit_batch(queue, instance, template, descs0,
                             batch_id="batch-0000", tier=STAGE_TIER, mover_key=MOVER0)
    assert batch0["ok"] is True
    _stage_batch(queue, batch0, origin, stage, out_base, tmp_path)

    # Window 1: boundary-1 + cotangent genB (rollover, same slot new bytes) + checkpoint.
    files1 = [str(origin / "boundary-1.pt"), str(origin / "cotangent-0.pt"),
              str(origin / "checkpoint-0.pt")]
    assert po.require_prewrite(
        queue, instance, template, batch_id="batch-0001",
        tier=STAGE_TIER,
        class_bytes={"payload": 12288, "checkpoint": 2048, "temp": 0},
        paths=files1)["ok"] is True
    gen_b = po.mint_generation()
    assert gen_b != gen_a
    descs1 = [
        _desc(origin, template, "boundary-1", "payload", "boundary-1.pt", b"D" * 4096, instance),
        _desc(origin, template, "cotangent-0", "payload", "cotangent-0.pt", b"E" * 8192,
              instance, gen=gen_b),
        _desc(origin, template, "checkpoint-0", "checkpoint", "checkpoint-0.pt", b"F" * 2048,
              instance),
    ]
    batch1 = po.commit_batch(queue, instance, template, descs1,
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
    # Due rows are empty once staged (no second scheduler here).
    assert po.due_mover_rows(queue, instance, template) == []
    recovered = {e["batch_id"]: e["event"]
                 for e in po.recover_batches(queue, instance, template)
                 if "batch_id" in e}
    assert recovered == {"batch-0000": "output-batch-staged",
                         "batch-0001": "output-batch-staged"}

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
    early = po.safe_release_instance(queue, instance, template)
    assert early["ok"] is False
    assert early["refusal"] == "active-batches-retain"

    # Retire window 1, then the owner-active retain names the owner.
    ret1 = po.retire_batch(queue, batch1, stage_root=str(stage),
                           residency_root=str(out_base))
    assert ret1["ok"] is True
    assert ret1["receipt"]["tokens_released"] == 1
    po.mark_batch_retired(queue.root, instance, "batch-0001")
    held = po.safe_release_instance(queue, instance, template)
    assert held["ok"] is False
    assert held["refusal"] == "owner-active-retain"
    # Finish the owner; leftover reclaim is idempotent (egress already
    # released each mover exactly once — receipts + empty ledger are proof).
    queue.finish(OWNER, status="executed", detail={"status": "executed"},
                 claim_snapshot=claimed)
    sdk = rlc if HAS_PIN else None
    first = po.safe_release_instance(queue, instance, template, lease_sdk=sdk)
    assert first["ok"] is True and first["released"] == 0
    assert first["lease_proof"] == ("sdk-census-clean" if HAS_PIN
                                    else "sdk-absent-structural")
    second = po.safe_release_instance(queue, instance, template, lease_sdk=sdk)
    assert second["ok"] is True and second["released"] == 0
    assert queue.tier_ledger(STAGE_TIER).holder_tokens(MOVER1) == {}
    assert queue.tier_ledger(STAGE_TIER).holder_tokens(
        po.reservation_holder(instance)) == {}


def test_owner_superseded_retain(tmp_path: Path) -> None:
    origin = tmp_path / "pool-origin" / "outputs"
    origin.mkdir(parents=True)
    template = _template(str(origin))
    queue = _queue(tmp_path)
    bound = _bind(queue, template)
    instance, claimed = bound["instance"], bound["claimed"]
    assert po.admit_instance(queue, instance, template)["ok"] is True
    # Owner finishes, then the SAME key is resubmitted and claimed: the old
    # terminal never frees scope bound to the old attempt.
    queue.finish(OWNER, status="executed", detail={"status": "executed"},
                 claim_snapshot=claimed)
    claimed2 = _publish_claim(queue, OWNER)
    control2 = _file_broker_control(queue, OWNER)
    assert control2["nonce"] != bound["control"]["nonce"]
    retained = po.safe_release_instance(queue, instance, template)
    assert retained["ok"] is False
    assert retained["refusal"] == "owner-superseded-retain"


def test_funding_mover_live_retain(tmp_path: Path) -> None:
    origin = tmp_path / "pool-origin" / "outputs"
    origin.mkdir(parents=True)
    template = _template(str(origin))
    queue = _queue(tmp_path)
    bound = _bind(queue, template)
    instance = bound["instance"]
    assert po.admit_instance(queue, instance, template)["ok"] is True
    payload = b"A" * 4096
    paths = [str(origin / "boundary-0.pt")]
    assert po.require_prewrite(
        queue, instance, template, batch_id="batch-w", tier=STAGE_TIER,
        class_bytes={"payload": len(payload), "checkpoint": 0, "temp": 0},
        paths=paths)["ok"] is True
    # Crash-prefix funding with a LIVE mover: tokens acquired under the
    # batch holder, one share moved, intent filed; the mover row is live
    # (claimed) so the remainder must not be reclaimed blindly.
    ns = po.batch_namespace(
        instance, "batch-w",
        po.output_manifest_sha256([{
            "schema": po.DESCRIPTOR_SCHEMA_V2, "slot": "boundary-0",
            "artifact_class": "payload", "path": paths[0],
            "bytes": len(payload),
            "sha256": hashlib.sha256(payload).hexdigest(),
            "producer_generation": "0" * 32,
            "owner_action_key": OWNER,
            "owner_attempt": dict(instance["owner_attempt"])}]))
    ledger = queue.tier_ledger(STAGE_TIER)
    assert ledger.acquire(ns, {STAGE_BARE: 1}) is True
    mover = "a" * 64
    po._write_funding(po._funding_dir(queue.root, instance) / "batch-w.funding.json", {
        "batch_id": "batch-w", "tier": STAGE_TIER, "mover_key": mover,
        "batch_gib": 1, "manifest_digest": "0" * 64})
    queue.publish(action_key=mover, cas_root="/cas", worker_script="/w.py",
                  checkout_root="/co", resources={"cpu": 1, "mem_gb": 1})
    live = queue.claim(owner="mover-worker")
    assert live is not None and live["action_key"] == mover
    retained = po.safe_release_instance(queue, instance, template)
    assert retained["ok"] is False
    assert retained["refusal"] == "funding-mover-live-retain"


def test_due_rows_validate_and_lifecycle_classify(tmp_path: Path) -> None:
    origin = tmp_path / "pool-origin" / "outputs"
    origin.mkdir(parents=True)
    template = _template(str(origin))
    queue = _queue(tmp_path)
    bound = _bind(queue, template)
    instance = bound["instance"]
    assert po.admit_instance(queue, instance, template)["ok"] is True
    paths = [str(origin / "boundary-0.pt")]
    assert po.require_prewrite(
        queue, instance, template, batch_id="batch-0000", tier=STAGE_TIER,
        class_bytes={"payload": 4096, "checkpoint": 0, "temp": 0},
        paths=paths)["ok"] is True
    descs = [_desc(origin, template, "boundary-0", "payload", "boundary-0.pt",
                   b"A" * 4096, instance)]
    batch = po.commit_batch(queue, instance, template, descs,
                            batch_id="batch-0000", tier=STAGE_TIER,
                            mover_key=MOVER0)
    assert batch["ok"] is True
    # Committed but unstaged, mover absent everywhere: one frozen row whose
    # residency block passes the REAL pool validator. Sealer plumbing
    # (cas/worker/checkout) is the submitter lane's (pbrun hunk); this lane
    # owns identity/demand/residency only.
    rows = po.due_mover_rows(queue, instance, template)
    assert [(r["batch_id"], r["reason"]) for r in rows] == [
        ("batch-0000", "needs-publish")]
    assert rows[0]["action_key"] == MOVER0
    assert rows[0]["manifest_digest"] == batch["manifest_digest"]
    pool.PoolQueue.validate_residency(rows[0]["residency"], rows[0]["resources"])
    # Lifecycle classification through real queue transitions (minimal rows
    # labeled as state-machine fixtures, not movement jobs — execution proof
    # stays at the stage_move tool level in the two-window test).
    queue.publish(action_key=MOVER0, cas_root="/cas", worker_script="/w.py",
                  checkout_root="/co", resources={"cpu": 1, "mem_gb": 1},
                  max_attempts=1)
    assert pool._read_json(queue.item_path(pool.READY, MOVER0)) is not None
    assert po.due_mover_rows(queue, instance, template) == []
    assert [(e["batch_id"], e["event"]) for e in
            po.recover_batches(queue, instance, template)] == [
        ("batch-0000", "output-mover-live-wait")]
    mover_claim = queue.claim(owner="mover-worker", tags=[])
    assert mover_claim is not None and mover_claim["action_key"] == MOVER0
    queue.finish(MOVER0, status="failed", detail={"status": "failed"},
                 claim_snapshot=mover_claim)
    rows = po.due_mover_rows(queue, instance, template)
    assert [(r["batch_id"], r["reason"]) for r in rows] == [
        ("batch-0000", "retry-failed-mover")]
    recovered = po.recover_batches(queue, instance, template)
    assert [(e["batch_id"], e["event"]) for e in recovered] == [
        ("batch-0000", "output-mover-failed-retry")]


def test_pin_lifecycle_owner_split(tmp_path: Path) -> None:
    """Real coherent acquire/open/release on staged batch material."""

    if rlc is None:
        pytest.skip(PIN_DEPENDENCY)
    origin = tmp_path / "pool-origin" / "outputs"
    origin.mkdir(parents=True)
    template = _template(str(origin))
    queue = _queue(tmp_path)
    stage = tmp_path / "stage"
    stage.mkdir()
    assert stage_release.register_stage_root(
        queue, tier_id=STAGE_TIER, stage_root=stage) == "registered"
    bound = _bind(queue, template)
    instance = bound["instance"]
    out_base = po.output_fragment_root(queue.root / pool.RESIDENCY)
    assert po.admit_instance(queue, instance, template)["ok"] is True
    files = [str(origin / "boundary-0.pt"), str(origin / "cotangent-0.pt"),
             str(origin / "checkpoint-0.pt")]
    assert po.require_prewrite(
        queue, instance, template, batch_id="batch-0000", tier=STAGE_TIER,
        class_bytes={"payload": 12288, "checkpoint": 2048, "temp": 0},
        paths=files)["ok"] is True
    descs = [
        _desc(origin, template, "boundary-0", "payload", "boundary-0.pt", b"A" * 4096, instance),
        _desc(origin, template, "cotangent-0", "payload", "cotangent-0.pt", b"B" * 8192, instance),
        _desc(origin, template, "checkpoint-0", "checkpoint", "checkpoint-0.pt", b"C" * 2048, instance),
    ]
    batch = po.commit_batch(queue, instance, template, descs,
                            batch_id="batch-0000", tier=STAGE_TIER, mover_key=MOVER0)
    assert batch["ok"] is True
    _stage_batch(queue, batch, origin, stage, out_base, tmp_path)
    total = sum(d["bytes"] for d in descs)

    generation, keys = _file_sidecars(queue, batch, out_base)
    assert len(keys) == 3
    selected = rlc.covers_for_keys(
        out_base, batch["batch_namespace"], keys, tier_id=STAGE_TIER,
        manifest_sha256=batch["manifest_digest"], epoch="")
    assert selected["ok"] is True, selected
    assert selected["covers"] == [{"mover_action_key": MOVER0,
                                   "manifest_sha256": batch["manifest_digest"]}]
    attempt = dict(instance["owner_attempt"])
    token = uuid.uuid4().hex
    pinned = rlc.acquire(
        queue, consumer_action_key=batch["batch_namespace"], attempt=attempt,
        tier_id=STAGE_TIER, epoch="",
        span={"start_bytes": 0, "end_bytes": total},
        holder={"host": "fixture-host", "pid": os.getpid()},
        acquire_token=token, covers=selected["covers"],
        expected=selected["expected"], residency_root=str(out_base),
        owner_action_key=OWNER)
    assert pinned["ok"] is True, pinned
    pin_id, ref_id = pinned["pin_id"], pinned["ref_id"]
    assert (out_base / "leases" / OWNER / f"{pin_id}.lease.json").is_file()
    assert not (out_base / "leases" / batch["batch_namespace"]).exists()
    again = rlc.acquire(
        queue, consumer_action_key=batch["batch_namespace"], attempt=attempt,
        tier_id=STAGE_TIER, epoch="",
        span={"start_bytes": 0, "end_bytes": total},
        holder={"host": "fixture-host", "pid": os.getpid()},
        acquire_token=token, covers=selected["covers"],
        expected=selected["expected"], residency_root=str(out_base),
        owner_action_key=OWNER)
    assert again["ok"] is True and again["ref_id"] == ref_id
    assert again.get("duplicate") is True
    first_key = keys[0]
    fd, serving = rlc.open_pinned(queue, pinned["pin"], ref_id, first_key,
                                  residency_root=str(out_base))
    try:
        assert serving["tier_id"] == STAGE_TIER
        assert serving["pin_id"] == pin_id
        assert serving["range_ref"] == first_key
        got = b""
        while True:
            block = os.read(fd, 1 << 16)
            if not block:
                break
            got += block
        assert hashlib.sha256(got).hexdigest() == selected["expected"][first_key]["sha256"]
    finally:
        os.close(fd)
    assert rlc.release(queue, pin_id, ref_id, consumer_action_key=OWNER,
                       residency_root=str(out_base)) is True
    with pytest.raises(rlc.ReaderLeaseError):
        rlc.open_pinned(queue, pinned["pin"], ref_id, first_key,
                        residency_root=str(out_base))
    assert rlc.release(queue, pin_id, ref_id, consumer_action_key=OWNER,
                       residency_root=str(out_base)) is True
    live = rlc.acquire(
        queue, consumer_action_key=batch["batch_namespace"], attempt=attempt,
        tier_id=STAGE_TIER, epoch="",
        span={"start_bytes": 0, "end_bytes": total},
        holder={"host": "fixture-host", "pid": os.getpid()},
        acquire_token=uuid.uuid4().hex, covers=selected["covers"],
        expected=selected["expected"], residency_root=str(out_base),
        owner_action_key=OWNER)
    assert live["ok"] is True
    victim = Path(str(rm.compose(rm.read_fragments(
        out_base, batch["batch_namespace"]))["entries"][first_key]["stage_path"]))
    victim.write_bytes(b"Z" * victim.lstat().st_size)
    os.utime(victim, (victim.lstat().st_atime + 5, victim.lstat().st_mtime + 5))
    with pytest.raises(rlc.ReaderLeaseError):
        rlc.open_pinned(queue, live["pin"], live["ref_id"], first_key,
                        residency_root=str(out_base))
    assert rlc.release(queue, live["pin_id"], live["ref_id"],
                       consumer_action_key=OWNER,
                       residency_root=str(out_base)) is True
    rlc.write_retiring(str(rlc.leases_root(queue, str(out_base))),
                       consumer_action_key=batch["batch_namespace"],
                       mover_action_key=MOVER0, generation=generation)
    refused = rlc.acquire(
        queue, consumer_action_key=batch["batch_namespace"], attempt=attempt,
        tier_id=STAGE_TIER, epoch="",
        span={"start_bytes": 0, "end_bytes": total},
        holder={"host": "fixture-host", "pid": os.getpid()},
        acquire_token=uuid.uuid4().hex, covers=selected["covers"],
        expected=selected["expected"], residency_root=str(out_base),
        owner_action_key=OWNER)
    assert refused["ok"] is False and refused["refusal"] == "retiring"
    rlc.clear_retiring(str(rlc.leases_root(queue, str(out_base))),
                       consumer_action_key=batch["batch_namespace"],
                       mover_action_key=MOVER0)
    ret = po.retire_batch(queue, batch, stage_root=str(stage),
                          residency_root=str(out_base))
    assert ret["ok"] is True
    po.mark_batch_retired(queue.root, instance, "batch-0000")


def test_crash_unknown_retains_and_taint_fails_closed(tmp_path: Path) -> None:
    origin = tmp_path / "pool-origin" / "outputs"
    origin.mkdir(parents=True)
    template = _template(str(origin))
    queue = _queue(tmp_path)
    bound = _bind(queue, template)
    instance = bound["instance"]
    assert po.admit_instance(queue, instance, template)["ok"] is True
    commitments = po._commitments_path(queue.root, instance)
    commitments.parent.mkdir(parents=True, exist_ok=True)
    commitments.write_text("{corrupt")
    retained = po.safe_release_instance(queue, instance, template)
    assert retained["ok"] is False
    assert retained["refusal"].startswith("unknown-retain")
    assert queue.tier_ledger(STAGE_TIER).holder_tokens(
        po.reservation_holder(instance)) == {}


def test_prefix_escape_and_bool_rejection(tmp_path: Path) -> None:
    origin = tmp_path / "pool-origin" / "outputs"
    outside = tmp_path / "outside"
    origin.mkdir(parents=True)
    outside.mkdir(parents=True)
    (outside / "evil.pt").write_bytes(b"X" * 64)
    (origin / "link").symlink_to(outside, target_is_directory=True)
    template = _template(str(origin))
    queue = _queue(tmp_path)
    bound = _bind(queue, template)
    instance = bound["instance"]
    with pytest.raises(po.ProducedOutputError):
        po.validate_descriptor({
            "schema": po.DESCRIPTOR_SCHEMA_V2, "slot": "boundary-0",
            "artifact_class": "payload",
            "path": str(origin / "link" / "evil.pt"),
            "bytes": 64, "sha256": hashlib.sha256(b"X" * 64).hexdigest(),
            "producer_generation": po.mint_generation(),
            "owner_action_key": instance["owner_action_key"],
            "owner_attempt": dict(instance["owner_attempt"]),
        }, template, instance)
    bad = dict(po.validate_template(template))
    bad["version"] = True
    with pytest.raises(po.ProducedOutputError):
        po.validate_template(bad)
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
        }, template, instance)


def test_sparse_window_refusal_without_bulk_io(tmp_path: Path) -> None:
    origin = tmp_path / "pool-origin" / "outputs"
    origin.mkdir(parents=True)
    template = _template(str(origin), durable_maxima={
        "payload_max_bytes": 4 * GIB,
        "checkpoint_max_bytes": 1 << 20,
        "temp_max_bytes": 1 << 20,
    })
    queue = _queue(tmp_path)
    bound = _bind(queue, template)
    instance = bound["instance"]
    assert po.admit_instance(queue, instance, template)["ok"] is True
    sparse = origin / "boundary-0.pt"
    assert po.require_prewrite(
        queue, instance, template, batch_id="sparse",
        tier=STAGE_TIER,
        class_bytes={"payload": 3 * GIB, "checkpoint": 0, "temp": 0},
        paths=[str(sparse)])["ok"] is True
    with open(sparse, "wb") as handle:
        handle.truncate(3 * GIB)
    assert sparse.lstat().st_size == 3 * GIB
    desc = po.validate_descriptor({
        "schema": po.DESCRIPTOR_SCHEMA_V2, "slot": "boundary-0",
        "artifact_class": "payload", "path": str(sparse),
        "bytes": 3 * GIB, "sha256": "0" * 64,
        "producer_generation": po.mint_generation(),
        "owner_action_key": instance["owner_action_key"],
        "owner_attempt": dict(instance["owner_attempt"]),
    }, template, instance)
    refused = po.commit_batch(queue, instance, template, [desc],
                              batch_id="sparse", tier=STAGE_TIER,
                              mover_key="a" * 64)
    assert refused["ok"] is False
    assert refused["refusal"] == "batch-exceeds-window"


def test_lease_sdk_dependency_named_not_stubbed(tmp_path: Path) -> None:
    assert "PB730" in po.SDK_DEPENDENCY and "pin_id_for" in po.SDK_DEPENDENCY
    assert "LIVENESS" in po.SDK_DEPENDENCY and "funded" in po.SDK_DEPENDENCY
    assert po.READER_HELPER_ROOT_ENV == "PRISMABUILD_READER_HELPER_ROOT"


def test_owner_namespace_separation_and_object_set(tmp_path: Path) -> None:
    origin = tmp_path / "pool-origin" / "outputs"
    origin.mkdir(parents=True)
    template = _template(str(origin))
    queue = _queue(tmp_path)
    bound = _bind(queue, template)
    instance = bound["instance"]
    namespace = po.instance_namespace(instance)
    assert namespace != OWNER and len(namespace) == 64
    assert po.reservation_holder(instance) == namespace
    path = po.declare_instance(queue.root, instance)
    assert OWNER in str(path) and namespace not in str(path)
    twin_a = {"0:/src/alpha.pt": {"bytes": 4096, "sha256": "a" * 64}}
    twin_b = {"0:/src/beta.pt": {"bytes": 4096, "sha256": "b" * 64}}
    assert po.manifest_object_set_id(twin_a) != po.manifest_object_set_id(twin_b)
    assert po.label_span_for_manifest(14336) == {
        "coordinate_space": "output-manifest",
        "start_bytes": 0, "end_bytes": 14336}


def test_pin_census_taint_retains(tmp_path: Path) -> None:
    if rlc is None:
        pytest.skip(PIN_DEPENDENCY)
    origin = tmp_path / "pool-origin" / "outputs"
    origin.mkdir(parents=True)
    template = _template(str(origin))
    queue = _queue(tmp_path)
    bound = _bind(queue, template)
    instance, claimed = bound["instance"], bound["claimed"]
    out_base = po.output_fragment_root(queue.root / pool.RESIDENCY)
    assert po.admit_instance(queue, instance, template)["ok"] is True
    queue.finish(OWNER, status="executed", detail={"status": "executed"},
                 claim_snapshot=claimed)
    junk_dir = out_base / "leases" / OWNER
    junk_dir.mkdir(parents=True, exist_ok=True)
    (junk_dir / "junk.json").write_text("{}")
    tainted = po.safe_release_instance(queue, instance, template, lease_sdk=rlc)
    assert tainted["ok"] is False
    assert tainted["refusal"] == "unknown-retain: pin-census-tainted"
    (junk_dir / "junk.json").unlink()
    clean = po.safe_release_instance(queue, instance, template, lease_sdk=rlc)
    assert clean == {"ok": True, "released": 0, "lease_proof": "sdk-census-clean"}
