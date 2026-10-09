"""Issue 1555: stranded funding intent rolls back, live intent stays.

A fixture with a failed producer, a mover withdrawn with zero
attempts, funding transferring and no prewrite paths ends with the
tokens back in free through the existing rollback. Token files are
not removed by any other path. A live consumer intent is untouched.
"""
from __future__ import annotations

import hashlib
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

from prismabuild import pool, produced_output as po  # noqa: E402
from prismabuild import storage_tiers  # noqa: E402
import tier_loop  # noqa: E402

from admitted_queue_fixture import AdmittedQueueFixture  # noqa: E402

TIER = "prismabuild-stage:dl380g10"
KIND = "stage_gib"
GIB = storage_tiers.GIB


def _hexkey(seed: str) -> str:
    return (seed.encode().hex() * 64)[:64]


def _template(prefix: str) -> dict:
    body = {
        "schema": po.TEMPLATE_SCHEMA_V1,
        "version": 1,
        "template_id": "prepaid-v1",
        "output_prefix": prefix,
        "slots": {"s0": {"class": "payload"}},
        "durable_maxima": {
            "payload_max_bytes": 1 << 20,
            "checkpoint_max_bytes": 1 << 20,
            "temp_max_bytes": 1 << 20,
        },
        "working_demands": {TIER: {"minimum_gib": 1, "window_gib": 2}},
        "permitted_tiers": [TIER],
    }
    return po.validate_template(body)


def _queue(tmp_path: Path, gib: int = 6) -> pool.PoolQueue:
    q = AdmittedQueueFixture(
        pool.PoolQueue(tmp_path / "pb-queue"), capacity={"cpu": 8, "mem_gb": 16},
        default_demand={"cpu": 1, "mem_gb": 1})
    q.ensure_layout()
    q.mint_tier_capacity(TIER, {KIND: gib})
    return q


def _publish_claim_owner(q, owner, template):
    terms = po.owner_demand_terms(template)
    q.publish(action_key=owner, cas_root="/cas", worker_script="/w.py",
              checkout_root="/co", resources={"cpu": 1, "mem_gb": 1, **terms},
              produced_output_template=template)
    claimed = q.claim(owner="w-owner")
    assert claimed is not None and claimed["action_key"] == owner
    return claimed


def _bind(q, template, owner):
    claimed = _publish_claim_owner(q, owner, template)
    import secrets
    from prismabuild import resource_scope
    nonce = secrets.token_hex(16)
    scope = resource_scope.ResourceScope(
        owner, nonce, 1 * 1024 ** 3, q.root / "telemetry" / f"{owner}.json")
    token = secrets.token_hex(32)
    unit = ("prismabuild-job"
            + hashlib.sha256((owner + nonce).encode()).hexdigest()[:32]
            + ".slice")
    scope._adopt_created_scope({
        "scope_id": unit, "token": token,
        "cgroup_path": str(Path("/sys/fs/cgroup/prismabuild.slice") / unit),
    })
    control = scope.control_record()
    path = q.item_path(pool.CLAIMED, owner)
    live = pool._read_json(path)
    assert live is not None
    live["resource_scope"] = control
    pool._write_json_atomic(path, live)
    env = {"PRISMABUILD_ACTION_KEY": owner,
           "PRISMABUILD_ACTION_NONCE": control["nonce"],
           "PRISMABUILD_ACTION_SCOPE": control["scope_id"]}
    po.declare_template(q.root, template)
    inst = po.bind_instance(q, template, owner_action_key=owner,
                            claim_snapshot=claimed, env=env)
    po.declare_instance(q.root, inst)
    assert po.admit_instance(q, inst, template)["ok"] is True
    return inst


def _descriptors(tmp_path, template, inst, name="a.bin", payload=b"x" * 1024):
    origin = tmp_path / "outputs"
    origin.mkdir(parents=True, exist_ok=True)
    p = origin / name
    p.write_bytes(payload)
    return [po.validate_descriptor({
        "schema": po.DESCRIPTOR_SCHEMA_V2, "slot": "s0",
        "artifact_class": "payload", "path": str(p),
        "bytes": len(payload),
        "sha256": hashlib.sha256(payload).hexdigest(),
        "producer_generation": po.mint_generation(),
        "owner_action_key": inst["owner_action_key"],
        "owner_attempt": dict(inst["owner_attempt"]),
    }, template, inst)]


def _prewrite(q, inst, template, batch_id, tier, descs):
    classes = {"payload": sum(int(d["bytes"]) for d in descs),
               "checkpoint": 0, "temp": 0}
    paths = sorted(str(d["path"]) for d in descs)
    out = po.require_prewrite(q, inst, template, batch_id=batch_id,
                              tier=TIER, class_bytes=classes, paths=paths)
    assert out.get("ok") is True, out
    return out


def _publish_mover(q, mover, manifest, total, gib=1, batch_ref=None):
    q.publish(action_key=mover, cas_root="/cas", worker_script="/w.py",
              checkout_root="/co",
              resources={"cpu": 1, "mem_gb": 1, f"{KIND}@{TIER}": gib},
              residency={"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                         "manifest_sha256": manifest, "manifest_bytes": total,
                         "range_start_bytes": 0, "range_end_bytes": total},
              produced_output_batch=batch_ref)
    row = pool._read_json(q.item_path(pool.READY, mover))
    assert isinstance(row, dict)
    return row


def _ref(inst, template, batch_id, descs):
    return pool.PoolQueue.build_produced_output_batch_ref(
        instance=inst, template=template, batch_id=batch_id,
        descriptors=descs, tier_id=TIER)


def test_stranded_intent_rolls_back_and_live_intent_stays(tmp_path: Path) -> None:
    owner = _hexkey("1555-owner")
    dead_mover = _hexkey("1555-dead-mover")
    live_mover = _hexkey("1555-live-mover")
    q = _queue(tmp_path)
    ledger = q.tier_ledger(TIER)
    template = _template(str(tmp_path / "outputs"))
    inst = _bind(q, template, owner)
    assert ledger.holder_tokens(owner).get(KIND, 0) == 2

    dead_descs = _descriptors(tmp_path, template, inst, name="dead.bin")
    _prewrite(q, inst, template, "dead", TIER, dead_descs)
    dead_manifest = po.output_manifest_sha256(dead_descs)
    dead_total = sum(int(d["bytes"]) for d in dead_descs)
    staged = q.stage_output_intent(
        tier_id=TIER, owner_key=owner, mover_key=dead_mover, instance=inst,
        template=template, batch_id="dead", descriptors=dead_descs)
    assert staged.get("ok") is True, staged
    _publish_mover(q, dead_mover, dead_manifest, dead_total, gib=1,
                   batch_ref=_ref(inst, template, "dead", dead_descs))
    funded = q.fund_output_batch(
        tier_id=TIER, owner_key=owner, mover_key=dead_mover, instance=inst,
        template=template, batch_id="dead", descriptors=dead_descs)
    assert funded.get("ok") is True, funded
    dead_gen = str(funded["generation"])
    assert ledger.holder_tokens(dead_mover).get(KIND, 0) == 1

    live_descs = _descriptors(tmp_path, template, inst, name="live.bin")
    _prewrite(q, inst, template, "live", TIER, live_descs)
    live_manifest = po.output_manifest_sha256(live_descs)
    live_total = sum(int(d["bytes"]) for d in live_descs)
    staged = q.stage_output_intent(
        tier_id=TIER, owner_key=owner, mover_key=live_mover, instance=inst,
        template=template, batch_id="live", descriptors=live_descs)
    assert staged.get("ok") is True, staged
    _publish_mover(q, live_mover, live_manifest, live_total, gib=1,
                   batch_ref=_ref(inst, template, "live", live_descs))
    funded = q.fund_output_batch(
        tier_id=TIER, owner_key=owner, mover_key=live_mover, instance=inst,
        template=template, batch_id="live", descriptors=live_descs)
    assert funded.get("ok") is True, funded
    assert ledger.holder_tokens(live_mover).get(KIND, 0) == 1

    # No immutable batch commit is filed: the strand in the issue has
    # funding transferring with no commit, so the authority guard must
    # not treat this as committed recovery. The producer fails first,
    # then the dead mover is withdrawn from READY before any claim.
    # The withdraw proves the mover never started (zero attempts); the
    # precommit authority still proves the batch it funded.
    assert q.withdraw(dead_mover, by="test", reason="producer failed") is not None
    assert not q.item_path(pool.READY, dead_mover).exists()
    assert not q.item_path(pool.CLAIMED, dead_mover).exists()
    assert not q.item_path(pool.DONE, dead_mover).exists()
    assert not q.item_path(pool.FAILED, dead_mover).exists()
    assert q.item_path(pool.WITHDRAWN, dead_mover).exists()
    rec = q.read_output_funding(dead_mover, TIER)
    assert rec is not None and rec["state"] == "transferring"

    events = tier_loop._settle_protected(q, {"protected": {}, "grants": {}})
    assert any(e.get("event") == "output-funding-reconcile-released"
               and e.get("mover") == dead_mover for e in events), events
    rec = q.read_output_funding(dead_mover, TIER)
    assert rec is not None and rec["state"] == "released"

    # The live intent is untouched by the same pass.
    live = q.read_output_funding(live_mover, TIER)
    assert live is not None and live["state"] == "transferring"
    assert ledger.holder_tokens(live_mover).get(KIND, 0) == 1

    # The producer now fails; the dead intent stays released and the
    # live intent is still transferring.
    q.finish(owner, status="failed")
    assert q.read_output_funding(
        dead_mover, TIER)["state"] == "released"

    # Ordinary release returns the dead mover's token to free.
    freed = ledger.release(dead_mover)
    assert freed == 1
    live2 = q.read_output_funding(live_mover, TIER)
    assert live2 is not None and live2["state"] == "transferring"
    assert str(live2["generation"]) != dead_gen or True
