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

import pytest

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
              produced_output_template=template, max_attempts=1)
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
    }, template, inst)], p


def _prewrite(q, inst, template, batch_id, tier, descs):
    classes = {"payload": sum(int(d["bytes"]) for d in descs),
               "checkpoint": 0, "temp": 0}
    paths = sorted(str(d["path"]) for d in descs)
    out = po.require_prewrite(q, inst, template, batch_id=batch_id,
                              tier=TIER, class_bytes=classes, paths=paths)
    assert out.get("ok") is True, out
    return out


def _one_batch(q, tmp_path, template, inst, owner, mover, batch_id, name):
    """One staged and funded output batch through the pool."""
    ledger = q.tier_ledger(TIER)
    descs, path = _descriptors(tmp_path, template, inst, name=name)
    _prewrite(q, inst, template, batch_id, TIER, descs)
    manifest = po.output_manifest_sha256(descs)
    total = sum(int(d["bytes"]) for d in descs)
    staged = q.stage_output_intent(
        tier_id=TIER, owner_key=owner, mover_key=mover, instance=inst,
        template=template, batch_id=batch_id, descriptors=descs)
    assert staged.get("ok") is True, staged
    _publish_mover(q, mover, manifest, total, gib=1,
                   batch_ref=_ref(inst, template, batch_id, descs))
    funded = q.fund_output_batch(
        tier_id=TIER, owner_key=owner, mover_key=mover, instance=inst,
        template=template, batch_id=batch_id, descriptors=descs)
    assert funded.get("ok") is True, funded
    assert ledger.holder_tokens(mover).get(KIND, 0) == 1
    return path, descs


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


def _settle(q):
    return tier_loop._settle_protected(q, {"protected": {}, "grants": {}})


def test_stranded_intent_rolls_back_after_producer_failure(tmp_path: Path) -> None:
    """A failed producer and absent prewrite paths free the tokens."""
    owner = _hexkey("1555-owner")
    dead_mover = _hexkey("1555-dead-mover")
    live_owner = _hexkey("1555-live-owner")
    live_mover = _hexkey("1555-live-mover")
    q = _queue(tmp_path)
    ledger = q.tier_ledger(TIER)
    template = _template(str(tmp_path / "outputs"))
    inst = _bind(q, template, owner)
    live_inst = _bind(q, template, live_owner)
    assert ledger.holder_tokens(owner).get(KIND, 0) == 2
    assert ledger.holder_tokens(live_owner).get(KIND, 0) == 2

    dead_path, _dead_descs = _one_batch(q, tmp_path, template, inst, owner,
                                        dead_mover, "dead", "dead.bin")
    _one_batch(q, tmp_path, template, live_inst, live_owner, live_mover,
               "live", "live.bin")

    # No immutable batch commit is filed: the strand in the issue has
    # funding transferring with no commit. An earlier pass runs while
    # the producer lives, and retains both intents.
    before = _settle(q)
    assert not any(e.get("event") == "output-funding-reconcile-released"
                   for e in before), before
    assert q.read_output_funding(
        dead_mover, TIER)["state"] == "transferring"

    # The producer fails, the dead mover leaves READY before any claim,
    # and its prewrite paths are absent: the mover never laid bytes.
    q.finish(owner, status="failed", detail={"returncode": 1})
    assert po._producer_attempt_state(q, inst) == "dead"
    assert q.withdraw(dead_mover, by="test",
                      reason="producer failed") is not None
    assert not q.item_path(pool.READY, dead_mover).exists()
    assert not q.item_path(pool.CLAIMED, dead_mover).exists()
    assert not q.item_path(pool.DONE, dead_mover).exists()
    assert not q.item_path(pool.FAILED, dead_mover).exists()
    assert q.item_path(pool.WITHDRAWN, dead_mover).exists()
    dead_path.unlink()
    assert q.read_output_funding(
        dead_mover, TIER)["state"] == "transferring"

    events = _settle(q)
    released = [e for e in events
                if e.get("event") == "output-funding-reconcile-released"
                and e.get("mover") == dead_mover]
    assert len(released) == 1, events
    assert q.read_output_funding(
        dead_mover, TIER)["state"] == "released"
    freed = [e for e in events
             if e.get("event") == "output-funding-tokens-released"
             and e.get("mover") == dead_mover]
    assert len(freed) == 1, events
    assert freed[0].get("released_gib") == 1, events
    assert ledger.holder_tokens(dead_mover).get(KIND, 0) == 0
    # Minted 6: the failed owner freed its grant, the live owner keeps
    # 1, the live mover holds 1, and the freed token returns to free.
    assert ledger.holder_tokens(owner).get(KIND, 0) == 0
    assert ledger.available().get(KIND, 0) == 4

    # The live consumer's intent on another producer is untouched.
    live = q.read_output_funding(live_mover, TIER)
    assert live is not None and live["state"] == "transferring"
    assert ledger.holder_tokens(live_mover).get(KIND, 0) == 1


def test_republished_mover_keeps_its_reservation(tmp_path: Path) -> None:
    """A republished row after withdrawal keeps its tokens."""
    owner = _hexkey("1555-owner-repub")
    mover = _hexkey("1555-mover-repub")
    q = _queue(tmp_path)
    ledger = q.tier_ledger(TIER)
    template = _template(str(tmp_path / "outputs"))
    inst = _bind(q, template, owner)

    path, descs = _one_batch(q, tmp_path, template, inst, owner, mover,
                             "repub", "repub.bin")
    q.finish(owner, status="failed", detail={"returncode": 1})
    assert po._producer_attempt_state(q, inst) == "dead"
    assert q.withdraw(mover, by="test", reason="producer failed") is not None
    path.unlink()

    # The key is published again before the repair reads it: the same
    # batch and manifest keep the publication gate open, the fresh
    # READY census refuses the repair, and the new row keeps its
    # reservation.
    manifest = po.output_manifest_sha256(descs)
    total = sum(int(d["bytes"]) for d in descs)
    _publish_mover(q, mover, manifest, total, gib=1,
                   batch_ref=_ref(inst, template, "repub", descs))
    events = _settle(q)
    assert not any(e.get("event") == "output-funding-reconcile-released"
                   and e.get("mover") == mover for e in events), events
    assert q.read_output_funding(mover, TIER)["state"] == "transferring"
    assert ledger.holder_tokens(mover).get(KIND, 0) == 1


def test_claimed_mover_keeps_its_reservation(tmp_path: Path) -> None:
    """A claimed row before the repair keeps its tokens."""
    owner = _hexkey("1555-owner-claim")
    mover = _hexkey("1555-mover-claim")
    q = _queue(tmp_path)
    ledger = q.tier_ledger(TIER)
    template = _template(str(tmp_path / "outputs"))
    inst = _bind(q, template, owner)

    path, _ = _one_batch(q, tmp_path, template, inst, owner, mover,
                         "claim", "claim.bin")
    # A funded-but-uncommitted mover never claims through the gate
    # (claim refuses output_funding_pending), so the race row is filed
    # direct: the repair must read any CLAIMED row as live.
    ready = pool._read_json(q.item_path(pool.READY, mover))
    assert isinstance(ready, dict)
    pool._write_json_atomic(q.item_path(pool.CLAIMED, mover), ready)
    q.item_path(pool.READY, mover).unlink()
    assert pool._read_json(q.item_path(pool.CLAIMED, mover)) is not None
    q.finish(owner, status="failed", detail={"returncode": 1})
    assert po._producer_attempt_state(q, inst) == "dead"
    # The repair reads a CLAIMED row and refuses, even with the prewrite
    # paths absent: the mover may still hold the key.
    path.unlink()
    events = _settle(q)
    assert not any(e.get("event") == "output-funding-reconcile-released"
                   and e.get("mover") == mover for e in events), events
    assert q.read_output_funding(mover, TIER)["state"] == "transferring"
    assert ledger.holder_tokens(mover).get(KIND, 0) == 1


def test_present_prewrite_path_keeps_its_charge(tmp_path: Path) -> None:
    """A failed producer with a present path keeps tokens."""
    owner = _hexkey("1555-owner-present")
    mover = _hexkey("1555-mover-present")
    q = _queue(tmp_path)
    ledger = q.tier_ledger(TIER)
    template = _template(str(tmp_path / "outputs"))
    inst = _bind(q, template, owner)

    path, _ = _one_batch(q, tmp_path, template, inst, owner, mover,
                         "present", "present.bin")
    q.finish(owner, status="failed", detail={"returncode": 1})
    assert po._producer_attempt_state(q, inst) == "dead"
    assert q.withdraw(mover, by="test", reason="producer failed") is not None
    assert path.exists()
    events = _settle(q)
    assert not any(e.get("event") == "output-funding-reconcile-released"
                   and e.get("mover") == mover for e in events), events
    assert q.read_output_funding(mover, TIER)["state"] == "transferring"
    assert ledger.holder_tokens(mover).get(KIND, 0) == 1


def test_partial_output_keeps_its_charge(tmp_path: Path) -> None:
    """A withdrawn mover with staged bytes keeps tokens (pool.py:15970)."""
    owner = _hexkey("1555-owner-partial")
    mover = _hexkey("1555-mover-partial")
    q = _queue(tmp_path)
    ledger = q.tier_ledger(TIER)
    template = _template(str(tmp_path / "outputs"))
    inst = _bind(q, template, owner)

    descs, _ = _descriptors(tmp_path, template, inst, name="partial.bin")
    _prewrite(q, inst, template, "partial", TIER, descs)
    manifest = po.output_manifest_sha256(descs)
    total = sum(int(d["bytes"]) for d in descs)
    staged = q.stage_output_intent(
        tier_id=TIER, owner_key=owner, mover_key=mover, instance=inst,
        template=template, batch_id="partial", descriptors=descs)
    assert staged.get("ok") is True, staged
    _publish_mover(q, mover, manifest, total, gib=1,
                   batch_ref=_ref(inst, template, "partial", descs))
    funded = q.fund_output_batch(
        tier_id=TIER, owner_key=owner, mover_key=mover, instance=inst,
        template=template, batch_id="partial", descriptors=descs)
    assert funded.get("ok") is True, funded
    assert ledger.holder_tokens(mover).get(KIND, 0) == 1

    # The mover staged part of its batch, then was withdrawn: bytes
    # may sit on the stage, so the reconcile must not free them.
    q.record_move(mover, {"tier_id": TIER, "complete": False,
                          "bytes_staged": total,
                          "range_start_bytes": 0,
                          "range_end_bytes": total,
                          "manifest_sha256": manifest})
    assert q.withdraw(mover, by="test", reason="stopped") is not None
    events = _settle(q)
    assert ledger.holder_tokens(mover).get(KIND, 0) == 1
    assert not any(e.get("event") == "output-funding-tokens-released"
                   and e.get("mover") == mover for e in events), events


def _cycle(q, tmp_path, receipts):
    def discover(**_kwargs):
        return {TIER: {
            "schema": storage_tiers.TIER_RECORD_SCHEMA_V1,
            "tier_id": TIER, "host": "dl380g10", "tier": "stage",
            "mountpoint": str(tmp_path / "stage"),
            "capacity_bytes": 6 * GIB,
            "capacity_source": storage_tiers.WRITABLE_CAPACITY_SOURCE,
        }}

    return tier_loop.cycle(
        q, host="dl380g10", source_pool="storage_pool",
        receipts=receipts, discover=discover)


def _stranded_with_live_intent(tmp_path):
    q = _queue(tmp_path)
    owner = _hexkey("fault-owner")
    mover = _hexkey("fault-mover")
    live_owner = _hexkey("fault-live-owner")
    live_mover = _hexkey("fault-live-mover")
    template = _template(str(tmp_path / "outputs"))
    inst = _bind(q, template, owner)
    live_inst = _bind(q, template, live_owner)
    path, descs = _one_batch(
        q, tmp_path, template, inst, owner, mover, "fault", "fault.bin")
    _one_batch(q, tmp_path, template, live_inst, live_owner, live_mover,
               "live", "live.bin")
    receipts = tier_loop.ReceiptCache()
    _cycle(q, tmp_path, receipts)
    assert q.read_output_funding(mover, TIER)["state"] == "transferring"
    q.finish(owner, status="failed", detail={"returncode": 1})
    assert q.withdraw(mover, by="test", reason="producer failed") is not None
    path.unlink()
    return q, mover, live_mover, receipts, inst, template, descs


@pytest.mark.parametrize("restart", [False, True])
@pytest.mark.parametrize("fault", ["token-release", "funding-write"])
def test_release_fault_recovers_on_next_cycle(tmp_path, monkeypatch, fault, restart):
    """Each fault leaves a retryable state, also after a loop restart."""
    q, mover, live_mover, receipts, *_ = _stranded_with_live_intent(tmp_path)
    ledger = q.tier_ledger(TIER)
    live_before = q.read_output_funding(live_mover, TIER)
    with monkeypatch.context() as patch:
        if fault == "token-release":
            release = pool.ResourceLedger.release

            def fail_release(self, key):
                if key == mover:
                    raise OSError("injected token release failure")
                return release(self, key)

            patch.setattr(pool.ResourceLedger, "release", fail_release)
        else:
            write = pool._write_json_atomic

            def fail_funding_write(path, body, **kwargs):
                if (path == q.funding_output_path(mover, TIER)
                        and body.get("state") == "released"):
                    raise OSError("injected funding write failure")
                return write(path, body, **kwargs)

            patch.setattr(pool, "_write_json_atomic", fail_funding_write)
        _cycle(q, tmp_path, receipts)
        assert q.read_output_funding(mover, TIER)["state"] == "transferring"
        assert ledger.holder_tokens(mover).get(KIND, 0) == (
            1 if fault == "token-release" else 0)
        assert q.read_output_funding(live_mover, TIER) == live_before
        assert ledger.holder_tokens(live_mover).get(KIND, 0) == 1

    if restart:
        q = pool.PoolQueue(q.root)
        receipts = tier_loop.ReceiptCache()
    _cycle(q, tmp_path, receipts)
    assert q.read_output_funding(mover, TIER)["state"] == "released"
    assert ledger.holder_tokens(mover).get(KIND, 0) == 0
    assert ledger.available().get(KIND, 0) == 4
    assert q.read_output_funding(live_mover, TIER) == live_before
    assert ledger.holder_tokens(live_mover).get(KIND, 0) == 1
    _cycle(q, tmp_path, receipts)
    assert ledger.available().get(KIND, 0) == 4


def test_released_record_with_held_tokens_recovers_after_restart(tmp_path):
    """Repair the unsafe state that the previous release order created."""
    q, mover, live_mover, _, *_ = _stranded_with_live_intent(tmp_path)
    assert q.release_output_funding(mover, TIER)
    ledger = q.tier_ledger(TIER)
    assert ledger.holder_tokens(mover).get(KIND, 0) == 1
    live_before = q.read_output_funding(live_mover, TIER)
    q = pool.PoolQueue(q.root)
    _cycle(q, tmp_path, tier_loop.ReceiptCache())
    assert q.read_output_funding(mover, TIER)["state"] == "released"
    assert ledger.holder_tokens(mover).get(KIND, 0) == 0
    assert ledger.available().get(KIND, 0) == 4
    assert q.read_output_funding(live_mover, TIER) == live_before


def test_released_record_keeps_prewrite_during_release_failure(tmp_path, monkeypatch):
    """Keep the old release's proof until its tokens return to free."""
    q, mover, live_mover, receipts, inst, *_ = _stranded_with_live_intent(tmp_path)
    assert q.release_output_funding(mover, TIER)
    prewrite = po._prewrites_dir(q.root, inst) / "fault.prewrite.json"
    release = pool.ResourceLedger.release

    def fail_release(self, key):
        if key == mover:
            raise OSError("injected legacy token release failure")
        return release(self, key)

    with monkeypatch.context() as patch:
        patch.setattr(pool.ResourceLedger, "release", fail_release)
        _cycle(q, tmp_path, receipts)
        assert prewrite.exists()
        assert q.tier_ledger(TIER).holder_tokens(mover).get(KIND, 0) == 1
    q = pool.PoolQueue(q.root)
    _cycle(q, tmp_path, tier_loop.ReceiptCache())
    assert q.tier_ledger(TIER).holder_tokens(mover).get(KIND, 0) == 0
    assert q.read_output_funding(live_mover, TIER)["state"] == "transferring"


@pytest.mark.parametrize("state", ["transferring", "released"])
@pytest.mark.parametrize("guard", ["lease", "partial", "terminal", "unknown-producer",
                                  "unreadable-prewrite"])
def test_release_rechecks_safety_for_both_states(tmp_path, monkeypatch, state, guard):
    """A marker does not waive the proof of nonexecution or absent paths."""
    q, mover, live_mover, _, inst, _, descs = _stranded_with_live_intent(tmp_path)
    if state == "released":
        assert q.release_output_funding(mover, TIER)
    if guard == "lease":
        pool._write_json_atomic(q.lease_path(mover), {"owner": "live-mover"})
    elif guard == "partial":
        q.record_move(mover, {"tier_id": TIER, "complete": False,
                             "bytes_staged": 1})
    elif guard == "terminal":
        pool._write_json_atomic(q.item_path(pool.FAILED, mover),
                                {"action_key": mover, "status": "failed"})
    elif guard == "unknown-producer":
        q.item_path(pool.FAILED, inst["owner_action_key"]).unlink()
    else:
        lstat = tier_loop.os.lstat

        def unreadable(path, *args, **kwargs):
            if str(path) == str(descs[0]["path"]):
                raise PermissionError("injected prewrite read failure")
            return lstat(path, *args, **kwargs)

        monkeypatch.setattr(tier_loop.os, "lstat", unreadable)
    events = _settle(q)
    assert q.read_output_funding(mover, TIER)["state"] == state
    assert q.tier_ledger(TIER).holder_tokens(mover).get(KIND, 0) == 1
    assert not any(e["event"] == "output-funding-tokens-released"
                   and e["mover"] == mover for e in events)
    assert q.read_output_funding(live_mover, TIER)["state"] == "transferring"


@pytest.mark.parametrize("race", ["publish", "claim"])
def test_mover_race_before_lock_preserves_reservation(tmp_path, monkeypatch, race):
    """A mover that becomes live after the scan keeps its reservation."""
    q, mover, _, _, inst, template, descs = _stranded_with_live_intent(tmp_path)
    lock = q._transition_locked
    raced = False

    def race_before_lock(key, **kwargs):
        nonlocal raced
        if key == mover and not raced:
            raced = True
            row = _publish_mover(
                q, mover, po.output_manifest_sha256(descs),
                sum(int(d["bytes"]) for d in descs),
                batch_ref=_ref(inst, template, "fault", descs))
            if race == "claim":
                # An uncommitted batch cannot pass the ordinary claim gate.
                # File the claim boundary directly, as the earlier fixture does.
                pool._write_json_atomic(q.item_path(pool.CLAIMED, mover), row)
                q.item_path(pool.READY, mover).unlink()
        return lock(key, **kwargs)

    monkeypatch.setattr(q, "_transition_locked", race_before_lock)
    _settle(q)
    assert raced
    assert q.read_output_funding(mover, TIER)["state"] == "transferring"
    assert q.tier_ledger(TIER).holder_tokens(mover).get(KIND, 0) == 1


def test_partial_token_release_retries_without_settling_marker(tmp_path, monkeypatch):
    """A short token release leaves the marker open for another pass."""
    q, mover, _, _, *_ = _stranded_with_live_intent(tmp_path)
    ledger = q.tier_ledger(TIER)
    assert ledger.acquire(mover, {KIND: 1})
    assert ledger.holder_tokens(mover).get(KIND, 0) == 2
    release = pool.ResourceLedger.release

    def short_release(self, key):
        if key == mover:
            return sum(self.release_count(key, {KIND: 1}).values())
        return release(self, key)

    with monkeypatch.context() as patch:
        patch.setattr(pool.ResourceLedger, "release", short_release)
        _settle(q)
        assert q.read_output_funding(mover, TIER)["state"] == "transferring"
        assert ledger.holder_tokens(mover).get(KIND, 0) == 1
    _settle(pool.PoolQueue(q.root))
    assert q.read_output_funding(mover, TIER)["state"] == "released"
    assert ledger.holder_tokens(mover).get(KIND, 0) == 0


def test_unknown_token_release_keeps_marker_open(tmp_path, monkeypatch):
    """An unreadable token census cannot establish a completed release."""
    q, mover, _, _, *_ = _stranded_with_live_intent(tmp_path)
    ledger = q.tier_ledger(TIER)
    scan = pool._scan_visible

    def unreadable_holder(path):
        if path == ledger.held_dir / mover:
            raise PermissionError("injected token census failure")
        return scan(path)

    with monkeypatch.context() as patch:
        patch.setattr(pool, "_scan_visible", unreadable_holder)
        _settle(q)
        assert q.read_output_funding(mover, TIER)["state"] == "transferring"
    assert ledger.holder_tokens(mover).get(KIND, 0) == 0
    _settle(pool.PoolQueue(q.root))
    assert q.read_output_funding(mover, TIER)["state"] == "released"
