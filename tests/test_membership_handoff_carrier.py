"""Membership handoff is a proven decision, not an owner-shaped string (R13).

``pool.withdraw`` persists ``membership_handoff`` in the immutable
decision only after proving, against the live claim under the key lock:
the claim still is the planned attempt (``_same_claim``), its
generation is uncovered, and restart permission with remaining budget
and existing lineage still holds. The tier-loop window reads that exact
decision back. A supervisor-shaped ``by`` with no (or a failing) proof
files an ordinary cancellation and still retires the window.
"""

from __future__ import annotations

import hashlib
import json
import socket
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

from prismabuild import (  # noqa: E402
    adaptive_cpu,
    pool,
    residency_plan,
    storage_tiers,
)
import tier_loop  # noqa: E402

TIER = "prismabuild-stage:dl380g10"
STAGE_KIND = f"stage_gib@{TIER}"
MANIFEST = "ef" * 32
GIB = 1 << 30
SPAN = 2 * GIB


def _hexkey(seed: str) -> str:
    return (seed.encode().hex() * 64)[:64]


CONSUMER = _hexkey("r13consumer")
MOVER = _hexkey("r13mover0")
EGRESS = _hexkey("r13egress0")


@pytest.fixture()
def queue(tmp_path: Path) -> pool.PoolQueue:
    q = pool.PoolQueue(tmp_path / "pb-queue")
    q.ensure_layout()
    q.mint_tier_capacity(TIER, {"stage_gib": 8})
    return q


def _sealed_shape(monkeypatch) -> None:
    monkeypatch.setattr(adaptive_cpu, "action_identity",
                        lambda item: ("shape", False))


def _owner(host: str) -> str:
    return f"{host}:supervisor-9:8"


def _publish(queue: pool.PoolQueue, key: str, **kw) -> None:
    queue.publish(action_key=key, cas_root=queue.root / "cas",
                  checkout_root=queue.root / "co",
                  worker_script=queue.root / "worker.py",
                  resources=kw.pop("resources", {"cpu": 1}),
                  max_attempts=kw.pop("max_attempts", 3),
                  retry_safe=kw.pop("retry_safe", True),
                  tags=kw.pop("tags", ["x86"]), **kw)


def _claim(queue: pool.PoolQueue, host: str, tag: str = "x86") -> dict:
    snap = queue.claim(tags=[tag], owner=f"{host}:1:{tag}",
                       capacity={"cpu": 4, "mem_gb": 16})
    assert snap is not None
    return snap


def _staged(queue: pool.PoolQueue, tmp_path: Path) -> dict[str, object]:
    """A frozen plan with a window-published staged mover, both live."""
    def row(key, resources):
        return {"action_key": key, "cas_root": str(queue.root / "cas"),
                "checkout_root": str(queue.root / "co"),
                "worker_script": str(queue.root / "worker.py"),
                "tags": ["dl380g10"], "resources": resources}

    plan = residency_plan.build_plan(
        consumer_action_key=CONSUMER, tier_id=TIER,
        stage_root="/stage/prewarm", manifest_sha256=MANIFEST,
        manifest_bytes=SPAN, phases=[{
            "name": "phase-0000", "start_bytes": 0, "end_bytes": SPAN,
            "stage_gib": 2,
            "mover_row": {
                **row(MOVER, {STAGE_KIND: 2, "mem_gb": 1}),
                "retry_safe": True, "max_attempts": 3,
                "residency": {
                    "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                    "manifest_sha256": MANIFEST, "manifest_bytes": SPAN,
                    "range_start_bytes": 0, "range_end_bytes": SPAN}},
            "egress_row": row(EGRESS, {"mem_gb": 1})}])
    residency_plan.freeze(queue, plan)
    queue.publish(action_key=CONSUMER, cas_root=queue.root / "cas",
                  checkout_root=queue.root / "co",
                  worker_script=queue.root / "worker.py",
                  resources={"cpu": 1, "mem_gb": 1},
                  max_attempts=3, retry_safe=True, tags=["x86"],
                  residency={"schema": pool.RESIDENCY_SCHEMA_V1,
                             "tier_id": TIER, "manifest_sha256": MANIFEST,
                             "manifest_bytes": SPAN,
                             "leads": residency_plan.leads_for(plan)})
    events = tier_loop.residency_window(
        queue, tiers={TIER: {"tier_id": TIER, "tier": "stage",
                             "mountpoint": str(tmp_path / "stage")}})
    assert MOVER in [e["action_key"] for e in events
                     if e["event"] == "mover-published"]
    return plan


def test_shape_without_proof_retires_consumer_plan(
    queue: pool.PoolQueue, tmp_path: Path,
) -> None:
    """A supervisor-shaped `by` with no handoff proof retires the window
    exactly like an operator cancellation -- shape alone preserves
    nothing."""
    host = socket.gethostname()
    owner = _owner(host)
    _staged(queue, tmp_path)
    consumer = queue.withdraw(CONSUMER, reason="shape only", by=owner)
    assert consumer["residency_plan_superseded"] is True


def test_shape_without_proof_mover_retires_at_tick(
    queue: pool.PoolQueue, tmp_path: Path,
) -> None:
    """The window side of the same rule: a supervisor-shaped mover marker
    with no proven handoff retires the plan at the next tick."""
    host = socket.gethostname()
    owner = _owner(host)
    plan = _staged(queue, tmp_path)
    queue.withdraw(MOVER, reason="shape only", by=owner)
    assert residency_plan.superseded(queue, plan) is None
    tick = tier_loop.residency_window(
        queue, tiers={TIER: {"tier_id": TIER, "tier": "stage",
                             "mountpoint": str(tmp_path / "stage")}})
    assert [e for e in tick if e["event"] == "residency-plan-superseded"]
    assert residency_plan.superseded(queue, plan) is not None


def test_proven_handoff_persists_exact_decision(
    queue: pool.PoolQueue, tmp_path: Path, monkeypatch,
) -> None:
    """The live marker carries the proven identity -- owner, attempt,
    budget, generation -- typed and equal to the decision's own, and the
    window reads it back as a handoff, not a cancellation."""
    _sealed_shape(monkeypatch)
    host = socket.gethostname()
    owner = _owner(host)
    _staged(queue, tmp_path)
    snap = queue.claim(tags=["dl380g10"], owner=f"{host}:1:mm",
                       capacity={"cpu": 4, "mem_gb": 16})
    assert snap is not None and snap["action_key"] == MOVER
    requeue = queue.plan_requeue(dict(snap))
    queue.withdraw(MOVER, reason=f"resign {owner}: test", by=owner,
                   membership_handoff=requeue["snapshot"])
    marker = json.loads(queue.item_path(pool.WITHDRAWN, MOVER).read_text())
    proof = marker.get("membership_handoff")
    assert isinstance(proof, dict)
    assert proof["owner"] == owner == marker["withdrawn_by"]
    assert proof["attempts"] == marker["attempts"] == 0
    assert proof["max_attempts"] == marker["max_attempts"] == 3
    assert (proof["published_unix"] == marker["published_unix"]
            == snap["published_unix"])
    assert tier_loop._operator_withdrawal(queue, MOVER) is False


def test_handoff_proof_refuses_before_withdrawal(
    queue: pool.PoolQueue, monkeypatch,
) -> None:
    """Stale reads, foreign claims, covered generations and exhausted
    budgets refuse BEFORE anything is stopped or filed: the live claim
    still stands and no marker lands."""
    _sealed_shape(monkeypatch)
    host = socket.gethostname()
    owner = _owner(host)

    stale_key = _hexkey("r13stale")
    _publish(queue, stale_key)
    live = _claim(queue, host)
    tampered = dict(live, claimed_unix=float(live["claimed_unix"]) + 100.0)
    with pytest.raises(pool.PoolContractError):
        queue.withdraw(stale_key, reason="t", by=owner,
                       membership_handoff=tampered)
    assert not queue.item_path(pool.WITHDRAWN, stale_key).exists()
    assert queue.item_path(pool.CLAIMED, stale_key).exists()

    other_key = _hexkey("r13other")
    _publish(queue, other_key, tags=["other"])
    foreign = _claim(queue, host, tag="other")
    with pytest.raises(pool.PoolContractError):
        queue.withdraw(stale_key, reason="t", by=owner,
                       membership_handoff=dict(foreign))
    assert not queue.item_path(pool.WITHDRAWN, stale_key).exists()

    spent_key = _hexkey("r13spent")
    _publish(queue, spent_key, max_attempts=1)
    spent = queue.claim(tags=["x86"], owner=f"{host}:1:spent",
                        capacity={"cpu": 4, "mem_gb": 16})
    assert spent is not None
    with pytest.raises(pool.PoolContractError):
        queue.withdraw(spent_key, reason="t", by=owner,
                       membership_handoff=dict(spent))
    assert not queue.item_path(pool.WITHDRAWN, spent_key).exists()

    covered_key = _hexkey("r13covered")
    _publish(queue, covered_key)
    covered = _claim(queue, host)
    queue.withdraw(covered_key, reason="operator", by="operator:test")
    with pytest.raises(pool.PoolContractError):
        queue.withdraw(covered_key, reason="t", by=owner,
                       membership_handoff=dict(covered))
    marker = json.loads(
        queue.item_path(pool.WITHDRAWN, covered_key).read_text())
    assert "membership_handoff" not in marker


def test_forged_snapshot_bindings_refuse(
    queue: pool.PoolQueue, monkeypatch,
) -> None:
    """A snapshot copied from the live claim but with flipped retry
    permission, inflated budget, another action's key, or another
    attempt's scope authorizes nothing: authorization is derived from
    live fields, and the live job is never stopped by a forgery."""
    _sealed_shape(monkeypatch)
    host = socket.gethostname()
    owner = _owner(host)

    flip_key = _hexkey("r13flip")
    _publish(queue, flip_key, retry_safe=False, max_attempts=1)
    flip_live = queue.claim(tags=["x86"], owner=f"{host}:1:flip",
                            capacity={"cpu": 4, "mem_gb": 16})
    assert flip_live is not None
    assert flip_live["retry_safe"] is False
    forged_safe = dict(flip_live, retry_safe=True, max_attempts=3)
    with pytest.raises(pool.PoolContractError):
        queue.withdraw(flip_key, reason="t", by=owner,
                       membership_handoff=forged_safe)
    assert not queue.item_path(pool.WITHDRAWN, flip_key).exists()
    assert queue.item_path(pool.CLAIMED, flip_key).exists()

    inflate_key = _hexkey("r13inflate")
    _publish(queue, inflate_key, max_attempts=1)
    inflate_live = queue.claim(tags=["x86"], owner=f"{host}:1:inflate",
                               capacity={"cpu": 4, "mem_gb": 16})
    assert inflate_live is not None
    forged_budget = dict(inflate_live, max_attempts=3)
    with pytest.raises(pool.PoolContractError):
        queue.withdraw(inflate_key, reason="t", by=owner,
                       membership_handoff=forged_budget)
    assert not queue.item_path(pool.WITHDRAWN, inflate_key).exists()

    key_key = _hexkey("r13key")
    _publish(queue, key_key)
    key_live = _claim(queue, host)
    wrong_key = dict(key_live, action_key=_hexkey("r13wrong"))
    with pytest.raises(pool.PoolContractError):
        queue.withdraw(key_key, reason="t", by=owner,
                       membership_handoff=wrong_key)
    assert not queue.item_path(pool.WITHDRAWN, key_key).exists()

    scope_key = _hexkey("r13scope")
    _publish(queue, scope_key)
    scope_live = _claim(queue, host)
    nonce = "ab" * 16
    unit = ("prismabuild-job"
            + hashlib.sha256(
                (scope_key + nonce).encode()).hexdigest()[:32] + ".slice")
    scoped = dict(scope_live, resource_scope={
        "action_key": scope_key, "scope_id": unit, "nonce": nonce,
        "token": "cd" * 32, "memory_max_bytes": 1 << 30,
        "cgroup_path": "/sys/fs/cgroup/prismabuild.slice/" + unit,
        "socket_path": "/run/prismabuild/resources.sock"})
    queue.item_path(pool.CLAIMED, scope_key).write_text(json.dumps(scoped))
    scopeless = {k: v for k, v in scoped.items() if k != "resource_scope"}
    with pytest.raises(pool.PoolContractError):
        queue.withdraw(scope_key, reason="t", by=owner,
                       membership_handoff=scopeless)
    assert not queue.item_path(pool.WITHDRAWN, scope_key).exists()
    assert json.loads(
        queue.item_path(pool.CLAIMED, scope_key).read_text()) == scoped


def test_malformed_carrier_never_authorizes() -> None:
    """The shared validator refuses every malformed shape: non-decisions,
    missing/mismatched proof, untyped counters, and non-finite
    generations -- against the authoritative marker fields."""
    host = socket.gethostname()
    owner = f"{host}:supervisor-9:8"
    good = {"action_key": "a" * 64, "status": "withdrawn",
            "attempts": 1, "max_attempts": 4, "published_unix": 100.5,
            "withdrawn_by": owner,
            "membership_handoff": {"owner": owner, "attempts": 1,
                                   "max_attempts": 4,
                                   "published_unix": 100.5}}
    assert pool.membership_handoff_authorized(good) is True
    assert pool.membership_handoff_authorized(None) is False
    assert pool.membership_handoff_authorized("withdrawn") is False
    no_proof = dict(good)
    del no_proof["membership_handoff"]
    assert pool.membership_handoff_authorized(no_proof) is False
    foreign_owner = dict(good, withdrawn_by="operator:test")
    assert pool.membership_handoff_authorized(foreign_owner) is False
    unshaped_owner = dict(
        good, withdrawn_by="operator:test",
        membership_handoff=dict(good["membership_handoff"],
                                owner="operator:test"))
    assert pool.membership_handoff_authorized(unshaped_owner) is False
    bool_counter = dict(
        good, attempts=True,
        membership_handoff=dict(good["membership_handoff"], attempts=True))
    assert pool.membership_handoff_authorized(bool_counter) is False
    str_budget = dict(
        good, max_attempts="4",
        membership_handoff=dict(good["membership_handoff"],
                                max_attempts="4"))
    assert pool.membership_handoff_authorized(str_budget) is False
    for bad_gen in (float("nan"), float("inf"), "100.5", True, None):
        bad = dict(
            good, published_unix=bad_gen,
            membership_handoff=dict(good["membership_handoff"],
                                    published_unix=bad_gen))
        assert pool.membership_handoff_authorized(bad) is False, bad_gen
    drifted = dict(
        good, membership_handoff=dict(good["membership_handoff"],
                                      published_unix=101.5))
    assert pool.membership_handoff_authorized(drifted) is False


def test_repeat_handoff_never_retargets_successor(
    queue: pool.PoolQueue, monkeypatch,
) -> None:
    """A proven handoff stays bound to its exact claimed generation: a
    repeat request adopts the already-filed decision instead of
    retargeting onto a newer READY row waiting behind it.

    The successor is hand-placed to simulate the torn read this guard
    exists for (a concurrent publication whose marker retirement is not
    yet visible -- ``publish`` retires atomically in-process, so real
    transitions alone cannot stage both sides at once; hand-written
    READY rows simulate races elsewhere in this suite too).  The repeat
    returns the filed decision gracefully; the successor's bytes, the
    live marker, and the withdrawal lineage are byte-identical after.
    """
    _sealed_shape(monkeypatch)
    host = socket.gethostname()
    owner = _owner(host)
    key = _hexkey("r15retarget")
    _publish(queue, key)
    snap_a = _claim(queue, host)
    pre = queue.plan_requeue(dict(snap_a))
    queue.withdraw(key, reason=f"resign {owner}: t", by=owner,
                   membership_handoff=pre["snapshot"])
    live_a = json.loads(queue.item_path(pool.WITHDRAWN, key).read_text())
    assert pool.membership_handoff_authorized(live_a) is True
    successor = {"action_key": key,
                 "published_unix": float(live_a["published_unix"]) + 1000.0,
                 "attempts": 1, "max_attempts": 3}
    queue.item_path(pool.READY, key).write_text(json.dumps(successor))
    out = queue.withdraw(key, reason=f"resign {owner}: again", by=owner,
                         membership_handoff=pre["snapshot"])
    assert out["status"] == "already_withdrawn", out
    assert json.loads(queue.item_path(pool.READY, key).read_text()) == successor
    live_after = json.loads(queue.item_path(pool.WITHDRAWN, key).read_text())
    assert live_after["published_unix"] == live_a["published_unix"]
    assert live_after["membership_handoff"] == live_a["membership_handoff"]


def _scoped_live(queue: pool.PoolQueue, key: str, snap: dict,
                 nonce: str) -> dict:
    """Rewrite the live claim adding a broker-valid scope block (the
    contract `_scope_from_record` enforces), returning the new bytes."""
    from prismabuild.resource_scope import BROKER_SOCKET

    unit = ("prismabuild-job"
            + hashlib.sha256((key + nonce).encode()).hexdigest()[:32]
            + ".slice")
    scoped = dict(snap, resource_scope={
        "action_key": key, "scope_id": unit, "nonce": nonce,
        "token": "cd" * 32, "memory_max_bytes": 1 << 30,
        "cgroup_path": "/sys/fs/cgroup/prismabuild.slice/" + unit,
        "socket_path": str(BROKER_SOCKET)})
    queue.item_path(pool.CLAIMED, key).write_text(json.dumps(scoped))
    return scoped


def test_malformed_scope_block_refuses_without_withdrawal(
    queue: pool.PoolQueue, monkeypatch,
) -> None:
    """A malformed non-mapping scope block is never read as 'no scope':
    the handoff refuses and nothing is stopped or filed."""
    _sealed_shape(monkeypatch)
    host = socket.gethostname()
    owner = _owner(host)
    key = _hexkey("r15malformed")
    _publish(queue, key)
    snap = _claim(queue, host)
    broken = dict(snap, resource_scope="broken")
    queue.item_path(pool.CLAIMED, key).write_text(json.dumps(broken))
    with pytest.raises(pool.PoolContractError):
        queue.withdraw(key, reason="t", by=owner,
                       membership_handoff=dict(snap))
    assert not queue.item_path(pool.WITHDRAWN, key).exists()
    assert json.loads(
        queue.item_path(pool.CLAIMED, key).read_text()) == broken


def test_scope_intent_gate(queue: pool.PoolQueue, monkeypatch) -> None:
    """Intent-only identity matches exactly or refuses; a matching
    legitimate prelaunch identity proceeds; scope beside intent must
    name its nonce."""
    _sealed_shape(monkeypatch)
    host = socket.gethostname()
    owner = _owner(host)

    intent_key = _hexkey("r15intent")
    _publish(queue, intent_key)
    intent_live = _claim(queue, host)
    intented = dict(intent_live, resource_scope_intent={
        "action_key": intent_key, "nonce": "ab" * 16})
    queue.item_path(pool.CLAIMED, intent_key).write_text(json.dumps(intented))
    stale = dict(intented)
    stale["resource_scope_intent"] = dict(stale["resource_scope_intent"],
                                          nonce="cd" * 16)
    with pytest.raises(pool.PoolContractError):
        queue.withdraw(intent_key, reason="t", by=owner,
                       membership_handoff=stale)
    assert not queue.item_path(pool.WITHDRAWN, intent_key).exists()
    malformed = dict(intented, resource_scope_intent="broken")
    queue.item_path(pool.CLAIMED, intent_key).write_text(json.dumps(malformed))
    with pytest.raises(pool.PoolContractError):
        queue.withdraw(intent_key, reason="t", by=owner,
                       membership_handoff=dict(malformed))
    queue.item_path(pool.CLAIMED, intent_key).write_text(json.dumps(intented))
    out = queue.withdraw(intent_key, reason="t", by=owner,
                         membership_handoff=dict(intented))
    assert out["status"] == "withdrawn", out
    assert pool.membership_handoff_authorized(
        json.loads(queue.item_path(pool.WITHDRAWN, intent_key).read_text()))

    both_key = _hexkey("r15both")
    _publish(queue, both_key)
    both_live = _claim(queue, host)
    scoped = _scoped_live(queue, both_key, both_live, "ab" * 16)
    drifted = dict(scoped, resource_scope_intent={
        "action_key": both_key, "nonce": "ef" * 16})
    queue.item_path(pool.CLAIMED, both_key).write_text(json.dumps(drifted))
    with pytest.raises(pool.PoolContractError):
        queue.withdraw(both_key, reason="t", by=owner,
                       membership_handoff=dict(drifted))
    assert not queue.item_path(pool.WITHDRAWN, both_key).exists()
