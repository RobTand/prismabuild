"""#2091: each-cycle output observations are not another capacity debit.

Use real batch/mover fixtures for evidence controls and the actual cycle for
its event plumbing. Logical staged payload bytes are not allocated space.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from prismabuild import batch_ledger as bl
from prismabuild import pool, produced_output as po
from prismabuild import residency_map as rm
from prismabuild import tier_cycle as tc
from prismabuild import window_credit
import test_window_progress_protection as protection
from test_output_window_obligation import _owes
from test_produced_output_restage import (
    TIER, _World, _descriptors, _prewrite, _staged_world,
)


@pytest.fixture(autouse=True)
def _isolated_output_runtime(monkeypatch):
    monkeypatch.setenv("PRISMABUILD_READER_LEASE", "1")
    for name in ("PRISMABUILD_ACTION_NONCE", "PRISMABUILD_ACTION_SCOPE",
                 "PRISMABUILD_READER_HELPER_ROOT", "PRISMABUILD_ACTION_KEY"):
        monkeypatch.delenv(name, raising=False)


def test_cycle_reconciles_once_after_residency_and_preserves_events(
    tmp_path: Path, monkeypatch, capsys,
) -> None:
    queue = pool.PoolQueue(tmp_path / "queue")
    order = []
    observations = []

    def residency(q, **kwargs):
        assert q is queue
        order.append("residency")
        return []

    def reconcile(q, tiers):
        assert q is queue
        assert tiers == {}
        order.append("output")
        event = {"event": "output-batch-staged", "batch_id": "fixture-batch",
                 "namespace": "fixture-namespace", "staged_logical_bytes": 23}
        observations.append(event)
        return [event]

    monkeypatch.setattr(tc, "residency_window", residency)
    monkeypatch.setattr(po, "output_scope_tick", reconcile)
    # Permit the owner's usual direct-import style as well.
    monkeypatch.setattr(tc, "output_scope_tick", reconcile, raising=False)
    # Reconciliation is independent of #905's opt-in gate/relief policy.
    monkeypatch.delenv("PRISMABUILD_TIER_OUTPUT_WINDOWS", raising=False)
    for now in (1000.0, 1001.0):
        tc.cycle(queue, host="reconciliation-host", source_pool="test-source",
                 receipts=tc.ReceiptCache(), now=now, discover=lambda **_kwargs: {})
    assert order == ["residency", "output", "residency", "output"]
    assert len(observations) == 2
    printed = [json.loads(line) for line in capsys.readouterr().out.splitlines()
               if line.startswith("{")]
    for events in (printed, queue.host_events("reconciliation-host")):
        reconciled = [event for event in events
                      if event.get("event") == "output-batch-staged"]
        assert len(reconciled) == 2, events
        for event, expected in zip(reconciled, observations):
            assert all(event.get(key) == value for key, value in expected.items())


def _batch_observation(events, batch_id="b1"):
    batches = [event for event in events if event.get("batch_id") == batch_id]
    assert len(batches) == 1, events
    return batches[0]


@pytest.mark.parametrize("enforced", [False, True])
def test_staged_logical_bytes_require_valid_composition_and_complete_receipt(
    tmp_path: Path, monkeypatch, enforced: bool,
) -> None:
    world = _World(tmp_path)
    descriptors = _descriptors(world.template, world.inst, "p1", b"logical" * 17)
    _prewrite(world.q, world.inst, world.template, "b1", descriptors)
    first = world.first_publish("b1", descriptors)
    assert first.get("ok") is True, first
    world.run_mover(str(first["mover_key"]), "observation-mover")
    batch_path = bl.BatchLedger.for_instance(world.q.root, world.inst).path
    before = batch_path.read_bytes()
    live = _batch_observation(po.output_scope_tick(world.q, {TIER: {}}))

    # Fragments can survive a killed mover. Its receipt is independently required.
    real_complete = bl.mover_receipt_complete
    monkeypatch.setattr(bl, "mover_receipt_complete", lambda *_args: False)
    pending = _batch_observation(po.output_scope_tick(world.q, {TIER: {}}))
    assert pending["event"] == "output-recovery-unknown"
    assert pending.get("staged_logical_bytes") is None, pending
    monkeypatch.setattr(bl, "mover_receipt_complete", lambda *_args: None)
    unreadable_receipt = _batch_observation(po.output_scope_tick(world.q, {TIER: {}}))
    assert unreadable_receipt["event"] == "output-recovery-unknown"
    assert unreadable_receipt.get("staged_logical_bytes") is None, unreadable_receipt
    monkeypatch.setattr(bl, "mover_receipt_complete", real_complete)

    # Validation failures must not contribute even when a receipt is complete.
    real_compose = rm.compose
    def invalid(*args, **kwargs):
        raise ValueError("fixture invalid composed entry")
    monkeypatch.setattr(rm, "compose", invalid)
    invalid_event = _batch_observation(po.output_scope_tick(world.q, {TIER: {}}))
    assert invalid_event["event"] == "output-batch-invalid"
    assert invalid_event.get("staged_logical_bytes") is None, invalid_event
    monkeypatch.setattr(rm, "compose", real_compose)

    assert batch_path.read_bytes() == before
    assert Path(descriptors[0]["path"]).is_file()

    # Observe all three actual capacity decisions beside this diagnostic.
    # Commitment always owes 3; only gate/fence/relief enforce the opt-in flag.
    capacity_root = tmp_path / "capacity-controls"
    capacity_root.mkdir()
    ctx = protection._setup_two_consumers(capacity_root, stage_gib=6)
    monkeypatch.setattr(po, "unheld_window_gib", _owes(3))
    if enforced:
        monkeypatch.setenv(tc.OUTPUT_WINDOWS_ENV, "1")
    else:
        monkeypatch.delenv(tc.OUTPUT_WINDOWS_ENV, raising=False)
    seen = {"gate": [], "fence": []}
    real_gate, real_fence = window_credit.gate_newcomer, window_credit.fence_fits

    def gate(**kwargs):
        seen["gate"].append(kwargs)
        return real_gate(**kwargs)

    def fence(**kwargs):
        seen["fence"].append(kwargs)
        return real_fence(**kwargs)

    monkeypatch.setattr(window_credit, "gate_newcomer", gate)
    monkeypatch.setattr(window_credit, "fence_fits", fence)
    tiers = protection._tiers(capacity_root)
    events = tc.residency_window(ctx["queue"], tiers=tiers)
    charged = 3 if enforced else 0
    assert seen["gate"] and seen["fence"], seen
    assert all(call["output_gib"] == charged for calls in seen.values() for call in calls), seen
    assert {"held_gib": 2, "ready_gib": 0, "output_gib": charged,
            "capacity_gib": 6} in seen["fence"], seen
    gated = protection._gated(events)
    assert set(gated) == {protection.CONSUMER_B}, gated
    commitment = gated[protection.CONSUMER_B]["commitment"]
    assert commitment["unheld_output_gib"] == 3, commitment
    assert commitment["committed_gib"] == 5, commitment
    assert tc.window_pressure(ctx["queue"], tiers=tiers).get(protection.TIER) == (4 if enforced else 1)

    # Payload length, not st_blocks, window capacity, or receipt-declared size.
    assert live["event"] == "output-batch-staged"
    assert live.get("staged_logical_bytes") == sum(d["bytes"] for d in descriptors), live


def test_unreadable_fragments_are_unknown_not_a_zero_or_missing_batch(
    tmp_path: Path, monkeypatch,
) -> None:
    world = _World(tmp_path)
    descriptors = _descriptors(world.template, world.inst, "p1", b"unreadable" * 13)
    _prewrite(world.q, world.inst, world.template, "b1", descriptors)
    first = world.first_publish("b1", descriptors)
    world.run_mover(str(first["mover_key"]), "unreadable-observation")

    def unreadable(*args, **kwargs):
        raise OSError("fixture unreadable residency evidence")

    monkeypatch.setattr(rm, "read_fragments", unreadable)
    unknown = _batch_observation(po.output_scope_tick(world.q, {TIER: {}}))
    assert unknown["event"] == "output-recovery-unknown"
    assert unknown.get("staged_logical_bytes") is None, unknown


def test_retired_predecessor_does_not_mask_live_restage_bytes(tmp_path: Path) -> None:
    world, descriptors = _staged_world(tmp_path, payload=b"successor" * 19)
    retired = po.output_scope_tick(world.q, {TIER: {}})
    assert not [event for event in retired if event.get("batch_id") == "b1"], retired
    successor = world.ensure("b1")
    assert successor.get("ok") is True, successor
    assert successor.get("replay") is not True
    world.run_mover(str(successor["mover_key"]), "restaged-observation")
    batch_path = bl.BatchLedger.for_instance(world.q.root, world.inst).path
    before = batch_path.read_bytes()
    observed = _batch_observation(po.output_scope_tick(world.q, {TIER: {}}))
    assert batch_path.read_bytes() == before
    assert observed["event"] == "output-batch-staged", observed
    assert observed["generation"] == 1, observed
    assert observed.get("staged_logical_bytes") == sum(d["bytes"] for d in descriptors), observed
