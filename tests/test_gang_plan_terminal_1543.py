"""#1543: terminal plan evidence must end an already-elected gang's fence.

Use the real plan reader, supersession writer, claim pass, census and sweep.
A refusal string alone is not terminal evidence: transient reads and changes
of the filed plan must restart confirmation, not permanently cancel a gang.
"""
from __future__ import annotations

from admitted_queue_fixture import simulated_admission, simulated_capacity

from pathlib import Path

import json

import pytest

from prismabuild import _gang, pool, residency_plan
from test_gang_residency_members import (
    TIER, _frozen_plan, _hexkey, _publish_lead, _stage_lead,
)
from test_gang_reservation_1517 import gang_fleet  # noqa: F401
from test_measurement_drains_gpu_backfill import fleet  # noqa: F401


def _elected_plan_wait(gang_fleet, monkeypatch, tmp_path, name, *, lead_state="executed"):
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    lead = _hexkey(f"{name}-lead")
    group, (first, second) = members(name, declares_manifest=True)
    stage = tmp_path / "stage"
    stage.mkdir()
    queue.tier(TIER).mint({"stage_gib": 8})
    plan = _frozen_plan(queue, first, lead)
    # Elect the healthy sibling while its partner waits for its plan's lead.
    # The consumer is READY, not a failed or withdrawn gang member.
    assert gclaim("sparklina") is None
    assert gclaim("sparky") is None
    assert _gang.elections(queue, group, 2)[1]["action_key"] == second
    refill = publish(f"{name}-refill", priority=-10, timeout_s=None,
                     cpu=1, gpu=0, mem_gb=1, tags=["sparky"])
    assert gclaim("sparky") is None
    assert denial(refill, "sparky")["reason"] in (
        "deferred_for_gang_reservation", "deferred_behind_withheld_row")

    if lead_state != "executed":
        if lead_state != "absent":
            _publish_lead(queue, clock, lead, stage)
        if lead_state == "failed":
            monkeypatch.setattr(pool.socket, "gethostname", lambda: "sparklina")
            claimed = queue.claim(**simulated_admission(queue, simulated_capacity({"cpu": 8, "mem_gb": 16})), tags=["sparklina"])
            assert claimed is not None and claimed["action_key"] == lead
            queue.finish(lead, status="failed", detail={"returncode": 1})
    else:
        _stage_lead(queue, clock, monkeypatch, lead, first, stage)
        row = pool.record_store.value_or_raise(
            queue.records.item(pool.record_store.READY, first))
        assert queue.residency_verdict(row)["state"] == "map_not_composed"
    return group, first, second, lead, refill, plan


def _refuse_plan(queue, first, plan, *, revision="one"):
    # A readable document deterministically refused by the schema validator,
    # not a missing file or ESTALE. Keep consumer and manifest identity.
    path = queue.records.residency_plan_path(first)
    path.unlink()
    path.write_text(json.dumps({**plan, "schema": f"tests.refused-plan.{revision}"}))


def _assert_released(gang_fleet, group, first, second, lead, refill):
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    assert _gang.teardown(queue, group) is not None, (
        "stable terminal plan evidence left the elected sibling fencing forever")
    queue.sweep_gangs()
    for member in (first, second):
        assert queue.records.item_path(pool.record_store.WITHDRAWN, member).exists()
        assert not queue.records.item_path(pool.record_store.READY, member).exists()
    # Teardown only withdraws consumers. It must not withdraw their independent
    # movers or return tokens still representing staged bytes behind the owner.
    assert not queue.records.item_path(pool.record_store.WITHDRAWN, lead).exists()
    ended = pool.record_store.value_or_raise(
        queue.records.item(pool.record_store.DONE, lead))
    assert queue.tier(TIER).holdings(lead) == (
        {"stage_gib": 2} if ended and ended["status"] == "executed" else {})
    for _ in range(4):
        if gclaim("sparky") == refill:
            break
    else:
        pytest.fail(f"gang fence survived teardown: {denial(refill, 'sparky')}")


@pytest.mark.parametrize("terminal,lead_state", [
    ("refused", "executed"), ("superseded", "failed"), ("superseded", "absent"),
])
def test_a_terminal_plan_releases_an_already_elected_sibling(
        gang_fleet, monkeypatch, tmp_path, terminal, lead_state):
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    group, first, second, lead, refill, plan = _elected_plan_wait(
        gang_fleet, monkeypatch, tmp_path, terminal, lead_state=lead_state)
    if terminal == "refused":
        _refuse_plan(queue, first, plan)
        reason = "residency_plan_unreadable"
    else:
        residency_plan.mark_superseded(queue, first, reason="coordinator refused this plan")
        reason = ("residency_lead_terminal" if lead_state == "failed"
                  else "residency_lead_not_resident")
        assert queue._residency_supersession(first) is not None
    assert gclaim("sparklina") is None
    assert denial(first, "sparklina")["reason"] == reason
    assert _gang.teardown(queue, group) is None
    clock.now += _gang.TERMINAL_CONFIRM_S - 1
    assert gclaim("sparklina") is None
    assert _gang.teardown(queue, group) is None, "confirmation ended too early"
    clock.now += 2
    assert gclaim("sparklina") is None
    _assert_released(gang_fleet, group, first, second, lead, refill)


@pytest.mark.parametrize("interruption", ["recovered", "unreadable", "replaced"])
def test_plan_confirmation_restarts_after_uncertainty_or_changed_evidence(
        gang_fleet, monkeypatch, tmp_path, interruption):
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    group, first, second, lead, refill, plan = _elected_plan_wait(
        gang_fleet, monkeypatch, tmp_path, interruption)
    _refuse_plan(queue, first, plan)
    assert gclaim("sparklina") is None
    assert denial(first, "sparklina")["reason"] == "residency_plan_unreadable"
    clock.now += _gang.TERMINAL_CONFIRM_S / 2
    path = queue.records.residency_plan_path(first)
    if interruption == "recovered":
        path.unlink()
        path.write_text(json.dumps(plan))
        assert gclaim("sparklina") is None
        assert denial(first, "sparklina")["reason"] == "residency_map_not_composed"
        _refuse_plan(queue, first, plan)
    elif interruption == "unreadable":
        original = pool.record_store.RecordStore.text_document_at

        def unavailable(records, candidate, *args, **kwargs):
            if Path(candidate) == Path(path):
                raise OSError("temporary plan read failure")
            return original(records, candidate, *args, **kwargs)

        with monkeypatch.context() as transient:
            transient.setattr(pool.record_store.RecordStore, "text_document_at", unavailable)
            assert gclaim("sparklina") is None
            assert denial(first, "sparklina")["reason"] == "residency_plan_unreadable"
    else:
        _refuse_plan(queue, first, plan, revision="two")
    assert _gang.teardown(queue, group) is None
    # First observation of the new/recovered evidence starts a fresh window.
    assert gclaim("sparklina") is None
    assert denial(first, "sparklina")["reason"] == "residency_plan_unreadable"
    clock.now += _gang.TERMINAL_CONFIRM_S / 2 + 1
    assert gclaim("sparklina") is None
    assert _gang.teardown(queue, group) is None, (
        "old terminal evidence survived uncertainty or a plan replacement")
    clock.now += _gang.TERMINAL_CONFIRM_S / 2 + 1
    assert gclaim("sparklina") is None
    _assert_released(gang_fleet, group, first, second, lead, refill)
