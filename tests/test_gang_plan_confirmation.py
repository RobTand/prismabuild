"""Claim-pass boundaries missed by sustained terminal-plan refusal tests (#1543)."""
from pathlib import Path
import json

import pytest

from prismabuild import _gang, pool, residency_plan
from test_gang_residency_members import (
    TIER, _compose_map, _frozen_plan, _hexkey, _stage_lead,
)
from test_gang_reservation_1517 import gang_fleet  # noqa: F401
from test_measurement_drains_gpu_backfill import fleet  # noqa: F401


def _waiting_plan(gang_fleet, monkeypatch, tmp_path, name):
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    group, (first, second) = members(name, declares_manifest=True)
    lead = _hexkey(name + "-lead")
    stage = tmp_path / "stage"
    stage.mkdir()
    queue.tier(TIER).mint({"stage_gib": 8})
    plan = _frozen_plan(queue, first, lead)
    _stage_lead(queue, clock, monkeypatch, lead, first, stage)
    assert gclaim("sparky") is None
    assert _gang.elections(queue, group, 2)[1]["action_key"] == second
    refill = publish(name + "-refill", priority=-10, timeout_s=None,
                     cpu=1, gpu=0, mem_gb=1, tags=["sparky"])
    assert gclaim("sparky") is None
    assert denial(refill, "sparky")["reason"] in (
        "deferred_for_gang_reservation", "deferred_behind_withheld_row")
    return group, first, second, lead, refill, plan


def _released(gang_fleet, group, first, second, lead, refill):
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    assert _gang.teardown(queue, group) is not None
    queue.sweep_gangs()
    for key in (first, second):
        assert queue.records.item_path(pool.record_store.WITHDRAWN, key).exists()
        assert not queue.records.item_path(pool.record_store.READY, key).exists()
    assert not queue.records.item_path(pool.record_store.WITHDRAWN, lead).exists()
    assert queue.tier(TIER).holdings(lead) == {"stage_gib": 2}
    for _ in range(4):
        if gclaim("sparky") == refill:
            break
    else:
        pytest.fail(f"superseded gang still fences LOW work: {denial(refill, 'sparky')}")


@pytest.mark.parametrize("memory_blocked", [True, False])
def test_resident_superseded_plan_enters_terminal_lifecycle(
        gang_fleet, monkeypatch, tmp_path, memory_blocked):
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    group, first, second, lead, refill, plan = _waiting_plan(
        gang_fleet, monkeypatch, tmp_path, "resident-superseded")
    incumbent = None
    if memory_blocked:
        incumbent = publish("incumbent", priority=10, timeout_s=None,
                            cpu=1, gpu=0, mem_gb=48, tags=["sparklina"])
        assert gclaim("sparklina") == incumbent
    _compose_map(monkeypatch, queue, first, [lead])
    row = pool.record_store.value_or_raise(queue.records.item(pool.record_store.READY, first))
    assert queue.residency_verdict(row)["state"] == "resident"
    residency_plan.mark_superseded(queue, first, reason="retired current plan")
    assert queue._terminal_plan_signature(first) is not None
    assert gclaim("sparklina") is None, "superseded plan launched before confirmation"
    assert _gang.teardown(queue, group) is None
    clock.now += _gang.TERMINAL_CONFIRM_S - 1
    assert gclaim("sparklina") is None
    assert _gang.teardown(queue, group) is None
    clock.now += 2
    assert gclaim("sparklina") is None
    _released(gang_fleet, group, first, second, lead, refill)
    if incumbent:
        assert incumbent in queue.ledger("sparklina").held_keys()


@pytest.mark.parametrize("read_number", [1, 2])
@pytest.mark.parametrize("error_type", [OSError, FileNotFoundError])
def test_one_shot_verdict_plan_read_failure_restarts_confirmation(
        gang_fleet, monkeypatch, tmp_path, read_number, error_type):
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    group, first, second, lead, refill, plan = _waiting_plan(
        gang_fleet, monkeypatch, tmp_path, "one-shot-read")
    path = queue.records.residency_plan_path(first)
    path.unlink()
    path.write_text(json.dumps({**plan, "schema": "tests.invalid-plan"}))
    assert gclaim("sparklina") is None
    clock.now += _gang.TERMINAL_CONFIRM_S + 1
    original = pool.record_store.RecordStore.text_document_at
    failed = []
    reads = []

    def one_shot(records, candidate, *args, **kwargs):
        if Path(candidate) == path:
            reads.append(True)
            if len(reads) == read_number:
                failed.append(True)
                raise error_type("one-shot verdict read uncertainty")
        return original(candidate, *args, **kwargs)

    with monkeypatch.context() as fault:
        fault.setattr(pool.record_store.RecordStore, "text_document_at", one_shot)
        assert gclaim("sparklina") is None
        assert failed == [True]
        assert denial(first, "sparklina")["reason"] == "residency_plan_unreadable"
        assert queue._terminal_plan_signature(first) is not None
        assert _gang.teardown(queue, group) is None, "uncertain verdict reused aged proof"
    assert gclaim("sparklina") is None
    clock.now += _gang.TERMINAL_CONFIRM_S / 2 + 1
    assert gclaim("sparklina") is None
    assert _gang.teardown(queue, group) is None
    clock.now += _gang.TERMINAL_CONFIRM_S / 2 + 1
    assert gclaim("sparklina") is None
    _released(gang_fleet, group, first, second, lead, refill)
