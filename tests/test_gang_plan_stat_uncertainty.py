"""Unavailable filing observations cannot admit or confirm a gang (#2323)."""
from pathlib import Path
from types import SimpleNamespace

import pytest

from prismabuild import _gang, pool, residency_plan
from test_gang_plan_confirmation import _released, _waiting_plan
from test_gang_residency_members import _compose_map
from test_gang_reservation_1517 import gang_fleet  # noqa: F401
from test_gang_teardown_with_prelaunch_1543 import _fail_lead
from test_measurement_drains_gpu_backfill import fleet  # noqa: F401
from test_prelaunch_admission_1594 import TIER, _consumer_sha, _gang_pair


@pytest.mark.parametrize("signature_number", [None, 1, 2, 3],
                         ids=["sustained", "admission", "confirmation", "commit"])
@pytest.mark.parametrize("stat_number,failure", [
    (1, "unavailable"), (2, "unavailable"), (3, "unavailable"), (4, "unavailable"),
    (2, "absent"), (4, "absent"), (2, "replaced"), (4, "replaced"),
], ids=["initial", "after-bytes", "marker-owner", "after-marker",
        "absent-after-bytes", "absent-after-marker",
        "replaced-after-bytes", "replaced-after-marker"])
def test_stat_fault_at_each_plan_proof_boundary_resets_confirmation(
        gang_fleet, monkeypatch, tmp_path, signature_number, stat_number, failure):
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    group, first, second, lead, refill, plan = _waiting_plan(
        gang_fleet, monkeypatch, tmp_path, "stat-boundary")
    _compose_map(monkeypatch, queue, first, [lead])
    residency_plan.mark_superseded(queue, first, reason="retired plan")
    assert gclaim("sparklina") is None
    mark = _gang.terminal_mark_path(queue, group, first)
    assert mark.exists()
    clock.now += _gang.TERMINAL_CONFIRM_S + 1
    path = queue.records.residency_plan_path(first)
    original_proof = pool.PoolQueue._terminal_plan_signature
    original_stat = Path.stat
    proofs = []
    stats = []
    faults = []

    def unavailable(candidate, *args, **kwargs):
        if candidate == path:
            stats.append(True)
            if len(stats) == stat_number:
                faults.append(True)
                if failure == "absent":
                    raise FileNotFoundError("plan temporarily absent during revalidation")
                if failure == "replaced":
                    info = original_stat(candidate, *args, **kwargs)
                    return SimpleNamespace(st_ino=info.st_ino + 1,
                                           st_mtime_ns=info.st_mtime_ns,
                                           st_size=info.st_size)
                raise OSError("injected terminal-plan stat failure")
        return original_stat(candidate, *args, **kwargs)

    def proof(owner, key):
        if key == first:
            proofs.append(True)
            if signature_number is None or len(proofs) == signature_number:
                stats.clear()
                with monkeypatch.context() as fault:
                    fault.setattr(Path, "stat", unavailable)
                    return original_proof(owner, key)
        return original_proof(owner, key)

    with monkeypatch.context() as fault:
        fault.setattr(pool.PoolQueue, "_terminal_plan_signature", proof)
        assert gclaim("sparklina") is None
        assert faults, "the requested real stat boundary was not reached"
        assert first not in queue.ledger("sparklina").held_keys()
        assert queue.records.item_path(pool.record_store.READY, first).exists()
        assert _gang.teardown(queue, group) is None
        assert not mark.exists(), "unavailable stat preserved the aged confirmation"
    assert gclaim("sparklina") is None
    clock.now += _gang.TERMINAL_CONFIRM_S - 1
    assert gclaim("sparklina") is None
    assert _gang.teardown(queue, group) is None
    clock.now += 2
    assert gclaim("sparklina") is None
    _released(gang_fleet, group, first, second, lead, refill)


@pytest.mark.parametrize("observation", [
    "initial-verdict", "fresh-verdict", "before-fresh", "after-fresh",
])
@pytest.mark.parametrize("error_type", [OSError, FileNotFoundError])
def test_one_shot_verdict_stat_failure_resets_lead_confirmation(
        gang_fleet, monkeypatch, tmp_path, observation, error_type):
    queue, clock, publish, finish, gclaim, denial, group, keys, leads = _gang_pair(
        gang_fleet, tmp_path, "verdict-stat", declare_manifest=True, declare_plan=True)
    first, second = keys
    queue.tier(TIER).mint({"stage_gib": 8})
    _fail_lead(queue, clock, monkeypatch, leads[0], _consumer_sha(queue, first))
    assert gclaim("sparklina") is None
    mark = _gang.terminal_mark_path(queue, group, first)
    assert mark.exists()
    clock.now += _gang.TERMINAL_CONFIRM_S + 1
    original_verdict = pool.PoolQueue.residency_verdict
    original_lead_proof = pool.PoolQueue._gang_lead_terminal_confirmed
    original_incarnation = residency_plan.incarnation
    original_stat = Path.stat
    path = queue.records.residency_plan_path(first)
    faults = []

    def unavailable(candidate, *args, **kwargs):
        if candidate == path and not faults:
            faults.append(True)
            raise error_type("one-shot verdict plan stat failure")
        return original_stat(candidate, *args, **kwargs)

    def verdict(owner, item, *, fresh=False):
        if item["action_key"] == first and observation == (
                "fresh-verdict" if fresh else "initial-verdict"):
            with monkeypatch.context() as fault:
                fault.setattr(Path, "stat", unavailable)
                return original_verdict(owner, item, fresh=fresh)
        return original_verdict(owner, item, fresh=fresh)

    def lead_proof(owner, item, key, residency):
        calls = []

        def incarnation(candidate):
            calls.append(True)
            target = 1 if observation == "before-fresh" else 2
            if candidate == path and len(calls) == target:
                with monkeypatch.context() as fault:
                    fault.setattr(Path, "stat", unavailable)
                    return original_incarnation(candidate)
            return original_incarnation(candidate)

        with monkeypatch.context() as fault:
            if observation in ("before-fresh", "after-fresh"):
                fault.setattr(residency_plan, "incarnation", incarnation)
            return original_lead_proof(owner, item, key, residency)

    with monkeypatch.context() as fault:
        fault.setattr(pool.PoolQueue, "residency_verdict", verdict)
        fault.setattr(pool.PoolQueue, "_gang_lead_terminal_confirmed", lead_proof)
        assert gclaim("sparklina") is None
        assert faults == [True]
        assert _gang.teardown(queue, group) is None
        assert not mark.exists(), "uncertain verdict reused aged lead confirmation"
    assert gclaim("sparklina") is None
    clock.now += _gang.TERMINAL_CONFIRM_S - 1
    assert gclaim("sparklina") is None
    assert _gang.teardown(queue, group) is None
    clock.now += 2
    assert gclaim("sparklina") is None
    assert _gang.teardown(queue, group) is not None


@pytest.mark.parametrize("error_type", [OSError, FileNotFoundError])
def test_resident_verdict_cannot_hide_an_earlier_stat_failure(
        gang_fleet, monkeypatch, tmp_path, error_type):
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    group, first, second, lead, refill, plan = _waiting_plan(
        gang_fleet, monkeypatch, tmp_path, "resident-stat")
    _compose_map(monkeypatch, queue, first, [lead])
    path = queue.records.residency_plan_path(first)
    original_stat = Path.stat
    faults = []

    def unavailable(candidate, *args, **kwargs):
        if candidate == path and not faults:
            faults.append(True)
            raise error_type("one-shot resident verdict stat failure")
        return original_stat(candidate, *args, **kwargs)

    with monkeypatch.context() as fault:
        fault.setattr(Path, "stat", unavailable)
        assert gclaim("sparklina") is None
        assert faults == [True]
        assert first not in queue.ledger("sparklina").held_keys()
        assert denial(first, "sparklina")["reason"] == "residency_plan_unreadable"
    assert gclaim("sparklina") == first
