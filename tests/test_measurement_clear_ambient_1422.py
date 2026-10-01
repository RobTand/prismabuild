"""PB #1422: attributed measurement CPUs, not off-affinity aggregate idle.

Reuse the real CAS/queue/controllers/ledger fleet. Only sampler observations and
clock are controlled; no decision, idle verdict, or ambient check is replaced.
This RED checkpoint covers the coordinator's ambient-clear blocker;
reservation-aware continuous-backfill proof belongs separately to #1419.
"""
from __future__ import annotations

import pytest

from test_measurement_drains_gpu_backfill import fleet as gpu_backfill_fleet
from prismabuild import adaptive_cpu, pool

fleet = gpu_backfill_fleet
TIERS = {"preferred": list(range(20)), "fallback": []}


def _measurement_decision(fleet, *, busy, attribution="clear", with_holder=False,
                          baseline_samples=()):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    holder = None
    if with_holder:
        holder = publish("incumbent", priority=-10)
        assert claim() == holder
    else:
        # The normal claim capacity prelude, without admitting an action.
        ledger = queue.ledger()
        ledger.configure_cpu_tiers(TIERS)
        ledger.ensure_capacity({"cpu": 20, "gpu": 1, "mem_gb": 120})
    base = adaptive_cpu.local_state_base(queue.ledger().base)
    adaptive_cpu.write_json(base / adaptive_cpu.IDLE_BASELINE, {
        "schema": adaptive_cpu.IDLE_BASELINE_SCHEMA, "identity": 20,
        "samples": list(baseline_samples),
    })
    measurement = publish("measurement", measurement=True, pinned=True,
                          priority=0, cpu=8)
    item = pool._read_json(queue.item_path(pool.READY, measurement))
    assert isinstance(item, dict)
    assert adaptive_cpu.action_identity(item)[1] is True
    predicted = queue.ledger().free_cpu_allocation(8, TIERS)
    assert predicted is not None and len(predicted) == 8
    readings["busy_cpus"] = busy
    # Complete, valid foreign readings on every predicted CPU are below the
    # existing .10 ambient limit. The remaining aggregate busy lives elsewhere.
    outside = (busy - len(predicted) * 0.082) / (20 - len(predicted))
    assert 0 <= outside <= 1
    readings["foreign"] = {str(cpu): 0.082 if cpu in predicted else outside
                           for cpu in range(20)}
    if attribution == "unknown":
        readings["foreign"] = {}
    elif attribution == "partial":
        readings["foreign"] = {str(predicted[0]): 0.082}
    elif attribution == "excess":
        readings["foreign"][str(predicted[0])] = 0.2
    tick(0.01)
    controller = adaptive_cpu.Controller(queue.ledger(), TIERS)
    with controller.locked():
        metadata = controller.decision(item, item["resources"])
        decision = controller.last_decision
        observed = decision["sample"]
        assert isinstance(observed, dict)
        baseline = controller.idle(observed, list(queue.ledger().held_dir.iterdir()))
    assert observed["sampled_unix"] == clock[0]
    assert isinstance(observed["busy_cpus"], (int, float))
    assert observed["busy_cpus"] + 8 < 20
    assert baseline["basis"] == ("measured" if baseline_samples else "unmeasured")
    assert baseline["exceeds"] is True
    return queue, measurement, holder, metadata, decision


@pytest.mark.parametrize("busy", [2.3, 5.0])
def test_clear_measurement_cpu_attribution_continues_past_aggregate_idle(fleet, busy):
    queue, measurement, holder, metadata, decision = _measurement_decision(fleet, busy=busy)
    assert not queue.ledger().held_keys()
    assert metadata is not None, decision
    assert metadata["measurement"] is True
    assert decision["reason"] == "admitted"
    # A CPU decision is not a token or a claim: the real GPU/fit gates still
    # own admission, and this test neither spends nor fabricates a permission.
    assert queue.item_path(pool.READY, measurement).exists()
    assert not queue.item_path(pool.CLAIMED, measurement).exists()


@pytest.mark.parametrize("attribution,with_holder,expected", [
    ("unknown", False, "measurement_host_not_idle"),
    ("partial", False, "measurement_host_not_idle"),
    ("excess", False, "measurement_foreign_ambient"),
    ("clear", True, "measurement_holder"),
])
def test_ambient_clear_never_skips_unknown_foreign_or_holder_gates(
        fleet, attribution, with_holder, expected):
    queue, measurement, holder, metadata, decision = _measurement_decision(
        fleet, busy=2.3, attribution=attribution, with_holder=with_holder)
    assert metadata is None
    assert decision["reason"] == expected, decision
    assert queue.item_path(pool.READY, measurement).exists()
    if holder is not None:
        assert queue.item_path(pool.CLAIMED, holder).exists()
        assert queue.ledger().holds_gpu(holder)
        assert decision["holder"] == holder


@pytest.mark.parametrize("measured,psi,admitted", [
    (False, 0.10, False), (False, 0.20, False),
    (True, 0.04, True), (True, 0.041, False),
])
def test_clear_cpu_attribution_preserves_same_reference_psi_gate(
        fleet, monkeypatch, measured, psi, admitted):
    original_sample = adaptive_cpu.Controller.sample
    monkeypatch.setattr(adaptive_cpu.Controller, "sample", lambda self:
                        dict(original_sample(self), psi_some=psi))
    clock = fleet[1]
    history = [{"sampled_unix": clock[0] - age,
                "busy_cpus": 0.5, "psi_some": 0.04} for age in (30, 20, 10)]
    queue, measurement, _, metadata, decision = _measurement_decision(
        fleet, busy=2.3, baseline_samples=history if measured else ())
    assert (metadata is not None) is admitted, decision
    assert decision["reason"] == ("admitted" if admitted else "measurement_host_not_idle")
    observed = decision["sample"]
    assert isinstance(observed, dict)
    assert observed["psi_some"] == psi
    assert queue.item_path(pool.READY, measurement).exists()
