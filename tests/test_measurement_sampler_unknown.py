"""Unknown CPU telemetry refuses measurement, not useful backfill (#1317).

Real sealed requests, controllers, queue ordering and held tokens; only clocks
and sampler observations are controlled. No GPU execution or subprocesses.
"""
from __future__ import annotations

import pytest

from test_measurement_drains_gpu_backfill import fleet  # noqa: F401
from prismabuild import adaptive_cpu, pool


def _silence(monkeypatch, clock, readings, kind):
    observed = clock[0]

    def sample(self):
        if kind == "missing":
            return {}
        return {
            "sampled_unix": observed if kind == "stale" else clock[0] + (
                1 if kind == "future" else 0),
            "cpu_count": 20, "interval_s": 1.0,
            "psi_some": None if kind == "partial" else 0.0,
            "busy_cpus": readings["busy_cpus"],
            "foreign_per_cpu_busy": dict(readings["foreign"]),
        }

    monkeypatch.setattr(adaptive_cpu.Controller, "sample", sample)
    clock[0] += adaptive_cpu.MAX_SAMPLE_AGE_S + 1


def _reason(value):
    return value.get("reason") if isinstance(value, dict) else None


def _diagnostic(queue, key):
    result = []
    for record in queue._host_denial_records().values():
        if isinstance(record, dict) and record.get("action_key") == key:
            evidence = record.get("evidence", {})
            result.append({"reason": record.get("reason"),
                           "decision": _reason(evidence.get("decision")),
                           "cpu_decision": _reason(evidence.get("cpu_decision")),
                           "gpu_decision": _reason(evidence.get("gpu_decision")),
                           "kept_for": evidence.get("kept_for"),
                           "withheld_for": evidence.get("withheld_for")})
    return result


def _assert_unknown(queue, measurement, denial, clock):
    waiting = denial(measurement)
    decision = waiting["evidence"]["decision"]
    assert decision["reason"] == "measurement_sampler_unknown", waiting
    assert decision["fresh"] is False
    assert waiting["evidence"].get("withhold", {}).get("withhold") is not True
    assert queue.item_path(pool.READY, measurement).exists()
    item = pool._read_json(queue.item_path(pool.READY, measurement))
    assert queue._carried_withhold(queue._host_denial_records(), item,
                                 host="sparklina", now=clock[0]) is None


def _claim_backfill_after_gap(queue, measurement, backfill, claim, denial, clock, *, q=None):
    result = claim(q=q)
    _assert_unknown(queue, measurement, denial, clock)
    if result is None:
        # The clock gap also invalidates the GPU's consecutive-low history.
        # Preserve its independent gate: one more fresh GPU observation is
        # required, not an override or a new CPU observation.
        waiting = denial(backfill)
        assert waiting["reason"] == "adaptive_gpu_refused", _diagnostic(queue, backfill)
        assert waiting["evidence"]["decision"]["reason"] == "sharing_probe_not_authorized"
        result = claim(q=q)
    assert result == backfill, _diagnostic(queue, backfill)
    return result


@pytest.mark.parametrize("kind", ["stale", "missing", "future", "partial"])
def test_unknown_observations_do_not_protect_a_long_incumbent_drain(fleet, monkeypatch, kind):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    holder = publish("incumbent", timeout_s=4200)
    assert claim() == holder
    measurement = publish("measurement", measurement=True, pinned=True)
    backfill = publish("backfill")
    _silence(monkeypatch, clock, readings, kind)
    _claim_backfill_after_gap(queue, measurement, backfill, claim, denial, clock)
    assert queue.item_path(pool.CLAIMED, holder).exists()
    assert set(queue.ledger().held_keys()) == {holder, backfill}


def test_absent_initial_observation_is_unknown_not_idle(fleet, monkeypatch):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    measurement = publish("measurement", measurement=True, pinned=True)
    backfill = publish("backfill")
    _silence(monkeypatch, clock, readings, "missing")
    assert claim() == backfill
    _assert_unknown(queue, measurement, denial, clock)
    assert set(queue.ledger().held_keys()) == {backfill}


@pytest.mark.parametrize("repeated", [False, True])
def test_transient_carry_cannot_extend_a_measurement_cpu_observation(fleet, repeated):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    holder = publish("incumbent", timeout_s=4200)
    assert claim() == holder
    measurement = publish("measurement", measurement=True, pinned=True)
    assert claim() is None
    item = pool._read_json(queue.item_path(pool.READY, measurement))
    assert isinstance(item, dict)
    records = queue._host_denial_records()
    carried = queue._carried_withhold(records, item, host="sparklina", now=clock[0])
    assert carried is not None
    tick(adaptive_cpu.MAX_SAMPLE_AGE_S + 1)
    if repeated:
        published = item["published_unix"]
        assert isinstance(published, (int, float))
        generation = f"{measurement}:{float(published)!r}"
        previous = records[generation]
        assert isinstance(previous, dict)
        # A transient/busy pass may be new, but its CPU observation is not.
        records = {generation: dict(previous, reason="transition_busy",
                                   denied_unix=clock[0],
                                   evidence={"withhold_carried": carried})}
        assert "transition_busy" in pool.WITHHOLD_CARRYING_REASONS
    assert queue._carried_withhold(records, item, host="sparklina", now=clock[0]) is None
    assert queue.item_path(pool.READY, measurement).exists()
    assert queue.item_path(pool.CLAIMED, holder).exists()


@pytest.mark.parametrize("bad", ["partial", "huge_timestamp", "carried_unknown"])
def test_a_legacy_bad_observation_is_not_carried(fleet, bad):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    holder = publish("incumbent", timeout_s=4200)
    assert claim() == holder
    measurement = publish("measurement", measurement=True, pinned=True)
    assert claim() is None
    item = pool._read_json(queue.item_path(pool.READY, measurement))
    assert isinstance(item, dict)
    records = queue._host_denial_records()
    published = item["published_unix"]
    assert isinstance(published, (int, float))
    generation = f"{measurement}:{float(published)!r}"
    previous = records[generation]
    assert isinstance(previous, dict)
    evidence = previous["evidence"]
    assert isinstance(evidence, dict)
    decision = evidence["decision"]
    assert isinstance(decision, dict)
    observed = decision["sample"]
    assert isinstance(observed, dict)
    # Before the typed refusal, partial telemetry could protect an exclusive
    # measurement drain even while its timestamp was inside the five seconds.
    if bad == "carried_unknown":
        unproven = queue._carried_withhold(records, item, host="sparklina", now=clock[0])
        assert unproven is not None
        unproven.pop("measurement_cpu_sample_fresh")
        records = {generation: dict(previous, reason="transition_busy",
                                   evidence={"withhold_carried": unproven})}
    else:
        if bad == "partial":
            invalid = dict(decision, reason="measurement_host_not_idle", fresh=False,
                           sample=dict(observed, psi_some=None))
        else:
            invalid = dict(decision, sample=dict(observed, sampled_unix=10 ** 1000))
        records = {generation: dict(previous, evidence=dict(evidence, decision=invalid))}
    assert queue._carried_withhold(records, item, host="sparklina", now=clock[0]) is None
    assert queue.item_path(pool.READY, measurement).exists()
    assert set(queue.ledger().held_keys()) == {holder}


@pytest.mark.parametrize("recover_after_expiry", [False, True])
def test_silence_restart_and_recovery_preserve_the_original_deadline(
        fleet, monkeypatch, recover_after_expiry):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    holder = publish("incumbent", timeout_s=4200)
    assert claim() == holder
    measurement = publish("measurement", measurement=True, pinned=True)
    assert claim() is None
    original = denial(measurement)["evidence"]["withhold"]["drain_until_unix"]
    fresh_sample = adaptive_cpu.Controller.sample
    _silence(monkeypatch, clock, readings, "stale")
    if recover_after_expiry:
        tick(original - clock[0] + 1)
    backfill = publish("backfill", timeout_s=8000)
    restarted = pool.PoolQueue(queue.root)
    _claim_backfill_after_gap(queue, measurement, backfill, claim, denial, clock, q=restarted)
    record = pool._read_json(queue.passes_path(measurement))
    assert isinstance(record, dict)
    episodes = record["measurement_drains"]
    assert isinstance(episodes, dict)
    episode = episodes["sparklina"]
    assert isinstance(episode, dict)
    assert episode["measurement_until_unix"] == original
    monkeypatch.setattr(adaptive_cpu.Controller, "sample", fresh_sample)
    assert claim(q=restarted) is None, "fresh recovery bypassed holder isolation"
    recovered = denial(measurement)["evidence"]["withhold"]
    assert recovered["drain_until_unix"] == original
    assert recovered["withhold"] is (clock[0] < original)
    assert recovered["why"] == (
        "measurement_drain_expired" if recover_after_expiry else "draining_for_measurement")
    assert queue.item_path(pool.READY, measurement).exists()
    assert set(queue.ledger().held_keys()) == {holder, backfill}
