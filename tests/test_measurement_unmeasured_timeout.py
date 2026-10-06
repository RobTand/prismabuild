"""Unknown baseline protection is bounded; measurement admission is unchanged.

CPU-only real queue/controller simulations reuse the existing fleet fixture.
180 seconds is the proposed fairness cap, not a statistical idle threshold.
"""
from __future__ import annotations

import json
import pytest

from test_measurement_drains_gpu_backfill import fleet, T0  # noqa: F401
from prismabuild import adaptive_cpu, adaptive_gpu, pool

FAIRNESS_BOUND_S = 180


def _unmeasured(fleet, *, gpu=1, holder_gpu=None):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    holder = publish("incumbent", timeout_s=4200,
                     gpu=gpu if holder_gpu is None else holder_gpu)
    assert claim() == holder
    # A restarted sampler has no calibrated idle history while PB holds work.
    base = adaptive_cpu.local_state_base(queue.ledger().base)
    adaptive_cpu.write_json(base / adaptive_cpu.IDLE_BASELINE, {
        "schema": adaptive_cpu.IDLE_BASELINE_SCHEMA, "identity": 20, "samples": [],
    })
    readings["busy_cpus"] = 2.0
    # Genuine unknown attribution, not complete clearance that now correctly
    # reaches measurement_holder (#1422). Keep the unknown-budget proof.
    readings["foreign"] = {}
    measurement = publish("measurement", measurement=True, pinned=True, gpu=gpu)
    assert claim() is None
    decision = denial(measurement)["evidence"]["decision"]
    assert decision["reason"] == "measurement_host_not_idle"
    assert decision["fresh"] is True
    assert decision["baseline"]["basis"] == "unmeasured"
    assert pool._measurement_foreign_clear(decision) is False
    first = denial(measurement)["evidence"]["withhold"]
    assert first["withhold"] is True
    return holder, measurement, first, clock[0]


def _backfill(fleet, measurement, backfill, *, q=None):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    result = claim(q=q)
    if result is None:
        waiting = denial(measurement)["evidence"]
        assert waiting["decision"]["reason"] == "measurement_host_not_idle"
        assert waiting["withhold"]["withhold"] is False, waiting
        # A sampling gap correctly resets the independent GPU sharing proof.
        other = denial(backfill)["evidence"]
        assert other["decision"]["reason"] == "sharing_probe_not_authorized", other
        result = claim(q=q)
    assert result == backfill
    meta = adaptive_cpu.read_json(queue.ledger().held_dir / backfill / adaptive_gpu.METADATA)
    assert meta["probe"] is True


@pytest.mark.parametrize("restart", [False, True])
def test_fresh_unmeasured_baseline_cannot_protect_a_long_drain_indefinitely(fleet, restart):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    holder, measurement, first, started = _unmeasured(fleet)
    backfill = publish("backfill")
    assert claim() is None
    assert denial(backfill)["evidence"]["withheld_for"] == measurement
    tick(FAIRNESS_BOUND_S + 1)
    restarted = pool.PoolQueue(queue.root) if restart else queue
    _backfill(fleet, measurement, backfill, q=restarted)
    waiting = denial(measurement)["evidence"]["withhold"]
    assert waiting["why"] == "measurement_baseline_unknown"
    assert waiting["drain_until_unix"] <= started + FAIRNESS_BOUND_S
    assert queue.item_path(pool.READY, measurement).exists()
    assert queue.item_path(pool.CLAIMED, holder).exists()
    assert queue.ledger().holds_gpu(holder)


def test_a_proven_holder_drain_is_independent_and_unknown_recovery_cannot_renew(fleet):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    holder, measurement, first, started = _unmeasured(fleet)
    tick(FAIRNESS_BOUND_S + 1)
    readings["busy_cpus"] = 0.0
    assert claim() is None
    waiting = denial(measurement)["evidence"]
    assert waiting["decision"]["reason"] == "measurement_holder"
    assert waiting["withhold"]["withhold"] is True
    assert waiting["withhold"]["drain_until_unix"] > clock[0]
    readings["busy_cpus"] = 2.0
    backfill = publish("backfill")
    _backfill(fleet, measurement, backfill, q=pool.PoolQueue(queue.root))
    waiting = denial(measurement)["evidence"]["withhold"]
    assert waiting["why"] == "measurement_baseline_unknown"
    assert waiting["drain_until_unix"] <= started + FAIRNESS_BOUND_S
    assert queue.item_path(pool.READY, measurement).exists()
    assert queue.item_path(pool.CLAIMED, holder).exists()


@pytest.mark.parametrize("repeated", [False, True])
def test_fresh_cpu_carry_cannot_outlive_the_unknown_cutoff(fleet, repeated):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    holder, measurement, first, started = _unmeasured(fleet)
    tick(FAIRNESS_BOUND_S - 2)
    assert claim() is None
    item = pool._read_json(queue.item_path(pool.READY, measurement))
    assert isinstance(item, dict)
    records = queue._host_denial_records()
    carried = queue._carried_withhold(records, item, host="sparklina", now=clock[0])
    assert carried is not None
    assert carried["measurement_unmeasured"] is True
    if repeated:
        published = item["published_unix"]
        assert isinstance(published, (int, float))
        generation = f"{measurement}:{float(published)!r}"
        previous = records[generation]
        assert isinstance(previous, dict)
        records = {generation: dict(previous, reason="transition_busy",
                                   evidence={"withhold_carried": carried})}
    tick(3)
    assert clock[0] - carried["measurement_cpu_sampled_unix"] < adaptive_cpu.MAX_SAMPLE_AGE_S
    assert queue._carried_withhold(records, item, host="sparklina", now=clock[0]) is None
    assert queue.item_path(pool.CLAIMED, holder).exists()


def _episode(queue, key):
    record = pool._read_json(queue.passes_path(key))
    assert isinstance(record, dict)
    episodes = record["measurement_drains"]
    assert isinstance(episodes, dict)
    episode = episodes["sparklina"]
    assert isinstance(episode, dict)
    return record, episode


@pytest.mark.parametrize("bad,valid_json", [(None, True), ("later", True),
                         (True, True), (float("nan"), False), (10**500, True)],
                         ids=["null", "text", "boolean", "nan", "oversized"])
def test_bad_deadline_cannot_be_erased_into_a_new_allowance(fleet, bad, valid_json):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    holder, measurement, first, started = _unmeasured(fleet)
    record, episode = _episode(queue, measurement)
    original = episode["measurement_until_unix"]
    episode["unmeasured_until_unix"] = bad
    # Corrupted on-disk metadata, including a nonstandard nonfinite scalar;
    # the canonical production writer rightly cannot manufacture NaN.
    corrupt = json.dumps(record)
    queue.passes_path(measurement).write_text(corrupt)
    assert claim(q=pool.PoolQueue(queue.root)) is None
    if not valid_json:
        # Nonfinite JSON is refused by the canonical census before the
        # permissive legacy reader can repair deadline fields (#1435).
        assert denial(measurement)["reason"] == "measurement_census_unavailable"
        assert queue.passes_path(measurement).read_text() == corrupt
        backfill = publish("backfill")
        assert claim(q=pool.PoolQueue(queue.root)) is None
        assert denial(backfill)["reason"] == "measurement_census_unavailable"
        assert queue.passes_path(measurement).read_text() == corrupt
        assert set(queue.ledger().held_keys()) == {holder}
        assert queue.item_path(pool.READY, measurement).exists()
        assert queue.item_path(pool.READY, backfill).exists()
        return
    waiting = denial(measurement)["evidence"]["withhold"]
    assert waiting["withhold"] is False and waiting["drain_resolves"] is False
    record, episode = _episode(queue, measurement)
    assert episode["unmeasured_until_unix"] == 0.0
    assert episode["measurement_until_unix"] == original
    backfill = publish("backfill")
    _backfill(fleet, measurement, backfill, q=pool.PoolQueue(queue.root))
    assert _episode(queue, measurement)[1]["unmeasured_until_unix"] == 0.0


def test_expired_unknown_does_not_keep_fitting_cpu_room(fleet):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    holder, measurement, first, started = _unmeasured(fleet, gpu=0)
    # Fill physically free memory without tripping the independent projected
    # CPU-cost gate. Retaining the measurement's 8 GiB would block this row.
    backfill = publish("all-free-memory", cpu=2, gpu=0, mem_gb=112)
    assert claim() is None
    tick(FAIRNESS_BOUND_S + 1)
    assert claim() == backfill, denial(backfill)
    waiting = denial(measurement)["evidence"]["withhold"]
    assert waiting["drain_resolves"] is False
    assert queue.item_path(pool.READY, measurement).exists()
    assert queue.item_path(pool.CLAIMED, holder).exists()


def test_foreign_suspension_does_not_reset_or_clear_unknown_history(fleet):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    holder, measurement, first, started = _unmeasured(fleet)
    original = _episode(queue, measurement)[1]["unmeasured_until_unix"]
    readings["foreign"]["2"] = 0.2
    assert claim() is None
    foreign = _episode(queue, measurement)[1]["foreign_unix"]
    readings["foreign"].pop("2")  # No affirmative clearance proof.
    tick(FAIRNESS_BOUND_S + 1)
    assert claim() is None
    waiting = denial(measurement)["evidence"]["withhold"]
    assert waiting["why"] == "foreign_load" and waiting["withhold"] is False
    episode = _episode(queue, measurement)[1]
    assert episode["foreign_unix"] == foreign
    assert episode["unmeasured_until_unix"] == original


@pytest.mark.parametrize("change", [
    {"source": "adaptive_gpu_refused"}, {"fresh": False}, {"fresh": 1},
    {"reason": "measurement_holder"}, {"reason": "host_pressure"},
    {"basis": "measured"}, {"exceeds": False}, {"basis": None},
])
def test_unknown_classifier_does_not_relabel_independent_protection(change):
    source = change.get("source", "adaptive_cpu_refused")
    decision = {"reason": "measurement_host_not_idle", "fresh": True,
                "baseline": {"basis": "unmeasured", "exceeds": True}}
    assert pool._measurement_unmeasured_refusal("adaptive_cpu_refused", decision)
    for key in ("fresh", "reason"):
        if key in change:
            decision[key] = change[key]
    for key in ("basis", "exceeds"):
        if key in change:
            decision["baseline"][key] = change[key]
    assert not pool._measurement_unmeasured_refusal(source, decision)
