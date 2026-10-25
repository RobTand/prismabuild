"""An elected measurement cannot drain away proven foreign CPU load (#2267)."""
from __future__ import annotations

import pytest

from test_measurement_drains_gpu_backfill import fleet
from prismabuild import _measurement_reservation as reservation, adaptive_cpu, pool


@pytest.mark.parametrize("measurement_count", [1, 2])
def test_foreign_load_suspends_every_elected_measurement_drain(fleet, measurement_count):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    holder = publish("holder", cpu=2, gpu=0, mem_gb=1)
    assert claim() == holder
    measurements = []
    for index in range(measurement_count):
        measurements.append(publish(f"measurement-{index}", measurement=True, priority=index,
                                    cpu=4, gpu=0, mem_gb=6))
        assert claim() is None
    selections = [reservation.selection(
        pool.record_store.read_document(queue.records.passes_path(key)))
        for key in measurements]
    assert all(chosen is not None for chosen in selections)

    readings["busy_cpus"] = 2.0
    readings["foreign"]["2"] = 0.2
    backfill = publish("backfill", cpu=2, gpu=0, mem_gb=1)
    assert claim() == backfill, "a starved measurement's election kept draining the host"
    for key in measurements:
        waiting = denial(key)
        assert waiting["reason"] == "adaptive_cpu_refused_starved"
        assert waiting["evidence"]["decision"]["reason"] == "measurement_foreign_ambient"
        assert queue.records.item_path(pool.record_store.READY, key).exists()
    queue.finish(backfill, status="executed")
    tick(pool.WITHHOLD_CEILING_S + 1)
    next_backfill = publish("backfill-after-restart", cpu=2, gpu=0, mem_gb=1)
    assert claim(q=pool.PoolQueue(queue.root)) == next_backfill
    assert [reservation.selection(
        pool.record_store.read_document(queue.records.passes_path(key)))
        for key in measurements] == selections


@pytest.mark.parametrize("evidence", ["missing", "stale", "clear"])
def test_suspension_requires_current_foreign_evidence_in_each_pass(fleet, evidence):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    holder = publish("holder", cpu=2, gpu=0, mem_gb=1)
    assert claim() == holder
    measurement = publish("measurement", measurement=True, priority=0,
                          cpu=4, gpu=0, mem_gb=6)
    assert claim() is None
    chosen = reservation.selection(
        pool.record_store.read_document(queue.records.passes_path(measurement)))
    readings["busy_cpus"] = 2.0
    readings["foreign"]["2"] = 0.2
    backfill = publish("backfill", cpu=2, gpu=0, mem_gb=1)
    assert claim() == backfill
    queue.finish(backfill, status="executed")
    lower = publish("waiting-for-proof", cpu=2, gpu=0, mem_gb=1)
    kwargs = {}
    if evidence == "missing":
        kwargs["cpu_sampler"] = lambda: {}
    elif evidence == "stale":
        kwargs["cpu_sampler"] = lambda: {
            "sampled_unix": clock.now - adaptive_cpu.MAX_SAMPLE_AGE_S - 1,
            "cpu_count": 20, "interval_s": 1.0, "psi_some": 0.0,
            "busy_cpus": 2.0, "foreign_per_cpu_busy": dict(readings["foreign"])}
    else:
        readings["foreign"]["2"] = 0.0
        readings["busy_cpus"] = 0.0
    assert claim(q=pool.PoolQueue(queue.root), **kwargs) is None
    assert denial(lower)["evidence"]["withheld_for"] == measurement
    assert reservation.selection(
        pool.record_store.read_document(queue.records.passes_path(measurement))) == chosen
    if evidence == "clear":
        queue.finish(holder, status="executed")
        tick(adaptive_cpu.MAX_INTERVAL_S + adaptive_cpu.MAX_SAMPLE_AGE_S + 1)
        assert claim() == measurement
