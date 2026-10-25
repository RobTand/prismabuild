"""An elected measurement cannot drain away proven foreign CPU load (#2267)."""
from __future__ import annotations

import pytest

from test_measurement_drains_gpu_backfill import fleet
from prismabuild import _measurement_reservation as reservation, pool


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
