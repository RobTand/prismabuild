"""A host's recorded non-draining GPU refusal must not starve CPU work (#1526)."""
from __future__ import annotations

import pytest

from prismabuild import _measurement_reservation as reservation, pool
from test_measurement_drains_gpu_backfill import fleet as fleet_fixture

fleet = fleet_fixture


@pytest.mark.parametrize("fault,reason", [
    ("foreign", "host_or_device_congested"),
    ("sample", "sample_invalid_or_stale"),
    ("thermal", "host_or_device_congested"),
])
def test_a_non_draining_gpu_refusal_releases_cpu_across_passes(fleet, fault, reason):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    if fault == "foreign":
        sample["foreign_processes"] = [{"pid": 12345}]
    elif fault == "sample":
        readings["gpu_stale"] = True
    else:
        sample["devices"][0].update(limited=True, throttle_active_mask=0x40,
                                    throttle_active_reasons=["hw_thermal_slowdown"])
    gpu = publish("unrunnable-gpu", priority=0, tags=["gb10"])
    assert claim() is None
    refused = denial(gpu)
    assert refused["evidence"]["decision"]["reason"] == reason
    if fault == "foreign":
        assert refused["evidence"]["decision"]["foreign_processes"]

    for index in range(3):
        cpu = publish(f"portable-cpu-{index}", priority=1, cpu=4, gpu=0,
                      mem_gb=8, tags=["gb10"])
        assert claim() == cpu, denial(cpu)
        queue.finish(cpu, status="executed")
        assert queue.item_path(pool.READY, gpu).exists()


def test_a_measurement_elected_on_the_other_host_does_not_veto_cpu_here(fleet):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    sky_holder = publish("sky-holder", priority=-10, timeout_s=1200,
                         cpu=2, gpu=1, mem_gb=8, tags=["sparky"])
    assert claim("sparky") == sky_holder
    lina_holder = publish("lina-holder", priority=-10, timeout_s=1200,
                          cpu=2, gpu=1, mem_gb=8, pinned=True)
    assert claim("sparklina") == lina_holder
    gpu = publish("measurement-on-sky", measurement=True, priority=0,
                  cpu=8, gpu=1, mem_gb=48, tags=["gb10"])
    assert claim("sparky") is None
    selected = reservation.selection(pool._read_json(queue.passes_path(gpu)) or {})
    assert selected is not None and selected["host"] == "sparky"
    assert claim("sparklina") is None
    refused = denial(gpu, "sparklina")
    assert refused["evidence"]["withhold"]["why"] == "measurement_reserved_on_other_host"

    for index in range(3):
        cpu = publish(f"cpu-beside-other-election-{index}", priority=1,
                      cpu=4, gpu=0, mem_gb=8, tags=["gb10"])
        assert claim("sparklina") == cpu, denial(cpu, "sparklina")
        queue.finish(cpu, status="executed")
        assert queue.item_path(pool.READY, gpu).exists()
        assert queue.ledger("sparky").held_keys() == [sky_holder]


@pytest.mark.parametrize("change", [
    "other-host", "other-action", "other-publication", "other-attempt",
    "unknown-reason", "unknown-decision", "pool-drain",
])
def test_only_this_attempts_known_non_draining_verdict_releases_the_veto(change):
    item = {"action_key": "a" * 64, "published_unix": 1000.0, "attempts": 0}
    record = {**item, "host": "sparklina", "reason": "adaptive_gpu_refused",
              "evidence": {"decision": {"reason": "host_or_device_congested"},
                           "drain_resolves": False}}
    records = {f"{item['action_key']}:1000.0": record}
    assert pool.PoolQueue._ready_gpu_refused_here(item, records, host="sparklina")
    if change == "other-host":
        record["host"] = "sparky"
    elif change == "other-action":
        record["action_key"] = "b" * 64
    elif change == "other-publication":
        record["published_unix"] = 999.0
    elif change == "other-attempt":
        record["attempts"] = 1
    elif change == "unknown-reason":
        record["reason"] = "transition_busy"
    elif change == "unknown-decision":
        record["evidence"]["decision"] = {}
    else:
        record["evidence"]["drain_resolves"] = True
    assert not pool.PoolQueue._ready_gpu_refused_here(item, records, host="sparklina")


def test_an_elsewhere_gang_verdict_releases_only_its_recorded_host():
    item = {"action_key": "a" * 64, "published_unix": 1000.0, "attempts": 0}
    records = {f"{item['action_key']}:1000.0": {
        **item, "host": "sparklina", "reason": "gang_member_elected_elsewhere",
        "evidence": {"election": {"host": "sparky"}},
    }}
    assert pool.PoolQueue._ready_gpu_refused_here(item, records, host="sparklina")
    assert not pool.PoolQueue._ready_gpu_refused_here(item, records, host="sparky")
