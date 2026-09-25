"""A big GPU row refused on an idle, SW-capped GB10 is not overtaken (#1125).

The 2026-09-25 incident: Stage A quantum ``e22d1e37b562`` (``cpu 10, gpu 1,
mem_gb 101``) waited more than 90 minutes on an idle sparky.  An idle GB10
reports ``sw_power_cap`` (mask ``0x4``) at about 5 W, so its sample reads
``limited``.  The #719 first-job exception admits a GPU job on that device
only on an empty host: no holder and no broker job, CPU-only jobs included.
The drain classifier mapped every limited ``host_or_device_congested``
refusal to "no drain resolves this", so the big row was overtaken, and each
small shard admitted behind it kept a broker job on the host, so the
exception never fired.

These fixtures pin the fix: when occupancy is the only condition the
exception did not meet, the refusal withholds -- the GPU drain for holders,
the whole box for broker jobs -- and a limited refusal for any other reason
(here, thermal) is still overtaken, with or without a holder on the device.

Nothing here touches the live queue, a real pool or a real device.
"""
from __future__ import annotations

import hashlib
from pathlib import Path
import sys
import time

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from prismabuild import adaptive_cpu, adaptive_gpu, pool  # noqa: E402

T0 = 2_000_000.0
GIB = adaptive_gpu.GIB

#: Idle GB10 limiter breakdown with every limiter off.
QUIET_LIMITERS = {
    "gpu_idle": False, "hw_power_brake_slowdown": False, "hw_slowdown": False,
    "hw_thermal_slowdown": False, "sw_power_cap": False,
    "sw_thermal_slowdown": False, "sync_boost": False,
}


def _device(**overrides) -> dict:
    """Sparky at 02:35Z on 2026-09-25: idle, SW cap only, mask ``0x4``."""

    device = {"uuid": "GPU-1", "name": "NVIDIA GB10", "power_w": 5.15,
              "power_limit_w": None, "power_reference_w": 140.,
              "power_reference_scope": "soc_tdp", "memory_domain": "shared_system",
              "limited": True, "sm_clock_mhz": 208., "max_sm_clock_mhz": 3003.,
              "throttle_active_mask": 0x4,
              "throttle_reasons": dict(QUIET_LIMITERS, sw_power_cap=True)}
    device.update(overrides)
    return device


def _thermal_device() -> dict:
    """The same idle device, limited by HW thermal slowdown instead."""

    return _device(throttle_active_mask=0x40,
                   throttle_reasons=dict(QUIET_LIMITERS, hw_thermal_slowdown=True))


def _key(seed: str) -> str:
    return hashlib.sha256(seed.encode()).hexdigest()


def _denial(q: pool.PoolQueue, key: str) -> dict:
    path = adaptive_cpu.local_state_base(q.ledger().base) / pool.CLAIM_DENIALS
    records = adaptive_cpu.read_json(path).get("records", {})
    return next(value for value in records.values() if value["action_key"] == key)


def _key_of(item):
    return None if item is None else item["action_key"]


@pytest.fixture()
def box(tmp_path: Path, monkeypatch):
    """Sparky's admission path with a broker sample the test controls.

    As in ``test_a_gpu_refused_row_is_not_overtaken_by_gpu_rows``: ``tick``
    publishes a fresh broker sample whose ``jobs`` are every holder on the
    ledger, CPU-only holders included, which is what the broker reports.
    """

    clock = [T0]
    monkeypatch.setattr(time, "time", lambda: clock[0])
    monkeypatch.setattr(adaptive_cpu, "action_identity", lambda item: ("shape", False))
    monkeypatch.setattr(adaptive_gpu, "action_contract", lambda item, demand: (
        "shape", False, False, int(demand.get("mem_gb", 0)) * GIB))
    monkeypatch.setattr(adaptive_cpu.Controller, "sample", lambda self: {
        "sampled_unix": clock[0], "busy_cpus": 0., "psi_some": 0.,
        "cpu_count": 20, "interval_s": 1.})
    sample = {"schema": "prismabuild.gpu_capacity.v1", "sample_id": str(T0),
              "sampled_unix": T0, "complete": True, "attributed": True,
              "devices": [_device()],
              "host_total_bytes": 128 * GIB, "host_available_bytes": 120 * GIB,
              "memory_pressure_some": 0., "memory_pressure_full": 0.,
              "cpu_pressure_some": 0., "foreign_processes": [], "jobs": []}
    monkeypatch.setattr(adaptive_gpu.Controller, "sample", lambda self: dict(sample))
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    capacity = {"cpu": 20, "gpu": 1, "mem_gb": 120}
    tiers = {"preferred": list(range(20)), "fallback": []}

    def publish(name: str, resources: dict[str, int]) -> str:
        clock[0] += 0.001
        key = _key(name)
        queue.publish(action_key=key, cas_root=str(tmp_path / "cas"),
                      checkout_root=str(tmp_path), worker_script="worker.py",
                      resources=resources, needs_gpu=bool(resources.get("gpu")))
        return key

    def tick(seconds: float = 2.0) -> None:
        clock[0] += seconds
        sample.update(sampled_unix=clock[0], sample_id=str(clock[0]))
        sample["jobs"] = []
        for key in queue.ledger().held_keys():
            record = {"action_key": key, "nonce": key + "-attempt",
                      "scope_unit": key + "-scope", "sampled_unix": clock[0],
                      "cpu_seconds": .01 * (clock[0] - T0),
                      "wall_seconds": clock[0] - T0, "complete": True}
            adaptive_cpu.write_json(
                adaptive_cpu.local_telemetry_path(queue.ledger().base, key), record)
            sample["jobs"].append({"action_key": key, "nonce": record["nonce"],
                                   "scope_id": record["scope_unit"], "complete": True})

    def claim():
        return _key_of(queue.claim(capacity=capacity, cpu_tiers=tiers,
                                   adaptive_cpu=True, has_gpu=True))

    return queue, clock, sample, publish, tick, claim


BIG = {"cpu": 10, "gpu": 1, "mem_gb": 101}
SMALL_GPU = {"cpu": 4, "gpu": 1, "mem_gb": 6}
SMALL_CPU = {"cpu": 2, "mem_gb": 4}


# -- broker jobs: the whole box drains ------------------------------------------


def test_a_row_refused_for_broker_jobs_withholds_the_whole_box(box) -> None:
    """The incident's steady state: CPU-only shards keep a broker job on the host."""

    queue, clock, sample, publish, tick, claim = box
    shard = publish("cpu-shard-1", SMALL_CPU)
    assert claim() == shard
    big = publish("stage-a-quantum", BIG)
    behind = publish("cpu-shard-2", SMALL_CPU)
    tick()

    assert claim() is None, (
        "a CPU-only shard overtook a row whose only unmet SW-cap condition is "
        "the broker jobs on its host (#1125)")
    denial = _denial(queue, big)
    assert denial["reason"] == "adaptive_gpu_refused_withholding"
    decision = denial["evidence"]["decision"]
    assert decision["reason"] == "host_or_device_congested" and decision["limited"] is True
    exception = decision["sw_cap_idle_exception"]
    assert exception["exception_reason"] == "broker_jobs_present"
    assert exception["holders"] == 0 and exception["broker_jobs"] == 1
    withhold = denial["evidence"]["withhold"]
    assert withhold["mode"] == "exclusive" and withhold["why"] == "drains_soon"
    assert "withheld_kinds" not in denial["evidence"], "a broker-job drain is the whole box"
    assert queue.item_path(pool.READY, behind).exists()

    # The shard leaves; the host is empty and the exception admits the row.
    queue.finish(shard, status="executed", detail={})
    tick()
    assert claim() == big
    metadata = adaptive_cpu.read_json(
        queue.ledger().held_dir / big / adaptive_gpu.METADATA)
    assert metadata["sw_cap_idle_exception"]["exception_reason"] == "sw_cap_idle_first_job"


# -- holders, then broker jobs: the drain ratchets to the whole box -------------


def test_a_row_refused_for_a_gpu_holder_withholds_gpu_rows_then_the_box(box) -> None:
    """The incident's arrival: a GPU holder, then the CPU shards beside it.

    Holders present take the GPU drain, so CPU-only rows still fill the box.
    Once the GPU holder leaves, those rows are the broker jobs in the way, and
    the next refusal takes the whole-box drain.
    """

    queue, clock, sample, publish, tick, claim = box
    holder = publish("gpu-holder", SMALL_GPU)
    assert claim() == holder, "the idle SW-cap exception admits the first GPU job"
    big = publish("stage-a-quantum", BIG)
    for _ in range(pool.STARVATION_FLOOR - 1):
        queue.record_pass(big)
    gpu_row = publish("gpu-test-shard", SMALL_GPU)
    cpu_row = publish("cpu-test-shard", SMALL_CPU)
    tick()

    assert claim() == cpu_row, "CPU-only work stopped filling the box behind a GPU drain"
    denial = _denial(queue, big)
    assert denial["reason"] == "adaptive_gpu_refused_withholding", (
        "a row whose only unmet SW-cap condition is the GPU holder was overtaken (#1125)")
    exception = denial["evidence"]["decision"]["sw_cap_idle_exception"]
    assert exception["exception_reason"] == "holders_present_no_sharing_exception"
    assert denial["evidence"]["withhold"]["mode"] == "gpu"
    assert denial["evidence"]["withheld_kinds"] == ["gpu"]
    waited = _denial(queue, gpu_row)
    assert waited["reason"] == "deferred_behind_withheld_row"
    assert waited["evidence"]["withheld_for"] == big
    assert queue.passes(gpu_row) == 0

    # The GPU holder leaves.  The CPU shard it let through is now in the way.
    queue.finish(holder, status="executed", detail={})
    late = publish("cpu-test-shard-2", SMALL_CPU)
    tick()
    assert claim() is None, "a CPU-only shard overtook the row once only broker jobs remained"
    denial = _denial(queue, big)
    exception = denial["evidence"]["decision"]["sw_cap_idle_exception"]
    assert exception["exception_reason"] == "broker_jobs_present"
    withhold = denial["evidence"]["withhold"]
    assert withhold["mode"] == "exclusive" and withhold["why"] == "drains_soon"
    assert queue.item_path(pool.READY, late).exists()
    assert queue.item_path(pool.READY, gpu_row).exists()

    queue.finish(cpu_row, status="executed", detail={})
    tick()
    assert claim() == big


# -- controls: any other limiter is overtaken -----------------------------------


def test_a_thermally_limited_refusal_with_broker_jobs_is_still_overtaken(box) -> None:
    queue, clock, sample, publish, tick, claim = box
    shard = publish("cpu-shard-1", SMALL_CPU)
    assert claim() == shard
    sample["devices"] = [_thermal_device()]
    big = publish("stage-a-quantum", BIG)
    behind = publish("cpu-shard-2", SMALL_CPU)
    tick()

    assert claim() == behind, "a thermal limiter is the device's state; no drain fixes it"
    denial = _denial(queue, big)
    assert denial["reason"] == "adaptive_gpu_refused"
    assert "withhold" not in denial["evidence"]
    exception = denial["evidence"]["decision"]["sw_cap_idle_exception"]
    assert exception["exception_reason"] == "broker_jobs_present"


def test_a_thermally_limited_refusal_with_a_holder_is_still_overtaken(box) -> None:
    """Occupancy is judged before the device, so the reason alone misleads.

    The exception names the holder even though the device is throttled for
    heat; the classifier must read the limiter too.
    """

    queue, clock, sample, publish, tick, claim = box
    holder = publish("gpu-holder", SMALL_GPU)
    assert claim() == holder
    sample["devices"] = [_thermal_device()]
    big = publish("stage-a-quantum", BIG)
    for _ in range(pool.STARVATION_FLOOR - 1):
        queue.record_pass(big)
    gpu_row = publish("gpu-test-shard", SMALL_GPU)
    cpu_row = publish("cpu-test-shard", SMALL_CPU)
    tick()

    assert claim() == cpu_row
    denial = _denial(queue, big)
    assert denial["reason"] == "adaptive_gpu_refused"
    exception = denial["evidence"]["decision"]["sw_cap_idle_exception"]
    assert exception["exception_reason"] == "holders_present_no_sharing_exception"
    # Nothing is withheld, so the GPU row is judged on its own and refused.
    assert _denial(queue, gpu_row)["reason"] == "adaptive_gpu_refused"
    assert queue.passes(gpu_row) == 1


# -- the classifier ---------------------------------------------------------------


def _exception(reason: str, device: dict | None = None, **overrides) -> dict:
    """What ``sw_cap_idle_first_job`` records for ``device`` when it denies."""

    device = _device() if device is None else device
    holders = [object()] if reason == "holders_present_no_sharing_exception" else []
    jobs = [{"action_key": "a"}] if reason in (
        "holders_present_no_sharing_exception", "broker_jobs_present") else []
    eligible, diagnosis = adaptive_gpu.sw_cap_idle_first_job(
        {"jobs": jobs, "foreign_processes": []}, device, 140., holders=holders,
        measurement=False, pressure=False)
    assert not eligible and diagnosis["exception_reason"] == reason
    diagnosis.update(overrides)
    return diagnosis


def _refusal(exception) -> dict:
    return {"reason": "host_or_device_congested", "limited": True,
            "sw_cap_idle_exception": exception}


@pytest.mark.parametrize("exception,measurement,expected", [
    (_exception("holders_present_no_sharing_exception"), False, ("gpu", False)),
    (_exception("broker_jobs_present"), False, ("exclusive", False)),
    # Occupancy is denied before the device is read: a thermal, power-brake or
    # HW-slowdown limiter, an unknown mask bit, or power or clocks above the
    # idle gate still read as a holder or a broker job.
    (_exception("holders_present_no_sharing_exception", _thermal_device()), False,
     (None, False)),
    (_exception("broker_jobs_present", _thermal_device()), False, (None, False)),
    (_exception("broker_jobs_present", _device(
        throttle_active_mask=0x84,
        throttle_reasons=dict(QUIET_LIMITERS, sw_power_cap=True,
                              hw_power_brake_slowdown=True))), False, (None, False)),
    (_exception("broker_jobs_present", _device(
        throttle_active_mask=0xC,
        throttle_reasons=dict(QUIET_LIMITERS, sw_power_cap=True, hw_slowdown=True))),
     False, (None, False)),
    (_exception("holders_present_no_sharing_exception", _device(
        throttle_active_mask=0x104)), False, (None, False)),
    (_exception("holders_present_no_sharing_exception", _device(power_w=120.)),
     False, (None, False)),
    (_exception("broker_jobs_present", _device(sm_clock_mhz=2800.)), False,
     (None, False)),
    (_exception("broker_jobs_present", pressure=True), False, (None, False)),
    (_exception("broker_jobs_present", foreign_processes=[{"pid": 7}]), False,
     (None, False)),
    (_exception("broker_jobs_present", throttle_reasons=None), False, (None, False)),
    (_exception("holders_present_no_sharing_exception"), True, (None, False)),
    ({"exception_reason": "not_evaluated"}, False, (None, False)),
    (None, False, (None, False)),
], ids=["holders", "broker-jobs", "thermal-holders", "thermal-broker-jobs",
        "power-brake", "hw-slowdown", "unknown-mask-bit", "power-above-gate",
        "clock-above-gate", "pressure", "foreign-recorded", "limiters-missing",
        "measurement", "not-evaluated", "no-exception"])
def test_only_occupancy_on_an_idle_sw_capped_gb10_drains(
    exception, measurement: bool, expected,
) -> None:
    assert pool._adaptive_refusal_drains(
        "adaptive_gpu_refused", _refusal(exception), demand={"gpu": 1},
        measurement=measurement, cpu_count=20) == expected


def test_the_exception_itself_is_unchanged_by_the_drain_question() -> None:
    """#719 stands: a holder or a broker job still denies the first job."""

    for reason in adaptive_gpu.SW_CAP_OCCUPANCY_REASONS:
        diagnosis = _exception(reason)
        assert adaptive_gpu.sw_cap_idle_after_drain(diagnosis) is True
        assert diagnosis["exception_reason"] == reason


def test_the_drain_table_names_exactly_the_occupancy_reasons() -> None:
    """The pool's drain table and the exception's occupancy reasons are one set:
    a reason the helper accepts but the table lacks would drop to "overtaken"."""
    assert set(pool.DRAIN_SW_CAP_IDLE) == set(adaptive_gpu.SW_CAP_OCCUPANCY_REASONS)
