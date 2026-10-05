"""A pending measurement drains PB-owned GPU load without preemption (#1185).

Only clocks and sampler observations are controlled. Sealed identities, both
adaptive controllers, queue ordering, sharing probes, ledgers and denial files
are real. These CPU-only simulations never submit work or use a GPU themselves.
"""
from __future__ import annotations

from pathlib import Path
import platform
import sys
import time

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "tools" / "fleet")]
from prismabuild import adaptive_cpu, adaptive_gpu, core as pb, pool  # noqa: E402
import pbstatus  # noqa: E402

T0 = 2_000_000.0
GIB = adaptive_gpu.GIB


@pytest.fixture()
def fleet(tmp_path, monkeypatch):
    clock = [T0]
    monkeypatch.setattr(time, "time", lambda: clock[0])
    monkeypatch.setattr(pool.socket, "gethostname", lambda: "sparklina")
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    capacity = {"cpu": 20, "gpu": 1, "mem_gb": 120}
    tiers = {"preferred": list(range(20)), "fallback": []}
    readings = {"busy_cpus": 0.0, "foreign": {str(cpu): 0.0 for cpu in range(20)}}
    sample = {
        "schema": "prismabuild.gpu_capacity.v1", "sample_id": str(T0),
        "sampled_unix": T0, "complete": True, "attributed": True,
        "devices": [{"uuid": "GPU-1", "name": "NVIDIA GB10", "power_w": 15.0,
                     "power_limit_w": None, "power_reference_w": 140.0,
                     "power_reference_scope": "soc_tdp", "memory_domain": "shared_system",
                     "limited": False}],
        "host_total_bytes": 128 * GIB, "host_available_bytes": 120 * GIB,
        "memory_pressure_some": 0.0, "memory_pressure_full": 0.0,
        "cpu_pressure_some": 0.0, "foreign_processes": [], "jobs": [],
    }
    monkeypatch.setattr(adaptive_cpu.Controller, "sample", lambda self: {
        "sampled_unix": clock[0], "cpu_count": 20, "interval_s": 1.0,
        "psi_some": 0.0, "busy_cpus": readings["busy_cpus"],
        "foreign_per_cpu_busy": dict(readings["foreign"]),
    })
    monkeypatch.setattr(adaptive_gpu.Controller, "sample", lambda self:
                        None if readings.get("gpu_missing") else dict(sample))
    monkeypatch.setattr(adaptive_cpu.Controller, "_foreign_pids", lambda *args: {})
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    (checkout / "task.py").write_text("print('fixture')\n")
    cas = pb.PrismaBuildCAS(tmp_path / "cas")

    def publish(name, *, measurement=False, pinned=False, priority=-10, timeout_s=4200,
                cpu=2, gpu=1, mem_gb=8, tags=None, retry_safe=None, max_attempts=3):
        clock[0] += 0.001
        action = pb.seal_action({
            "schema": pb.ACTION_SCHEMA_V2,
            "task": {"definition_id": "tests/gpu-backfill", "definition_version": "v1",
                     "task_class": "measurement" if measurement else "generation",
                     "determinism": "deterministic", "artifact_family": "generic",
                     "artifact_kind": "generic", "argv": [sys.executable, "task.py"],
                     "working_directory": ".", "result_path": name},
            "inputs": [], "code_closure": pb.build_code_closure(checkout, ["task.py"]),
            "params": {"gpu_exclusive": False, "execution_timeout_s": timeout_s},
            "environment": {"variables": {}, "toolchain": {
                **pb.executable_toolchain_contract(sys.executable),
                "system": platform.system(), "machine": platform.machine(),
                "libc": "-".join(platform.libc_ver()),
            } if measurement else {}},
            "execution_scope": {"portability": "host_class_keyed" if measurement else "portable",
                                "platform_key": None, "host_class": "gb10" if measurement else None},
        })
        cas.publish_action_request(action)
        key = action["action_key"]
        queue.publish(action_key=key, cas_root=str(cas.root), checkout_root=str(checkout),
                      worker_script="worker.py", resources={"cpu": cpu, "gpu": gpu, "mem_gb": mem_gb},
                      needs_gpu=bool(gpu),
                      tags=(["sparklina"] if pinned else []) if tags is None else tags,
                      priority=priority, retry_safe=retry_safe, max_attempts=max_attempts)
        return key

    def tick(seconds=2.0):
        clock[0] += seconds
        sampled = clock[0] - adaptive_gpu.MAX_SAMPLE_AGE_S - 1 if readings.get("gpu_stale") else clock[0]
        if readings.get("gpu_future"):
            sampled = clock[0] + 1
        sample.update(sampled_unix=sampled, sample_id=str(clock[0]), jobs=[])
        for key in queue.ledger().held_keys():
            record = {"action_key": key, "nonce": key + "-attempt", "scope_unit": key + "-scope",
                      "sampled_unix": clock[0], "cpu_seconds": 0.01 * (clock[0] - T0),
                      "wall_seconds": clock[0] - T0, "complete": True}
            adaptive_cpu.write_json(adaptive_cpu.local_telemetry_path(queue.ledger().base, key), record)
            sample["jobs"].append({"action_key": key, "nonce": record["nonce"],
                                   "scope_id": record["scope_unit"], "complete": True})

    def claim(host="sparklina", q=None):
        monkeypatch.setattr(pool.socket, "gethostname", lambda: host)
        tick(0.01)
        active = q or queue
        result = active.claim(capacity=capacity, cpu_tiers=tiers, adaptive_cpu=True,
                              has_gpu=True, tags=["gb10", host])
        if result is not None and len(active.ledger(host).held_keys()) == 1:
            key = result["action_key"]
            record = pool._read_json(active.item_path(pool.CLAIMED, key))
            assert isinstance(record, dict)
            if (record.get("needs_gpu")
                    and not adaptive_gpu.action_contract(record, record["resources"])[1]):
                # Observe a settled, real sharing permission. Do not mock the
                # broker or reserve/spend this permission: the next claim must
                # still decide for itself, and its metadata proves a probe.
                tick(adaptive_gpu.SETTLE_S + 1)
                cpu = adaptive_cpu.Controller(active.ledger(host), tiers)
                gpu = adaptive_gpu.Controller(active.ledger(host))
                with cpu.locked():
                    permission = gpu.decision(record, record["resources"])
                assert permission is not None and permission["probe"] is True, gpu.last_decision
        return None if result is None else result["action_key"]

    def denial(key, host="sparklina"):
        base = adaptive_cpu.local_state_base(queue.ledger(host).base)
        records = adaptive_cpu.read_json(base / pool.CLAIM_DENIALS).get("records", {})
        return next(value for value in records.values() if value["action_key"] == key)

    return queue, clock, readings, sample, publish, tick, claim, denial


def test_a_measurement_drains_on_its_first_denial_and_portable_backfill_uses_the_other_host(fleet):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    holder = publish("incumbent")
    assert claim() == holder
    measurement = publish("measurement", measurement=True, pinned=True)
    backfill = publish("backfill")
    assert claim() is None, "GPU backfill joined before the measurement's starvation floor"
    waiting = denial(measurement)
    assert waiting["evidence"]["withhold"]["why"] == "draining_for_measurement"
    assert denial(backfill)["evidence"]["withheld_for"] == measurement
    assert claim("sparky") == backfill, "the measurement drained a host it cannot use"
    queue.finish(holder, status="executed")
    tick(adaptive_gpu.PSI_AVG10_S + 1)
    assert claim("sparklina") == measurement, denial(measurement)


def test_an_old_bounded_gpu_incumbent_does_not_reopen_the_host_to_sharing_backfill(fleet):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    holder = publish("old-incumbent")
    assert claim() == holder
    tick(pool.WITHHOLD_CEILING_S + 100)
    assert queue.holder_bound(holder)["bound"] == "long"
    for _ in range(2):
        tick(1)
        cpu = adaptive_cpu.Controller(queue.ledger(), {"preferred": list(range(20)), "fallback": []})
        gpu = adaptive_gpu.Controller(queue.ledger())
        record = pool._read_json(queue.item_path(pool.CLAIMED, holder))
        with cpu.locked():
            permission = gpu.decision(record, record["resources"])
    assert permission is not None and permission["probe"] is True, gpu.last_decision
    measurement = publish("measurement", measurement=True, pinned=True)
    for _ in range(pool.STARVATION_FLOOR):
        queue.record_pass(measurement)
    backfill = publish("backfill")
    assert claim() is None, "a later sharing probe refilled a long measurement drain"
    assert denial(measurement)["evidence"]["withhold"]["why"] == "draining_for_measurement"
    queue.finish(holder, status="executed")
    tick(adaptive_gpu.PSI_AVG10_S + 1)
    assert claim() == measurement, denial(measurement)
    assert queue.item_path(pool.READY, backfill).exists()


def test_affirmative_foreign_clear_replaces_the_old_measurement_foreign_veto(fleet):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    holder = publish("incumbent")
    assert claim() == holder
    measurement = publish("measurement", measurement=True, pinned=True)
    readings["busy_cpus"] = 2.0
    readings["foreign"]["2"] = 0.2
    for _ in range(pool.STARVATION_FLOOR):
        assert claim() is None
    assert denial(measurement)["evidence"]["decision"]["reason"] == "measurement_foreign_ambient"
    readings["foreign"]["2"] = 0.0
    backfill = publish("backfill")
    assert claim() is None, "the historical foreign veto bypassed a current PB-owned drain"
    waiting = denial(measurement)
    assert waiting["evidence"]["decision"]["measurement_pool_drain"]["foreign_clear"] is True
    assert waiting["evidence"]["withhold"]["why"] == "draining_for_measurement"
    queue.finish(holder, status="executed")
    readings["busy_cpus"] = 0.0
    tick(adaptive_gpu.PSI_AVG10_S + 1)
    assert claim() == measurement, denial(measurement)
    assert queue.item_path(pool.READY, backfill).exists()


def test_current_foreign_cpu_load_never_becomes_a_measurement_drain(fleet):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    holder = publish("incumbent")
    assert claim() == holder
    measurement = publish("measurement", measurement=True, pinned=True)
    readings["busy_cpus"] = 2.0
    readings["foreign"]["2"] = 0.2
    for _ in range(pool.STARVATION_FLOOR):
        assert claim() is None
    backfill = publish("backfill")
    assert claim() == backfill
    gpu_meta = adaptive_cpu.read_json(queue.ledger().held_dir / backfill / adaptive_gpu.METADATA)
    assert gpu_meta["probe"] is True
    waiting = denial(measurement)
    assert waiting["evidence"]["decision"]["reason"] == "measurement_foreign_ambient"
    assert waiting["evidence"]["withhold"]["withhold"] is False
    assert queue.item_path(pool.READY, measurement).exists()


def test_a_restart_and_higher_priority_refill_do_not_renew_the_measurement_episode(fleet):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    holder = publish("incumbent")
    assert claim() == holder
    measurement = publish("measurement", measurement=True, pinned=True)
    assert claim() is None
    until = denial(measurement)["evidence"]["withhold"]["drain_until_unix"]
    higher = publish("higher-priority", priority=0, timeout_s=8000)
    assert claim() == higher
    restarted = pool.PoolQueue(queue.root)
    assert claim(q=restarted) is None
    assert denial(measurement)["evidence"]["withhold"]["drain_until_unix"] == until
    tick(until - clock[0] + 1)
    assert claim(q=restarted) is None
    expired = denial(measurement)["evidence"]["withhold"]
    # #1419: this finite original opportunity elected a host. Time alone
    # neither retires that election nor proves either incumbent released.
    assert expired["withhold"] is True
    assert expired["why"] == "measurement_reservation_waiting"
    assert expired["selection"]["host"] == "sparklina"
    assert expired["drain_until_unix"] == until
    tick(10)
    assert claim(q=restarted) is None
    assert denial(measurement)["evidence"]["withhold"]["drain_until_unix"] == until


@pytest.mark.parametrize("bad", ["partial", "negative", "stale_cpu", "future_cpu", "stale_gpu",
                               "missing_gpu", "future_gpu", "incomplete_gpu", "unattributed_gpu"])
def test_unproven_readings_cannot_retire_a_measurement_foreign_veto(fleet, monkeypatch, bad):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    holder = publish("incumbent")
    assert claim() == holder
    measurement = publish("measurement", measurement=True, pinned=True)
    readings["busy_cpus"] = 2.0
    readings["foreign"]["2"] = 0.2
    assert claim() is None
    assert denial(measurement)["evidence"]["withhold"]["why"] == "foreign_load"
    def foreign_stamp():
        record = pool._read_json(queue.passes_path(measurement))
        assert isinstance(record, dict)
        episodes = record["measurement_drains"]
        assert isinstance(episodes, dict)
        episode = episodes["sparklina"]
        assert isinstance(episode, dict)
        return episode.get("foreign_unix")

    foreign_before = foreign_stamp()
    assert isinstance(foreign_before, (int, float))
    readings["foreign"]["2"] = 0.0
    if bad == "partial":
        del readings["foreign"]["2"]
    elif bad == "negative":
        readings["foreign"]["2"] = -0.1
    elif bad in ("stale_cpu", "future_cpu"):
        offset = -adaptive_cpu.MAX_SAMPLE_AGE_S - 1 if bad == "stale_cpu" else 1
        monkeypatch.setattr(adaptive_cpu.Controller, "sample", lambda self: {
            "sampled_unix": clock[0] + offset, "cpu_count": 20, "interval_s": 1.0,
            "psi_some": 0.0, "busy_cpus": 2.0,
            "foreign_per_cpu_busy": dict(readings["foreign"]),
        })
    elif bad == "stale_gpu":
        readings["gpu_stale"] = True
    elif bad == "missing_gpu":
        readings["gpu_missing"] = True
    elif bad == "future_gpu":
        readings["gpu_future"] = True
    elif bad == "incomplete_gpu":
        sample["complete"] = False
    else:
        sample["attributed"] = False
    assert claim() is None
    waiting = denial(measurement)["evidence"]
    if bad in ("stale_cpu", "future_cpu"):
        assert waiting["decision"]["reason"] == "measurement_sampler_unknown"
        assert waiting["decision"]["fresh"] is False
        assert waiting.get("withhold", {}).get("withhold") is not True
        # Unknown telemetry does not clear the old foreign veto or become a
        # drain. Its original host/generation record remains unchanged.
        assert foreign_stamp() == foreign_before
    else:
        verdict = waiting["withhold"]
        assert verdict["why"] == "foreign_load" and verdict["withhold"] is False
    assert queue.item_path(pool.READY, measurement).exists()


def test_an_unbounded_incumbent_has_only_one_bounded_measurement_episode(fleet):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    holder = publish("no-declared-end", timeout_s=None)
    assert claim() == holder
    measurement = publish("measurement", measurement=True, pinned=True)
    assert claim() is None
    initial = denial(measurement)["evidence"]["withhold"]
    assert initial["drain_until_unix"] - clock[0] == pytest.approx(pool.WITHHOLD_CEILING_S)
    for _ in range(3):
        tick(pool.WITHHOLD_CEILING_S / 4)
        assert claim() is None
        verdict = denial(measurement)["evidence"]["withhold"]
        assert verdict["why"] == "draining_for_measurement"
        assert verdict["drain_until_unix"] == initial["drain_until_unix"]
    tick(pool.WITHHOLD_CEILING_S / 4 + 1)
    assert claim() is None
    verdict = denial(measurement)["evidence"]["withhold"]
    # The bounded attention episode ends, but the host election made during
    # it does not: an undeclared incumbent no longer reopens refill (#1419).
    assert verdict["why"] == "measurement_reservation_waiting" and verdict["withhold"] is True
    assert verdict["selection"]["host"] == "sparklina"
    assert verdict["drain_until_unix"] == initial["drain_until_unix"]


def test_portable_measurements_keep_distinct_host_deadlines(fleet):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    unknown = publish("unknown-incumbent", timeout_s=None)
    assert claim("sparky") == unknown
    bounded = publish("bounded-incumbent", timeout_s=4200)
    assert claim("sparklina") == bounded
    measurement = publish("portable-measurement", measurement=True)
    assert claim("sparky") is None
    first = denial(measurement, "sparky")["evidence"]["withhold"]["drain_until_unix"]
    assert claim("sparklina") is None
    second = denial(measurement)["evidence"]["withhold"]["drain_until_unix"]
    assert second > first + 2000, (first, second)
    assert claim("sparky", q=pool.PoolQueue(queue.root)) is None
    assert denial(measurement, "sparky")["evidence"]["withhold"]["drain_until_unix"] == first


def test_transient_foreign_load_cannot_renew_a_measurement_episode(fleet):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    holder = publish("unbounded-incumbent", timeout_s=None)
    assert claim() == holder
    measurement = publish("measurement", measurement=True, pinned=True)
    assert claim() is None
    original = denial(measurement)["evidence"]["withhold"]["drain_until_unix"]
    tick(100)
    readings["busy_cpus"] = 2.0
    readings["foreign"]["2"] = 0.2
    assert claim() is None
    assert denial(measurement)["evidence"]["withhold"]["why"] == "foreign_load"
    readings["foreign"]["2"] = 0.0
    assert claim(q=pool.PoolQueue(queue.root)) is None
    assert denial(measurement)["evidence"]["withhold"]["drain_until_unix"] == original


def test_an_expired_measurement_does_not_withhold_for_a_holder_tail(fleet):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    holder = publish("unbounded-incumbent", timeout_s=None)
    assert claim() == holder
    measurement = publish("measurement", measurement=True, pinned=True)
    assert claim() is None
    until = denial(measurement)["evidence"]["withhold"]["drain_until_unix"]
    tick(until - clock[0] + 1)
    assert claim() is None
    queue.finish(holder, status="executed")
    assert claim() is None, "a holder tail still fails the measurement idle gate"
    verdict = denial(measurement)["evidence"]["withhold"]
    # The tail is not renewed into a new episode; the host election keeps
    # lower-priority refill out until the measurement itself runs (#1419).
    assert verdict["why"] == "measurement_reservation_waiting" and verdict["withhold"] is True
    assert verdict["drain_until_unix"] == until
    tick(adaptive_gpu.PSI_AVG10_S + 1)
    assert claim() == measurement, denial(measurement)


def test_measurement_deadlines_survive_transient_carry_without_renewal(fleet):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    holder = publish("incumbent")
    assert claim() == holder
    measurement = publish("measurement", measurement=True, pinned=True)
    assert claim() is None
    item = pool._read_json(queue.item_path(pool.READY, measurement))
    records = queue._host_denial_records()
    carried = queue._carried_withhold(records, item, host="sparklina", now=clock[0])
    assert carried is not None
    generation = f"{measurement}:{float(item['published_unix'])!r}"
    for reason in sorted(pool.WITHHOLD_CARRYING_REASONS):
        value = dict(records[generation], reason=reason, evidence={"withhold_carried": carried})
        repeated = queue._carried_withhold({generation: value}, item, host="sparklina", now=clock[0])
        assert repeated is not None and repeated["drain_until_unix"] == carried["drain_until_unix"]
        assert queue._carried_withhold({generation: value}, item, host="sparklina",
                                     now=carried["drain_until_unix"] + 1) is None
    row = {"action_key_prefix": measurement[:12], "state": "READY", "admission_denials": [
        dict(denial(measurement), age_s=0.0)]}
    assert f"draining_for_measurement {measurement[:12]}" in "\n".join(
        pbstatus.pool_job_lines([row], {"empty": False}))
