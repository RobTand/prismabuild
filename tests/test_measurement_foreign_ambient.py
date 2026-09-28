"""The measurement's foreign verdict judges its own CPUs, never a sum (#1185).

#1233 re-judged the idle verdict on the foreign busy summed across every
unheld CPU (``surviving``).  A thin spread no single CPU feels -- 0.04 on
all 20 -- starved the measurement exactly like a hot neighbour on the CPUs
the measurement would actually be given.  The verdict now judges only the
CPUs PB would allocate to the measurement (the ledger's own selection
rule) against measured ambient, plus the live window's PSI via the
unchanged idle gate:

* foreign above ambient on the measurement's own CPUs refuses
  ``measurement_foreign_ambient`` -- typed, never drainable, never green;
* foreign anywhere else is not the measurement's problem and keeps the old
  ``measurement_host_not_idle`` withhold;
* the typed check runs on every fresh attributed sample, so a persistent
  excess on those CPUs keeps refusing no matter how long it runs -- no
  excursion promotion for the measurement's CPUs;
* the refusal names the foreign pids on the excess CPUs, read once per
  sample.

Thresholds derive from sparklina's measured series (2026-09-28, 15 live
samples over 650 s at ~43 s cadence): ambient per-CPU foreign peaks at
0.080 while the incident's loaded CPUs read 0.14-0.28, so the per-CPU
line is 0.10 -- clear of both populations.  PSI stays window-relative
(the kernel's 10 s average is stable at 0.0029 +/- 0.0003 ambient and the
established window already judges it); only per-CPU foreign moves to the
absolute line, which is where the sum was the defect.
"""
from __future__ import annotations

import hashlib
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "tools" / "fleet")]
from prismabuild import adaptive_cpu, pool  # noqa: E402

T0 = 2_000_000.0


def _key(seed: str) -> str:
    return hashlib.sha256(seed.encode()).hexdigest()


@pytest.fixture()
def clock(monkeypatch):
    now = [T0]
    monkeypatch.setattr(pool, "_now", lambda: now[0])
    monkeypatch.setattr(adaptive_cpu.time, "time", lambda: now[0])
    return now


@pytest.fixture(autouse=True)
def generation_actions(monkeypatch):
    monkeypatch.setattr(adaptive_cpu, "action_identity", lambda item: ("shape", False))


@pytest.fixture()
def queue(tmp_path: Path) -> pool.PoolQueue:
    q = pool.PoolQueue(tmp_path / "pb-queue")
    q.ensure_layout()
    return q


def _publish(queue, clock, key, resources):
    clock[0] += 0.001
    queue.publish(action_key=key, cas_root="/cas", checkout_root="/co",
                  worker_script="/w.py", resources=resources)
    return key


def _denial(q: pool.PoolQueue, key: str) -> dict:
    path = adaptive_cpu.local_state_base(q.ledger().base) / pool.CLAIM_DENIALS
    records = adaptive_cpu.read_json(path).get("records", {})
    return next(value for value in records.values() if value["action_key"] == key)


def _claim(q, capacity, *, tiers):
    item = q.claim(capacity=capacity, cpu_tiers=tiers, adaptive_cpu=True)
    return None if item is None else item["action_key"]


def _sample(clock, *, per_cpu, psi=0.003):
    return {
        "sampled_unix": clock[0], "cpu_count": 20, "interval_s": 1.,
        "psi_some": psi, "busy_cpus": sum(per_cpu.values()),
        "per_cpu_busy": dict(per_cpu),
        "foreign_per_cpu_busy": dict(per_cpu),
    }


def _flat(level, *, quiet=()):
    return {str(cpu): (0. if cpu in quiet else level) for cpu in range(20)}


def test_foreign_excess_on_the_measurements_own_cpus_is_typed_ambient(
    queue: pool.PoolQueue, clock, monkeypatch,
) -> None:
    """RED: a hot neighbour on the CPUs the measurement would be given.

    The measurement needs 4 CPUs -- the ledger's first four free tokens,
    CPUs 0-3 -- and CPUs 0-1 read 0.20 foreign while the other 18 read
    0.01.  The old box-wide sum (0.42 + 0.18 = 0.60) stays under the
    unmeasured prior (1.0), so the old rule seeds the window and admits
    the measurement onto loaded CPUs.  GREEN: the refusal is typed
    ``measurement_foreign_ambient``, names those CPUs and their readings,
    starves (the row behind claims), and never drains.
    """

    capacity = {"cpu": 20, "mem_gb": 120}
    tiers = {"preferred": list(range(20)), "fallback": []}
    per_cpu = _flat(0.01)
    per_cpu["0"] = per_cpu["1"] = 0.20
    monkeypatch.setattr(adaptive_cpu.Controller, "sample",
                        lambda self: _sample(clock, per_cpu=per_cpu))
    measurement = _key("measurement")
    monkeypatch.setattr(adaptive_cpu, "action_identity",
                        lambda item: ("shape", item["action_key"] == measurement))

    def claim():
        return _claim(queue, capacity, tiers=tiers)

    _publish(queue, clock, measurement, {"cpu": 4, "mem_gb": 40})
    for _ in range(pool.STARVATION_FLOOR - 1):
        queue.record_pass(measurement)
    behind = _publish(queue, clock, _key("behind"), {"cpu": 4, "mem_gb": 40})
    assert claim() == behind, (
        "foreign load on the measurement's own CPUs must starve it, "
        "not withhold the box")
    denial = _denial(queue, measurement)
    assert denial["reason"] == "adaptive_cpu_refused_starved", denial
    decision = denial["evidence"]["decision"]
    assert decision["reason"] == "measurement_foreign_ambient", decision
    assert decision["foreign_cpus"] == [0, 1], decision
    assert decision["per_cpu_ambient_max"] == pytest.approx(0.10), decision


def test_foreign_only_off_the_measurements_cpus_keeps_the_withhold(
    queue: pool.PoolQueue, clock, monkeypatch,
) -> None:
    """RED: foreign load everywhere except where the measurement goes.

    CPUs 10-19 read 0.30 foreign (S = 3.0, over every baseline) while the
    measurement's CPUs 0-3 read 0.01.  The old sum starves the measurement
    for load it will never run on.  GREEN: no typed verdict -- the refusal
    stays ``measurement_host_not_idle`` and the box withholds for the
    measurement instead of overtaking it.
    """

    capacity = {"cpu": 20, "mem_gb": 120}
    tiers = {"preferred": list(range(20)), "fallback": []}
    per_cpu = _flat(0.01)
    for cpu in range(10, 20):
        per_cpu[str(cpu)] = 0.30
    monkeypatch.setattr(adaptive_cpu.Controller, "sample",
                        lambda self: _sample(clock, per_cpu=per_cpu))
    measurement = _key("measurement")
    monkeypatch.setattr(adaptive_cpu, "action_identity",
                        lambda item: ("shape", item["action_key"] == measurement))

    _publish(queue, clock, measurement, {"cpu": 4, "mem_gb": 40})
    behind = _publish(queue, clock, _key("behind"), {"cpu": 4, "mem_gb": 40})
    got = _claim(queue, capacity, tiers=tiers)
    assert got != behind, (
        "foreign load off the measurement's CPUs must not starve it: "
        f"the row behind claimed as {got}")
    denial = _denial(queue, measurement)
    decision = denial["evidence"]["decision"]
    assert decision["reason"] == "measurement_host_not_idle", decision


def test_persistent_excess_on_predicted_cpus_never_promotes(
    queue: pool.PoolQueue, clock, monkeypatch,
) -> None:
    """RED: an excess that outlives the window still refuses.

    Foreign 0.20 sits on the measurement's CPUs pass after pass.  The old
    rule seeds the first sample (0.58 under the 1.0 prior) and reads every
    later one as its own baseline, admitting the measurement onto loaded
    CPUs.  GREEN: every pass refuses ``measurement_foreign_ambient`` -- the typed
    check is absolute against measured ambient, not window-relative, so
    there is nothing for the excursion to promote.
    """

    capacity = {"cpu": 20, "mem_gb": 120}
    tiers = {"preferred": list(range(20)), "fallback": []}
    per_cpu = _flat(0.01)
    per_cpu["0"] = per_cpu["1"] = 0.20
    monkeypatch.setattr(adaptive_cpu.Controller, "sample",
                        lambda self: _sample(clock, per_cpu=per_cpu))
    measurement = _key("measurement")
    monkeypatch.setattr(adaptive_cpu, "action_identity",
                        lambda item: ("shape", item["action_key"] == measurement))

    _publish(queue, clock, measurement, {"cpu": 4, "mem_gb": 40})
    for _ in range(12):
        clock[0] += 120.0
        queue.record_pass(measurement)
        got = _claim(queue, capacity, tiers=tiers)
        assert got != measurement, (
            f"measurement admitted onto foreign-loaded CPUs at t={clock[0]}")
    denial = _denial(queue, measurement)
    assert denial["evidence"]["decision"]["reason"] == "measurement_foreign_ambient"


def test_ambient_refusal_names_foreign_pids(
    queue: pool.PoolQueue, clock, monkeypatch,
) -> None:
    """RED: the typed refusal carries the processes behind the excess.

    A census of /proc, read once per sample and capped, names candidate
    pids per excess CPU so the refusal is actionable without a second
    tool.  GREEN: ``foreign_pids`` maps each excess CPU to a pid list.
    """

    capacity = {"cpu": 20, "mem_gb": 120}
    tiers = {"preferred": list(range(20)), "fallback": []}
    per_cpu = _flat(0.01)
    per_cpu["0"] = per_cpu["1"] = 0.20
    monkeypatch.setattr(adaptive_cpu.Controller, "sample",
                        lambda self: _sample(clock, per_cpu=per_cpu))
    measurement = _key("measurement")
    monkeypatch.setattr(adaptive_cpu, "action_identity",
                        lambda item: ("shape", item["action_key"] == measurement))

    _publish(queue, clock, measurement, {"cpu": 4, "mem_gb": 40})
    assert _claim(queue, capacity, tiers=tiers) != measurement
    decision = _denial(queue, measurement)["evidence"]["decision"]
    assert decision["reason"] == "measurement_foreign_ambient", decision
    pids = decision["foreign_pids"]
    assert set(pids) == {0, 1}, decision
    for cpu, names in pids.items():
        assert names and all(type(pid) is int for pid in names), (cpu, names)


def test_refused_seeds_join_provisional_and_measure_at_depth() -> None:
    """RED: an unmeasured host under load still accumulates evidence.

    #1014 drops a sample an empty baseline's prior refuses, so a host that
    boots under foreign load never seeds its window and stays
    ``basis: unmeasured`` forever.  GREEN: holder-free refused samples join
    flagged provisional; the window still judges ``unmeasured`` until five
    of them span 180 s, then measures against them.
    """
    state: dict = {}
    now = [T0]

    def sample(level):
        now[0] += 60.0
        per_cpu = {str(cpu): level for cpu in range(20)}
        return {
            "sampled_unix": now[0], "cpu_count": 20, "interval_s": 60.,
            "psi_some": 0.003, "busy_cpus": sum(per_cpu.values()),
            "per_cpu_busy": dict(per_cpu),
            "foreign_per_cpu_busy": dict(per_cpu),
        }

    verdicts = []
    for _ in range(5):
        verdict, state, _ = adaptive_cpu.idle_judgement_with_reference(
            state, sample(0.10), holders=[], identity="host")
        verdicts.append(verdict)
    assert all(v["basis"] == "unmeasured" for v in verdicts[:4]), verdicts
    assert verdicts[4]["basis"] == "measured", verdicts
    # The latching verdict judges against the four provisional seeds; the
    # fifth joins them as the measured baseline at once.
    assert verdicts[4]["samples"] == 4, verdicts
