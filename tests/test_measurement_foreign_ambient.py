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
import io
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
    CPUs 0-3 -- and CPUs 0-1 read 0.60 foreign (busy 1.38, over the 1.0
    unmeasured prior) while the other 18 read 0.01.  The old rule refuses
    ``measurement_host_not_idle`` and withholds the whole box for load no
    drain clears, so the row behind never claims.  GREEN: the refusal is
    typed ``measurement_foreign_ambient``, names those CPUs and their
    readings, starves (the row behind claims), and never drains.
    """

    capacity = {"cpu": 20, "mem_gb": 120}
    tiers = {"preferred": list(range(20)), "fallback": []}
    per_cpu = _flat(0.01)
    per_cpu["0"] = per_cpu["1"] = 0.60
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
    assert decision["per_cpu_foreign_max"] == pytest.approx(0.10), decision


def test_foreign_only_off_the_measurements_cpus_keeps_the_withhold(
    queue: pool.PoolQueue, clock, monkeypatch,
) -> None:
    """RED: foreign load everywhere except where the measurement goes.

    CPUs 10-19 read 0.30 foreign (S = 3.0, over every baseline) while the
    measurement's CPUs 0-3 read 0.01.  The old sum starves the measurement
    for load it will never run on.  GREEN: no typed verdict -- the refusal
    stays ``measurement_host_not_idle``.  Pool mechanics: with no holders
    in the way there is nothing a drain could clear, so the pool reports
    the measurement starved and the admittable row behind still claims;
    what the redesign pins is the DECISION -- host_not_idle, never the
    typed ambient and never the old box-wide sum -- while the measurement
    stays portable to the box that is actually idle.
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
    for _ in range(pool.STARVATION_FLOOR - 1):
        queue.record_pass(measurement)
    behind = _publish(queue, clock, _key("behind"), {"cpu": 4, "mem_gb": 40})
    assert _claim(queue, capacity, tiers=tiers) == behind, (
        "the box flows around a measurement no drain can place")
    denial = _denial(queue, measurement)
    assert denial["reason"] == "adaptive_cpu_refused_starved", denial
    decision = denial["evidence"]["decision"]
    assert decision["reason"] == "measurement_host_not_idle", decision


def test_provisional_depth_measures_a_loaded_host(
    queue: pool.PoolQueue, clock, monkeypatch,
) -> None:
    """GREEN: refused seeds latch into a baseline at depth, never before.

    Foreign 0.60 sits on the measurement's CPUs pass after pass (busy
    1.38, over the 1.0 prior).  #1014 drops those refused seeds, so the
    old rule withholds forever on a host that boots under load.  The
    provisional path instead measures the steady load once five seeds
    span 180 s -- the first rounds refuse typed-ambient while unmeasured,
    never promoted into an excursion, and after the latch the load *is*
    the baseline, so the measurement is admitted onto the host it measured.
    """

    capacity = {"cpu": 20, "mem_gb": 120}
    tiers = {"preferred": list(range(20)), "fallback": []}
    per_cpu = _flat(0.01)
    per_cpu["0"] = per_cpu["1"] = 0.60
    monkeypatch.setattr(adaptive_cpu.Controller, "sample",
                        lambda self: _sample(clock, per_cpu=per_cpu))
    measurement = _key("measurement")
    monkeypatch.setattr(adaptive_cpu, "action_identity",
                        lambda item: ("shape", item["action_key"] == measurement))

    _publish(queue, clock, measurement, {"cpu": 4, "mem_gb": 40})
    # Before depth the host is unmeasured, so the prior refuses and the
    # typed check fires on the 0.60 excess: starved with nobody behind,
    # hence unclaimed.  The latch never goes blind or soft first.
    for _ in range(2):
        clock[0] += 60.0
        queue.record_pass(measurement)
        assert _claim(queue, capacity, tiers=tiers) is None
    # Past depth the steady load is the baseline: the measurement runs.
    admitted = False
    for _ in range(4):
        clock[0] += 60.0
        queue.record_pass(measurement)
        if _claim(queue, capacity, tiers=tiers) == measurement:
            admitted = True
            break
    assert admitted, "five refused seeds spanning 180 s must measure the host"
    first = _denial(queue, measurement)
    assert first["evidence"]["decision"]["reason"] == "measurement_foreign_ambient", first


def test_ambient_refusal_names_foreign_pids(
    queue: pool.PoolQueue, clock, monkeypatch,
) -> None:
    """The typed refusal carries the processes behind the excess.

    A census of /proc, read once per sample and capped, names candidate
    pids per excess CPU so the refusal is actionable without a second
    tool.  The census runs against a fake /proc: the test pins exactly
    which pids live on the excess CPUs, so it passes on an idle box and
    a loaded one alike (#1310 review).
    """

    on_cpu = {"101": 0, "102": 0, "103": 1}

    def fake_stat(pid: str) -> bytes:
        fields = [b"R"] + [b"7"] * 35 + [str(on_cpu[pid]).encode()]
        return pid.encode() + b" (fake) " + b" ".join(fields) + b" 0 0"

    monkeypatch.setattr(adaptive_cpu.os, "listdir",
                        lambda path: [*on_cpu, "self"])
    real_open = open

    def fake_open(path, mode="r", *args, **kwargs):
        if (isinstance(path, str) and path.startswith("/proc/")
                and path.endswith("/stat")):
            return io.BytesIO(fake_stat(path.split("/")[2]))
        return real_open(path, mode, *args, **kwargs)

    monkeypatch.setattr("builtins.open", fake_open)
    capacity = {"cpu": 20, "mem_gb": 120}
    tiers = {"preferred": list(range(20)), "fallback": []}
    per_cpu = _flat(0.01)
    per_cpu["0"] = per_cpu["1"] = 0.60
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
    # String keys: the denial record is read back through JSON, where int
    # dict keys do not survive (normalized where the evidence is built).
    assert pids == {"0": [101, 102], "1": [103]}, decision


def test_foreign_pid_census_is_capped_and_reads_stat_field_39(
    tmp_path: Path, monkeypatch,
) -> None:
    """The PID census pins without /proc: cap, keys, field (#1310 review).

    Six foreign pids on CPU 0 keep four; one on CPU 1 keeps one; the
    non-numeric entry never opens.  The CPU comes from stat field 39 --
    index 36 of the fields after ``(comm)`` -- so the filler around it is
    distinct from the answer.
    """
    from types import SimpleNamespace

    on_cpu = {"11": 0, "12": 0, "13": 0, "14": 0, "15": 0, "16": 0,
              "17": 1}

    def fake_stat(pid: str) -> bytes:
        fields = [b"R"] + [b"7"] * 35 + [str(on_cpu[pid]).encode()]
        return pid.encode() + b" (fake) " + b" ".join(fields) + b" 0 0"

    monkeypatch.setattr(adaptive_cpu.os, "listdir",
                        lambda path: [*on_cpu, "self"])
    real_open = open

    def fake_open(path, mode="r", *args, **kwargs):
        if (isinstance(path, str) and path.startswith("/proc/")
                and path.endswith("/stat")):
            return io.BytesIO(fake_stat(path.split("/")[2]))
        return real_open(path, mode, *args, **kwargs)

    monkeypatch.setattr("builtins.open", fake_open)
    ledger = SimpleNamespace(base=tmp_path / "ledger")
    controller = adaptive_cpu.Controller(
        ledger, {"preferred": [0, 1], "fallback": []})
    found = controller._foreign_pids({0, 1}, 1234.0)
    assert found == {0: [11, 12, 13, 14], 1: [17]}, found


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
    # The prior refuses everything, as an unmeasured host's fixed line
    # refuses load: only then are the refused samples seeds at all.
    prior_rule = lambda current: True

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
            state, sample(0.10), holders=[], identity="host",
            prior_rule=prior_rule)
        verdicts.append(verdict)
    assert all(v["basis"] == "unmeasured" for v in verdicts[:4]), verdicts
    assert verdicts[4]["basis"] == "measured", verdicts
    # The latching verdict judges against the four provisional seeds; the
    # fifth joins them as the measured baseline at once.
    assert verdicts[4]["samples"] == 4, verdicts
    # The completing pass strips every provisional flag (#1310 review: the
    # strip used to sit only in the refused-seed branch, which a completing
    # pass never takes, so four flags persisted and the baseline read as
    # one sample).  The next verdict measures against all five seeds.
    assert not any(s.get("provisional") for s in state["samples"]), state
    later, state, _ = adaptive_cpu.idle_judgement_with_reference(
        state, sample(0.10), holders=[], identity="host",
        prior_rule=prior_rule)
    assert later["basis"] == "measured", later
    assert later["samples"] >= 5, later
