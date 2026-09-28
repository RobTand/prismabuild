"""The measurement verdict on foreign load is typed per-CPU (#1185).

#1233's box-wide surviving-sum is retired: it let foreign load far from the
measurement's own CPUs overtake a row no drain could ever place.  The rule
now checks only the measurement's own predicted CPUs against
``PER_CPU_FOREIGN_MAX`` (0.10, sparklina-derived), excluding PB-held busy:

* foreign above the line on those CPUs refuses ``measurement_foreign_ambient``
  and starves (no drain clears it; the rows behind keep claiming),
* anything else -- sub-threshold foreign, load off those CPUs, a sample
  that cannot answer the question -- refuses ``measurement_host_not_idle``
  and withholds, exactly as before,
* holder-only busy keeps the #924 withhold-then-admit behavior.
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


def _sample(clock, *, per_cpu, psi=0.034):
    return {
        "sampled_unix": clock[0], "cpu_count": 20, "interval_s": 1.,
        "psi_some": psi, "busy_cpus": sum(per_cpu.values()),
        "per_cpu_busy": dict(per_cpu),
        "foreign_per_cpu_busy": dict(per_cpu),
    }


def _flat(level, *, quiet=()):
    return {str(cpu): (0. if cpu in quiet else level) for cpu in range(20)}


def test_a_busy_holder_with_subthreshold_foreign_withholds(
    queue: pool.PoolQueue, clock, monkeypatch,
) -> None:
    """The mixed shape (#1233 case 1, retired): held CPU busy AND foreign.

    The foreign 0.06 on the measurement's predicted CPUs sits under the
    0.10 per-CPU line, so the typed check does not fire: the refusal stays
    ``measurement_host_not_idle`` and the box withholds behind the busy
    holder.  Above-line foreign on those same CPUs starves instead -- see
    ``test_measurement_foreign_ambient.py``.
    """

    capacity = {"cpu": 20, "mem_gb": 120}
    tiers = {"preferred": list(range(20)), "fallback": []}
    per_cpu = _flat(0.06, quiet=())
    per_cpu["0"] = 0.9          # the holder's own CPU, busy with its work
    monkeypatch.setattr(adaptive_cpu.Controller, "sample",
                        lambda self: _sample(clock, per_cpu=per_cpu))
    measurement = _key("measurement")
    monkeypatch.setattr(adaptive_cpu, "action_identity",
                        lambda item: ("shape", item["action_key"] == measurement))

    def claim():
        return _claim(queue, capacity, tiers=tiers)

    holder = _publish(queue, clock, _key("holder"), {"cpu": 1, "mem_gb": 1})
    assert claim() == holder
    _publish(queue, clock, measurement, {"cpu": 4, "mem_gb": 40})
    for _ in range(pool.STARVATION_FLOOR - 1):
        queue.record_pass(measurement)
    behind = _publish(queue, clock, _key("behind"), {"cpu": 4, "mem_gb": 40})
    assert claim() is None, (
        "sub-threshold foreign on the measurement's CPUs is not proven "
        "ambient: the box withholds behind the busy holder")
    denial = _denial(queue, measurement)
    assert denial["reason"] == "adaptive_cpu_refused_withholding", denial
    decision = denial["evidence"]["decision"]
    assert decision["reason"] == "measurement_host_not_idle", decision


def test_thin_foreign_spread_below_the_per_cpu_line_withholds(
    queue: pool.PoolQueue, clock, monkeypatch,
) -> None:
    """The thin shape (#1233 case 2, retired): every CPU under the line.

    Foreign busy spreads 0.04 across all 20 CPUs: S = 0.8 exceeds the
    measured baseline maximum 0.3 while no CPU crosses the per-CPU line, so
    the typed check does not fire and the refusal stays
    ``measurement_host_not_idle`` with the box withheld.  The #1233 sum is
    retired by design: sub-line foreign is contention-unknown, and
    withholding it is the conservative answer (#1185).
    """

    capacity = {"cpu": 20, "mem_gb": 120}
    tiers = {"preferred": list(range(20)), "fallback": []}
    state = {"level": 0.015}
    monkeypatch.setattr(adaptive_cpu.Controller, "sample",
                        lambda self: _sample(clock, per_cpu=_flat(state["level"])))
    measurement = _key("measurement")
    monkeypatch.setattr(adaptive_cpu, "action_identity",
                        lambda item: ("shape", item["action_key"] == measurement))

    def claim():
        return _claim(queue, capacity, tiers=tiers)

    # Seed the idle window at busy 0.3 with no holder: the prior does not
    # refuse 0.3 (<= .05 x 20), so the sample joins the baseline.
    seeder = _publish(queue, clock, _key("seeder"), {"cpu": 1, "mem_gb": 1})
    assert claim() == seeder
    queue.finish(seeder, status="executed")
    clock[0] += 1.0
    state["level"] = 0.04
    _publish(queue, clock, measurement, {"cpu": 4, "mem_gb": 40})
    for _ in range(pool.STARVATION_FLOOR - 1):
        queue.record_pass(measurement)
    behind = _publish(queue, clock, _key("behind"), {"cpu": 4, "mem_gb": 40})
    assert claim() is None, (
        "a thin foreign spread under the per-CPU line withholds, "
        "never starves")
    denial = _denial(queue, measurement)
    assert denial["reason"] == "adaptive_cpu_refused_withholding", denial
    decision = denial["evidence"]["decision"]
    assert decision["reason"] == "measurement_host_not_idle", decision


def test_holder_only_busy_with_foreign_below_the_verdict_keeps_the_withhold(
    queue: pool.PoolQueue, clock, monkeypatch,
) -> None:
    """#1233 case 3: no foreign-alone excess, the #924 withhold stands.

    The holder's own held CPUs are the busy ones and the unheld CPUs read
    quiet: S is far below the prior, so the re-judged verdict does not
    exceed, the refusal stays measurement_host_not_idle, and the box
    withholds while the holder drains and admits the measurement once the
    host reads idle.
    """

    capacity = {"cpu": 20, "mem_gb": 120}
    tiers = {"preferred": list(range(20)), "fallback": []}
    state = {"busy": True}
    per_cpu = _flat(0.)

    def sample():
        level = 0.6 if state["busy"] else 0.
        per_cpu["0"] = per_cpu["1"] = level
        return _sample(clock, per_cpu=per_cpu)

    monkeypatch.setattr(adaptive_cpu.Controller, "sample", lambda self: sample())
    measurement = _key("measurement")
    monkeypatch.setattr(adaptive_cpu, "action_identity",
                        lambda item: ("shape", item["action_key"] == measurement))

    def claim():
        return _claim(queue, capacity, tiers=tiers)

    holder = _publish(queue, clock, _key("holder"), {"cpu": 2, "mem_gb": 1})
    assert claim() == holder
    _publish(queue, clock, measurement, {"cpu": 4, "mem_gb": 40})
    for _ in range(pool.STARVATION_FLOOR - 1):
        queue.record_pass(measurement)
    behind = _publish(queue, clock, _key("behind"), {"cpu": 4, "mem_gb": 40})
    assert claim() is None, "the holder's own busy load must still withhold"
    denial = _denial(queue, measurement)
    assert denial["reason"] == "adaptive_cpu_refused_withholding", denial
    assert denial["evidence"]["decision"]["reason"] == "measurement_host_not_idle"
    assert denial["evidence"]["withhold"]["why"] == "drains_soon"
    queue.finish(holder, status="executed")
    state["busy"] = False
    clock[0] += adaptive_cpu.MAX_INTERVAL_S + adaptive_cpu.MAX_SAMPLE_AGE_S + 1
    assert claim() == measurement
