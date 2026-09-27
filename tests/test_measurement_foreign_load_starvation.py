"""A measurement's foreign-load excess starves it, never withholding (#1231).

The 2026-09-27 incident: a priority-1 GPU measurement reached the head of
sparky's walk just after a campaign quantum finished.  Every pass refused it
``measurement_host_not_idle`` -- the host never reads idle against the fixed
unmeasured prior (``busy_cpus`` 3.44 > .05 x 20), because the operator
sessions' load is always there and #1014 keeps refused seeds out of the
window -- and the refusal withheld the whole box, so 211 ready campaign rows
waited behind it with the GPU at about 5 W until the coordinator withdrew
the action by hand.

The busy CPUs were foreign load, not pool holders: no holder drain could
clear them.  #1160 already separates held from foreign CPUs for
``host_pressure``; the measurement-idle gate now uses the same separation.
When a fresh sample's attribution proves the excess busy is on CPUs no
holder holds -- every held CPU quiet, foreign busy above the idle fraction
on unheld ones -- the measurement is refused ``measurement_foreign_load``
and starves: it keeps its place, the denial is visible under
``pbstatus --starvation`` with the foreign CPUs named, and the rows behind
it keep claiming.  A drain that is really pending -- the busy is on a held
CPU -- keeps the #924 withhold-then-admit behavior, and a sample without
attribution keeps the conservative refusal exactly as before.
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


def _foreign_sample(clock, *, held_quiet, foreign_level=0.43, spread=15):
    """The incident's sample shape on a 20-CPU box.

    ``busy_cpus`` sums to 3.44, above the unmeasured prior line
    ``busy_cpus > 1 (.05 x 20 CPUs)``.  The holder's own CPU (0) is quiet;
    the busy is foreign (the operator sessions' load) on every other CPU,
    and the per-CPU attribution says so (#1210's counters).
    """
    per_cpu = {"0": 0. if held_quiet else 0.43}
    per_cpu.update({str(cpu): foreign_level for cpu in range(1, 5)})
    rest = 3.44 - 4 * foreign_level
    per_cpu.update({str(cpu): rest / spread for cpu in range(5, 5 + spread)})
    return {
        "sampled_unix": clock[0], "cpu_count": 20, "interval_s": 1.,
        "psi_some": 0.034, "busy_cpus": 3.44,
        "per_cpu_busy": per_cpu, "foreign_per_cpu_busy": dict(per_cpu),
    }


def test_foreign_busy_with_a_transient_holder_starves_the_measurement(
    queue: pool.PoolQueue, clock, monkeypatch,
) -> None:
    """The incident: holders present, the excess busy all foreign.

    RED (the defect): the measurement withheld the whole box behind a
    transient holder whose CPU read quiet, so the lower-priority row stayed
    unclaimed.  GREEN: the attribution proves no drain clears the busy, the
    measurement is refused ``measurement_foreign_load`` and starves, and the
    row behind it claims on the same poll.
    """

    capacity = {"cpu": 20, "mem_gb": 120}
    tiers = {"preferred": list(range(20)), "fallback": []}
    state = {"held_quiet": True}
    monkeypatch.setattr(adaptive_cpu.Controller, "sample",
                        lambda self: _foreign_sample(clock, **state))
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
    assert claim() == behind, (
        "a measurement whose idle excess is foreign load withheld the box "
        "behind a holder whose CPU read quiet")
    denial = _denial(queue, measurement)
    assert denial["reason"] == "adaptive_cpu_refused_starved", denial
    assert denial["evidence"]["decision"]["reason"] == "measurement_foreign_load"
    assert denial["evidence"]["decision"]["foreign_cpus"], denial
    assert denial["evidence"]["starved"]["why"] == "foreign_load"


def test_foreign_busy_with_no_holders_names_the_foreign_cpus(
    queue: pool.PoolQueue, clock, monkeypatch,
) -> None:
    """With no holder at all the box always flowed; the refusal now says why.

    The pre-fix valve starved this shape already (``_withhold_verdict``'s
    no-holder branch), but the denial carried no attribution: the decision
    read ``measurement_host_not_idle`` with the busy attributed to nobody.
    The refusal now names the load the pool does not own.
    """

    capacity = {"cpu": 20, "mem_gb": 120}
    tiers = {"preferred": list(range(20)), "fallback": []}
    monkeypatch.setattr(adaptive_cpu.Controller, "sample",
                        lambda self: _foreign_sample(clock, held_quiet=True))
    measurement = _key("measurement")
    monkeypatch.setattr(adaptive_cpu, "action_identity",
                        lambda item: ("shape", item["action_key"] == measurement))

    def claim():
        return _claim(queue, capacity, tiers=tiers)

    _publish(queue, clock, measurement, {"cpu": 4, "mem_gb": 40})
    for _ in range(pool.STARVATION_FLOOR - 1):
        queue.record_pass(measurement)
    behind = _publish(queue, clock, _key("behind"), {"cpu": 4, "mem_gb": 40})
    assert claim() == behind
    denial = _denial(queue, measurement)
    assert denial["reason"] == "adaptive_cpu_refused_starved", denial
    assert denial["evidence"]["decision"]["reason"] == "measurement_foreign_load"
    assert denial["evidence"]["decision"]["foreign_cpus"][0] == 1, denial
    assert denial["evidence"]["decision"]["baseline"]["basis"] == "unmeasured"


def test_holder_busy_keeps_the_withhold_then_admits(
    queue: pool.PoolQueue, clock, monkeypatch,
) -> None:
    """No regression on #924: a drain that is really pending still holds.

    The attribution shows the busy on the holder's own held CPU: the
    measurement_host_not_idle withhold stands while the holder drains, and
    the measurement is admitted once the host reads idle.  The row behind
    it waits exactly as it did before #1231.
    """

    capacity = {"cpu": 20, "mem_gb": 120}
    tiers = {"preferred": list(range(20)), "fallback": []}
    state = {"busy": True}
    per_cpu = {str(cpu): 0. for cpu in range(20)}

    def sample():
        level = 0.6 if state["busy"] else 0.
        per_cpu["0"] = per_cpu["1"] = level
        return {
            "sampled_unix": clock[0], "cpu_count": 20, "interval_s": 1.,
            "psi_some": 0.034, "busy_cpus": 2. * level,
            "per_cpu_busy": dict(per_cpu),
            "foreign_per_cpu_busy": dict(per_cpu),
        }

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
    # The drain completes and the host reads idle: the measurement runs.
    queue.finish(holder, status="executed")
    state["busy"] = False
    clock[0] += adaptive_cpu.MAX_INTERVAL_S + adaptive_cpu.MAX_SAMPLE_AGE_S + 1
    assert claim() == measurement
