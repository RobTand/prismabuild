"""The foreign-load starve rule derives from the idle verdict itself (#1233).

#1232 judged a measurement's foreign excess with a per-CPU yardstick
(``foreign_cpu[cpu] > IDLE_BUSY_FRACTION``) and only when every held CPU
read quiet.  Both halves were stricter than the invariant they served --
"the excess would survive every holder draining" (#1231):

* a busy holder plus a foreign excess that alone exceeds the verdict still
  withheld the whole box (sparky's steady state: one admission per row
  boundary for as long as the measurement is queued), and
* foreign load spread thinly can exceed the host's idle verdict with no
  single CPU above ``.05``.

The rule now re-judges the idle verdict on the foreign busy that survives
every drain -- ``S = sum(foreign_per_cpu_busy[c] for c not in held)``,
judged by the same :func:`_judge_idle` against the same baseline or prior
-- and starves whenever that alone exceeds, whatever the held CPUs do.  PSI
cannot be attributed and stays conservative: a verdict exceeded only on
``psi_some`` withholds as before, and holder-only busy keeps the #924
withhold-then-admit behavior.
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


def test_a_busy_holder_with_a_foreign_excess_of_its_own_still_starves(
    queue: pool.PoolQueue, clock, monkeypatch,
) -> None:
    """The mixed shape (#1233 case 1): held CPU busy AND foreign-alone excess.

    RED (the defect): the foreign busy on unheld CPUs alone exceeds the
    unmeasured prior (19 x .06 = 1.14 > 1.0), but a busy held CPU made
    #1232's gate keep the conservative whole-box withhold, so the row behind
    stayed unclaimed.  GREEN: draining the holder cannot clear the foreign
    excess, so the measurement starves and the row behind claims.
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
    assert claim() == behind, (
        "a foreign excess that survives the holder draining still withheld "
        "the box behind a busy holder")
    denial = _denial(queue, measurement)
    assert denial["reason"] == "adaptive_cpu_refused_starved", denial
    decision = denial["evidence"]["decision"]
    assert decision["reason"] == "measurement_foreign_load"
    assert decision["foreign_busy_cpus"] == pytest.approx(1.14), decision
    assert decision["held_cpus"] == [0], decision
    assert decision["foreign_idle"]["exceeds"] is True, decision


def test_thin_foreign_spread_exceeding_the_verdict_with_no_cpu_above_the_fraction(
    queue: pool.PoolQueue, clock, monkeypatch,
) -> None:
    """The thin shape (#1233 case 2): every CPU under .05, S over the baseline.

    The host's idle window is seeded at busy 0.3 (measured, no holders), then
    foreign busy spreads 0.04 across all 20 CPUs: S = 0.8 exceeds the
    measured baseline maximum 0.3 while no CPU crosses .05, so no per-CPU
    yardstick ever fires.  GREEN: the re-judged verdict starves the
    measurement and names S.
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
    assert claim() == behind
    denial = _denial(queue, measurement)
    assert denial["reason"] == "adaptive_cpu_refused_starved", denial
    decision = denial["evidence"]["decision"]
    assert decision["reason"] == "measurement_foreign_load", decision
    assert decision["foreign_busy_cpus"] == pytest.approx(0.8), decision
    assert decision["foreign_idle"]["basis"] == "measured", decision
    assert decision["foreign_idle"]["exceeds"] is True, decision


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


def _controller(queue):
    ledger = queue.ledger()
    return adaptive_cpu.Controller(ledger, {"preferred": list(range(20)),
                                            "fallback": []})


def _sample_at(unix, *, busy, psi=0.034, interval=1.0, holders=None):
    return {"sampled_unix": unix, "cpu_count": 20, "interval_s": interval,
            "psi_some": psi, "busy_cpus": busy}


def test_an_identity_change_rejudges_on_the_fresh_window(tmp_path):
    """#1236 review: the reset verdict judged against nothing, not old samples.

    A changed CPU topology is a different host: the verdict is judged on a
    fresh empty window, and the re-judgement must not read the old-identity
    samples the pass's state file still carries.
    """
    queue = pool.PoolQueue(tmp_path / "q")
    queue.ensure_layout()
    controller = _controller(queue)
    seed = _sample_at(2000000.0, busy=0.3)
    assert controller.idle(seed, [])["basis"] == "unmeasured"
    grew = _sample_at(2000001.0, busy=0.3)
    assert controller.idle(grew, [])["basis"] == "measured"

    # A topology change under the controller: the state file still carries
    # the old-identity samples, but the next judgement resets the window.
    state = adaptive_cpu.read_json(controller.base / adaptive_cpu.IDLE_BASELINE)
    state["identity"] = 999
    adaptive_cpu.write_json(controller.base / adaptive_cpu.IDLE_BASELINE, state)
    controller._idle = None
    after = _sample_at(2000002.0, busy=2.0)
    verdict = controller.idle(after, [])
    assert verdict["state"] == "idle" and verdict["basis"] == "unmeasured"

    rejudged = controller.rejudge_idle_foreign(after, [], busy_cpus=0.5)
    assert rejudged["exceeds"] is False, rejudged
    assert rejudged["basis"] == "unmeasured", rejudged


def test_a_holder_tail_rejudges_against_nothing(tmp_path):
    """#1236 review: a forced holder tail must not re-judge as foreign load.

    The tail verdict is forced ``exceeds`` and judges against no reference at
    all; a re-judgement that read the remembered window could read a holder's
    own tail as load no drain clears -- and in that state no CPU is held, so
    every tail CPU counts as unheld -- starving the measurement.
    """
    queue = pool.PoolQueue(tmp_path / "q")
    queue.ensure_layout()
    controller = _controller(queue)
    seed = _sample_at(2000000.0, busy=0.3, interval=100.0)
    assert controller.idle(seed, [])["basis"] == "unmeasured"
    grew = _sample_at(2000001.0, busy=0.3, interval=100.0)
    assert controller.idle(grew, [])["basis"] == "measured"
    # A holder is seen inside the next sample's interval, then leaves.
    controller.idle(_sample_at(2000050.0, busy=1.0), ["holder"])
    tail = _sample_at(2000050.5, busy=2.0, interval=1.0)
    verdict = controller.idle(tail, [])
    assert verdict["state"] == "holder_tail" and verdict["exceeds"] is True

    rejudged = controller.rejudge_idle_foreign(tail, [], busy_cpus=2.0)
    assert rejudged["exceeds"] is False, rejudged


def test_a_stale_cache_key_does_not_rejudge(tmp_path):
    """#1236 review: the re-judgement is bound to this sample and holder state.

    The cached verdict belongs to ``(sampled_unix, holders)``; a different
    sample must not be re-judged against it.
    """
    queue = pool.PoolQueue(tmp_path / "q")
    queue.ensure_layout()
    controller = _controller(queue)
    seed = _sample_at(2000000.0, busy=0.3)
    assert controller.idle(seed, [])["basis"] == "unmeasured"
    grew = _sample_at(2000001.0, busy=0.3)
    assert controller.idle(grew, [])["basis"] == "measured"

    other = _sample_at(2000006.0, busy=2.0)
    rejudged = controller.rejudge_idle_foreign(other, [], busy_cpus=2.0)
    assert rejudged["state"] == "stale-verdict", rejudged
    assert rejudged["exceeds"] is False, rejudged
