"""A refused item withholds its box only while the box drains soon (#924).

The 2026-09-23 incident: a ready GPU measurement (``fed96645``) sat behind a
stream of 2-CPU test shards on sparky for about 25 minutes after the GPU it
needed had gone free.  Two things let the shards keep overtaking it.  An
adaptive refusal -- the measurement was refused ``measurement_host_not_idle``
-- never withheld the box at all, and a token shortage stopped withholding
once the item's own first-denial clock passed ``WITHHOLD_CEILING_S``, whatever
was in its way.  Each shard that finished was replaced by the next one before
the item could take the room.

The line is now drawn per *holder*, from what the holder declared
(``PoolQueue.holder_bound``): an item withholds while the holders in its way
drain soon, and is reported starved, keeping its place, while they do not.
The opposite failure is the 2026-09-04 one, kept green in ``test_pool.py``: a
holder that runs for hours or a day (R12, a progress-governed campaign action
with no total timeout) must not hold the box shut for anyone.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "tools" / "fleet")]
from prismabuild import adaptive_cpu, core as pb, pool  # noqa: E402
import pbstatus  # noqa: E402

T0 = 2_000_000.0


def _key(seed: str) -> str:
    return hashlib.sha256(seed.encode()).hexdigest()


@pytest.fixture()
def clock(monkeypatch):
    now = [T0]
    # One clock for the pool and the adaptive controller.  ``adaptive_cpu.time``
    # is the ``time`` module, so this also covers the controller's ``now``.
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


def _publish(q: pool.PoolQueue, clock, key: str, resources, **kw) -> str:
    # A distinct publish time per item, so the ready order is the order the
    # fixture publishes in rather than a digest tie-break.
    clock[0] += 0.001
    q.publish(action_key=key, cas_root=kw.pop("cas_root", "/cas"),
              checkout_root=kw.pop("checkout_root", "/co"),
              worker_script="/w.py", resources=resources, **kw)
    return key


def _sealed(tmp_path: Path, name: str, *, timeout_s=None, progress=False):
    """A sealed request that declares how long the action may run."""

    checkout = tmp_path / f"checkout-{name}"
    checkout.mkdir()
    (checkout / "task.py").write_text("print('fixture')\n")
    params: dict[str, object] = {}
    if progress:
        params[pb.PROGRESS_PARAM] = {
            "schema": pb.PROGRESS_POLICY_SCHEMA_V1,
            "phases": [{"name": "run", "grace_s": 600}]}
    if timeout_s is not None:
        params["execution_timeout_s"] = timeout_s
    action = pb.seal_action({
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {"definition_id": f"tests/{name}", "definition_version": "v1",
                 "task_class": "generation", "determinism": "deterministic",
                 "artifact_family": "generic", "artifact_kind": "generic",
                 "argv": [sys.executable, "task.py"], "working_directory": ".",
                 "result_path": "result"},
        "inputs": [], "code_closure": pb.build_code_closure(checkout, ["task.py"]),
        "params": params,
        "environment": {"variables": {}, "toolchain": {}},
        "execution_scope": {"portability": "portable", "platform_key": None,
                            "host_class": None},
    })
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    cas.publish_action_request(action)
    return action["action_key"], str(cas.root), str(checkout)


def _denial(q: pool.PoolQueue, key: str) -> dict:
    path = adaptive_cpu.local_state_base(q.ledger().base) / pool.CLAIM_DENIALS
    records = adaptive_cpu.read_json(path).get("records", {})
    return next(value for value in records.values() if value["action_key"] == key)


def _claim(q, capacity, *, adaptive, tiers=None):
    if not adaptive:
        return q.claim(capacity=capacity)
    return q.claim(capacity=capacity, cpu_tiers=tiers, adaptive_cpu=True)


def _key_of(item):
    return None if item is None else item["action_key"]


# -- the issue fixture -------------------------------------------------------


def _twenty_cpu_gpu_box(queue, clock, monkeypatch, *, adaptive):
    capacity = {"cpu": 20, "gpu": 1, "mem_gb": 120}
    tiers = {"preferred": list(range(20)), "fallback": []}
    monkeypatch.setattr(adaptive_cpu.Controller, "sample", lambda self: {
        "sampled_unix": clock[0], "cpu_count": 20, "interval_s": 1.,
        "psi_some": 0., "busy_cpus": 0.})

    def claim():
        return _key_of(_claim(queue, capacity, adaptive=adaptive, tiers=tiers))

    # A multi-hour GPU holder, and a GPU action that has waited behind it
    # long enough that its own first-denial clock is past the ceiling.
    clock[0] = T0 - 3000
    holder = _publish(queue, clock, _key("gpu-holder"), {"cpu": 2, "gpu": 1, "mem_gb": 48})
    assert claim() == holder
    clock[0] = T0 - 1500
    waiting = _publish(queue, clock, _key("gpu-action"), {"cpu": 4, "gpu": 1, "mem_gb": 48})
    return claim, holder, waiting


@pytest.mark.parametrize("adaptive", [False, True], ids=["ledger", "adaptive_cpu"])
def test_a_multi_hour_gpu_holder_does_not_hold_the_box_shut_for_cpu_work(
    queue: pool.PoolQueue, clock, monkeypatch, adaptive: bool,
) -> None:
    """The 2026-09-04 half: the GPU holder is hours old and does not drain soon.

    The GPU action is past the floor and inside its own ceiling, which before
    #924 withheld the box for fifteen minutes behind a holder that would not
    leave in them.  It now keeps its place and the box runs the shards.
    """

    claim, holder, waiting = _twenty_cpu_gpu_box(queue, clock, monkeypatch,
                                                 adaptive=adaptive)
    for _ in range(pool.STARVATION_FLOOR - 1):
        assert claim() is None
    shard = _publish(queue, clock, _key("shard"), {"cpu": 2, "mem_gb": 4})
    assert claim() == shard
    denial = _denial(queue, waiting)
    assert denial["reason"] == "reservation_unavailable_starved"
    assert denial["evidence"]["starved"]["why"] == "holder_does_not_drain_soon"
    [named] = denial["evidence"]["starved"]["holders"]
    assert named["action_key"] == holder[:12] and named["bound"] == "long"
    assert queue.passes(waiting) == pool.STARVATION_FLOOR


@pytest.mark.parametrize("adaptive", [False, True], ids=["ledger", "adaptive_cpu"])
def test_a_gpu_action_behind_2cpu_shards_is_admitted_within_one_shard_lifetime(
    queue: pool.PoolQueue, clock, monkeypatch, adaptive: bool,
) -> None:
    """#924's acceptance fixture, on a 20-CPU GPU box.

    While the GPU is held the box runs 2-CPU shards on the other eighteen
    CPUs.  When the GPU frees, two CPUs come back with it and the action needs
    four.  The shards in its way are transient, so it withholds the box and is
    admitted when the first of them finishes.  Before #924 it was past its own
    ceiling and never withheld again -- and under adaptive admission the
    controller refused the shortage, and a refusal never withheld at all --
    so each freed pair of CPUs went to the next shard in the stream.
    """

    claim, holder, waiting = _twenty_cpu_gpu_box(queue, clock, monkeypatch,
                                                 adaptive=adaptive)
    for _ in range(pool.STARVATION_FLOOR):
        queue.record_pass(waiting)

    clock[0] = T0 - 100
    shards = [_publish(queue, clock, _key(f"shard-{n}"), {"cpu": 2, "mem_gb": 4})
              for n in range(9)]
    for shard in shards:
        assert claim() == shard
    stream = [_publish(queue, clock, _key(f"stream-{n}"), {"cpu": 2, "mem_gb": 4})
              for n in range(3)]
    assert queue.withhold_age(waiting) > pool.WITHHOLD_CEILING_S

    # The GPU frees.
    clock[0] = T0
    queue.finish(holder, status="executed")
    assert claim() is None, (
        "the stream took the CPUs the ready GPU action was waiting for")
    denial = _denial(queue, waiting)
    # Under adaptive admission the controller refuses the shortage itself
    # (``projected_cpu_cost``), and that refusal now withholds as well.
    assert denial["reason"] == ("adaptive_cpu_refused_withholding" if adaptive
                                else "reservation_unavailable_withholding")
    assert denial["evidence"]["withhold"]["why"] == "drains_soon"
    assert denial["evidence"]["withhold"]["gpu_first"] is True
    assert all(queue.item_path(pool.READY, key).exists() for key in stream)

    # One shard lifetime later the first shard finishes, and the action runs.
    clock[0] = T0 + 30
    queue.finish(shards[0], status="executed")
    assert claim() == waiting


# -- constraint 1: a holder that does not drain soon never blocks the box -----


def test_a_progress_governed_unbounded_holder_never_holds_the_box_shut(
    queue: pool.PoolQueue, clock, tmp_path: Path,
) -> None:
    """Sparky, 2026-09-23: R12 held 100 of 104 GB, and 12-GB shards withheld.

    R12 (``683cb3caa5ea``) is progress-governed and asks for no total timeout,
    so nothing bounds how long it runs -- about a day.  A 12-GB shard that
    cannot fit beside it is refused past the floor, inside its own ceiling,
    which before #924 withheld the box for the next fifteen minutes of every
    item behind it.  It now keeps its passes and its place, is reported
    starved, and the box runs what fits.
    """

    capacity = {"cpu": 20, "gpu": 1, "mem_gb": 104}
    r12, cas_root, checkout = _sealed(tmp_path, "r12", progress=True)
    clock[0] = T0 - 36_000
    _publish(queue, clock, r12, {"cpu": 9, "gpu": 1, "mem_gb": 100},
             cas_root=cas_root, checkout_root=checkout)
    assert _key_of(queue.claim(capacity=capacity)) == r12

    clock[0] = T0
    shard = _publish(queue, clock, _key("12gb-shard"), {"cpu": 1, "mem_gb": 12})
    small = _publish(queue, clock, _key("2gb-shard"), {"cpu": 1, "mem_gb": 2})
    for _ in range(pool.STARVATION_FLOOR - 1):
        queue.record_pass(shard)
    assert queue.withhold_age(shard) <= pool.WITHHOLD_CEILING_S

    assert _key_of(queue.claim(capacity=capacity)) == small, (
        "a day-long holder held the box shut for a shard that cannot fit beside it")
    assert queue.holder_bound(r12)["bound"] == "unbounded"
    denial = _denial(queue, shard)
    assert denial["reason"] == "reservation_unavailable_starved"
    starved = denial["evidence"]["starved"]
    assert starved["why"] == "holder_does_not_drain_soon"
    [named] = starved["holders"]
    assert named["action_key"] == r12[:12]
    assert named["bound"] == "unbounded" and named["governed_by"] == "progress"
    assert named["requested_timeout_s"] is None
    assert named["age_s"] == pytest.approx(36_000.0, abs=1.0)
    assert queue.passes(shard) == pool.STARVATION_FLOOR
    assert queue.item_path(pool.READY, shard).exists()


@pytest.mark.parametrize("timeout_s,withholds", [(86_400, False), (2_500, True)],
                         ids=["ends-in-a-day", "ends-inside-the-ceiling"])
def test_a_measurement_behind_a_bounded_holder_reads_the_declared_end(
    queue: pool.PoolQueue, clock, monkeypatch, tmp_path: Path,
    timeout_s: int, withholds: bool,
) -> None:
    """"Drains soon" is what the holder declared, not a new constant.

    A holder claimed 2000 s ago is past the pool's transient line.  One whose
    sealed timeout ends in a day does not drain soon, so the measurement
    behind it does not hold the box shut; one whose timeout ends inside
    ``WITHHOLD_CEILING_S`` from now does, so it may.
    """

    capacity = {"cpu": 8, "mem_gb": 16}
    tiers = {"preferred": list(range(8)), "fallback": []}
    state = {"busy_cpus": 0.}
    monkeypatch.setattr(adaptive_cpu.Controller, "sample", lambda self: {
        "sampled_unix": clock[0], "cpu_count": 8, "interval_s": 1.,
        "psi_some": 0., **state})
    measurement = _key("measurement")
    monkeypatch.setattr(adaptive_cpu, "action_identity",
                        lambda item: ("shape", item["action_key"] == measurement))

    def claim():
        return _key_of(_claim(queue, capacity, adaptive=True, tiers=tiers))

    holder, cas_root, checkout = _sealed(tmp_path, "bounded", timeout_s=timeout_s)
    clock[0] = T0 - 2000
    _publish(queue, clock, holder, {"cpu": 1, "mem_gb": 1},
             cas_root=cas_root, checkout_root=checkout)
    assert claim() == holder
    state["busy_cpus"] = 1.

    clock[0] = T0
    _publish(queue, clock, measurement, {"cpu": 1, "mem_gb": 1})
    for _ in range(pool.STARVATION_FLOOR - 1):
        queue.record_pass(measurement)
    behind = _publish(queue, clock, _key("behind"), {"cpu": 1, "mem_gb": 1})

    if withholds:
        assert claim() is None
        assert queue.holder_bound(holder)["bound"] == "transient"
        assert _denial(queue, measurement)["reason"] == "adaptive_cpu_refused_withholding"
    else:
        assert claim() == behind
        assert queue.holder_bound(holder)["bound"] == "long"
        denial = _denial(queue, measurement)
        assert denial["reason"] == "adaptive_cpu_refused_starved"
        assert denial["evidence"]["starved"]["why"] == "holder_does_not_drain_soon"


# -- #939: a shard that seals its deadline ------------------------------------

#: The deadline a pbtest shard seals when its claimants announce 3600 s, as
#: dl380g10 does: ``pbtest.shard_ceiling``, sealed as ``execution_timeout_s``.
SHARD_DEADLINE_S = 3600.0


def _gpu_action_behind_a_shard(queue, clock, tmp_path: Path, *, timeout_s, age_s):
    """A 2-CPU shard claimed ``age_s`` ago, then a GPU action that needs its CPUs.

    The box has four CPUs and a free GPU.  The GPU action needs all four, so
    the shard is in its way; a 1-CPU item behind it fits beside the shard and
    is what the box admits unless the GPU action withholds.
    """

    capacity = {"cpu": 4, "gpu": 1, "mem_gb": 16}
    shard, cas_root, checkout = _sealed(tmp_path, "shard", timeout_s=timeout_s)
    clock[0] = T0 - age_s
    _publish(queue, clock, shard, {"cpu": 2, "mem_gb": 4},
             cas_root=cas_root, checkout_root=checkout)
    assert _key_of(queue.claim(capacity=capacity)) == shard
    clock[0] = T0 - 1
    gpu_action = _publish(queue, clock, _key("gpu-action"), {"cpu": 4, "gpu": 1, "mem_gb": 4})
    behind = _publish(queue, clock, _key("behind"), {"cpu": 1, "mem_gb": 1})
    # ``_publish`` steps the clock; the shard was claimed ``age_s`` before T0
    # to within that step, and the claim pass below runs at T0 exactly.
    clock[0] = T0
    return (lambda: _key_of(queue.claim(capacity=capacity))), shard, gpu_action, behind


def test_a_gpu_action_withholds_behind_a_sealed_shard_until_its_end_and_no_longer(
    queue: pool.PoolQueue, clock, tmp_path: Path,
) -> None:
    """#939's acceptance fixture.

    The shard is older than ``WITHHOLD_CEILING_S``, so its age no longer says
    it drains soon.  Its sealed deadline does: one second before its declared
    end the GPU action withholds the box.  One second after that end the shard
    has outlived what it declared -- its worker is killing it, or is gone and
    the lease is expiring -- and the GPU action stops withholding, so the box
    admits the item behind it.
    """

    claim, shard, gpu_action, behind = _gpu_action_behind_a_shard(
        queue, clock, tmp_path, timeout_s=SHARD_DEADLINE_S,
        age_s=SHARD_DEADLINE_S - 1)
    assert claim() is None, "the GPU action did not withhold behind a shard about to end"
    denial = _denial(queue, gpu_action)
    assert denial["reason"] == "reservation_unavailable_withholding"
    assert denial["evidence"]["withhold"]["why"] == "drains_soon"
    bound = queue.holder_bound(shard)
    assert bound["bound"] == "transient"
    assert bound["governed_by"] == "deadline"
    assert bound["requested_timeout_s"] == SHARD_DEADLINE_S
    assert queue.item_path(pool.READY, behind).exists()

    clock[0] = T0 + 2
    assert claim() == behind, (
        "the GPU action kept the box shut behind a shard past its declared end")
    denial = _denial(queue, gpu_action)
    assert denial["reason"] == "reservation_unavailable_starved"
    assert denial["evidence"]["starved"]["why"] == "holder_does_not_drain_soon"
    [named] = denial["evidence"]["starved"]["holders"]
    assert named["action_key"] == shard[:12] and named["bound"] == "overdue"
    assert queue.item_path(pool.READY, gpu_action).exists()


@pytest.mark.parametrize("timeout_s,age_s,withholds,bound", [
    (SHARD_DEADLINE_S, SHARD_DEADLINE_S - pool.WITHHOLD_CEILING_S + 100, True, "transient"),
    (SHARD_DEADLINE_S, SHARD_DEADLINE_S, True, "transient"),
    (SHARD_DEADLINE_S, SHARD_DEADLINE_S + 1, False, "overdue"),
    (SHARD_DEADLINE_S, pool.WITHHOLD_CEILING_S + 100, False, "long"),
    (None, SHARD_DEADLINE_S - 1, False, "long"),
    (600.0, 700.0, False, "overdue"),
], ids=["end-inside-the-ceiling", "at-its-end", "past-its-end",
        "end-beyond-the-ceiling", "unsealed-pre-939-shard", "past-a-short-end-while-young"])
def test_a_sealed_shard_is_read_by_its_declared_end(
    queue: pool.PoolQueue, clock, tmp_path: Path,
    timeout_s, age_s: float, withholds: bool, bound: str,
) -> None:
    """Where a shard's declared end puts it, one moment at a time.

    ``unsealed-pre-939-shard`` is the same shard as it was submitted before
    #939: no declared end, so past ``WITHHOLD_CEILING_S`` of age it reads
    ``long`` however close it is to the deadline it actually runs under.
    ``end-beyond-the-ceiling`` is #924's own line, unchanged: a bounded holder
    whose end is further off than ``WITHHOLD_CEILING_S`` does not drain soon.
    ``past-a-short-end-while-young`` is why ``overdue`` outranks age: a young
    holder is presumed to drain soon, and a holder past its own declared end
    has already broken that presumption.
    """

    claim, shard, gpu_action, behind = _gpu_action_behind_a_shard(
        queue, clock, tmp_path, timeout_s=timeout_s, age_s=age_s)

    assert claim() == (None if withholds else behind)
    assert queue.holder_bound(shard)["bound"] == bound
    assert _denial(queue, gpu_action)["reason"] == (
        "reservation_unavailable_withholding" if withholds
        else "reservation_unavailable_starved")


# -- the measurement's exclusive need ----------------------------------------


def test_a_measurement_withholds_while_its_holders_drain_and_through_their_tail(
    queue: pool.PoolQueue, clock, monkeypatch,
) -> None:
    """``fed96645``: refused ``measurement_host_not_idle`` while shards ran.

    Past the floor the measurement withholds while the holders in its way are
    transient.  When the last one leaves, the next CPU sample still carries
    its tail; the measurement keeps the box for one sample window of that,
    and is admitted when the host reads idle.
    """

    capacity = {"cpu": 8, "mem_gb": 16}
    tiers = {"preferred": list(range(8)), "fallback": []}
    state = {"busy_cpus": 0.}
    monkeypatch.setattr(adaptive_cpu.Controller, "sample", lambda self: {
        "sampled_unix": clock[0], "cpu_count": 8, "interval_s": 1.,
        "psi_some": 0., **state})
    measurement = _key("measurement")
    monkeypatch.setattr(adaptive_cpu, "action_identity",
                        lambda item: ("shape", item["action_key"] == measurement))

    def claim():
        return _key_of(_claim(queue, capacity, adaptive=True, tiers=tiers))

    first = _publish(queue, clock, _key("holder"), {"cpu": 1, "mem_gb": 1})
    assert claim() == first
    state["busy_cpus"] = 1.
    _publish(queue, clock, measurement, {"cpu": 1, "mem_gb": 1})
    shards = [_publish(queue, clock, _key(f"shard-{n}"), {"cpu": 1, "mem_gb": 1})
              for n in range(3)]
    # Below the floor the shards still overtake it, as they always did.
    assert claim() == shards[0]
    assert claim() == shards[1]
    # At the floor it withholds: every holder in its way is transient.
    assert claim() is None, "the measurement never withheld against a shard stream"
    denial = _denial(queue, measurement)
    assert denial["reason"] == "adaptive_cpu_refused_withholding"
    assert denial["evidence"]["decision"]["reason"] == "measurement_host_not_idle"
    assert denial["evidence"]["withhold"]["why"] == "drains_soon"

    for key in (first, *shards[:2]):
        queue.finish(key, status="executed")
    # The holders are gone; the sample still carries their load.
    assert claim() is None
    assert _denial(queue, measurement)["evidence"]["withhold"]["why"] == "holder_tail"
    clock[0] += 30
    assert claim() is None
    state["busy_cpus"] = 0.
    assert claim() == measurement


def test_load_the_pool_does_not_own_never_withholds_a_measurement(
    queue: pool.PoolQueue, clock, monkeypatch,
) -> None:
    """The livelock the review found: withholding for foreign load.

    With no holder in its way the busy host is not the pool's to drain.
    Withholding would only cut the box to one admission per drain while the
    foreign load stays, so the item is reported starved and the box runs what
    it can, and it does not withhold again for ``WITHHOLD_CEILING_S``.  The
    same applies past one sample window of a holder's tail.
    """

    capacity = {"cpu": 8, "mem_gb": 16}
    tiers = {"preferred": list(range(8)), "fallback": []}
    state = {"busy_cpus": 2.}
    monkeypatch.setattr(adaptive_cpu.Controller, "sample", lambda self: {
        "sampled_unix": clock[0], "cpu_count": 8, "interval_s": 1.,
        "psi_some": 0., **state})
    measurement = _key("measurement")
    monkeypatch.setattr(adaptive_cpu, "action_identity",
                        lambda item: ("shape", item["action_key"] == measurement))

    def claim():
        return _key_of(_claim(queue, capacity, adaptive=True, tiers=tiers))

    _publish(queue, clock, measurement, {"cpu": 1, "mem_gb": 1})
    for _ in range(pool.STARVATION_FLOOR - 1):
        queue.record_pass(measurement)
    shards = [_publish(queue, clock, _key(f"shard-{n}"), {"cpu": 1, "mem_gb": 1})
              for n in range(2)]
    assert claim() == shards[0]
    denial = _denial(queue, measurement)
    assert denial["reason"] == "adaptive_cpu_refused_starved"
    assert denial["evidence"]["starved"]["why"] == "foreign_load"
    # A shard is now in its way and transient, but the load that refused it
    # was not the pool's: it does not start withholding on the next pass.
    assert claim() == shards[1]
    assert _denial(queue, measurement)["evidence"]["starved"]["why"] == "foreign_load"


def test_a_holder_tail_is_bounded_by_one_sample_window(
    queue: pool.PoolQueue, clock, monkeypatch,
) -> None:
    capacity = {"cpu": 8, "mem_gb": 16}
    tiers = {"preferred": list(range(8)), "fallback": []}
    state = {"busy_cpus": 0.}
    monkeypatch.setattr(adaptive_cpu.Controller, "sample", lambda self: {
        "sampled_unix": clock[0], "cpu_count": 8, "interval_s": 1.,
        "psi_some": 0., **state})
    measurement = _key("measurement")
    monkeypatch.setattr(adaptive_cpu, "action_identity",
                        lambda item: ("shape", item["action_key"] == measurement))

    def claim():
        return _key_of(_claim(queue, capacity, adaptive=True, tiers=tiers))

    holder = _publish(queue, clock, _key("holder"), {"cpu": 1, "mem_gb": 1})
    assert claim() == holder
    state["busy_cpus"] = 1.
    _publish(queue, clock, measurement, {"cpu": 1, "mem_gb": 1})
    for _ in range(pool.STARVATION_FLOOR - 1):
        queue.record_pass(measurement)
    behind = _publish(queue, clock, _key("behind"), {"cpu": 1, "mem_gb": 1})
    assert claim() is None
    queue.finish(holder, status="executed")
    clock[0] += adaptive_cpu.MAX_INTERVAL_S + adaptive_cpu.MAX_SAMPLE_AGE_S + 1
    assert claim() == behind, "a busy host long after the last holder left is not a tail"
    assert _denial(queue, measurement)["evidence"]["starved"]["why"] == "foreign_load"


# -- the refill epoch ---------------------------------------------------------


def test_a_veto_that_keeps_being_refilled_expires(queue: pool.PoolQueue, clock) -> None:
    """Work ahead of the item may refill the box while it withholds.

    Each refill is young, so the holders in the way stay transient forever
    and the veto never ends by age.  Past ``WITHHOLD_CEILING_S`` of the episode
    with a holder claimed during it, the item stops withholding until the
    refills have gone, and then a new episode starts.
    """

    capacity = {"cpu": 4}
    ledger = queue.ledger()
    ledger.ensure_capacity(capacity)
    first = _publish(queue, clock, _key("first"), {"cpu": 1})
    assert _key_of(queue.claim(capacity=capacity)) == first
    waiting = _publish(queue, clock, _key("waiting"), {"cpu": 4})
    for _ in range(pool.STARVATION_FLOOR - 1):
        queue.record_pass(waiting)

    def verdict():
        answer = queue._withhold_verdict(waiting, ledger=ledger, need={"cpu": 4},
                                         mode="tokens")
        queue.record_pass(waiting)
        return answer

    assert verdict()["withhold"] is True
    epoch = json.loads(queue.passes_path(waiting).read_text())["epoch_unix"]

    # Higher-priority work ahead of it refills the box, young every time.
    clock[0] += pool.WITHHOLD_CEILING_S - 10
    refill = _publish(queue, clock, _key("refill-0"), {"cpu": 1}, priority=5)
    assert _key_of(queue.claim(capacity=capacity)) == refill
    queue.finish(first, status="executed")
    assert verdict()["withhold"] is True, "inside the episode the veto stands"
    clock[0] += 20
    answer = verdict()
    assert answer["withhold"] is False and answer["why"] == "refilled_past_ceiling"
    assert answer["refills"] == [refill[:12]]
    record = json.loads(queue.passes_path(waiting).read_text())
    assert record["epoch_unix"] == epoch and record["expired_unix"] == clock[0]

    # Work claimed after the expiry is not a refill of that veto; the episode
    # stays expired only while the refills it counted are still held.
    queue.finish(refill, status="executed")
    clock[0] += 1
    later = _publish(queue, clock, _key("later"), {"cpu": 1}, priority=5)
    assert _key_of(queue.claim(capacity=capacity)) == later
    answer = verdict()
    assert answer["withhold"] is True and answer["why"] == "drains_soon"
    record = json.loads(queue.passes_path(waiting).read_text())
    assert record["epoch_unix"] == clock[0] and "expired_unix" not in record


# -- GPU-first -----------------------------------------------------------------


def test_a_gpu_action_withholds_on_its_first_denial_when_the_gpu_is_free(
    queue: pool.PoolQueue, clock,
) -> None:
    """Every admission behind a ready GPU action on a free GPU takes what it needs."""

    capacity = {"cpu": 4, "gpu": 1}
    shard = _publish(queue, clock, _key("shard"), {"cpu": 2})
    assert _key_of(queue.claim(capacity=capacity)) == shard
    gpu_action = _publish(queue, clock, _key("gpu-action"), {"cpu": 4, "gpu": 1})
    cpu_only = _publish(queue, clock, _key("cpu-only"), {"cpu": 1})
    assert queue.claim(capacity=capacity) is None
    assert queue.passes(gpu_action) == 1
    assert _denial(queue, gpu_action)["evidence"]["withhold"]["gpu_first"] is True
    assert queue.item_path(pool.READY, cpu_only).exists()


def test_a_cpu_only_action_still_waits_for_the_floor(queue: pool.PoolQueue, clock) -> None:
    capacity = {"cpu": 4, "gpu": 1}
    shard = _publish(queue, clock, _key("shard"), {"cpu": 2})
    assert _key_of(queue.claim(capacity=capacity)) == shard
    _publish(queue, clock, _key("big"), {"cpu": 4})
    small = _publish(queue, clock, _key("small"), {"cpu": 1})
    assert _key_of(queue.claim(capacity=capacity)) == small


@pytest.mark.parametrize("sample,gpu_first,foreign", [
    ({"foreign_processes": [], "sampled_unix": T0}, True, False),
    ({"foreign_processes": [{"pid": 4242}], "sampled_unix": T0}, False, True),
    ({"foreign_processes": [], "sampled_unix": T0 - 60}, False, False),
    ({}, False, False),
], ids=["clean", "foreign-serve", "stale", "absent"])
def test_gpu_first_needs_a_clean_fresh_gpu_sample(
    queue: pool.PoolQueue, clock, sample, gpu_first: bool, foreign: bool,
) -> None:
    """A free GPU token is not a free GPU when a serve outside the pool owns it."""

    ledger = queue.ledger()
    ledger.ensure_capacity({"cpu": 4, "gpu": 1})
    assert ledger.acquire(_key("shard"), {"cpu": 2}) is True
    waiting = _publish(queue, clock, _key("gpu-action"), {"cpu": 4, "gpu": 1})
    answer = queue._withhold_verdict(waiting, ledger=ledger, need={"cpu": 4, "gpu": 1},
                                     mode="tokens", gpu_sample=sample)
    queue.record_pass(waiting)
    assert answer["gpu_first"] is gpu_first
    assert answer["eligible"] is gpu_first
    record = json.loads(queue.passes_path(waiting).read_text())
    assert ("foreign_unix" in record) is foreign


# -- classification -----------------------------------------------------------


@pytest.mark.parametrize("source,decision,measurement,demand,expected", [
    ("adaptive_cpu_refused", {"reason": "measurement_host_not_idle"}, True, {"cpu": 1},
     ("exclusive", False)),
    ("adaptive_cpu_refused", {"reason": "measurement_holder"}, True, {"cpu": 1},
     ("exclusive", False)),
    ("adaptive_cpu_refused", {"reason": "unbounded_cpu_not_exclusive"}, False, {},
     ("exclusive", False)),
    ("adaptive_cpu_refused", {"reason": "host_pressure"}, False, {"cpu": 2}, (None, False)),
    ("adaptive_cpu_refused", {"reason": "host_pressure"}, True, {"cpu": 2},
     ("exclusive", False)),
    ("adaptive_cpu_refused", {"reason": "host_pressure"}, False, {"cpu": 20},
     ("exclusive", False)),
    ("adaptive_cpu_refused", {"reason": "borrow_evidence_unavailable"}, False, {"cpu": 4},
     ("tokens", False)),
    ("adaptive_cpu_refused", {"reason": "projected_cpu_cost"}, False, {"cpu": 4},
     ("tokens", False)),
    ("adaptive_cpu_refused", {"reason": "max_actions"}, False, {"cpu": 1}, (None, False)),
    # #1085: a GPU refusal that exists only because the pool's own GPU holders
    # are on the device drains with them, for the GPU kind alone.
    ("adaptive_gpu_refused", {"reason": "exclusive_holder"}, False, {"gpu": 1},
     ("gpu", False)),
    ("adaptive_gpu_refused", {"reason": "sharing_probe_not_authorized"}, False,
     {"gpu": 1}, ("gpu", False)),
    ("adaptive_gpu_refused", {"reason": "holder_telemetry_unavailable"}, False,
     {"gpu": 1}, ("gpu", False)),
    ("adaptive_gpu_refused", {"reason": "max_actions"}, False, {"gpu": 1},
     ("gpu", False)),
    # Device and host state, not holder presence: no drain is known to fix it.
    ("adaptive_gpu_refused", {"reason": "host_or_device_congested"}, False,
     {"gpu": 1}, (None, False)),
    ("adaptive_gpu_refused", {"reason": "gpu_memory_budget"}, False, {"gpu": 1},
     (None, False)),
    ("adaptive_gpu_refused", {"reason": "exclusive_holder"}, True, {"gpu": 1},
     ("exclusive", False)),
    ("adaptive_gpu_refused", {"reason": "host_or_device_congested",
                              "foreign_processes": [{"pid": 1}]}, True, {"gpu": 1},
     (None, True)),
    ("adaptive_gpu_refused", {"reason": "host_or_device_congested", "limited": True},
     True, {"gpu": 1}, (None, False)),
    ("adaptive_gpu_refused", {"reason": "sample_invalid_or_stale"}, True, {"gpu": 1},
     (None, False)),
])
def test_only_refusals_draining_resolves_are_eligible_to_withhold(
    source, decision, measurement, demand, expected,
) -> None:
    assert pool._adaptive_refusal_drains(
        source, decision, demand=demand, measurement=measurement,
        cpu_count=20) == expected


def test_a_usage_refusal_with_the_tokens_free_is_not_a_shortage(
    queue: pool.PoolQueue, clock,
) -> None:
    ledger = queue.ledger()
    ledger.ensure_capacity({"cpu": 4})
    waiting = _publish(queue, clock, _key("waiting"), {"cpu": 2})
    for _ in range(pool.STARVATION_FLOOR):
        queue.record_pass(waiting)
    answer = queue._withhold_verdict(waiting, ledger=ledger, need={"cpu": 2},
                                     mode="tokens", adaptive=True)
    assert answer["withhold"] is False and answer["why"] == "not_a_token_shortage"


# -- reporting ------------------------------------------------------------------


def test_pbstatus_lists_starved_ready_items_with_the_holders_they_named() -> None:
    jobs = [
        {"state": "READY", "action_key_prefix": "aaaaaaaaaaaa", "admission_passes": 7,
         "admission_wait_s": 1200.0, "admission_denials": [
             {"host": "sparky", "reason": "reservation_unavailable_starved", "age_s": 3.0,
              "decision_reason": None,
              "evidence": {"starved": {"why": "holder_does_not_drain_soon", "holders": [
                  {"action_key": "683cb3caa5ea", "bound": "unbounded"}]}}},
             {"host": "sparklina", "reason": "reservation_unavailable_withholding",
              "evidence": {}}]},
        {"state": "READY", "action_key_prefix": "bbbbbbbbbbbb", "admission_denials": [
            {"host": "sparky", "reason": "adaptive_cpu_refused_past_ceiling",
             "decision_reason": "measurement_host_not_idle",
             "evidence": {"starved": {"why": "refilled_past_ceiling", "holders": []}}}]},
        {"state": "CLAIMED", "action_key_prefix": "cccccccccccc", "admission_denials": [
            {"host": "sparky", "reason": "reservation_unavailable_starved", "evidence": {}}]},
    ]
    rows = pbstatus._starvation_starved(jobs)
    assert [(row["action_key_prefix"], row["host"], row["why"]) for row in rows] == [
        ("aaaaaaaaaaaa", "sparky", "holder_does_not_drain_soon"),
        ("bbbbbbbbbbbb", "sparky", "refilled_past_ceiling")]
    assert rows[0]["holders"] == [{"action_key": "683cb3caa5ea", "bound": "unbounded"}]
    assert rows[1]["decision_reason"] == "measurement_host_not_idle"
