"""PB #1429: the lifetime contract bounds an opted-in action, phase by phase.

The sealed ``execution_timeout_s`` stays a payload budget. The opt-in
``params.lifetime`` contract is separate: one absolute deadline that starts
at publication, a stop instant ahead of it that no credit moves, and per-phase
evidence. These fixtures use the real queue, ledgers, materializer, launcher
and broker authority. Only clocks, injected stalls and the kernel half of the
broker are controlled. Each phase is delayed or failed in turn, and a finite
audit must not survive any of them.
"""
from __future__ import annotations

import os
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "tools" / "fleet")]
sys.path[:0] = [str(ROOT / "tests")]
from prismabuild import _measurement_reservation as reservation  # noqa: E402
from prismabuild import core as pb, lifetime_fence, materialize, pool  # noqa: E402
from admitted_queue_fixture import AdmittedQueueFixture  # noqa: E402
from lifetime_fixtures_1429 import (  # noqa: E402
    FENCE_S,
    TASK_OK,
    TASK_SLEEPS,
    WORKER,
    Broker,
    alive,
    claim,
    claim_on_host,
    fenced_queue,
    fenced_snapshot_queue,
    publish_fenced,
    seal,
    wait_for,
)
from test_measurement_drains_gpu_backfill import fleet as fleet_fixture  # noqa: E402
from test_measurement_reservation_backfill_1419 import (  # noqa: E402
    _assert_holder_unchanged,
    _bounded_measurement_wait,
    _observe_real_sharing_permission,
)

fleet = fleet_fixture

#: How long a loaded box may take to start a payload, and to stop one.
STARTUP_S = 90.0
STOP_S = 45.0


@pytest.fixture()
def broker(tmp_path, monkeypatch):
    fixture = Broker(tmp_path, monkeypatch)
    yield fixture
    fixture.close()


class Skew:
    """A clock that runs ahead of the real one by a settable amount."""

    def __init__(self, monkeypatch) -> None:
        self.seconds = 0.0
        real_now = time.time
        self.now = lambda: real_now() + self.seconds
        monkeypatch.setattr(pool, "_now", self.now)
        monkeypatch.setattr(materialize, "_now", self.now)

    def to(self, instant: float) -> None:
        """Run the clock so that it reads ``instant`` right now."""

        self.seconds = instant - (self.now() - self.seconds)


def _clock_of(item) -> lifetime_fence.Clock:
    return lifetime_fence.clock(published_unix=item["published_unix"], fence_s=FENCE_S)


def _finish(queue, item, outcome):
    return queue.finish(item["action_key"], status=str(outcome["status"]),
                        detail=outcome, claim_snapshot=item)


def _audit(queue, path):
    terminal = pool._read_json(path)
    assert isinstance(terminal, dict)
    return reservation.attempt_release_audit(queue.queue, terminal)


def _execute(queue, item, **kwargs):
    kwargs.setdefault("containment", True)
    return queue.execute(item, heartbeat_s=0.05, timeout_grace_s=0.5, **kwargs)


def _phases(outcome):
    return outcome["lifetime_evidence"]["phases"]


# -- reproduction of the open defect: each fails on the head before this change --


def test_a_candidate_that_meets_the_contract_backfills_before_the_opportunity(fleet):
    """The positive case: a finite bound strictly before the original opportunity."""

    queue, clock, readings, sample, publish, tick, claim_row, denial = fleet
    incumbent, measurement, original_end, snapshot = _bounded_measurement_wait(fleet)
    key = publish_fenced(fleet, "enforced-backfill", fence_s=FENCE_S)
    row = pool._read_json(queue.item_path(pool.READY, key))
    assert isinstance(row, dict)
    bound = reservation.candidate_release_bound(queue, row)
    assert bound == row["lifetime_deadline_unix"]
    assert bound < original_end
    _observe_real_sharing_permission(fleet, key)
    assert claim_on_host(fleet) == key, denial(key)
    # Capacity, isolation and the waiting measurement are unchanged.
    assert set(queue.ledger().held_keys()) == {incumbent, key}
    _assert_holder_unchanged(queue, incumbent, snapshot)
    assert queue.item_path(pool.READY, measurement).exists()
    assert queue.item_path(pool.CLAIMED, key).exists()


def test_a_stalled_supervisor_cannot_delay_the_fence_stop(tmp_path, monkeypatch):
    """Synchronous checkpoint I/O must not postpone the stop instant."""

    queue, action = fenced_queue(tmp_path, TASK_SLEEPS)
    item = claim(queue)
    pid_file = tmp_path / "checkout" / "pid"
    skew = Skew(monkeypatch)
    monkeypatch.setattr(lifetime_fence, "ALARM_POLL_S", 0.02, raising=False)
    stalled, release = threading.Event(), threading.Event()
    real_observe = pool._observe_execution

    def blocked_observe(process, previous=None, **kwargs):
        # The shared mount stops answering while the payload runs; time passes.
        stalled.set()
        wait_for(pid_file.exists, STARTUP_S)
        skew.to(_clock_of(item).stop_unix + 1.0)
        release.wait(timeout=30)
        return real_observe(process, previous, **kwargs)

    monkeypatch.setattr(pool, "_observe_execution", blocked_observe)
    result: dict[str, object] = {}
    runner = threading.Thread(target=lambda: result.update(
        outcome=_execute(queue, item, containment=False)))
    runner.start()
    pid = None
    try:
        assert stalled.wait(timeout=STARTUP_S), "the supervisor never reached its first checkpoint"
        assert wait_for(pid_file.exists, STARTUP_S), "the payload never started"
        pid = int(pid_file.read_text())
        # The supervisor is stuck inside checkpoint I/O and the stop instant
        # has passed: the payload must already be gone.
        assert wait_for(lambda: not alive(pid), STOP_S), (
            "the payload outlived the fence while its supervisor was stalled")
    finally:
        release.set()
        runner.join(timeout=60)
        if pid is not None and alive(pid):
            os.kill(pid, 9)
    assert result["outcome"]["termination_reason"] == lifetime_fence.FENCE_TERMINATION_REASON


def test_checkout_stops_at_the_fence_instead_of_finishing_every_git_call(
        tmp_path, monkeypatch):
    """A materialization that outlives the fence launches no further Git."""

    queue, action, cas = fenced_snapshot_queue(tmp_path)
    item = claim(queue)
    monkeypatch.setattr(pool, "LOCAL_CHECKOUT_ROOT", tmp_path / "materialized",
                        raising=False)
    skew = Skew(monkeypatch)
    launched: list[tuple[str, ...]] = []
    real_git = pb._git_run

    def slow_git(root, *args, **kwargs):
        launched.append(args)
        if len(launched) == 1:
            skew.to(_clock_of(item).stop_unix + 1.0)   # the first call took the whole fence
        return real_git(root, *args, **kwargs)

    monkeypatch.setattr(pb, "_git_run", slow_git)
    outcome = queue.execute(item, heartbeat_s=0.05, timeout_grace_s=0.2)
    assert outcome["status"] == "failed"
    assert outcome["termination_reason"] == lifetime_fence.FENCE_TERMINATION_REASON
    assert outcome["lifetime_evidence"]["expired_phase"] == "checkout"
    assert len(launched) == 1, launched
    assert cas.lookup(action) is None


# -- the control: a contained attempt that keeps the contract --


def test_a_clean_contained_attempt_audits_a_finite_release_bound(tmp_path, broker):
    queue, action = fenced_queue(tmp_path)
    item = claim(queue)
    key = item["action_key"]
    outcome = _execute(queue, item)
    assert outcome["status"] == "executed", outcome
    assert set(_phases(outcome)) == set(lifetime_fence.EXECUTION_PHASES)
    dst = _finish(queue, item, outcome)
    assert dst == queue.item_path(pool.DONE, key)
    assert queue.ledger().held_keys() == []
    terminal = pool._read_json(dst)
    assert reservation.attempt_release_audit(queue.queue, terminal) == (
        _clock_of(item).deadline_unix)
    # The terminal row completes the record with the one phase that follows
    # the ledger return; the immutable archive keeps what the run filed.
    final = terminal["lifetime_evidence"]["phases"]
    assert set(final) == set(lifetime_fence.PHASES)
    for component in lifetime_fence.COMPONENTS:
        assert final[component.phase]["enforced"] is True, component.phase
        assert final[component.phase]["mechanism"] == component.mechanism
        assert final[component.phase]["ended_unix"] < final[component.phase]["bound_unix"]
    assert "resource_release" not in terminal["detail"]["lifetime_evidence"]["phases"]
    record = broker.record(key)
    assert record["stopped_unix"] and record["released_unix"]
    assert terminal["lifetime_evidence"]["claim"]["nonce"] == record["nonce"]


def test_an_uncontained_attempt_never_audits_a_finite_bound(tmp_path):
    # No scope: nothing stops or settles a kernel group, so two phases have
    # no mechanism and the audit stays UNKNOWN however clean the run was.
    queue, action = fenced_queue(tmp_path)
    item = claim(queue)
    outcome = _execute(queue, item, containment=False)
    assert outcome["status"] == "executed", outcome
    dst = _finish(queue, item, outcome)
    assert dst == queue.item_path(pool.DONE, item["action_key"])
    assert queue.ledger().held_keys() == []
    phases = pool._read_json(dst)["lifetime_evidence"]["phases"]
    assert phases["termination"]["mechanism"] is None
    assert phases["scope_settlement"]["mechanism"] is None
    assert phases["scope_settlement"]["evidence"] == "uncontained-no-scope"
    unproven = {phase for phase, entry in phases.items() if entry["enforced"] is not True}
    assert unproven == {"termination", "scope_settlement"}
    assert _audit(queue, dst) is None


def test_an_unfenced_action_runs_exactly_as_before(tmp_path, broker):
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    (checkout / "task.py").write_text(TASK_OK)
    action = seal(checkout, {})
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    cas.publish_action_request(action)
    queue = AdmittedQueueFixture(
        pool.PoolQueue(tmp_path / "queue"), capacity={"cpu": 8, "mem_gb": 16},
        default_demand={"cpu": 1, "mem_gb": 1})
    queue.publish(action_key=action["action_key"], cas_root=cas.root,
                  checkout_root=checkout, worker_script=WORKER)
    item = queue.queue.claim(capacity={"cpu": 8, "mem_gb": 16}, tags=[])
    assert item is not None and "lifetime_deadline_unix" not in item
    before = {thread.name for thread in threading.enumerate()}
    outcome = _execute(queue, item)
    assert outcome["status"] == "executed"
    assert "lifetime_evidence" not in outcome
    assert {thread.name for thread in threading.enumerate()} == before
    dst = _finish(queue, item, outcome)
    terminal = pool._read_json(dst)
    assert "lifetime_evidence" not in terminal
    assert "lifetime_evidence" not in terminal["detail"]
    assert "container_cleanup" not in terminal["detail"]
    assert queue.ledger().held_keys() == []


# -- payload, credited waits and termination --


def _outlive_the_stop(tmp_path, monkeypatch, *, containment=True, **execute_kwargs):
    """Run a payload that never ends; time passes to just after the stop instant."""

    queue, action = fenced_queue(tmp_path, TASK_SLEEPS)
    item = claim(queue)
    pid_file = tmp_path / "checkout" / "pid"
    skew = Skew(monkeypatch)
    monkeypatch.setattr(lifetime_fence, "ALARM_POLL_S", 0.02)
    real_observe = pool._observe_execution
    started = [False]

    def observe(process, previous=None, **kwargs):
        if not started[0]:
            started[0] = True
            wait_for(pid_file.exists, STARTUP_S)
            skew.to(_clock_of(item).stop_unix + 1.0)
        return real_observe(process, previous, **kwargs)

    monkeypatch.setattr(pool, "_observe_execution", observe)
    outcome = _execute(queue, item, containment=containment, **execute_kwargs)
    return queue, item, outcome, skew, pid_file


def test_a_payload_the_fence_stops_still_keeps_the_release_bound(
        tmp_path, broker, monkeypatch):
    queue, item, outcome, skew, pid_file = _outlive_the_stop(tmp_path, monkeypatch)
    key = item["action_key"]
    assert not alive(int(pid_file.read_text()))
    assert outcome["status"] == "timeout"
    assert outcome["termination_reason"] == lifetime_fence.FENCE_TERMINATION_REASON
    phases = _phases(outcome)
    assert phases["payload"]["evidence"].startswith("stopped-at-fence:fired=")
    assert phases["termination"]["evidence"] == "scope-stopped-at-fence"
    # The stop was delivered at the stop instant, ahead of the deadline.
    fired = float(phases["payload"]["evidence"].split("fired=")[1])
    assert _clock_of(item).stop_unix <= fired < _clock_of(item).deadline_unix
    assert "expired_phase" not in outcome["lifetime_evidence"]
    # The broker was told the fence reason, and told once before cleanup.
    record = broker.record(key)
    assert record["stop_reason"] == lifetime_fence.FENCE_TERMINATION_REASON
    assert [name for name, _ in broker.kernel.calls].count("stop") == 1
    # A fenced stop is the action's ending: the same deadline refuses a retry,
    # so the row is concluded rather than requeued for no box to take.
    dst = _finish(queue, item, outcome)
    assert dst == queue.item_path(pool.FAILED, key)
    assert not queue.item_path(pool.READY, key).exists()
    assert queue.ledger().held_keys() == []
    assert _audit(queue, dst) == _clock_of(item).deadline_unix
    terminal = pool._read_json(dst)
    attempt = queue.attempt_outcomes(terminal)[0]
    assert attempt["detail"]["termination_reason"] == lifetime_fence.FENCE_TERMINATION_REASON


def test_a_stalled_supervisor_does_not_delay_the_broker_stop(
        tmp_path, broker, monkeypatch):
    queue, action = fenced_queue(tmp_path, TASK_SLEEPS)
    item = claim(queue)
    key = item["action_key"]
    pid_file = tmp_path / "checkout" / "pid"
    skew = Skew(monkeypatch)
    monkeypatch.setattr(lifetime_fence, "ALARM_POLL_S", 0.02)
    stalled, release = threading.Event(), threading.Event()
    real_observe = pool._observe_execution

    def blocked_observe(process, previous=None, **kwargs):
        stalled.set()
        wait_for(pid_file.exists, STARTUP_S)
        skew.to(_clock_of(item).stop_unix + 1.0)
        release.wait(timeout=30)
        return real_observe(process, previous, **kwargs)

    monkeypatch.setattr(pool, "_observe_execution", blocked_observe)
    result: dict[str, object] = {}
    runner = threading.Thread(target=lambda: result.update(
        outcome=_execute(queue, item)))
    runner.start()
    pid = None
    try:
        assert stalled.wait(timeout=STARTUP_S)
        assert wait_for(pid_file.exists, STARTUP_S)
        pid = int(pid_file.read_text())
        # While the supervisor is blocked the alarm alone has told the broker
        # to stop the exact scope, and the payload is gone.
        assert wait_for(lambda: broker.record(key).get("stop_reason")
                        == lifetime_fence.FENCE_TERMINATION_REASON, STOP_S)
        assert wait_for(lambda: not alive(pid), STOP_S)
    finally:
        release.set()
        runner.join(timeout=60)
        if pid is not None and alive(pid):
            os.kill(pid, 9)
    outcome = result["outcome"]
    assert outcome["termination_reason"] == lifetime_fence.FENCE_TERMINATION_REASON
    assert _phases(outcome)["payload"]["evidence"].startswith("stopped-at-fence:fired=")
    assert [name for name, _ in broker.kernel.calls].count("stop") == 1


def test_credited_waits_extend_the_payload_budget_and_never_the_fence(
        tmp_path, broker, monkeypatch):
    queue, action = fenced_queue(tmp_path, TASK_SLEEPS)
    item = claim(queue)
    pid_file = tmp_path / "checkout" / "pid"
    skew = Skew(monkeypatch)
    monkeypatch.setattr(lifetime_fence, "ALARM_POLL_S", 0.02)
    real_observe = pool._observe_execution
    calls = [0]

    def observe(process, previous=None, **kwargs):
        calls[0] += 1
        if calls[0] == 1:
            wait_for(pid_file.exists, STARTUP_S)
        elif calls[0] <= 4:
            time.sleep(0.25)          # slow checkpoint I/O: credited to the payload budget
        elif calls[0] == 5:
            skew.to(_clock_of(item).stop_unix + 1.0)
        return real_observe(process, previous, **kwargs)

    monkeypatch.setattr(pool, "_observe_execution", observe)
    outcome = _execute(queue, item, timeout_s=600.0)
    assert outcome["termination_reason"] == lifetime_fence.FENCE_TERMINATION_REASON
    assert outcome["execution_timeout_s"] == 600.0
    credited = _phases(outcome)["credited_waits"]
    assert credited["enforced"] is True
    assert float(credited["evidence"].split("=")[1]) >= 0.5
    # The fence stopped the payload at its own absolute instant, long before
    # the credit-extended payload budget could have.
    assert outcome["lifetime_evidence"]["stop_unix"] == _clock_of(item).stop_unix
    assert _phases(outcome)["payload"]["evidence"].startswith("stopped-at-fence:fired=")
    assert not alive(int(pid_file.read_text()))


def test_a_failed_fence_stop_keeps_the_tokens_until_settlement_is_proved(
        tmp_path, broker, monkeypatch):
    broker.kernel.stop_error = OSError("the kernel refuses the stop")
    queue, item, outcome, skew, pid_file = _outlive_the_stop(tmp_path, monkeypatch)
    key = item["action_key"]
    assert "fence_stop_error" in outcome
    termination = _phases(outcome)["termination"]
    assert termination["enforced"] is False and termination["mechanism"] is None
    assert termination["evidence"] == "fence-stop-failed"
    # The original result and log survive; cleanup cannot stop the scope either.
    pending = _finish(queue, item, outcome)
    assert pending == queue.item_path(pool.CLAIMED, key)
    live = pool._read_json(pending)
    assert live["finish_pending"]["detail"]["termination_reason"] == (
        lifetime_fence.FENCE_TERMINATION_REASON)
    assert queue.ledger().held_keys() == [key]
    assert queue.lease_path(key).exists()
    assert not broker.record(key).get("released_unix")
    assert reservation.attempt_release_audit(queue.queue, live) is None
    # Once the kernel answers, the saved finish settles the scope and only
    # then returns the tokens.
    broker.kernel.stop_error = None
    assert queue.reap_stale() == []
    dst = queue.item_path(pool.FAILED, key)
    assert dst.exists() and queue.ledger().held_keys() == []
    assert broker.record(key)["released_unix"]
    terminal = pool._read_json(dst)
    attempt = queue.attempt_outcomes(terminal)[0]
    assert attempt["detail"]["fence_stop_error"] == outcome["fence_stop_error"]
    assert _audit(queue, dst) is None


# -- phases that run out of time before the launch --


def _refusal_checkout(tmp_path, monkeypatch, broker):
    queue, action, cas = fenced_snapshot_queue(tmp_path)
    item = claim(queue)
    monkeypatch.setattr(pool, "LOCAL_CHECKOUT_ROOT", tmp_path / "materialized",
                        raising=False)
    skew = Skew(monkeypatch)
    real_git = pb._git_run
    launched = []

    def slow_git(root, *args, **kwargs):
        launched.append(args)
        skew.to(_clock_of(item).stop_unix + 1.0)
        return real_git(root, *args, **kwargs)

    monkeypatch.setattr(pb, "_git_run", slow_git)
    return queue, item, skew


def _refusal_readiness(tmp_path, monkeypatch, broker):
    queue, action = fenced_queue(tmp_path)
    item = claim(queue)
    skew = Skew(monkeypatch)
    real = pool.PoolQueue.withdrawal_covers
    fired = []

    def covers(self, *args, **kwargs):
        if not fired:
            fired.append(True)
            skew.to(_clock_of(item).stop_unix + 1.0)
            return None
        return real(self, *args, **kwargs)

    monkeypatch.setattr(pool.PoolQueue, "withdrawal_covers", covers)
    return queue, item, skew


def _refusal_prelaunch(tmp_path, monkeypatch, broker):
    queue, action = fenced_queue(tmp_path)
    item = claim(queue)
    skew = Skew(monkeypatch)
    real = pool.PoolQueue._start_resource_scope

    def slow_scope(self, entry):
        scope = real(self, entry)
        skew.to(_clock_of(item).stop_unix + 1.0)
        return scope

    monkeypatch.setattr(pool.PoolQueue, "_start_resource_scope", slow_scope)
    return queue, item, skew


def _refusal_launch_environment(tmp_path, monkeypatch, broker):
    """The scope is ready and every check passed; the environment then takes the fence."""

    queue, action = fenced_queue(tmp_path)
    item = claim(queue)
    skew = Skew(monkeypatch)
    real = pool.PoolQueue.launch_environment

    def slow_environment(self, entry):
        environment = real(self, entry)
        skew.to(_clock_of(item).stop_unix + 1.0)
        return environment

    monkeypatch.setattr(pool.PoolQueue, "launch_environment", slow_environment)
    return queue, item, skew


def _refusal_status_cleanup(tmp_path, monkeypatch, broker):
    """Clearing the last attempt's status file on the shared mount takes the fence."""

    queue, action = fenced_queue(tmp_path)
    item = claim(queue)
    skew = Skew(monkeypatch)
    status = queue.action_status_path(item["action_key"])
    real = Path.unlink

    def slow_unlink(self, *args, **kwargs):
        if self == status:
            skew.to(_clock_of(item).stop_unix + 1.0)
        return real(self, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", slow_unlink)
    return queue, item, skew


@pytest.mark.parametrize("phase,arrange", [
    pytest.param("checkout", _refusal_checkout, id="checkout"),
    pytest.param("readiness", _refusal_readiness, id="readiness"),
    pytest.param("prelaunch", _refusal_prelaunch, id="scope-setup"),
    pytest.param("prelaunch", _refusal_launch_environment, id="launch-environment"),
    pytest.param("prelaunch", _refusal_status_cleanup, id="status-cleanup"),
])
def test_a_phase_past_the_stop_instant_refuses_the_launch_and_returns_the_tokens(
        tmp_path, broker, monkeypatch, phase, arrange):
    queue, item, skew = arrange(tmp_path, monkeypatch, broker)
    key = item["action_key"]
    # The observation is the first thing the supervisor does once a payload
    # exists, so a call to it means a payload was launched.
    launched: list[object] = []
    real_observe = pool._observe_execution

    def observe(*args, **kwargs):
        launched.append(args)
        return real_observe(*args, **kwargs)

    monkeypatch.setattr(pool, "_observe_execution", observe)
    outcome = _execute(queue, item)
    assert launched == [], "a payload started after the stop instant"
    assert outcome["status"] == "failed"
    assert outcome["termination_reason"] == lifetime_fence.FENCE_TERMINATION_REASON
    assert outcome["lifetime_evidence"]["expired_phase"] == phase
    assert _phases(outcome)[phase]["enforced"] is False
    assert "payload" not in _phases(outcome)
    made_scope = any(name == "create" for name, _ in broker.kernel.calls)
    assert made_scope is (phase == "prelaunch")
    dst = _finish(queue, item, outcome)
    # Concluded, not requeued: the deadline would refuse a second claim.
    assert dst == queue.item_path(pool.FAILED, key)
    assert not queue.item_path(pool.READY, key).exists()
    assert queue.ledger().held_keys() == []
    assert not queue.lease_path(key).exists()
    if made_scope:
        assert broker.record(key)["released_unix"]
    assert _audit(queue, dst) is None
    # The original log is preserved in the immutable attempt.
    attempt = queue.attempt_outcomes(pool._read_json(dst))[0]
    stderr = (queue.root / attempt["logs"]["stderr"]["path"]).read_text()
    assert "lifetime fence expired" in stderr


def test_prelaunch_ends_at_the_final_check_after_the_launch_is_prepared(
        tmp_path, broker, monkeypatch):
    # Preparation that stays inside the fence still counts: prelaunch ends at
    # the check that follows it, never before the status files and the
    # environment, so the audit sees the time the launch really took.
    queue, action = fenced_queue(tmp_path)
    item = claim(queue)
    skew = Skew(monkeypatch)
    real = pool.PoolQueue.launch_environment
    prepared: list[float] = []

    def slow_environment(self, entry):
        environment = real(self, entry)
        skew.seconds += 30.0         # long, and still well before the stop instant
        prepared.append(pool._now())
        return environment

    monkeypatch.setattr(pool.PoolQueue, "launch_environment", slow_environment)
    outcome = _execute(queue, item)
    assert outcome["status"] == "executed", outcome
    prelaunch = _phases(outcome)["prelaunch"]
    assert prelaunch["enforced"] is True
    assert prepared and prelaunch["ended_unix"] >= prepared[0]
    assert prelaunch["ended_unix"] < _clock_of(item).stop_unix
    assert _phases(outcome)["readiness"]["ended_unix"] < prepared[0] - 29.0


# -- settlement: cleanup, scope settlement and resource release --


def _clean_attempt(tmp_path, broker):
    queue, action = fenced_queue(tmp_path)
    item = claim(queue)
    outcome = _execute(queue, item)
    assert outcome["status"] == "executed", outcome
    return queue, item, outcome


def test_unsettled_cleanup_holds_the_tokens_and_the_audit(tmp_path, broker, monkeypatch):
    queue, item, outcome = _clean_attempt(tmp_path, broker)
    key = item["action_key"]
    real = queue.cleanup_action_containers
    monkeypatch.setattr(
        queue, "cleanup_action_containers",
        lambda record, **kwargs: {"complete": False, "used": True, "removed": [],
                                  "remaining": ["container-1"],
                                  "error": "daemon unreachable"})
    dst = _finish(queue, item, outcome)
    assert dst == queue.item_path(pool.CLAIMED, key)
    live = pool._read_json(dst)
    assert live["finish_pending"] is not None
    assert queue.ledger().held_keys() == [key]
    assert reservation.attempt_release_audit(queue.queue, live) is None
    # Cleanup answers again: the saved finish concludes with the original result.
    monkeypatch.setattr(queue, "cleanup_action_containers", real)
    assert queue.reap_stale() == []
    done = queue.item_path(pool.DONE, key)
    assert done.exists() and queue.ledger().held_keys() == []
    assert _audit(queue, done) == _clock_of(item).deadline_unix


def test_a_scope_that_stays_populated_holds_the_tokens_until_it_empties(
        tmp_path, broker, monkeypatch):
    queue, item, outcome = _clean_attempt(tmp_path, broker)
    key = item["action_key"]
    broker.kernel.stuck = True         # a task in uninterruptible sleep survives the stop
    dst = _finish(queue, item, outcome)
    assert dst == queue.item_path(pool.CLAIMED, key)
    assert queue.ledger().held_keys() == [key]
    assert not broker.record(key).get("released_unix")
    assert queue.reap_stale() == []          # still populated: still held
    assert queue.ledger().held_keys() == [key]
    live = pool._read_json(dst)
    assert reservation.attempt_release_audit(queue.queue, live) is None
    # The kernel finally lets go; settlement comes after the deadline, so the
    # tokens return on proof and the audit reads UNKNOWN.
    broker.kernel.stuck = False
    skew = Skew(monkeypatch)
    skew.to(_clock_of(item).deadline_unix + 1.0)
    assert queue.reap_stale() == []
    done = queue.item_path(pool.DONE, key)
    assert done.exists() and queue.ledger().held_keys() == []
    assert broker.record(key)["released_unix"]
    terminal = pool._read_json(done)
    assert terminal["status"] == "executed"
    assert _audit(queue, done) is None
    phases = terminal["lifetime_evidence"]["phases"]
    assert phases["scope_settlement"]["enforced"] is False
    assert phases["resource_release"]["enforced"] is False


def test_settlement_after_the_deadline_returns_tokens_but_reads_unknown(
        tmp_path, broker, monkeypatch):
    queue, item, outcome = _clean_attempt(tmp_path, broker)
    key = item["action_key"]
    Skew(monkeypatch).to(_clock_of(item).deadline_unix + 1.0)
    dst = _finish(queue, item, outcome)
    # Expiry never withholds capacity once settlement is proved, and never
    # releases it before; the audit just does not call a late release a bound.
    assert dst == queue.item_path(pool.DONE, key)
    assert queue.ledger().held_keys() == []
    assert _audit(queue, dst) is None
    terminal = pool._read_json(dst)
    phases = terminal["lifetime_evidence"]["phases"]
    assert {phase for phase, entry in phases.items() if entry["enforced"] is not True} == {
        "cleanup", "scope_settlement", "resource_release"}
    # Original results, logs and attempt evidence stay intact.
    assert terminal["status"] == "executed"
    attempt = queue.attempt_outcomes(terminal)[0]
    assert attempt["detail"]["returncode"] == outcome["returncode"]
    assert terminal["detail"]["container_cleanup"]["complete"] is True


def test_a_release_the_ledger_does_not_show_is_not_filed(tmp_path, broker, monkeypatch):
    queue, item, outcome = _clean_attempt(tmp_path, broker)
    key = item["action_key"]
    monkeypatch.setattr(pool.PoolQueue, "_release_reservation",
                        lambda self, *args, **kwargs: 0)
    dst = _finish(queue, item, outcome)
    assert dst == queue.item_path(pool.DONE, key)
    assert queue.ledger().held_keys() == [key]      # not returned, so not recorded as returned
    terminal = pool._read_json(dst)
    assert "lifetime_evidence" not in terminal        # no release record was filed
    assert "resource_release" not in terminal["detail"]["lifetime_evidence"]["phases"]
    assert _audit(queue, dst) is None


def test_a_lease_expiry_conclusion_never_supplies_a_finite_bound(tmp_path, broker):
    # The worker dies before it finishes. Only the lease reaper concludes the
    # claim: it stops and settles the scope before the tokens return, and what
    # it files carries no lifetime evidence, so no bound is audited.
    queue, item, outcome = _clean_attempt(tmp_path, broker)
    key = item["action_key"]
    assert queue.reap_stale(timeout_s=-1) == [key]
    ready, failed = queue.item_path(pool.READY, key), queue.item_path(pool.FAILED, key)
    assert ready.exists() != failed.exists()
    assert queue.ledger().held_keys() == []
    assert broker.record(key)["released_unix"]
    conclusion = pool._read_json(ready if ready.exists() else failed)
    assert "lifetime_evidence" not in conclusion
    assert "lifetime_evidence" not in (conclusion.get("detail") or {})
    assert reservation.attempt_release_audit(queue.queue, conclusion) is None


def test_a_stale_owner_never_files_evidence_for_a_live_successor(tmp_path, broker):
    queue, item, outcome = _clean_attempt(tmp_path, broker)
    key = item["action_key"]
    # A successor attempt holds the key now: a different claim, a new scope.
    live = pool._read_json(queue.item_path(pool.CLAIMED, key))
    live["claimed_by"] = "successor"
    live["claimed_unix"] = live["claimed_unix"] + 1.0
    pool._write_json_atomic(queue.item_path(pool.CLAIMED, key), live)
    before = queue.item_path(pool.CLAIMED, key).read_bytes()
    _finish(queue, item, outcome)
    assert queue.item_path(pool.CLAIMED, key).read_bytes() == before
    assert not queue.item_path(pool.DONE, key).exists()
    assert queue.ledger().held_keys() == [key]


# -- the stop alarm --


def test_the_alarm_stops_a_running_payload_once_at_the_stop_instant():
    now = [0.0]
    stops: list[float] = []
    alarm = lifetime_fence.StopAlarm(
        10.0, lambda: stops.append(now[0]), alive=lambda: True,
        now=lambda: now[0], poll_s=0.005)
    alarm.arm()
    time.sleep(0.05)
    assert stops == [] and alarm.fired_unix is None
    now[0] = 10.0
    assert wait_for(lambda: stops)
    assert alarm.fire() is True              # idempotent: no second stop
    assert alarm.disarm() is False
    assert stops == [10.0] and alarm.fired_unix == 10.0 and alarm.error is None


def test_the_alarm_never_stops_a_payload_that_ended_or_a_disarmed_alarm():
    now = [0.0]
    stops: list[float] = []
    ended = lifetime_fence.StopAlarm(
        5.0, lambda: stops.append(1), alive=lambda: False, now=lambda: now[0], poll_s=0.005)
    ended.arm()
    now[0] = 6.0
    time.sleep(0.05)
    assert ended.fire() is False and ended.disarm() is True and stops == []
    quiet = lifetime_fence.StopAlarm(
        5.0, lambda: stops.append(2), alive=lambda: True, now=lambda: now[0], poll_s=0.005)
    now[0] = 0.0
    quiet.arm()
    assert quiet.disarm() is True
    now[0] = 9.0
    time.sleep(0.05)
    assert quiet.fire() is False and stops == []


def test_a_stop_that_raises_is_recorded_and_not_repeated():
    calls = []

    def stop():
        calls.append(1)
        raise OSError("the broker is down")

    alarm = lifetime_fence.StopAlarm(
        0.0, stop, alive=lambda: True, now=lambda: 1.0, poll_s=0.005)
    assert alarm.fire() is True
    assert isinstance(alarm.error, OSError) and calls == [1]
    assert alarm.fire() is True and calls == [1]


def test_concurrent_callers_deliver_exactly_one_stop():
    delivered: list[int] = []
    alarm = lifetime_fence.StopAlarm(
        0.0, lambda: (time.sleep(0.05), delivered.append(1)),
        alive=lambda: True, now=lambda: 1.0, poll_s=0.005)
    results: list[bool] = []
    threads = [threading.Thread(target=lambda: results.append(alarm.fire()))
               for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)
    assert delivered == [1] and results == [True] * 8


# -- the materializer under a deadline --


def _item(tmp_path):
    from test_pool import _materialization_item
    return _materialization_item(tmp_path)


def test_git_calls_are_clipped_to_the_time_the_deadline_leaves(tmp_path, monkeypatch):
    item = _item(tmp_path)
    timeouts: list[float] = []
    real = pb._git_run

    def spy(root, *args, **kwargs):
        timeouts.append(kwargs["timeout"])
        return real(root, *args, **kwargs)

    monkeypatch.setattr(pb, "_git_run", spy)
    with materialize._execution_checkout(
            item, local_checkout_root=tmp_path / "bounded",
            deadline_unix=time.time() + 30.0):
        pass
    assert timeouts and all(0 < seconds <= 30.0 for seconds in timeouts)
    timeouts.clear()
    with materialize._execution_checkout(item, local_checkout_root=tmp_path / "plain"):
        pass
    assert timeouts and set(timeouts) == {120.0}      # unchanged without a deadline


def test_a_git_call_that_outlives_the_deadline_is_the_deadline_refusal(tmp_path, monkeypatch):
    item = _item(tmp_path)

    def hang(root, *args, **kwargs):
        raise subprocess.TimeoutExpired("git", kwargs["timeout"])

    monkeypatch.setattr(pb, "_git_run", hang)
    with pytest.raises(materialize.MaterializationDeadline):
        with materialize._execution_checkout(
                item, local_checkout_root=tmp_path / "bounded",
                deadline_unix=time.time() + 30.0):
            pass
    # No tree is left behind, and without a deadline the same stall is the
    # ordinary materialization error with the old 120 s call limit.
    assert not any((tmp_path / "bounded").iterdir())
    with pytest.raises(materialize.MaterializationError) as plain:
        with materialize._execution_checkout(item, local_checkout_root=tmp_path / "plain"):
            pass
    assert not isinstance(plain.value, materialize.MaterializationDeadline)


def test_a_deadline_already_passed_launches_no_git_at_all(tmp_path, monkeypatch):
    item = _item(tmp_path)
    launched: list[object] = []
    monkeypatch.setattr(pb, "_git_run", lambda *a, **k: launched.append(a))
    with pytest.raises(materialize.MaterializationDeadline):
        with materialize._execution_checkout(
                item, local_checkout_root=tmp_path / "bounded",
                deadline_unix=time.time() - 1.0):
            pass
    assert launched == []


def test_the_link_check_stops_at_the_deadline(tmp_path, monkeypatch):
    tree = tmp_path / "tree"
    (tree / "a" / "b").mkdir(parents=True)
    now = [100.0]
    monkeypatch.setattr(materialize, "_now", lambda: now[0])
    materialize._require_contained_materialized_links(tree, deadline_unix=200.0)
    now[0] = 200.0
    with pytest.raises(materialize.MaterializationDeadline):
        materialize._require_contained_materialized_links(tree, deadline_unix=200.0)


def test_bounded_tree_removal_never_follows_a_link(tmp_path):
    tree = tmp_path / "tree"
    (tree / "a" / "b").mkdir(parents=True)
    (tree / "a" / "b" / "file").write_text("x")
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "keep").write_text("kept")
    (tree / "a" / "to-dir").symlink_to(outside)
    (tree / "dangling").symlink_to(tmp_path / "nowhere")
    materialize._remove_tree_before(tree, time.time() + 60.0)
    assert not tree.exists()
    assert (outside / "keep").read_text() == "kept"


def _swap_a_directory_for_a_link(monkeypatch, directory: Path, target: Path, *,
                                 before: str) -> list[bool]:
    """Replace ``directory`` by a link to ``target`` just before ``before`` is unlinked.

    This is what a surviving descendant can do between the removal's listing
    of a directory and its deletion of the entries.
    """

    real_unlink = os.unlink
    swapped: list[bool] = []

    def unlink(path, *args, **kwargs):
        if not swapped and os.fspath(path).endswith(before):
            swapped.append(True)
            directory.rename(directory.with_name(directory.name + "-moved"))
            directory.symlink_to(target)
        return real_unlink(path, *args, **kwargs)

    monkeypatch.setattr(os, "unlink", unlink)
    return swapped


def test_bounded_tree_removal_never_follows_a_directory_swapped_for_a_link(
        tmp_path, monkeypatch):
    # The removal lists a directory, and a descendant replaces it by a link to
    # a tree outside the checkout before the entries are deleted. Deleting by
    # path name would follow the link and remove the outside file.
    tree = tmp_path / "tree"
    (tree / "a" / "b").mkdir(parents=True)
    (tree / "a" / "b" / "victim").write_text("inside")
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "victim").write_text("outside")
    swapped = _swap_a_directory_for_a_link(
        monkeypatch, tree / "a" / "b", outside, before="victim")
    with pytest.raises(OSError):
        materialize._remove_tree_before(tree, time.time() + 60.0)
    assert swapped == [True]
    assert (outside / "victim").read_text() == "outside"


def test_bounded_tree_removal_never_follows_a_swapped_top_directory(tmp_path, monkeypatch):
    tree = tmp_path / "tree"
    tree.mkdir()
    (tree / "victim").write_text("inside")
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "victim").write_text("outside")
    swapped = _swap_a_directory_for_a_link(monkeypatch, tree, outside, before="victim")
    with pytest.raises(OSError):
        materialize._remove_tree_before(tree, time.time() + 60.0)
    assert swapped == [True]
    assert (outside / "victim").read_text() == "outside"


def test_bounded_tree_removal_refuses_a_directory_replaced_by_another_directory(
        tmp_path, monkeypatch):
    # A link is not the only swap: another real directory can take the name
    # between the listing and the open. The open succeeds, so only the
    # comparison with the listed entry keeps the deletion inside the tree.
    tree = tmp_path / "tree"
    (tree / "a" / "b").mkdir(parents=True)
    (tree / "a" / "b" / "mine").write_text("inside")
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    (elsewhere / "theirs").write_text("outside")
    real_open = os.open
    swapped: list[bool] = []

    def opening(path, flags, *args, **kwargs):
        if not swapped and kwargs.get("dir_fd") is not None and os.fspath(path) == "b":
            swapped.append(True)
            (tree / "a" / "b").rename(tree / "a" / "b-moved")
            elsewhere.rename(tree / "a" / "b")
        return real_open(path, flags, *args, **kwargs)

    monkeypatch.setattr(os, "open", opening)
    with pytest.raises(OSError):
        materialize._remove_tree_before(tree, time.time() + 60.0)
    assert swapped == [True]
    assert (tree / "a" / "b" / "theirs").read_text() == "outside"
    assert (tree / "a" / "b-moved" / "mine").read_text() == "inside"


def test_bounded_tree_removal_refuses_a_link_at_its_root(tmp_path):
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "keep").write_text("kept")
    tree = tmp_path / "tree"
    tree.symlink_to(outside)
    with pytest.raises(OSError):
        materialize._remove_tree_before(tree, time.time() + 60.0)
    assert (outside / "keep").read_text() == "kept"
    assert tree.is_symlink()


def test_bounded_tree_removal_stops_at_the_deadline_and_leaves_the_rest(tmp_path):
    tree = tmp_path / "tree"
    (tree / "a").mkdir(parents=True)
    (tree / "a" / "file").write_text("x")
    with pytest.raises(materialize.MaterializationDeadline):
        materialize._remove_tree_before(tree, time.time() - 1.0)
    assert (tree / "a" / "file").exists()


def test_a_removal_past_its_deadline_is_a_recorded_leak_not_a_failure(
        tmp_path, monkeypatch, capsys):
    item = _item(tmp_path)
    root = tmp_path / "materialized"
    now = [time.time()]
    monkeypatch.setattr(materialize, "_now", lambda: now[0])
    with materialize._execution_checkout(
            item, local_checkout_root=root,
            deadline_unix=now[0] + 1000.0, cleanup_deadline_unix=now[0] + 500.0) as checkout:
        temporary = checkout.parent
        now[0] += 600.0          # the payload ran on, past the removal deadline
    # The action's result stands; the tree is a durable, visible leak.
    assert temporary.is_dir()
    (leak,) = (root / "cleanup-failures").glob("*.json")
    assert "deadline" in leak.read_text()
    assert "checkout cleanup failed" in capsys.readouterr().err


def test_a_removal_inside_its_deadline_removes_the_tree(tmp_path):
    item = _item(tmp_path)
    root = tmp_path / "materialized"
    with materialize._execution_checkout(
            item, local_checkout_root=root,
            deadline_unix=time.time() + 600.0,
            cleanup_deadline_unix=time.time() + 900.0) as checkout:
        temporary = checkout.parent
        assert (checkout / "payload.txt").exists()
    assert not temporary.exists()
    assert not (root / "cleanup-failures").exists()
