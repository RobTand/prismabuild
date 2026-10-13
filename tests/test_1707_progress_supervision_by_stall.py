"""Supervise payloads by progress: stop on a stall, not a wall clock (#1707)."""
import copy
import json
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import core as pb, pool  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import pbstatus  # noqa: E402
import pbrun  # noqa: E402

from admitted_queue_fixture import AdmittedQueueFixture  # noqa: E402

def _claimed(tmp_path, *, timeout_s=None, stall_s=None, progress=None):
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    (checkout / "task.py").write_text("open('result', 'w').write('ok')\n")
    params = {}
    if timeout_s is not None:
        params["execution_timeout_s"] = timeout_s
    if stall_s is not None:
        params["stall_allowance_s"] = stall_s
    if progress is not None:
        params[pb.PROGRESS_PARAM] = progress
    action = pb.seal_action({
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {"definition_id": "tests/1707", "definition_version": "v1",
                 "task_class": "generation", "determinism": "deterministic",
                 "artifact_family": "generic", "artifact_kind": "generic",
                 "argv": [sys.executable, "task.py"],
                 "working_directory": ".", "result_path": "result"},
        "inputs": [], "code_closure": pb.build_code_closure(checkout, ["task.py"]),
        "params": params,
        "environment": {"variables": {}, "toolchain": {}},
        "execution_scope": {"portability": "portable", "platform_key": None,
                            "host_class": None},
    })
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    cas.publish_action_request(action)
    queue = AdmittedQueueFixture(
        pool.PoolQueue(tmp_path / "queue"),
        capacity={"cpu": 8, "mem_gb": 16},
        default_demand={"cpu": 1, "mem_gb": 1})
    queue.publish(action_key=action["action_key"], cas_root=cas.root,
                  checkout_root=checkout,
                  worker_script=Path(__file__).resolve().parents[1]
                  / "tools" / "prismabuild_worker.py")
    return queue, queue.claim()


class _Clock:
    def __init__(self):
        self.elapsed = 0.0

    def monotonic(self):
        return self.elapsed

    def time(self):
        return 10000.0 + self.elapsed


@dataclass(frozen=True)
class _Step:
    seconds: float
    stdout: str = ""
    stderr: str = ""
    cpu: float = 0.0
    report: tuple[str, int] | None = None
    foreign: bool = False
    returncode: int | None = None


class _ScriptedProcess:
    pid = 99999999

    def __init__(self, monkeypatch, queue, clock, steps):
        self.clock = clock
        self.steps = iter(steps)
        self.returncode = None
        self.stdout_text = self.stderr_text = ""
        self.cpu = 0.0
        self.terminated = False
        self.environment = None
        monkeypatch.setattr(pool, "time", clock)
        monkeypatch.setattr(pool, "_now", clock.time)
        monkeypatch.setattr(pool, "subprocess", SimpleNamespace(
            **{**vars(subprocess), "Popen": self.launch}))
        monkeypatch.setattr(pb, "_terminate_process_group", self.terminate)
        monkeypatch.setattr(pool, "_observe_child", lambda *args, **kwargs: {
            "source": "scripted-payload", "cpu_seconds": self.cpu})
        monkeypatch.setattr(queue.queue, "_box_window", lambda *args: {
            "source": "scripted-box"})

    def launch(self, argv, **kwargs):
        assert self.environment is None
        self.argv = argv
        self.environment = kwargs["env"]
        return self

    def poll(self):
        return self.returncode

    def communicate(self, *, timeout):
        if self.terminated:
            return self.stdout_text, self.stderr_text
        step = next(self.steps)
        self.clock.elapsed += step.seconds
        self.stdout_text, self.stderr_text = step.stdout, step.stderr
        self.cpu = step.cpu
        if step.report is not None:
            phase, units = step.report
            Path(self.environment[pb.ACTION_PROGRESS_PATH_ENV]).write_text(
                json.dumps({
                    "schema": pb.PROGRESS_RECORD_SCHEMA_V1,
                    "token": ("foreign" if step.foreign else
                              self.environment[pb.ACTION_PROGRESS_TOKEN_ENV]),
                    "phase": phase, "units_completed": units,
                    "reported_unix": self.clock.time(), "unit": "widgets"}))
        if step.returncode is not None:
            self.returncode = step.returncode
            return self.stdout_text, self.stderr_text
        raise subprocess.TimeoutExpired(
            self.argv, timeout, output=step.stdout.encode(),
            stderr=step.stderr.encode())

    def terminate(self, process, *, grace_s):
        assert process is self
        self.terminated = True
        self.returncode = -15

    def assert_finished(self):
        assert list(self.steps) == []
        assert self.returncode is not None


def _progress_policy(*phases):
    return {"schema": pb.PROGRESS_POLICY_SCHEMA_V1,
            "phases": [{"name": name, "grace_s": grace}
                       for name, grace in phases]}


# -- the watch itself, on a fake clock --------------------------------------

def test_steady_activity_never_hits_a_deadline_it_does_not_have():
    """Steady output past the old 7200 s budget is not a stall."""

    watch = pool.ActivityWatch(1800.0, started=0.0)
    now = 0.0
    out = 0
    for _ in range(8):
        now += 1000.0
        out += 10
        assert watch.observe(now=now, stdout_bytes=out) is True
    assert now > 7200.0
    assert now < watch.stall_deadline()


def test_silence_past_the_allowance_is_a_stall():
    """No output and no CPU past the allowance ends the run."""

    watch = pool.ActivityWatch(100.0, started=0.0)
    assert watch.observe(now=10.0, stdout_bytes=5) is True
    assert watch.observe(now=20.0, stdout_bytes=5, cpu_seconds=3.0) is False
    assert 20.0 < watch.stall_deadline() <= 110.0
    assert 200.0 >= watch.stall_deadline()


def test_cpu_activity_alone_restarts_the_quiet():
    """A payload burning CPU without printing is still active."""

    watch = pool.ActivityWatch(100.0, started=0.0)
    assert watch.observe(now=10.0, cpu_seconds=12.0) is False
    assert watch.observe(now=20.0, cpu_seconds=25.0) is True
    assert watch.stall_deadline() == 120.0


def test_checkpoint_io_does_not_spend_the_allowance():
    """Shared I/O pauses the quiet; it never grants fresh time."""

    watch = pool.ActivityWatch(100.0, started=0.0)
    watch.observe(now=10.0, stdout_bytes=7)
    watch.shift(25.0)
    assert watch.stall_deadline() == 135.0


# -- the sealed allowance ----------------------------------------------------

def test_the_submitter_sets_the_allowance_and_the_ceiling_clamps_it(tmp_path):
    _, item = _claimed(tmp_path, stall_s=60.0)
    stalled = pool.default_stall(item, 7200.0)
    assert stalled.requested == 60.0
    assert stalled.effective == 60.0
    assert stalled.clamped is False
    clamped = pool.default_stall(item, 0.4)
    assert clamped.effective == 0.4
    assert clamped.clamped is True
    assert clamped.as_record()["stall_allowance_s"] == 0.4


def test_the_default_allowance_is_one_long_job_interval(tmp_path):
    _, item = _claimed(tmp_path)
    stalled = pool.default_stall(item, 7200.0)
    assert stalled.requested is None
    assert stalled.effective == pool.DEFAULT_STALL_ALLOWANCE_S == 1800.0
    assert stalled.clamped is False


@pytest.mark.parametrize("bad", [0, -5, "60", True, float("inf")])
def test_an_unusable_stall_allowance_is_refused_at_seal(tmp_path, bad):
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    (checkout / "task.py").write_text("open('result','w').write('ok')\n")
    with pytest.raises(pb.ActionContractError):
        pb.seal_action({
            "schema": pb.ACTION_SCHEMA_V2,
            "task": {"definition_id": "t", "definition_version": "v1",
                     "task_class": "generation", "determinism": "deterministic",
                     "artifact_family": "generic", "artifact_kind": "generic",
                     "argv": [sys.executable, "task.py"],
                     "working_directory": ".", "result_path": "result"},
            "inputs": [],
            "code_closure": pb.build_code_closure(checkout, ["task.py"]),
            "params": {"stall_allowance_s": bad},
            "environment": {"variables": {}, "toolchain": {}},
            "execution_scope": {"portability": "portable",
                                "platform_key": None, "host_class": None},
        })


# -- the run: steady work, silence, explicit budget, memory guard ------------

def test_a_quiet_payload_stops_on_its_stall_allowance(tmp_path, monkeypatch):
    """Silence past the allowance stops with a stall verdict."""

    queue, item = _claimed(tmp_path, stall_s=0.5)
    process = _ScriptedProcess(monkeypatch, queue, _Clock(), [_Step(0.5)])
    outcome = queue.execute(item, timeout_s=5.0, heartbeat_s=0.05,
                            timeout_grace_s=0.2)
    assert outcome["status"] == "timeout"
    assert outcome["termination_reason"] == "no_progress"
    assert outcome["execution_governed_by"] == "stall"
    assert outcome["elapsed_s"] < 5.0
    assert outcome["stall"]["allowance_s"] == 0.5
    assert outcome["progress_observation"]["source"] == "activity"
    process.assert_finished()


def test_an_explicit_budget_still_stops_an_active_payload(tmp_path, monkeypatch):
    """A sealed wall-clock budget ends even a payload showing activity."""

    queue, item = _claimed(tmp_path, timeout_s=0.5)
    process = _ScriptedProcess(monkeypatch, queue, _Clock(), [
        _Step(0.5, stdout="still working\n")])
    outcome = queue.execute(item, timeout_s=5.0, heartbeat_s=0.05,
                            timeout_grace_s=0.2)
    assert outcome["status"] == "timeout"
    assert outcome["termination_reason"] == "execution_deadline"
    assert outcome["execution_governed_by"] == "deadline"
    assert outcome["execution_timeout_requested_s"] == 0.5
    assert outcome["elapsed_s"] < 5.0
    process.assert_finished()


def test_the_memory_guard_still_names_its_own_victims():
    """The D30 guard keeps its verdict and its precedence."""

    assert pool.PoolQueue._resource_failure(
        {"oom_local": 2}) == "memory_limit_oom"
    assert pool.PoolQueue._resource_failure(
        {"termination_reason": "memory_limit_oom",
         "termination_evidence": {"victim": "payload"}}) == "memory_limit_oom"
    assert pool.PoolQueue._resource_failure({}) is None


@pytest.mark.parametrize(("steps", "heartbeat", "status", "accepted", "rejection"), [
    ([_Step(0.5, report=("run", 1)), _Step(0.5, report=("run", 2)),
      _Step(0.5, report=("run", 3), returncode=0)], 0.5, "executed", 3, None),
    ([_Step(0.5, stdout="a"), _Step(0.5, stdout="ab")],
     0.5, "timeout", 0, None),
    ([_Step(0.5, report=("run", 1)), _Step(0.5, report=("run", 1)),
      _Step(0.5, report=("run", 1))], 0.5, "timeout", 1, "replayed"),
    ([_Step(0.5, report=("run", 1), foreign=True),
      _Step(0.5, report=("run", 2), foreign=True)],
     0.5, "timeout", 0, "foreign token"),
    ([_Step(1.0, report=("run", 1)),
      _Step(1.0, report=("run", 2), returncode=0)], 2.0, "executed", 2, None),
])
def test_supervision_prefactor_preserves_semantic_execution_outcomes(
        tmp_path, monkeypatch, steps, heartbeat, status, accepted, rejection):
    queue, item = _claimed(
        tmp_path,
        progress=_progress_policy(("run", 1.0)))
    process = _ScriptedProcess(monkeypatch, queue, _Clock(), steps)
    outcome = queue.execute(item, timeout_s=1.0, heartbeat_s=heartbeat)
    observed = outcome["progress_observation"]
    assert outcome["status"] == status
    assert outcome["execution_governed_by"] == "progress"
    assert outcome["execution_timeout_s"] is None
    assert observed["source"] == "action-progress"
    assert observed["accepted_count"] == accepted
    assert observed["last_rejection"] == rejection
    assert "stdout_bytes" not in observed
    if accepted:
        assert observed["last_accepted"]["units_completed"] == accepted
    if status == "timeout":
        assert outcome["termination_reason"] == "no_progress"
        assert outcome["returncode"] is None
        assert outcome["stall"]["phase"] == "run"
        assert outcome["stall"]["allowance_s"] == 1.0
        assert outcome["stall"]["quiet_s"] == 1.0
        assert observed["rejected_count"] == (3 if rejection else 0)
        assert outcome["execution_observation"]["stdout_bytes"] == (
            2 if steps[-1].stdout else 0)
    else:
        assert outcome["returncode"] == 0
        assert "stall" not in outcome
        assert "termination_reason" not in outcome
        assert outcome["elapsed_s"] > 1.0
    assert not queue.action_progress_path(item["action_key"]).exists()
    process.assert_finished()


@pytest.mark.parametrize(
    ("steps", "budget", "status", "reason", "advances", "stdout_bytes", "stderr_bytes", "cpu"), [
    ([_Step(0.5), _Step(0.5)], None, "timeout", "no_progress", 0, 0, 0, 0.0),
    ([_Step(0.5, stdout="a"), _Step(0.5, stdout="ab"),
      _Step(0.5, stdout="abc", stderr="é", returncode=0)],
     None, "executed", None, 3, 3, 2, 0.0),
    ([_Step(0.5, cpu=1), _Step(0.5, cpu=2), _Step(0.5, cpu=3, returncode=0)],
     None, "executed", None, 2, 0, 0, 2.0),
    ([_Step(0.5, stdout="a"), _Step(0.5, stdout="ab")],
     1.0, "timeout", "execution_deadline", 2, 2, 0, 0.0),
    ([_Step(0.5, returncode=7)], None, "failed", None, 0, 0, 0, 0.0),
])
def test_supervision_prefactor_preserves_activity_execution_outcomes(
        tmp_path, monkeypatch, steps, budget, status, reason, advances,
        stdout_bytes, stderr_bytes, cpu):
    queue, item = _claimed(tmp_path,
                           stall_s=1.0, timeout_s=budget)
    process = _ScriptedProcess(monkeypatch, queue, _Clock(), steps)
    outcome = queue.execute(item, timeout_s=1.0, heartbeat_s=0.5)
    observed = outcome["progress_observation"]
    assert outcome["status"] == status
    assert outcome.get("termination_reason") == reason
    assert outcome["execution_governed_by"] == (
        "deadline" if budget is not None else "stall")
    assert observed["source"] == "activity"
    assert observed["accepted_count"] == advances
    assert observed["rejected_count"] == 0
    assert observed["cpu_seconds"] == cpu
    assert observed["stdout_bytes"] == stdout_bytes
    assert observed["stderr_bytes"] == stderr_bytes
    if reason == "no_progress":
        assert outcome["stall"] == {
            "allowance_s": 1.0, "phase": "activity", "last_landing": None,
            "quiet_s": 1.0, "stdout_bytes": 0, "stderr_bytes": 0,
            "cpu_seconds": 0.0, "observations": 3, "advances": 0}
    else:
        assert "stall" not in outcome
    process.assert_finished()


@pytest.mark.parametrize("activity_steps", [
    [_Step(0.5, stdout="a", stderr="é"),
     _Step(0.5, stdout="ab", stderr="éé")],
    [_Step(0.5, cpu=1.0), _Step(0.5, cpu=2.0)],
])
def test_activity_sampling_does_not_mutate_semantic_accounting(
        tmp_path, monkeypatch, activity_steps):
    queue, item = _claimed(
        tmp_path, progress=_progress_policy(("startup", 1.0), ("run", 1.0)))
    process = _ScriptedProcess(monkeypatch, queue, _Clock(), [
        _Step(0.5, report=("run", 12)), *activity_steps])
    outcome = queue.execute(item, timeout_s=1.0, heartbeat_s=0.5)
    semantic = outcome["progress_observation"]
    assert outcome["status"] == "timeout"
    assert outcome["termination_reason"] == "no_progress"
    assert outcome["elapsed_s"] == 1.5
    assert semantic["accepted_count"] == 1
    assert semantic["phases_entered"] == 2
    assert semantic["phase"] == "run"
    assert semantic["last_accepted"]["units_completed"] == 12
    assert semantic["quiet_s"] == 1.0
    assert semantic["delivered_units_per_s"] == 12.0
    assert outcome["stall"]["delivered_units_per_s"] == 12.0
    assert "stdout_bytes" not in semantic
    assert "stderr_bytes" not in semantic
    assert "cpu_seconds" not in semantic
    observed = outcome["execution_observation"]
    assert observed["stdout_bytes"] == len(activity_steps[-1].stdout.encode())
    assert observed["stderr_bytes"] == len(activity_steps[-1].stderr.encode())
    assert observed["child"]["cpu_seconds"] == activity_steps[-1].cpu
    process.assert_finished()


@pytest.mark.parametrize(("returncode", "status"), [(0, "executed"), (7, "failed")])
def test_final_semantic_report_preserves_the_process_verdict(
        tmp_path, monkeypatch, returncode, status):
    queue, item = _claimed(tmp_path, progress=_progress_policy(("run", 1.0)))
    process = _ScriptedProcess(monkeypatch, queue, _Clock(), [
        _Step(0.5, report=("run", 6), returncode=returncode)])
    outcome = queue.execute(item, timeout_s=1.0, heartbeat_s=0.5)
    assert outcome["status"] == status
    assert outcome["returncode"] == returncode
    assert "termination_reason" not in outcome
    assert "stall" not in outcome
    assert outcome["progress_observation"]["accepted_count"] == 1
    assert outcome["progress_observation"]["last_accepted"]["units_completed"] == 6
    assert outcome["progress_observation"]["quiet_s"] == 0.0
    assert not queue.action_progress_path(item["action_key"]).exists()
    process.assert_finished()


def test_supervision_prefactor_preserves_checkpoint_and_wait_credits(tmp_path, monkeypatch):
    queue, item = _claimed(tmp_path,
                           progress=_progress_policy(("run", 1.0)))
    clock = _Clock()
    process = _ScriptedProcess(monkeypatch, queue, clock, [
        _Step(0.5), _Step(0.5), _Step(0.5, report=("run", 1), returncode=0)])
    write_lease = queue.write_lease
    writes = []

    def delayed_lease(*args, **kwargs):
        write_lease(*args, **kwargs)
        writes.append(clock.elapsed)
        clock.elapsed += 10.0 if len(writes) == 1 else 3.0

    waits = []

    def verified_wait(*args, **kwargs):
        waits.append(clock.elapsed)
        return {"exempt": True, "since_unix": clock.time() - 1.0,
                "reason": "verified mover"}

    monkeypatch.setattr(queue.queue, "write_lease", delayed_lease)
    monkeypatch.setattr(queue.queue, "staged_wait_verdict", verified_wait)
    outcome = queue.execute(item, timeout_s=1.0, heartbeat_s=0.5)
    assert writes == [0.0, 10.5, 14.0]
    assert waits == [17.0]
    assert outcome["status"] == "executed"
    assert outcome["elapsed_s"] == 17.5
    assert outcome["progress_observation"]["staged_wait_exempt_s"] == 1.0
    assert outcome["progress_observation"]["accepted_count"] == 1
    assert outcome["progress_observation"]["quiet_s"] == 0.0
    assert "stall" not in outcome
    process.assert_finished()


def test_activity_checkpoint_refunds_preserve_quiet(tmp_path, monkeypatch):
    queue, item = _claimed(tmp_path, stall_s=1.0)
    clock = _Clock()
    process = _ScriptedProcess(monkeypatch, queue, clock, [_Step(0.5), _Step(0.5)])
    write_lease = queue.write_lease
    writes = []

    def delayed_lease(*args, **kwargs):
        write_lease(*args, **kwargs)
        writes.append(clock.elapsed)
        clock.elapsed += 10.0 if len(writes) == 1 else 3.0

    monkeypatch.setattr(queue.queue, "write_lease", delayed_lease)
    outcome = queue.execute(item, timeout_s=1.0, heartbeat_s=0.5)
    assert writes == [0.0, 10.5, 14.0]
    assert outcome["status"] == "timeout"
    assert outcome["termination_reason"] == "no_progress"
    assert outcome["elapsed_s"] == 17.0
    assert outcome["progress_observation"]["accepted_count"] == 0
    assert outcome["progress_observation"]["quiet_s"] == 1.0
    assert outcome["stall"]["quiet_s"] == 1.0
    process.assert_finished()


def test_execution_samples_keep_activity_totals_and_output_time_separate(monkeypatch):
    clock = _Clock()
    monkeypatch.setattr(pool, "_now", clock.time)
    cpu_samples = iter([3.0, 5.0, 5.0])
    monkeypatch.setattr(pool, "_observe_child", lambda *args, **kwargs: {
        "cpu_seconds": next(cpu_samples)})
    process = SimpleNamespace(poll=lambda: None)
    watch = pool.ActivityWatch(10.0, started=0.0)
    initial = pool._observe_execution(process)
    assert not watch.observe(now=0.0, cpu_seconds=pool._observed_cpu_s(initial))
    clock.elapsed = 2.0
    moving = pool._observe_execution(process, initial, stdout="é".encode(), stderr=b"err")
    assert moving["stdout_bytes"] == 2
    assert moving["stderr_bytes"] == 3
    assert moving["last_output_unix"] == 10002.0
    assert watch.observe(
        now=2.0, stdout_bytes=moving["stdout_bytes"],
        stderr_bytes=moving["stderr_bytes"], cpu_seconds=pool._observed_cpu_s(moving))
    clock.elapsed = 5.0
    quiet = pool._observe_execution(process, moving, stdout="é".encode(), stderr=b"err")
    assert quiet["sampled_unix"] == 10005.0
    assert quiet["last_output_unix"] == 10002.0
    assert not watch.observe(
        now=5.0, stdout_bytes=quiet["stdout_bytes"],
        stderr_bytes=quiet["stderr_bytes"], cpu_seconds=pool._observed_cpu_s(quiet))
    assert watch.stall_deadline() == 12.0
    assert watch.as_record(now=5.0)["cpu_seconds"] == 5.0
    assert watch.as_record(now=5.0)["accepted_count"] == 1


@pytest.mark.parametrize("with_contention", [False, True])
def test_supervision_records_preserve_rates_and_each_credit(monkeypatch, with_contention):
    report = json.dumps({
        "schema": pb.PROGRESS_RECORD_SCHEMA_V1, "token": "launch",
        "phase": "run", "units_completed": 24, "reported_unix": 10002.0,
        "unit": "bytes"}).encode()
    monkeypatch.setattr(pb, "_read_regular_file_nofollow", lambda *args, **kwargs: report)
    watch = pool.ProgressWatch(
        Path("unused-report"), "launch", pool.ProgressPolicy(
            (pool.ProgressPhase("run", 10.0, None),), None), started=0.0)
    assert watch.sample(now=2.0)
    watch.shift(3.0)
    assert watch.exempt_staged_wait(
        {"exempt": True}, now=6.0, since_monotonic=2.0) == 4.0
    assert watch.exempt_pool_contention(
        {"verdict": "over"}, now=8.0, since_monotonic=4.0) == 2.0
    assert watch.exempt_start_gate(now=10.0, since_monotonic=4.0) == 2.0
    watch.grant_start_gate_edge(1.0)
    assert watch.exempt_reader_plan(
        {"exempt": True}, now=12.0, since_monotonic=4.0) == 2.0
    assert watch.exempt_export_wait(
        {"exempt": True}, now=14.0, since_monotonic=4.0) == 2.0
    assert watch.exempt_queued_child_wait(
        {"exempt": True}, now=16.0, since_monotonic=4.0) == 2.0
    assert watch.exempt_staged_wait(
        {"exempt": True}, now=16.0, since_monotonic=2.0) == 0.0
    assert watch.exempt_pool_contention(
        {"verdict": "under"}, now=18.0, since_monotonic=16.0) == 0.0
    assert watch.exempt_queued_child_wait(
        {"exempt": False}, now=18.0, since_monotonic=16.0) == 0.0
    contention = None
    if with_contention:
        monkeypatch.setattr(pool, "POOL_MEMBER_STAT", lambda member: [0] * 11)
        contention = pool.PoolContentionProbe({
            "members": ["disk"], "max_read_await_ms": 10.0,
            "max_backlog_ms": 20.0, "priced_bytes_per_s": 80.0,
            "stage_root": "/unused-stage"}, now=0.0)
        monkeypatch.setattr(contention, "judge", lambda **kwargs: pytest.fail("sampled pool"))
    monkeypatch.setattr(watch, "sample", lambda **kwargs: pytest.fail("sampled report"))
    before = copy.deepcopy(vars(watch))
    assert not pool._stall_due(watch, now=29.999)
    assert pool._stall_due(watch, now=30.0)
    records = pool._supervision_records(
        watch, now=32.0, stalled=True, contention=contention)
    assert vars(watch) == before
    assert records["stall"] == {
        "allowance_s": 10.0, "phase": "run",
        "last_landing": {"phase": "run", "units_completed": 24,
                         "unit": "bytes", "reported_unix": 10002.0},
        "quiet_s": 12.0,
        "credited_s": {"staged_wait": 4.0, "pool_contention": 2.0,
                       "start_gate": 3.0, "reader_plan": 2.0,
                       "export_wait": 2.0, "queued_child_wait": 2.0},
        "delivered_units_per_s": 0.8,
        "delivered_bytes_per_s": 0.8 if with_contention else None,
        "priced_bytes_per_s": 80.0 if with_contention else None,
        "pool": None, "start_gate_holder": None}
    observed = records["progress_observation"]
    assert observed["accepted_count"] == 1
    assert observed["rejected_count"] == 0
    assert observed["phases_entered"] == 1
    assert observed["delivered_units_per_s"] == 0.8
    assert ("pool_contention" in observed) is with_contention
    if with_contention:
        assert observed["pool_contention"]["priced_bytes_per_s"] == 80.0
    assert "stall" not in pool._supervision_records(watch, now=32.0, stalled=False)


def test_activity_supervision_records_do_not_sample_or_renew(monkeypatch):
    watch = pool.ActivityWatch(10.0, started=0.0)
    assert watch.observe(now=1.0, stdout_bytes=4, stderr_bytes=2, cpu_seconds=3.0)
    watch.shift(2.0)
    monkeypatch.setattr(watch, "observe", lambda **kwargs: pytest.fail("sampled activity"))
    before = copy.deepcopy(vars(watch))
    assert not pool._stall_due(watch, now=12.999)
    assert pool._stall_due(watch, now=13.0)
    records = pool._supervision_records(watch, now=15.0, stalled=True)
    assert vars(watch) == before
    assert records["stall"] == {
        "allowance_s": 10.0, "phase": "activity", "last_landing": None,
        "quiet_s": 12.0, "stdout_bytes": 4, "stderr_bytes": 2,
        "cpu_seconds": 3.0, "observations": 1, "advances": 1}
    assert records["progress_observation"]["quiet_s"] == 12.0
    assert records["progress_observation"]["accepted_count"] == 1
    assert "stall" not in pool._supervision_records(watch, now=15.0, stalled=False)


# -- what the operator sees ---------------------------------------------------

def _row_with(tmp_path, key, observation):
    queue = pool.PoolQueue(tmp_path / "queue")
    fixture = AdmittedQueueFixture(
        queue, capacity={"cpu": 2, "mem_gb": 4},
        default_demand={"cpu": 1, "mem_gb": 1})
    fixture.publish(action_key=key, cas_root=tmp_path / "cas",
                    checkout_root=tmp_path, worker_script="/worker.py")
    item = fixture.claim(owner="worker")
    assert item is not None
    lease = json.loads(queue.lease_path(key).read_text(encoding="utf-8"))
    lease["published_unix"] = item["published_unix"]
    lease["progress_observation"] = observation
    queue.lease_path(key).write_text(json.dumps(lease), encoding="utf-8")
    return next(row for row in pbstatus.read_pool(queue.root)["jobs"]
                if row["action_key"] == key)


def test_pbstatus_shows_age_phase_counters_and_silence(tmp_path):
    """Each active action reads its age, phase, counters and silence."""

    watch = pool.ActivityWatch(900.0, started=0.0)
    assert watch.observe(now=1.0, stdout_bytes=120, cpu_seconds=30.0) is True
    row = _row_with(tmp_path, "d" * 64, watch.as_record(now=42.0))
    assert row["age_s"] is not None
    said = row["progress_observation"]
    assert "activity" in said
    assert "quiet 41/900s" in said
    assert "accepted" in said
    assert "out 120B" in said
    assert "cpu" in said
    rendered = "\n".join(pbstatus.pool_job_lines([row], {"empty": False}))
    assert "AGE" in rendered and "OUTPUT" in rendered and "PROGRESS" in rendered
    assert "activity quiet 41/900s" in rendered


# -- submission ---------------------------------------------------------------

def test_stall_and_progress_phases_cannot_combine():
    with pytest.raises(SystemExit, match="stall-s"):
        pbrun.parse_args(["--stall-s", "60", "--progress-phase", "run=60",
                          "--", "true"])


def test_awaited_batch_cli_parses_the_sealed_declaration(tmp_path):
    state = str(tmp_path / "controller-state")
    args = pbrun.parse_args([
        "--progress-phase", "run=60",
        "--awaited-batch", f"{'a' * 64}:{'b' * 64}:{state}",
        "--", "true",
    ])
    assert args.awaited_batch == {
        "schema": pb.AWAITED_BATCH_SCHEMA_V1,
        "parent_key": "a" * 64,
        "plan_key": "b" * 64,
        "controller_state": state,
    }


@pytest.mark.parametrize(("declared", "message"), [
    ("broken", "--awaited-batch must be PARENT:PLAN:STATE"),
    (f"invalid:{'b' * 64}:/controller-state", "parent_key"),
    (f"{'a' * 64}:invalid:/controller-state", "plan_key"),
    (f"{'a' * 64}:{'b' * 64}:relative-state", "controller_state"),
])
def test_awaited_batch_cli_refuses_malformed_declarations(declared, message):
    with pytest.raises(SystemExit, match=message):
        pbrun.parse_args([
            "--progress-phase", "run=60", "--awaited-batch", declared,
            "--", "true",
        ])


def test_stall_s_is_refused_on_the_slurm_lane():
    with pytest.raises(ValueError, match="--stall-s requires pool transport"):
        pbrun.require_stall_scope(stall_s=60.0, transport="slurm")
    pbrun.require_stall_scope(stall_s=60.0, transport="pool")
    pbrun.require_stall_scope(stall_s=None, transport="slurm")
