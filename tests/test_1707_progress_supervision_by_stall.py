"""Supervise payloads by progress: stop on a stall, not a wall clock (#1707)."""
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import core as pb, pool, census, submission, contracts  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import pbstatus  # noqa: E402
import pbrun  # noqa: E402

from admitted_queue_fixture import AdmittedQueueFixture  # noqa: E402

def _claimed(tmp_path, *, stall_s=None):
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    (checkout / "task.py").write_text("open('result', 'w').write('ok')\n")
    params = {}
    if stall_s is not None:
        params["stall_allowance_s"] = stall_s
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
    return queue, queue.claim(tags=contracts.offered_contract_tags(contracts.WorkerFacts()))


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


def test_the_default_allowance_matches_the_former_worker_ceiling(tmp_path):
    _, item = _claimed(tmp_path)
    stalled = pool.default_stall(item, 7200.0)
    assert stalled.requested is None
    assert stalled.effective == pool.DEFAULT_STALL_ALLOWANCE_S == 7200.0
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

def test_the_memory_guard_still_names_its_own_victims():
    """The D30 guard keeps its verdict and its precedence."""

    assert pool.PoolQueue._resource_failure(
        {"oom_local": 2}) == "memory_limit_oom"
    assert pool.PoolQueue._resource_failure(
        {"termination_reason": "memory_limit_oom",
         "termination_evidence": {"victim": "payload"}}) == "memory_limit_oom"
    assert pool.PoolQueue._resource_failure({}) is None


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
    lease = json.loads(queue.records.lease_path(key).read_text(encoding="utf-8"))
    lease["published_unix"] = item["published_unix"]
    lease["progress_observation"] = observation
    queue.records.lease_path(key).write_text(json.dumps(lease), encoding="utf-8")
    return next(row for row in census.action_rows(queue.root).value["jobs"]
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
        pbrun.main([
            "--progress-phase", "run=60", "--awaited-batch", declared,
            "--", "true",
        ])


def test_stall_s_is_refused_on_the_slurm_lane():
    with pytest.raises(ValueError, match="--stall-s requires pool transport"):
        submission.require_stall_scope(stall_s=60.0, transport="slurm")
    submission.require_stall_scope(stall_s=60.0, transport="pool")
    submission.require_stall_scope(stall_s=None, transport="slurm")


@pytest.mark.parametrize("source", ["stdout", "stderr", "cpu"])
def test_default_activity_completes_past_the_old_budget_on_a_fake_clock(tmp_path, monkeypatch, source):
    from test_progress_keeps_a_working_action_alive import _virtual_execution

    def script(child):
        for count in range(1, 9):
            if source == "stdout":
                child.stdout += "durable unit\n"
            elif source == "stderr":
                child.stderr += "durable unit\n"
            yield count * 1000.0

    with _virtual_execution(tmp_path, script, policy=None, ceiling=7200.0,
                            heartbeat_s=1000.0) as child:
        if source == "cpu":
            monkeypatch.setattr(pool, "_observe_child", lambda *args, **kwargs: {
                "cpu_seconds": child.now})
        outcome = child.execute()
        assert child.stopped_at is None
        assert child.now == 8000.0
    assert outcome["status"] == "executed"
    assert outcome["execution_timeout_s"] is None
    assert outcome["execution_governed_by"] == "stall"
    assert outcome["progress_observation"]["accepted_count"] == 8


def test_an_implicit_allowance_clamp_is_reported(tmp_path):
    _, item = _claimed(tmp_path)
    allowance = pool.default_stall(item, 10.0)
    assert allowance.effective == 10.0
    assert allowance.clamped is True


def test_stall_allowance_changes_action_identity_and_refuses_semantic_policy(tmp_path):
    from test_progress_keeps_a_working_action_alive import _claimed as semantic_claim

    _, item = semantic_claim(tmp_path, mode="silent", seconds=1,
                             policy={"schema": pb.PROGRESS_POLICY_SCHEMA_V1,
                                     "phases": [{"name": "run", "grace_s": 30.0}]})
    action = pool._sealed_action_request(str(item["cas_root"]), item["action_key"])
    body = {key: value for key, value in action.items() if key != "action_key"}
    body["params"][pb.STALL_ALLOWANCE_PARAM] = 60.0
    with pytest.raises(pb.ActionContractError, match="cannot combine"):
        pb.seal_action(body)
    body["params"].pop(pb.PROGRESS_PARAM)
    first = pb.seal_action(body)
    body["params"][pb.STALL_ALLOWANCE_PARAM] = 120.0
    assert pb.seal_action(body)["action_key"] != first["action_key"]


def test_an_explicit_budget_ends_default_activity_at_its_fake_clock_limit(tmp_path):
    from test_progress_keeps_a_working_action_alive import _virtual_execution

    def script(child):
        for tick in range(1, 10):
            child.stdout += "working\n"
            yield float(tick)

    with _virtual_execution(tmp_path, script, policy=None, ceiling=10.0,
                            timeout_s=2.0, heartbeat_s=1.0) as child:
        outcome = child.execute()
        assert child.stopped_at == 2.0
    assert outcome["termination_reason"] == "execution_deadline"
    assert outcome["progress_observation"]["accepted_count"] == 2


def test_d30_ends_default_activity_on_the_fake_clock(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from test_progress_keeps_a_working_action_alive import _virtual_execution, _running_until

    stopped = []
    scope = SimpleNamespace(
        wrap_argv=lambda argv, **kwargs: argv,
        terminate_owned=stopped.append)
    with _virtual_execution(tmp_path, _running_until(0.1), policy=None,
                            ceiling=10.0) as child:
        monkeypatch.setattr(child.queue, "_start_resource_scope", lambda item: scope)
        monkeypatch.setattr(child.queue, "_sample_resource_scope", lambda scope: {
            "oom_local": 1})
        outcome = child.queue.execute(child.item, containment=True,
                                      timeout_s=10.0, heartbeat_s=0.1)
        assert child.stopped_at == 0.1
    assert stopped == ["memory_limit_oom"]
    assert outcome["status"] == "failed"
    assert outcome["termination_reason"] == "memory_limit_oom"
