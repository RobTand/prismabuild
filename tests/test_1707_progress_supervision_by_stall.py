"""Supervise payloads by progress: stop on a stall, not a wall clock (#1707)."""
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import core as pb, pool  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import pbstatus  # noqa: E402
import pbrun  # noqa: E402

from admitted_queue_fixture import AdmittedQueueFixture  # noqa: E402

CHATTY = '''
import sys, time
deadline = time.monotonic() + float(sys.argv[1])
while time.monotonic() < deadline:
    print("still working", flush=True)
    time.sleep(0.05)
open("result", "w").write("ok")
'''

SILENT = '''
import sys, time
time.sleep(float(sys.argv[1]))
open("result", "w").write("ok")
'''


def _claimed(tmp_path, *, source, seconds, timeout_s=None, stall_s=None):
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    (checkout / "task.py").write_text(source)
    params = {}
    if timeout_s is not None:
        params["execution_timeout_s"] = timeout_s
    if stall_s is not None:
        params["stall_allowance_s"] = stall_s
    action = pb.seal_action({
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {"definition_id": "tests/1707", "definition_version": "v1",
                 "task_class": "generation", "determinism": "deterministic",
                 "artifact_family": "generic", "artifact_kind": "generic",
                 "argv": [sys.executable, "task.py", str(seconds)],
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
    _, item = _claimed(tmp_path, source=CHATTY, seconds=1, stall_s=60.0)
    stalled = pool.default_stall(item, 7200.0)
    assert stalled.requested == 60.0
    assert stalled.effective == 60.0
    assert stalled.clamped is False
    clamped = pool.default_stall(item, 0.4)
    assert clamped.effective == 0.4
    assert clamped.clamped is True
    assert clamped.as_record()["stall_allowance_s"] == 0.4


def test_the_default_allowance_is_one_long_job_interval(tmp_path):
    _, item = _claimed(tmp_path, source=CHATTY, seconds=1)
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

def test_a_quiet_payload_stops_on_its_stall_allowance(tmp_path):
    """Silence past the allowance stops with a stall verdict."""

    queue, item = _claimed(tmp_path, source=SILENT, seconds=30, stall_s=0.5)
    outcome = queue.execute(item, timeout_s=5.0, heartbeat_s=0.05,
                            timeout_grace_s=0.2)
    assert outcome["status"] == "timeout"
    assert outcome["termination_reason"] == "no_progress"
    assert outcome["execution_governed_by"] == "stall"
    assert outcome["elapsed_s"] < 5.0
    assert outcome["stall"]["allowance_s"] == 0.5
    assert outcome["progress_observation"]["source"] == "activity"


def test_an_explicit_budget_still_stops_an_active_payload(tmp_path):
    """A sealed wall-clock budget ends even a payload showing activity."""

    queue, item = _claimed(tmp_path, source=CHATTY, seconds=30, timeout_s=0.5)
    outcome = queue.execute(item, timeout_s=5.0, heartbeat_s=0.05,
                            timeout_grace_s=0.2)
    assert outcome["status"] == "timeout"
    assert outcome["termination_reason"] == "execution_deadline"
    assert outcome["execution_governed_by"] == "deadline"
    assert outcome["execution_timeout_requested_s"] == 0.5
    assert outcome["elapsed_s"] < 5.0


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


def test_stall_s_is_refused_on_the_slurm_lane():
    with pytest.raises(ValueError, match="--stall-s requires pool transport"):
        pbrun.require_stall_scope(stall_s=60.0, transport="slurm")
    pbrun.require_stall_scope(stall_s=60.0, transport="pool")
    pbrun.require_stall_scope(stall_s=None, transport="slurm")
