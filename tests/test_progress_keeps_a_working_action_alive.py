"""An action that keeps committing work is not killed for taking a long time.

Two GLM-5.3 pricing rows were terminated at a 14,400 s limit while publishing
measured anchors, with 5,952 and 5,920 of them durably journalled at the
moment they were killed (#480).  Nothing about either run was stuck; the only
thing that had happened was time.  These tests fix both halves of the answer:
a run that advances outlives the limit that killed those, and a run that only
*looks* busy still ends.
"""
import json
import os
import sys
from pathlib import Path

import pytest
from prismabuild import core as pb, pool


#: The action side of the contract, written without importing PrismaBuild on
#: purpose: what the worker enforces is the record on disk, so the fixture
#: writes that record rather than calling the helper that produces it.
#: ``report_action_progress`` is tested separately, against the same bytes.
REPORTER = '''
import json, os, sys, time
path = os.environ.get("PRISMABUILD_ACTION_PROGRESS_PATH")
token = os.environ.get("PRISMABUILD_ACTION_PROGRESS_TOKEN")
mode = sys.argv[1]
units = 0
deadline = time.monotonic() + float(sys.argv[2])
while time.monotonic() < deadline:
    time.sleep(0.05)
    if mode == "silent":
        continue
    if mode == "chatty":
        print("still working", flush=True)
        continue
    units += 1
    record = {"schema": "prismabuild.action_progress.v1",
              "token": token, "phase": "run",
              "units_completed": 1 if mode == "replay" else units,
              "unit": "widgets", "reported_unix": time.time()}
    if mode == "foreign":
        record["token"] = "0" * 32
    if mode == "undeclared":
        record["phase"] = "somewhere-else"
    if mode == "regressing":
        record["units_completed"] = 100 - units
    if mode == "torn":
        record = "{not json"
    tmp = path + ".tmp"
    with open(tmp, "w") as handle:
        handle.write(record if isinstance(record, str) else json.dumps(record))
    os.replace(tmp, path)
open("result", "w").write("ok")
'''

PHASES = ("startup", "run", "publish")


def _policy(*graces):
    return {"schema": pb.PROGRESS_POLICY_SCHEMA_V1,
            "phases": [{"name": name, "grace_s": grace}
                       for name, grace in zip(PHASES, graces)]}


def _claimed(tmp_path, *, mode, seconds, policy, timeout_s=None):
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    (checkout / "task.py").write_text(REPORTER)
    params = {}
    if policy is not None:
        params[pb.PROGRESS_PARAM] = policy
    if timeout_s is not None:
        params["execution_timeout_s"] = timeout_s
    action = pb.seal_action({
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {"definition_id": "tests/progress", "definition_version": "v1",
                 "task_class": "generation", "determinism": "deterministic",
                 "artifact_family": "generic", "artifact_kind": "generic",
                 "argv": [sys.executable, "task.py", mode, str(seconds)],
                 "working_directory": ".", "result_path": "result"},
        "inputs": [], "code_closure": pb.build_code_closure(checkout, ["task.py"]),
        "params": params,
        "environment": {"variables": {}, "toolchain": {}},
        "execution_scope": {"portability": "portable", "platform_key": None,
                            "host_class": None},
    })
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    cas.publish_action_request(action)
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.publish(action_key=action["action_key"], cas_root=cas.root,
                  checkout_root=checkout,
                  worker_script=Path(__file__).resolve().parents[1]
                  / "tools" / "prismabuild_worker.py")
    return queue, queue.claim()


def _run(tmp_path, **kwargs):
    ceiling = kwargs.pop("ceiling", 0.4)
    queue, item = _claimed(tmp_path, **kwargs)
    return queue.execute(item, timeout_s=ceiling, heartbeat_s=0.05,
                         timeout_grace_s=0.2)


# -- the regression itself ------------------------------------------------

def test_a_progressing_action_outlives_the_ceiling_that_killed_the_glm_rows(tmp_path):
    """The whole issue in one assertion: advancing beats elapsed time."""

    outcome = _run(tmp_path, mode="report", seconds=1.5, ceiling=0.4,
                   policy=_policy(2.0, 2.0, 2.0))
    assert outcome["status"] == "executed", outcome.get("stderr")
    # It ran nearly four times the limit a box without the contract enforces.
    assert outcome["elapsed_s"] > 1.0
    assert outcome["execution_governed_by"] == "progress"
    # And the ceiling is still on the record, aimed at the quiet instead.
    assert outcome["worker_timeout_ceiling_s"] == 0.4
    assert outcome["execution_timeout_ceiling_s"] is None
    assert outcome["progress_stall_ceiling_s"] == 0.4
    progress = outcome["progress_observation"]
    assert progress["accepted_count"] > 1
    assert progress["last_accepted"]["units_completed"] > 1
    assert progress["last_accepted"]["unit"] == "widgets"


def test_the_same_action_without_the_contract_is_killed_by_the_ceiling(tmp_path):
    """The control: nothing about the *work* is what makes the difference."""

    outcome = _run(tmp_path, mode="report", seconds=1.5, ceiling=0.4, policy=None)
    assert outcome["status"] == "timeout"
    assert outcome["termination_reason"] == "execution_deadline"
    assert outcome["execution_governed_by"] == "deadline"
    assert "progress_observation" not in outcome


# -- what must still end ---------------------------------------------------

@pytest.mark.parametrize("mode,rejection", [
    ("silent", None),
    ("chatty", None),
    ("replay", "replayed"),
    ("regressing", "replayed"),
    ("foreign", "foreign token"),
    ("undeclared", "undeclared phase"),
    ("torn", "unparsable"),
])
def test_looking_busy_is_not_progress(tmp_path, mode, rejection):
    """Bytes, heartbeats, replayed counters and foreign records are not work."""

    outcome = _run(tmp_path, mode=mode, seconds=30, ceiling=5.0,
                   policy=_policy(0.4, 0.4, 0.4))
    assert outcome["status"] == "timeout"
    assert outcome["termination_reason"] == "no_progress"
    assert outcome["returncode"] is None
    # Bounded by the declared allowance, not by the 30 s the action wanted.
    assert outcome["elapsed_s"] < 5.0
    progress = outcome["progress_observation"]
    assert progress["last_accepted"] is None
    assert progress["accepted_count"] == 0
    if rejection is None:
        assert progress["rejected_count"] == 0
    else:
        assert progress["rejected_count"] > 0
        assert progress["last_rejection"] == rejection


def test_a_declaring_action_that_never_reports_ends_within_the_stated_bound(tmp_path):
    """Point 5: missing progress has a documented, computable bounded outcome."""

    outcome = _run(tmp_path, mode="silent", seconds=30, ceiling=5.0,
                   policy=_policy(0.3, 0.3, 0.3))
    assert outcome["status"] == "timeout"
    # The first phase's allowance governs from the launch; the sum is the
    # bound only for an action that walks through its phases.
    assert outcome["progress_no_progress_bound_s"] == pytest.approx(0.9)
    assert outcome["elapsed_s"] < 5.0


# -- precedence ------------------------------------------------------------

def test_an_explicitly_requested_deadline_still_ends_a_progressing_action(tmp_path):
    """Point 1: the contract must not silently override a submitted budget."""

    outcome = _run(tmp_path, mode="report", seconds=30, ceiling=None,
                   timeout_s=0.5, policy=_policy(60, 60, 60))
    assert outcome["status"] == "timeout"
    assert outcome["termination_reason"] == "execution_deadline"
    assert outcome["execution_timeout_requested_s"] == 0.5
    assert outcome["elapsed_s"] < 5.0
    # It really was advancing; the deadline ended it anyway.
    assert outcome["progress_observation"]["accepted_count"] > 0


def test_a_withdrawal_beats_a_progressing_action(tmp_path):
    """Point 5: progress does not defeat cancellation."""

    queue, item = _claimed(tmp_path, mode="report", seconds=30,
                           policy=_policy(60, 60, 60))
    queue.withdraw(item["action_key"], reason="operator", signal_child=False)
    outcome = queue.execute(item, timeout_s=None, heartbeat_s=0.05,
                            timeout_grace_s=0.2)
    assert outcome["status"] == "withdrawn"


# -- the clamp -------------------------------------------------------------

def test_the_worker_ceiling_clamps_the_allowance_and_says_so(tmp_path):
    """Point 6, the honest half: a box may shorten the quiet, never in silence."""

    outcome = _run(tmp_path, mode="silent", seconds=30, ceiling=0.3,
                   policy=_policy(600, 600, 600))
    assert outcome["status"] == "timeout"
    assert outcome["termination_reason"] == "no_progress"
    assert outcome["progress_stall_clamped"] is True
    first = outcome["progress_phases"][0]
    assert first["grace_requested_s"] == 600
    assert first["grace_ceiling_s"] == 0.3
    assert first["grace_s"] == 0.3
    assert first["grace_clamped"] is True
    assert outcome["progress_no_progress_bound_s"] == pytest.approx(0.9)


# -- the channel -----------------------------------------------------------

def test_the_action_is_told_where_to_report_only_when_it_declared(tmp_path):
    """The env forwarding is the whole channel; nothing else reaches the action."""

    sealed = {"schema": pb.ACTION_SCHEMA_V2,
              "task": {"definition_id": "t", "definition_version": "v1",
                       "task_class": "generation", "determinism": "deterministic",
                       "artifact_family": "generic", "artifact_kind": "generic",
                       "argv": ["/bin/true"], "working_directory": ".",
                       "result_path": "result"},
              "inputs": [], "code_closure": [],
              "params": {}, "environment": {"variables": {}, "toolchain": {}},
              "execution_scope": {"portability": "portable", "platform_key": None,
                                  "host_class": None}}
    os.environ[pb.ACTION_PROGRESS_PATH_ENV] = "/queue/k.progress"
    os.environ[pb.ACTION_PROGRESS_TOKEN_ENV] = "deadbeef"
    try:
        assert pb._progress_environment(sealed, {}) == {}
        declaring = {**sealed, "params": {pb.PROGRESS_PARAM: _policy(1)}}
        assert pb._progress_environment(declaring, {}) == {
            pb.ACTION_PROGRESS_PATH_ENV: "/queue/k.progress",
            pb.ACTION_PROGRESS_TOKEN_ENV: "deadbeef"}
        with pytest.raises(pb.ActionContractError, match="seals"):
            pb._progress_environment(
                declaring, {pb.ACTION_PROGRESS_PATH_ENV: "/elsewhere"})
    finally:
        del os.environ[pb.ACTION_PROGRESS_PATH_ENV]
        del os.environ[pb.ACTION_PROGRESS_TOKEN_ENV]


def test_the_reporter_writes_what_the_worker_accepts(tmp_path):
    """Both sides of the wire, checked against each other rather than a fixture."""

    path = tmp_path / "k.progress"
    os.environ[pb.ACTION_PROGRESS_PATH_ENV] = str(path)
    os.environ[pb.ACTION_PROGRESS_TOKEN_ENV] = "tok"
    try:
        assert pb.report_action_progress("run", 7, unit="anchors") is True
    finally:
        del os.environ[pb.ACTION_PROGRESS_PATH_ENV]
        del os.environ[pb.ACTION_PROGRESS_TOKEN_ENV]
    assert pb.report_action_progress("run", 8) is False  # no channel, no-op
    declared = pb.validate_progress_policy(_policy(60, 60, 60))
    watch = pool.ProgressWatch(path, "tok", pool.ProgressPolicy(
        tuple(pool.ProgressPhase(p["name"], p["grace_s"], None)
              for p in declared["phases"]), None), started=0.0)
    assert watch.sample(now=1.0) is True
    assert watch.last_accepted["units_completed"] == 7
    assert watch.last_accepted["phase"] == "run"
    # The same record twice is not twice the work.
    assert watch.sample(now=2.0) is False
    assert watch.last_rejection == "replayed"


def test_a_wall_clock_jump_cannot_extend_or_end_a_run(tmp_path):
    """Point: the watchdog times with the monotonic clock, never the record's."""

    path = tmp_path / "k.progress"
    declared = pb.validate_progress_policy(_policy(60, 60, 60))
    watch = pool.ProgressWatch(path, "tok", pool.ProgressPolicy(
        tuple(pool.ProgressPhase(p["name"], p["grace_s"], None)
              for p in declared["phases"]), None), started=100.0)
    path.write_text(json.dumps({
        "schema": pb.PROGRESS_RECORD_SCHEMA_V1, "token": "tok", "phase": "run",
        "units_completed": 3, "reported_unix": 0}))          # 1970, and irrelevant
    assert watch.sample(now=150.0) is True
    assert watch.stall_deadline() == 210.0


def test_entering_a_later_phase_rearms_the_allowance_once(tmp_path):
    """Point 3: finalization is quiet on purpose, and is declared, not guessed."""

    path = tmp_path / "k.progress"
    declared = pb.validate_progress_policy(_policy(10, 20, 30))
    watch = pool.ProgressWatch(path, "tok", pool.ProgressPolicy(
        tuple(pool.ProgressPhase(p["name"], p["grace_s"], None)
              for p in declared["phases"]), None), started=0.0)
    assert watch.stall_deadline() == 10.0                    # startup's
    for phase, units, now, deadline in (("run", 1, 5.0, 25.0),
                                        ("publish", 1, 8.0, 38.0)):
        path.write_text(json.dumps({
            "schema": pb.PROGRESS_RECORD_SCHEMA_V1, "token": "tok",
            "phase": phase, "units_completed": units}))
        assert watch.sample(now=now) is True
        assert watch.stall_deadline() == deadline
    # Naming an earlier phase again buys neither advancement nor its allowance.
    path.write_text(json.dumps({
        "schema": pb.PROGRESS_RECORD_SCHEMA_V1, "token": "tok",
        "phase": "startup", "units_completed": 1}))
    assert watch.sample(now=9.0) is False
    assert watch.stall_deadline() == 38.0


# -- submission ------------------------------------------------------------

@pytest.mark.parametrize("bad,message", [
    (["run"], "NAME=SECONDS"),
    (["run=soon"], "non-numeric"),
    (["run=0"], "positive finite"),
    (["run=1", "run=2"], "repeats"),
])
def test_an_unusable_phase_declaration_is_refused_at_submit(bad, message):
    import pbrun
    with pytest.raises(SystemExit, match=message):
        pbrun.parse_progress_phases(bad)


def test_phase_order_is_the_order_it_was_declared():
    import pbrun
    policy = pbrun.parse_progress_phases(["startup=1800", "encode=900"])
    assert [phase["name"] for phase in policy["phases"]] == ["startup", "encode"]
    assert policy["schema"] == pb.PROGRESS_POLICY_SCHEMA_V1
