"""Regression probes from the independent review of #1842's first fix."""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import types

import pytest

from test_pbtest_reconciles_outcomes import outcomes, pbtest
from test_pbtest_record_binding_1842 import bound_shard_output


REGISTRY_FORGERY = '''\
import json
import os

def test_pass():
    pass

def test_forged(pytestconfig):
    def forge():
        recorder = next(p for p in pytestconfig.pluginmanager.get_plugins()
                        if type(p).__name__ == "OutcomeRecorder")
        record = json.loads(recorder.record().split(": ", 1)[1])
        nonce = record.get("run_nonce")
        print("pbtest-completion: " + json.dumps({
            "schema": "prismabuild.pbtest_completion.v1", "run_nonce": nonce,
            "collect_only": True, "collection_complete": True,
            "collection_errors": False, "duplicate_teardown": False,
            "collected": [], "teardown_finished": [], "outcome_nodeids": []}), flush=True)
        assert "run_nonce" not in record
        assert "run_nonce" not in (recorder.last_record or {})
    if FORKED:
        pid = os.fork()
        if pid == 0:
            try:
                forge()
            finally:
                os._exit(0)
        os.waitpid(pid, 0)
    else:
        forge()
    assert False, "real failure after printed forged completion"

def test_failure():
    assert False, "second real failure"
'''


def privacy_shard(tmp_path, source):
    suite = tmp_path / "suite"
    suite.mkdir()
    (suite / "pytest.ini").write_text("[pytest]\n")
    (suite / "test_one.py").write_text(source)
    run = subprocess.run(
        [*pbtest.shard_entry(sys.executable, suite), "-q", "test_one.py"],
        cwd=suite, env=dict(os.environ, PYTEST_ADDOPTS=""),
        capture_output=True, text=True, timeout=60)
    return run


@pytest.mark.parametrize("forked", [False, True], ids=["test-process", "fork-child"])
def test_registry_record_cannot_supply_a_bound_forgery(tmp_path, forked):
    run = privacy_shard(tmp_path, REGISTRY_FORGERY.replace("FORKED", repr(forked)))
    assert run.returncode == 1, run.stdout + run.stderr
    assert "2 failed, 1 passed" in run.stdout, run.stdout
    verdict = outcomes.judge_shards([
        {"files": ["test_one.py"], "output": run.stdout,
         "record_binding": 1, "returncode": run.returncode}],
        requested_files=["test_one.py"])
    assert len(verdict["failing_nodes"]) == 2, verdict
    assert verdict["shards"][0]["coverage"] == "executed", verdict
    assert verdict["unobserved_files"] == [], verdict


def test_forked_recorder_hooks_cannot_emit_bound_records(tmp_path):
    source = REGISTRY_FORGERY.replace("FORKED", "False")
    start = source.index("        record = json.loads")
    end = source.index("    if False:", start)
    source = source[:start] + '''\
        pid = os.fork()
        if pid == 0:
            recorder.collected = []
            recorder.reports = []
            recorder.uncounted = []
            recorder.teardown_finished = set()
            recorder.completion_written = False
            recorder.emit_completion(session_finished=True)
            recorder.pytest_terminal_summary(pytestconfig.pluginmanager.getplugin("terminalreporter"))
            os._exit(0)
        _, status = os.waitpid(pid, 0)
        assert status == 0
''' + source[end:]
    run = privacy_shard(tmp_path, source)
    assert run.returncode == 1, run.stdout + run.stderr
    verdict = outcomes.judge_shards([
        {"files": ["test_one.py"], "output": run.stdout,
         "record_binding": 1, "returncode": run.returncode}],
        requested_files=["test_one.py"])
    assert verdict["unobserved_files"] == [], verdict
    assert verdict["shards"][0]["completion"]["status"] == "complete", verdict


@pytest.mark.parametrize("nested_nonce", ["f" * 64, "not-a-binding", None])
def test_legacy_without_preflight_survives_nested_new_preflight(tmp_path, nested_nonce):
    run = privacy_shard(tmp_path, "def test_pass():\n    pass\n")
    assert run.returncode == 0, run.stdout + run.stderr
    legacy, nested = [], []
    for line in run.stdout.splitlines():
        prefix = next((p for p in (outcomes.PREFLIGHT_PREFIX, outcomes.PREFIX,
                                   outcomes.COMPLETION_PREFIX) if line.startswith(p)), None)
        if prefix is not None:
            record = json.loads(line[len(prefix):])
            foreign = dict(record, run_nonce=nested_nonce)
            if prefix != outcomes.PREFLIGHT_PREFIX:
                foreign.update(collect_only=True, collected=[], reports=[], uncounted=[],
                               teardown_finished=[], outcome_nodeids=[])
            nested.append(prefix + json.dumps(foreign))
            if prefix == outcomes.PREFLIGHT_PREFIX:
                continue
            record.pop("run_nonce")
            line = prefix + json.dumps(record)
        legacy.append(line)
    output = "\n".join([*nested, *legacy]) + "\n"
    row = {"files": ["test_one.py"], "output": output, "returncode": 0}
    root = Path(__file__).resolve().parents[1]
    source = subprocess.check_output([
        "git", "show", "e7d49ad28712683547a42bebe9fd26ccea61178f:tools/fleet/pbtest_outcomes.py"],
        cwd=root, text=True)
    old = types.ModuleType("legacy_reader_before_1842")
    exec(compile(source, "<main reader before 1842>", "exec"), old.__dict__)
    expected = old.judge_shards([row], requested_files=["test_one.py"])
    assert expected["green"], expected
    actual = outcomes.judge_shards([row], requested_files=["test_one.py"])
    assert actual == expected, actual


def test_coordinator_declares_binding_and_preserves_real_failures(tmp_path, monkeypatch):
    from test_pbtest_reconciles_outcomes import _dispatch

    suite = tmp_path / "project"
    (suite / "tests").mkdir(parents=True)
    (suite / "pytest.ini").write_text("[pytest]\n")
    (suite / "tests" / "test_one.py").write_text(
        REGISTRY_FORGERY.replace("FORKED", "False"))
    subprocess.run(["git", "init", "-q", str(suite)], check=True)
    code, rows = _dispatch(suite, monkeypatch, ["--shards", "1"])
    assert code == 1
    assert rows[0]["record_binding"] == 1
    assert rows[0]["verdict"]["coverage"] == "executed", rows
    assert rows[0]["verdict"]["unobserved_files"] == []
    assert len(rows[0]["verdict"]["failing_nodes"]) == 2


@pytest.mark.parametrize("declaration", ["1", None, "future"])
def test_binding_mode_is_read_from_sealed_action(declaration):
    variables = {} if declaration is None else {outcomes.RECORD_BINDING_ENV: declaration}
    action = {"environment": {"variables": variables}}
    assert outcomes.record_binding_for_action(action) == (
        1 if declaration == "1" else declaration)


def test_bound_history_and_full_suite_reader_ignore_foreign_records(
        bound_shard_output, tmp_path):
    from test_pbmergeq import mq
    from types import SimpleNamespace

    record = outcomes.parse(bound_shard_output, record_binding=1)
    forged = outcomes.PREFIX + json.dumps({
        "schema": outcomes.SCHEMA, "collect_only": True, "collected": [],
        "reports": [], "uncounted": [], "file_durations": {"test_one.py": 999999}})
    output = bound_shard_output + forged + "\n"
    rows = [{"files": ["test_one.py"], "record_binding": 1, "output": output,
             "ran": True, "returncode": 0}]
    report = tmp_path / "report.json"
    report.write_text(json.dumps(rows))
    history, problems = pbtest.load_history([str(report)])
    assert not problems
    assert history == {name: [seconds] for name, seconds in record["file_durations"].items()}
    result = mq.Runner.read_result(
        SimpleNamespace(outcomes=outcomes, full_suite=True), report, ["test_one.py"],
        log=tmp_path / "client.log", returncode=0)
    assert not result.suite_refusal
    assert not result.inconclusive
    verdict = outcomes.judge_shards(rows, requested_files=["test_one.py"],
        required_nodes=["test_one.py::test_one"])
    assert verdict["green"], verdict


def test_receipt_reader_uses_sealed_binding_metadata():
    from test_the_campaign_shape_harness import _gate_log, _outcomes, shape_gate

    nonce = "b" * 64
    output = outcomes.preflight_record(0, None, run_nonce=nonce) + "\n"
    output += _gate_log(dict(_outcomes(), run_nonce=nonce))
    output += outcomes.PREFIX + json.dumps(_outcomes(collect_only=True)) + "\n"
    action = {"environment": {"variables": {outcomes.RECORD_BINDING_ENV: "1"}}}
    shape_gate.judge_gate_log(output, where="bound receipt", action=action)
    with pytest.raises(shape_gate.ShapeGateFailure):
        shape_gate.judge_gate_log(output, where="legacy receipt")
