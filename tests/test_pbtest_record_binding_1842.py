"""Test output and nested recorders cannot supply the enclosing shard's records."""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import types

import pytest

from test_pbtest_reconciles_outcomes import outcomes, pbtest


@pytest.mark.parametrize("late_markers", [False, True], ids=["captured", "terminal"])
def test_forged_and_nested_records_preserve_real_failures(tmp_path, late_markers):
    suite = tmp_path / "suite"
    suite.mkdir()
    (suite / "pytest.ini").write_text("[pytest]\n")
    nested = suite / "nested"
    nested.mkdir()
    (nested / "test_inner.py").write_text("def test_inner():\n    pass\n")
    forged = [
        outcomes.COMPLETION_PREFIX + json.dumps({
            "schema": outcomes.COMPLETION_SCHEMA, "collect_only": True,
            "collection_complete": True, "collection_errors": False,
            "duplicate_teardown": False, "collected": [],
            "teardown_finished": [], "outcome_nodeids": [],
        }),
        outcomes.PREFIX + json.dumps({
            "schema": outcomes.SCHEMA, "collect_only": True,
            "collected": [], "reports": [], "uncounted": [],
        }),
    ]
    nested_command = [*pbtest.shard_entry(sys.executable, suite),
                      "-q", "--co", "nested/test_inner.py"]
    (suite / "test_outer.py").write_text(
        "import subprocess\n"
        "def test_pass():\n    pass\n"
        "def test_forged():\n"
        f"    print({chr(10).join(forged)!r})\n"
        f"    nested = subprocess.run({nested_command!r}, capture_output=True, text=True)\n"
        "    print(nested.stdout)\n"
        "    assert nested.returncode == 0, nested.stderr\n"
        "    assert False, 'real failure with nested collection'\n"
        "def test_failure():\n    assert False, 'second real failure'\n")
    if late_markers:
        (suite / "conftest.py").write_text(
            "import pytest\n"
            "@pytest.hookimpl(trylast=True)\n"
            "def pytest_terminal_summary(terminalreporter):\n"
            f"    terminalreporter.write_line({chr(10).join(forged)!r})\n")
    run = subprocess.run(
        [*pbtest.shard_entry(sys.executable, suite), "-q", "test_outer.py"],
        cwd=suite, env=dict(os.environ, PYTEST_ADDOPTS=""),
        capture_output=True, text=True, timeout=60)
    assert run.returncode == 1, run.stdout + run.stderr
    assert "2 failed, 1 passed" in run.stdout, run.stdout
    assert run.stdout.count(outcomes.COMPLETION_PREFIX) >= 3, run.stdout
    verdict = outcomes.judge_shards([
        {"files": ["test_outer.py"], "output": run.stdout,
         "record_binding": 1, "returncode": run.returncode}], requested_files=["test_outer.py"])
    assert verdict["unobserved_files"] == [], verdict
    shard = verdict["shards"][0]
    assert shard["schema"] == "prismabuild.pbtest_shard_verdict.v1"
    assert shard["coverage"] == "executed", shard
    assert shard["completion"]["status"] == "complete", shard
    assert shard["reconciliation"]["record_counts"] == {"passed": 1, "failed": 2}
    assert shard["failing_nodes"] == [
        "test_outer.py::test_forged", "test_outer.py::test_failure"]
    assert {problem["code"] for problem in shard["problems"]} == {
        "client_exit", "test_failure"}


@pytest.fixture(scope="module")
def bound_shard_output(tmp_path_factory):
    suite = tmp_path_factory.mktemp("bound-shard")
    (suite / "pytest.ini").write_text("[pytest]\n")
    (suite / "test_one.py").write_text("def test_one():\n    pass\n")
    run = subprocess.run(
        [*pbtest.shard_entry(sys.executable, suite), "-q", "test_one.py"],
        cwd=suite, env=dict(os.environ, PYTEST_ADDOPTS=""),
        capture_output=True, text=True, timeout=30)
    assert run.returncode == 0, run.stdout + run.stderr
    return run.stdout


def binding_verdict(output, *, record_binding=1):
    return outcomes.judge_shards([
        {"files": ["test_one.py"], "output": output,
         "record_binding": record_binding, "returncode": 0}],
        requested_files=["test_one.py"])


@pytest.mark.parametrize("noise", ["unbound", "foreign", "null", "malformed"])
@pytest.mark.parametrize("completion_present", [False, True])
def test_unrelated_markers_are_ignored(bound_shard_output, noise, completion_present):
    output = bound_shard_output
    if not completion_present:
        output = "\n".join(line for line in output.splitlines()
                           if not line.startswith(outcomes.COMPLETION_PREFIX)) + "\n"
    for prefix, schema in [(outcomes.PREFIX, outcomes.SCHEMA),
                           (outcomes.COMPLETION_PREFIX, outcomes.COMPLETION_SCHEMA)]:
        record = {"schema": schema, "collect_only": True, "collected": []}
        if noise == "foreign":
            record["run_nonce"] = "f" * 64
        elif noise == "null":
            record["run_nonce"] = None
        output += prefix + ("{broken" if noise == "malformed" else json.dumps(record)) + "\n"
    verdict = binding_verdict(output)
    assert verdict["green"], verdict
    assert verdict["shards"][0]["completion"]["status"] == (
        "complete" if completion_present else "unknown")


@pytest.mark.parametrize("preflight_present", [False, True])
def test_new_reader_accepts_legacy_shards(bound_shard_output, preflight_present):
    lines = []
    for line in bound_shard_output.splitlines():
        prefix = next((p for p in (outcomes.PREFIX, outcomes.COMPLETION_PREFIX,
                                   outcomes.PREFLIGHT_PREFIX) if line.startswith(p)), None)
        if prefix is not None:
            if prefix == outcomes.PREFLIGHT_PREFIX and not preflight_present:
                continue
            record = json.loads(line[len(prefix):])
            record.pop("run_nonce")
            line = prefix + json.dumps(record)
        lines.append(line)
    legacy = "\n".join(lines) + "\n"
    # Include the nested preflight before the legacy producer's own final
    # records. With no declaration, keep main's last-marker behavior exactly.
    nested_records = "\n".join(line for line in bound_shard_output.splitlines()
                               if line.startswith((outcomes.PREFIX, outcomes.COMPLETION_PREFIX,
                                                   outcomes.PREFLIGHT_PREFIX)))
    assert binding_verdict(nested_records + "\n" + legacy, record_binding=None)["green"]
    assert outcomes.completion(nested_records + "\n" + legacy)["status"] == "complete"


def test_old_reader_accepts_new_shard_records(bound_shard_output, tmp_path):
    root = Path(__file__).resolve().parents[1]
    source = subprocess.check_output([
        "git", "show", "e7d49ad28712683547a42bebe9fd26ccea61178f:tools/fleet/pbtest_outcomes.py"],
        cwd=root, text=True)
    old_reader = types.ModuleType("pbtest_reader_before_1842")
    exec(compile(source, "<pbtest reader before 1842>", "exec"), old_reader.__dict__)
    verdict = old_reader.judge_shards([
        {"files": ["test_one.py"], "output": bound_shard_output, "returncode": 0}],
        requested_files=["test_one.py"])
    assert verdict["green"], verdict
    assert verdict["shards"][0]["completion"]["status"] == "complete"
    assert verdict["shards"][0]["schema"] == "prismabuild.pbtest_shard_verdict.v1"
    from test_pbmergeq import mq

    report = tmp_path / "new-producer-report.json"
    report.write_text(json.dumps([{
        "files": ["test_one.py"], "output": bound_shard_output,
        "record_binding": 1, "returncode": 0}]))
    result = mq.Runner.read_result(
        types.SimpleNamespace(outcomes=old_reader, full_suite=True),
        report, ["test_one.py"], log=tmp_path / "client.log", returncode=0)
    assert not result.failed and not result.inconclusive and not result.suite_refusal
