"""The CLI inventory and report contracts consumed by release gates (#1900)."""
from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest

from test_pbtest_reports_whether_pytest_ran import _one_shard, pbtest
from pbtest_shard_output import ONE_PASS


@pytest.fixture
def plan_checkout(tmp_path):
    checkout = tmp_path / "checkout"
    tests = checkout / "tests"
    tests.mkdir(parents=True)
    (tests / "test_plain.py").write_text("def test_plain(): pass\n")
    (tests / "test_fenced.py").write_text(
        'import pytest\npytestmark = pytest.mark.pbtest_capability("archive")\n')
    (tests / "pbtest_capabilities.json").write_text(json.dumps({
        "schema": "prismabuild.pbtest_capabilities.v1",
        "capabilities": {"archive": {"tags": ["x86", "archive"]}},
    }))
    return checkout


def _invoke_pbtest_plan(monkeypatch, checkout, report, *options, paths=("tests",)):
    monkeypatch.setattr(sys, "argv", [
        "pbtest.py", "--checkout", str(checkout), "--plan-only",
        "--json", str(report), *options, *paths])
    return pbtest.main(), json.loads(report.read_text())


@pytest.fixture
def forbid_plan_execution(monkeypatch):
    def execution_reached(*args, **kwargs):
        pytest.fail("plan-only reached execution or checkout preparation")

    for owner, name in [
        (pbtest, "_Progress"), (pbtest, "shard_entry"),
        (pbtest, "placement"), (pbtest, "fleet_queue"),
        (pbtest, "start_shard"), (pbtest, "publish_shard"),
        (pbtest.submission, "preparation"),
        (pbtest.movement_actions, "checkout_identity"),
        (pbtest.subprocess, "Popen"),
    ]:
        monkeypatch.setattr(owner, name, execution_reached)


def test_plan_contract_without_git_python_or_submission(
        plan_checkout, tmp_path, monkeypatch, forbid_plan_execution):
    report = tmp_path / "plan.json"
    code, plan = _invoke_pbtest_plan(monkeypatch, plan_checkout, report, "--tag", "custom",
                                     "--tag", "second", "--tag", "custom")
    assert code == 0
    assert plan == {
        "schema": "prismabuild.pbtest_plan.v1",
        "files": ["tests/test_fenced.py", "tests/test_plain.py"],
        "capabilities": {"tests/test_fenced.py": ["archive"], "tests/test_plain.py": []},
        "tags": {"tests/test_fenced.py": ["archive", "custom", "second", "x86"],
                 "tests/test_plain.py": ["custom", "second"]},
        "left_out": {}, "refusals": [],
    }
    assert not Path(str(report) + ".progress.json").exists()


@pytest.mark.parametrize("options", [
    ["--python", "unused", "--tmpdir", "relative", "--basetemp", "../unused"],
    ["--env", "TMPDIR=unused", "--mem-gb", "0", "--cpus-per-shard", "0"],
    ["--shards", "0", "--max-clients", "0", "--workers-per-shard", "0",
     "--threads-per-shard", "-1"],
    ["--gpu-memory-gb", "0", "--disk-metadata", "--transport", "slurm"],
    ["--pytest-args", '["-k", "no_case_matches", "-m", "nothing"]'],
    ["--history", "/no/history", "--timeout-s", "0", "--test-timeout-s", "0",
     "--wait-s", "0", "--priority", "-10", "--profile", "unused"],
    ["--data-manifest", "/no/manifest", "--residency", "stage"],
])
def test_plan_ignores_execution_options(
        plan_checkout, tmp_path, monkeypatch, forbid_plan_execution, options):
    code, plan = _invoke_pbtest_plan(monkeypatch, plan_checkout, tmp_path / "plan.json", *options)
    assert code == 0
    assert plan["files"] == ["tests/test_fenced.py", "tests/test_plain.py"]
    assert plan["tags"]["tests/test_plain.py"] == []


def test_plan_explicit_paths_config_and_gpu_tag_match_execution_defaults(
        plan_checkout, tmp_path, monkeypatch, forbid_plan_execution):
    custom = plan_checkout / "custom.json"
    custom.write_text(json.dumps({"schema": "prismabuild.pbtest_capabilities.v1",
                                  "capabilities": {"archive": {"tags": ["custom"]}}}))
    monkeypatch.setattr(pbtest, "RUNTIME_ROOT", Path("/mnt/shared/prismabuild-fleet/repo"))
    code, plan = _invoke_pbtest_plan(
        monkeypatch, plan_checkout, tmp_path / "plan.json", "--capabilities", "custom.json",
        "--gpu", paths=("tests/test_plain.py", "tests/test_fenced.py", "tests/test_plain.py"))
    assert code == 0
    assert plan["files"] == ["tests/test_plain.py", "tests/test_fenced.py"]
    assert plan["tags"] == {"tests/test_plain.py": ["gb10"],
                            "tests/test_fenced.py": ["custom", "gb10"]}


@pytest.mark.parametrize("declaration", [
    'pytest.mark.pbtest_capability(dynamic)',
    'pytest.mark.pbtest_capability("undefined")',
    'pytest.mark.pbtest_capability(',
])
def test_plan_names_every_unclassifiable_file(
        plan_checkout, tmp_path, monkeypatch, forbid_plan_execution, declaration):
    for name in ("test_fenced.py", "test_plain.py"):
        (plan_checkout / "tests" / name).write_text("import pytest\npytestmark = " + declaration)
    code, plan = _invoke_pbtest_plan(monkeypatch, plan_checkout, tmp_path / "plan.json")
    assert code == 2
    assert len(plan["refusals"]) == 2
    assert {row["code"] for row in plan["refusals"]} == {"capability_unclassifiable"}
    assert all(any(file in row["reason"] for row in plan["refusals"])
               for file in plan["files"])


@pytest.mark.parametrize("case, expected", [
    ("missing_path", "discovery_failed"), ("empty", "population_empty"),
    ("missing_config", "capabilities_invalid"), ("invalid_config", "capabilities_invalid"),
    ("outside_config", "capabilities_invalid"),
])
def test_plan_refusal_codes_are_written_with_exit_two(
        plan_checkout, tmp_path, monkeypatch, forbid_plan_execution, case, expected):
    options = []
    paths = ("tests",)
    if case == "missing_path":
        paths = ("tests/test_plain.py", "missing")
    elif case == "empty":
        (plan_checkout / "empty").mkdir()
        paths = ("empty",)
    elif case == "missing_config":
        options = ["--capabilities", "missing.json"]
    elif case == "invalid_config":
        (plan_checkout / "tests/pbtest_capabilities.json").write_text("{}")
    else:
        outside = tmp_path / "outside.json"
        outside.write_text("{}")
        options = ["--capabilities", str(outside)]
    code, plan = _invoke_pbtest_plan(
        monkeypatch, plan_checkout, tmp_path / "plan.json", *options, paths=paths)
    assert code == 2
    assert len(plan["refusals"]) == 1
    assert set(plan["refusals"][0]) == {"code", "reason"}
    assert plan["refusals"][0]["code"] == expected
    assert plan["refusals"][0]["reason"]
    if case == "invalid_config":
        assert "tests/pbtest_capabilities.json" in plan["refusals"][0]["reason"]


def test_plan_and_execution_call_the_same_population_owner(tmp_path, monkeypatch):
    calls = []
    original = pbtest._pbtest_population

    def observe_population(*args):
        result = original(*args)
        calls.append(json.loads(json.dumps(result[0])))
        return result

    monkeypatch.setattr(pbtest, "_pbtest_population", observe_population)
    _, row = _one_shard(tmp_path, monkeypatch, ONE_PASS, 0, with_exit=True)
    checkout = tmp_path / "run/checkout"
    code, plan = _invoke_pbtest_plan(monkeypatch, checkout, tmp_path / "plan.json")
    assert code == 0
    assert len(calls) == 2
    assert calls[0] == calls[1]
    assert {**calls[0], "tags": plan["tags"]} == plan
    assert row["files"] == plan["files"]


@pytest.mark.parametrize("output, returncode, expected_exit", [
    (ONE_PASS, 0, 0), ("", 1, 1), ("", 0, 1),
])
def test_consumer_report_remains_a_list_with_versioned_rows(
        tmp_path, monkeypatch, output, returncode, expected_exit):
    code, row = _one_shard(tmp_path, monkeypatch, output, returncode, with_exit=True)
    assert code == expected_exit
    assert row["schema"] == "prismabuild.pbtest_report.v1"
    assert {"shard", "files", "action_key", "ran", "timed_out", "returncode",
            "summary", "output", "verdict", "receipt_path"} <= row.keys()
    assert row["verdict"]["schema"] == "prismabuild.pbtest_shard_verdict.v1"
    assert isinstance(row["shard"], int)
    assert isinstance(row["files"], list)
    assert type(row["ran"]) is type(row["timed_out"]) is bool
    assert row["returncode"] == returncode
    assert isinstance(row["summary"], str) and isinstance(row["output"], str)
    assert row["action_key"] is None or isinstance(row["action_key"], str)
    assert row["receipt_path"] is None or isinstance(row["receipt_path"], str)


@pytest.mark.parametrize("options, message", [
    (["--plan-only"], "--json"), ([], "--python"),
])
def test_plan_and_execution_usage_errors_exit_two(tmp_path, monkeypatch, capsys, options, message):
    monkeypatch.setattr(sys, "argv", ["pbtest.py", "--checkout", str(tmp_path), *options])
    with pytest.raises(SystemExit) as error:
        pbtest.main()
    assert error.value.code == 2
    assert message in capsys.readouterr().err


def test_plan_output_failure_exits_two_without_submission(
        plan_checkout, tmp_path, monkeypatch, capsys, forbid_plan_execution):
    blocker = tmp_path / "blocker"
    blocker.write_text("a file cannot be an output directory")
    monkeypatch.setattr(sys, "argv", [
        "pbtest.py", "--checkout", str(plan_checkout), "--plan-only",
        "--json", str(blocker / "plan.json")])
    assert pbtest.main() == 2
    assert "cannot publish requested plan" in capsys.readouterr().err
    assert blocker.read_text() == "a file cannot be an output directory"
