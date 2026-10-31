"""Bound causes travel from the test process through real shard output (#1919)."""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from test_pbtest_reconciles_outcomes import pbtest

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("workers", [0, 2], ids=["single", "xdist"])
@pytest.mark.parametrize("phase", ["setup", "call", "teardown"])
@pytest.mark.parametrize("budgeted", [False, True], ids=["full-bound", "shard-budget"])
def test_bound_cause_survives_worker_reports_and_shard_verdict(
        tmp_path, workers, phase, budgeted):
    checkout = tmp_path / "suite"
    checkout.mkdir()
    (checkout / "pytest.ini").write_text("[pytest]\n")
    # The budget includes the plugin's 30-second reporting margin. Pin the
    # plugin's start, rather than waiting for a real shard to run out of time.
    (checkout / "conftest.py").write_text('''
import signal
from types import SimpleNamespace
import pytest
import prismabuild.pytest_test_bound as bound
pytest_plugins = ["prismabuild.pytest_test_bound"]

@pytest.hookimpl(trylast=True)
def pytest_configure(config):
    bound.time = SimpleNamespace(monotonic=lambda: 100.0)
    bound.signal.setitimer = lambda *_args: None
    config._prismabuild_test_bound = 120.0
    config._prismabuild_shard_budget = 31.0 if BUDGETED else 0.0
    config._prismabuild_session_start = 95.0

@pytest.fixture
def fixture():
    if PHASE == "setup":
        signal.getsignal(signal.SIGALRM)(signal.SIGALRM, None)
    yield
    if PHASE == "teardown":
        signal.getsignal(signal.SIGALRM)(signal.SIGALRM, None)
'''.replace("BUDGETED", repr(budgeted)).replace("PHASE", repr(phase)))
    (checkout / "test_bound.py").write_text('''
import signal

def test_bound(fixture):
    if PHASE == "call":
        signal.getsignal(signal.SIGALRM)(signal.SIGALRM, None)

def test_assertion():
    assert False, "The bound was tightened to the time this test had left in its shard"
'''.replace("PHASE", repr(phase)))
    environment = dict(os.environ, PYTHONPATH=str(ROOT / "src"))
    environment.pop("PRISMABUILD_TEST_TIMEOUT_S", None)
    environment.pop("PRISMABUILD_SHARD_BUDGET_S", None)
    distribute = ["-n", str(workers)] if workers else []
    done = subprocess.run(
        [*pbtest.shard_entry(sys.executable, checkout), "-q", *distribute, "test_bound.py"],
        cwd=checkout, env=environment, capture_output=True, text=True, timeout=60)
    output = done.stdout + done.stderr
    assert done.returncode == 1, output
    owner = pbtest.pbtest_outcomes
    record = owner.parse(output)
    expected = [{"nodeid": "test_bound.py::test_bound", "when": phase,
                 "kind": "shard_budget_exhausted" if budgeted else "per_test_bound"}]
    assert record["bound_failures"] == expected
    assert all(len(row) == 5 for row in record["reports"])
    verdict = owner.judge_shards(
        [{"files": ["test_bound.py"], "output": output, "returncode": done.returncode}],
        requested_files=["test_bound.py"])["shards"][0]
    assert verdict["schema"] == "prismabuild.pbtest_shard_verdict.v1"
    assert verdict["bound_failures"] == expected
    assert sorted(verdict["failing_nodes"]) == [
        "test_bound.py::test_assertion", "test_bound.py::test_bound"]
    assert not verdict["green"] and not verdict["unobserved_files"]
    if budgeted:
        assert record["first_tightened_test_budget_remaining_s"] == 26.0
        assert verdict["first_tightened_test_budget_remaining_s"] == record[
            "first_tightened_test_budget_remaining_s"]
    else:
        assert "first_tightened_test_budget_remaining_s" not in record
        assert "first_tightened_test_budget_remaining_s" not in verdict


def _bound_observation(**extra):
    owner = pbtest.pbtest_outcomes
    record = {"schema": owner.SCHEMA, "collected": ["test_bound.py::test_bound"],
              "reports": [["test_bound.py::test_bound", "call", "failed", None, None]],
              **extra}
    return {"files": ["test_bound.py"], "returncode": 1,
            "output": owner.PREFIX + json.dumps(record) + "\n1 failed in 1.00s\n"}


def test_legacy_bound_cause_is_unknown_without_parsing_failure_text():
    row = _bound_observation()
    row["output"] += "The bound was tightened to the time this test had left in its shard\n"
    verdict = pbtest.pbtest_outcomes.judge_shards(
        [row], requested_files=row["files"])["shards"][0]
    assert verdict["bound_failures"] is None
    assert verdict["failing_nodes"] == ["test_bound.py::test_bound"]
    assert not verdict["unobserved_files"]


@pytest.mark.parametrize("metadata", [
    {"bound_failures": None},
    {"bound_failures": [{"nodeid": "test_bound.py::test_bound", "when": "call", "kind": "unknown"}]},
    {"bound_failures": [{"nodeid": "foreign", "when": "call", "kind": "shard_budget_exhausted"}]},
    {"bound_failures": [{"nodeid": "test_bound.py::test_bound", "when": "setup", "kind": "per_test_bound"}]},
    {"bound_failures": [{"nodeid": "test_bound.py::test_bound", "when": [], "kind": "per_test_bound"}]},
    {"first_tightened_test_budget_remaining_s": True},
    {"first_tightened_test_budget_remaining_s": float("nan")},
    {"first_tightened_test_budget_remaining_s": float("inf")},
])
def test_invalid_bound_metadata_leaves_coverage_unknown(metadata):
    row = _bound_observation(**metadata)
    verdict = pbtest.pbtest_outcomes.judge_shards(
        [row], requested_files=row["files"])["shards"][0]
    assert verdict["bound_failures"] is None
    assert verdict["unobserved_files"] == row["files"]
    assert any(p["code"] == "outcomes_invalid" for p in verdict["problems"])


def test_additive_bound_metadata_fields_and_overspent_budget_remain_readable():
    failures = [{"nodeid": "test_bound.py::test_bound", "when": "call",
                 "kind": "shard_budget_exhausted", "future_diagnostic": "ignored"}]
    row = _bound_observation(bound_failures=failures,
                             first_tightened_test_budget_remaining_s=-2.0)
    verdict = pbtest.pbtest_outcomes.judge_shards(
        [row], requested_files=row["files"])["shards"][0]
    assert verdict["bound_failures"] == failures
    assert verdict["first_tightened_test_budget_remaining_s"] == -2.0
    assert not verdict["unobserved_files"]


def test_later_phase_tightening_retains_budget_at_test_start(monkeypatch):
    from types import SimpleNamespace
    from prismabuild import pytest_test_bound as bound

    config = SimpleNamespace(_prismabuild_test_bound=3.0,
                             _prismabuild_shard_budget=100.0,
                             _prismabuild_session_start=0.0,
                             pluginmanager=SimpleNamespace(get_plugin=lambda name: None))
    item = SimpleNamespace(config=config, nodeid="test_bound.py::test_bound")
    clock = iter([60.0, 68.0])
    with monkeypatch.context() as patch:
        patch.setattr(bound.time, "monotonic", lambda: next(clock))
        patch.setattr(bound.signal, "signal", lambda *args: None)
        patch.setattr(bound.signal, "setitimer", lambda *args: None)
        setup = bound._bounded(item, "setup")
        assert not setup[0].budgeted
        bound._disarm(setup)
        call = bound._bounded(item, "call")
        assert call[0].budgeted
        bound._disarm(call)
    assert config._prismabuild_first_tightened_test == {
        "started_monotonic": 60.0, "budget_remaining_s": 40.0}
