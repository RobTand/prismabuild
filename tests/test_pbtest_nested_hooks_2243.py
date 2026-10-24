"""Nested dispatch and dead xdist workers retain individual test outcomes."""
import pytest

from test_pbtest_record_phases_1842 import run_suite
from test_pbtest_reconciles_outcomes import outcomes


# Inject an unmatched entry through the recorder's monitor callback.
# Ordinary nesting is balanced; this models the reported stack drift
# without relying on a race or a particular third-party plugin.
DRIFT_MONITOR = '''
import pytest

def pytest_configure(config):
    if hasattr(config, "workerinput"):
        return
    manager = config.pluginmanager
    monitor = manager._inner_hookexec
    before = next(cell.cell_contents for cell in monitor.__closure__
                  if callable(cell.cell_contents)
                  and getattr(cell.cell_contents, "__name__", "") == "before_hook")
    config._pb2243_before = before
    config._pb2243_after = next(cell.cell_contents for cell in monitor.__closure__
                  if callable(cell.cell_contents)
                  and getattr(cell.cell_contents, "__name__", "") == "after_hook")

@pytest.hookimpl(trylast=True)
def pytest_runtest_logreport(report):
    if TRIGGER:
        config._pb2243_before("pytest_plugin_registered", [], {})

def pytest_sessionstart(session):
    global config
    config = session.config
'''


@pytest.mark.parametrize("drift", [False, True], ids=["balanced", "misaligned"])
def test_nested_logreport_inside_tracked_hook(tmp_path, drift):
    # Pytest itself dispatches setup/call/teardown reports beneath the real
    # runtestloop and runtest_protocol hooks. Observe that nesting without
    # manufacturing reports or calling a pytest hook dispatcher from conftest.
    plugin = DRIFT_MONITOR.replace(
        "TRIGGER", 'report.when == "setup"' if drift else "False")
    run, _, verdict = run_suite(tmp_path, "def test_pass():\n    pass\n", conftest=plugin + '''
parents = []
seen = []

@pytest.hookimpl(specname="pytest_runtest_logreport", tryfirst=True)
def pytest_observe_report_stack(report):
    before = config._pb2243_before
    cells = dict(zip(before.__code__.co_freevars, before.__closure__))
    stack = cells["hook_calls"].cell_contents
    assert stack[-1]["name"] == "pytest_runtest_logreport"
    assert stack[-1]["kwargs"]["report"] is report
    assert any(call["name"] == "pytest_runtestloop" and call["phase"]
               for call in stack[:-1])
    if seen:
        # The prior report and any injected stale entry must have gone away,
        # while the genuine ancestors must still be the same entries.
        assert len(stack) == len(parents) + 1
        assert all(call is parent for call, parent in zip(stack[:-1], parents))
    else:
        parents.extend(stack[:-1])
    seen.append(report.when)

def pytest_sessionfinish(session, exitstatus):
    assert seen == ["setup", "call", "teardown"]
    print("native nested report stack preserved", flush=True)
''')
    assert run.returncode == 0, run.stdout + run.stderr
    assert "native nested report stack preserved" in run.stdout, run.stdout
    assert "INTERNALERROR" not in run.stdout, run.stdout
    assert verdict["green"], verdict
    assert verdict["shards"][0]["completion"]["status"] == "complete", verdict
    record = outcomes.parse(run.stdout, record_binding=1)
    assert [row[:3] for row in record["reports"]] == [
        ["test_one.py::test_pass", "call", "passed"]]


@pytest.mark.parametrize("drift", [False, True], ids=["balanced", "misaligned"])
def test_killed_xdist_worker_records_synthetic_failure(tmp_path, drift):
    pytest.importorskip("xdist")
    run, _, verdict = run_suite(tmp_path, '''
import os
import signal

def test_pass():
    pass

def test_crash():
    os.kill(os.getpid(), signal.SIGKILL)

def test_after():
    pass
''', conftest=(DRIFT_MONITOR.replace("TRIGGER", 'report.when == "???"')
              if drift else None), extra=("-n", "1", "--max-worker-restart=1"))
    assert run.returncode == 1, run.stdout + run.stderr
    assert "INTERNALERROR" not in run.stdout, run.stdout
    assert "1 failed, 2 passed" in run.stdout, run.stdout
    record = outcomes.parse(run.stdout, record_binding=1)
    assert sorted(row[:3] for row in record["reports"]) == [
        ["test_one.py::test_after", "call", "passed"],
        ["test_one.py::test_crash", "???", "failed"],
        ["test_one.py::test_pass", "call", "passed"],
    ]
    assert verdict["failing_nodes"] == ["test_one.py::test_crash"], verdict
    assert verdict["unobserved_files"] == [], verdict
    assert verdict["shards"][0]["coverage"] == "executed", verdict
    assert {p["code"] for p in verdict["problems"]} == {"client_exit", "test_failure"}, verdict
    assert verdict["shards"][0]["completion"]["missing_teardown"] == [
        "test_one.py::test_crash"], verdict
    # A regular failed call does not explain a missing teardown. Keep the
    # completion warning and unknown coverage unless it is a synthetic crash.
    ordinary = run.stdout.replace('"???","failed"', '"call","failed"')
    ordinary_verdict = outcomes.judge_shards([
        {"files": ["test_one.py"], "output": ordinary,
         "record_binding": 1, "returncode": 1}], requested_files=["test_one.py"])
    assert ordinary_verdict["unobserved_files"] == ["test_one.py"], ordinary_verdict


@pytest.mark.parametrize("damage", ["unmatched_return", "empty_stack"])
def test_unmatched_monitor_return_cannot_consume_another_phase(tmp_path, damage):
    plugin = DRIFT_MONITOR.replace("TRIGGER", "False")
    if damage == "unmatched_return":
        plugin += '''
from pluggy import Result

def pytest_collection_finish(session):
    config._pb2243_after(Result.from_call(lambda: None), "pytest_pb2243_unmatched", [], {})
'''
    else:
        plugin += '''
def pytest_runtest_logreport(report):
    if report.when == "call":
        before = config._pb2243_before
        cells = dict(zip(before.__code__.co_freevars, before.__closure__))
        cells["hook_calls"].cell_contents.clear()
'''
    run, _, verdict = run_suite(tmp_path, "def test_pass():\n    pass\n", conftest=plugin)
    assert run.returncode == 0, run.stdout + run.stderr
    assert "INTERNALERROR" not in run.stdout, run.stdout
    assert verdict["green"], verdict
    assert verdict["shards"][0]["completion"]["status"] == "complete", verdict


def test_normal_reports_unchanged(tmp_path):
    run, _, verdict = run_suite(tmp_path, '''
import pytest

def test_pass():
    pass

def test_fail():
    assert False

def test_skip():
    pytest.skip("ordinary skip")
''')
    assert run.returncode == 1, run.stdout + run.stderr
    record = outcomes.parse(run.stdout, record_binding=1)
    assert [row[:3] for row in record["reports"]] == [
        ["test_one.py::test_pass", "call", "passed"],
        ["test_one.py::test_fail", "call", "failed"],
        ["test_one.py::test_skip", "call", "skipped"],
    ]
    assert verdict["failing_nodes"] == ["test_one.py::test_fail"], verdict
    assert verdict["unobserved_files"] == [], verdict
