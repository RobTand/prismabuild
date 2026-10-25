"""Nested dispatch and dead xdist workers retain individual test outcomes."""
import pytest

from test_pbtest_record_phases_1842 import run_suite
from test_pbtest_reconciles_outcomes import outcomes


# Hold a real receiver-thread dispatch open while the controller dispatches.
THREAD_HOLDER = '''
import threading
import pytest

_entered = threading.Event()
_release = threading.Event()
_thread = None
_errors = []

@pytest.hookimpl(tryfirst=True)
def pytest_report_from_serializable(config, data):
    if data == "pb2243":
        _entered.set()
        assert _release.wait(10), "controller did not release receiver hook"
        return object()

def hold(config):
    global _thread
    def dispatch():
        try:
            config.hook.pytest_report_from_serializable(config=config, data="pb2243")
        except BaseException as error:
            _errors.append(error)
    _thread = threading.Thread(target=dispatch)
    _thread.start()
    assert _entered.wait(10), "receiver hook did not enter"

def release():
    _release.set()
    if _thread is not None:
        _thread.join(10)
        assert not _thread.is_alive(), "receiver hook did not return"
    assert not _errors, _errors

def pytest_unconfigure(config):
    release()
'''


def test_report_returns_while_other_thread_is_inside_a_hook(tmp_path):
    run, _, verdict = run_suite(tmp_path, "def test_pass():\n    pass\n",
                                conftest=THREAD_HOLDER + '''
def pytest_configure(config):
    pytest_runtest_logreport.config = config

@pytest.hookimpl(tryfirst=True)
def pytest_runtest_logreport(report):
    if report.when == "setup":
        hold(pytest_runtest_logreport.config)

def pytest_runtest_logfinish(nodeid, location):
    release()
''')
    assert "INTERNALERROR" not in run.stdout, run.stdout
    assert run.returncode == 0, run.stdout + run.stderr
    assert verdict["green"], verdict
    assert verdict["shards"][0]["completion"]["status"] == "complete", verdict
    record = outcomes.parse(run.stdout, record_binding=1)
    assert [row[:3] for row in record["reports"]] == [
        ["test_one.py::test_pass", "call", "passed"]]


def test_adapter_dispatch_while_other_thread_is_inside_a_hook(tmp_path):
    run, _, verdict = run_suite(tmp_path, '''
def test_keep():
    pass

def test_drop():
    pass
''', conftest=THREAD_HOLDER + '''
def pytest_collection_modifyitems(config, items):
    drop = [item for item in items if item.name == "test_drop"]
    items[:] = [item for item in items if item.name != "test_drop"]
    config.hook.pytest_deselected(items=drop)

@pytest.hookimpl(tryfirst=True)
def pytest_deselected(items):
    hold(items[0].config)

def pytest_collection_finish(session):
    release()
''')
    assert "INTERNALERROR" not in run.stdout, run.stdout
    assert run.returncode == 0, run.stdout + run.stderr
    record = outcomes.parse(run.stdout, record_binding=1)
    assert record["deselected"] == ["test_one.py::test_drop"], record
    assert verdict["green"], verdict


def test_killed_xdist_worker_records_synthetic_failure(tmp_path):
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
''', extra=("-n", "1", "--max-worker-restart=1"))
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
    plugin = '''
import pytest

def pytest_configure(config):
    monitor = config.pluginmanager._inner_hookexec
    callbacks = {cell.cell_contents.__name__: cell.cell_contents
                 for cell in monitor.__closure__ if callable(cell.cell_contents)}
    config._pb2243_before = callbacks["before_hook"]
    config._pb2243_after = callbacks["after_hook"]

def pytest_sessionstart(session):
    global config
    config = session.config
'''
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
        cells["thread_calls"].cell_contents().clear()
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
