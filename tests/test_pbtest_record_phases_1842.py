"""Independent round-three probes; run only in a PB-admitted action."""
import json
import os
import subprocess
import sys

import pytest

from test_pbtest_reconciles_outcomes import outcomes, pbtest


def run_suite(tmp_path, source, conftest=None, extra=()):
    suite = tmp_path / "suite"
    suite.mkdir()
    (suite / "pytest.ini").write_text("[pytest]\n")
    (suite / "test_one.py").write_text(source)
    if conftest is not None:
        (suite / "conftest.py").write_text(conftest)
    run = subprocess.run(
        [*pbtest.shard_entry(sys.executable, suite), "-q", *extra, "test_one.py"],
        cwd=suite, env=dict(os.environ, PYTEST_ADDOPTS=""),
        capture_output=True, text=True, timeout=60)
    row = {"files": ["test_one.py"], "output": run.stdout,
           "record_binding": 1, "returncode": run.returncode}
    verdict = outcomes.judge_shards([row], requested_files=["test_one.py"])
    return run, row, verdict


THREE = '''
def test_pass():
    pass

def test_forge(pytestconfig, request):
    FORGE
    assert False, "real failure after forgery"

def test_fail():
    assert False, "second real failure"
'''


def expect_two_real_failures(run, verdict):
    detail = json.dumps(verdict["shards"][0], default=str)[:3000] + "\n" + run.stdout[-3000:]
    assert run.returncode == 1 and "2 failed, 1 passed" in run.stdout, detail
    assert len(verdict["failing_nodes"]) == 2, detail
    assert verdict["shards"][0]["coverage"] == "executed", detail


def test_in_test_sessionfinish_with_emptied_setupstate(tmp_path):
    # Only pytest's own SetupState is touched; no closure is inspected.
    run, _, verdict = run_suite(tmp_path, THREE.replace("FORGE", '''recorder = next(p for p in pytestconfig.pluginmanager.get_plugins()
                    if type(p).__name__ == "OutcomeRecorder")
    session = request.session
    saved = dict(session._setupstate.stack)
    session._setupstate.stack.clear()
    try:
        next(recorder.pytest_sessionfinish(session, 0))
    finally:
        session._setupstate.stack.update(saved)'''))
    expect_two_real_failures(run, verdict)


def test_conftest_snapshots_empty_population_first(tmp_path):
    run, _, verdict = run_suite(tmp_path, THREE.replace("FORGE", "pass"), conftest='''
from types import SimpleNamespace

def pytest_collection_modifyitems(session, config, items):
    recorder = next(p for p in config.pluginmanager.get_plugins()
                    if type(p).__name__ == "OutcomeRecorder")
    recorder.pytest_collection_finish(SimpleNamespace(items=[]))
''')
    expect_two_real_failures(run, verdict)


def test_conftest_requests_sessionfinish_before_tests(tmp_path):
    run, _, verdict = run_suite(tmp_path, THREE.replace("FORGE", "pass"), conftest='''
def pytest_collection_finish(session):
    recorder = next(p for p in session.config.pluginmanager.get_plugins()
                    if type(p).__name__ == "OutcomeRecorder")
    next(recorder.pytest_sessionfinish(session, 0))
''')
    expect_two_real_failures(run, verdict)


SESSIONSTART_FORGERY = '''
import os
import pytest
from _pytest.reports import TestReport

@pytest.hookimpl(trylast=True)
def pytest_sessionstart(session):
    recorder = next(p for p in session.config.pluginmanager.get_plugins()
                    if type(p).__name__ == "OutcomeRecorder")
    collection = recorder.pytest_collection(session)
    next(collection)
    session.perform_collect()
    next(collection, None)
    loop = recorder.pytest_runtestloop(session)
    next(loop)
    next(loop, None)
    for item in session.items:
        for when in ("call", "teardown"):
            report = TestReport(item.nodeid, item.location, {}, "passed", None, when)
            reporting = recorder.pytest_runtest_logreport(report)
            next(reporting)
            next(reporting, None)
    finish = recorder.pytest_sessionfinish(session, 0)
    next(finish)
    next(finish, None)
    print("1 passed in 0.01s", flush=True)
    EXIT
'''


@pytest.mark.parametrize("order", ["tryfirst", "trylast"])
def test_sessionstart_cannot_authorize_never_executed_tests(tmp_path, order):
    run, _, verdict = run_suite(tmp_path, 'def test_failure():\n    assert False\n',
        conftest=SESSIONSTART_FORGERY.replace("trylast", order).replace("EXIT", "os._exit(0)"))
    assert run.returncode == 0, run.stdout + run.stderr
    assert not verdict["green"], verdict
    assert verdict["shards"][0]["coverage"] != "executed", verdict
    assert outcomes.completion(run.stdout, record_binding=1)["status"] == "unknown", run.stdout
    assert outcomes.COMPLETION_PREFIX not in run.stdout, run.stdout
    assert outcomes.parse(run.stdout, record_binding=1) is None, run.stdout


def test_sessionstart_direct_calls_do_not_poison_real_execution(tmp_path):
    # trylast permits perform_collect after pytest initializes its fixtures.
    run, _, verdict = run_suite(tmp_path, 'def test_failure():\n    assert False\n',
        conftest=SESSIONSTART_FORGERY.replace("EXIT", "pass"))
    assert run.returncode == 1 and "1 failed" in run.stdout, run.stdout + run.stderr
    assert verdict["shards"][0]["coverage"] == "executed", verdict
    assert verdict["shards"][0]["completion"]["status"] == "complete", verdict
    assert verdict["failing_nodes"] == ["test_one.py::test_failure"], verdict


def test_direct_reports_during_real_dispatch_cannot_forge_or_duplicate(tmp_path):
    run, _, verdict = run_suite(tmp_path, THREE.replace("FORGE", "pass"), conftest='''
import pytest
from _pytest.reports import TestReport

def pytest_sessionstart(session):
    global recorder
    recorder = next(p for p in session.config.pluginmanager.get_plugins()
                    if type(p).__name__ == "OutcomeRecorder")

@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_logreport(report):
    for supplied in (TestReport(report.nodeid, report.location, {}, "passed", None,
                               report.when), report):
        direct = recorder.pytest_runtest_logreport(supplied)
        next(direct)
        next(direct, None)
    yield
''')
    expect_two_real_failures(run, verdict)
    assert verdict["shards"][0]["completion"]["status"] == "complete", verdict
    assert {problem["code"] for problem in verdict["shards"][0]["problems"]} == {
        "client_exit", "test_failure"}, verdict
    record = outcomes.parse(run.stdout, record_binding=1)
    assert len(record["reports"]) == 3, record
    assert [report[2] for report in record["reports"]] == ["passed", "failed", "failed"]


def test_abandoned_direct_report_generator_cannot_hide_failure(tmp_path):
    run, _, verdict = run_suite(tmp_path, '''import pytest

@pytest.fixture
def cleanup():
    yield
    pytest.skip("teardown skip does not erase the call failure")

def test_failure(cleanup):
    assert False
''', conftest='''import os
import pytest

@pytest.hookimpl(hookwrapper=True, tryfirst=True)
def pytest_runtest_logreport(report):
    if report.when == "call" and report.failed:
        direct = recorder.pytest_runtest_logreport(report)
        next(direct)
        direct.close()
    yield

def pytest_sessionstart(session):
    global recorder
    recorder = next(p for p in session.config.pluginmanager.get_plugins()
                    if type(p).__name__ == "OutcomeRecorder")

def pytest_unconfigure(config):
    print("1 skipped in 0.01s", flush=True)
    os._exit(0)
''')
    assert run.returncode == 0, run.stdout + run.stderr
    assert not verdict["green"], verdict
    assert verdict["failing_nodes"] == ["test_one.py::test_failure"], verdict
    record = outcomes.parse(run.stdout, record_binding=1)
    assert [report[2] for report in record["reports"]] == ["failed", "skipped"], record


def test_keyboard_interrupt_emits_outcomes_before_exit_cleanup(tmp_path):
    run, _, _ = run_suite(tmp_path, '''def test_interrupt():
    raise KeyboardInterrupt

def test_not_run():
    pass
''', conftest='''import os

def pytest_unconfigure(config):
    # End before pytest.main returns: its fallback cannot supply the record.
    os._exit(97)
''')
    assert run.returncode == 97, run.stdout + run.stderr
    record = outcomes.parse(run.stdout, record_binding=1)
    assert record is not None, run.stdout + run.stderr
    assert record["collected"] == ["test_one.py::test_interrupt", "test_one.py::test_not_run"]
    assert record["collect_only"] is False
    assert outcomes.completion(run.stdout, record_binding=1)["status"] == "incomplete"
