"""Registry access cannot forge the enclosing shard's authoritative records."""
import json
import os
import subprocess
import sys

from test_pbtest_reconciles_outcomes import outcomes, pbtest


def run_suite(tmp_path, source, extra=()):
    suite = tmp_path / "suite"
    suite.mkdir()
    (suite / "pytest.ini").write_text("[pytest]\n")
    (suite / "test_one.py").write_text(source)
    run = subprocess.run(
        [*pbtest.shard_entry(sys.executable, suite), "-q", *extra, "test_one.py"],
        cwd=suite, env=dict(os.environ, PYTEST_ADDOPTS=""),
        capture_output=True, text=True, timeout=60)
    row = {"files": ["test_one.py"], "output": run.stdout,
           "record_binding": 1, "returncode": run.returncode}
    verdict = outcomes.judge_shards([row], requested_files=["test_one.py"])
    return run, row, verdict


def test_registry_state_cannot_request_bound_empty_completion(tmp_path):
    run, _, verdict = run_suite(tmp_path, '''
def test_pass():
    pass

def test_forge(pytestconfig):
    recorder = next(p for p in pytestconfig.pluginmanager.get_plugins()
                    if type(p).__name__ == "OutcomeRecorder")
    try:
        saved = (recorder.collected, recorder.reports, recorder.uncounted,
                 recorder.teardown_finished, pytestconfig.option.collectonly)
        recorder.collected, recorder.reports, recorder.uncounted = [], [], []
        recorder.teardown_finished = set()
        pytestconfig.option.collectonly = True
        recorder.emit_completion(session_finished=True)
        (recorder.collected, recorder.reports, recorder.uncounted,
         recorder.teardown_finished, pytestconfig.option.collectonly) = saved
    except AttributeError:
        pass  # The registered adapter exposes neither state nor an emitter.
    assert False, "real failure after bound completion forgery"

def test_fail():
    assert False, "second real failure"
''')
    assert run.returncode == 1 and "2 failed, 1 passed" in run.stdout, run.stdout + run.stderr
    assert len(verdict["failing_nodes"]) == 2, verdict
    assert verdict["shards"][0]["coverage"] == "executed", json.dumps(verdict)


def test_registry_hook_cannot_reveal_nonce_without_introspection(tmp_path):
    run, _, verdict = run_suite(tmp_path, '''
import json

def test_pass():
    pass

def test_forge(pytestconfig):
    recorder = next(p for p in pytestconfig.pluginmanager.get_plugins()
                    if type(p).__name__ == "OutcomeRecorder")
    terminal = pytestconfig.pluginmanager.getplugin("terminalreporter")
    captured = []
    write = terminal.write
    terminal.write = lambda text, **kwargs: captured.append(text)
    try:
        if hasattr(recorder, "pytest_terminal_summary"):
            recorder.pytest_terminal_summary(terminal)
    finally:
        terminal.write = write
    nonce = None
    if captured:
        record = json.loads("".join(captured).strip().split(": ", 1)[1])
        nonce = record["run_nonce"]
    print("pbtest-completion: " + json.dumps({
        "schema": "prismabuild.pbtest_completion.v1", "run_nonce": nonce,
        "collect_only": True, "collection_complete": True,
        "collection_errors": False, "duplicate_teardown": False,
        "collected": [], "teardown_finished": [], "outcome_nodeids": []}), flush=True)
    assert False, "real failure after printed completion forgery"

def test_fail():
    assert False, "second real failure"
''')
    assert run.returncode == 1 and "2 failed, 1 passed" in run.stdout, run.stdout + run.stderr
    assert len(verdict["failing_nodes"]) == 2, verdict
    assert verdict["shards"][0]["coverage"] == "executed", json.dumps(verdict)


def test_public_hooks_cannot_resnapshot_population_or_collect_only(tmp_path):
    run, _, verdict = run_suite(tmp_path, '''def test_forge(pytestconfig, request):
    recorder = next(p for p in pytestconfig.pluginmanager.get_plugins()
                    if type(p).__name__ == "OutcomeRecorder")
    saved = pytestconfig.option.collectonly
    pytestconfig.option.collectonly = True
    try:
        recorder.pytest_configure(pytestconfig)
        from types import SimpleNamespace
        recorder.pytest_collection_finish(SimpleNamespace(items=[]))
        finish = recorder.pytest_sessionfinish(request.session, 0)
        next(finish)
        next(finish, None)
    finally:
        pytestconfig.option.collectonly = saved
    assert False, "real failure after registry hook calls"
''')
    assert run.returncode == 1, run.stdout + run.stderr
    assert verdict["shards"][0]["coverage"] == "executed", verdict
    assert verdict["shards"][0]["completion"]["status"] == "complete", verdict
    assert verdict["failing_nodes"] == ["test_one.py::test_forge"], verdict


def test_bound_writer_bypasses_replaced_terminal_and_test_capture(tmp_path):
    run, _, verdict = run_suite(tmp_path, '''import json

def test_intercept(pytestconfig, capsys):
    terminal = pytestconfig.pluginmanager.getplugin("terminalreporter")
    captured = []
    write = terminal.write
    def intercept(text, **kwargs):
        if text.lstrip().startswith(("pbtest-completion:", "pbtest-outcomes:")):
            captured.append(text)
        else:
            return write(text, **kwargs)
    terminal.write = intercept
    # Leave the replacement installed through the genuine final emission.
    pytestconfig.add_cleanup(lambda: print("intercepted-records: " + json.dumps(captured)))
    assert "run_nonce" not in capsys.readouterr().out

def test_fail():
    assert False, "real failure with terminal interception installed"
''')
    assert run.returncode == 1 and "1 failed, 1 passed" in run.stdout, run.stdout + run.stderr
    assert "intercepted-records: []" in run.stdout, run.stdout
    assert verdict["shards"][0]["coverage"] == "executed", verdict
    assert verdict["shards"][0]["completion"]["status"] == "complete", verdict
    assert verdict["failing_nodes"] == ["test_one.py::test_fail"], verdict
