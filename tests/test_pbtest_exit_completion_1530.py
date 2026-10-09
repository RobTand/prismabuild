"""Test completion evidence must survive a blocked pytest exit hook (#1530)."""
from __future__ import annotations

import json
import os
from pathlib import Path
import select
import signal
import subprocess
import sys

import pytest

from test_pbtest_reports_whether_pytest_ran import _one_shard, pbtest


BLOCKER = '''\
import faulthandler
import os
import threading
import pytest


def block():
    faulthandler.dump_traceback()
    os.write(int(os.environ["PBTEST_BLOCK_FD"]), b"blocked\\n")
    threading.Event().wait()


@pytest.hookimpl(tryfirst=True)
def pytest_runtestloop(session):
    if os.environ["PBTEST_INCOMPLETE"] == "1":
        block()


@pytest.hookimpl(tryfirst=True)
def pytest_sessionfinish(session, exitstatus):
    if os.environ["PBTEST_EXIT_HOOK"] != "sessionfinish":
        return
    if os.environ["PBTEST_WORKER_EXIT"] == "1":
        if hasattr(session.config, "workerinput"):
            with open(os.environ["PBTEST_STACK_ROOT"] + str(os.getpid()), "w") as stream:
                faulthandler.dump_traceback(file=stream)
            threading.Event().wait()
    elif not hasattr(session.config, "workerinput"):
        block()


@pytest.hookimpl(tryfirst=True)
def pytest_unconfigure(config):
    if (os.environ["PBTEST_EXIT_HOOK"] == "unconfigure"
            and not hasattr(config, "workerinput")):
        block()
'''


def blocked_shard(tmp_path: Path, *, workers: int, incomplete: bool,
                  worker_exit: bool = False, stop_early: bool = False,
                  exit_hook: str = "sessionfinish") -> str:
    checkout = tmp_path / "suite"
    checkout.mkdir()
    (checkout / "pytest.ini").write_text("[pytest]\n")
    (checkout / "conftest.py").write_text(BLOCKER)
    (checkout / "test_one.py").write_text(
        "def test_one():\n    assert " + ("False" if stop_early else "True") +
        "\n\ndef test_two():\n    assert True\n")
    read_fd, write_fd = os.pipe()
    environment = dict(os.environ, PBTEST_BLOCK_FD=str(write_fd),
                       PBTEST_INCOMPLETE=str(int(incomplete)),
                       PBTEST_WORKER_EXIT=str(int(worker_exit)),
                       PBTEST_STACK_ROOT=str(checkout / "worker-stack-"),
                       PBTEST_EXIT_HOOK=exit_hook,
                       PYTHONUNBUFFERED="1", PYTEST_ADDOPTS="")
    arguments = ["-q", "-p", "no:cacheprovider"]
    if workers > 1:
        arguments += ["-n", str(workers)]
    if stop_early:
        arguments += ["-x"]
    process = subprocess.Popen(
        [*pbtest.shard_entry(sys.executable, checkout), *arguments, "test_one.py"],
        cwd=checkout, env=environment, stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT, text=True, start_new_session=True,
        pass_fds=(write_fd,))
    os.close(write_fd)
    try:
        if worker_exit:
            with pytest.raises(subprocess.TimeoutExpired):
                process.communicate(timeout=10)
        else:
            ready, _, _ = select.select([read_fd], [], [], 30)
            assert ready, "The shard did not reach the controlled blocker."
            assert os.read(read_fd, 64) == b"blocked\n"
    finally:
        os.close(read_fd)
        os.killpg(process.pid, signal.SIGKILL)
        output, _ = process.communicate(timeout=10)
    if worker_exit:
        stacks = [path.read_text() for path in checkout.glob("worker-stack-*")]
        assert len(stacks) == workers
        assert all(" in pytest_sessionfinish" in stack for stack in stacks)
    else:
        assert " in block" in output, output
    return output


@pytest.mark.parametrize("workers", [1, 2], ids=["single", "xdist"])
@pytest.mark.parametrize("incomplete", [False, True], ids=["after-tests", "during-test"])
def test_timeout_reports_the_verified_test_population(
        tmp_path, monkeypatch, capsys, workers, incomplete):
    if workers > 1:
        import xdist  # Required for this acceptance case.
    output = blocked_shard(tmp_path, workers=workers, incomplete=incomplete)
    code, record = _one_shard(tmp_path, monkeypatch,
                             output + "\npbrun: timeout on dl380g10\n", 1,
                             with_exit=True)
    console = capsys.readouterr().out
    assert code == 1
    assert record["ran"] is False
    assert record["timed_out"] is True
    assert record["reconciliation"] is None
    assert "0/1 shards green" in console
    completion = record.get("test_completion")
    assert completion is not None, record
    if incomplete:
        assert completion["status"] == "unknown"
        assert "TEST COMPLETION UNVERIFIED" in record["summary"]
    else:
        assert completion["status"] == "complete"
        assert completion["collected"] == ["test_one.py::test_one", "test_one.py::test_two"]
        assert completion["teardown_finished"] == completion["collected"]
        assert "TESTS COMPLETE; PROCESS DID NOT EXIT" in record["summary"]
        assert "execution/coverage unknown" not in record["summary"]
    assert record["summary"] in console


def test_progress_and_trace_events_do_not_prove_completion(tmp_path, monkeypatch):
    output = ".. [100%]\n" + "pbtest-trace: " + json.dumps({
        "schema": "prismabuild.pbtest_trace.v1", "event": "phase",
        "nodeid": "test_one.py::test_one", "when": "teardown", "outcome": "passed"})
    code, record = _one_shard(tmp_path, monkeypatch, output, 1, with_exit=True)
    assert code == 1
    assert record["test_completion"]["status"] == "unknown"
    assert record["ran"] is False


def test_xdist_completion_precedes_worker_exit_hooks(tmp_path, monkeypatch, capsys):
    import xdist
    output = blocked_shard(tmp_path, workers=2, incomplete=False, worker_exit=True)
    assert pbtest.pbtest_outcomes.parse(output) is None
    code, record = _one_shard(
        tmp_path, monkeypatch, output + "\npbrun: abcdef012345 timeout on dl380g10 in 10s\n",
        1, with_exit=True)
    assert code == 1
    assert record["test_completion"]["status"] == "complete"
    assert record["ran"] is False
    assert "TESTS COMPLETE; PROCESS DID NOT EXIT" in record["summary"]
    assert "0/1 shards green" in capsys.readouterr().out


def test_early_stop_records_an_incomplete_population(tmp_path, monkeypatch, capsys):
    output = blocked_shard(tmp_path, workers=1, incomplete=False, stop_early=True)
    code, record = _one_shard(
        tmp_path, monkeypatch, output + "\npbrun: timeout on dl380g10\n", 1,
        with_exit=True)
    assert code == 1
    completion = record["test_completion"]
    assert completion["status"] == "incomplete"
    assert completion["missing_teardown"] == ["test_one.py::test_two"]
    assert completion["missing_outcomes"] == ["test_one.py::test_two"]
    assert "TEST COMPLETION UNVERIFIED" in record["summary"]
    assert "0/1 shards green" in capsys.readouterr().out


@pytest.mark.parametrize("change", [
    {"collection_complete": False}, {"collection_errors": True},
    {"collect_only": True}, {"duplicate_teardown": True},
    {"collected": ["test_one.py::test_one", "test_one.py::test_one"]},
    {"teardown_finished": []}, {"outcome_nodeids": []},
    {"teardown_finished": ["test_one.py::test_other"]},
])
def test_inconsistent_completion_cannot_prove_the_population(change):
    record = {
        "schema": "prismabuild.pbtest_completion.v1",
        "collection_complete": True, "collect_only": False,
        "collection_errors": False, "duplicate_teardown": False,
        "collected": ["test_one.py::test_one"],
        "teardown_finished": ["test_one.py::test_one"],
        "outcome_nodeids": ["test_one.py::test_one"], **change,
    }
    completion = pbtest.pbtest_outcomes.completion(
        "pbtest-completion: " + json.dumps(record))
    assert completion["status"] == "incomplete"
    assert completion["problems"]


@pytest.mark.parametrize("record", [
    "{", "[]", '{"schema":"foreign"}',
    '{"schema":"prismabuild.pbtest_completion.v1","collected":"test_one"}',
])
def test_invalid_completion_remains_unknown(record):
    assert pbtest.pbtest_outcomes.completion(
        "pbtest-completion: " + record)["status"] == "unknown"


@pytest.mark.parametrize("workers", [1, 2], ids=["single", "xdist"])
def test_timeout_after_summary_keeps_final_counts_and_names_the_exit_hang(
        tmp_path, monkeypatch, capsys, workers):
    if workers > 1:
        import xdist
    output = blocked_shard(
        tmp_path, workers=workers, incomplete=False, exit_hook="unconfigure")
    summary = pbtest.pytest_summary(output.splitlines())
    assert summary.startswith("2 passed"), output
    assert pbtest.pbtest_outcomes.parse(output) is not None
    code, record = _one_shard(
        tmp_path, monkeypatch, output + "\npbrun: timeout on dl380g10\n", 1,
        with_exit=True)
    console = capsys.readouterr().out
    assert code == 1
    assert record["ran"] is True
    assert record["summary"] == summary
    assert record["timed_out"] is True
    assert record["test_completion"]["status"] == "complete"
    assert "TESTS COMPLETE; PROCESS DID NOT EXIT" in console
    assert "0/1 shards green" in console


def test_complete_tests_without_final_outcomes_cannot_make_an_exit_zero_green(
        tmp_path, monkeypatch, capsys):
    output = blocked_shard(tmp_path, workers=1, incomplete=False)
    code, record = _one_shard(tmp_path, monkeypatch, output, 0, with_exit=True)
    assert record["test_completion"]["status"] == "complete"
    assert record["timed_out"] is False
    assert record["ran"] is False
    assert code == 1
    assert "0/1 shards green" in capsys.readouterr().out
