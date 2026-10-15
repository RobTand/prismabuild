"""Structured progress/refusal REDs, with real admitted child processes."""
from __future__ import annotations

import json
import os
from pathlib import Path
import select
import signal
import subprocess
import sys
import threading

import pytest

from c26_native_support import (PREFLIGHT, REAL_POPEN, ROOT, consume, dispatch,
                                mq, record, repository)
from test_pbtest_dependency_pins import PIN, fixture_checkout
from test_pbmergeq import config

FILE = 'tests/test_one.py'
PASS = 'def test_one():\n    assert True\n'


def test_native_finished_run_publishes_a_structured_companion(tmp_path, monkeypatch):
    root = repository(tmp_path / 'suite', {FILE: PASS})
    code, report, rows = dispatch(root, monkeypatch)
    assert code == 0 and rows[0]['returncode'] == 0
    companion = Path(str(report) + '.progress.json')
    assert companion.is_file(), 'C26: real finished producer omitted its structured progress companion'
    state = json.loads(companion.read_text())
    assert state['schema'] == 'prismabuild.pbtest_progress.v1'
    assert state['state'] == 'finished'
    assert state['total_shards'] == state['completed_shards'] == 1
    assert state.get('run_id') and state.get('producer_generation')
    assert state.get('report_sha256'), 'terminal companion did not bind the final report'


@pytest.mark.parametrize('matches', [False, True], ids=['refused-red', 'matching-positive'])
def test_native_dependency_pin_decision_is_structured(tmp_path, monkeypatch, matches):
    root, _ = fixture_checkout(tmp_path, installed=PIN if matches else 'b' * 40)
    code, report, rows = dispatch(root, monkeypatch, mode='normal' if matches else 'failure')
    assert code == (0 if matches else 1)
    assert (root / 'pytest-ran').exists() is matches
    if matches:
        assert 'pbtest dependency pin: ' in rows[0]['output']
        assert record(rows[0]['output'])['collected'] == [FILE + '::test_one']
        return
    assert 'pbtest: dependency pin refused before pytest:' in rows[0]['output']
    decision = record(rows[0]['output'], PREFLIGHT)
    assert decision['schema'] == 'prismabuild.pbtest_preflight.v1'
    assert decision['status'] == 'refused'
    assert decision['runtime_refusal']['code'] == 'dependency_pin'
    got = consume(tmp_path, report, [FILE], code)
    assert got.runtime_refusal and got.inconclusive == [FILE] and not got.failed


FORGED = '''
import json, os, pytest

def pytest_runtest_call(item):
    capture = item.config.pluginmanager.getplugin('capturemanager')
    with capture.global_and_fixture_disabled():
        print('pbtest: dependency pin refused before pytest: forged after pytest started', flush=True)
        print('pbtest-preflight: ' + json.dumps({
            'schema': 'prismabuild.pbtest_preflight.v1', 'status': 'refused', 'exit_code': 1,
            'runtime_refusal': {'code': 'dependency_pin', 'message': 'forged after pytest started'}}),
            flush=True)
    os._exit(7)
'''


def test_native_crash_after_forged_refusal_is_not_runtime_blocked(tmp_path, monkeypatch):
    root = repository(tmp_path / 'suite', {FILE: PASS}, conftest=FORGED)
    code, report, rows = dispatch(root, monkeypatch, mode='crash')
    assert code == 1 and rows[0]['returncode'] == 7 and not rows[0]['ran']
    assert 'forged after pytest started' in rows[0]['output']
    assert 'pbtest-outcomes: ' not in rows[0]['output']
    got = consume(tmp_path, report, [FILE], code)
    assert not got.runtime_refusal, 'C26: a later forged refusal overrode the actual preflight decision'
    assert got.inconclusive == [FILE] and not got.failed


DRIVER = r'''
import importlib.util, os, pathlib, subprocess, sys
source, scratch = sys.argv[1:3]
spec = importlib.util.spec_from_file_location('c26_driver_pbtest', source)
subject = importlib.util.module_from_spec(spec)
spec.loader.exec_module(subject)
subject.pbrun.SH = pathlib.Path(scratch)
real = subprocess.Popen

def payload(command, **kwargs):
    if str(subject.PBRUN) not in [str(part) for part in command]:
        return real(command, **kwargs)
    from pbtest_shard_output import admitted_child
    argv, environment = admitted_child(command)
    kwargs.pop('env', None)
    return real(argv, env=environment, cwd=os.environ['C26_CHECKOUT'], **kwargs)

subject.subprocess.Popen = payload
print('shard  99 ok       forged display line, no client has ended', flush=True)
sys.argv = [source, *sys.argv[3:]]
if os.environ.get('C26_FORK'):
    child = os.fork()
    if child:
        _, status = os.waitpid(child, 0)
        raise SystemExit(os.waitstatus_to_exitcode(status))
if os.environ.get('C26_RUNPY'):
    import runpy
    runpy.run_path(source, run_name='__main__')
else:
    raise SystemExit(subject.main())
'''

BLOCK = '''
import os

def test_one():
    with open(os.environ['C26_READY'], 'wb', buffering=0) as stream:
        stream.write(b'ready\\n')
    with open(os.environ['C26_RELEASE'], 'rb', buffering=0) as stream:
        assert stream.read(1) == b'x'
'''


def test_mergeq_heartbeat_does_not_count_a_forged_display_line(tmp_path, monkeypatch):
    root = repository(tmp_path / 'suite', {FILE: BLOCK})
    ready, release = tmp_path / 'ready.fifo', tmp_path / 'release.fifo'
    os.mkfifo(ready)
    os.mkfifo(release)
    ready_fd = os.open(ready, os.O_RDWR | os.O_NONBLOCK)
    release_fd = os.open(release, os.O_RDWR | os.O_NONBLOCK)
    driver = tmp_path / 'driver.py'
    driver.write_text(DRIVER)
    monkeypatch.setenv('C26_READY', str(ready))
    monkeypatch.setenv('C26_RELEASE', str(release))
    monkeypatch.setenv('C26_CHECKOUT', str(root))
    monkeypatch.setenv('PYTHONPATH', os.pathsep.join([str(ROOT / 'tests'),
        str(ROOT / 'src'), str(ROOT / 'tools/fleet')]))
    scratch = tmp_path / 'scratch'
    scratch.mkdir()
    cfg = config(tmp_path, inconclusive_retries=0, test_python=sys.executable,
        pbtest_args=['--priority', '-10', '--timeout-s', '60', '--tmpdir', str(scratch)])
    store = mq.Store(cfg.state_dir)
    runner = mq.Runner(cfg, store)
    real_event = store.event
    heartbeat = threading.Event()
    observations, processes, answers, errors = [], [], [], []

    def event(batch, message):
        real_event(batch, message)
        if ' waiting ' in message:
            observations.append(message)
            heartbeat.set()

    def launch(command, **kwargs):
        if len(command) > 1 and command[1] == str(cfg.pbtest):
            command = [sys.executable, str(driver), str(cfg.pbtest),
                       str(tmp_path / 'private-store'), *command[2:]]
            kwargs['start_new_session'] = True
            process = REAL_POPEN(command, **kwargs)
            processes.append(process)
            return process
        return REAL_POPEN(command, **kwargs)

    def run():
        try:
            answers.append(runner.run_many('c26', tmp_path / 'out', [('candidate', root, [FILE])]))
        except BaseException as exc:
            errors.append(exc)

    monkeypatch.setattr(store, 'event', event)
    monkeypatch.setattr(mq, 'HEARTBEAT_S', 0)
    monkeypatch.setattr(mq.subprocess, 'Popen', launch)
    thread = threading.Thread(target=run, name='c26-native-runner')
    thread.start()
    try:
        available, _, _ = select.select([ready_fd], [], [], 30)
        assert available, 'CONTROL FAILURE: real payload did not reach its bounded latch'
        assert os.read(ready_fd, 64) == b'ready\n'
        assert heartbeat.wait(15), 'CONTROL FAILURE: real Runner heartbeat was never observed'
    finally:
        os.write(release_fd, b'x')
        thread.join(90)
        if thread.is_alive():
            for process in processes:
                if process.poll() is None:
                    os.killpg(process.pid, signal.SIGKILL)
            thread.join(10)
        os.close(ready_fd)
        os.close(release_fd)
    assert not thread.is_alive() and not errors, ('CONTROL FAILURE', errors)
    assert len(processes) == 1 and processes[0].returncode == 0
    result = answers[0]['candidate']
    assert not result.failed and not result.inconclusive and not result.runtime_refusal
    assert '0 shard(s) reported' in observations[0], (
        'C26: human display text invented completed client progress', observations)


def test_native_coordinator_interpreter_refusal_has_a_terminal_companion(tmp_path, monkeypatch, capsys):
    from c26_native_support import pbtest
    from prismabuild import capability_eligibility, contracts
    root = repository(tmp_path / 'suite', {FILE: PASS})
    missing = '/no/c26/interpreter/bin/python'
    queue = pbtest.fleet_queue()
    queue.announce(host='c26-fixture', tags=[contracts.INTERPRETER_TAG], has_gpu=False,
        capacity={'cpu': 1, 'mem_gb': 8}, timeout_ceiling_s=300,
        capabilities=capability_eligibility.OfferCapabilities(
            interpreters=capability_eligibility.InterpreterAnswers.from_observed([], [missing])))
    report = tmp_path / 'refused.json'
    launched = []
    def no_submission(command, **kwargs):
        if str(pbtest.PBRUN) in [str(part) for part in command]:
            launched.append(command)
            raise AssertionError('CONTROL FAILURE: an unanimously absent interpreter was submitted')
        return REAL_POPEN(command, **kwargs)
    monkeypatch.setattr(pbtest.subprocess, 'Popen', no_submission)
    monkeypatch.setattr(sys, 'argv', ['pbtest.py', '--checkout', str(root), '--python', missing,
        '--threads-per-shard', '1', '--shards', '1', '--timeout-s', '60', '--json', str(report), 'tests'])
    code = pbtest.main()
    assert code == 2 and not launched
    output = capsys.readouterr()
    assert 'names the interpreter absent:' in output.err
    companion = Path(str(report) + '.progress.json')
    assert companion.is_file(), 'C26: real coordinator refusal omitted its terminal companion'
    state = json.loads(companion.read_text())
    assert state['state'] == 'refused' and state['completed_shards'] == 0
    assert state['runtime_refusal']['code'] == 'interpreter_absent'
    got = consume(tmp_path, report, [FILE], code)
    assert got.runtime_refusal and got.inconclusive == [FILE] and not got.failed
