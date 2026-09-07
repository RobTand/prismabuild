"""Cancellation must dispose of the reader owned by the interrupted call."""
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tools/fleet'))
import pbstatus


def test_interrupted_wait_reaps_its_child_and_closes_pipe(monkeypatch):
    children = []
    pipes = []
    fork, pipe = os.fork, os.pipe

    def track_fork():
        pid = fork()
        if pid:
            children.append(pid)
        return pid

    def track_pipe():
        fds = pipe()
        pipes.extend(fds)
        return fds

    def interrupt(*args):
        raise KeyboardInterrupt

    monkeypatch.setattr(pbstatus.os, 'fork', track_fork)
    monkeypatch.setattr(pbstatus.os, 'pipe', track_pipe)
    monkeypatch.setattr(pbstatus.select, 'select', interrupt)
    try:
        with pytest.raises(KeyboardInterrupt):
            pbstatus.bounded('pool', lambda: time.sleep(30),
                             deadline=pbstatus.Deadline(10), abandoned=[])
        with pytest.raises(ChildProcessError):
            os.waitpid(children[0], os.WNOHANG)
        for fd in pipes:
            with pytest.raises(OSError):
                os.fstat(fd)
    finally:
        for pid in children:
            try:
                os.kill(pid, signal.SIGKILL)
                os.waitpid(pid, 0)
            except (ProcessLookupError, ChildProcessError):
                pass
        for fd in pipes:
            try:
                os.close(fd)
            except OSError:
                pass


@pytest.mark.parametrize('signum', [signal.SIGINT, signal.SIGTERM])
def test_signal_to_parent_reaps_the_reader(tmp_path, signum):
    child_file = tmp_path / 'reader.pid'
    program = f'''
import os, sys, time
from pathlib import Path
sys.path.insert(0, 'tools/fleet')
import pbstatus
def read():
    Path({str(child_file)!r}).write_text(str(os.getpid()))
    time.sleep(30)
pbstatus.bounded('pool', read, deadline=pbstatus.Deadline(20), abandoned=[])
'''
    parent = subprocess.Popen([sys.executable, '-c', program], cwd=ROOT,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                              start_new_session=True)
    try:
        until = time.monotonic() + 5
        while not child_file.exists() and parent.poll() is None and time.monotonic() < until:
            time.sleep(0.01)
        assert child_file.exists(), 'reader never reached its blocked section'
        child = int(child_file.read_text())
        os.kill(parent.pid, signum)
        parent.communicate(timeout=3)
        assert not Path(f'/proc/{child}').exists(), 'cancelled parent left its reader alive'
    finally:
        try:
            os.killpg(parent.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        parent.communicate(timeout=3)


def test_runtime_transport_lookup_shares_the_census_deadline(monkeypatch, capsys):
    def blocked():
        time.sleep(2)
        return 'pool'
    monkeypatch.setattr(pbstatus, 'default_transport', blocked)
    started = time.monotonic()
    code = pbstatus.main(['--json', '--timeout-s', '0.1'])
    elapsed = time.monotonic() - started
    assert elapsed < 1, f'runtime metadata lookup exceeded the deadline: {elapsed}'
    report = json.loads(capsys.readouterr().out)
    assert code == 3 and report['complete'] is False
    assert 'transport' in report['timed_out_sections']
