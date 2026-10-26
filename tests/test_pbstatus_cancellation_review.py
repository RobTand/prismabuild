"""Cancellation must dispose of the reader owned by the interrupted call."""
import json
import os
from pathlib import Path
import signal
import select
import subprocess
import sys
import threading

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tools/fleet'))
import pbstatus


_KERNEL_REAP = pbstatus._reader._reap_within


def _reap_after_exit(pid, grace_s):
    descriptor = os.pidfd_open(pid)
    try:
        exited = select.poll()
        exited.register(descriptor, select.POLLIN)
        assert exited.poll(60_000), "killed reader did not exit"
    finally:
        os.close(descriptor)
    return _KERNEL_REAP(pid, grace_s)


@pytest.fixture(autouse=True)
def settled_reader_exit(monkeypatch):
    # Arrange exit delivery before checking reaping, independent of host load.
    monkeypatch.setattr(pbstatus._reader, '_reap_within', _reap_after_exit)


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
    monkeypatch.setattr(pbstatus._reader.select, 'select', interrupt)
    try:
        with pytest.raises(KeyboardInterrupt):
            pbstatus.bounded('pool', threading.Event().wait,
                             deadline=pbstatus.Deadline(60), abandoned=[])
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
    ready, gate = tmp_path / 'ready', tmp_path / 'gate'
    fork_ready, fork_gate = tmp_path / 'fork-ready', tmp_path / 'fork-gate'
    for path in (ready, gate, fork_ready, fork_gate):
        os.mkfifo(path)
    ready_fd = os.open(ready, os.O_RDWR | os.O_CLOEXEC)
    gate_fd = os.open(gate, os.O_RDWR | os.O_CLOEXEC)
    fork_ready_fd = os.open(fork_ready, os.O_RDWR | os.O_CLOEXEC)
    fork_gate_fd = os.open(fork_gate, os.O_RDWR | os.O_CLOEXEC)
    program = f'''
import os, sys, signal
sys.path[:0] = ['tools/fleet', 'tests']
import pbstatus
from test_pbstatus_cancellation_review import _reap_after_exit
signal.signal(signal.SIGINT, signal.default_int_handler)
pbstatus._reader._reap_within = _reap_after_exit
children = []
fork = os.fork
def track_fork():
    pid = fork()
    if pid:
        children.append(pid)
        with open({str(fork_ready)!r}, 'wb', buffering=0) as channel:
            channel.write(b'R')
        with open({str(fork_gate)!r}, 'rb', buffering=0) as channel:
            channel.read(1)
    return pid
os.fork = track_fork
def read():
    with open({str(ready)!r}, 'wb', buffering=0) as channel:
        channel.write(str(os.getpid()).encode())
    with open({str(gate)!r}, 'rb', buffering=0) as channel:
        channel.read(1)
try:
    pbstatus.bounded('pool', read, deadline=pbstatus.Deadline(60), abandoned=[])
finally:
    pid = children[0]
    try:
        result = os.waitpid(pid, os.WNOHANG)
    except ChildProcessError:
        print('reader-reaped', flush=True)
    else:
        print('reader-not-reaped', flush=True)
        # A failing cleanup mutant must not leave an orphan behind.
        if result == (0, 0):
            os.kill(pid, signal.SIGKILL)
            os.waitpid(pid, 0)
'''
    parent = subprocess.Popen([sys.executable, '-c', program], cwd=ROOT,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                              start_new_session=True, text=True)
    child_fd = None
    try:
        assert select.select([ready_fd], [], [], 60)[0], 'reader did not reach its gate'
        child = int(os.read(ready_fd, 64))
        child_fd = os.pidfd_open(child)
        assert select.select([fork_ready_fd], [], [], 60)[0], 'parent did not reach fork gate'
        assert os.read(fork_ready_fd, 1) == b'R'
        os.kill(parent.pid, signum)
        os.write(fork_gate_fd, b'R')
        output, errors = parent.communicate(timeout=60)
        assert output.strip() == 'reader-reaped', errors
        assert select.select([child_fd], [], [], 60)[0], 'cancelled reader survived'
        assert parent.returncode != 0
    finally:
        try:
            os.killpg(parent.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        parent.communicate(timeout=60)
        for fd in (ready_fd, gate_fd, fork_ready_fd, fork_gate_fd, child_fd):
            if fd is not None:
                os.close(fd)


def test_runtime_transport_lookup_shares_the_census_deadline(monkeypatch, capsys):
    clock = [0.0]

    class CensusDeadline(pbstatus.Deadline):
        def remaining(self):
            return self.timeout_s - clock[0]

    def expire_at_reader_wait(*args):
        clock[0] = 60.0
        return [], [], []

    monkeypatch.setattr(pbstatus, 'default_transport', threading.Event().wait)
    monkeypatch.setattr(pbstatus, 'Deadline', CensusDeadline)
    monkeypatch.setattr(pbstatus._reader.select, 'select', expire_at_reader_wait)
    code = pbstatus.main(['--json', '--timeout-s', '60'])
    report = json.loads(capsys.readouterr().out)
    assert code == 3 and report['complete'] is False
    assert 'transport' in report['timed_out_sections']
