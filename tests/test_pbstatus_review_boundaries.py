"""Review regressions for an un-reapable reader's externally visible lifetime.

Written by the independent review of PR #358 (codex-nfs-review) and copied here
unchanged apart from this note, because the tests that catch a defect belong
next to the code that had it.  The three are the review's own reproductions:
an abandoned reader must not hold the caller's captured output, must not hold
the caller's unrelated ``flock``, and a census that could not be read must not
be reported as a complete one.
"""
import fcntl
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


def test_abandoned_reader_releases_captured_stdout_and_stderr():
    # Model failed SIGKILL delivery, without inducing a live kernel/NFS fault.
    # The process group belongs only to this test and is always cleaned up.
    program = '''
import os,sys,time
sys.path.insert(0, 'tools/fleet')
import pbstatus
pbstatus.wedged_peers = lambda: {'peers': [], 'scanned': 0, 'truncated': False, 'note': None}
pbstatus.read_pool = lambda root: time.sleep(4)
pbstatus.os.kill = lambda pid, sig: None
raise SystemExit(pbstatus.main(['--transport', 'pool', '--json', '--timeout-s', '0.05']))
'''
    p = subprocess.Popen([sys.executable, '-c', program], cwd=ROOT,
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                         text=True, start_new_session=True)
    try:
        try:
            out, err = p.communicate(timeout=1.5)
        except subprocess.TimeoutExpired:
            pytest.fail(f'parent rc={p.poll()}, but its abandoned child still holds captured output pipes')
        assert p.returncode == 3
        assert json.loads(out)['abandoned_children']
    finally:
        try:
            os.killpg(p.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        p.communicate(timeout=3)


def test_abandoned_reader_does_not_retain_unrelated_flock(tmp_path, monkeypatch):
    lock_path = tmp_path / 'caller.lock'
    fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
    fcntl.flock(fd, fcntl.LOCK_EX)
    actual_kill = os.kill
    abandoned = []
    monkeypatch.setattr(pbstatus.os, 'kill', lambda pid, sig: None)
    try:
        result = pbstatus.bounded('pool', lambda: time.sleep(4),
                                  deadline=pbstatus.Deadline(0.05), abandoned=abandoned)
        assert result['status'] == 'timed_out' and abandoned
        os.close(fd)
        fd = None
        contender = os.open(lock_path, os.O_RDWR)
        try:
            try:
                fcntl.flock(contender, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                pytest.fail('abandoned reader retained the caller unrelated flock')
        finally:
            os.close(contender)
    finally:
        if fd is not None:
            os.close(fd)
        for child in abandoned:
            actual_kill(child['pid'], signal.SIGKILL)
            os.waitpid(child['pid'], 0)


def test_error_does_not_report_complete_census(monkeypatch, capsys):
    monkeypatch.setattr(pbstatus, 'wedged_peers', lambda: {'peers': [], 'scanned': 0, 'truncated': False, 'note': None})
    def unavailable(root):
        raise PermissionError('queue unavailable')
    monkeypatch.setattr(pbstatus, 'read_pool', unavailable)
    monkeypatch.setattr(pbstatus, 'read_endings', lambda root, limit: [])
    monkeypatch.setattr(pbstatus, 'queue_root_note', lambda root, **kwargs: None)
    pbstatus.main(['--transport', 'pool', '--json'])
    report = json.loads(capsys.readouterr().out)
    assert report['complete'] is False, report
