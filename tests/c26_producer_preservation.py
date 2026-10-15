"""POST-PRODUCER gate only. Do not collect this file on the old baseline.

The root admits this complete file after the producer/reader interface exists.
It is intentionally outside the default test_*.py discovery pattern.
"""
from __future__ import annotations

import ctypes
import hashlib
import json
import os
from pathlib import Path
import select
import threading
import time

import pytest

from c26_native_support import ROOT, dispatch, mq, repository
from test_c26_progress_characterization import (
    BLOCK, FILE, PASS, test_mergeq_heartbeat_does_not_count_a_forged_display_line as heartbeat_control)


def test_terminal_companion_binds_report_without_copying_populations(tmp_path, monkeypatch):
    root = repository(tmp_path / 'suite', {FILE: PASS})
    code, report, _ = dispatch(root, monkeypatch)
    assert code == 0
    state = json.loads(Path(str(report) + '.progress.json').read_text())
    assert state['report_sha256'] == hashlib.sha256(report.read_bytes()).hexdigest()
    assert not ({'verdicts', 'collected', 'population'} & state.keys())


@pytest.mark.parametrize('damage', ['uuid', 'generation', 'entrypoint', 'schema', 'counters', 'partial'])
def test_reader_refuses_foreign_or_invalid_real_companion(tmp_path, monkeypatch, damage):
    root = repository(tmp_path / 'suite', {FILE: PASS})
    code, report, _ = dispatch(root, monkeypatch)
    assert code == 0
    companion = Path(str(report) + '.progress.json')
    original = json.loads(companion.read_text())
    identity = {key: original[field] for key, field in (
        ('run_id', 'run_id'), ('launch_entrypoint', 'launch_entrypoint'),
        ('generation', 'producer_generation'))}
    owner = mq.load_outcomes(ROOT / 'tools/fleet/pbtest.py')
    assert owner.read_progress(companion, **identity) is not None
    changed = dict(original)
    if damage == 'uuid':
        changed['run_id'] = '0' * 32
    elif damage == 'generation':
        changed['producer_generation'] = str(tmp_path / 'other-generation')
    elif damage == 'entrypoint':
        changed['launch_entrypoint'] = str(tmp_path / 'other-entrypoint.py')
    elif damage == 'schema':
        changed['schema'] = 'prismabuild.pbtest_progress.v999'
    elif damage == 'counters':
        changed['completed_shards'] = changed['total_shards'] + 1
    companion.write_text('{"schema":' if damage == 'partial' else json.dumps(changed))
    assert owner.read_progress(companion, **identity) is None


@pytest.mark.parametrize('wrapper', ['runpy', 'fork'])
def test_same_generation_wrapper_retains_invocation_identity(tmp_path, monkeypatch, wrapper):
    monkeypatch.setenv('C26_RUNPY' if wrapper == 'runpy' else 'C26_FORK', '1')
    heartbeat_control(tmp_path, monkeypatch)
    companion = tmp_path / 'out/candidate.json.progress.json'
    state = json.loads(companion.read_text())
    assert state['producer_generation'] == str(ROOT)
    assert Path(state['launch_entrypoint']) == ROOT / 'tools/fleet/pbtest.py'


@pytest.mark.parametrize('shards', [2])
def test_later_client_completion_is_visible_before_first_client_ends(tmp_path, monkeypatch, shards):
    ready, release, fast = (tmp_path / name for name in ('ready', 'release', 'fast'))
    for path in (ready, release, fast):
        os.mkfifo(path)
    ready_fd, release_fd, fast_fd = [os.open(path, os.O_RDWR | os.O_NONBLOCK)
                                     for path in (ready, release, fast)]
    for name, path in (('C26_READY', ready), ('C26_RELEASE', release), ('C26_FAST_SIGNAL', fast)):
        monkeypatch.setenv(name, str(path))
    root = repository(tmp_path / 'suite', {'tests/test_a_blocked.py': BLOCK,
                                          'tests/test_b_fast.py': PASS})
    libc = ctypes.CDLL(None, use_errno=True)
    watch = libc.inotify_init1(os.O_CLOEXEC | os.O_NONBLOCK)
    assert watch >= 0, 'CONTROL FAILURE: inotify unavailable'
    assert libc.inotify_add_watch(watch, os.fsencode(tmp_path), 0x80) >= 0
    answers, errors = [], []
    def run():
        try:
            answers.append(dispatch(root, monkeypatch, shards=shards))
        except BaseException as exc:
            errors.append(exc)
    thread = threading.Thread(target=run, name='c26-out-of-order-producer')
    thread.start()
    snapshot = None
    try:
        available, _, _ = select.select([fast_fd], [], [], 30)
        assert available, 'CONTROL FAILURE: second native client did not complete'
        assert os.read(fast_fd, 64) == b'finished\n'
        companion = tmp_path / 'report.json.progress.json'
        assert companion.is_file(), 'producer omitted its initialized companion'
        deadline = time.monotonic() + 15
        while True:
            state = json.loads(companion.read_text())
            if state['completed_shards'] == 1:
                snapshot = state
                break
            remaining = deadline - time.monotonic()
            assert remaining > 0, 'CONTROL FAILURE: no completion snapshot after second client exit'
            available, _, _ = select.select([watch], [], [], remaining)
            assert available, 'CONTROL FAILURE: completion snapshot was not published'
            os.read(watch, 65536)
    finally:
        os.write(release_fd, b'x')
        thread.join(90)
        for fd in (ready_fd, release_fd, fast_fd, watch):
            os.close(fd)
    assert not thread.is_alive() and not errors, ('CONTROL FAILURE', errors)
    assert answers[0][0] == 0
    assert snapshot['total_shards'] == 2 and snapshot['completed_shards'] == 1
    assert snapshot['state'] == 'running'


def test_terminal_companion_write_failure_keeps_the_real_report_and_failed_client(tmp_path, monkeypatch):
    import errno
    root = repository(tmp_path / 'suite', {FILE: PASS})
    real_replace = os.replace
    failed = []
    def fail_terminal(source, destination, *args, **kwargs):
        if str(destination).endswith('.progress.json'):
            state = json.loads(Path(source).read_text())
            if state.get('state') == 'finished':
                failed.append(dict(state))
                raise OSError(errno.EIO, 'controlled terminal companion failure')
        return real_replace(source, destination, *args, **kwargs)
    monkeypatch.setattr(os, 'replace', fail_terminal)
    code, report, rows = dispatch(root, monkeypatch)
    assert code == 1 and failed
    assert rows[0]['returncode'] == 0 and rows[0]['ran']
    from c26_native_support import consume
    got = consume(tmp_path, report, [FILE], code)
    assert got.inconclusive == [FILE] and not got.failed


def test_unwritable_initial_companion_refuses_before_any_submission(tmp_path, monkeypatch):
    from c26_native_support import REAL_POPEN, pbtest
    import errno
    import sys
    root = repository(tmp_path / 'suite', {FILE: PASS})
    report = tmp_path / 'refused.json'
    real_replace = os.replace
    blocked, launched = [], []
    def fail_initial(source, destination, *args, **kwargs):
        if str(destination).endswith('.progress.json'):
            blocked.append(str(destination))
            raise PermissionError(errno.EACCES, 'controlled unwritable companion')
        return real_replace(source, destination, *args, **kwargs)
    def no_submission(command, **kwargs):
        if str(pbtest.PBRUN) in [str(part) for part in command]:
            launched.append(command)
            raise AssertionError('CONTROL FAILURE: launched despite unwritable requested output')
        return REAL_POPEN(command, **kwargs)
    monkeypatch.setattr(os, 'replace', fail_initial)
    monkeypatch.setattr(pbtest.subprocess, 'Popen', no_submission)
    monkeypatch.setattr(sys, 'argv', ['pbtest.py', '--checkout', str(root), '--python', sys.executable,
        '--threads-per-shard', '1', '--shards', '1', '--json', str(report), 'tests'])
    assert pbtest.main() == 2
    assert blocked and not launched
