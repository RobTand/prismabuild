"""Mirror publication is independent of another admission pass."""
import fcntl
import json
import os
from pathlib import Path
import select
import subprocess
import sys
import time
from types import SimpleNamespace

import pytest

from prismabuild import adaptive_snapshot
from prismabuild.adaptive_mirrors import LocalMirrorPublisher


def test_removing_local_evidence_removes_its_shared_mirror(tmp_path):
    local, shared = tmp_path / 'local', tmp_path / 'shared'
    local.mkdir()
    source = local / 'gpu-state.json'
    source.write_text(json.dumps({'sampled_unix': 10., 'sample_id': 'first'}))
    assert adaptive_snapshot.copy_snapshot(local, shared) == ['gpu-state.json']
    source.unlink()
    adaptive_snapshot.copy_snapshot(local, shared)
    assert not (shared / source.name).exists(), 'removed evidence remains published'


@pytest.fixture
def mirrors(tmp_path, monkeypatch):
    local, shared = tmp_path / 'local', tmp_path / 'shared'
    local.mkdir()
    clock = [100.]
    monkeypatch.setattr(adaptive_snapshot, 'time',
                        SimpleNamespace(monotonic=lambda: clock[0], time=time.time))
    observer = LocalMirrorPublisher(local, shared, clock=lambda: clock[0])
    try:
        yield observer, local, shared, clock
    finally:
        observer.close()
        for child in adaptive_snapshot._children:
            child.wait(timeout=10)
        adaptive_snapshot._children.clear()


def write_source(local, sample):
    (local / 'gpu-state.json').write_text(json.dumps({'sampled_unix': sample, 'capacity': 2}))


def finish_copy(observer):
    child = adaptive_snapshot._children[-1]
    assert child.wait(timeout=10) == 0
    observer.reconcile()


def test_dirty_mirror_keeps_its_rate_deadline_without_another_admission(mirrors):
    observer, local, shared, clock = mirrors
    write_source(local, 10.)
    observer.reconcile()
    finish_copy(observer)
    assert observer.reconcile() is None
    assert observer.diagnostics['starts'] == 1
    write_source(local, 11.)
    clock[0] = 100.5
    assert observer.reconcile() == 101.
    assert observer.diagnostics['starts'] == 1
    clock[0] = 101.
    observer.reconcile()
    finish_copy(observer)
    assert json.loads((shared / 'gpu-state.json').read_text())['sampled_unix'] == 11.
    assert observer.diagnostics['starts'] == 2
    assert observer.reconcile() is None


@pytest.mark.parametrize('mutation', ['in-place', 'replacement', 'removal'])
def test_local_writes_and_removal_notify_the_owned_observer(mirrors, mutation):
    observer, local, shared, clock = mirrors
    write_source(local, 10.)
    observer.reconcile()
    finish_copy(observer)
    clock[0] += 1.
    path = local / 'gpu-state.json'
    if mutation == 'in-place':
        write_source(local, 11.)
    elif mutation == 'replacement':
        replacement = local / '.replacement'
        replacement.write_text(json.dumps({'sampled_unix': 11., 'capacity': 2}))
        replacement.replace(path)
    else:
        path.unlink()
    assert select.select([observer.fileno()], [], [], 10)[0]
    observer.reconcile()
    finish_copy(observer)
    if mutation == 'removal':
        assert not (shared / path.name).exists()
    else:
        assert json.loads((shared / path.name).read_text())['sampled_unix'] == 11.
    completion = json.loads((shared / 'publisher-result.json').read_text())
    assert completion['status'] == 'published'
    assert completion['changed_files'] == ['gpu-state.json']


def test_identical_replacement_acknowledges_locally_without_shared_writes(mirrors):
    observer, local, shared, clock = mirrors
    write_source(local, 10.)
    observer.reconcile()
    finish_copy(observer)
    before = {name: (shared / name).stat() for name in ('gpu-state.json', 'publisher-result.json')}
    replacement = local / '.replacement'
    replacement.write_bytes((local / 'gpu-state.json').read_bytes())
    replacement.replace(local / 'gpu-state.json')
    clock[0] += 1.
    observer.reconcile()
    finish_copy(observer)
    for name, info in before.items():
        current = (shared / name).stat()
        assert (current.st_ino, current.st_mtime_ns) == (info.st_ino, info.st_mtime_ns)
    assert observer.reconcile() is None
    assert observer.diagnostics['starts'] == 2
    assert observer.diagnostics['completed'] == 2


def test_change_during_copy_survives_the_copied_signature(mirrors, monkeypatch):
    observer, local, shared, clock = mirrors
    reached, announce = os.pipe()
    resume, release = os.pipe()
    original = subprocess.Popen
    launches = [0]
    script = '''
import os, sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from prismabuild import adaptive_snapshot as snapshot
write = snapshot._write
shared = Path(sys.argv[3])
armed = True
def blocked(path, value):
    global armed
    if armed and path == shared / 'gpu-state.json':
        armed = False
        os.write(int(sys.argv[6]), b'x')
        os.read(int(sys.argv[7]), 1)
    write(path, value)
snapshot._write = blocked
raise SystemExit(snapshot.main(sys.argv[2:6]))
'''

    def launch(argv, **kwargs):
        launches[0] += 1
        if launches[0] == 1:
            kwargs['pass_fds'] += (announce, resume)
            argv = [sys.executable, '-c', script, str(Path(adaptive_snapshot.__file__).parent.parent),
                    *argv[2:], str(announce), str(resume)]
        return original(argv, **kwargs)

    monkeypatch.setattr(adaptive_snapshot.subprocess, 'Popen', launch)
    try:
        write_source(local, 10.)
        observer.reconcile()
        assert select.select([reached], [], [], 10)[0], 'copy did not reach injected boundary'
        os.read(reached, 1)
        write_source(local, 11.)
        observer.reconcile()
        assert observer.diagnostics['starts'] == 1
        os.write(release, b'x')
        assert adaptive_snapshot._children[-1].wait(timeout=10) == 0
        clock[0] += 1.
        observer.reconcile()
        finish_copy(observer)
        assert json.loads((shared / 'gpu-state.json').read_text())['sampled_unix'] == 11.
        assert observer.diagnostics['starts'] == 2
    finally:
        for descriptor in (reached, announce, resume, release):
            os.close(descriptor)


def test_failed_copy_retries_pending_state_and_reports_recovery(mirrors):
    observer, local, shared, clock = mirrors
    (local / 'gpu-state.json').write_text('not JSON')
    observer.reconcile()
    assert adaptive_snapshot._children[-1].wait(timeout=10) == 1
    assert observer.reconcile() == 101.
    assert observer.diagnostics['failed'] == 1
    assert json.loads((shared / 'publisher-result.json').read_text())['status'] == 'failed'
    write_source(local, 11.)
    clock[0] += 1.
    observer.reconcile()
    finish_copy(observer)
    assert json.loads((shared / 'publisher-result.json').read_text())['status'] == 'published'
    assert observer.reconcile() is None


def test_quiescent_session_has_no_deadline_and_closes_its_thread(mirrors):
    observer, _, _, _ = mirrors
    assert observer.reconcile() is None
    assert observer.diagnostics['starts'] == 0
    observer.start()
    observer.close()
    assert not observer._thread.is_alive()


def test_restart_adopts_pending_state_and_the_original_rate_deadline(mirrors):
    observer, local, shared, clock = mirrors
    write_source(local, 10.)
    observer.reconcile()
    finish_copy(observer)
    write_source(local, 11.)
    observer.close()
    replacement = LocalMirrorPublisher(local, shared, clock=lambda: clock[0])
    try:
        assert replacement.reconcile() == 101.
        clock[0] += 1.
        replacement.reconcile()
        finish_copy(replacement)
        assert json.loads((shared / 'gpu-state.json').read_text())['sampled_unix'] == 11.
    finally:
        replacement.close()


@pytest.mark.parametrize('mutation', ['symlink', 'unreadable', 'fifo'])
def test_unknown_local_evidence_is_not_copied_as_fresh_or_missing(mirrors, mutation):
    observer, local, shared, clock = mirrors
    write_source(local, 10.)
    observer.reconcile()
    finish_copy(observer)
    source = local / 'gpu-state.json'
    if mutation == 'symlink':
        source.unlink()
        source.symlink_to(local / 'missing')
    elif mutation == 'unreadable':
        source.chmod(0)
    else:
        source.unlink()
        os.mkfifo(source)
    clock[0] += 1.
    assert observer.reconcile() == clock[0] + .25
    assert observer.diagnostics['source_failures'] == 1
    assert json.loads((shared / source.name).read_text())['sampled_unix'] == 10.
    source.unlink()
    write_source(local, 11.)
    clock[0] += .25
    observer.reconcile()
    finish_copy(observer)
    assert json.loads((shared / source.name).read_text())['sampled_unix'] == 11.


def test_only_one_observer_owns_a_local_mirror_set(mirrors):
    _, local, shared, _ = mirrors
    with pytest.raises(BlockingIOError):
        LocalMirrorPublisher(local, shared)


def test_missing_source_directory_is_unknown_instead_of_a_set_of_removals(tmp_path):
    local, shared = tmp_path / 'local', tmp_path / 'shared'
    local.mkdir()
    write_source(local, 10.)
    adaptive_snapshot.copy_snapshot(local, shared)
    (local / 'gpu-state.json').unlink()
    local.rmdir()
    with pytest.raises(FileNotFoundError):
        adaptive_snapshot.copy_snapshot(local, shared)
    assert json.loads((shared / 'gpu-state.json').read_text())['sampled_unix'] == 10.


def test_failed_copy_uses_capped_recovery_without_same_result_feedback(mirrors):
    observer, local, _, clock = mirrors
    (local / 'gpu-state.json').write_text('not JSON')
    intervals = []
    for _ in range(6):
        observer.reconcile()
        assert adaptive_snapshot._children[-1].wait(timeout=10) == 1
        deadline = observer.reconcile()
        intervals.append(deadline - clock[0])
        assert observer.reconcile() == deadline
        clock[0] = deadline
    assert intervals == [1., 1., 1., 2., 4., 5.]
    assert observer.diagnostics['starts'] == observer.diagnostics['failed'] == 6


def test_exited_legacy_owner_cannot_make_an_unknown_lock_busy_wait(mirrors):
    observer, local, _, clock = mirrors
    write_source(local, 10.)
    descriptor = os.open(local / 'publish.lock', os.O_CREAT | os.O_RDWR, 0o600)
    fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    child = subprocess.Popen([sys.executable, '-c', 'pass'])
    pidfd = os.pidfd_open(child.pid)
    try:
        assert select.select([pidfd], [], [], 10)[0]
        ticks = adaptive_snapshot._start_ticks(child.pid)
        assert ticks is not None
        (local / 'publisher-owner.json').write_text(json.dumps({
            'pid': child.pid, 'start_ticks': ticks, 'started_monotonic': clock[0] - 2.}))
        assert observer.reconcile() == clock[0] + .05
        assert observer._owner_fd is None
    finally:
        os.close(pidfd)
        os.close(descriptor)
        child.wait(timeout=10)
