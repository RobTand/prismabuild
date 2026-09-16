"""Private process fixtures for advisory writer exclusion and recovery."""
import importlib.util
import json
import os
from pathlib import Path
import signal
import sys
import time

import pytest

WORKER = Path(__file__).resolve().parents[1] / 'tools/fleet/worker_loop.py'


@pytest.fixture
def worker(tmp_path):
    spec = importlib.util.spec_from_file_location('offer_adapter_test', WORKER)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.PUBLICATION_LOCK_ROOT = tmp_path / 'private-lock'
    return module


def publish(worker, callback, abandoned=None, budget=1):
    return worker.publish_offer(callback, budget_s=budget, retry_s=.01,
                                abandoned=[] if abandoned is None else abandoned)


def reap(pid, timeout=3):
    deadline = time.monotonic()+timeout
    while time.monotonic()<deadline:
        try:
            done, _ = os.waitpid(pid, os.WNOHANG)
        except ChildProcessError:
            return True
        if done == pid:
            return True
        time.sleep(.01)
    return False


def test_healthy_publication_preserves_offer_wire_format(worker, tmp_path):
    queue = worker.pool.PoolQueue(tmp_path / 'queue')
    result = publish(worker, lambda: queue.announce(host='test-host', tags=['x86'],
                     has_gpu=False, capacity={'cpu': 1, 'mem_gb': 2}))
    assert result.status == 'published', result
    offer = json.loads((queue.root / 'workers/test-host.json').read_text())
    assert offer['capacity'] == {'cpu': 1, 'mem_gb': 2}
    assert offer['host'] == 'test-host'
    assert len(queue.offers()) == 1


def test_failed_callback_is_not_a_success(worker):
    def fail():
        raise OSError('private fixture write failed')
    result = publish(worker, fail)
    assert result.status != 'published'
    assert 'private fixture write failed' in result.error


def test_retained_real_writer_fences_siblings_and_recovers(worker, tmp_path, monkeypatch):
    bounded_module = sys.modules[worker.bounded.__module__]
    original_stop = bounded_module._stop_reader
    fifo = tmp_path / 'hold-writer'
    os.mkfifo(fifo)
    entered = tmp_path / 'entered'
    completed = tmp_path / 'completed'
    admitted = tmp_path / 'second-writer'
    abandoned = []
    # Simulate a kernel-retained child without inducing a production NFS fault.
    def retain(pid, section, started, records):
        records.append({'pid': pid, 'starttime_ticks': bounded_module._starttime_ticks(pid),
                        'section': section, 'since_unix': time.time()})
    def blocked():
        entered.write_text('entered')
        fd = os.open(fifo, os.O_WRONLY)
        os.close(fd)
        completed.write_text('completed')
    monkeypatch.setattr(bounded_module, '_stop_reader', retain)
    pid = None
    try:
        first = publish(worker, blocked, abandoned, budget=.8)
        assert entered.exists(), first
        assert first.status != 'published' and len(abandoned) == 1, first
        pid = abandoned[0]['pid']
        # Restore ordinary cleanup for competing publishers.
        monkeypatch.setattr(bounded_module, '_stop_reader', original_stop)
        again = publish(worker, lambda: admitted.write_text('bad'), abandoned, budget=.3)
        assert again.status != 'published' and len(abandoned) == 1
        sibling = publish(worker, lambda: admitted.write_text('bad'), budget=.3)
        assert sibling.status != 'published', sibling
        assert not admitted.exists()
        reader = os.open(fifo, os.O_RDONLY | os.O_NONBLOCK)
        try:
            assert reap(pid), 'retained fixture writer failed to exit after release'
        finally:
            os.close(reader)
        assert completed.exists()
        recovered = publish(worker, lambda: admitted.write_text('recovered'), abandoned)
        assert recovered.status == 'published', recovered
        assert abandoned == [] and admitted.read_text() == 'recovered'
    finally:
        monkeypatch.setattr(bounded_module, '_stop_reader', original_stop)
        if pid is not None and not reap(pid, .01):
            os.kill(pid, signal.SIGKILL)
            assert reap(pid), 'private retained-writer fixture leaked a process'


@pytest.mark.parametrize('unsafe', ['root_symlink', 'root_writable', 'file_symlink', 'fifo', 'hardlink'])
def test_unsafe_lock_path_refuses_without_callback(worker, tmp_path, unsafe):
    root = worker.PUBLICATION_LOCK_ROOT
    called = tmp_path / 'called'
    target = tmp_path / 'target'
    target.write_text('unchanged')
    if unsafe == 'root_symlink':
        real = tmp_path / 'real-dir'
        real.mkdir(mode=0o700)
        root.symlink_to(real, target_is_directory=True)
    else:
        root.mkdir(mode=0o700)
        lock = worker.publication_lock_path()
        if unsafe == 'root_writable':
            root.chmod(0o777)
        elif unsafe == 'file_symlink':
            lock.symlink_to(target)
        elif unsafe == 'fifo':
            os.mkfifo(lock)
        elif unsafe == 'hardlink':
            target.chmod(0o600)
            os.link(target, lock)
    result = publish(worker, lambda: called.write_text('bad'), budget=.3)
    assert result.status != 'published', result
    assert not called.exists()
    assert target.read_text() == 'unchanged'
