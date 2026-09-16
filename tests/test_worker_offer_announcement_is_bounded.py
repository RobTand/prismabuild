"""Causal private-FIFO regression for the worker's pre-admission offer write."""
import importlib.util
import json
import os
from pathlib import Path
import select
import signal
import sys
import time
from unittest.mock import patch

WORKER = Path(__file__).resolve().parents[1] / 'tools/fleet/worker_loop.py'


def test_blocked_offer_returns_without_admission(tmp_path):
    spec = importlib.util.spec_from_file_location('offer_regression_worker', WORKER)
    worker = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(worker)
    queue = worker.pool.PoolQueue(tmp_path / 'queue')
    fifo = tmp_path / 'blocked-write'
    os.mkfifo(fifo)
    entered, served = tmp_path / 'entered', tmp_path / 'served'
    def announce(**kwargs):
        entered.write_text('entered')
        fd = os.open(fifo, os.O_WRONLY)
        os.close(fd)
    def serve(**kwargs):
        served.write_text('admitted')
        return None
    queue.announce = announce
    queue.serve_once = serve
    read_fd, write_fd = os.pipe()
    pid = os.fork()
    if pid == 0:
        os.close(read_fd)
        try:
            worker.SH = tmp_path
            worker.PUBLICATION_LOCK_ROOT = tmp_path / 'writer-lock'
            worker.OFFER_PUBLISH_TIMEOUT_S = 0.3
            argv = ['worker_loop.py', '--class', 'x86', '--mem-gb', '2',
                    '--cpu-slots', '1', '--all-cores', '--assume-idle',
                    '--once', '--poll-s', '0']
            with patch.object(worker.pool, 'PoolQueue', return_value=queue), \
                 patch.object(worker, 'read_maintenance_gate', return_value=None), \
                 patch.object(worker, 'loaded_runtime_commit', return_value='fixed'), \
                 patch.object(worker, 'published_commit', return_value='fixed'), \
                 patch.object(worker, '_generation_at', return_value='fixed'), \
                 patch.object(worker.cpu_topology, 'inherited_tiers', return_value=None), \
                 patch.object(sys, 'argv', argv):
                result = {'returncode': worker._run_loop(lambda: False)}
        except BaseException as exc:
            result = {'error': repr(exc)}
        try:
            os.write(write_fd, json.dumps(result).encode())
        finally:
            os._exit(0)
    os.close(write_fd)
    data, reaped = b'', False
    deadline = time.monotonic() + 8
    try:
        while time.monotonic() < deadline:
            readable, _, _ = select.select([read_fd], [], [], min(.05, max(0, deadline-time.monotonic())))
            if readable:
                chunk = os.read(read_fd, 65536)
                data += chunk
            done, _ = os.waitpid(pid, os.WNOHANG)
            if done == pid:
                reaped = True
                # Drain the final small result after exit.
                while True:
                    chunk = os.read(read_fd, 65536)
                    if not chunk:
                        break
                    data += chunk
                break
        assert entered.exists(), f'fixture never reached announce: {data!r}'
        assert reaped, 'worker remains blocked in offer publication after entering it'
        result = json.loads(data)
        assert 'error' not in result, result
        assert result['returncode'] != 0, result
        assert not served.exists(), 'worker admitted work after unavailable offer publication'
    finally:
        os.close(read_fd)
        if not reaped:
            os.kill(pid, signal.SIGKILL)
            cleanup_deadline = time.monotonic() + 3
            while time.monotonic() < cleanup_deadline:
                done, _ = os.waitpid(pid, os.WNOHANG)
                if done == pid:
                    reaped = True
                    break
                time.sleep(.01)
            assert reaped, 'private FIFO fixture child did not exit'
