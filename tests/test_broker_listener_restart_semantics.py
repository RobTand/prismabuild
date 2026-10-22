"""Descriptor waits preserve broker death, payload survival and in-process stop."""
import importlib.util
import os
from pathlib import Path
import select
import signal
import socket
import subprocess
import sys
import tempfile
import threading

import pytest

ROOT = Path(__file__).resolve().parents[1]

# Run the actual main, replacing only privileged startup and the sampler.
BROKER_MAIN_PROBE = r'''
import importlib.util, os, selectors, signal, subprocess, sys, threading
from pathlib import Path
spec = importlib.util.spec_from_file_location('probe_broker', sys.argv[1])
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)
endpoint, mode = sys.argv[2:4]
notify, gate, completed, trigger = map(int, sys.argv[4:8])
result = sys.argv[8]
class Authority:
    def __init__(self, *args, **kwargs):
        self.installation_paths = {}; self.installed_sha256 = {}; self.child = None
    def run(self, uid, pid, request, stdio):
        code = "import os,pathlib,sys; os.read(int(sys.argv[1]),1); pathlib.Path(sys.argv[3]).write_text('completed'); os.write(int(sys.argv[2]),b'C')"
        self.child = subprocess.Popen([sys.executable, '-c', code, str(gate), str(completed), result],
            stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            pass_fds=(gate, completed), start_new_session=True)
        os.write(notify, (str(self.child.pid)+'\n').encode())
        return self.child
    def handle(self, uid, pid, request):
        assert request['op'] == 'stop'
        os.write(notify, b'K')
        self.child.kill()
        return {'ok': True}
class Monitor:
    def __init__(self, *args, **kwargs):
        self.parked = threading.Event()
    def healthy(self): return True
    def start(self):
        self.thread = threading.Thread(target=self.parked.wait, daemon=True)
        self.thread.start()
        if mode == 'signal_thread':
            def send():
                os.read(trigger, 1)
                signal.pthread_kill(self.thread.ident, signal.SIGTERM)
            threading.Thread(target=send, daemon=True).start()
    def close(self):
        self.parked.set()
        self.thread.join()
base_server = m.Server
class ReadyServer(base_server):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        os.write(notify, b'R')
m.Server = ReadyServer
base_selector = selectors.DefaultSelector
class ObservedSelector(base_selector):
    def select(self, timeout=None):
        if mode == 'signal_thread': os.write(notify, b'W')
        return super().select(timeout)
selectors.DefaultSelector = ObservedSelector
m.Authority = Authority
m.ResourceMonitor = Monitor
m._load_gpu_module = lambda *a, **k: None
m._trusted_file = lambda path: Path(m.__file__)
m.os.geteuid = lambda: 0
m.os.chown = lambda *a: None
sys.argv = ['broker', '--socket', endpoint, '--state-dir', str(Path(endpoint).parent/'state'),
            '--maintenance-state', str(Path(endpoint).parent/'maintenance'), '--uid', str(os.getuid())]
m.main()
'''


def _broker_probe_event(fd, expected):
    assert select.select([fd], [], [], 5)[0], 'broker probe event did not arrive'
    assert os.read(fd, len(expected)) == expected


@pytest.mark.parametrize('mode', ['payload', 'signal_thread'])
def test_broker_sigterm_preserves_payload_completion_and_native_exit(mode):
    notify_r, notify_w = os.pipe()
    gate_r, gate_w = os.pipe()
    completed_r, completed_w = os.pipe()
    trigger_r, trigger_w = os.pipe()
    with tempfile.TemporaryDirectory(prefix='pb1937-', dir='/home/rob/pbt17') as folder:
        endpoint = str(Path(folder) / 'broker.sock')
        result = Path(folder) / 'result'
        process = subprocess.Popen([sys.executable, '-c', BROKER_MAIN_PROBE,
            str(ROOT / 'tools/fleet/resource_broker.py'), endpoint, mode,
            str(notify_w), str(gate_r), str(completed_w), str(trigger_r), str(result)],
            pass_fds=(notify_w, gate_r, completed_w, trigger_r),
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        for fd in (notify_w, gate_r, completed_w, trigger_r):
            os.close(fd)
        payload_fd = None
        try:
            _broker_probe_event(notify_r, b'R')
            if mode == 'signal_thread':
                _broker_probe_event(notify_r, b'W')
                wait_channel = Path(f'/proc/{process.pid}/wchan').read_text().strip()
                assert 'ep' in wait_channel, f'listener not blocked in selector: {wait_channel}'
                os.write(trigger_w, b'x')
                try:
                    process.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    pytest.fail('SIGTERM delivered to monitor left the listener asleep')
            else:
                with socket.socket(socket.AF_UNIX) as client:
                    client.settimeout(5)
                    client.connect(endpoint)
                    client.sendall(b'{"op":"run","action_key":"a","nonce":"b","token":"c"}\n')
                    # The payload PID arrives before the child may finish.
                    assert select.select([notify_r], [], [], 5)[0]
                    data = os.read(notify_r, 128).replace(b'W', b'')
                    if not data:
                        assert select.select([notify_r], [], [], 5)[0]
                        data = os.read(notify_r, 128).replace(b'W', b'')
                    payload_pid = int(data.strip())
                    payload_fd = os.pidfd_open(payload_pid)
                    process.send_signal(signal.SIGTERM)
                    process.wait(timeout=5)
                    try:
                        os.write(gate_w, b'x')
                    except BrokenPipeError:
                        pass
                    _broker_probe_event(completed_r, b'C')
                    assert result.read_text() == 'completed'
                    assert client.recv(4096) == b'', 'broker death must remain control-channel EOF'
            assert process.returncode == -signal.SIGTERM
        finally:
            if process.poll() is None:
                process.kill()
            _, errors = process.communicate(timeout=5)
            if errors:
                print(errors.decode())
            if payload_fd is not None:
                try:
                    signal.pidfd_send_signal(payload_fd, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                os.close(payload_fd)
            for fd in (notify_r, gate_w, completed_r, trigger_w):
                os.close(fd)


def test_broker_stop_writer_is_serialized_with_pipe_teardown(monkeypatch):
    spec = importlib.util.spec_from_file_location('race_broker', ROOT / 'tools/fleet/resource_broker.py')
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    paused = threading.Event()
    resume = threading.Event()
    close_entered = threading.Event()
    close_finished = threading.Event()
    failures = []
    replacement_r, replacement_w = os.pipe()
    native_write = os.write
    reused = None
    with tempfile.TemporaryDirectory(prefix='pb1937-', dir='/home/rob/pbt17') as folder:
        server = m.Server(str(Path(folder) / 'broker.sock'), m.Handler)
        actual_lock = getattr(server, 'stop_lock', threading.Lock())

        class ObservedLock:
            def __enter__(self):
                if threading.current_thread() is closer:
                    close_entered.set()
                actual_lock.acquire()
            def __exit__(self, *args):
                actual_lock.release()

        server.stop_lock = ObservedLock()

        def intercept(fd, data):
            if fd == server.stop_write and threading.current_thread() is stopper:
                paused.set()
                assert resume.wait(5)
            return native_write(fd, data)

        def stop_probe():
            try:
                server.request_stop()
            except BaseException as exc:
                failures.append(exc)

        def close_probe():
            server.server_close()
            close_finished.set()
            close_entered.set()

        stopper = threading.Thread(target=stop_probe)
        closer = threading.Thread(target=close_probe)
        monkeypatch.setattr(m.os, 'write', intercept)
        stopper.start()
        try:
            assert paused.wait(5)
            closer.start()
            assert close_entered.wait(5)
            if close_finished.is_set():
                # Force reuse after a premature close; EBADF alone is not the only risk.
                reused = server.stop_write
                os.dup2(replacement_w, reused)
            resume.set()
            stopper.join(5)
            closer.join(5)
            assert not stopper.is_alive() and not closer.is_alive()
            assert not failures, f'stop writer reached a closed pipe: {failures}'
            assert not select.select([replacement_r], [], [], 0)[0], 'late stop wrote to a reused descriptor'
        finally:
            resume.set()
            stopper.join(5)
            if closer.ident is not None:
                closer.join(5)
            server.server_close()
            if reused is not None:
                os.close(reused)
            os.close(replacement_r)
            os.close(replacement_w)
