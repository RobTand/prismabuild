"""Listener shutdown and child completion are descriptor events, without ticks."""
import http.client
import importlib.util
import json
import os
from pathlib import Path
import queue
import select
import selectors
import socket
import socketserver
import subprocess
import sys
import threading
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))
sys.path.insert(0, str(ROOT / 'tools/fleet'))
import pbmetrics
from prismabuild import role_session


@pytest.fixture
def listener_wait_calls(monkeypatch):
    calls = queue.Queue()

    def observed_selector(base):
        class ObservedSelector(base):
            def select(self, timeout=None):
                calls.put((set(self.get_map()), timeout))
                return super().select(timeout)
        return ObservedSelector

    monkeypatch.setattr(socketserver, '_ServerSelector',
                        observed_selector(socketserver._ServerSelector))
    monkeypatch.setattr(selectors, 'DefaultSelector',
                        observed_selector(selectors.DefaultSelector))
    return calls


@pytest.fixture
def listener_broker():
    spec = importlib.util.spec_from_file_location(
        'listener_broker', ROOT / 'tools/fleet/resource_broker.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_metrics_blocks_on_listener_and_stop_and_serves_scrapes(
        tmp_path, monkeypatch, listener_wait_calls):
    servers = queue.Queue()
    base = pbmetrics.ThreadingHTTPServer

    class ObservedHTTPServer(base):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            servers.put(self)

    monkeypatch.setattr(pbmetrics, 'ThreadingHTTPServer', ObservedHTTPServer)
    monkeypatch.setattr(pbmetrics, 'collect_metrics', lambda *a, **k: 'probe_metric 1\n')
    monkeypatch.setattr(role_session, 'read_maintenance_gate', lambda: None)
    stop = role_session.StopRequest()
    args = pbmetrics.parser().parse_args(['--queue-root', str(tmp_path), '--port', '0'])
    results = []
    thread = threading.Thread(target=lambda: results.append(
        pbmetrics._serve_requests(args, refused=75, stop=stop)))
    thread.start()
    try:
        server = servers.get(timeout=5)
        descriptors, timeout = listener_wait_calls.get(timeout=5)
        assert timeout is None, 'metrics listener still wakes on a fixed tick'
        assert server.fileno() in descriptors
        assert len(descriptors) == 2, 'metrics stop must share the listener wait'
        connection = http.client.HTTPConnection(*server.server_address, timeout=5)
        try:
            connection.request('GET', '/metrics')
            response = connection.getresponse()
            assert response.status == 200
            assert response.read() == b'probe_metric 1\n'
        finally:
            connection.close()
        listener_wait_calls.get(timeout=5)  # Back at the idle descriptor wait.
        stop.set()
        thread.join(2)
        assert not thread.is_alive(), 'idle stop did not wake the listener'
        assert results == [0]
    finally:
        stop.set()
        thread.join(5)
        stop._close()


def test_broker_listener_has_no_shutdown_tick(listener_broker, listener_wait_calls, tmp_path):
    m = listener_broker
    with m.Server(str(tmp_path / 'listen.sock'), m.Handler) as server:
        thread = threading.Thread(target=server.serve_forever)
        thread.start()
        try:
            descriptors, timeout = listener_wait_calls.get(timeout=5)
            assert timeout is None, 'broker listener still wakes on a fixed tick'
            assert server.fileno() in descriptors
            assert len(descriptors) == 2, 'broker stop must share the listener wait'
        finally:
            server.shutdown()
            thread.join(2)
        assert not thread.is_alive(), 'shutdown did not wake the idle listener'


@pytest.mark.parametrize('ending', ['exit', 'disconnect', 'unexpected', 'shutdown'])
def test_broker_run_waits_on_pidfd_and_client_without_tick(
        listener_broker, monkeypatch, tmp_path, ending):
    m = listener_broker
    calls = queue.Queue()
    launched = threading.Event()
    stopped = threading.Event()
    child = subprocess.Popen([sys.executable, '-c', 'import sys; sys.stdin.buffer.read(1)'],
                             stdin=subprocess.PIPE)

    def observe_run_wait(readers, writers, errors, timeout=None):
        if any(isinstance(fd, int) and os.readlink(f'/proc/self/fd/{fd}') == 'anon_inode:[pidfd]'
               for fd in readers):
            calls.put((readers, timeout))
        return select.select(readers, writers, errors, timeout)

    def launch_probe(*args):
        launched.set()
        return child

    def stop_probe(uid, pid, request):
        assert request['op'] == 'stop'
        stopped.set()
        child.terminate()
        return {'ok': True}

    monkeypatch.setattr(m, 'select', SimpleNamespace(select=observe_run_wait))
    try:
        with m.Server(str(tmp_path / 'run.sock'), m.Handler) as server:
            server.authority = SimpleNamespace(run=launch_probe, handle=stop_probe)
            thread = threading.Thread(target=server.serve_forever)
            thread.start()
            client = socket.socket(socket.AF_UNIX)
            client.settimeout(5)
            try:
                client.connect(server.server_address)
                client.sendall(b'{"op":"run","action_key":"a","nonce":"b","token":"c"}\n')
                assert launched.wait(5)
                readers, timeout = calls.get(timeout=5)
                assert timeout is None, 'run handler still wakes on a fixed tick'
                assert len(readers) == 2, 'listener stop must not cancel an active run'
                if ending == 'exit':
                    child.stdin.write(b'x')
                    child.stdin.flush()
                    answer = json.loads(client.makefile('rb').readline())
                    assert answer == {'ok': True, 'returncode': 0}
                    assert not stopped.is_set()
                elif ending in ('disconnect', 'unexpected'):
                    if ending == 'disconnect':
                        client.close()
                    else:
                        client.sendall(b'x')
                    assert stopped.wait(5), 'client EOF/data did not stop the exact run'
                else:
                    server.shutdown()
                    server.server_close()
                    child.stdin.write(b'x')
                    child.stdin.flush()
                    answer = json.loads(client.makefile('rb').readline())
                    assert answer == {'ok': True, 'returncode': 0}
                    assert not stopped.is_set(), 'listener shutdown cancelled its payload'
            finally:
                client.close()
                server.shutdown()
                thread.join(5)
        assert child.wait(timeout=5) is not None
    finally:
        if child.poll() is None:
            child.kill()
        child.wait(timeout=5)
        child.stdin.close()


def test_metrics_stop_joins_the_inflight_scrape(tmp_path, monkeypatch):
    servers = queue.Queue()
    entered = threading.Event()
    release = threading.Event()
    closed = threading.Event()
    base = pbmetrics.ThreadingHTTPServer

    class JoiningHTTPServer(base):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            servers.put(self)

        def server_close(self):
            super().server_close()
            closed.set()

    def blocked_scrape(*args, **kwargs):
        entered.set()
        assert release.wait(5)
        return 'joined_metric 1\n'

    monkeypatch.setattr(pbmetrics, 'ThreadingHTTPServer', JoiningHTTPServer)
    monkeypatch.setattr(pbmetrics, 'collect_metrics', blocked_scrape)
    monkeypatch.setattr(role_session, 'read_maintenance_gate', lambda: None)
    stop = role_session.StopRequest()
    args = pbmetrics.parser().parse_args(['--queue-root', str(tmp_path), '--port', '0'])
    thread = threading.Thread(target=pbmetrics._serve_requests,
                              args=(args,), kwargs={'refused': 75, 'stop': stop})
    thread.start()
    connection = None
    try:
        server = servers.get(timeout=5)
        connection = http.client.HTTPConnection(*server.server_address, timeout=5)
        connection.request('GET', '/metrics')
        assert entered.wait(5)
        stop.set()
        assert closed.wait(5), 'stop did not close the listener'
        assert thread.is_alive(), 'stop returned while a scrape was still collecting'
        release.set()
        assert connection.getresponse().read() == b'joined_metric 1\n'
        thread.join(5)
        assert not thread.is_alive()
    finally:
        release.set()
        stop.set()
        thread.join(5)
        if connection is not None:
            connection.close()
        stop._close()


def test_broker_close_retains_main_daemon_request_semantics(listener_broker, tmp_path):
    m = listener_broker
    entered = threading.Event()
    release = threading.Event()
    handlers = []

    class HeldHandler(m.Handler):
        def handle(self):
            handlers.append(threading.current_thread())
            entered.set()
            assert release.wait(5)

    server = m.Server(str(tmp_path / 'daemon.sock'), HeldHandler)
    listener = threading.Thread(target=server.serve_forever)
    listener.start()
    client = socket.socket(socket.AF_UNIX)
    try:
        client.connect(server.server_address)
        assert entered.wait(5)
        server.shutdown()
        listener.join(5)
        server.server_close()
        assert handlers[0].daemon and handlers[0].is_alive()
    finally:
        release.set()
        client.close()
        server.shutdown()
        listener.join(5)
        for handler in handlers:
            handler.join(5)
        server.server_close()


def test_metrics_cache_still_expires_on_the_next_scrape_at_ten_seconds(tmp_path, monkeypatch):
    now = [100.0]
    collected = []

    def collect_probe(*args, **kwargs):
        collected.append(now[0])
        return f'cached_metric {len(collected)}\n'

    monkeypatch.setattr(pbmetrics, 'time', SimpleNamespace(monotonic=lambda: now[0]))
    monkeypatch.setattr(pbmetrics, 'collect_metrics', collect_probe)
    assert pbmetrics.DEFAULT_CACHE_SECONDS == 10
    cache = pbmetrics.MetricsCache(tmp_path, pbmetrics.DEFAULT_CACHE_SECONDS, 3600, 500)
    assert cache.get() == 'cached_metric 1\n'
    now[0] = 109.9
    assert cache.get() == 'cached_metric 1\n'
    now[0] = 110
    assert collected == [100.0], 'expiry must not start a background refresh'
    assert cache.get() == 'cached_metric 2\n'
    assert collected == [100.0, 110]


def test_broker_listener_stop_preserves_an_unfinished_request(listener_broker, tmp_path):
    m = listener_broker
    receiving = threading.Event()
    finished = threading.Event()

    class FramingHandler(m.Handler):
        def handle(self):
            receiving.set()
            super().handle()
        def finish(self):
            finished.set()

    with m.Server(str(tmp_path / 'framing.sock'), FramingHandler) as server:
        server.authority = SimpleNamespace(handle=lambda *a: {'ok': True})
        thread = threading.Thread(target=server.serve_forever)
        thread.start()
        client = socket.socket(socket.AF_UNIX)
        client.settimeout(5)
        try:
            client.connect(server.server_address)
            client.sendall(b'{"op":')
            assert receiving.wait(5)
            server.shutdown()
            server.server_close()
            client.sendall(b'"status"}\n')
            assert json.loads(client.recv(4096)) == {'ok': True}
            assert finished.wait(5)
        finally:
            client.close()
            server.shutdown()
            thread.join(5)


def test_metrics_drains_unrelated_native_signal_notifications(
        tmp_path, monkeypatch, listener_wait_calls):
    monkeypatch.setattr(role_session, 'read_maintenance_gate', lambda: None)
    stop = role_session.StopRequest()
    args = pbmetrics.parser().parse_args(['--queue-root', str(tmp_path), '--port', '0'])
    thread = threading.Thread(target=pbmetrics._serve_requests,
                              args=(args,), kwargs={'refused': 75, 'stop': stop})
    thread.start()
    try:
        listener_wait_calls.get(timeout=5)
        os.write(stop._write_fd, b'x')  # Native wakeup without a requested stop.
        listener_wait_calls.get(timeout=5)
        assert not select.select([stop._read_fd], [], [], 0)[0]
        assert thread.is_alive()
    finally:
        stop.set()
        thread.join(5)
        assert not thread.is_alive()
        stop._close()
