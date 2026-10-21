"""#1895 live Unix HTTP stream fixtures, without contacting a host daemon."""
import json
import queue
import secrets
import socket
import threading

import pytest

from prismabuild import gpu_capacity
from test_gpu_container_runtime import ERRORS, MISSING, daemon_row, runtime_authority

__all__ = ['runtime_authority']


class LaunchEventDaemon:
    def __init__(self, broker, backend, row, ticket, *, removed=False, nvidia_injection=False):
        self.row = row
        self.inspected = threading.Event()
        self.events = queue.SimpleQueue()
        self.errors = []
        self.endpoint = '\0pb1895-' + secrets.token_hex(16)
        self.server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.server.bind(self.endpoint)
        self.server.listen()
        def inspect(cid):
            assert cid == row['Id']
            self.inspected.set()
            if removed: raise ValueError('No such container')
            return row
        backend.inspect_container = inspect
        self.thread = threading.Thread(target=self.serve, daemon=True)
        self.thread.start()
        self.watch = broker.DockerLaunchWatch(backend, ticket, socket_path=self.endpoint,
            scope=row['Config']['Labels']['prismabuild.scope'], nvidia_injection=nvidia_injection)
        self.ticket = ticket

    def serve(self):
        try:
            with self.server.accept()[0] as client:
                request = b''
                while not request.endswith(b'\r\n\r\n'):
                    data = client.recv(8192)
                    if not data: return
                    request += data
                assert b'since=' in request
                assert b'prismabuild.shim' in request
                client.sendall(b'HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n')
                while True:
                    event = self.events.get()
                    if event is None: return
                    client.sendall(json.dumps(event).encode() + b'\n')
        except OSError:
            pass
        except BaseException as exc:
            self.errors.append(exc)

    def emit(self, action, *, cid=None):
        self.events.put({'Type': 'container', 'Action': action, 'Actor': {
            'ID': cid or self.row['Id'], 'Attributes': {'prismabuild.shim': self.ticket, 'prismabuild.scope': self.row['Config']['Labels']['prismabuild.scope'], 'exitCode': '125'}}})

    def close(self):
        self.events.put(None)
        self.watch.close()
        self.thread.join(timeout=5)
        self.server.close()
        assert not self.thread.is_alive()
        assert not self.errors


@pytest.mark.parametrize('history', [[], ['start'], ['start', 'die'], ['die']])
def test_auto_remove_fault_needs_daemon_proof_that_payload_never_started(runtime_authority, monkeypatch, history):
    import os
    from test_resource_broker import create, container_ticket
    broker, authority, backend, runtime = runtime_authority
    _, record = create(authority)
    ticket = container_ticket(authority, record, monkeypatch)
    row = daemon_row(record)
    row['Config']['Labels']['prismabuild.shim'] = ticket
    daemon = LaunchEventDaemon(broker, backend, row, ticket)
    try:
        authority.launch_watches[ticket] = daemon.watch
        daemon.emit('create')
        assert daemon.inspected.wait(5)
        for action in history: daemon.emit(action)
        daemon.emit('destroy')
        def removed(cid): raise ValueError('No such container')
        backend.inspect_container = removed
        reply = authority.handle(os.getuid(), os.getpid(), {'op': 'container_outcome',
            'scope_id': record['scope_id'], 'ticket': ticket, 'container_id': row['Id'],
            'daemon_stderr': 'docker: Error response from daemon: ' + ERRORS[0]['daemon_error']})
        assert reply['launch_outcome']['kind'] == ('host_fault' if not history else 'payload_exit')
        assert runtime.state['latched'] is (not history)
        if not history and daemon.watch.row is None:
            assert runtime.state['launch_evidence']['inspect_snapshot'] is None
            assert runtime.state['launch_evidence']['container_id'] == row['Id']
    finally:
        daemon.close()


@pytest.mark.parametrize('missing', ['create', 'stream', 'identity', 'scope', 'duplicate_create', 'error'])
def test_missing_or_mismatched_daemon_proof_cannot_fence(runtime_authority, missing):
    broker, _, backend, _ = runtime_authority
    row = daemon_row({'scope_id': 'fixture'})
    daemon = LaunchEventDaemon(broker, backend, row, 'a'*64)
    try:
        if missing == 'scope': row['Config']['Labels']['prismabuild.scope'] = 'other-scope'
        if missing != 'create':
            daemon.emit('create')
            if missing != 'scope': assert daemon.inspected.wait(5)
        if missing == 'duplicate_create': daemon.emit('create')
        if missing == 'stream': daemon.events.put(None)
        else: daemon.emit('destroy', cid='e'*64 if missing == 'identity' else None)
        assert daemon.watch.never_started(row['Id'], '' if missing == 'error' else
            'docker: Error response from daemon: ' + ERRORS[0]['daemon_error']) is None
    finally:
        daemon.close()


@pytest.mark.parametrize('code', [0, 1, 125, 126, 127])
@pytest.mark.parametrize('oom', [False, True])
def test_payload_exit_and_spoofed_daemon_error_never_fence(code, oom):
    row = daemon_row({'scope_id': 'fixture'}, ERRORS[0]['daemon_error'], started=True)
    row['State'].update(ExitCode=code, OOMKilled=oom)
    outcome = gpu_capacity.container_launch_outcome(row, cdi_sources={MISSING})
    assert outcome.kind == 'payload_exit'
    assert outcome.payload_returncode == code


@pytest.mark.parametrize('history', [[], ['start'], ['die']])
@pytest.mark.parametrize('error', [ERRORS[0]['daemon_error'],
    'OCI runtime create failed: nvidia-container-cli: initialization error: nvml error: driver not loaded'])
def test_fast_auto_remove_before_inspection_still_classifies_exact_launch(runtime_authority, monkeypatch, history, error):
    import os
    from test_resource_broker import create, container_ticket
    broker, authority, backend, runtime = runtime_authority
    _, record = create(authority)
    ticket = container_ticket(authority, record, monkeypatch)
    row = daemon_row(record)
    row['Config']['Labels']['prismabuild.shim'] = ticket
    daemon = LaunchEventDaemon(broker, backend, row, ticket, removed=True, nvidia_injection=True)
    try:
        authority.launch_watches[ticket] = daemon.watch
        daemon.emit('create')
        for action in history: daemon.emit(action)
        daemon.emit('destroy')
        reply = authority.handle(os.getuid(), os.getpid(), {'op': 'container_outcome',
            'scope_id': record['scope_id'], 'ticket': ticket, 'container_id': row['Id'],
            'daemon_stderr': 'docker: Error response from daemon: ' + error})
        assert reply['launch_outcome']['kind'] == ('payload_exit' if history else 'host_fault')
        assert runtime.state['latched'] is (not history)
        if not history and daemon.watch.row is None:
            assert runtime.state['launch_evidence']['inspect_snapshot'] is None
            assert runtime.state['launch_evidence']['container_id'] == row['Id']
    finally:
        daemon.close()
