"""The bridge exposes drain observations without granting maintenance authority."""
import io
import json
import os
from pathlib import Path
import socket
import sys
import threading

import pytest

from test_resource_broker import Backend, auth, container_ticket, create, module
from test_a_settled_tombstone_is_reaped_and_a_live_scope_is_not import _retired


def _query(authority, uid=None, **extra):
    return authority.handle(os.getuid() if uid is None else uid, os.getpid(),
                            {'op': 'maintenance_query', **extra})


def _snapshot(authority, backend):
    return (json.dumps(authority.records, sort_keys=True),
            {path.name: path.read_bytes() for path in authority.state_dir.iterdir()},
            json.dumps(backend.groups, sort_keys=True), dict(backend.charge),
            authority.maintenance_path.read_bytes() if authority.maintenance_path.exists() else None)


def test_query_keeps_pending_creation_and_all_administrative_routes_private(tmp_path):
    broker = module()
    backend = Backend()
    authority = broker.Authority(tmp_path / 'broker', os.getuid(), backend, max_memory_bytes=1 << 30)
    request, _ = create(authority)
    authority.handle(0, os.getpid(), {'op': 'maintenance_begin', 'owner': 'cutover'})
    scope = broker.scope_id(request['action_key'], request['nonce'])
    authority.records[scope].pop('released_unix', None)
    authority.records[scope]['pending'] = True
    backend.groups[scope] = {'budget': 64 << 20, 'populated': False}
    before = _snapshot(authority, backend)
    backend.ops.clear()
    for _ in range(2):
        result = _query(authority)
        assert result['draining'] and result['active_scopes'] == 1
        assert result['maintenance_owner'] == 'cutover'
        assert not backend.ops and _snapshot(authority, backend) == before
    for operation in ('maintenance_begin', 'maintenance_end', 'maintenance_force_end',
                      'maintenance_takeover', 'maintenance_status'):
        with pytest.raises(PermissionError):
            authority.handle(os.getuid(), os.getpid(), {'op': operation, 'owner': 'cutover'})
    with pytest.raises(PermissionError):
        _query(authority, uid=os.getuid() + 1)


@pytest.mark.parametrize('extra', [{'proof': {}}, {'owner': 'cutover'}, {'action_key': 'a' * 64}])
def test_query_accepts_no_caller_evidence(tmp_path, extra):
    authority = module().Authority(tmp_path / 'broker', os.getuid(), Backend(), max_memory_bytes=1 << 30)
    with pytest.raises(ValueError, match='field'):
        _query(authority, **extra)


@pytest.mark.parametrize('uid', [0, None])
def test_authorized_query_reports_open_and_closed_gate(tmp_path, uid):
    authority = module().Authority(tmp_path / 'broker', os.getuid(), Backend(), max_memory_bytes=1 << 30)
    assert _query(authority, uid)['draining'] is False
    authority.handle(0, os.getpid(), {'op': 'maintenance_begin', 'owner': 'bridge'})
    result = _query(authority, uid)
    assert result['draining'] and result['health'] and result['active_scopes'] == 0
    assert result['maintenance_owner'] == 'bridge'


def test_retired_container_tombstone_agrees_with_root_drain(tmp_path, monkeypatch):
    # Adopted from review-r8o-bridge-probes.py, N1.
    authority = module().Authority(tmp_path / 'state', os.getuid(), Backend(), max_memory_bytes=1 << 30)
    backend = authority.backend
    request, record = create(authority)
    container_ticket(authority, record, monkeypatch)
    authority.handle(os.getuid(), os.getpid(), auth(request, record, 'stop'))
    assert authority.handle(os.getuid(), os.getpid(), auth(request, record, 'release'))['retired']
    root = authority.handle(0, os.getpid(), {'op': 'maintenance_begin', 'owner': 'bridge'})
    before = _snapshot(authority, backend)
    backend.ops.clear()
    result = _query(authority)
    assert result['active_scopes'] == root['active_scopes'] == 0
    assert result['health'] == root['health'] is True
    assert not backend.ops and _snapshot(authority, backend) == before


@pytest.mark.parametrize('vanished', [False, True])
def test_query_classifies_settled_tombstone_without_reaping_or_rewriting(tmp_path, monkeypatch, vanished):
    authority = module().Authority(tmp_path / 'state', os.getuid(), Backend(), max_memory_bytes=1 << 30)
    backend = authority.backend
    _, _, scope = _retired(authority, backend, monkeypatch)
    if vanished:
        backend.groups.pop(scope)
    before = _snapshot(authority, backend)
    for _ in range(2):
        result = _query(authority)
        assert result['health'] and result['active_scopes'] == 0
        assert not backend.ops and _snapshot(authority, backend) == before
    root = authority.handle(0, os.getpid(), {'op': 'maintenance_status'})
    assert root['active_scopes'] == result['active_scopes']
    assert authority.records[scope]['released_unix'] > 0


@pytest.mark.parametrize('fault', ['populated', 'unfrozen', 'identity', 'unknown', 'inventory', 'monitor',
                                  'vanished_unsettled', 'vanished_reappears'])
def test_query_preserves_root_refusals_without_mutation(tmp_path, monkeypatch, fault):
    authority = module().Authority(tmp_path / 'state', os.getuid(), Backend(), max_memory_bytes=1 << 30)
    backend = authority.backend
    _, _, scope = _retired(authority, backend, monkeypatch)
    if fault == 'populated':
        backend.groups[scope]['populated'] = True
    elif fault == 'unfrozen':
        backend.stopped.clear()
    elif fault == 'identity':
        backend.identity[scope] = [64, 999999]
    elif fault == 'unknown':
        backend.groups['foreign.scope'] = {'populated': False, 'budget': 1}
    elif fault == 'inventory':
        monkeypatch.setattr(backend, 'inventory', lambda: (_ for _ in ()).throw(OSError('unreadable inventory')))
    elif fault == 'monitor':
        authority.health_check = lambda: False
    else:
        backend.groups.pop(scope)
        if fault == 'vanished_unsettled':
            authority.records[scope].pop('settled_unix')
        else:
            monkeypatch.setattr(backend, 'exists', lambda name: True)
    before = _snapshot(authority, backend)
    result = _query(authority)
    assert result['active_scopes'] > 0 or not result['health']
    assert not backend.ops and _snapshot(authority, backend) == before


def test_query_uses_socket_peer_credentials(tmp_path):
    broker = module()
    authority = broker.Authority(tmp_path / 'state', os.getuid(), Backend(), max_memory_bytes=1 << 30)
    directory = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        endpoint = f'/proc/self/fd/{directory}/bridge.sock'
        with broker.Server(endpoint, broker.Handler) as server:
            server.authority = authority
            thread = threading.Thread(target=server.serve_forever)
            thread.start()
            try:
                with socket.socket(socket.AF_UNIX) as client:
                    client.settimeout(5)
                    client.connect(endpoint)
                    client.sendall(b'{"op":"maintenance_query"}\n')
                    with client.makefile('rb') as reply:
                        result = json.loads(reply.readline())
                assert result['ok'] and result['health'] and result['active_scopes'] == 0
            finally:
                server.shutdown()
                thread.join(timeout=5)
                assert not thread.is_alive()
    finally:
        os.close(directory)


@pytest.mark.parametrize('operation,reason,healthy', [
    ('begin', None, True), ('begin', 'fleet cutover', True),
    ('begin', '', True), ('begin', None, False), ('end', None, True),
])
def test_local_root_gate_client_sends_only_owned_operation(tmp_path, monkeypatch, capsys, operation, reason, healthy):
    broker = module()
    authority = broker.Authority(tmp_path / 'state', os.getuid(), Backend(), max_memory_bytes=1 << 30)
    if operation == 'end':
        authority.handle(0, os.getpid(), {'op': 'maintenance_begin', 'owner': 'bridge'})
    authority.health_check = lambda: healthy
    trusted, requests = [], []

    class RootClient:
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def settimeout(self, value): assert value == 30
        def connect(self, path): assert path == '/fake/local-broker.sock'
        def sendall(self, raw):
            request = json.loads(raw)
            requests.append(request)
            self.response = authority.handle(0, os.getpid(), request)
        def makefile(self, mode): return io.BytesIO(json.dumps(self.response).encode() + b'\n')

    monkeypatch.setattr(broker.os, 'geteuid', lambda: 0)
    monkeypatch.setattr(broker, '_trusted_file', lambda path: trusted.append(path) or path)
    monkeypatch.setattr(broker.socket, 'socket', lambda *args: RootClient())
    monkeypatch.setattr(sys, 'argv', ['resource_broker.py', '--socket', '/fake/local-broker.sock',
                                    '--maintenance-' + operation, 'bridge',
                                    *(['--reason', reason] if reason is not None else [])])
    broker.main()
    assert trusted == [Path(broker.__file__).resolve()]
    expected = {'op': 'maintenance_' + operation, 'owner': 'bridge'}
    if operation == 'begin':
        expected['reason'] = reason or 'operator maintenance'
        assert authority.maintenance['reason'] == expected['reason']
        assert json.loads(authority.maintenance_path.read_bytes())['reason'] == expected['reason']
    assert requests == [expected]
    result = json.loads(capsys.readouterr().out)
    assert result['draining'] is (operation == 'begin') and result['health'] is healthy
    if not healthy:
        assert result['errors'] == ['resource monitor is not healthy']
        assert result['errors_truncated'] is False


def test_local_gate_client_refuses_nonroot_before_rpc(monkeypatch):
    broker = module()
    monkeypatch.setattr(broker.os, 'geteuid', lambda: 1000)
    monkeypatch.setattr(sys, 'argv', ['resource_broker.py', '--maintenance-begin', 'bridge'])
    with pytest.raises(SystemExit, match='root'):
        broker.main()


def test_local_gate_client_refuses_untrusted_install_before_rpc(monkeypatch):
    broker = module()
    monkeypatch.setattr(broker.os, 'geteuid', lambda: 0)
    monkeypatch.setattr(sys, 'argv', ['resource_broker.py', '--maintenance-begin', 'bridge'])
    monkeypatch.setattr(broker, '_trusted_file', lambda path: (_ for _ in ()).throw(ValueError('untrusted local helper')))
    with pytest.raises(ValueError, match='untrusted'):
        broker.main()


def test_query_returns_only_documented_fields(tmp_path):
    authority = module().Authority(tmp_path / 'state', os.getuid(), Backend(), max_memory_bytes=1 << 30)
    assert set(_query(authority)) == {
        'ok', 'draining', 'maintenance_owner', 'active_scopes', 'active_scopes_truncated',
        'health', 'errors', 'errors_truncated', 'maintenance_protocol',
    }


def test_ending_maintenance_rechecks_root_health_after_healthy_query(tmp_path, monkeypatch):
    authority = module().Authority(tmp_path / 'state', os.getuid(), Backend(), max_memory_bytes=1 << 30)
    _, _, scope = _retired(authority, authority.backend, monkeypatch)
    monkeypatch.setattr(authority.backend, 'release',
                        lambda scope: (_ for _ in ()).throw(OSError('reap busy')))
    authority.handle(0, os.getpid(), {'op': 'maintenance_begin', 'owner': 'bridge'})
    root = authority.handle(0, os.getpid(), {'op': 'maintenance_status'})
    assert not root['health'] and root['errors']
    query = _query(authority)
    assert query['health'] and query['active_scopes'] == 0
    with pytest.raises(ValueError, match='not healthy'):
        authority.handle(0, os.getpid(), {'op': 'maintenance_end', 'owner': 'bridge'})
    assert authority.maintenance['draining'] and scope in authority.backend.groups
