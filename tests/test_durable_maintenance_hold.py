"""Persistent maintenance authority keeps reboot from reopening a named hold."""
import importlib.util
import json
import os
from pathlib import Path

import pytest


SOURCE = Path(__file__).resolve().parents[1] / 'tools/fleet/resource_broker.py'


def broker_module():
    spec = importlib.util.spec_from_file_location('durable_broker', SOURCE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class Backend:
    def __init__(self): self.groups = {}
    def healthy(self): return True
    def inventory(self): return {}
    def exists(self, scope): return scope in self.groups


def authority(tmp_path, module=None):
    module = module or broker_module()
    volatile = tmp_path / 'run' / 'jobs'
    durable = tmp_path / 'var' / 'maintenance.json'
    volatile.parent.mkdir(parents=True, exist_ok=True)
    gate = volatile.parent / 'maintenance.json'
    if not gate.exists():
        gate.write_text(json.dumps({'schema': module.MAINTENANCE_SCHEMA, 'draining': False,
                                    'changed_unix': 1.0, 'reason': 'test initialization'}))
        gate.chmod(0o644)
    return module.Authority(volatile, os.getuid(), Backend(), max_memory_bytes=1024 ** 3,
                            maintenance_state=durable)


def create(authority):
    return authority.handle(os.getuid(), os.getpid(), {
        'op': 'create', 'action_key': 'a' * 64, 'nonce': 'b' * 32,
        'memory_max_bytes': 64 * 1024 ** 2})


def begin(authority, owner='operator'):
    return authority.handle(0, os.getpid(), {
        'op': 'maintenance_begin', 'owner': owner, 'reason': 'durability test'})


def test_reboot_restores_named_hold_after_volatile_state_is_lost(tmp_path):
    current = authority(tmp_path)
    assert current.maintenance_error is None
    begin(current, 'operator-hold')
    durable = current.maintenance_state_path
    evidence = current.maintenance_evidence_path
    assert json.loads(durable.read_text())['owner'] == 'operator-hold'
    for path in (current.state_dir, current.maintenance_path):
        if path.is_dir():
            for child in path.iterdir(): child.unlink()
            path.rmdir()
        else: path.unlink()
    rebooted = authority(tmp_path)
    assert rebooted.maintenance_state_path == durable
    assert rebooted.maintenance_evidence_path == evidence
    assert rebooted.handle(0, os.getpid(), {'op': 'maintenance_status'})['maintenance_owner'] == 'operator-hold'
    assert create(rebooted) == {'ok': False, 'maintenance': True, 'retryable': True,
                                'error': 'resource broker is draining for maintenance'}


def test_foreign_owner_cannot_replace_or_release_a_durable_hold(tmp_path):
    current = authority(tmp_path)
    begin(current, 'operator-hold')
    rebooted = authority(tmp_path)
    with pytest.raises(PermissionError, match='operator-hold'):
        begin(rebooted, 'client-upgrade')
    with pytest.raises(PermissionError, match='operator-hold'):
        rebooted.handle(0, os.getpid(), {'op': 'maintenance_end', 'owner': 'client-upgrade'})


def test_durable_release_restores_open_gate_after_restart(tmp_path):
    current = authority(tmp_path)
    begin(current)
    assert current.handle(0, os.getpid(), {'op': 'maintenance_end', 'owner': 'operator'})['draining'] is False
    rebooted = authority(tmp_path)
    assert rebooted.handle(0, os.getpid(), {'op': 'maintenance_status'})['draining'] is False
    assert json.loads(rebooted.maintenance_path.read_text())['draining'] is False


@pytest.mark.parametrize('operation', ['maintenance_begin', 'maintenance_end'])
def test_failed_durable_transition_keeps_volatile_gate_closed(tmp_path, monkeypatch, operation):
    current = authority(tmp_path)
    if operation == 'maintenance_end': begin(current)
    source = type(current).admin.__globals__
    atomic = source['_atomic']
    def fail(path, value, **kwargs):
        if path == current.maintenance_state_path: raise OSError('durable disk failed')
        return atomic(path, value, **kwargs)
    monkeypatch.setitem(source, '_atomic', fail)
    with pytest.raises(OSError, match='durable disk failed'):
        current.handle(0, os.getpid(), {'op': operation, 'owner': 'operator'} if operation.endswith('end')
                       else {'op': operation, 'owner': 'operator'})
    assert current.maintenance['draining'] is True
    assert json.loads(current.maintenance_path.read_text())['draining'] is True
    assert create(current)['maintenance'] is True


def test_failed_volatile_close_after_durable_hold_removes_open_gate(tmp_path, monkeypatch):
    current = authority(tmp_path)
    source = type(current).admin.__globals__
    atomic = source['_atomic']
    attempts = 0
    def fail(path, value, **kwargs):
        nonlocal attempts
        if path == current.maintenance_path:
            attempts += 1
            raise OSError('run disk failed')
        return atomic(path, value, **kwargs)
    monkeypatch.setitem(source, '_atomic', fail)
    with pytest.raises(OSError, match='run disk failed'): begin(current)
    assert attempts >= 2 and current.maintenance['draining'] is True
    assert not current.maintenance_path.exists()
    assert json.loads(current.maintenance_state_path.read_text())['draining'] is True


@pytest.mark.parametrize('fault', ['missing', 'corrupt'])
def test_missing_or_corrupt_established_durable_evidence_fails_closed(tmp_path, fault):
    current = authority(tmp_path)
    begin(current)
    if fault == 'missing': current.maintenance_state_path.unlink()
    else: current.maintenance_state_path.write_text('not json')
    rebooted = authority(tmp_path)
    status = rebooted.handle(0, os.getpid(), {'op': 'maintenance_status'})
    assert status['draining'] is True and status['health'] is False
    assert create(rebooted)['maintenance'] is True
    assert json.loads(rebooted.maintenance_path.read_text())['draining'] is True


def test_valid_volatile_gate_is_migrated_only_before_durable_initialization(tmp_path):
    module = broker_module()
    state = tmp_path / 'run' / 'jobs'
    gate = state.parent / 'maintenance.json'
    gate.parent.mkdir()
    gate.write_text(json.dumps({'schema': module.MAINTENANCE_SCHEMA, 'draining': True,
                                'changed_unix': 4.0, 'owner': 'legacy-holder'}))
    gate.chmod(0o644)
    durable = tmp_path / 'var' / 'maintenance.json'
    migrated = module.Authority(state, os.getuid(), Backend(), max_memory_bytes=1024 ** 3,
                                maintenance_state=durable)
    assert json.loads(durable.read_text())['owner'] == 'legacy-holder'
    durable.unlink()
    blocked = module.Authority(state, os.getuid(), Backend(), max_memory_bytes=1024 ** 3,
                               maintenance_state=durable)
    assert blocked.maintenance_error == 'durable maintenance evidence is missing'


def test_private_fixture_default_never_reaches_host_persistent_state(tmp_path):
    module = broker_module()
    current = module.Authority(tmp_path / 'private-run' / 'jobs', os.getuid(), Backend(),
                               max_memory_bytes=1024 ** 3)
    assert current.maintenance_state_path == tmp_path / 'private-run' / 'maintenance.json'
