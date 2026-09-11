"""Boot admission waits for the updater's current, healthy client proof.

The fake broker writes only a private gate; no host service or gate is touched.
"""
import json
import os
from pathlib import Path

import pytest

from test_client_upgrade import CLIENT, setup, upgrade
from test_resource_broker import Backend, module as broker_module
from test_worker_loop_proves_it_parked import _worker_loop


@pytest.fixture
def current_host(setup, tmp_path):
    updater, broker, generation = setup
    for name, member in upgrade.MEMBERS.items():
        (updater.install / name).write_bytes((generation / member).read_bytes())
    updater.gate = tmp_path / 'run' / 'maintenance.json'
    updater.gate.parent.mkdir(exist_ok=True)
    updater.gate.unlink(missing_ok=True)
    updater.parked_root = updater.gate.parent / 'rollout' / 'parked'
    updater.procs = lambda: []
    original = updater.rpc

    def rpc(endpoint, operation, **fields):
        reply = original(endpoint, operation, **fields)
        if operation in ('maintenance_begin', 'maintenance_end'):
            updater.gate.write_text(json.dumps({
                'schema': 'prismabuild.resource-maintenance.v1',
                'draining': reply['draining'], 'changed_unix': 458.0,
                **({'owner': reply['maintenance_owner']}
                   if 'maintenance_owner' in reply else {}),
            }))
        return reply

    updater.rpc = rpc
    return updater, broker


def test_current_healthy_clients_materialize_an_open_boot_gate(current_host):
    updater, broker = current_host
    result = updater.run()
    assert result['state'] == 'current'
    assert updater.gate.is_file(), 'no explicit gate was published after current-client proof'
    assert json.loads(updater.gate.read_text())['draining'] is False
    assert broker.operations == [
        'maintenance_status', 'maintenance_status',
        'maintenance_begin', 'maintenance_end',
    ]
    broker.operations.clear()
    assert updater.run()['state'] == 'current'
    assert broker.operations == ['maintenance_status']


def test_boot_initialization_preserves_active_scopes_across_ticks(current_host):
    updater, broker = current_host
    broker.active = 1
    assert updater.run()['state'] == 'draining'
    assert broker.holder == CLIENT
    assert json.loads(updater.gate.read_text())['draining'] is True
    broker.operations.clear()
    assert updater.run()['state'] == 'draining'
    assert broker.operations == ['maintenance_status']
    broker.active = 0
    assert updater.run()['state'] == 'current'
    assert json.loads(updater.gate.read_text())['draining'] is False


def test_unhealthy_current_clients_leave_a_missing_gate_closed(current_host):
    updater, broker = current_host
    broker.fail_health = True
    with pytest.raises(RuntimeError, match='unhealthy or stale'):
        updater.run()
    assert not updater.gate.exists()
    assert broker.operations == ['maintenance_status']


def test_a_foreign_hold_arriving_during_boot_initialization_is_retained(current_host):
    updater, broker = current_host
    original = updater.rpc
    checks = 0

    def rpc(endpoint, operation, **fields):
        nonlocal checks
        if operation == 'maintenance_status':
            checks += 1
            if checks == 2:
                broker.holder = 'operator-stop'
        return original(endpoint, operation, **fields)

    updater.rpc = rpc
    assert updater.run()['state'] == 'held'
    assert broker.holder == 'operator-stop'
    assert 'maintenance_end' not in broker.operations


def test_an_unreadable_gate_is_not_treated_as_absent(current_host, monkeypatch):
    updater, broker = current_host
    original = Path.lstat

    def lstat(path):
        if path == updater.gate:
            raise PermissionError('private gate unavailable')
        return original(path)

    monkeypatch.setattr(Path, 'lstat', lstat)
    with pytest.raises(PermissionError, match='private gate unavailable'):
        updater.run()
    assert broker.operations == ['maintenance_status']


def test_real_broker_and_worker_complete_the_private_boot_handshake(current_host):
    updater, _ = current_host
    authority = broker_module().Authority(updater.gate.parent / 'jobs', os.getuid(),
                                         Backend(), max_memory_bytes=1024 ** 3)
    authority.installed_sha256 = {
        name: value for name, value in updater.installed().items()
        if name != 'upgrade_client.py'
    }
    updater.rpc = lambda endpoint, operation, **fields: authority.admin(
        0, {'op': operation, **fields})
    worker = _worker_loop(updater.gate)
    assert authority.maintenance['draining'] is False  # legacy cold-start shape
    assert worker.maintenance_requested()  # absence is still no admission proof
    assert updater.run()['state'] == 'current'
    assert worker.read_maintenance_gate() is None
    assert authority.maintenance['draining'] is False


@pytest.mark.parametrize('changed', ['health', 'installed_sha256'])
def test_a_broker_change_during_initialization_keeps_the_gate_closed(current_host, changed):
    updater, broker = current_host
    original = updater.rpc

    def rpc(endpoint, operation, **fields):
        reply = original(endpoint, operation, **fields)
        if operation == 'maintenance_begin':
            reply[changed] = False if changed == 'health' else {}
        return reply

    updater.rpc = rpc
    with pytest.raises(RuntimeError, match='unhealthy or stale'):
        updater.run()
    assert broker.holder == CLIENT
    assert json.loads(updater.gate.read_text())['draining'] is True
    assert 'maintenance_end' not in broker.operations


def test_failed_open_mirror_is_not_reported_as_current(current_host):
    updater, _ = current_host
    original = updater.rpc

    def rpc(endpoint, operation, **fields):
        reply = original(endpoint, operation, **fields)
        if operation == 'maintenance_end':
            updater.gate.write_text('{"draining": true}')
        return reply

    updater.rpc = rpc
    with pytest.raises(RuntimeError, match='not explicitly open'):
        updater.run()


@pytest.mark.parametrize('scopes', [None, False, '0'])
def test_an_unknown_scope_count_cannot_open_the_gate(current_host, scopes):
    updater, broker = current_host
    broker.active = scopes
    assert updater.run()['state'] == 'draining'
    assert broker.holder == CLIENT
    assert 'maintenance_end' not in broker.operations
