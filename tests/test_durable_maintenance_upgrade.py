"""A durable broker may only adopt an updater that preserves its downgrade fence."""
import importlib.util
import json
from pathlib import Path

import pytest


TEST = Path(__file__).with_name('test_client_upgrade.py')
spec = importlib.util.spec_from_file_location('client_upgrade_fixture_for_durable', TEST)
fixture = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fixture)


def _set_candidate(generation, name, data):
    path = generation / fixture.upgrade.MEMBERS[name]
    path.write_bytes(data)
    receipt = generation / 'RUNTIME_VERSION.json'
    value = json.loads(receipt.read_text())
    value['files'][fixture.upgrade.MEMBERS[name]] = fixture.upgrade.digest(data)
    receipt.write_text(json.dumps(value))


def _durable_installed(updater):
    (updater.install / 'resource_broker.py').write_bytes(
        b'MAINTENANCE_DURABLE_PROTOCOL = 1\ninstalled durable broker')


def test_durable_broker_refuses_candidate_with_pre_durable_updater(tmp_path):
    updater, backend, generation = fixture.setup.__wrapped__(tmp_path)
    _durable_installed(updater)
    _set_candidate(generation, 'resource_broker.py',
                   b'MAINTENANCE_DURABLE_PROTOCOL = 1\ncandidate durable broker')
    _set_candidate(generation, 'upgrade_client.py', b'CLIENT_UPGRADE_PROTOCOL = 2\nold updater')
    original = updater.rpc
    def rpc(endpoint, operation, **fields):
        reply = original(endpoint, operation, **fields)
        if operation == 'maintenance_status': reply['maintenance_durable_protocol'] = 1
        return reply
    updater.rpc = rpc
    with pytest.raises(ValueError, match='durable'):
        updater.run()
    assert backend.operations == ['maintenance_status']


def test_durable_source_refuses_missing_running_capability(tmp_path):
    updater, backend, generation = fixture.setup.__wrapped__(tmp_path)
    _durable_installed(updater)
    _set_candidate(generation, 'resource_broker.py',
                   b'MAINTENANCE_DURABLE_PROTOCOL = 1\ncandidate durable broker')
    _set_candidate(generation, 'upgrade_client.py', b'CLIENT_UPGRADE_PROTOCOL = 2\nold updater')
    with pytest.raises(ValueError, match='durable'):
        updater.run()
    assert backend.operations == ['maintenance_status']
