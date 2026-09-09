"""A maintenance drain is a claim with a holder, and only the holder ends it.

The gate at /run/prismabuild/maintenance.json is the fleet's only stop, and
every caller of it is root, so the caller UID cannot tell the periodic client
upgrade apart from a person who stopped a host deliberately. These tests pin
the holder that does tell them apart, and they pin the mixed-version window it
has to survive: the client upgrade converges host by host on a ~60s timer, so a
client that knows about holders talks to a broker that does not, and a broker
that knows about them answers a client that does not.
"""
import importlib.util
import json
import os
from pathlib import Path
import subprocess

import pytest

REPO = Path(__file__).resolve().parents[1]
BROKER = REPO / 'tools/fleet/resource_broker.py'
CLIENT = REPO / 'tools/fleet/upgrade_client.py'
#: The merge this change branches from, whose broker predates drain holders.
#: The PB Git snapshot carries it, as the legacy client upgrade test relies on.
BEFORE_HOLDERS = '1c10a68'


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


broker_module = load('resource_broker', BROKER)
upgrade = load('upgrade_client', CLIENT)


class Backend:
    """Only what a maintenance sweep asks of a host that owns no scopes."""

    def healthy(self):
        return True

    def inventory(self):
        return {}

    def exists(self, scope):
        return False


def authority(state_dir, module=broker_module):
    return module.Authority(state_dir, os.getuid(), Backend(), max_memory_bytes=1024 ** 3)


def before_holders(tmp_path):
    """The real broker as it shipped before drains had holders."""
    source = subprocess.check_output(
        ['git', 'show', BEFORE_HOLDERS + ':tools/fleet/resource_broker.py'], cwd=REPO)
    path = tmp_path / 'resource_broker_before_holders.py'
    path.write_bytes(source)
    return load('resource_broker_before_holders', path)


def admin(a, **request):
    return a.handle(0, os.getpid(), request)


def gate(a):
    return json.loads(a.maintenance_path.read_text())


def socket_rpc(current):
    """Drive a live Authority the way the client's socket transport does.

    Handler.handle turns a refusal into {'ok': False, 'error': ...} and
    request() raises RuntimeError from that reply, so a client under test sees
    a refusal in exactly the shape the wire gives it. `current` is a list so a
    test can replace the running broker between two calls, which is what a
    client upgrade does to its own host.
    """
    def rpc(endpoint, operation, **fields):
        try:
            reply = current[0].handle(0, os.getpid(), {'op': operation, **fields})
        except (ValueError, PermissionError, OSError, KeyError, TypeError) as exc:
            reply = {'ok': False, 'error': str(exc)[:1500]}
        if reply.get('ok') is not True:
            raise RuntimeError(reply.get('error', 'maintenance operation refused'))
        return reply
    return rpc


def upgrader(tmp_path, current):
    return upgrade.Upgrader(dict(install_dir=str(tmp_path / 'installed'),
                                 state_dir=str(tmp_path / 'state')),
                            rpc=socket_rpc(current), sleep=lambda _: None)


@pytest.fixture
def broker(tmp_path):
    return authority(tmp_path / 'state')


def test_beginning_a_drain_that_is_already_open_keeps_the_stated_reason(broker):
    admin(broker, op='maintenance_begin', reason='rob is replacing a disk')
    stated = gate(broker)
    admin(broker, op='maintenance_begin')
    assert gate(broker)['reason'] == 'rob is replacing a disk'
    assert gate(broker)['changed_unix'] == stated['changed_unix']
    assert broker.maintenance['draining'] is True


def test_a_stated_holder_is_the_only_caller_that_reopens_admission(broker):
    admin(broker, op='maintenance_begin', reason='rob is replacing a disk', owner='rob')
    for request in ({'owner': upgrade.MAINTENANCE_OWNER}, {}):
        with pytest.raises(PermissionError, match='held by rob'):
            admin(broker, op='maintenance_end', **request)
    assert broker.maintenance['draining'] is True
    assert admin(broker, op='maintenance_end', owner='rob')['draining'] is False
    assert gate(broker)['draining'] is False


def test_a_stated_holder_is_not_taken_over_by_another_caller_beginning(broker):
    admin(broker, op='maintenance_begin', reason='rob is replacing a disk', owner='rob')
    stated = gate(broker)
    with pytest.raises(PermissionError, match='held by rob'):
        admin(broker, op='maintenance_begin', owner=upgrade.MAINTENANCE_OWNER)
    assert gate(broker) == stated


def test_forcing_a_held_drain_open_records_what_it_took(broker):
    admin(broker, op='maintenance_begin', reason='rob is replacing a disk', owner='rob')
    status = admin(broker, op='maintenance_force_end', owner='operator-console')
    assert status['draining'] is False
    assert gate(broker)['forced_end_of'] == 'rob'
    assert gate(broker)['forced_end_by'] == 'operator-console'
    assert admin(broker, op='maintenance_status')['draining'] is False


def test_a_drain_nobody_claimed_stays_releasable_by_any_caller(broker):
    # Deliberate, and the reason a mixed-version fleet cannot deadlock: a client
    # that predates holders cannot name itself, so the drain it opens carries no
    # claim, and the newer broker underneath it must still let it reopen.
    admin(broker, op='maintenance_begin')
    assert admin(broker, op='maintenance_status')['maintenance_owner'] == 'unattributed'
    assert admin(broker, op='maintenance_end', owner=upgrade.MAINTENANCE_OWNER)['draining'] is False


def test_status_names_a_holder_only_while_that_drain_is_open(broker):
    idle = admin(broker, op='maintenance_status')
    assert idle['maintenance_protocol'] >= 2 and 'maintenance_owner' not in idle
    assert admin(broker, op='maintenance_begin', owner='rob')['maintenance_owner'] == 'rob'
    assert 'maintenance_owner' not in admin(broker, op='maintenance_end', owner='rob')


@pytest.mark.parametrize('request_fields', [
    {'op': 'maintenance_begin', 'owner': ''},
    {'op': 'maintenance_begin', 'owner': 7},
    {'op': 'maintenance_begin', 'holder': 'rob'},
    {'op': 'maintenance_status', 'owner': 'rob'},
    {'op': 'maintenance_end', 'reason': 'not an end field'},
    {'op': 'maintenance_force_end', 'reason': 'not an end field'},
])
def test_maintenance_fields_stay_strictly_validated(broker, request_fields):
    with pytest.raises(ValueError, match='invalid maintenance fields'):
        broker.handle(0, os.getpid(), request_fields)
    assert not broker.maintenance['draining']


def test_forcing_a_drain_open_still_requires_root(broker):
    admin(broker, op='maintenance_begin', owner='rob')
    with pytest.raises(PermissionError, match='root'):
        broker.handle(1000, os.getpid(), {'op': 'maintenance_force_end', 'owner': 'rob'})
    assert broker.maintenance['draining'] is True


def test_a_client_without_holders_opens_and_closes_its_own_drain(broker):
    # The exact requests the client upgrade sent before holders existed. A new
    # broker answering an old client on the same host must complete this.
    assert admin(broker, op='maintenance_begin')['draining'] is True
    assert admin(broker, op='maintenance_status')['draining'] is True
    assert admin(broker, op='maintenance_end')['draining'] is False


def test_a_broker_without_holders_still_reads_a_claimed_gate(tmp_path):
    state = tmp_path / 'state'
    admin(authority(state), op='maintenance_begin', reason='rob is replacing a disk', owner='rob')
    older = authority(state, module=before_holders(tmp_path))
    assert older.maintenance['draining'] is True
    assert older.handle(0, os.getpid(), {'op': 'maintenance_status'})['draining'] is True


def test_a_new_client_never_states_a_holder_to_a_broker_without_them(tmp_path):
    older = [authority(tmp_path / 'state', module=before_holders(tmp_path))]
    client = upgrader(tmp_path, older)
    status = client.open_drain()
    assert status['draining'] is True
    assert 'maintenance_owner' not in status
    assert client.held_elsewhere(status) is None
    assert client.close_drain(status)['draining'] is False


def test_a_drain_opened_before_holders_closes_against_the_broker_that_replaced_it(tmp_path):
    # This is the upgrade transaction itself: the drain opens through the broker
    # being replaced and closes through the one that replaced it.
    state = tmp_path / 'state'
    running = [authority(state, module=before_holders(tmp_path))]
    client = upgrader(tmp_path, running)
    client.open_drain()
    running[0] = authority(state)
    health = client.call('status')
    assert health['maintenance_owner'] == 'unattributed'
    assert client.close_drain(health)['draining'] is False


def test_a_new_client_leaves_a_drain_another_holder_stated(tmp_path):
    running = [authority(tmp_path / 'state')]
    admin(running[0], op='maintenance_begin', reason='rob is replacing a disk', owner='rob')
    client = upgrader(tmp_path, running)
    status = client.open_drain()
    assert client.held_elsewhere(status) == 'rob'
    assert running[0].maintenance['reason'] == 'rob is replacing a disk'
    with pytest.raises(RuntimeError, match='held by rob'):
        client.close_drain(status)
