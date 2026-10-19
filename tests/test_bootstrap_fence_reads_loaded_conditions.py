"""The bootstrap fence is proven from systemd's typed loaded Conditions.

`systemctl show` prints `Conditions=[unprintable]` on systemd 255 (sparky,
sparklina) and 259 (dl380g10). The busctl replies below are the fleet's actual
`busctl --json=short get-property` output for the fenced and unfenced upgrader.
"""
import importlib.util
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
PLAN = {'id': 'fence-case'}
OBJECTS = {
    '/org/freedesktop/systemd1/unit/prismabuild_2dclient_2dupgrade_2etimer': 'prismabuild-client-upgrade.timer',
    '/org/freedesktop/systemd1/unit/prismabuild_2dclient_2dupgrade_2eservice': 'prismabuild-client-upgrade.service',
}


@pytest.fixture
def helper(tmp_path, monkeypatch):
    spec = importlib.util.spec_from_file_location('fence_bootstrap', ROOT / 'tools/fleet/bootstrap_client.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module, 'STATE', tmp_path / 'state')
    monkeypatch.setattr(module, 'UNITS', tmp_path / 'units')
    monkeypatch.setattr(module, '_bootstrap_trusted', Path)
    block, drops = module.fence_paths(PLAN)
    block.parent.mkdir(parents=True)
    block.write_bytes(b'blocked\n')
    for drop in drops:
        drop.parent.mkdir(parents=True)
        drop.write_bytes(module.fence_content(block))
    monkeypatch.setattr(module, '_bootstrap_systemctl', lambda *args: '\n'.join((
        'MainPID=0', 'ActiveState=inactive', 'Job=', 'UnitFileState=static', 'Conditions=[unprintable]')))
    return module


def systemd(helper, monkeypatch, **conditions):
    block, _ = helper.fence_paths(PLAN)
    loaded = [['ConditionPathExists', False, True, str(block), 0]]

    def busctl(*args):
        assert args[:2] == ('get-property', 'org.freedesktop.systemd1')
        assert args[3:] == ('org.freedesktop.systemd1.Unit', 'Conditions')
        unit = OBJECTS[args[2]]
        return json.dumps({'type': 'a(sbbsi)', 'data': conditions.get(unit, loaded)}, separators=(',', ':'))
    monkeypatch.setattr(helper, '_bootstrap_busctl', busctl)


def test_the_loaded_negated_block_condition_proves_the_fence(helper, monkeypatch):
    systemd(helper, monkeypatch)
    assert helper.fenced(PLAN)


@pytest.mark.parametrize('unit, loaded', [
    ('prismabuild-client-upgrade.timer', []),
    ('prismabuild-client-upgrade.service', [['ConditionPathExists', False, True, '/var/lib/other/blocked', 0]]),
    ('prismabuild-client-upgrade.service', [['ConditionPathExists', False, False, 'BLOCK', 0]]),
])
def test_a_fence_systemd_has_not_loaded_is_not_fenced(helper, monkeypatch, unit, loaded):
    block, _ = helper.fence_paths(PLAN)
    loaded = [[str(block) if field == 'BLOCK' else field for field in row] for row in loaded]
    systemd(helper, monkeypatch, **{unit: loaded})
    assert not helper.fenced(PLAN)


def test_a_running_upgrader_is_not_fenced(helper, monkeypatch):
    systemd(helper, monkeypatch)
    monkeypatch.setattr(helper, '_bootstrap_systemctl', lambda *args: 'MainPID=812\nActiveState=activating\nJob=')
    assert not helper.fenced(PLAN)
