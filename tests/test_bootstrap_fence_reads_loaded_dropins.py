"""The bootstrap fence is proven from systemd's loaded drop-ins, not Conditions text.

The readbacks below mirror `systemctl show` on the fleet: systemd 255 (sparky,
sparklina) and 259 (dl380g10) print `Conditions=[unprintable]`.
"""
import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
PLAN = {'id': 'fence-case'}


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
    return module


def readbacks(helper, **changes):
    _, drops = helper.fence_paths(PLAN)
    table = {unit: {'MainPID': '0', 'ActiveState': 'inactive', 'Job': '', 'UnitFileState': 'enabled',
                    'DropInPaths': str(drop), 'NeedDaemonReload': 'no', 'Conditions': '[unprintable]'}
             for unit, drop in zip(helper.UPGRADE_UNITS, drops)}
    for unit, values in changes.items():
        table[unit].update(values)

    def systemctl(*args):
        assert args[0] == 'show'
        wanted = args[2].removeprefix('--property=').split(',')
        return '\n'.join(key + '=' + table[args[1]][key] for key in wanted if key in table[args[1]])
    return systemctl


def test_loaded_dropins_prove_the_fence_when_conditions_are_unprintable(helper, monkeypatch):
    monkeypatch.setattr(helper, '_bootstrap_systemctl', readbacks(helper))
    assert helper.fenced(PLAN)


@pytest.mark.parametrize('unit, values', [
    ('prismabuild-client-upgrade.timer', {'DropInPaths': ''}),
    ('prismabuild-client-upgrade.service', {'DropInPaths': '/etc/systemd/system/other.conf'}),
    ('prismabuild-client-upgrade.timer', {'NeedDaemonReload': 'yes'}),
    ('prismabuild-client-upgrade.service', {'ActiveState': 'activating', 'MainPID': '812'}),
])
def test_an_unloaded_or_running_upgrader_is_not_fenced(helper, monkeypatch, unit, values):
    monkeypatch.setattr(helper, '_bootstrap_systemctl', readbacks(helper, **{unit: values}))
    assert not helper.fenced(PLAN)
