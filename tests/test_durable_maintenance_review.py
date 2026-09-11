"""Review regressions: storage errors must fence every admission path.

All state lives in pytest's private directory; no real gate, service, or
persistent broker directory is read or modified.
"""
import json
import os
from pathlib import Path
import sys

import pytest

from test_durable_maintenance_hold import Backend, authority, begin, broker_module


def closed(path):
    try:
        return json.loads(path.read_text()).get('draining') is not False
    except FileNotFoundError:
        return True


def restart(module, current):
    return module.Authority(current.state_dir, os.getuid(), Backend(),
                            max_memory_bytes=1024 ** 3,
                            maintenance_state=current.maintenance_state_path)


def test_restored_open_state_still_waits_for_boot_client_verification(tmp_path):
    module = broker_module()
    current = authority(tmp_path, module)
    assert current.maintenance['draining'] is False
    current.maintenance_path.unlink()  # /run was cleared at boot.
    rebooted = restart(module, current)
    assert closed(current.maintenance_path), 'durable open state bypassed the boot updater check'
    assert rebooted.maintenance['draining'] is True


def test_boot_hold_releases_only_after_real_updater_health_checks(tmp_path):
    from test_client_upgrade import setup as client_setup, upgrade

    module = broker_module()
    current = authority(tmp_path / 'broker', module)
    current.maintenance_path.unlink()
    rebooted = restart(module, current)
    assert rebooted.maintenance['owner'] == upgrade.MAINTENANCE_OWNER
    client = tmp_path / 'client'
    client.mkdir()
    updater, _, generation = client_setup.__wrapped__(client)
    for name, member in upgrade.MEMBERS.items():
        (updater.install / name).write_bytes((generation / member).read_bytes())
    updater.gate = rebooted.maintenance_path
    updater.procs = lambda: []
    updater.rpc = lambda endpoint, operation, **fields: rebooted.admin(0, {'op': operation, **fields})
    rebooted.installed_sha256 = {name: value for name, value in updater.installed().items()
                                  if name != 'upgrade_client.py'}
    rebooted.health_check = lambda: False
    with pytest.raises(RuntimeError, match='unhealthy or stale'):
        updater.run()
    assert closed(updater.gate)
    rebooted.health_check = lambda: True
    result = updater.run()
    assert result['state'] == 'current'
    assert result['maintenance_durable_protocol'] == 1
    assert rebooted.maintenance['draining'] is False
    assert json.loads(rebooted.maintenance_state_path.read_text())['draining'] is False
    assert not closed(updater.gate)


@pytest.mark.parametrize('fault', ['missing', 'corrupt', 'symlink', 'writable', 'unreadable'])
def test_bad_initialized_marker_cannot_recreate_open_authority(tmp_path, monkeypatch, fault):
    module = broker_module()
    current = authority(tmp_path, module)
    marker = current.maintenance_evidence_path
    if fault == 'missing':
        marker.unlink()
    elif fault == 'corrupt':
        marker.write_text('not json')
    elif fault == 'symlink':
        marker.unlink()
        marker.symlink_to('absent')
    elif fault == 'writable':
        marker.chmod(0o666)
    else:
        lstat = Path.lstat

        def unavailable(path, *args, **kwargs):
            if path == marker:
                raise PermissionError('marker metadata unavailable')
            return lstat(path, *args, **kwargs)

        monkeypatch.setattr(Path, 'lstat', unavailable)
    rebooted = restart(module, current)
    assert rebooted.maintenance['draining'] is True
    assert rebooted._maintenance_status()['health'] is False
    assert closed(current.maintenance_path)


def test_restore_mirror_failure_also_fences_broker_scope_creation(tmp_path, monkeypatch):
    module = broker_module()
    current = authority(tmp_path, module)
    atomic = module._atomic

    def fail_mirror(path, value, **kwargs):
        if path == current.maintenance_path:
            raise OSError('mirror filesystem unavailable')
        return atomic(path, value, **kwargs)

    monkeypatch.setattr(module, '_atomic', fail_mirror)
    rebooted = restart(module, current)
    assert closed(current.maintenance_path)
    status = rebooted.handle(0, os.getpid(), {'op': 'maintenance_status'})
    assert status['draining'] is True, 'closed worker mirror must also fence broker creates'


def test_unreadable_durable_parent_closes_existing_open_worker_gate(tmp_path, monkeypatch):
    module = broker_module()
    current = authority(tmp_path, module)

    def unavailable(self):
        raise PermissionError('persistent directory unavailable')

    monkeypatch.setattr(module.Authority, '_private_maintenance_parent', unavailable)
    try:
        restart(module, current)
    except (OSError, ValueError):
        pass
    assert closed(current.maintenance_path), 'startup failure left uncontained admission open'


@pytest.mark.parametrize('which', ['marker', 'canonical'])
def test_first_migration_write_failure_closes_legacy_open_gate(tmp_path, monkeypatch, which):
    module = broker_module()
    jobs = tmp_path / 'run' / 'jobs'
    gate = jobs.parent / 'maintenance.json'
    gate.parent.mkdir()
    gate.write_text(json.dumps({'schema': module.MAINTENANCE_SCHEMA,
                                'draining': False, 'changed_unix': 1.0}))
    gate.chmod(0o644)
    state = tmp_path / 'var' / 'maintenance.json'
    broken = state if which == 'canonical' else state.with_name(state.name + '.initialized')
    atomic = module._atomic

    def fail_migration(path, value, **kwargs):
        if path == broken:
            raise OSError('initial persistence failed')
        return atomic(path, value, **kwargs)

    monkeypatch.setattr(module, '_atomic', fail_migration)
    try:
        module.Authority(jobs, os.getuid(), Backend(), max_memory_bytes=1024 ** 3,
                         maintenance_state=state)
    except (OSError, ValueError):
        pass
    assert closed(gate), 'failed initial commit left legacy gate open'


def test_error_after_open_mirror_replace_recloses_the_visible_gate(tmp_path, monkeypatch):
    module = broker_module()
    current = authority(tmp_path, module)
    begin(current)
    atomic = module._atomic

    def fail_after_replace(path, value, **kwargs):
        atomic(path, value, **kwargs)
        if path == current.maintenance_path and value.get('draining') is False:
            # The namespace move succeeded; directory fsync/cleanup then failed.
            raise OSError('directory fsync failed after open replace')

    monkeypatch.setattr(module, '_atomic', fail_after_replace)
    with pytest.raises(OSError, match='directory fsync'):
        current.handle(0, os.getpid(), {'op': 'maintenance_end', 'owner': 'operator'})
    assert current.maintenance['draining'] is True
    assert closed(current.maintenance_path), 'failed release exposed an open worker gate'


@pytest.mark.parametrize('existing', ['legacy_hold', 'broken_state_link', 'broken_marker_link'])
def test_explicit_initialization_requires_proven_fresh_state(tmp_path, monkeypatch, existing):
    module = broker_module()
    state = tmp_path / 'var' / 'maintenance.json'
    marker = state.with_name(state.name + '.initialized')
    state.parent.mkdir(mode=0o700)
    jobs = tmp_path / 'run' / 'jobs'
    gate = jobs.parent / 'maintenance.json'
    gate.parent.mkdir()
    if existing == 'legacy_hold':
        gate.write_text(json.dumps({'schema': module.MAINTENANCE_SCHEMA,
                                    'draining': True, 'owner': 'operator'}))
    else:
        (state if existing == 'broken_state_link' else marker).symlink_to('missing-target')
    real_lstat = Path.lstat

    def private_root_metadata(path, *args, **kwargs):
        result = real_lstat(path, *args, **kwargs)
        if path == state.parent:
            fields = list(result)
            fields[4] = 0  # Only the private fixture parent models root ownership.
            return os.stat_result(fields)
        return result

    with monkeypatch.context() as patch:
        patch.setattr(module.os, 'geteuid', lambda: 0)
        patch.setattr(Path, 'lstat', private_root_metadata)
        patch.setattr(sys, 'argv', ['resource_broker.py', '--state-dir', str(jobs),
                                  '--maintenance-state', str(state),
                                  '--initialize-maintenance-state'])
        try:
            module.main()
        except (SystemExit, OSError, ValueError):
            pass
    assert not state.is_file(), 'initialization replaced existing or legacy hold evidence with open'
    if existing == 'broken_state_link':
        assert state.is_symlink()
    if existing == 'broken_marker_link':
        assert marker.is_symlink()
