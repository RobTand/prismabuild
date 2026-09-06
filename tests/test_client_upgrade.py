"""Upgrade transaction tests use fake broker/service; no live host mutation."""
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

spec = importlib.util.spec_from_file_location('upgrade_client', Path(__file__).parents[1] / 'tools/fleet/upgrade_client.py')
upgrade = importlib.util.module_from_spec(spec)
spec.loader.exec_module(upgrade)


@pytest.fixture
def setup(tmp_path):
    store = tmp_path / 'generations'
    generation = store / 'abc-generation'
    install = tmp_path / 'installed'
    state = tmp_path / 'state'
    install.mkdir()
    state.mkdir()
    files = {}
    for name, member in upgrade.MEMBERS.items():
        data = ('new:' + name).encode()
        target = generation / member
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
        files[member] = upgrade.digest(data)
        (install / name).write_bytes(('old:' + name).encode())
    (generation / 'RUNTIME_VERSION.json').write_text(json.dumps({
        'schema': 'prismaquant.prismabuild.runtime_version.v1',
        'generation': generation.name, 'commit': 'a' * 40, 'files': files}))
    config = dict(runtime=str(generation), generation_store=str(store),
                  install_dir=str(install), state_dir=str(state))
    backend = SimpleNamespace(draining=False, active=0, running=True,
                              operations=[], fail_health=False)

    def rpc(endpoint, op):
        backend.operations.append(op)
        if op == 'maintenance_begin':
            backend.draining = True
        if op == 'maintenance_end':
            backend.draining = False
        if backend.fail_health and op == 'maintenance_status':
            return dict(ok=True, health=False, draining=backend.draining, active_scopes=0)
        return dict(ok=True, health=True, draining=backend.draining,
                    active_scopes=backend.active,
                    installed_sha256={name: upgrade.digest((install / name).read_bytes())
                                      for name in upgrade.MEMBERS if name != 'upgrade_client.py'})

    def command(argv, **kwargs):
        verb = argv[1]
        backend.operations.append(verb)
        if verb == 'is-active':
            return SimpleNamespace(returncode=0 if backend.running else 3)
        if verb == 'stop':
            assert backend.active == 0, 'must never interrupt an active scope'
            assert backend.draining, 'must close admission before stop'
            backend.running = False
        if verb == 'start':
            backend.running = True
            if (install / 'resource_broker.py').read_text().startswith('old:'):
                backend.fail_health = False
        return SimpleNamespace(returncode=0)

    updater = upgrade.Upgrader(config, rpc=rpc, command=command, sleep=lambda _: None)
    return updater, backend, generation


def test_updates_exact_hashes_and_reopens_only_after_health(setup):
    updater, backend, _ = setup
    result = updater.run()
    assert result['state'] == 'updated'
    assert result['installed'] == result['desired']['files']
    assert backend.operations == ['maintenance_begin', 'stop', 'start',
                                  'maintenance_status', 'maintenance_end']
    assert not updater.journal.exists()
    backend.operations.clear()
    assert updater.run()['state'] == 'current'
    assert backend.operations == ['maintenance_status']


def test_active_jobs_drain_without_restart_and_next_tick_upgrades(setup):
    updater, backend, _ = setup
    old = updater.installed()
    backend.active = 2
    assert updater.run()['state'] == 'draining'
    assert updater.installed() == old
    assert backend.operations == ['maintenance_begin']
    assert backend.draining
    backend.active = 0
    assert updater.run()['state'] == 'updated'


def test_bad_new_health_restores_complete_previous_installation(setup):
    updater, backend, _ = setup
    old = updater.installed()
    backend.fail_health = True
    result = updater.run()
    assert result['state'] == 'rolled_back'
    assert result['installed'] == old
    assert backend.running and not backend.draining
    assert 'did not become healthy' in result['error']
    assert not updater.journal.exists()


def test_interrupted_copy_recovers_before_reading_a_broken_publication(setup):
    updater, backend, generation = setup
    old = updater.installed()
    original_copy = updater.copy_files

    def crash(source):
        (updater.install / 'resource_broker.py').write_bytes(b'partial replacement')
        raise KeyboardInterrupt()

    updater.copy_files = crash
    with pytest.raises(KeyboardInterrupt):
        updater.run()
    assert updater.journal.exists()
    assert not backend.running
    (generation / 'RUNTIME_VERSION.json').unlink()
    updater.copy_files = original_copy
    assert updater.run()['state'] == 'rolled_back'
    assert updater.installed() == old
    assert backend.running and not backend.draining


def test_recovery_after_reopening_admission_never_stops_new_work(setup):
    updater, backend, _ = setup
    updater.run()
    upgrade.atomic(updater.journal, {'desired': {'generation': 'interrupted'},
                                    'previous': {name: upgrade.digest((updater.state / 'previous' / name).read_bytes())
                                                 for name in upgrade.MEMBERS}})
    backend.active = 1
    backend.operations.clear()
    with pytest.raises(RuntimeError, match='still owns active work'):
        updater.run()
    assert backend.operations == ['is-active', 'maintenance_begin']
    assert updater.journal.exists()


@pytest.mark.parametrize('fault', ['hash', 'symlink', 'generation', 'outside'])
def test_invalid_publication_does_not_drain_or_mutate(setup, fault, tmp_path):
    updater, backend, generation = setup
    old = updater.installed()
    member = generation / next(iter(upgrade.MEMBERS.values()))
    if fault == 'hash':
        member.write_bytes(b'changed after publication')
    elif fault == 'symlink':
        data = member.read_bytes()
        member.unlink()
        target = tmp_path / 'elsewhere'
        target.write_bytes(data)
        member.symlink_to(target)
    elif fault == 'generation':
        receipt = generation / 'RUNTIME_VERSION.json'
        value = json.loads(receipt.read_text())
        value['generation'] = 'different'
        receipt.write_text(json.dumps(value))
    else:
        updater.config['generation_store'] = str(tmp_path)
    with pytest.raises(ValueError):
        updater.run()
    assert updater.installed() == old
    assert not backend.operations


def test_same_generation_changed_hashes_still_converge(setup):
    updater, backend, generation = setup
    updater.run()
    member = 'tools/resource_payload.py'
    (generation / member).write_bytes(b'successor bytes')
    receipt = generation / 'RUNTIME_VERSION.json'
    data = json.loads(receipt.read_text())
    data['files'][member] = upgrade.digest(b'successor bytes')
    receipt.write_text(json.dumps(data))
    assert updater.run()['state'] == 'updated'
    assert (updater.install / 'resource_payload.py').read_bytes() == b'successor bytes'


def test_current_version_clears_orphaned_drain(setup):
    updater, backend, _ = setup
    updater.run()
    backend.draining = True
    assert updater.run()['state'] == 'current'
    assert not backend.draining


def test_corrupted_backup_refuses_recovery_without_stopping_service(setup):
    updater, backend, _ = setup
    updater.run()
    previous = {name: upgrade.digest((updater.state / 'previous' / name).read_bytes())
                for name in upgrade.MEMBERS}
    upgrade.atomic(updater.journal, {'desired': {'generation': 'interrupted'},
                                    'previous': previous})
    (updater.state / 'previous' / 'resource_payload.py').write_bytes(b'corrupt backup')
    backend.operations.clear()
    with pytest.raises(RuntimeError, match='previous client hash mismatch'):
        updater.run()
    assert not backend.operations
    assert updater.journal.exists()
