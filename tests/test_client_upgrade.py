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
        data = (b'CLIENT_UPGRADE_PROTOCOL = 2\n' if name == 'upgrade_client.py'
                else b'') + ('new:' + name).encode()
        target = generation / member
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
        files[member] = upgrade.digest(data)
        (install / name).write_bytes((b'CLIENT_UPGRADE_PROTOCOL = 2\n'
                                     if name == 'upgrade_client.py' else b'')
                                    + ('old:' + name).encode())
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
                                      for name in (*upgrade.MEMBERS, *(['gpu_capacity.py']
                                          if (install / 'gpu_capacity.py').exists() else []))
                                      if name != 'upgrade_client.py'})

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
            if ('requires_gpu_capacity' in (install / 'resource_broker.py').read_text()
                    and not (install / 'gpu_capacity.py').exists()):
                backend.running = False
                raise RuntimeError('new broker cannot import missing gpu_capacity.py')
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

    def crash(source, files):
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


def test_reader_export_roundtrip_checks_exact_bytes(setup):
    updater, _, _ = setup
    assert upgrade.decode_export(upgrade.encode_export(updater.config)) == upgrade.desired(updater.config)


@pytest.mark.parametrize('fault', ['bytes', 'members', 'generation', 'schema', 'size'])
def test_root_rejects_malformed_reader_export(setup, fault):
    updater, _, _ = setup
    value = json.loads(upgrade.encode_export(updater.config))
    if fault == 'bytes':
        value['blobs']['gpu_memory.py'] = 'YWJj'
    elif fault == 'members':
        value['blobs']['unexpected.py'] = 'YWJj'
    elif fault == 'generation':
        value['desired']['generation'] = '../../escape'
    elif fault == 'schema':
        value['schema'] = 'wrong'
    data = b' ' * (upgrade.MAX_EXPORT + 1) if fault == 'size' else json.dumps(value).encode()
    with pytest.raises(ValueError):
        upgrade.decode_export(data)


def test_reader_drops_all_credentials_before_loading_shared_runtime(setup, monkeypatch):
    updater, _, _ = setup
    updater.config['reader_uid'] = 1000
    expected = upgrade.encode_export(updater.config)
    seen = []
    monkeypatch.setattr(upgrade, 'trusted', lambda path: Path(path))
    monkeypatch.setattr(upgrade.pwd, 'getpwuid', lambda uid: SimpleNamespace(pw_gid=1000))

    def run(argv, **kwargs):
        seen.append((argv, kwargs))
        assert kwargs['user'] == 1000 and kwargs['group'] == 1000
        assert kwargs['extra_groups'] == []
        assert argv[:2] == ['/usr/bin/python3', '-I']
        assert '--export-runtime' in argv and kwargs['cwd'] == '/'
        assert kwargs['env'] == {'PATH': '/usr/bin:/bin'}
        kwargs['stdout'].write(expected)
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(upgrade.subprocess, 'run', run)
    assert upgrade.desired_as_reader('/etc/prismabuild/client-upgrade.json', updater.config) == upgrade.desired(updater.config)
    assert len(seen) == 1


def test_reader_cannot_be_root(setup):
    updater, _, _ = setup
    updater.config['reader_uid'] = 0
    with pytest.raises(ValueError, match='unprivileged UID'):
        upgrade.desired_as_reader('/unused', updater.config)



def add_optional_runtime(generation):
    member = 'src/prismabuild/gpu_capacity.py'
    (generation / member).write_bytes(b'new:gpu_capacity.py')
    (generation / 'tools/resource_broker.py').write_bytes(b'new:requires_gpu_capacity')
    receipt = generation / 'RUNTIME_VERSION.json'
    value = json.loads(receipt.read_text())
    for name in (member, 'tools/resource_broker.py'):
        value['files'][name] = upgrade.digest((generation / name).read_bytes())
    receipt.write_text(json.dumps(value))


def test_new_dependency_installs_before_starting_new_broker(setup):
    updater, backend, generation = setup
    assert not (updater.install / 'gpu_capacity.py').exists()
    add_optional_runtime(generation)
    result = updater.run()
    assert result['state'] == 'updated'
    assert (updater.install / 'gpu_capacity.py').read_bytes() == b'new:gpu_capacity.py'
    assert result['installed'] == result['desired']['files']
    assert updater.run()['state'] == 'current'


def test_failed_new_dependency_upgrade_restores_prior_absence(setup):
    updater, backend, generation = setup
    old = updater.installed()
    add_optional_runtime(generation)
    backend.fail_health = True
    result = updater.run()
    assert result['state'] == 'rolled_back'
    assert 'did not become healthy' in result['error']
    assert updater.installed() == old
    assert not (updater.install / 'gpu_capacity.py').exists()
    assert backend.running and not backend.draining


def test_optional_member_survives_unprivileged_export_validation(setup):
    updater, _, generation = setup
    add_optional_runtime(generation)
    version, blobs = upgrade.decode_export(upgrade.encode_export(updater.config))
    assert set(blobs) == set(upgrade.MEMBERS) | {'gpu_capacity.py'}
    assert version['files']['gpu_capacity.py'] == upgrade.digest(blobs['gpu_capacity.py'])


def test_dependency_addition_waits_for_active_jobs_without_staging_into_install(setup):
    updater, backend, generation = setup
    add_optional_runtime(generation)
    backend.active = 1
    assert updater.run()['state'] == 'draining'
    assert not (updater.install / 'gpu_capacity.py').exists()
    assert backend.operations == ['maintenance_begin']
    backend.active = 0
    assert updater.run()['state'] == 'updated'


def test_interrupted_dependency_install_restores_prior_absence_from_journal(setup):
    updater, backend, generation = setup
    add_optional_runtime(generation)
    old = updater.installed()
    original_copy = updater.copy_files

    def interrupted(source, files):
        original_copy(source, files)
        raise KeyboardInterrupt()

    updater.copy_files = interrupted
    with pytest.raises(KeyboardInterrupt):
        updater.run()
    transaction = json.loads(updater.journal.read_text())
    assert transaction['previous']['gpu_capacity.py'] is None
    assert (updater.install / 'gpu_capacity.py').exists()
    (generation / 'RUNTIME_VERSION.json').unlink()
    updater.copy_files = original_copy
    assert updater.run()['state'] == 'rolled_back'
    assert updater.installed() == old
    assert not (updater.install / 'gpu_capacity.py').exists()


def test_rolling_back_to_older_generation_removes_optional_dependency(setup):
    updater, backend, generation = setup
    receipt = generation / 'RUNTIME_VERSION.json'
    old_receipt = receipt.read_bytes()
    old_broker = (generation / 'tools/resource_broker.py').read_bytes()
    add_optional_runtime(generation)
    assert updater.run()['state'] == 'updated'
    receipt.write_bytes(old_receipt)
    (generation / 'tools/resource_broker.py').write_bytes(old_broker)
    assert updater.run()['state'] == 'updated'
    assert not (updater.install / 'gpu_capacity.py').exists()
    assert updater.run()['state'] == 'current'


def test_failed_removal_recovers_previously_installed_dependency(setup):
    updater, backend, generation = setup
    receipt = generation / 'RUNTIME_VERSION.json'
    old_receipt = receipt.read_bytes()
    old_broker = (generation / 'tools/resource_broker.py').read_bytes()
    add_optional_runtime(generation)
    assert updater.run()['state'] == 'updated'
    with_dependency = updater.installed()
    receipt.write_bytes(old_receipt)
    (generation / 'tools/resource_broker.py').write_bytes(old_broker)
    original_copy = updater.copy_files
    failed = False

    def interrupted_once(source, files):
        nonlocal failed
        original_copy(source, files)
        if not failed:
            failed = True
            raise RuntimeError('injected failed downgrade')

    updater.copy_files = interrupted_once
    assert updater.run()['state'] == 'rolled_back'
    assert updater.installed() == with_dependency
    assert (updater.install / 'gpu_capacity.py').exists()


def test_journal_cannot_mark_required_broker_as_previously_absent(setup):
    updater, backend, _ = setup
    previous = updater.installed()
    previous['resource_broker.py'] = None
    upgrade.atomic(updater.journal, {'previous': previous, 'desired': {}})
    with pytest.raises(ValueError, match='absent required member'):
        updater.run()
    assert not backend.operations


def test_legacy_updater_needs_bridge_then_new_dependency_converges(setup, tmp_path):
    # Load the exact four-member updater deployed before this change, not a
    # fake that reuses the new transaction implementation. The PB Git snapshot
    # carries the parent commit, including this historical source.
    import subprocess
    source = subprocess.check_output(['git', 'show',
        '161c9ae:tools/fleet/upgrade_client.py'], cwd=Path(__file__).parents[1])
    legacy_path = tmp_path / 'legacy_upgrade.py'
    legacy_path.write_bytes(source)
    spec = importlib.util.spec_from_file_location('legacy_upgrade', legacy_path)
    legacy = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(legacy)
    updater, backend, generation = setup
    old_receipt = (generation / 'RUNTIME_VERSION.json').read_bytes()
    old_broker = (generation / 'tools/resource_broker.py').read_bytes()
    old = legacy.Upgrader(updater.config, rpc=updater.rpc,
                          command=updater.command, sleep=lambda _: None)
    add_optional_runtime(generation)
    # Publishing the new dependency directly repeatedly rolls back the old
    # updater; merely adding MEMBERS in the candidate cannot repair that.
    assert old.run()['state'] == 'rolled_back'
    assert old.run()['state'] == 'rolled_back'
    assert not (updater.install / 'gpu_capacity.py').exists()
    (generation / 'RUNTIME_VERSION.json').write_bytes(old_receipt)
    (generation / 'tools/resource_broker.py').write_bytes(old_broker)
    # A bridge retains the old broker and carries only the new updater code.
    bridge = Path(upgrade.__file__).read_bytes()
    (generation / 'tools/upgrade_client.py').write_bytes(bridge)
    receipt = json.loads(old_receipt)
    receipt['files']['tools/upgrade_client.py'] = upgrade.digest(bridge)
    (generation / 'RUNTIME_VERSION.json').write_text(json.dumps(receipt))
    assert old.run()['state'] == 'updated'
    assert (updater.install / 'upgrade_client.py').read_bytes() == bridge
    assert old.run()['state'] == 'current'
    # Execute the actual bytes the legacy updater installed for stage two.
    spec = importlib.util.spec_from_file_location('bridge_upgrade', updater.install / 'upgrade_client.py')
    installed_bridge = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(installed_bridge)
    next_updater = installed_bridge.Upgrader(updater.config, rpc=updater.rpc,
                                            command=updater.command, sleep=lambda _: None)
    add_optional_runtime(generation)
    assert next_updater.run()['state'] == 'updated'
    assert (updater.install / 'gpu_capacity.py').exists()


def test_optional_transaction_rejects_prebridge_updater_before_drain(setup):
    updater, backend, generation = setup
    add_optional_runtime(generation)
    member = generation / 'tools/upgrade_client.py'
    member.write_bytes(b'# Legacy updater without dependency-aware recovery\n')
    receipt = generation / 'RUNTIME_VERSION.json'
    value = json.loads(receipt.read_text())
    value['files']['tools/upgrade_client.py'] = upgrade.digest(member.read_bytes())
    receipt.write_text(json.dumps(value))
    with pytest.raises(ValueError, match='dependency-aware recovery protocol'):
        updater.run()
    assert not backend.operations
    assert not updater.journal.exists()



def test_optional_transaction_requires_recoverable_previous_updater(setup):
    updater, backend, generation = setup
    add_optional_runtime(generation)
    (updater.install / 'upgrade_client.py').write_bytes(b'# pre-bridge updater')
    with pytest.raises(ValueError, match='dependency-aware recovery protocol'):
        updater.run()
    assert not backend.operations
    assert not updater.journal.exists()
