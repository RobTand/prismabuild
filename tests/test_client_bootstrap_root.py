"""Owed native root installation and interruption qualification.

Systemd and broker I/O use private actors. Files, metadata, copying, installed
loaders, lock FDs and isolated installed-reader children are real.
"""
from contextlib import contextmanager
import json
import os
from pathlib import Path
import runpy
import subprocess
import sys
from types import SimpleNamespace

import pytest
from test_client_proof_prior_trust import tmp_path
from client_proof_bridge_fixture import publication
from receipt_reader_fixture import bind_current_receipt_reader
from prismabuild.digest_primitives import canonical_sha256, raw_sha256

ROOT = Path(__file__).resolve().parents[1]


def a174_updater():
    sys.path.insert(0, str(ROOT / 'tests/compat/support'))
    from record_compat import retained
    with retained() as generations:
        name, old, _ = generations[0]
        assert name == 'a17477b2b206-1791745175-65dffa700dfb'
        raw = (old / 'tools/upgrade_client.py').read_bytes()
    assert raw_sha256(raw) == 'c3ce4b76c5709905edc435381ae79f9c616063fb7633a2802bbb337d004e2d58'
    return raw


@pytest.fixture
def private_bootstrap(publication, monkeypatch):
    bind_current_receipt_reader(publication)
    generation, install, state, config_path, _, _ = publication
    # Load the independent tool by captured source into its own module.
    import importlib.util
    spec = importlib.util.spec_from_file_location('private_bootstrap', ROOT / 'tools/fleet/bootstrap_client.py')
    helper = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(helper)
    reader = runpy.run_path(str(ROOT / 'tools/fleet/upgrade_client.py'))
    reader = SimpleNamespace(**reader)
    config = json.loads(config_path.read_bytes())
    version, blobs = reader.desired(config)
    for name in helper.SERVICE_MEMBERS:
        (install / name).write_bytes(blobs[name])
    old = a174_updater()
    (install / 'upgrade_client.py').write_bytes(old)
    for name in helper.LEAVES:
        (install / name).unlink(missing_ok=True)
    source_files = {name: raw_sha256((install / name).read_bytes())
                    for name in (*helper.SERVICE_MEMBERS, 'upgrade_client.py')}
    base = install.parent / 'independent'
    base.mkdir()
    base.chmod(0o755)
    (base / 'target-config.json').write_text(json.dumps(config))
    plan = {'id': 'root-case', 'direction': 'forward', 'source': {'generation': helper.SOURCE},
            'target': {'generation': generation.name}, 'source_files': source_files,
            'target_files': version['files'], 'boots': {'private': 'old-boot'},
            'brokers': {'private': {'pid': 1, 'starttime': '1'}}}
    @contextmanager
    def locked(plan, **kwargs):
        import fcntl
        with (state / 'upgrade.lock').open('a') as stream:
            fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
            yield 'private', base, config, reader, os.fstat(stream.fileno()).st_ino
    monkeypatch.setattr(helper, 'INSTALL', install)
    monkeypatch.setattr(helper, 'STATE', state)
    monkeypatch.setattr(helper, 'local_context', lambda plan, **kw: ('private', base, config))
    monkeypatch.setattr(helper, '_bootstrap_upgrade_lock', locked)
    monkeypatch.setattr(helper, 'ensure_fence', lambda *a: None)
    monkeypatch.setattr(helper, 'verify_services', lambda *a: ({'health': True}, {'draining': False}))
    monkeypatch.setattr(reader, 'desired_as_reader', lambda *a: (version, blobs))
    return helper, plan, install, state, reader, source_files, old


@pytest.mark.parametrize('name', ['digest_primitives.py', 'generation_receipt.py', 'park_proof.py', 'upgrade_client.py'])
@pytest.mark.parametrize('boundary', ['before-rename', 'after-rename', 'stage-fsync', 'installed-fsync'])
def test_every_four_member_boundary_resumes_independently_and_restores_only_updater(
        private_bootstrap, monkeypatch, name, boundary):
    helper, plan, install, state, reader, services, old = private_bootstrap
    original_commit = helper.commit_member
    original_sync = helper._bootstrap_sync
    original_replace = helper.os.replace
    armed = {'value': True}
    active = {'name': None}
    def interrupted(stage, member):
        active['name'] = member
        if member == name and boundary == 'before-rename' and armed['value']:
            armed['value'] = False
            raise OSError('power-loss-before-rename')
        original_commit(stage, member)
        if member == name and boundary == 'after-rename' and armed['value']:
            armed['value'] = False
            raise OSError('power-loss-after-rename')
    def syncing(directory):
        original_sync(directory)
        if armed['value'] and active['name'] == name and (
                boundary == 'stage-fsync' and directory != install
                or boundary == 'installed-fsync' and directory == install):
            armed['value'] = False
            raise OSError('power-loss-after-fsync')
    monkeypatch.setattr(helper, 'commit_member', interrupted)
    monkeypatch.setattr(helper, '_bootstrap_sync', syncing)
    before = {member: (install / member).stat().st_ino for member in helper.SERVICE_MEMBERS}
    with pytest.raises(OSError, match='power-loss'):
        helper.prepare_bootstrap(plan)
    assert not (state / 'transaction.json').exists()
    # Simulate a reboot at every live rename/fsync interruption boundary.
    monkeypatch.setattr(helper, 'bootstrap_boot_id', lambda: 'new-boot')
    monkeypatch.setattr(helper, 'observe_broker_process', lambda *a, **kw: {'pid': 234, 'starttime': '789'})
    helper.reconcile_boot(plan)
    # The frozen recovery tool survives every live replacement boundary.
    assert helper.prepare_bootstrap(plan)['phase'] == 'installed'
    assert {member: (install / member).stat().st_ino for member in helper.SERVICE_MEMBERS} == before
    result = helper.restore_updater({**plan, 'direction': 'restore'})
    assert result['retained_leaves'] == list(helper.LEAVES)
    assert (install / 'upgrade_client.py').read_bytes() == old
    assert all((install / leaf).read_bytes() == (ROOT / 'src/prismabuild' / leaf).read_bytes() for leaf in helper.LEAVES)
    assert {member: (install / member).stat().st_ino for member in helper.SERVICE_MEMBERS} == before


def test_root_capture_refuses_symlinks_and_nonregular_files(private_bootstrap):
    helper, _, install, _, _, _, _ = private_bootstrap
    path = install / 'alias'
    path.symlink_to(install / 'upgrade_client.py')
    with pytest.raises(OSError):
        helper._bootstrap_capture(path)
    path.unlink()
    os.mkfifo(path)
    with pytest.raises(ValueError, match='regular'):
        helper._bootstrap_capture(path)


def test_actual_final_receipt_and_proof_loaders_use_captured_installed_bytes(private_bootstrap):
    helper, plan, install, _, _, _, _ = private_bootstrap
    helper.prepare_bootstrap(plan)
    module, captured = helper._bootstrap_load_code(install / 'upgrade_client.py', plan['target_files']['upgrade_client.py'])
    assert captured == plan['target_files']['upgrade_client.py']
    assert module.receipt_owner().GenerationReceipt
    assert module.load_installed_park_proof(install).sha256 == plan['target_files']['park_proof.py']
    (install / 'digest_primitives.py').chmod(0o666)
    with pytest.raises(ValueError, match='root-trusted'):
        module.receipt_owner()


def test_native_isolated_installed_reader_executes_the_captured_source(private_bootstrap):
    helper, plan, install, _, _, _, _ = private_bootstrap
    helper.prepare_bootstrap(plan)
    program = """
import hashlib, json, os, pathlib, sys, types
assert sys.flags.isolated == 1 and os.geteuid() == 0
path = pathlib.Path(sys.argv[1])
raw = path.read_bytes()
module = types.ModuleType('captured_installed_reader')
module.__file__ = str(path)
exec(compile(raw, str(path), 'exec'), module.__dict__)
assert module.Upgrader._run_once.__globals__ is module.__dict__
assert module.receipt_owner().GenerationReceipt
proof = module.load_installed_park_proof(path.parent)
print(json.dumps({'captured_sha256': hashlib.sha256(raw).hexdigest(),
 'proof_sha256': proof.sha256, 'version': list(sys.version_info[:3]),
 'executable': sys.executable, 'affinity': sorted(os.sched_getaffinity(0)),
 'executable_sha256': hashlib.sha256(pathlib.Path('/proc/self/exe').read_bytes()).hexdigest()}))
"""
    result = subprocess.check_output(['/usr/bin/python3', '-I', '-c', program,
                                      str(install / 'upgrade_client.py')], text=True)
    observed = json.loads(result)
    assert observed['captured_sha256'] == plan['target_files']['upgrade_client.py']
    assert observed['proof_sha256'] == plan['target_files']['park_proof.py']
    assert observed['affinity'] == sorted(os.sched_getaffinity(0))
    print('bootstrap-installed-native-runtime: ' + result.strip())


def test_bootstrap_installed_reader_intake_permanently_drops_credentials(private_bootstrap, publication):
    helper, plan, install, _, _, _, _ = private_bootstrap
    helper.prepare_bootstrap(plan)
    config_path = publication[3]
    config = json.loads(config_path.read_bytes())
    config['reader_uid'] = 65534
    config_path.write_text(json.dumps(config))
    installed, _ = helper._bootstrap_load_code(install / 'upgrade_client.py', plan['target_files']['upgrade_client.py'])
    expected = installed.desired(config)
    actual = installed.desired_as_reader(config_path, config)
    assert actual == expected
    assert actual[0]['files'] == plan['target_files']


def test_root_reconciles_boot_without_replacing_intent_or_backup(private_bootstrap, monkeypatch):
    helper, plan, install, state, _, _, old = private_bootstrap
    helper.prepare_bootstrap(plan)
    before = json.loads((state / 'bootstrap/root-case/journal.json').read_bytes())
    monkeypatch.setattr(helper, 'bootstrap_boot_id', lambda: 'new-boot')
    monkeypatch.setattr(helper, 'observe_broker_process', lambda *a, **kw: {'pid': 234, 'starttime': '789'})
    result = helper.reconcile_boot(plan)
    after = json.loads((state / 'bootstrap/root-case/journal.json').read_bytes())
    assert result['boot_reconciliation']['boot_id'] == 'new-boot'
    assert after['previous'] == before['previous']
    assert after['plan_sha256'] == before['plan_sha256']
    assert (state / 'bootstrap/root-case/previous/upgrade_client.py').read_bytes() == old


@pytest.mark.parametrize('entry', ['reserved-no-journal', 'fencing', 'prepared', 'installed', 'accepted-forward'])
def test_root_reboot_entry_retains_original_backup(private_bootstrap, monkeypatch, entry):
    helper, plan, install, state, _, _, old = private_bootstrap
    if entry != 'reserved-no-journal':
        helper.prepare_bootstrap(plan)
        path = helper.journal_for(plan)
        journal = helper.read_journal(plan, path)
        journal['phase'] = entry
        helper._bootstrap_save(path, journal)
        before = (path.parent / 'previous/upgrade_client.py').read_bytes()
    if entry == 'accepted-forward':
        plan = {**plan, 'direction': 'restore'}
    monkeypatch.setattr(helper, 'bootstrap_boot_id', lambda: 'next-boot')
    monkeypatch.setattr(helper, 'observe_broker_process', lambda *a, **kw: {'pid': 44, 'starttime': '555'})
    result = helper.reconcile_boot(plan)
    assert result['boot_reconciliation']['initialized_without_journal'] == (entry == 'reserved-no-journal')
    if entry != 'reserved-no-journal':
        assert (path.parent / 'previous/upgrade_client.py').read_bytes() == before == old
    if entry != 'accepted-forward':
        helper.prepare_bootstrap(plan)
    assert helper.restore_updater({**plan, 'direction': 'restore'})['phase'] == 'restored-leaves-retained'
    assert (install / 'upgrade_client.py').read_bytes() == old


@pytest.mark.parametrize('boundary', ['before-rename', 'after-rename', 'stage-fsync', 'installed-fsync'])
def test_root_reverse_interruption_reboots_and_restores(private_bootstrap, monkeypatch, boundary):
    helper, plan, install, state, _, _, old = private_bootstrap
    helper.prepare_bootstrap(plan)
    plan = {**plan, 'direction': 'restore'}
    original_commit, original_sync = helper.commit_member, helper._bootstrap_sync
    armed = [True]
    live = [False]
    def commit(stage, name):
        live[0] = True
        if boundary == 'before-rename' and armed[0]:
            armed[0] = False
            raise OSError('interrupted reverse')
        original_commit(stage, name)
        if boundary == 'after-rename' and armed[0]:
            armed[0] = False
            raise OSError('interrupted reverse')
    def sync(directory):
        original_sync(directory)
        if live[0] and armed[0] and ((boundary == 'stage-fsync' and directory != install)
                or (boundary == 'installed-fsync' and directory == install)):
            armed[0] = False
            raise OSError('interrupted reverse')
    monkeypatch.setattr(helper, 'commit_member', commit)
    monkeypatch.setattr(helper, '_bootstrap_sync', sync)
    with pytest.raises(OSError, match='interrupted reverse'):
        helper.restore_updater(plan)
    monkeypatch.setattr(helper, 'bootstrap_boot_id', lambda: 'reverse-reboot')
    monkeypatch.setattr(helper, 'observe_broker_process', lambda *a, **kw: {'pid': 44, 'starttime': '555'})
    helper.reconcile_boot(plan)
    helper.restore_updater(plan)
    assert (install / 'upgrade_client.py').read_bytes() == old
    assert all((install / name).is_file() for name in helper.LEAVES)


@pytest.mark.parametrize('direction', ['forward', 'restore'])
@pytest.mark.parametrize('boundary', ['timer-drop', 'service-drop', 'block'])
def test_root_unfence_interruption_reboots_and_rechecks_selected_closure(
        private_bootstrap, monkeypatch, direction, boundary):
    helper, plan, install, state, reader, _, old = private_bootstrap
    helper.prepare_bootstrap(plan)
    if direction == 'restore':
        plan = {**plan, 'direction': 'restore'}
        helper.restore_updater(plan)
    _, base, config = helper.local_context(plan)
    expected = plan['source_files'] if direction == 'restore' else plan['target_files']
    monkeypatch.setattr(reader, 'desired_as_reader', lambda *a: ({'files': expected}, {}))
    units = install.parent / 'private-systemd'
    units.mkdir()
    monkeypatch.setattr(helper, 'UNITS', units)
    block, drops = helper.fence_paths(plan)
    for drop in drops:
        helper._bootstrap_write(drop, helper.fence_content(block), mode=0o644)
    helper._bootstrap_write(block, b'blocked', mode=0o644)
    path = helper.journal_for(plan)
    journal = helper.read_journal(plan, path)
    journal['units'] = {unit: {'ActiveState': 'active'} for unit in helper.UPGRADE_UNITS}
    helper._bootstrap_save(path, journal)
    helper._bootstrap_save(base / 'accepted.json', {'direction': direction, 'plan_sha256': canonical_sha256(plan)})
    monkeypatch.setattr(helper, 'fenced', lambda p: block.exists() and all(d.exists() for d in drops))
    calls = []
    monkeypatch.setattr(helper, '_bootstrap_systemctl', lambda *a: calls.append(a))
    original_sync = helper._bootstrap_sync
    target = {'timer-drop': drops[0].parent, 'service-drop': drops[1].parent, 'block': block.parent}[boundary]
    armed = [True]
    def syncing(directory):
        original_sync(directory)
        removed = {'timer-drop': not drops[0].exists(), 'service-drop': not drops[1].exists(), 'block': not block.exists()}[boundary]
        if directory == target and removed and armed[0]:
            armed[0] = False
            raise OSError('interrupted unfence')
    monkeypatch.setattr(helper, '_bootstrap_sync', syncing)
    with pytest.raises(OSError, match='interrupted unfence'):
        helper.unfence_bootstrap(plan)
    monkeypatch.setattr(helper, 'bootstrap_boot_id', lambda: 'unfence-reboot')
    monkeypatch.setattr(helper, 'observe_broker_process', lambda *a, **kw: {'pid': 44, 'starttime': '555'})
    helper.reconcile_boot(plan)
    assert helper.unfence_bootstrap(plan)['phase'] == 'accepted-' + direction
    assert not block.exists() and not any(d.exists() for d in drops)
    assert (install / 'upgrade_client.py').read_bytes() == (old if direction == 'restore' else (ROOT / 'tools/fleet/upgrade_client.py').read_bytes())
    assert all(call[0] in ('daemon-reload', 'start') for call in calls)
