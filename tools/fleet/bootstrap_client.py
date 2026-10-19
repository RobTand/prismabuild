#!/usr/bin/python3
"""Prepare, observe and restore the fixed a174 installed-client bootstrap.

Copy this tool and its reviewed reader closure to root-local custody first.
It never controls the broker, admission or serving processes.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import fcntl
import json
import os
import pwd
from pathlib import Path
import re
import runpy
import socket
import stat
import subprocess
import sys
import tempfile
import time
from types import ModuleType, SimpleNamespace
import uuid

CONTROL = Path('/opt/prismabuild-bootstrap')
INSTALL = Path('/opt/prismabuild-resource-broker')
STATE = Path('/var/lib/prismabuild-client-upgrade')
CONFIG = Path('/etc/prismabuild/client-upgrade.json')
UNITS = Path('/etc/systemd/system')
GATE = Path('/run/prismabuild/maintenance.json')
SOURCE = 'a17477b2b206-1791745175-65dffa700dfb'
SERVICE_MEMBERS = ('resource_broker.py', 'resource_payload.py', 'gpu_memory.py', 'gpu_capacity.py')
LEAVES = ('digest_primitives.py', 'generation_receipt.py', 'park_proof.py')
REPLACE = (*LEAVES, 'upgrade_client.py')
MEMBERS = (*SERVICE_MEMBERS, *REPLACE)
UPGRADE_UNITS = ('prismabuild-client-upgrade.timer', 'prismabuild-client-upgrade.service')
LIMIT = 4 * 1024 * 1024


def _bootstrap_digests():
    source = Path(__file__).absolute()
    if source.parent.name == 'fleet' and source.parent.parent.name == 'tools':
        path = source.parents[2] / 'src/prismabuild/digest_primitives.py'
    elif source.parent.name == 'tools':
        path = source.parents[1] / 'src/prismabuild/digest_primitives.py'
    else:
        path = _bootstrap_trusted(source.parent / 'reader/digest_primitives.py')
    return runpy.run_path(str(path))


def _bootstrap_sha(raw):
    return _bootstrap_digests()['raw_sha256'](raw)


def _bootstrap_canonical(value):
    return _bootstrap_digests()['_canonical_bytes'](value)


def _bootstrap_identity(value):
    if not isinstance(value, str) or re.fullmatch('[a-zA-Z0-9][a-zA-Z0-9._-]{0,95}', value) is None:
        raise ValueError('invalid bootstrap id')
    return value


def _bootstrap_read_json(path):
    def pairs(items):
        value = {}
        for key, item in items:
            if key in value:
                raise ValueError('duplicate bootstrap JSON key')
            value[key] = item
        return value
    raw, _ = _bootstrap_capture(path)
    return json.loads(raw, object_pairs_hook=pairs,
                      parse_constant=lambda value: (_ for _ in ()).throw(ValueError('nonfinite JSON')))


def _bootstrap_trusted(path):
    path = Path(path).absolute()
    for entry in (path, *path.parents):
        info = entry.lstat()
        if info.st_uid != 0 or info.st_mode & 0o022 or stat.S_ISLNK(info.st_mode):
            raise ValueError('unsafe root custody: ' + str(entry))
    return path


def _bootstrap_capture(path):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, 'rb') as stream:
        before = os.fstat(stream.fileno())
        if not stat.S_ISREG(before.st_mode) or before.st_size > LIMIT:
            raise ValueError('bootstrap requires a bounded regular file')
        raw = stream.read(LIMIT + 1)
        after = os.fstat(stream.fileno())
        fields = ('st_dev', 'st_ino', 'st_mode', 'st_uid', 'st_gid', 'st_size', 'st_mtime_ns', 'st_ctime_ns')
        if len(raw) != before.st_size or any(getattr(before, name) != getattr(after, name) for name in fields):
            raise ValueError('bootstrap file changed during _bootstrap_capture')
        return raw, {'uid': before.st_uid, 'gid': before.st_gid, 'mode': stat.S_IMODE(before.st_mode)}


def _bootstrap_sync(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _bootstrap_mkdir(path, *, mode=0o755):
    """Make every new parent link durable before callers depend on it."""
    path = Path(path)
    missing = []
    cursor = path
    while not cursor.exists():
        missing.append(cursor)
        cursor = cursor.parent
    _bootstrap_trusted(cursor)
    for directory in reversed(missing):
        directory.mkdir(mode=mode)
        _bootstrap_sync(directory)
        _bootstrap_sync(directory.parent)
        if not stat.S_ISDIR(directory.lstat().st_mode):
            raise ValueError('bootstrap directory readback differs')
        _bootstrap_trusted(directory)
    # Existing names may be remnants of a process loss between mkdir and its
    # parent fsync. Retrying must settle that durability debt as well.
    for directory in (path, *path.parents):
        _bootstrap_sync(directory)
    return _bootstrap_trusted(path)


def _bootstrap_seal_local(base):
    # The operator's install may have been interrupted before any journal.
    # Sync the complete fixed recovery closure and all its ancestor links.
    paths = [base / name for name in ('bootstrap_client.py', 'plan.json',
        'source-config.json', 'target-config.json', 'source-receipt.json', 'target-receipt.json')]
    paths += [base / 'reader' / name for name in REPLACE]
    if (base / 'accepted.json').exists():
        paths.append(base / 'accepted.json')
    for path in paths:
        before, metadata = _bootstrap_capture(_bootstrap_trusted(path))
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
        if _bootstrap_capture(path) != (before, metadata):
            raise ValueError('bootstrap local staging readback differs')
    for directory in (base / 'reader', base, *base.parents):
        _bootstrap_sync(directory)


def _bootstrap_write(path, raw, *, mode=0o600):
    _bootstrap_mkdir(path.parent)
    fd, name = tempfile.mkstemp(prefix='.bootstrap-', dir=path.parent)
    temporary = Path(name)
    try:
        with os.fdopen(fd, 'wb') as stream:
            stream.write(raw)
            stream.flush()
            os.fchmod(stream.fileno(), mode)
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        _bootstrap_sync(path.parent)
        captured, metadata = _bootstrap_capture(path)
        if captured != raw or metadata['mode'] != mode:
            raise ValueError('bootstrap durable file readback differs')
    finally:
        temporary.unlink(missing_ok=True)


def _bootstrap_save(path, value):
    _bootstrap_write(path, _bootstrap_canonical(value))


def _bootstrap_load_code(path, expected):
    raw, _ = _bootstrap_capture(_bootstrap_trusted(path))
    if _bootstrap_sha(raw) != expected:
        raise ValueError('bootstrap executing source differs: ' + str(path))
    module = ModuleType('_bootstrap_' + uuid.uuid4().hex)
    module.__file__ = str(path)
    code = compile(raw, str(path), 'exec')
    exec(code, module.__dict__)
    return module, _bootstrap_sha(raw)


def _bootstrap_validate_plan(plan):
    _bootstrap_identity(plan['id'])
    if (plan.get('schema') != 'prismabuild.client_bootstrap.plan.v1'
            or plan['source']['generation'] != SOURCE
            or plan['source']['receipt_sha256'] != '5cef73692997fbd9f3d8d187c9f7aad15ee6b699912ceb1cf88fe9444082c9dd'
            or plan['source']['content_sha256'] != 'dd694fdb23b193f46ab82349c81067b6a9fd68eeaaba3ec24e24bf4176767ff3'
            or plan.get('direction', 'forward') not in ('forward', 'restore')
            or set(plan['members']) != set(MEMBERS)
            or set(plan['target_files']) != set(MEMBERS)
            or set(plan['source_files']) != set((*SERVICE_MEMBERS, 'upgrade_client.py'))):
        raise ValueError('bootstrap is restricted to a174 five to final eight members')
    for name in SERVICE_MEMBERS:
        if plan['source_files'][name] != plan['target_files'][name]:
            raise ValueError('bootstrap changes a broker service member')
    for value in (*plan['target_files'].values(), *plan['source_files'].values(), plan['tool_sha256']):
        if re.fullmatch('[0-9a-f]{64}', value) is None:
            raise ValueError('invalid bootstrap digest')
    aliases = [alias for names in plan['roster'].values() for alias in names]
    if (not aliases or len(set(aliases)) != len(aliases)
            or any(host not in names for host, names in plan['roster'].items())):
        raise ValueError('ambiguous bootstrap aliases')
    for alias in aliases:
        _bootstrap_identity(alias)
    return plan


def local_context(plan, *, allow_boot_change=False):
    _bootstrap_validate_plan(plan)
    if bootstrap_runtime_identity() not in plan.get("qualified_runtimes", []):
        raise ValueError("host interpreter is outside the qualified bootstrap runtime population")
    host = socket.gethostname()
    hosts = [key for key, aliases in plan['roster'].items() if host in aliases]
    if len(hosts) != 1:
        raise ValueError('host is not enrolled in bootstrap')
    key = hosts[0]
    base = _bootstrap_trusted(CONTROL / plan['id'])
    _bootstrap_seal_local(base)
    if _bootstrap_sha(_bootstrap_capture(_bootstrap_trusted(base / 'bootstrap_client.py'))[0]) != plan['tool_sha256']:
        raise ValueError('bootstrap recovery tool differs')
    config = _bootstrap_read_json(_bootstrap_trusted(CONFIG))
    if (_bootstrap_sha(_bootstrap_capture(CONFIG)[0]) != plan['enrollments'][key]
            or config['install_dir'] != str(INSTALL) or config['state_dir'] != str(STATE)
            or config.get('maintenance_gate', str(GATE)) != str(GATE)
            or config.get('socket', '/run/prismabuild/resources.sock') != '/run/prismabuild/resources.sock'
            or Path(config['generation_store']) != Path(plan['target']['directory']).parent):
        raise ValueError('bootstrap enrollment changed')
    if bootstrap_boot_id() != plan['boots'][key] and not allow_boot_change:
        path = STATE / 'bootstrap' / plan['id'] / 'journal.json'
        if not path.exists():
            raise ValueError('bootstrap boot changed; run explicit reconcile-boot while fenced')
        journal = read_journal(plan, path)
        record = journal.get('boot_reconciliation', {})
        if (record.get('boot_id') != bootstrap_boot_id()
                or record.get('original_boot_id') != plan['boots'][key]
                or record.get('plan_sha256') != original_plan_hash(plan)):
            raise ValueError('bootstrap boot changed; run explicit reconcile-boot while fenced')
    _bootstrap_trusted(INSTALL)
    _bootstrap_trusted(STATE)
    if (STATE / 'transaction.json').exists() or (STATE / 'rollout-epoch.json').exists():
        raise ValueError('resolve automatic journal/epoch before bootstrap')
    _bootstrap_require_reservation(plan, base, config)
    return key, base, config


def _bootstrap_require_reservation(plan, base, config):
    uid = config.get('reader_uid', 1000)
    if type(uid) is not int or uid <= 0:
        raise ValueError('bootstrap runtime reader must be unprivileged')
    reservation = subprocess.run(['/usr/bin/python3', '-I', str(base / 'bootstrap_client.py'),
        'export-reservation', '--id', plan['id'], '--plan-sha256', _bootstrap_sha(_bootstrap_canonical(plan))],
        user=uid, group=pwd.getpwuid(uid).pw_gid, extra_groups=[], cwd='/',
        env={'PATH': '/usr/bin:/bin'}, capture_output=True, check=True, timeout=30)
    if len(reservation.stdout) > 1024 * 1024:
        raise ValueError('oversized bootstrap reservation')
    pending = json.loads(reservation.stdout)
    if (pending.get('id') != plan['id'] or pending.get('source') != plan['source']
            or pending.get('target') != plan['target']
            or pending.get('forward_plan_sha256') != original_plan_hash(plan)):
        raise ValueError('shared bootstrap reservation differs')

@contextmanager
def _bootstrap_upgrade_lock(plan, *, allow_boot_change=False):
    key, base, config = local_context(plan, allow_boot_change=allow_boot_change)
    path = _bootstrap_trusted(STATE / 'upgrade.lock')
    fd = os.open(path, os.O_RDWR | os.O_NOFOLLOW)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_ino != plan['locks'][key] or path.stat().st_ino != info.st_ino:
            raise ValueError('bootstrap upgrade.lock inode changed')
        if (STATE / 'transaction.json').exists() or (STATE / 'rollout-epoch.json').exists():
            raise ValueError('resolve automatic journal/epoch before bootstrap')
        reader, _ = _bootstrap_load_code(base / 'reader/upgrade_client.py', plan['target_files']['upgrade_client.py'])
        if plan['members'] != {**reader.MEMBERS, **reader.OPTIONAL_MEMBERS}:
            raise ValueError('bootstrap member paths differ from frozen updater contract')
        for name in LEAVES:
            if _bootstrap_sha(_bootstrap_capture(_bootstrap_trusted(base / 'reader' / name))[0]) != plan['target_files'][name]:
                raise ValueError('bootstrap private reader closure changed')
        for label in ('source', 'target'):
            receipt_raw, _ = _bootstrap_capture(_bootstrap_trusted(base / (label + '-receipt.json')))
            if _bootstrap_sha(receipt_raw) != plan[label]['receipt_sha256']:
                raise ValueError('private bootstrap receipt differs')
            captured = reader.receipt_owner().GenerationReceipt.capture(receipt_raw)
            receipt = captured.require_identity(plan[label]['generation'])
            if receipt['commit'] != plan[label]['commit']:
                raise ValueError('private bootstrap commit differs')
            for name, expected in plan[label + '_files'].items():
                if captured.member_digest(plan['members'][name]) != expected:
                    raise ValueError('private bootstrap member differs')
            intake = _bootstrap_read_json(_bootstrap_trusted(base / (label + '-config.json')))
            if intake != {**config, 'runtime': plan[label]['directory']}:
                raise ValueError('private bootstrap intake enrollment differs')
        if captured.member_digest('tools/bootstrap_client.py') != plan['tool_sha256']:
            raise ValueError('bootstrap tool is not the sealed target helper')
        rollout = reader.rollout_as_reader(CONFIG, config)
        if rollout is not None:
            raise ValueError('unresolved shared rollout epoch')
        yield key, base, config, reader, info.st_ino
    finally:
        os.close(fd)


def _bootstrap_systemctl(*args):
    return subprocess.run(['/usr/bin/systemctl', *args], check=True, capture_output=True,
                          text=True, timeout=30).stdout.strip()


def _bootstrap_busctl(*args):
    return subprocess.run(['/usr/bin/busctl', '--json=short', *args], check=True, capture_output=True,
                          text=True, timeout=30).stdout


def _bootstrap_loaded_conditions(unit):
    label = ''.join(c if c.isascii() and c.isalnum() and not (i == 0 and c.isdigit()) else '_%02x' % ord(c)
                    for i, c in enumerate(unit))
    value = json.loads(_bootstrap_busctl('get-property', 'org.freedesktop.systemd1',
                                         '/org/freedesktop/systemd1/unit/' + label,
                                         'org.freedesktop.systemd1.Unit', 'Conditions'))
    if value.get('type') != 'a(sbbsi)':
        raise ValueError('systemd returned an unexpected Conditions type')
    return value['data']


def _bootstrap_unit_state(unit):
    raw = _bootstrap_systemctl('show', unit, '--property=MainPID,ActiveState,Job,UnitFileState,Conditions')
    return dict(line.split('=', 1) for line in raw.splitlines() if '=' in line)


def fence_paths(plan):
    block = STATE / 'bootstrap' / plan['id'] / 'blocked'
    return block, [UNITS / (unit + '.d') / ('90-prismabuild-bootstrap-' + plan['id'] + '.conf')
                   for unit in UPGRADE_UNITS]


def fence_content(block):
    return ('[Unit]\nConditionPathExists=!' + str(block) + '\n').encode()


def fenced(plan):
    block, drops = fence_paths(plan)
    if not block.exists():
        return False
    _bootstrap_trusted(block)
    for path in drops:
        if _bootstrap_capture(_bootstrap_trusted(path))[0] != fence_content(block):
            return False
    # systemctl show prints Conditions as "[unprintable]" on systemd 255 and 259;
    # the typed D-Bus property names the negated block condition systemd loaded.
    for unit in UPGRADE_UNITS:
        loaded = [row[:4] for row in _bootstrap_loaded_conditions(unit)]
        if ['ConditionPathExists', False, True, str(block)] not in loaded:
            return False
    state = _bootstrap_unit_state(UPGRADE_UNITS[1])
    return (state.get('MainPID') == '0' and state.get('Job') in ('', '0')
            and state.get('ActiveState') in ('inactive', 'failed'))


def ensure_fence(plan, journal, journal_path):
    block, drops = fence_paths(plan)
    if 'units' not in journal:
        journal['units'] = {unit: _bootstrap_unit_state(unit) for unit in UPGRADE_UNITS}
        _bootstrap_save(journal_path, journal)
    _bootstrap_write(block, b'blocked\n', mode=0o644)
    for path in drops:
        _bootstrap_mkdir(path.parent)
        if path.exists() and _bootstrap_capture(_bootstrap_trusted(path))[0] != fence_content(block):
            raise ValueError('bootstrap drop-in already has different contents')
        _bootstrap_write(path, fence_content(block), mode=0o644)
    _bootstrap_systemctl('daemon-reload')
    _bootstrap_systemctl('stop', UPGRADE_UNITS[0])
    deadline = time.monotonic() + 120
    while not fenced(plan):
        if time.monotonic() >= deadline:
            raise ValueError('upgrader oneshot has not exited naturally; fence retained')
        time.sleep(1)


def journal_for(plan):
    directory = STATE / 'bootstrap' / plan['id']
    _bootstrap_mkdir(directory)
    return directory / 'journal.json'


def original_plan_hash(plan):
    return _bootstrap_sha(_bootstrap_canonical({**plan, 'direction': 'forward'}))


def read_journal(plan, path):
    journal = _bootstrap_read_json(_bootstrap_trusted(path))
    if (journal['plan_sha256'] != original_plan_hash(plan)
            or journal.get('original_plan', {**plan, 'direction': 'forward'}) != {**plan, 'direction': 'forward'}):
        raise ValueError('bootstrap journal belongs to a different plan')
    return journal


def private_stage(base, reader, files):
    directory = INSTALL / ('.bootstrap-' + uuid.uuid4().hex)
    _bootstrap_mkdir(directory, mode=0o700)
    actor = SimpleNamespace(install=directory, validate_files=reader.Upgrader.validate_files)
    reader.Upgrader.copy_files(actor, base, files)
    _bootstrap_sync(directory)
    for name, expected in files.items():
        if _bootstrap_sha(_bootstrap_capture(_bootstrap_trusted(directory / name))[0]) != expected:
            raise ValueError('bootstrap private stage readback differs')
    return directory


def commit_member(stage, name):
    # Both directories are on INSTALL's filesystem. The service members never
    # reach this operation. Interrupted renames are reconciled from live bytes.
    if name not in REPLACE:
        raise ValueError('bootstrap cannot commit a broker service file')
    captured = _bootstrap_capture(_bootstrap_trusted(stage / name))
    os.replace(stage / name, INSTALL / name)
    _bootstrap_sync(stage)
    _bootstrap_sync(INSTALL)
    if _bootstrap_capture(_bootstrap_trusted(INSTALL / name)) != captured:
        raise ValueError('bootstrap committed member readback differs')


def verify_services(plan, reader, config):
    files = {}
    for name in SERVICE_MEMBERS:
        raw, metadata = _bootstrap_capture(_bootstrap_trusted(INSTALL / name))
        if _bootstrap_sha(raw) != plan['source_files'][name]:
            raise ValueError('installed service differs: ' + name)
        files[name] = _bootstrap_sha(raw)
    broker = reader.request(config.get('socket', '/run/prismabuild/resources.sock'), 'maintenance_status')
    if (broker.get('health') is not True or broker.get('installed_sha256') != files
            or broker.get('maintenance_durable_protocol') != 1):
        raise ValueError('running broker differs or is unhealthy')
    gate = _bootstrap_read_json(_bootstrap_trusted(GATE))
    if (type(gate.get('draining')) is not bool or broker.get('draining') is not gate['draining']
            or broker.get('maintenance_owner') != gate.get('owner')):
        raise ValueError('gate/broker owner differs')
    return broker, gate


def prepare_bootstrap(plan):
    if plan.get('direction', 'forward') != 'forward':
        raise ValueError('prepare requires forward plan')
    local_context(plan)
    path = journal_for(plan)
    if path.exists():
        journal = read_journal(plan, path)
    else:
        journal = {'plan_sha256': original_plan_hash(plan), 'phase': 'fencing',
                   'original_plan': {**plan, 'direction': 'forward'}}
        _bootstrap_save(path, journal)
    ensure_fence(plan, journal, path)
    with _bootstrap_upgrade_lock(plan) as (_, base, config, reader, _):
        journal = read_journal(plan, path)
        if 'previous' not in journal:
            previous = {}
            backup = path.parent / 'previous'
            _bootstrap_mkdir(backup, mode=0o700)
            for name in REPLACE:
                try:
                    raw, metadata = _bootstrap_capture(_bootstrap_trusted(INSTALL / name))
                except FileNotFoundError:
                    if name == 'upgrade_client.py':
                        raise
                    previous[name] = None
                    continue
                expected = (plan['source_files']['upgrade_client.py'] if name == 'upgrade_client.py'
                            else plan['target_files'][name])
                if _bootstrap_sha(raw) != expected:
                    raise ValueError('unexpected prior bootstrap member: ' + name)
                _bootstrap_write(backup / name, raw)
                previous[name] = {'sha256': _bootstrap_sha(raw), **metadata}
            journal.update(previous=previous, phase='prepared')
            _bootstrap_save(path, journal)
        verify_services(plan, reader, config)
        version, blobs = reader.desired_as_reader(base / 'target-config.json',
                                                  _bootstrap_read_json(_bootstrap_trusted(base / 'target-config.json')))
        if version['files'] != plan['target_files'] or version['generation'] != plan['target']['generation']:
            raise ValueError('candidate runtime intake differs from plan')
        payload = path.parent / 'payload'
        _bootstrap_mkdir(payload, mode=0o700)
        for name, raw in blobs.items():
            _bootstrap_write(payload / name, raw)
        stage = private_stage(payload, reader, plan['target_files'])
        for name in REPLACE:
            journal['phase'] = 'install:' + name
            _bootstrap_save(path, journal)
            commit_member(stage, name)
        journal['phase'] = 'installed'
        _bootstrap_save(path, journal)
        return {'phase': 'installed', 'journal_sha256': _bootstrap_sha(_bootstrap_capture(path)[0])}


def restore_updater(plan):
    local_context(plan)
    path = journal_for(plan)
    journal = read_journal(plan, path)
    ensure_fence(plan, journal, path)
    with _bootstrap_upgrade_lock(plan) as (_, _, config, reader, _):
        journal = read_journal(plan, path)
        verify_services(plan, reader, config)
        backup = path.parent / 'previous'
        old = journal['previous']['upgrade_client.py']
        if _bootstrap_sha(_bootstrap_capture(_bootstrap_trusted(backup / 'upgrade_client.py'))[0]) != old['sha256']:
            raise ValueError('bootstrap previous updater changed')
        # Reuse full-map validation/copy privately; only the old updater is
        # renamed into the live directory. All three target leaves remain.
        payload = path.parent / 'reverse'
        _bootstrap_mkdir(payload, mode=0o700)
        files = dict(plan['target_files'])
        for name in MEMBERS:
            raw, _ = _bootstrap_capture(_bootstrap_trusted(backup / name if name == 'upgrade_client.py' else INSTALL / name))
            files[name] = _bootstrap_sha(raw)
            if name != 'upgrade_client.py' and files[name] != plan['target_files'][name]:
                raise ValueError('reverse requires complete retained target leaves')
            _bootstrap_write(payload / name, raw)
        if files['upgrade_client.py'] != plan['source_files']['upgrade_client.py']:
            raise ValueError('reverse updater is not designated a174')
        stage = private_stage(payload, reader, files)
        os.chown(stage / 'upgrade_client.py', old['uid'], old['gid'])
        os.chmod(stage / 'upgrade_client.py', old['mode'])
        with (stage / 'upgrade_client.py').open('rb') as stream:
            os.fsync(stream.fileno())
        journal['phase'] = 'restore:upgrade_client.py'
        _bootstrap_save(path, journal)
        commit_member(stage, 'upgrade_client.py')
        journal['phase'] = 'restored-leaves-retained'
        _bootstrap_save(path, journal)
        return {'phase': journal['phase'], 'retained_leaves': list(LEAVES)}


def observe_bootstrap(plan, *, nonce):
    if re.fullmatch('[0-9a-f]{64}', nonce) is None:
        raise ValueError('bootstrap requires a fresh coordinator nonce')
    with _bootstrap_upgrade_lock(plan) as (host, base, config, reader, inode):
        path = journal_for(plan)
        journal = read_journal(plan, path)
        if not fenced(plan):
            raise ValueError('bootstrap updater is not persistently fenced and exited')
        expected = dict(plan['target_files'])
        reverse = plan.get('direction') == 'restore'
        if reverse:
            expected['upgrade_client.py'] = plan['source_files']['upgrade_client.py']
        files, metadata = {}, {}
        for name in MEMBERS:
            raw, metadata[name] = _bootstrap_capture(_bootstrap_trusted(INSTALL / name))
            files[name] = _bootstrap_sha(raw)
        if files != expected:
            raise ValueError('installed bootstrap closure differs')
        installed, executing = _bootstrap_load_code(INSTALL / 'upgrade_client.py', files['upgrade_client.py'])
        if not reverse:
            owner = installed.receipt_owner()
            owner.GenerationReceipt.capture(_bootstrap_capture(_bootstrap_trusted(base / 'target-receipt.json'))[0]).require_identity(
                plan['target']['generation'])
        proof = installed.load_installed_park_proof(INSTALL) if not reverse else reader.load_installed_park_proof(INSTALL)
        if proof.sha256 != plan['target_files']['park_proof.py']:
            raise ValueError('installed proof differs')
        intake = base / ('source-config.json' if reverse else 'target-config.json')
        version, _ = installed.desired_as_reader(intake, _bootstrap_read_json(_bootstrap_trusted(intake)))
        if version['files'] != (plan['source_files'] if reverse else plan['target_files']):
            raise ValueError('installed reader candidate intake differs')
        broker, gate = verify_services(plan, reader, config)
        broker_process = observe_broker_process(plan)
        if _bootstrap_sha(_bootstrap_capture(INSTALL / 'upgrade_client.py')[0]) != executing:
            raise ValueError('installed reader changed while executing')
        convergence = observe_processes(plan, reader, gate)
        return {'runtime': bootstrap_runtime_identity(), 'convergence': convergence, 'schema': 'prismabuild.client_bootstrap.observation.v1', 'nonce': nonce,
                'observed_unix': time.time(), 'host': socket.gethostname(), 'boot_id': bootstrap_boot_id(),
                'boot_reconciliation': journal.get('boot_reconciliation'),
                'plan_sha256': _bootstrap_sha(_bootstrap_canonical(plan)), 'source': plan['source'], 'target': plan['target'],
                'files': files, 'metadata': metadata, 'executing_sha256': executing,
                'captured_sha256': executing, 'reader_loaded': True, 'proof_loaded': True,
                'fenced': True, 'oneshot_exited': True, 'automatic_journal': False, 'epoch': None,
                'broker_process': broker_process, 'enrollment_sha256': _bootstrap_sha(_bootstrap_capture(CONFIG)[0]), 'journal_sha256': _bootstrap_sha(_bootstrap_capture(path)[0]),
                'upgrade_lock': {'held': True, 'inode': inode}, 'broker': broker, 'gate': gate}


def bootstrap_runtime_identity():
    running = Path('/proc/self/exe').read_bytes()
    if running[:6] != b'\x7fELF\x02\x01' or Path(sys.executable).read_bytes() != running:
        raise ValueError('bootstrap requires the actual native installed interpreter')
    machine = int.from_bytes(running[18:20], 'little')
    if machine != {'x86_64': 62, 'aarch64': 183}.get(os.uname().machine):
        raise ValueError('bootstrap interpreter ELF differs from the native kernel')
    return {'version': list(sys.version_info[:3]),
        'executable': {'path': sys.executable, 'resolved_path': str(Path(sys.executable).resolve()),
                       'bytes': len(running), 'sha256': _bootstrap_sha(running)},
        'elf_machine': machine}


def bootstrap_boot_id():
    return Path('/proc/sys/kernel/random/boot_id').read_text().strip()


def reconcile_boot(plan, *, if_needed=False):
    """Approve a reboot using fresh root facts; retain the immutable intent."""
    host, _, _ = local_context(plan, allow_boot_change=True)
    current = bootstrap_boot_id()
    path = journal_for(plan)
    if if_needed:
        recorded = read_journal(plan, path).get('boot_reconciliation', {}) if path.exists() else {}
        if current == plan['boots'][host] or recorded.get('boot_id') == current:
            # An explicit operator step may be repeated after copying a reverse
            # plan. Validate the current binding; do not fabricate a reboot.
            local_context(plan)
            observe_broker_process(plan)
            return {'reconciled': False, 'boot_id': current}
    if current == plan['boots'][host]:
        raise ValueError('reconcile-boot requires a changed boot')
    existed = path.exists()
    journal = read_journal(plan, path) if existed else {
        'plan_sha256': original_plan_hash(plan), 'phase': 'fencing',
        'original_plan': {**plan, 'direction': 'forward'}}
    if not existed:
        # No live rename is legal before this journal. A missing journal with
        # a final updater is lost recovery state, never a new backup authority.
        raw, _ = _bootstrap_capture(_bootstrap_trusted(INSTALL / 'upgrade_client.py'))
        if _bootstrap_sha(raw) != plan['source_files']['upgrade_client.py']:
            raise ValueError('pre-journal reconciliation requires the original updater')
        _bootstrap_save(path, journal)
    if journal['phase'].startswith('accepted-') and not (
            journal['phase'] == 'accepted-forward' and plan.get('direction') == 'restore'):
        raise ValueError('accepted operation reconciliation requires explicit reverse of accepted forward')
    # Stop future timer activation and wait for a natural oneshot exit before
    # acquiring its permanent lock, including after accepted-forward reboot.
    ensure_fence(plan, journal, path)
    with _bootstrap_upgrade_lock(plan, allow_boot_change=True) as (host, _, config, reader, _):
        journal = read_journal(plan, path)
        verify_services(plan, reader, config)
        for name in REPLACE:
            try:
                raw, _ = _bootstrap_capture(_bootstrap_trusted(INSTALL / name))
            except FileNotFoundError:
                if name == 'upgrade_client.py':
                    raise
                continue
            permitted = {plan['target_files'][name]}
            if name == 'upgrade_client.py':
                permitted.add(plan['source_files'][name])
            if _bootstrap_sha(raw) not in permitted:
                raise ValueError('reboot found an unknown installed bootstrap member')
        previous = journal.get('previous', {}).get('upgrade_client.py')
        if journal['phase'] != 'fencing' and previous is None:
            raise ValueError('reboot recovery requires the original updater backup')
        if previous is not None and (previous['sha256'] != plan['source_files']['upgrade_client.py']
                or _bootstrap_sha(_bootstrap_capture(
                    _bootstrap_trusted(path.parent / 'previous/upgrade_client.py'))[0]) != previous['sha256']):
            raise ValueError('reboot found a changed previous updater backup')
        record = {'original_boot_id': plan['boots'][host], 'boot_id': bootstrap_boot_id(),
                  'broker_process': observe_broker_process(plan, allow_boot_change=True),
                  'plan_sha256': original_plan_hash(plan), 'approved_unix': time.time(),
                  'direction': plan.get('direction', 'forward'), 'previous_phase': journal['phase'],
                  'initialized_without_journal': not existed,
                  'previous_map_sha256': _bootstrap_sha(_bootstrap_canonical(journal.get('previous'))),
                  'previous_journal_sha256': _bootstrap_sha(_bootstrap_capture(path)[0])}
        journal.setdefault('original_plan', {**plan, 'direction': 'forward'})
        journal.setdefault('boot_reconciliations', []).append(record)
        journal['boot_reconciliation'] = record
        _bootstrap_save(path, journal)
        return {'phase': journal['phase'], 'boot_reconciliation': record}


def observe_broker_process(plan, *, allow_boot_change=False):
    host = next(host for host, aliases in plan['roster'].items() if socket.gethostname() in aliases)
    pid = _bootstrap_systemctl('show', 'prismabuild-resource-broker.service', '--property=MainPID', '--value')
    if not pid.isdecimal() or int(pid) <= 0:
        raise ValueError('broker process identity is unavailable')
    start = Path('/proc', pid, 'stat').read_text().rpartition(')')[2].split()[19]
    observed = {'pid': int(pid), 'starttime': start}
    expected = plan['brokers'][host]
    if bootstrap_boot_id() != plan['boots'][host] and not allow_boot_change:
        journal = read_journal(plan, journal_for(plan))
        expected = journal.get('boot_reconciliation', {}).get('broker_process')
    if not allow_boot_change and observed != expected:
        raise ValueError('bootstrap broker process changed')
    return observed


def observe_processes(plan, reader, gate):
    selected = Path(plan['source' if plan.get('direction') == 'restore' else 'target']['directory'])
    names = {'supervise.py', 'worker_loop.py', 'worker.py', 'prewarm_loop.py',
             'tier_loop.py', 'local_tier_loop.py', 'pbmetrics.py'}
    census = reader.load_installed_park_proof(INSTALL).module.proc_census()
    processes = []
    for pid, start, argv in census:
        scripts = [arg for arg in argv if Path(arg).name in names]
        for script in scripts:
            row = {'pid': pid, 'starttime': start, 'argv': list(argv), 'script': Path(script).name,
                   'argument': script, 'selected': False}
            try:
                resolved = Path(script).resolve(strict=True)
                row.update(path=str(resolved), selected=(Path(script).is_absolute() and Path(script) == resolved
                                                        and resolved.is_relative_to(selected)))
                if not row['selected']:
                    row['reason'] = 'matching script basename outside selected generation'
            except (OSError, RuntimeError) as exc:
                row['reason'] = 'matching script basename cannot be resolved: ' + str(exc)
            processes.append(row)
    kinds = [row['script'] for row in processes]
    required = ('tier_loop.py', 'prewarm_loop.py', 'pbmetrics.py') if socket.gethostname() == 'dl380g10' else ()
    complete = (gate.get('draining') is False and kinds.count('supervise.py') == 1
                and 'worker_loop.py' in kinds and all(row['selected'] for row in processes)
                and all(role in kinds for role in required))
    return {'complete': complete, 'processes': processes,
            'pending': [row for row in processes if not row['selected']]}


@contextmanager
def bootstrap_operation_lock(plan, *, allow_boot_change=False):
    local_context(plan, allow_boot_change=allow_boot_change)
    path = STATE / 'bootstrap.lock'
    fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_uid != 0 or info.st_mode & 0o022:
            raise ValueError('unsafe independent bootstrap lock')
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        os.fsync(fd)
        _bootstrap_sync(STATE)
        linked = path.lstat()
        if (linked.st_dev, linked.st_ino) != (info.st_dev, info.st_ino):
            raise ValueError('independent bootstrap lock readback differs')
        yield
    finally:
        os.close(fd)


def unfence_bootstrap(plan):
    with _bootstrap_upgrade_lock(plan) as (_, base, config, reader, _):
        decision = _bootstrap_read_json(_bootstrap_trusted(base / 'accepted.json'))
        if decision != {'plan_sha256': _bootstrap_sha(_bootstrap_canonical(plan)), 'direction': plan.get('direction', 'forward')}:
            raise ValueError('root-local acceptance decision differs')
        path = journal_for(plan)
        journal = read_journal(plan, path)
        expected = plan['source_files'] if plan.get('direction') == 'restore' else plan['target_files']
        selected, _ = reader.desired_as_reader(CONFIG, config)
        if selected['files'] != expected:
            raise ValueError('selected runtime differs before unfencing')
        for name, digest in expected.items():
            if _bootstrap_sha(_bootstrap_capture(_bootstrap_trusted(INSTALL / name))[0]) != digest:
                raise ValueError('installed bytes differ before unfencing')
        if journal['phase'] == 'accepted-' + plan.get('direction', 'forward'):
            return {'phase': journal['phase']}
        if journal['phase'] != 'unfencing' and not fenced(plan):
            raise ValueError('bootstrap fence changed before release')
        journal['phase'] = 'unfencing'
        _bootstrap_save(path, journal)
        block, drops = fence_paths(plan)
        for drop in drops:
            drop.unlink(missing_ok=True)
            _bootstrap_sync(drop.parent)
        block.unlink(missing_ok=True)
        _bootstrap_sync(block.parent)
        _bootstrap_systemctl('daemon-reload')
        if journal['units'][UPGRADE_UNITS[0]].get('ActiveState') == 'active':
            _bootstrap_systemctl('start', UPGRADE_UNITS[0])
        journal['phase'] = 'accepted-' + plan.get('direction', 'forward')
        _bootstrap_save(path, journal)
        return {'phase': journal['phase']}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('operation', choices=('prepare', 'observe', 'restore', 'unfence', 'export-reservation', 'reconcile-boot'),
                        help='bootstrap operation to perform using the root-local approved plan')
    parser.add_argument('--id', required=True, help='bootstrap identity naming the root-local plan directory')
    parser.add_argument('--plan-sha256', required=True, help='SHA256 of the canonical approved plan to require before execution')
    parser.add_argument('--nonce', help='observe: fresh 64-hex coordinator nonce to bind into the observation')
    parser.add_argument('--if-needed', action='store_true', help='reconcile-boot: verify an unchanged/already reconciled boot without rewriting')
    args = parser.parse_args()
    if args.if_needed and args.operation != 'reconcile-boot':
        parser.error('--if-needed requires reconcile-boot')
    if not sys.flags.isolated:
        parser.error('use the root-local tool with /usr/bin/python3 -I')
    base = _bootstrap_trusted(CONTROL / _bootstrap_identity(args.id))
    plan = _bootstrap_read_json(_bootstrap_trusted(base / 'plan.json'))
    if _bootstrap_sha(_bootstrap_canonical(plan)) != args.plan_sha256 or plan['id'] != args.id:
        parser.error('local plan identity differs')
    _bootstrap_trusted(Path(__file__).absolute())
    if _bootstrap_sha(_bootstrap_capture(Path(__file__))[0]) != plan['tool_sha256']:
        parser.error('executing helper differs from approved plan')
    if args.operation == 'export-reservation':
        config = _bootstrap_read_json(_bootstrap_trusted(CONFIG))
        uid = config.get('reader_uid', 1000)
        if uid <= 0 or os.getuid() != uid or os.geteuid() != uid:
            raise ValueError('reservation export requires the enrolled unprivileged UID')
        store = Path(config['generation_store']).parent
        pending_path = store / 'rollout/pending-bootstrap.json'
        if not pending_path.exists():
            pending_path = store / 'rollout' / ('bootstrap-' + plan['id'] + '-' + plan.get('direction', 'forward') + '.accepted.json')
        pending = _bootstrap_read_json(pending_path)
        selected = Path(config['runtime']).resolve(strict=True)
        if str(selected) not in (plan['source']['directory'], plan['target']['directory']):
            raise ValueError('runtime pointer escaped original bootstrap intent')
        print(json.dumps(pending))
        return 0
    if os.geteuid() != 0:
        parser.error('host mutations require root')
    with bootstrap_operation_lock(plan, allow_boot_change=args.operation == 'reconcile-boot'):
        if args.operation == 'reconcile-boot':
            result = reconcile_boot(plan, if_needed=args.if_needed)
        elif args.operation == 'prepare':
            result = prepare_bootstrap(plan)
        elif args.operation == 'restore':
            result = restore_updater(plan)
        elif args.operation == 'observe':
            result = observe_bootstrap(plan, nonce=args.nonce)
        else:
            # The operator installs a root-owned terminal decision after the
            # publisher has accepted the exact direction. No shared file releases.
            result = unfence_bootstrap(plan)
    print(json.dumps(result))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
