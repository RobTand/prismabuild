#!/usr/bin/python3
"""Converge a worker's privileged clients to its authorized published runtime."""
from __future__ import annotations

import argparse
import base64
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path, PurePosixPath
import pwd
import re
import socket
import stat
import subprocess
import sys
import tempfile
import time
import uuid

MEMBERS = {
    'resource_broker.py': 'tools/resource_broker.py',
    'resource_payload.py': 'tools/resource_payload.py',
    'gpu_memory.py': 'src/prismabuild/gpu_memory.py',
    'upgrade_client.py': 'tools/upgrade_client.py',
}
# This reader must converge fleet-wide in a bridge generation before a
# broker starts requiring a newly listed optional module.
OPTIONAL_MEMBERS = {'gpu_capacity.py': 'src/prismabuild/gpu_capacity.py'}
SERVICE = 'prismabuild-resource-broker.service'
#: The holder this agent claims on a drain it opens. It must stay stable across
#: client generations: a transaction opens a drain under one generation and can
#: close it under the next, and only the holder reopens admission.
MAINTENANCE_OWNER = 'client-upgrade'
#: Mirrors resource_broker.MAINTENANCE_UNOWNED. A drain opened through a broker
#: that predates holders carries this identity, and no caller may read it as a
#: stop somebody is holding.
MAINTENANCE_UNOWNED = 'unattributed'
#: The gate that stops this host. Root reads it directly: the status reply
#: states whether a drain is open but not when it opened, and the marker names
#: a parked loop leaves are keyed on when.
MAINTENANCE_GATE = '/run/prismabuild/maintenance.json'
#: What can still claim or prewarm work from the shared queue on this box, matched on the
#: basename of any argv element.
#:
#: Not on a generation-store prefix. A supervised loop carries the resolved
#: generation path, because the supervisor resolves it before spawning, but
#: `worker.py` is run by hand -- from the stable symlink, or from a checkout --
#: and it reaches the same live queue regardless, because the queue root is
#: written into the file. A prefix match would see every supervised loop and
#: miss every one-shot, which is the one this agent most needs to see.
#:
#: A basename match has false positives: an editor, a grep, a test named after
#: the file. Each one leaves this box reading as still admitting, and something
#: waiting on the drain keeps waiting, which is the direction to be wrong in.
SERVING_NAMES = frozenset({'worker_loop.py', 'worker.py', 'prewarm_loop.py'})
#: Mirrors worker_loop._KEY_SAFE. The two modules cannot import each other --
#: the loop runs unprivileged out of a published generation, this runs as root
#: out of the install directory -- so the spelling is asserted between them by
#: test rather than shared.
_KEY_SAFE = frozenset('0123456789abcdefghijklmnopqrstuvwxyz'
                      'ABCDEFGHIJKLMNOPQRSTUVWXYZ._')
#: Hostnames legitimately carry hyphens (`gx10-6b77`), and a marker name in the
#: rollout tree is parsed on dots, so a hyphen costs nothing there. That is the
#: whole reason this is a different set from the park marker's, which is parsed
#: on hyphens and must not contain one.
_NAME_SAFE = _KEY_SAFE | frozenset('-')
MAX_MEMBER = 4 * 1024 * 1024
MAX_EXPORT = 32 * 1024 * 1024
MAX_MARKER = 64 * 1024
CLIENT_UPGRADE_PROTOCOL = 2
MAINTENANCE_DURABLE_PROTOCOL = 1
CLIENT_UPGRADE_DURABLE_PROTOCOL = 1
CLIENT_UPGRADE_ROLLOUT_PROTOCOL = 1
#: Where the fleet records a rollout, beside the generation store rather than
#: inside it: a sealed generation is immutable, and these files are written
#: while one is being replaced.
ROLLOUT_DIRNAME = 'rollout'
AGENTS_DIRNAME = 'agents'
EPOCHS_DIRNAME = 'epochs'
ATTESTATION_SCHEMA = 'prismabuild.rollout_barrier.agent.v1'
ROLLOUT_INTENT_SCHEMA = 'prismabuild.rollout_barrier.intent.v1'
ROLLOUT_MARKER_SCHEMA = 'prismabuild.rollout_barrier.marker.v1'
ROLLOUT_LOCAL_SCHEMA = 'prismabuild.rollout_barrier.local.v1'
ROLLOUT_HOST_PHASES = frozenset({'drained', 'rotated', 'rolled-back', 'resumed', 'failed'})
ROLLOUT_DECISION_PHASES = frozenset(
    {'activated', 'rollback', 'reverted', 'resume', 'terminal'})
#: A marker path is plain descent and nothing else. The leading class refuses
#: `.`, `..` and the `.tmp-` siblings a write in progress leaves behind, so a
#: caller cannot name one and a listing cannot mistake one for a marker.
MARKER_SEGMENT = re.compile(r'[0-9A-Za-z][0-9A-Za-z._-]{0,127}\Z')
MARKER_DEPTH = 4
EPOCH_PATTERN = re.compile(r'[0-9a-f]{32}\Z')
GENERATION_PATTERN = re.compile(r'[0-9A-Za-z][0-9A-Za-z_.-]{0,254}\Z')
SHA256_PATTERN = re.compile(r'[0-9a-f]{64}\Z')


def digest(data):
    return hashlib.sha256(data).hexdigest()


def canonical_json(value):
    """The byte representation hashed by rollout intents and observations."""
    return (json.dumps(value, sort_keys=True, separators=(',', ':'),
                       ensure_ascii=True) + '\n').encode('ascii')


def _strict_json(data, what):
    def pairs(items):
        value = {}
        for key, item in items:
            if key in value:
                raise ValueError(f'duplicate key in {what}: {key}')
            value[key] = item
        return value

    try:
        return json.loads(data, object_pairs_hook=pairs)
    except (TypeError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f'invalid {what}: {exc}') from exc


def _number(value):
    return type(value) in (int, float) and math.isfinite(value) and value > 0


def validate_intent(value, *, epoch=None):
    """Validate and return one immutable rollout intent."""
    required = {'schema', 'epoch', 'from_generation', 'to_generation', 'roster',
                'agent_sha256', 'coordinator_sha256', 'drain_policy', 'armed_unix',
                'armed_by'}
    if not isinstance(value, dict) or set(value) != required:
        raise ValueError('invalid rollout intent fields')
    if value.get('schema') != ROLLOUT_INTENT_SCHEMA:
        raise ValueError('invalid rollout intent schema')
    if (not isinstance(value.get('coordinator_sha256'), str)
            or SHA256_PATTERN.fullmatch(value['coordinator_sha256']) is None):
        raise ValueError('invalid rollout coordinator hash')
    named = value.get('epoch')
    if not isinstance(named, str) or EPOCH_PATTERN.fullmatch(named) is None:
        raise ValueError('invalid rollout epoch')
    if epoch is not None and named != epoch:
        raise ValueError('rollout intent epoch does not match its directory')
    source = value.get('from_generation')
    target = value.get('to_generation')
    if (not isinstance(source, str) or GENERATION_PATTERN.fullmatch(source) is None
            or not isinstance(target, str) or GENERATION_PATTERN.fullmatch(target) is None
            or source == target):
        raise ValueError('invalid rollout generation transition')
    roster = value.get('roster')
    if (not isinstance(roster, list) or not roster or len(roster) > 256
            or any(not isinstance(host, str) or not host
                   or len(host) > 128 or sanitized(host, _NAME_SAFE) != host
                   for host in roster)
            or len(set(roster)) != len(roster)
            or roster != sorted(roster)):
        raise ValueError('invalid canonical rollout roster')
    if (not isinstance(value.get('agent_sha256'), str)
            or SHA256_PATTERN.fullmatch(value['agent_sha256']) is None):
        raise ValueError('invalid rollout agent hash')
    if value.get('drain_policy') != 'wait':
        raise ValueError('unsupported rollout drain policy')
    if not _number(value.get('armed_unix')):
        raise ValueError('invalid rollout arm time')
    if (not isinstance(value.get('armed_by'), str)
            or not value['armed_by'].strip() or len(value['armed_by']) > 256):
        raise ValueError('invalid rollout arming identity')
    return value


def intent_sha256(intent):
    return digest(canonical_json(validate_intent(intent)))


def marker_name(host, phase):
    if (not isinstance(phase, str)
            or phase not in ROLLOUT_HOST_PHASES | ROLLOUT_DECISION_PHASES):
        raise ValueError(f'unknown rollout marker phase: {phase}')
    if phase in ROLLOUT_DECISION_PHASES:
        if host is not None:
            raise ValueError('rollout decisions do not name a host')
        return phase + '.json'
    if (not isinstance(host, str) or not host
            or sanitized(host, _NAME_SAFE) != host):
        raise ValueError('invalid rollout marker host')
    return f'{host}.{phase}.json'


def make_marker(intent, phase, *, host=None, **evidence):
    """Construct a marker bound to the canonical bytes of its epoch intent."""
    intent = validate_intent(intent)
    marker_name(host, phase)
    return {'schema': ROLLOUT_MARKER_SCHEMA, 'epoch': intent['epoch'],
            'intent_sha256': intent_sha256(intent), 'phase': phase,
            **({'host': host} if host is not None else {}), **evidence}


def _observed(value, roster):
    if (not isinstance(value, dict) or set(value) != set(roster)
            or any(not isinstance(item, str) or SHA256_PATTERN.fullmatch(item) is None
                   for item in value.values())):
        raise ValueError('invalid rollout decision observations')


def validate_marker(filename, value, intent, sha=None):
    """Validate a marker's name, identity and phase-local semantics."""
    intent = validate_intent(intent)
    expected_sha = intent_sha256(intent)
    if sha is not None and sha != expected_sha:
        raise ValueError('rollout intent hash disagreement')
    if (not isinstance(value, dict) or value.get('schema') != ROLLOUT_MARKER_SCHEMA
            or value.get('epoch') != intent['epoch']
            or value.get('intent_sha256') != expected_sha):
        raise ValueError(f'invalid rollout marker identity: {filename}')
    phase = value.get('phase')
    if isinstance(phase, str) and phase in ROLLOUT_HOST_PHASES:
        host = value.get('host')
        if host not in intent['roster'] or filename != marker_name(host, phase):
            raise ValueError(f'invalid rollout host marker name: {filename}')
        expected = {'drained': intent['from_generation'],
                    'rotated': intent['to_generation'],
                    'rolled-back': intent['from_generation']}.get(phase)
        generation = value.get('generation')
        if (not isinstance(generation, str)
                or generation not in (intent['from_generation'], intent['to_generation'])
                or (expected is not None and generation != expected)):
            raise ValueError(f'invalid rollout marker generation: {filename}')
        if not _number(value.get('posted_unix')):
            raise ValueError(f'invalid rollout marker time: {filename}')
        if phase == 'drained':
            if (type(value.get('active_scopes')) is not int
                    or value.get('active_scopes') != 0
                    or type(value.get('rollout_protocol')) is not int
                    or value.get('rollout_protocol') != CLIENT_UPGRADE_ROLLOUT_PROTOCOL
                    or not _number(value.get('drain_changed_unix'))):
                raise ValueError(f'invalid drained proof: {filename}')
        if phase in {'rotated', 'rolled-back', 'resumed'}:
            if value.get('installed_agent_sha256') != intent['agent_sha256']:
                raise ValueError(f'invalid installed rollout agent: {filename}')
        if phase == 'failed' and (not isinstance(value.get('error'), str)
                                  or not value['error'].strip()):
            raise ValueError(f'invalid rollout failure: {filename}')
    elif isinstance(phase, str) and phase in ROLLOUT_DECISION_PHASES:
        if filename != marker_name(None, phase) or 'host' in value:
            raise ValueError(f'invalid rollout decision name: {filename}')
        if (not _number(value.get('decided_unix'))
                or not isinstance(value.get('decided_by'), str)
                or not value['decided_by'].strip()):
            raise ValueError(f'invalid rollout decision author: {filename}')
        generation = value.get('generation')
        direction = value.get('direction')
        expected = intent['to_generation'] if phase == 'activated' else None
        if phase in {'rollback', 'reverted'}:
            expected = intent['from_generation']
        if (expected is not None and generation != expected):
            raise ValueError(f'invalid rollout decision generation: {filename}')
        if phase == 'activated' and direction != 'forward':
            raise ValueError('activated decision is not forward')
        if phase in {'rollback', 'reverted'} and direction != 'rollback':
            raise ValueError(f'{phase} decision is not rollback')
        if phase == 'rollback' and (not isinstance(value.get('reason'), str)
                                    or not value['reason'].strip()):
            raise ValueError('rollback decision has no reason')
        if phase in {'resume', 'terminal'}:
            if direction not in {'forward', 'rollback'}:
                raise ValueError(f'invalid {phase} direction')
            expected = (intent['to_generation'] if direction == 'forward'
                        else intent['from_generation'])
            if generation != expected:
                raise ValueError(f'invalid {phase} generation')
        if phase == 'terminal':
            expected_outcome = 'completed' if direction == 'forward' else 'rolled_back'
            if value.get('outcome') != expected_outcome:
                raise ValueError('terminal outcome disagrees with direction')
        _observed(value.get('observed'), intent['roster'])
    else:
        raise ValueError(f'unknown rollout marker phase: {phase}')
    return value


def marker_sha256(value):
    return digest(canonical_json(value))


def marker_identity(value):
    """Fields which make a write-once marker the same protocol statement."""
    return {key: value.get(key) for key in (
        'schema', 'epoch', 'intent_sha256', 'phase', 'host', 'generation',
        'direction', 'outcome')}


def atomic(path, value):
    temp = path.with_name('.' + path.name + '.tmp')
    with temp.open('w') as stream:
        json.dump(value, stream, sort_keys=True)
        stream.write('\n')
        stream.flush()
        os.fsync(stream.fileno())
    temp.chmod(0o644)
    os.replace(temp, path)
    sync_dir(path.parent)


def sync_dir(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def trusted(path):
    """Local executable/config ancestors must be immutable to worker UID."""
    path = Path(path)
    for entry in (path, *path.parents):
        info = entry.lstat()
        if info.st_uid != 0 or info.st_mode & 0o022 or stat.S_ISLNK(info.st_mode):
            raise ValueError(f'unsafe root-owned installation path: {entry}')
    return path


def bounded_read(path, limit):
    with path.open('rb') as stream:
        data = stream.read(limit + 1)
    if len(data) > limit:
        raise ValueError(f'oversized runtime member: {path.name}')
    return data


def desired(config):
    # Root enrollment explicitly delegates publication authority to this store.
    # The manifest hashes detect mixed copies; they are not signatures.
    root = Path(config['runtime']).resolve(strict=True)
    store = Path(config['generation_store']).resolve(strict=True)
    if root.parent != store or root.name.startswith('.'):
        raise ValueError('published runtime is not an authorized sealed generation')
    receipt = json.loads(bounded_read(root / 'RUNTIME_VERSION.json', 1024 * 1024))
    if (receipt.get('schema') != 'prismaquant.prismabuild.runtime_version.v1'
            or receipt.get('generation') != root.name
            or not re.fullmatch('[0-9a-f]{40}', str(receipt.get('commit', '')))):
        raise ValueError('invalid published runtime receipt')
    blobs = {}
    selected = {**MEMBERS, **{name: member for name, member in OPTIONAL_MEMBERS.items()
                              if member in receipt['files']}}
    for name, member in selected.items():
        source = root / member
        if source.resolve(strict=True) != source:
            raise ValueError(f'published client member is a symlink: {member}')
        data = bounded_read(source, MAX_MEMBER)
        if digest(data) != receipt['files'].get(member):
            raise ValueError(f'published client hash mismatch: {member}')
        blobs[name] = data
    return {'generation': root.name, 'commit': receipt['commit'],
            'files': {name: digest(data) for name, data in blobs.items()}}, blobs


def encode_export(config):
    version, blobs = desired(config)
    return json.dumps({'schema': 'prismabuild.client_export.v1', 'desired': version,
                       'blobs': {name: base64.b64encode(data).decode('ascii')
                                 for name, data in blobs.items()}}).encode()


def decode_export(data):
    if len(data) > MAX_EXPORT:
        raise ValueError('oversized runtime export')
    value = json.loads(data)
    if value.get('schema') != 'prismabuild.client_export.v1':
        raise ValueError('invalid runtime export schema')
    version = value['desired']
    if (not re.fullmatch('[0-9a-f]{40}', str(version.get('commit', '')))
            or not re.fullmatch('[A-Za-z0-9][A-Za-z0-9_.-]{0,254}',
                                str(version.get('generation', '')))
            or not set(MEMBERS) <= set(version['files'])
            or not set(version['files']) <= set(MEMBERS) | set(OPTIONAL_MEMBERS)
            or set(value['blobs']) != set(version['files'])):
        raise ValueError('invalid runtime export identity or members')
    blobs = {}
    for name in version['files']:
        data = base64.b64decode(value['blobs'][name], validate=True)
        if len(data) > MAX_MEMBER or digest(data) != version['files'][name]:
            raise ValueError(f'runtime export hash mismatch: {name}')
        blobs[name] = data
    return version, blobs


def desired_as_reader(config_path, config):
    # NFS root_squash intentionally denies root access to UID-owned sealed
    # generations. Execute only this root-owned program after permanently
    # dropping the subprocess's credentials; never execute shared Python.
    uid = config.get('reader_uid', 1000)
    if type(uid) is not int or uid <= 0:
        raise ValueError('runtime reader_uid must be an unprivileged UID')
    gid = pwd.getpwuid(uid).pw_gid
    script = trusted(Path(__file__).absolute())
    with tempfile.TemporaryFile() as output:
        result = subprocess.run(
            ['/usr/bin/python3', '-I', str(script), '--export-runtime',
             '--config', str(config_path)], user=uid, group=gid, extra_groups=[],
            env={'PATH': '/usr/bin:/bin'}, cwd='/', stdout=output,
            stderr=subprocess.PIPE, timeout=30, check=False)
        if result.returncode:
            raise RuntimeError('unprivileged runtime reader failed: ' +
                               result.stderr.decode(errors='replace')[-2000:])
        output.seek(0)
        return decode_export(output.read(MAX_EXPORT + 1))


def request(endpoint, operation, **fields):
    with socket.socket(socket.AF_UNIX) as client:
        client.settimeout(10)
        client.connect(str(endpoint))
        client.sendall((json.dumps({'op': operation, **fields}) + '\n').encode())
        data = bytearray()
        while b'\n' not in data:
            chunk = client.recv(65536)
            if not chunk or len(data) + len(chunk) > 1024 * 1024:
                raise ValueError('invalid maintenance response')
            data.extend(chunk)
    reply = json.loads(data.split(b'\n')[0])
    if reply.get('ok') is not True:
        raise RuntimeError(reply.get('error', 'maintenance operation refused'))
    return reply


def gate_changed_unix(path):
    """When the drain in force closed admission, or None when that is unknown.

    A gate that is absent, unreadable, unparsable, not an object, or states no
    open drain all answer None. None is not "no drain": it is "no key", and a
    caller with no key matches no park marker, so the box reads as still
    admitting. That is the safe direction and it is why this reader does not
    restate the loop's fail-closed rules -- it cannot fail open.
    """
    try:
        value = json.loads(Path(path).read_text())
    except (OSError, ValueError):
        return None
    if not isinstance(value, dict) or value.get('draining') is not True:
        return None
    return value.get('changed_unix')


def sanitized(value, safe):
    """`value` with every character outside `safe` replaced, never dropped.

    Replacing rather than dropping keeps the length, so two distinct inputs
    cannot collapse onto one name.
    """
    return ''.join(c if c in safe else '_' for c in str(value))


def park_marker_name(pid, starttime, changed_unix):
    """The name a loop parked on `changed_unix` writes for itself.

    The start time is field 22 of the process's stat, so a marker left by a pid
    that has since been reused names the earlier process and not this one.
    """
    key = 'unknown' if changed_unix is None else sanitized(changed_unix, _KEY_SAFE)
    return f'{pid}-{starttime}-{key}'


def proc_census(root='/proc'):
    """Every live process as (pid, starttime, argv), from one snapshot.

    Taken fresh each time it is asked. A census accumulated across ticks counts
    markers for processes that have since exited, and a box does shed idle
    loops while it drains.

    The start tick is nonnegative ticks since boot, and 0 is one of its valid
    values: PID 1 reports 0 on some kernels. Only a malformed or negative tick
    is refused.

    Only a process directory proved gone may be omitted after a failed read.
    Inaccessible or malformed evidence raises: an incomplete census must never
    become an empty list that certifies a stopped box.
    """
    census = []
    entries = sorted(Path(root).iterdir())

    def starttime(line, pid):
        # comm may itself contain spaces and parentheses.
        prefix, separator, rest = line.rpartition(')')
        fields = rest.split()
        if (not separator or not prefix.startswith(f'{pid} (')
                or len(fields) <= 19 or not fields[19].isdigit()
                or int(fields[19]) < 0):
            raise ValueError(f'invalid process identity for pid {pid}')
        return fields[19]

    for entry in entries:
        if not entry.name.isdigit():
            continue
        try:
            stat_line = (entry / 'stat').read_text()
            before = starttime(stat_line, entry.name)
            cmdline = (entry / 'cmdline').read_bytes()
            after = starttime((entry / 'stat').read_text(), entry.name)
        except (OSError, ValueError):
            try:
                entry.stat()
            except FileNotFoundError:
                continue
            raise
        if before != after:
            raise ValueError(f'process identity changed for pid {entry.name}')
        argv = [part for part in cmdline.decode('utf-8', 'replace').split('\0') if part]
        census.append((int(entry.name), before, argv))
    return census


def serving(census, names=SERVING_NAMES):
    """The (pid, starttime) of everything in `census` that could claim work."""
    return [(pid, starttime) for pid, starttime, argv in census
            if any(PurePosixPath(part).name in names for part in argv)]


def drained(census, markers, changed_unix, active_scopes, names=SERVING_NAMES):
    """Whether this box has stopped admitting, and which processes have not.

    Two conditions, and neither is a clock. Every claimant and storage reader has
    left a park marker for this drain, and the broker holds no active scope.

    A loop holding a claim is inside serve_once, not at the top of its poll
    where the marker is written, so a marker for every serving process already
    means no loop here holds one. Reading the shared queue would add nothing
    and would spend the resource this whole design conserves.

    `worker.py` writes no marker at all, so a live one-shot leaves this box
    un-drained whatever else is true, which is what stops it claiming under a
    generation the fleet is halfway through leaving.

    The storage role records the same marker after its active cycle finishes.
    Legacy readers without that boundary stay unparked until they exit. This
    observation does not gate ordinary client convergence, which waits for
    broker scopes independently of these process markers.
    """
    unparked = [pid for pid, starttime in serving(census, names)
                if changed_unix is None
                or park_marker_name(pid, starttime, changed_unix) not in markers]
    return (changed_unix is not None and not unparked
            and type(active_scopes) is int and active_scopes == 0), unparked


def rollout_root(config):
    """Where this fleet records a rollout.

    A sibling of the generation store, because the store's contents are sealed
    read-only the moment they are published and these files are written while
    one generation is being replaced by another. An explicit `rollout_root`
    exists so a qualification harness can move the whole tree somewhere it can
    write, the same way `maintenance_gate` moves the drain.
    """
    override = config.get('rollout_root')
    if override:
        return Path(override)
    return Path(config['generation_store']).parent / ROLLOUT_DIRNAME


def live_generation(config):
    """Return the authorized generation currently named by the runtime pointer."""
    current = Path(config['runtime']).resolve(strict=True)
    store = Path(config['generation_store']).resolve(strict=True)
    if current.parent != store or current.name.startswith('.'):
        raise ValueError('live runtime is not an authorized sealed generation')
    return current.name


def _validate_rollout_snapshot(intent, markers, live):
    intent = validate_intent(intent)
    sha = intent_sha256(intent)
    if not isinstance(markers, dict):
        raise ValueError('invalid rollout marker map')
    for filename, value in markers.items():
        validate_marker(filename, value, intent, sha)

    def decision(phase):
        return markers.get(marker_name(None, phase))

    activated = decision('activated')
    rollback = decision('rollback')
    reverted = decision('reverted')
    resume = decision('resume')
    terminal = decision('terminal')
    if terminal is None and live not in (intent['from_generation'], intent['to_generation']):
        raise ValueError('live pointer names a third generation during rollout')
    if reverted and not rollback:
        raise ValueError('runtime reverted without a rollback decision')
    if resume:
        if resume['direction'] == 'forward':
            if not activated or rollback:
                raise ValueError('forward resume lacks an unrolled activation')
        elif not rollback or not reverted:
            raise ValueError('rollback resume lacks rollback and reversion decisions')
    if terminal:
        if not resume or terminal['direction'] != resume['direction']:
            raise ValueError('terminal decision lacks its matching resume')

    required = {
        'activated': 'drained', 'rollback': 'drained', 'reverted': 'drained',
        'terminal': 'resumed',
    }
    for phase, source_phase in required.items():
        value = decision(phase)
        if value is None:
            continue
        observed = value['observed']
        for host in intent['roster']:
            source_name = marker_name(host, source_phase)
            if source_name not in markers or observed[host] != marker_sha256(markers[source_name]):
                raise ValueError(f'{phase} observation does not match {source_name}')
    if resume:
        source_phase = 'rotated' if resume['direction'] == 'forward' else 'rolled-back'
        for host in intent['roster']:
            source_name = marker_name(host, source_phase)
            if (source_name not in markers
                    or resume['observed'][host] != marker_sha256(markers[source_name])):
                raise ValueError(f'resume observation does not match {source_name}')
        for host in intent['roster']:
            resumed_name = marker_name(host, 'resumed')
            if (resumed_name in markers
                    and markers[resumed_name]['generation'] != resume['generation']):
                raise ValueError(f'resumed marker disagrees with resume: {resumed_name}')
    return {'intent': intent, 'intent_sha256': sha, 'markers': markers,
            'live_generation': live}


def _rollout_file(path, what):
    if path.is_symlink() or not path.is_file():
        raise ValueError(f'rollout {what} is not an immutable regular file')
    data = bounded_read(path, MAX_MARKER)
    value = _strict_json(data, f'rollout {what}')
    if canonical_json(value) != data:
        raise ValueError(f'rollout {what} is not canonical JSON')
    return value


def _read_epoch(config, epoch):
    if EPOCH_PATTERN.fullmatch(str(epoch)) is None:
        raise ValueError('invalid rollout epoch name')
    root = rollout_root(config) / EPOCHS_DIRNAME / epoch
    if root.is_symlink() or not root.is_dir():
        raise ValueError(f'missing known rollout epoch: {epoch}')
    intent = validate_intent(_rollout_file(root / 'intent.json', 'intent'), epoch=epoch)
    markers = {}
    entries = sorted(root.iterdir(), key=lambda entry: entry.name)
    if len(entries) > 2048:
        raise ValueError('too many rollout markers in epoch')
    for entry in entries:
        if entry.name == 'intent.json' or entry.name.startswith('.tmp-'):
            continue
        if not entry.name.endswith('.json'):
            raise ValueError(f'unknown rollout epoch entry: {entry.name}')
        value = _rollout_file(entry, f'marker {entry.name}')
        markers[entry.name] = validate_marker(entry.name, value, intent)
    return _validate_rollout_snapshot(intent, markers, live_generation(config))


def read_rollout(config, epoch=None):
    """Read one explicit epoch, or select the sole nonterminal epoch.

    This is the unprivileged pure reader used by both the dropped-uid export
    child and private qualification fixtures.  It performs no writes.
    """
    if epoch is not None:
        return _read_epoch(config, epoch)
    epochs = rollout_root(config) / EPOCHS_DIRNAME
    try:
        entries = sorted(epochs.iterdir(), key=lambda entry: entry.name)
    except FileNotFoundError:
        return None
    if len(entries) > 1024:
        raise ValueError('too many rollout epochs')
    active = []
    for entry in entries:
        if entry.name.startswith('.tmp-'):
            continue
        if entry.is_symlink() or not entry.is_dir() or EPOCH_PATTERN.fullmatch(entry.name) is None:
            raise ValueError(f'invalid rollout epoch entry: {entry.name}')
        intent_path = entry / 'intent.json'
        if not intent_path.exists():
            finalized = [child for child in entry.iterdir()
                         if not child.name.startswith('.tmp-')]
            if not finalized:
                continue
            raise ValueError(f'missing rollout intent: {entry.name}')
        snapshot = _read_epoch(config, entry.name)
        if marker_name(None, 'terminal') not in snapshot['markers']:
            active.append(snapshot)
    if len(active) > 1:
        raise ValueError('multiple active rollout epochs')
    return active[0] if active else None


def encode_rollout(config, epoch=None):
    value = read_rollout(config, epoch)
    return canonical_json({'schema': 'prismabuild.rollout_barrier.export.v1',
                           'rollout': value})


def decode_rollout(data):
    if len(data) > MAX_EXPORT:
        raise ValueError('oversized rollout export')
    value = _strict_json(data, 'rollout export')
    if (not isinstance(value, dict)
            or set(value) != {'schema', 'rollout'}
            or value.get('schema') != 'prismabuild.rollout_barrier.export.v1'):
        raise ValueError('invalid rollout export schema')
    snapshot = value['rollout']
    if snapshot is None:
        return None
    if (not isinstance(snapshot, dict)
            or set(snapshot) != {'intent', 'intent_sha256', 'markers', 'live_generation'}):
        raise ValueError('invalid rollout export fields')
    checked = _validate_rollout_snapshot(snapshot['intent'], snapshot['markers'],
                                         snapshot['live_generation'])
    if checked['intent_sha256'] != snapshot['intent_sha256']:
        raise ValueError('rollout export intent hash mismatch')
    return checked


def rollout_as_reader(config_path, config, epoch=None):
    """Read rollout state as the configured squashed uid, never as root."""
    uid = config.get('reader_uid', 1000)
    if type(uid) is not int or uid <= 0:
        raise ValueError('runtime reader_uid must be an unprivileged UID')
    gid = pwd.getpwuid(uid).pw_gid
    script = trusted(Path(__file__).absolute())
    argv = ['/usr/bin/python3', '-I', str(script), '--export-rollout',
            '--config', str(config_path)]
    if epoch is not None:
        argv.extend(['--epoch', epoch])
    with tempfile.TemporaryFile() as output:
        result = subprocess.run(argv, user=uid, group=gid, extra_groups=[],
                                env={'PATH': '/usr/bin:/bin'}, cwd='/', stdout=output,
                                stderr=subprocess.PIPE, timeout=30, check=False)
        if result.returncode:
            raise RuntimeError('unprivileged rollout reader failed: '
                               + result.stderr.decode(errors='replace')[-2000:])
        output.seek(0)
        return decode_rollout(output.read(MAX_EXPORT + 1))


def marker_parts(relpath):
    """`relpath` split into segments, refusing anything but plain descent.

    Checked in both halves of the write. The parent builds these names itself,
    so the check is not defending against the parent; it is what lets the
    unprivileged child accept a path from its argv at all, given that the
    program it is a mode of runs as root the rest of the time.
    """
    parts = PurePosixPath(relpath).parts
    if not 1 <= len(parts) <= MARKER_DEPTH:
        raise ValueError(f'unsafe rollout marker path: {relpath!r}')
    for part in parts:
        if MARKER_SEGMENT.fullmatch(part) is None:
            raise ValueError(f'unsafe rollout marker path: {relpath!r}')
    return parts


def marker_path(root, relpath):
    """Where `relpath` lands under `root`, once it is shown to be safe."""
    return Path(root).joinpath(*marker_parts(relpath))


def make_dir(path):
    """One directory, world readable whatever umask the caller happens to carry.

    Every host reads this tree as the squashed uid. A directory created under a
    strict umask inherited from a service unit is one nobody else can enter,
    and the existence check that holds this to one write per agent version
    would then miss on every tick and write again every time. The budget would
    be gone and the status would not say so.

    Only a directory this call created is given a mode; one that was already
    there was somebody else's decision.
    """
    try:
        path.mkdir()
    except FileExistsError:
        return False
    path.chmod(0o755)
    return True


def post_marker(root, relpath, content):
    """Create one rollout marker, once, and never rewrite one.

    The tree is write-once by measurement, not by taste: issue #16 timed a
    `rename()` into a contended directory on this mount at 3 ms while a read of
    a frequently rewritten file in the same directory took 68,996 ms. Nothing
    here is appended, truncated or rewritten.

    The content lands in a `.tmp-<uuid4>` sibling and is *linked* onto its final
    name. A rename would silently replace a marker an earlier tick or another
    host already posted, and `link` refuses instead, which is the whole point:
    the name carries the claim, so a second post of one name is a second
    statement of the same fact and not a correction of it.

    Directories are created with `mkdir`, which fails EEXIST, rather than by
    renaming one into place, which succeeds onto an empty directory.
    """
    content = bytes(content)
    if len(content) > MAX_MARKER:
        raise ValueError('oversized rollout marker')
    parts = marker_parts(relpath)
    root = Path(root)
    root.parent.mkdir(parents=True, exist_ok=True)
    for step in (root, *(root.joinpath(*parts[:n]) for n in range(1, len(parts)))):
        make_dir(step)
    target = root.joinpath(*parts)
    temp = target.parent / ('.tmp-' + uuid.uuid4().hex)
    try:
        with temp.open('wb') as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        temp.chmod(0o644)
        try:
            os.link(temp, target)
        except FileExistsError:
            return False
    finally:
        temp.unlink(missing_ok=True)
    sync_dir(target.parent)
    return True


def post_child(config, relpath, stream):
    """The unprivileged half of a marker write.

    NFS root_squash denies root on the shared mount, so a write goes the same
    way the runtime read does: this root-owned program re-executed after
    permanently dropping to the reader uid. The uid is checked here rather than
    trusted from the caller, because that check is what makes it safe for this
    to be a mode of a program that otherwise runs as root.

    Content arrives on stdin rather than in the argv, so it never appears in a
    process listing and carries no length limit but this one.
    """
    uid = config.get('reader_uid', 1000)
    if type(uid) is not int or uid <= 0:
        raise SystemExit('runtime reader_uid must be an unprivileged UID')
    if os.getuid() != uid or os.geteuid() != uid:
        raise SystemExit('rollout marker writes require the configured unprivileged reader UID')
    content = stream.read(MAX_MARKER + 1)
    if len(content) > MAX_MARKER:
        raise SystemExit('oversized rollout marker')
    created = post_marker(rollout_root(config), relpath, content)
    sys.stdout.write(json.dumps({'created': created}) + '\n')
    return 0


def post_as_reader(config_path, config, relpath, content):
    """Spawn the unprivileged half, and fail loudly if it could not write."""
    uid = config.get('reader_uid', 1000)
    if type(uid) is not int or uid <= 0:
        raise ValueError('runtime reader_uid must be an unprivileged UID')
    gid = pwd.getpwuid(uid).pw_gid
    script = trusted(Path(__file__).absolute())
    marker_path(rollout_root(config), relpath)
    with tempfile.TemporaryFile() as source:
        source.write(content)
        source.flush()
        source.seek(0)
        result = subprocess.run(
            ['/usr/bin/python3', '-I', str(script), '--post-rollout-marker', str(relpath),
             '--config', str(config_path)], user=uid, group=gid, extra_groups=[],
            env={'PATH': '/usr/bin:/bin'}, cwd='/', stdin=source,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30, check=False)
    if result.returncode:
        raise RuntimeError('unprivileged rollout marker write failed: '
                           + result.stderr.decode(errors='replace')[-2000:])
    reply = _strict_json(result.stdout, 'rollout marker write reply')
    if (not isinstance(reply, dict) or set(reply) != {'created'}
            or type(reply['created']) is not bool):
        raise RuntimeError('invalid unprivileged rollout marker write reply')
    return reply['created']


def self_digest():
    """The hash of this agent's own bytes, as they are installed.

    Read from the file rather than from the install manifest, so the claim is
    about the code that is running and not about what a directory listing said
    at some other moment. A transaction replaces this file while the process
    that started it is still running the older source, which is exactly the
    moment the two answers differ.
    """
    return digest(bounded_read(Path(__file__).absolute(), MAX_MEMBER))


def attestation_name(host, client_sha):
    """`<host>.<sha>.json`, split from the right so an FQDN host still parses."""
    return f'{sanitized(host, _NAME_SAFE)}.{sanitized(client_sha, _NAME_SAFE)}.json'


def attestation_body(host, client_sha):
    return {'schema': ATTESTATION_SCHEMA, 'host': host, 'client_sha256': client_sha,
            'client_upgrade_protocol': CLIENT_UPGRADE_PROTOCOL,
            'posted_unix': time.time()}


class Upgrader:
    def __init__(self, config, *, rpc=request, command=subprocess.run, sleep=time.sleep,
                 reader=desired, procs=proc_census, poster=None,
                 rollout_reader=read_rollout):
        self.config = config
        self.reader = reader
        self.procs = procs
        # Both children need the enrollment file's path, which only the caller
        # has, so both arrive already bound to it. An agent given no poster
        # cannot write to the shared mount and says so by attesting nothing.
        self.poster = poster
        self.rollout_reader = rollout_reader
        # Capture while these are certainly the bytes executing this tick. A
        # transaction may replace the installed path beneath this process.
        self.executing_sha256 = self_digest()
        self.attestation = None
        self.gate = Path(config.get('maintenance_gate', MAINTENANCE_GATE))
        # Derived from the gate rather than named again, so a loop and this
        # agent pointed at one gate cannot disagree about where the markers are.
        self.parked_root = self.gate.parent / 'rollout' / 'parked'
        self.install = Path(config['install_dir'])
        self.state = Path(config['state_dir'])
        self.endpoint = config.get('socket', '/run/prismabuild/resources.sock')
        self.rpc = rpc
        self.command = command
        self.sleep = sleep
        self.status = self.state / 'status.json'
        self.journal = self.state / 'transaction.json'
        self.rollout_state = self.state / 'rollout-epoch.json'
        self.active_rollout = None

    def report(self, state, **details):
        # Whatever this tick decided about its own version travels in every
        # state, because the states that report a problem are the ones where
        # somebody wants to know which agent produced it.
        claim = {'attestation': self.attestation} if self.attestation else {}
        value = {'schema': 'prismabuild.client_upgrade.v1', 'state': state,
                 'host': socket.gethostname(), 'checked_unix': time.time(),
                 **claim, **details}
        atomic(self.status, value)
        print(json.dumps(value, sort_keys=True), flush=True)
        return value

    @staticmethod
    def durable_evidence(status):
        return {key: status[key] for key in ('maintenance_durable_protocol',
                'maintenance_state_path') if key in status}

    def ctl(self, verb, *, check=True):
        return self.command(['/usr/bin/systemctl', verb, SERVICE], check=check,
                            capture_output=True, text=True, timeout=45)

    def call(self, op, **fields):
        return self.rpc(self.endpoint, 'maintenance_' + op, **fields)

    @staticmethod
    def held_elsewhere(status):
        """The stated holder of the drain in `status`, when it is not ours.

        A broker that predates drain holders states none, and a drain nobody
        claimed records no stop this agent must respect. Both read as releasable
        here, which is exactly how this agent behaved before holders existed and
        is what keeps a bridge generation from deadlocking on a gate it cannot
        attribute.
        """
        if status.get('draining') is not True or status.get('maintenance_protocol', 1) < 2:
            return None
        holder = status.get('maintenance_owner')
        return None if holder in (MAINTENANCE_OWNER, MAINTENANCE_UNOWNED) else holder

    def open_drain(self):
        """Close admission, claiming the drain for this agent where it can.

        The owner travels only when a status reply proves the running broker
        understands the field. This agent converges host by host, so a new
        client talks to a broker that predates holders for a whole bridge
        generation, and that broker refuses any request field it does not know.

        A drain another holder already stated is returned untouched, because
        asking again must not become a takeover. The caller decides what to do
        with somebody else's stop.
        """
        probe = self.call('status')
        if self.held_elsewhere(probe) is not None:
            return probe
        if probe.get('maintenance_protocol', 1) >= 2:
            return self.call('begin', owner=MAINTENANCE_OWNER)
        return self.call('begin')

    def close_drain(self, status):
        """Reopen admission under this agent's own claim.

        `status` is the reply that just proved this broker healthy, so the
        decision reads the broker running now rather than the one running when
        the drain opened: a transaction restarts the service between the two,
        and a rollback starts an older one that would refuse the field. A drain
        nobody claimed is released either way, which is how a drain opened
        through an older broker still closes against a newer one.
        """
        if status.get('maintenance_protocol', 1) >= 2:
            return self.call('end', owner=MAINTENANCE_OWNER)
        return self.call('end')

    @property
    def rollout(self):
        # Resolved on use rather than in the constructor: an agent that never
        # attests never needs the shared mount named.
        return rollout_root(self.config)

    def attest(self):
        """Record, fleet-wide, that this version of the agent has run here.

        A coordinated rollout can only be armed once every roster host runs an
        agent that understands the barrier, and nothing on the shared mount
        says so today. The loops' `runtime_commit` answers for the loops. The
        generation receipt says what a host is supposed to install. What it
        actually installed is root-owned host-local state under `state_dir`,
        readable only on the box itself, because this agent is installed by a
        copy step and not by the runtime symlink.

        One write per host per version of this file, keyed on the version, so a
        tick that finds its own claim already posted costs one `stat`. That
        `stat` runs as root: the fleet root is `drwxrwxr-x rob rob`, so the
        squashed uid reads it fine and only the write needs the child.

        Best effort. Whether a rollout may be armed is somebody else's
        question, and an agent that could not answer it still has its members
        to converge.
        """
        if self.poster is None:
            return None
        try:
            relpath = f'{AGENTS_DIRNAME}/{attestation_name(socket.gethostname(), self.executing_sha256)}'
            if not marker_path(self.rollout, relpath).exists():
                body = attestation_body(socket.gethostname(), self.executing_sha256)
                self.poster(relpath, json.dumps(body, sort_keys=True).encode() + b'\n')
            self.attestation = {'marker': relpath}
        except (OSError, ValueError, KeyError, RuntimeError, subprocess.SubprocessError) as exc:
            self.attestation = {'error': str(exc)}
        return self.attestation

    def ensure_parked_root(self):
        """Create the directory a parked loop records itself in, and hand it over.

        `/run/prismabuild` is root-owned, so a loop running as the unprivileged
        worker uid cannot create this itself. This agent is the only root actor
        on a timer, which makes it the one that can. It is given to the same uid
        the runtime read already drops to.

        Best effort, and a failure is reported rather than raised: an agent that
        could not create a directory has still to converge its members.
        """
        uid = self.config.get('reader_uid', 1000)
        try:
            self.parked_root.mkdir(parents=True, exist_ok=True)
            os.chown(self.parked_root, uid, pwd.getpwuid(uid).pw_gid)
            self.parked_root.chmod(0o755)
        except (OSError, KeyError):
            return False
        return True

    def parked(self):
        try:
            return {entry.name for entry in self.parked_root.iterdir()}
        except OSError:
            return set()

    def drain_evidence(self, status, *, rollout=False):
        """What this box can still admit, given the drain `status` reports.

        Observation only. Nothing here opens or closes a drain, and nothing
        here decides anything: it puts the answer where a person and, later, a
        rollout coordinator can read it.

        An admitting box owes no proof, and the census costs a walk of /proc,
        so a box that is not draining is not asked.
        """
        if status.get('draining') is not True:
            return {}
        changed = gate_changed_unix(self.gate)
        try:
            census = self.procs()
            markers = self.parked()
        except (OSError, ValueError) as error:
            return {'drained': False, 'unparked': [],
                    'active_scopes': status.get('active_scopes'),
                    'evidence_errors': [f'process census incomplete: {error}']}
        if changed is None or gate_changed_unix(self.gate) != changed:
            return {'drained': False, 'unparked': [],
                    'active_scopes': status.get('active_scopes'),
                    'evidence_errors': ['maintenance gate missing or changed during census']}
        names = (SERVING_NAMES | {'prewarm_loop.py'}) if rollout else SERVING_NAMES
        settled, unparked = drained(census, markers, changed,
                                    status.get('active_scopes'), names)
        return {'drained': settled, 'unparked': unparked,
                'active_scopes': status.get('active_scopes')}

    def _local_rollout(self):
        try:
            data = bounded_read(self.rollout_state, MAX_MARKER)
        except FileNotFoundError:
            return None
        value = _strict_json(data, 'local rollout state')
        required = {'schema', 'epoch', 'intent_sha256', 'released'}
        if (not isinstance(value, dict) or set(value) != required
                or value.get('schema') != ROLLOUT_LOCAL_SCHEMA
                or not isinstance(value.get('epoch'), str)
                or EPOCH_PATTERN.fullmatch(value['epoch']) is None
                or not isinstance(value.get('intent_sha256'), str)
                or SHA256_PATTERN.fullmatch(value['intent_sha256']) is None
                or type(value.get('released')) is not bool):
            raise ValueError('invalid local rollout state')
        return value

    def _persist_rollout(self, snapshot, *, released=False):
        value = {'schema': ROLLOUT_LOCAL_SCHEMA,
                 'epoch': snapshot['intent']['epoch'],
                 'intent_sha256': snapshot['intent_sha256'],
                 'released': bool(released)}
        atomic(self.rollout_state, value)
        return value

    def _read_rollout(self, epoch=None):
        return self.rollout_reader(self.config, epoch=epoch)

    def _hold_for_unknown_rollout(self, error):
        """Close our gate when epoch state is unreadable, and never release it."""
        status = self.open_drain()
        holder = self.held_elsewhere(status)
        if holder is not None:
            return self.report('held', drain_owner=holder,
                               rollout_error=str(error), **self.drain_evidence(status))
        if status.get('draining') is not True:
            raise RuntimeError('broker failed to close admission after rollout read failure')
        return self.report('rollout_error', rollout_error=str(error),
                           **self.drain_evidence(status))

    def _post_epoch_marker(self, snapshot, value):
        """Post once and prove an EEXIST collision has the same semantic identity."""
        filename = marker_name(value.get('host'), value['phase'])
        validate_marker(filename, value, snapshot['intent'], snapshot['intent_sha256'])
        existing = snapshot['markers'].get(filename)
        if existing is not None:
            if marker_identity(existing) != marker_identity(value):
                raise RuntimeError(f'conflicting rollout marker already exists: {filename}')
            return existing
        if self.poster is None:
            raise RuntimeError('rollout marker writer is unavailable')
        relpath = f'{EPOCHS_DIRNAME}/{snapshot["intent"]["epoch"]}/{filename}'
        self.poster(relpath, canonical_json(value))
        refreshed = self._read_rollout(snapshot['intent']['epoch'])
        existing = refreshed['markers'].get(filename)
        if existing is None:
            raise RuntimeError(f'rollout marker write was not visible: {filename}')
        if marker_identity(existing) != marker_identity(value):
            raise RuntimeError(f'conflicting rollout marker already exists: {filename}')
        return existing

    def rotation_evidence(self, status, generation):
        """Fresh proof that every local admission process uses one generation."""
        if (status.get('health') is not True or status.get('draining') is not True
                or type(status.get('active_scopes')) is not int
                or status.get('active_scopes') != 0 or not self.loaded_matches(status)):
            return {'rotated': False, 'rotation_errors': ['broker proof is incomplete']}
        changed = gate_changed_unix(self.gate)
        if changed is None:
            return {'rotated': False, 'rotation_errors': ['maintenance gate identity is unknown']}
        try:
            census = self.procs()
            markers = self.parked()
        except (OSError, ValueError) as exc:
            return {'rotated': False,
                    'rotation_errors': [f'process census incomplete: {exc}']}
        expected_root = Path(self.config['generation_store']) / generation
        roles = {'worker_loop.py': 'worker', 'worker.py': 'one-shot-worker',
                 'supervise.py': 'supervisor', 'prewarm_loop.py': 'prewarmer'}
        observed = []
        errors = []
        counts = {role: 0 for role in roles.values()}
        for pid, starttime, argv in census:
            named = [(PurePosixPath(part).name, part) for part in argv
                     if PurePosixPath(part).name in roles]
            if not named:
                continue
            if len(named) != 1:
                errors.append(f'pid {pid} has ambiguous admission argv')
                continue
            basename, part = named[0]
            path = Path(part)
            role = roles[basename]
            counts[role] += 1
            try:
                relative = path.relative_to(expected_root)
            except ValueError:
                errors.append(f'pid {pid} {role} is outside generation {generation}')
                continue
            if not path.is_absolute() or '..' in relative.parts or relative.name != basename:
                errors.append(f'pid {pid} {role} has an untrusted generation path')
                continue
            parked = park_marker_name(pid, starttime, changed) in markers
            if role in {'worker', 'one-shot-worker', 'prewarmer'} and not parked:
                errors.append(f'pid {pid} {role} is not parked on this drain')
            observed.append({'pid': pid, 'starttime': starttime, 'role': role,
                             'path': part, 'parked': parked})
        if counts['supervisor'] != 1:
            errors.append('expected exactly one versioned supervisor')
        if counts['worker'] < 1:
            errors.append('no versioned worker loops were observed')
        if gate_changed_unix(self.gate) != changed:
            errors.append('maintenance gate changed during rotation census')
        return {'rotated': not errors, 'generation': generation,
                'gate_changed_unix': changed, 'processes': observed,
                **({'rotation_errors': errors} if errors else {})}

    def _refresh_epoch(self, snapshot):
        refreshed = self._read_rollout(snapshot['intent']['epoch'])
        if refreshed['intent_sha256'] != snapshot['intent_sha256']:
            raise ValueError('persisted rollout intent changed')
        return refreshed

    def _rollout_transitioning(self, intent, snapshot, version, *, expected, boundary):
        """Keep the durable drain while separately-read rollout views converge.

        The generation pointer and epoch records are each atomically published,
        but this updater reads them separately.  A coordinator pointer move can
        therefore legitimately land between the desired-receipt read and an
        epoch refresh.  That is not a participant install failure and must not
        publish a durable ``failed`` marker that would force rollback.
        """
        return self.report(
            'rollout_transitioning', epoch=intent['epoch'], boundary=boundary,
            expected_generation=expected,
            live_generation=snapshot['live_generation'],
            desired_generation=version['generation'])

    def _rollout_status(self, snapshot, local, version, installed, blobs):
        """Advance one host as far as the immutable coordinator decisions allow."""
        intent = snapshot['intent']
        host = socket.gethostname()
        if host not in intent['roster']:
            raise ValueError(f'host {host} is absent from rollout roster')
        if (self.executing_sha256 != intent['agent_sha256']
                or installed.get('upgrade_client.py') != intent['agent_sha256']):
            raise ValueError('rollout agent does not match executing and installed bytes')
        if local['intent_sha256'] != snapshot['intent_sha256']:
            raise ValueError('local rollout state names a different intent')

        terminal = snapshot['markers'].get(marker_name(None, 'terminal'))
        if local['released']:
            if terminal is not None:
                # A terminal marker has already hashed this host's resumed
                # proof. Its full chain was validated by the reader, so later
                # ordinary generations cannot strand stale local state.
                self.rollout_state.unlink()
                sync_dir(self.state)
                return self.report('rollout_terminal', epoch=intent['epoch'],
                                   generation=terminal['generation'],
                                   outcome=terminal['outcome'])
            if snapshot['live_generation'] not in (intent['from_generation'],
                                                    intent['to_generation']):
                raise ValueError('released rollout moved to a third generation')
            status = self.call('status')
            try:
                gate = _strict_json(bounded_read(self.gate, MAX_MARKER),
                                    'maintenance gate')
            except FileNotFoundError:
                gate = None
            if (status.get('draining') is not True and isinstance(gate, dict)
                    and gate.get('draining') is False):
                resume = snapshot['markers'].get(marker_name(None, 'resume'))
                if resume is None or resume['generation'] != snapshot['live_generation']:
                    raise ValueError('local rollout release has no matching resume decision')
                resumed = make_marker(
                    intent, 'resumed', host=host, generation=resume['generation'],
                    posted_unix=time.time(),
                    installed_agent_sha256=installed['upgrade_client.py'])
                self._post_epoch_marker(snapshot, resumed)
                # Work may have started after this durable release. Do not
                # interrupt it merely because the coordinator has not yet
                # observed every host's resumed marker.
                return self.report('rollout_resumed', epoch=intent['epoch'],
                                   generation=snapshot['live_generation'])
            local = self._persist_rollout(snapshot, released=False)

        status = self.open_drain()
        holder = self.held_elsewhere(status)
        if holder is not None:
            return self.report('held', epoch=intent['epoch'], drain_owner=holder,
                               **self.drain_evidence(status, rollout=True))
        if status.get('maintenance_durable_protocol') != MAINTENANCE_DURABLE_PROTOCOL:
            raise ValueError('rollout requires durable maintenance protocol 1')
        if status.get('draining') is not True:
            raise RuntimeError('broker failed to retain rollout drain')
        if status.get('health') is not True or not self.loaded_matches(status):
            raise RuntimeError('broker is unhealthy or stale while entering rollout drain')
        drained_evidence = self.drain_evidence(status, rollout=True)
        if not drained_evidence.get('drained'):
            return self.report('rollout_draining', epoch=intent['epoch'],
                               **drained_evidence)
        changed = gate_changed_unix(self.gate)
        drained_marker = make_marker(
            intent, 'drained', host=host, generation=intent['from_generation'],
            posted_unix=time.time(), active_scopes=0, drain_changed_unix=changed,
            rollout_protocol=CLIENT_UPGRADE_ROLLOUT_PROTOCOL)
        if marker_name(host, 'drained') not in snapshot['markers']:
            snapshot = self._refresh_epoch(snapshot)
            if snapshot['live_generation'] != intent['from_generation']:
                return self._rollout_transitioning(
                    intent, snapshot, version,
                    expected=intent['from_generation'], boundary='before-drained-proof')
        self._post_epoch_marker(snapshot, drained_marker)
        snapshot = self._refresh_epoch(snapshot)

        activated = snapshot['markers'].get(marker_name(None, 'activated'))
        rollback = snapshot['markers'].get(marker_name(None, 'rollback'))
        reverted = snapshot['markers'].get(marker_name(None, 'reverted'))
        resume = snapshot['markers'].get(marker_name(None, 'resume'))
        if resume is not None:
            direction = resume['direction']
            expected = resume['generation']
            phase = 'rotated' if direction == 'forward' else 'rolled-back'
        elif rollback is not None:
            if reverted is None:
                return self.report('rollout_rollback_wait', epoch=intent['epoch'],
                                   **drained_evidence)
            expected, phase = intent['from_generation'], 'rolled-back'
        elif activated is not None:
            expected, phase = intent['to_generation'], 'rotated'
        else:
            return self.report('rollout_drained', epoch=intent['epoch'],
                               **drained_evidence)
        if version['generation'] not in (intent['from_generation'], intent['to_generation']):
            raise ValueError('desired runtime names a third generation during rollout')
        if snapshot['live_generation'] != expected or version['generation'] != expected:
            return self._rollout_transitioning(
                intent, snapshot, version, expected=expected,
                boundary='decision-and-desired-read')
        if version['files'].get('upgrade_client.py') != intent['agent_sha256']:
            raise ValueError('rollout generation changes the participating agent')
        if installed != version['files']:
            return None  # the caller performs the ordinary verified transaction

        proof = self.rotation_evidence(status, expected)
        if not proof.get('rotated'):
            return self.report('rollout_rotating', epoch=intent['epoch'], **proof)
        phase_marker = make_marker(
            intent, phase, host=host, generation=expected, posted_unix=time.time(),
            installed_agent_sha256=installed['upgrade_client.py'],
            gate_changed_unix=proof['gate_changed_unix'], processes=proof['processes'])
        snapshot = self._refresh_epoch(snapshot)
        if snapshot['live_generation'] != expected:
            return self._rollout_transitioning(
                intent, snapshot, version, expected=expected,
                boundary='before-rotation-proof')
        self._post_epoch_marker(snapshot, phase_marker)
        snapshot = self._refresh_epoch(snapshot)
        resume = snapshot['markers'].get(marker_name(None, 'resume'))
        if resume is None:
            return self.report('rollout_' + phase.replace('-', '_'),
                               epoch=intent['epoch'], **proof)
        if resume['generation'] != expected:
            raise ValueError('resume authorizes a different generation')
        # The proof must be fresh at the release boundary, including on boot
        # when an old shared resumed marker survives loss of /run.
        status = self.call('status')
        proof = self.rotation_evidence(status, expected)
        snapshot = self._refresh_epoch(snapshot)
        if (not proof.get('rotated') or snapshot['live_generation'] != expected
                or self.installed().get('upgrade_client.py') != intent['agent_sha256']):
            return self.report('rollout_resuming', epoch=intent['epoch'], **proof)
        reopened = self.close_drain(status)
        if reopened.get('draining') is not False:
            raise RuntimeError('broker did not reopen admission for rollout resume')
        gate = _strict_json(bounded_read(self.gate, MAX_MARKER), 'maintenance gate')
        if not isinstance(gate, dict) or gate.get('draining') is not False:
            raise RuntimeError('maintenance gate is not explicitly open after rollout resume')
        self._persist_rollout(snapshot, released=True)
        snapshot = self._refresh_epoch(snapshot)
        resumed = make_marker(
            intent, 'resumed', host=host, generation=expected, posted_unix=time.time(),
            installed_agent_sha256=installed['upgrade_client.py'])
        self._post_epoch_marker(snapshot, resumed)
        return self.report('rollout_resumed', epoch=intent['epoch'], generation=expected,
                           **self.durable_evidence(reopened))

    def healthy(self):
        last = None
        for _ in range(30):
            try:
                reply = self.call('status')
                if (reply.get('health') is True and reply.get('draining') is True
                        and reply.get('active_scopes') == 0
                        and self.loaded_matches(reply)):
                    return reply
                last = repr(reply)
            except (OSError, ValueError, RuntimeError) as exc:
                last = str(exc)
            self.sleep(1)
        raise RuntimeError(f'broker did not become healthy while drained: {last}')

    @staticmethod
    def validate_files(files):
        if (not isinstance(files, dict) or not set(MEMBERS) <= set(files)
                or not set(files) <= set(MEMBERS) | set(OPTIONAL_MEMBERS)):
            raise ValueError('invalid installed client member set')
        for name, value in files.items():
            if value is None and name in OPTIONAL_MEMBERS:
                continue
            if not isinstance(value, str) or not re.fullmatch('[0-9a-f]{64}', value):
                raise ValueError('invalid installed client hash or absent required member')

    def copy_files(self, source, files):
        self.validate_files(files)
        for name, expected in files.items():
            target = self.install / name
            if expected is None:
                # Only an explicitly journaled optional member may be absent.
                # This restores absence after a failed dependency introduction.
                target.unlink(missing_ok=True)
                continue
            data = (source / name).read_bytes()
            if digest(data) != expected:
                raise RuntimeError(f'staged client hash mismatch: {name}')
            temporary = self.install / ('.' + name + '.upgrade')
            with temporary.open('wb') as stream:
                stream.write(data)
                stream.flush()
                os.fsync(stream.fileno())
            temporary.chmod(0o644)
            os.replace(temporary, target)
        sync_dir(self.install)

    def recover(self, transaction, *, keep_drain=False):
        previous = transaction.get('previous')
        self.validate_files(previous)
        actual = {name: digest((self.state / 'previous' / name).read_bytes())
                  if expected is not None else None for name, expected in previous.items()}
        if actual != previous:
            raise RuntimeError('previous client hash mismatch; recovery requires operator repair')
        # The persisted broker gate survives restart. If the service is alive,
        # independently prove the gate and idleness again before stopping it.
        if self.ctl('is-active', check=False).returncode == 0:
            status = self.open_drain()
            holder = self.held_elsewhere(status)
            if holder is not None:
                raise RuntimeError('rollback deferred: maintenance drain is held by ' + holder)
            if status.get('active_scopes') != 0 or status.get('draining') is not True:
                raise RuntimeError('rollback deferred: broker still owns active work')
            self.ctl('stop')
        self.copy_files(self.state / 'previous', previous)
        if self.installed(previous) != previous:
            raise RuntimeError('restored client hash mismatch')
        self.ctl('start')
        healthy = self.healthy()
        if not keep_drain:
            self.close_drain(healthy)
        self.journal.unlink()
        sync_dir(self.state)
        return self.report('rolled_back', desired=transaction['desired'],
                           installed=self.installed(), error=transaction.get('error'))

    def loaded_matches(self, reply):
        expected = {name: value for name, value in self.installed().items()
                    if name != 'upgrade_client.py'}
        return reply.get('installed_sha256') == expected

    def installed(self, names=None):
        if names is None:
            names = [*MEMBERS, *(name for name in OPTIONAL_MEMBERS
                                if (self.install / name).exists())]
        result = {}
        for name in names:
            path = self.install / name
            if name in OPTIONAL_MEMBERS and not path.exists():
                result[name] = None
            else:
                result[name] = digest(path.read_bytes())
        return result

    def run(self):
        self.active_rollout = None
        try:
            return self._run_once()
        except Exception as exc:
            snapshot = self.active_rollout
            if snapshot is not None:
                try:
                    failed = make_marker(
                        snapshot['intent'], 'failed', host=socket.gethostname(),
                        generation=snapshot['live_generation'], posted_unix=time.time(),
                        error=str(exc) or type(exc).__name__)
                    self._post_epoch_marker(self._refresh_epoch(snapshot), failed)
                except Exception:
                    # The original error remains the local status and exit.
                    # A marker read/write failure is itself fail-closed because
                    # rollout_state and the owned gate remain in place.
                    pass
            raise

    def _run_once(self):
        # The loops record that they parked in a directory under a root-owned
        # path they cannot create. This is the only root actor on a timer.
        self.ensure_parked_root()
        # Before anything can replace this file: the claim is about the bytes
        # that are running, and a transaction changes the bytes on disk.
        self.attest()
        try:
            local_rollout = self._local_rollout()
            rollout = self._read_rollout(local_rollout['epoch'] if local_rollout else None)
            if local_rollout is not None and rollout is None:
                raise ValueError('persisted rollout epoch disappeared')
            if rollout is not None:
                if local_rollout is None:
                    # Durable local ownership precedes the drain proof this host
                    # publishes. Recovery can therefore never mistake an epoch
                    # hold for an ordinary client-upgrade hold.
                    local_rollout = self._persist_rollout(rollout)
                elif local_rollout['intent_sha256'] != rollout['intent_sha256']:
                    raise ValueError('persisted rollout identity disagrees with intent')
                self.active_rollout = rollout
        except (OSError, ValueError, KeyError, TypeError, RuntimeError,
                subprocess.SubprocessError) as exc:
            return self._hold_for_unknown_rollout(exc)
        if (rollout is not None and local_rollout['released']
                and marker_name(None, 'terminal') in rollout['markers']):
            terminal = rollout['markers'][marker_name(None, 'terminal')]
            self.rollout_state.unlink()
            sync_dir(self.state)
            self.active_rollout = None
            return self.report('rollout_terminal', epoch=rollout['intent']['epoch'],
                               generation=terminal['generation'],
                               outcome=terminal['outcome'])
        if rollout is not None and not local_rollout['released']:
            # Establish the durable local hold before any further shared read
            # or desired-member export can fail. A valid persisted epoch must
            # never reach the ordinary error path while admission is open.
            held = self.open_drain()
            holder = self.held_elsewhere(held)
            if holder is not None:
                return self.report('held', epoch=rollout['intent']['epoch'],
                                   drain_owner=holder,
                                   **self.drain_evidence(held, rollout=True))
            if (held.get('draining') is not True
                    or held.get('maintenance_durable_protocol')
                    != MAINTENANCE_DURABLE_PROTOCOL):
                raise RuntimeError('rollout durable local hold could not be established')
        if self.journal.exists():
            transaction = _strict_json(bounded_read(self.journal, MAX_MARKER),
                                       'client transaction journal')
            keep = rollout is not None or transaction.get('rollout_epoch') is not None
            result = self.recover(transaction, keep_drain=keep)
            if rollout is not None:
                failed = make_marker(
                    rollout['intent'], 'failed', host=socket.gethostname(),
                    generation=rollout['intent']['from_generation'],
                    posted_unix=time.time(),
                    error=transaction.get('error', 'interrupted client transaction recovered'))
                self._post_epoch_marker(self._refresh_epoch(rollout), failed)
                return self.report('rollout_failed', epoch=rollout['intent']['epoch'],
                                   installed=result.get('installed'), error=failed['error'])
            return result
        version, blobs = self.reader(self.config)
        installed = self.installed()
        if rollout is not None:
            outcome = self._rollout_status(
                rollout, local_rollout, version, installed, blobs)
            if outcome is not None:
                return outcome
        if installed == version['files']:
            # Recover a crash after drain began but before a transaction existed.
            status = self.call('status')
            if status.get('health') is not True or not self.loaded_matches(status):
                raise RuntimeError('installed files match publication but running broker is unhealthy or stale')
            if status.get('draining') is not True:
                try:
                    self.gate.lstat()
                except FileNotFoundError:
                    # /run disappears at boot. A broker with no gate starts
                    # in-memory open, while workers now wait for an explicit
                    # open gate. Reuse the owned drain/health handshake; never
                    # turn an unavailable read into permission to initialize.
                    status = self.open_drain()
                    if status.get('draining') is not True:
                        raise RuntimeError('broker failed to close admission for gate initialization')
                    if status.get('health') is not True or not self.loaded_matches(status):
                        raise RuntimeError('broker became unhealthy or stale during gate initialization')
            if status.get('draining') is True:
                # Nothing here needs this host stopped, so release only a drain
                # this agent is named on. An operator's stop, and any drain this
                # agent cannot show is its own, outlives a tick that finds the
                # host already current: that release is the defect this path had.
                # A broker without holders cannot say whose drain this is, and
                # released it here before holders existed.
                if (status.get('maintenance_protocol', 1) >= 2
                        and status.get('maintenance_owner') != MAINTENANCE_OWNER):
                    return self.report('held', desired=version, installed=installed,
                                       drain_owner=status.get('maintenance_owner'),
                                       **self.drain_evidence(status))
                if type(status.get('active_scopes')) is not int or status['active_scopes'] != 0:
                    return self.report('draining', desired=version, installed=installed,
                                       **self.drain_evidence(status))
                reopened = self.close_drain(status)
                if reopened.get('draining') is not False:
                    raise RuntimeError('broker did not reopen admission')
            gate = json.loads(bounded_read(self.gate, MAX_MARKER))
            if not isinstance(gate, dict) or gate.get('draining') is not False:
                raise RuntimeError('maintenance gate is not explicitly open')
            return self.report('current', desired=version, installed=installed,
                               **self.durable_evidence(status))
        # A broker that has made a hold survive reboot cannot safely be replaced
        # by an older reader of only /run.  Check before staging or closing
        # admission; the installed updater is the downgrade fence after its
        # bridge generation has converged.
        durable_marker = rb'(?m)^MAINTENANCE_DURABLE_PROTOCOL[ \t]*=[ \t]*1[ \t]*$'
        updater_marker = rb'(?m)^CLIENT_UPGRADE_DURABLE_PROTOCOL[ \t]*=[ \t]*1[ \t]*$'
        if re.search(durable_marker, (self.install / 'resource_broker.py').read_bytes()) is not None:
            running = self.call('status')
            if running.get('maintenance_durable_protocol') != MAINTENANCE_DURABLE_PROTOCOL:
                raise ValueError('durable maintenance broker capability is missing or stale')
            if (re.search(durable_marker, blobs['resource_broker.py']) is None
                    or re.search(updater_marker, blobs['upgrade_client.py']) is None):
                raise ValueError('durable maintenance broker refuses non-durable candidate')
        rollout_marker = rb'(?m)^CLIENT_UPGRADE_ROLLOUT_PROTOCOL[ \t]*=[ \t]*1[ \t]*$'
        if (re.search(rollout_marker, (self.install / 'upgrade_client.py').read_bytes())
                is not None and re.search(rollout_marker, blobs['upgrade_client.py']) is None):
            raise ValueError('rollout-aware updater refuses an epoch-unaware candidate')
        # Keep an explicit union so additions and removals both carry their
        # previous existence through failures and process restarts.
        names = sorted(set(installed) | set(version['files']))
        if set(names) & set(OPTIONAL_MEMBERS):
            # A crash can execute the newly copied updater while this journal
            # still exists. Never replace it with a pre-bridge recovery reader
            # during a transaction that records optional member existence.
            pattern = rb'(?m)^CLIENT_UPGRADE_PROTOCOL[ \t]*=[ \t]*2[ \t]*$'
            for code in (blobs['upgrade_client.py'],
                         (self.install / 'upgrade_client.py').read_bytes()):
                if re.search(pattern, code) is None:
                    raise ValueError('optional client transaction requires dependency-aware recovery protocol 2')
        previous = self.installed(names)
        target_files = {name: version['files'].get(name) for name in names}
        self.validate_files(target_files)
        for directory in ('staged', 'previous'):
            path = self.state / directory
            path.mkdir(exist_ok=True, mode=0o700)
            for name in names:
                hashes = target_files if directory == 'staged' else previous
                target = path / name
                if hashes[name] is None:
                    target.unlink(missing_ok=True)
                    continue
                data = blobs[name] if directory == 'staged' else (self.install / name).read_bytes()
                with target.open('wb') as stream:
                    stream.write(data)
                    stream.flush()
                    os.fsync(stream.fileno())
                target.chmod(0o600)
            sync_dir(path)
        # No new scope can cross this operation's Authority.lock. Existing
        # attempts finish normally while subsequent timer ticks observe them.
        status = self.open_drain()
        holder = self.held_elsewhere(status)
        if holder is not None:
            # Somebody stopped this host on purpose. Upgrading through that stop
            # would restart the broker service under them, and this agent could
            # not reopen admission afterwards anyway.
            return self.report('held', desired=version, installed=installed, drain_owner=holder,
                               **self.drain_evidence(status))
        if status.get('draining') is not True:
            raise RuntimeError('broker failed to close admission')
        if status.get('active_scopes') != 0:
            return self.report('draining', desired=version, installed=installed,
                               **self.drain_evidence(status))
        transaction = {'desired': version, 'previous': previous,
                       **({'rollout_epoch': rollout['intent']['epoch']}
                          if rollout is not None else {})}
        atomic(self.journal, transaction)
        try:
            self.ctl('stop')
            self.copy_files(self.state / 'staged', target_files)
            if self.installed() != version['files']:
                raise RuntimeError('installed client hash mismatch')
            self.ctl('start')
            healthy = self.healthy()
            reopened = healthy if rollout is not None else self.close_drain(healthy)
            self.journal.unlink()
            sync_dir(self.state)
        except Exception as exc:
            transaction['error'] = str(exc)
            atomic(self.journal, transaction)
            result = self.recover(transaction, keep_drain=rollout is not None)
            if rollout is not None:
                failed = make_marker(
                    rollout['intent'], 'failed', host=socket.gethostname(),
                    generation=rollout['intent']['from_generation'],
                    posted_unix=time.time(), error=str(exc))
                self._post_epoch_marker(self._refresh_epoch(rollout), failed)
                return self.report('rollout_failed', epoch=rollout['intent']['epoch'],
                                   installed=result.get('installed'), error=str(exc))
            return result
        if rollout is not None:
            return self._rollout_status(
                self._refresh_epoch(rollout), local_rollout, version,
                self.installed(), blobs)
        return self.report('updated', desired=version, installed=self.installed(),
                           **self.durable_evidence(reopened))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', default='/etc/prismabuild/client-upgrade.json',
                        help='root-owned client enrollment configuration')
    parser.add_argument('--status', action='store_true',
                        help='print the latest local upgrade result without changing clients')
    parser.add_argument('--export-runtime', action='store_true', help=argparse.SUPPRESS)
    parser.add_argument('--export-rollout', action='store_true', help=argparse.SUPPRESS)
    parser.add_argument('--epoch', help=argparse.SUPPRESS)
    parser.add_argument('--post-rollout-marker', metavar='RELPATH', help=argparse.SUPPRESS)
    args = parser.parse_args()
    config = json.loads(trusted(args.config).read_text())
    if args.post_rollout_marker:
        return post_child(config, args.post_rollout_marker, sys.stdin.buffer)
    if args.export_rollout:
        uid = config.get('reader_uid', 1000)
        if os.getuid() != uid or os.geteuid() != uid or uid == 0:
            raise SystemExit('rollout export requires the configured unprivileged reader UID')
        data = encode_rollout(config, args.epoch)
        if len(data) > MAX_EXPORT:
            raise SystemExit('oversized rollout export')
        sys.stdout.buffer.write(data)
        return 0
    if args.export_runtime:
        uid = config.get('reader_uid', 1000)
        if os.getuid() != uid or os.geteuid() != uid or uid == 0:
            raise SystemExit('runtime export requires the configured unprivileged reader UID')
        data = encode_export(config)
        if len(data) > MAX_EXPORT:
            raise SystemExit('oversized runtime export')
        sys.stdout.buffer.write(data)
        return 0
    state = trusted(config['state_dir'])
    if args.status:
        print((state / 'status.json').read_text(), end='')
        return 0
    if os.geteuid() != 0:
        raise SystemExit('client upgrades require the installed root service')
    trusted(config['install_dir'])
    trusted(Path(__file__).absolute())
    with (state / 'upgrade.lock').open('a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return 0
        updater = Upgrader(
            config,
            reader=lambda value: desired_as_reader(args.config, value),
            poster=lambda relpath, content: post_as_reader(args.config, config, relpath, content),
            rollout_reader=lambda value, epoch=None: rollout_as_reader(
                args.config, value, epoch))
        try:
            outcome = updater.run()
            return 1 if outcome['state'] == 'rolled_back' else 0
        except Exception as exc:
            updater.report('error', error=str(exc), recovery_pending=updater.journal.exists())
            return 1


if __name__ == '__main__':
    raise SystemExit(main())
