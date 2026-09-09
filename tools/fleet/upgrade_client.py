#!/usr/bin/python3
"""Converge a worker's privileged clients to its authorized published runtime."""
from __future__ import annotations

import argparse
import base64
import fcntl
import hashlib
import json
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
#: What can still claim work off the shared queue on this box, matched on the
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
SERVING_NAMES = frozenset({'worker_loop.py', 'worker.py'})
#: Mirrors worker_loop._KEY_SAFE. The two modules cannot import each other --
#: the loop runs unprivileged out of a published generation, this runs as root
#: out of the install directory -- so the spelling is asserted between them by
#: test rather than shared.
_KEY_SAFE = frozenset('0123456789abcdefghijklmnopqrstuvwxyz'
                      'ABCDEFGHIJKLMNOPQRSTUVWXYZ._')
MAX_MEMBER = 4 * 1024 * 1024
MAX_EXPORT = 32 * 1024 * 1024
CLIENT_UPGRADE_PROTOCOL = 2


def digest(data):
    return hashlib.sha256(data).hexdigest()


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


def park_marker_name(pid, starttime, changed_unix):
    """The name a loop parked on `changed_unix` writes for itself.

    The start time is field 22 of the process's stat, so a marker left by a pid
    that has since been reused names the earlier process and not this one.
    """
    key = 'unknown' if changed_unix is None else ''.join(
        c if c in _KEY_SAFE else '_' for c in str(changed_unix))
    return f'{pid}-{starttime}-{key}'


def proc_census(root='/proc'):
    """Every live process as (pid, starttime, argv), from one snapshot.

    Taken fresh each time it is asked. A census accumulated across ticks counts
    markers for processes that have since exited, and a box does shed idle
    loops while it drains.

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
                or int(fields[19]) <= 0):
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


def serving(census):
    """The (pid, starttime) of everything in `census` that could claim work."""
    return [(pid, starttime) for pid, starttime, argv in census
            if any(PurePosixPath(part).name in SERVING_NAMES for part in argv)]


def drained(census, markers, changed_unix, active_scopes):
    """Whether this box has stopped admitting, and which processes have not.

    Two conditions, and neither is a clock. Every process that could claim has
    left a park marker for this drain, and the broker holds no active scope.

    A loop holding a claim is inside serve_once, not at the top of its poll
    where the marker is written, so a marker for every serving process already
    means no loop here holds one. Reading the shared queue would add nothing
    and would spend the resource this whole design conserves.

    `worker.py` writes no marker at all, so a live one-shot leaves this box
    un-drained whatever else is true, which is what stops it claiming under a
    generation the fleet is halfway through leaving.
    """
    unparked = [pid for pid, starttime in serving(census)
                if changed_unix is None
                or park_marker_name(pid, starttime, changed_unix) not in markers]
    return (changed_unix is not None and not unparked
            and type(active_scopes) is int and active_scopes == 0), unparked


class Upgrader:
    def __init__(self, config, *, rpc=request, command=subprocess.run, sleep=time.sleep,
                 reader=desired, procs=proc_census):
        self.config = config
        self.reader = reader
        self.procs = procs
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

    def report(self, state, **details):
        value = {'schema': 'prismabuild.client_upgrade.v1', 'state': state,
                 'host': socket.gethostname(), 'checked_unix': time.time(), **details}
        atomic(self.status, value)
        print(json.dumps(value, sort_keys=True), flush=True)
        return value

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

    def drain_evidence(self, status):
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
        settled, unparked = drained(census, markers, changed,
                                    status.get('active_scopes'))
        return {'drained': settled, 'unparked': unparked,
                'active_scopes': status.get('active_scopes')}

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

    def recover(self, transaction):
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
        self.close_drain(self.healthy())
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
        # The loops record that they parked in a directory under a root-owned
        # path they cannot create. This is the only root actor on a timer.
        self.ensure_parked_root()
        if self.journal.exists():
            return self.recover(json.loads(self.journal.read_text()))
        version, blobs = self.reader(self.config)
        installed = self.installed()
        if installed == version['files']:
            # Recover a crash after drain began but before a transaction existed.
            status = self.call('status')
            if status.get('health') is not True or not self.loaded_matches(status):
                raise RuntimeError('installed files match publication but running broker is unhealthy or stale')
            if status.get('draining') is True:
                # Nothing here needs this host stopped, so release only a drain
                # this agent is named on. An operator's stop, and any drain this
                # agent cannot show is its own, outlives a tick that finds the
                # host already current: that release is the defect this path had.
                # A broker without holders cannot say whose drain this is, and
                # released it here before holders existed.
                if status.get('maintenance_protocol', 1) < 2:
                    self.call('end')
                elif status.get('maintenance_owner') == MAINTENANCE_OWNER:
                    self.close_drain(status)
                else:
                    return self.report('held', desired=version, installed=installed,
                                       drain_owner=status.get('maintenance_owner'),
                                       **self.drain_evidence(status))
            return self.report('current', desired=version, installed=installed)
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
        transaction = {'desired': version, 'previous': previous}
        atomic(self.journal, transaction)
        try:
            self.ctl('stop')
            self.copy_files(self.state / 'staged', target_files)
            if self.installed() != version['files']:
                raise RuntimeError('installed client hash mismatch')
            self.ctl('start')
            self.close_drain(self.healthy())
            self.journal.unlink()
            sync_dir(self.state)
        except Exception as exc:
            transaction['error'] = str(exc)
            atomic(self.journal, transaction)
            return self.recover(transaction)
        return self.report('updated', desired=version, installed=self.installed())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', default='/etc/prismabuild/client-upgrade.json',
                        help='root-owned client enrollment configuration')
    parser.add_argument('--status', action='store_true',
                        help='print the latest local upgrade result without changing clients')
    parser.add_argument('--export-runtime', action='store_true', help=argparse.SUPPRESS)
    args = parser.parse_args()
    config = json.loads(trusted(args.config).read_text())
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
        updater = Upgrader(config, reader=lambda value: desired_as_reader(args.config, value))
        try:
            outcome = updater.run()
            return 1 if outcome['state'] == 'rolled_back' else 0
        except Exception as exc:
            updater.report('error', error=str(exc), recovery_pending=updater.journal.exists())
            return 1


if __name__ == '__main__':
    raise SystemExit(main())
