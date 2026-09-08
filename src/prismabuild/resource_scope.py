"""Per-attempt kernel accounting and aggregate memory containment.

A narrow root broker puts direct processes and daemon-created Docker containers
under the same systemd slice. All counters below include both, without census
races or trusting a process name to establish ownership.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import socket
import sys
import time
from typing import Any

BROKER_SOCKET = Path('/run/prismabuild/resources.sock')
SCOPE_RE = re.compile(r'prismabuild-job[a-f0-9]{32}\.slice')
MAX_MESSAGE_BYTES = 65536


class ResourceUnavailable(OSError):
    """Broker deferred unstarted work for maintenance or a protocol upgrade."""


def broker_request(request: dict, *, socket_path: Path = BROKER_SOCKET) -> dict:
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.settimeout(10)
        client.connect(str(socket_path))
        client.sendall(json.dumps(request, separators=(',', ':')).encode() + b'\n')
        data = bytearray()
        while b'\n' not in data:
            chunk = client.recv(min(4096, MAX_MESSAGE_BYTES + 1 - len(data)))
            if not chunk:
                raise OSError('resource broker closed without a response')
            data.extend(chunk)
            if len(data) > MAX_MESSAGE_BYTES:
                raise OSError('resource broker response exceeds 64 KiB')
    response = json.loads(data.split(b'\n', 1)[0])
    if (request.get('op') == 'create' and request.get('recovery_protocol') == 1
            and isinstance(response, dict) and response.get('ok') is False
            and response.get('error') == 'unknown request field'):
        feature = ('GPU memory budget' if 'gpu_memory_max_bytes' in request
                   else 'durable creation recovery')
        raise ResourceUnavailable(f'installed broker needs {feature} support')
    if (isinstance(response, dict) and response.get('ok') is False
            and response.get('maintenance') is True and response.get('retryable') is True):
        raise ResourceUnavailable(f'resource broker is in maintenance: {response}')
    if not isinstance(response, dict) or response.get('ok') is not True:
        raise OSError(f'resource broker refused request: {response}')
    return response


def read_cgroup(path: Path) -> dict[str, Any]:
    """Read hierarchical counters for the whole job, including its containers."""
    cpu = dict(line.split() for line in (path / 'cpu.stat').read_text().splitlines())
    events = dict(line.split() for line in (path / 'memory.events').read_text().splitlines())
    local_events = dict(line.split() for line in (path / 'memory.events.local').read_text().splitlines())
    counters = {
        'cpu_seconds': int(cpu['usage_usec']) / 1_000_000,
        'memory_current_bytes': int((path / 'memory.current').read_text()),
        'memory_peak_bytes': int((path / 'memory.peak').read_text()),
        'oom_kill': int(events.get('oom_kill', 0)),
        'oom_local': int(local_events['oom']),
    }
    # The kernel has always published the split; nothing read it, so an action
    # that spent its time in the kernel and one that spent it in user code
    # arrived at the receipt looking identical. Absent rather than zero on a
    # kernel that does not publish it: a missing split is not an idle kernel.
    for field, key in (('cpu_user_seconds', 'user_usec'),
                       ('cpu_system_seconds', 'system_usec')):
        if key in cpu:
            counters[field] = int(cpu[key]) / 1_000_000
    return counters


#: The ``/proc/<pid>/io`` counters worth carrying. ``rchar``/``wchar`` count
#: the bytes the process asked for, and are exact wherever the file lives;
#: ``read_bytes``/``write_bytes`` count what reached storage, and are zero on a
#: tmpfs for the same write. Both, because neither answers the other's
#: question.
IO_COUNTERS = ('rchar', 'wchar', 'syscr', 'syscw', 'read_bytes', 'write_bytes')


CGROUP_ROOT = Path('/sys/fs/cgroup')


def cgroup_membership(path: Path) -> str:
    """The scope as ``/proc/<pid>/cgroup`` spells it, for membership matching."""
    try:
        return '/' + str(Path(path).resolve().relative_to(CGROUP_ROOT))
    except ValueError:
        return ''


def procs_in_cgroup(membership: str) -> list[int]:
    """Every process whose own ``/proc`` entry names this scope or a leaf of it.

    The broker keeps the payload leaf ``drwx------ root root``, so the leaf the
    action actually runs in cannot be listed, entered or read by the worker
    that launched it -- and that leaf is where every one of its processes is.
    ``/proc/<pid>/cgroup`` is world-readable and says the same thing from the
    other side, so membership is read from the processes rather than from a
    directory this uid may not open.
    """
    if not membership:
        return []
    found: list[int] = []
    try:
        entries = os.listdir('/proc')
    except OSError:
        return []
    for entry in entries:
        if not entry.isdigit():
            continue
        try:
            text = Path('/proc', entry, 'cgroup').read_text()
        except OSError:
            continue  # the process exited between listdir and read
        for line in text.splitlines():
            parts = line.split(':', 2)
            if len(parts) != 3 or parts[0] != '0':
                continue
            where = parts[2]
            if where == membership or where.startswith(membership + '/'):
                found.append(int(entry))
            break
    return found


def scope_pids(path: Path) -> list[int]:
    """Every process in the scope, including the broker's payload leaf.

    The payload is a child of the root broker rather than of the pool worker,
    so no process tree from the worker reaches it. The cgroup is what both have
    in common, and it is hierarchical -- but only where it can be read. A
    subdirectory this uid cannot open is not an empty one, so the walk records
    that it was refused and the membership scan supplies what it could not see.
    """
    found: list[int] = []
    blocked = False
    stack = [Path(path)]
    while stack:
        group = stack.pop()
        try:
            entries = list(group.iterdir())
        except OSError:
            blocked = True
            continue
        for entry in entries:
            if entry.is_dir():
                stack.append(entry)
        try:
            text = (group / 'cgroup.procs').read_text()
        except OSError:
            blocked = True
            continue
        for line in text.split():
            try:
                found.append(int(line))
            except ValueError:
                continue
    if blocked:
        return sorted(set(found) | set(procs_in_cgroup(cgroup_membership(path))))
    return found


def read_process_io(pid: int) -> tuple[str, int, dict[str, int] | None] | None:
    """A process's identity, its parent, and its counters when readable.

    ``starttime`` from ``/proc/<pid>/stat`` is what makes the key exact: pids
    are recycled, and folding a new process's counters into an old one's
    accounting would silently invent I/O the action never did.

    The parent comes back because ``/proc/<pid>/io`` is not a per-process
    counter alone: on reap the kernel adds a child's totals into its parent's,
    exactly as ``RUSAGE_CHILDREN`` does. So a process whose parent is also in
    the scope keeps contributing through that parent after it exits, and adding
    its last reading to a retired total as well would count it twice. Measured
    on the fleet: a 64 MiB write reported as 129 MiB.

    Identity and counters are separate answers because they fail separately.
    ``/proc/<pid>/io`` needs ptrace-read access and a process this uid may not
    inspect refuses it while ``stat`` still answers. ``None`` for the counters
    means "here, but not readable"; ``None`` for the whole result means gone.
    """
    try:
        stat = Path(f'/proc/{pid}/stat').read_text()
    except (OSError, ValueError):
        return None
    try:
        # The command field is parenthesised and may itself contain spaces, so
        # the fields after it are found from the last ')' rather than by split.
        fields = stat[stat.rindex(')') + 1:].split()
        parent, starttime = int(fields[1]), fields[19]
    except (ValueError, IndexError):
        return None
    identity = f'{pid}:{starttime}'
    try:
        counters = dict(line.split(':', 1) for line in
                        Path(f'/proc/{pid}/io').read_text().splitlines() if ':' in line)
    except (OSError, ValueError):
        return identity, parent, None
    values: dict[str, int] = {}
    for name in IO_COUNTERS:
        try:
            values[name] = int(counters[name])
        except (KeyError, ValueError):
            return identity, parent, None
    return identity, parent, values


def _atomic_json(path: Path, record: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f'.{path.name}.{os.getpid()}.tmp')
    try:
        temp.write_text(json.dumps(record, sort_keys=True) + '\n')
        os.replace(temp, path)
    finally:
        temp.unlink(missing_ok=True)


class ResourceScope:
    """One exact key+nonce kernel slice; sampling never signals work.

    Call create before Popen, then wrap_argv (an unprivileged stdio proxy). sample atomically refreshes the
    telemetry file. Caller owns polling and must retain a reservation until
    children and owned containers are stopped and release succeeds.
    """
    def __init__(self, action_key: str, nonce: str, memory_max_bytes: int,
                 telemetry_path: Path, *, docker_owner: str | None = None,
                 shape_key: str | None = None, socket_path: Path = BROKER_SOCKET,
                 gpu_memory_max_bytes: int | None = None):
        if not re.fullmatch('[a-f0-9]{64}', action_key) or not re.fullmatch('[a-f0-9]{32}', nonce):
            raise ValueError('resource scope needs an action key and 32-hex attempt nonce')
        if isinstance(memory_max_bytes, bool) or not isinstance(memory_max_bytes, int) or memory_max_bytes <= 0:
            raise ValueError('resource scope memory limit must be a positive integer')
        if gpu_memory_max_bytes is not None and (type(gpu_memory_max_bytes) is not int
                or not 0 < gpu_memory_max_bytes <= 2**63 - 1):
            raise ValueError('resource scope GPU memory limit must be positive integer bytes')
        self.action_key = action_key
        self.nonce = nonce
        self.memory_max_bytes = memory_max_bytes
        self.gpu_memory_max_bytes = memory_max_bytes if gpu_memory_max_bytes is None else gpu_memory_max_bytes
        self._explicit_gpu_budget = gpu_memory_max_bytes is not None
        self.telemetry_path = Path(telemetry_path)
        self.socket_path = Path(socket_path)
        self.docker_owner = docker_owner
        self.shape_key = shape_key
        self.started = time.monotonic()
        self.unit: str | None = None
        self.cgroup_path: Path | None = None
        self.token: str | None = None
        self._last: dict[str, Any] = {}
        self._reason: str | None = None
        self._process_io: dict[str, Any] | None = None

    def _request(self, op: str, **extra) -> dict:
        request = {'op': op, 'action_key': self.action_key, 'nonce': self.nonce, **extra}
        if self.token is not None:
            request['token'] = self.token
        return broker_request(request, socket_path=self.socket_path)

    def create(self) -> dict:
        if self.token is not None:
            raise RuntimeError('resource scope is already created')
        extra = {'gpu_memory_max_bytes': self.gpu_memory_max_bytes} if self._explicit_gpu_budget else {}
        response = self._request('create', memory_max_bytes=self.memory_max_bytes, recovery_protocol=1, **extra)
        self._adopt_created_scope(response)
        return self.control_record()

    def recover_create(self) -> bool:
        """Recover authority or fence a missing attempt against delayed creation."""
        extra = {'gpu_memory_max_bytes': self.gpu_memory_max_bytes} if self._explicit_gpu_budget else {}
        response = self._request('recover_create', memory_max_bytes=self.memory_max_bytes, **extra)
        if response.get('missing') is True:
            expected = 'prismabuild-job' + hashlib.sha256((self.action_key + self.nonce).encode()).hexdigest()[:32] + '.slice'
            if response.get('scope_id') != expected:
                raise OSError('resource broker returned invalid creation absence evidence')
            return False
        self._adopt_created_scope(response)
        return True

    def _adopt_created_scope(self, response: dict) -> None:
        unit = response.get('scope_id', '')
        token = response.get('token', '')
        path = Path(response.get('cgroup_path', ''))
        expected = 'prismabuild-job' + hashlib.sha256((self.action_key + self.nonce).encode()).hexdigest()[:32] + '.slice'
        if (not isinstance(unit, str) or unit != expected
                or not isinstance(token, str) or re.fullmatch('[a-f0-9]{64}', token) is None
                or path != Path('/sys/fs/cgroup/prismabuild.slice') / unit):
            raise OSError('resource broker returned an invalid scope identity')
        acknowledged_gpu = response.get('gpu_memory_max_bytes')
        if (acknowledged_gpu is not None and (type(acknowledged_gpu) is not int
                                            or acknowledged_gpu != self.gpu_memory_max_bytes)
                or self._explicit_gpu_budget and acknowledged_gpu is None):
            raise OSError('resource broker did not acknowledge the exact GPU memory budget')
        self.unit, self.token, self.cgroup_path = unit, token, path

    def control_record(self) -> dict:
        """Lease metadata for exact-attempt recovery; retain with the reservation."""
        return {'action_key': self.action_key, 'nonce': self.nonce,
                'scope_id': self.unit, 'cgroup_path': str(self.cgroup_path),
                'token': self.token, 'socket_path': str(self.socket_path),
                'memory_max_bytes': self.memory_max_bytes,
                'gpu_memory_max_bytes': self.gpu_memory_max_bytes}

    def wrap_argv(self, argv: list[str]) -> list[str]:
        if self.token is None:
            raise RuntimeError('create the resource scope before launching')
        root = Path(__file__).resolve().parents[2]
        helper = root / 'tools/resource_exec.py'
        if not helper.is_file():
            helper = root / 'tools/fleet/resource_exec.py'
        return [sys.executable, str(helper), '--socket', str(self.socket_path),
                '--action-key', self.action_key, '--nonce', self.nonce,
                '--token', self.token, '--', *argv]

    def _prior_process_io(self) -> dict[str, Any]:
        """What an earlier sampler already accounted for *this* attempt.

        The telemetry file is the state, not this object: the pool builds one
        scope to launch the attempt and rebuilds another from the claim record
        to sample it after the child exits, and a total that restarted between
        the two would report the last two seconds as the whole run.

        The file is named for the action, not the attempt, and is never
        unlinked, so a retry on the same host finds its predecessor's readings
        sitting there. Only a record carrying this attempt's nonce is this
        attempt's: anything else is a previous run, and adopting it would open
        attempt two with attempt one's retired bytes and retire attempt one's
        dead roots a second time.
        """
        if self._process_io is not None:
            return self._process_io
        prior = None
        try:
            record = json.loads(self.telemetry_path.read_text())
            if record.get('nonce') == self.nonce:
                prior = record.get('process_io')
        except (OSError, ValueError, AttributeError):
            prior = None
        self._process_io = prior if isinstance(prior, dict) else {}
        return self._process_io

    def sample_process_io(self) -> dict[str, Any]:
        """Sum the scope's processes now, keeping what departed ones earned.

        ``/proc/<pid>/io`` disappears the moment a process is reaped, so this
        has to run while the work is alive; the finish path is already too
        late. A process that both starts and ends between two samples is
        therefore not counted, and ``processes_observed`` says how many were.

        Departure is not the same as loss. The kernel adds a reaped child's
        totals into its parent's, so a process whose parent is also in the
        scope goes on being counted through that parent and must not be added
        to a retired total as well. Only the scope's roots -- those whose
        parent is outside it, and whose own counters have therefore absorbed
        everything beneath them by the time they exit -- are retired.
        """
        prior = self._prior_process_io()
        live_before = dict(prior.get('live') or {})
        retired = {name: int((prior.get('retired') or {}).get(name, 0))
                   for name in IO_COUNTERS}
        observed = int(prior.get('processes_observed') or 0)
        members = {int(pid) for pid in (prior.get('members') or [])}
        errors: list[str] = []
        if self.cgroup_path is None:
            errors.append('resource scope is not created')
            pids: list[int] = []
        else:
            pids = scope_pids(self.cgroup_path)
        members |= set(pids)
        live: dict[str, dict[str, Any]] = {}
        unreadable = 0
        for pid in pids:
            entry = read_process_io(pid)
            if entry is None:
                continue  # exited between the scan and the read
            identity, parent, counters = entry
            if identity not in live_before:
                observed += 1
            if counters is None:
                # Present, and this uid may not inspect it. Keep whatever it
                # was last seen using -- retiring it here would count it twice
                # the moment it becomes readable again.
                unreadable += 1
                previous = live_before.get(identity) or {}
                live[identity] = {'ppid': parent,
                                  **{name: int(previous.get(name, 0))
                                     for name in IO_COUNTERS}}
                continue
            live[identity] = {'ppid': parent, **counters}
        if unreadable:
            errors.append(f'{unreadable} live process(es) in the scope '
                          f'could not be inspected by this uid')
        for identity, counters in live_before.items():
            if identity in live:
                continue
            if int(counters.get('ppid', 0)) in members:
                continue  # its parent's own counters absorbed it on reap
            for name in IO_COUNTERS:
                retired[name] += int(counters.get(name, 0))
        record: dict[str, Any] = {'source': 'proc_io'}
        for name in IO_COUNTERS:
            record[name] = retired[name] + sum(
                int(counters.get(name, 0)) for counters in live.values())
        record['processes_observed'] = observed
        record['processes_live'] = len(live)
        record['processes_unreadable'] = unreadable
        record['retired'] = retired
        record['live'] = live
        # Bounded by the attempt's own process count, and the reason a departed
        # process can be told from a reaped one on the next sample.
        record['members'] = sorted(members)[-4096:]
        if errors:
            record['errors'] = errors[-8:]
        self._process_io = record
        return record

    def sample(self) -> dict[str, Any]:
        errors: list[str] = []
        try:
            if self.cgroup_path is None:
                raise OSError('resource scope is not created')
            direct = read_cgroup(self.cgroup_path)
            self._last = direct
        except (OSError, ValueError, KeyError) as exc:
            errors.append(str(exc))
            direct = self._last or dict(cpu_seconds=0.0, memory_current_bytes=0,
                                        memory_peak_bytes=0, oom_kill=0, oom_local=0)
        # Sampling I/O must not be able to cost the attempt its telemetry --
        # and, because the pool samples outside a guard of its own, must not be
        # able to cost the attempt its verdict either. Collecting a measurement
        # never decides whether the action passed, so every exception it can
        # raise, including a TypeError from a malformed prior record, is
        # recorded here rather than left to the worker loop.
        try:
            process_io = self.sample_process_io()
        except Exception as exc:
            process_io = {'source': 'proc_io',
                          'errors': [f'{type(exc).__name__}: {exc}']}
        record = {
            'action_key': self.action_key, 'nonce': self.nonce,
            'host': socket.gethostname(), 'scope_unit': self.unit,
            'sampled_unix': time.time(), 'wall_seconds': time.monotonic() - self.started,
            **direct, 'memory_max_bytes': self.memory_max_bytes,
            'gpu_memory_max_bytes': self.gpu_memory_max_bytes,
            'process_io': process_io,
            'complete': not errors, 'errors': errors,
        }
        if self.shape_key:
            record['shape_key'] = self.shape_key
        if self._reason:
            record['termination_reason'] = self._reason
        _atomic_json(self.telemetry_path, record)
        return record

    def terminate_owned(self, reason: str) -> dict:
        """Broker kills the exact attempt's whole cgroup and audits the reason."""
        self._reason = reason
        result = self._request('stop', reason=reason)
        _atomic_json(self.telemetry_path.with_suffix('.termination.json'),
                     {'scope_unit': self.unit, 'reason': reason, 'result': result})
        return result

    def release(self) -> dict:
        """Release only after the broker proves the aggregate job scope empty."""
        return self._request('release')
