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
        raise ResourceUnavailable('installed broker needs durable creation recovery support')
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
    return {
        'cpu_seconds': int(cpu['usage_usec']) / 1_000_000,
        'memory_current_bytes': int((path / 'memory.current').read_text()),
        'memory_peak_bytes': int((path / 'memory.peak').read_text()),
        'oom_kill': int(events.get('oom_kill', 0)),
    }


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
                 shape_key: str | None = None, socket_path: Path = BROKER_SOCKET):
        if not re.fullmatch('[a-f0-9]{64}', action_key) or not re.fullmatch('[a-f0-9]{32}', nonce):
            raise ValueError('resource scope needs an action key and 32-hex attempt nonce')
        if isinstance(memory_max_bytes, bool) or not isinstance(memory_max_bytes, int) or memory_max_bytes <= 0:
            raise ValueError('resource scope memory limit must be a positive integer')
        self.action_key = action_key
        self.nonce = nonce
        self.memory_max_bytes = memory_max_bytes
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

    def _request(self, op: str, **extra) -> dict:
        request = {'op': op, 'action_key': self.action_key, 'nonce': self.nonce, **extra}
        if self.token is not None:
            request['token'] = self.token
        return broker_request(request, socket_path=self.socket_path)

    def create(self) -> dict:
        if self.token is not None:
            raise RuntimeError('resource scope is already created')
        response = self._request('create', memory_max_bytes=self.memory_max_bytes, recovery_protocol=1)
        self._adopt_created_scope(response)
        return self.control_record()

    def recover_create(self) -> bool:
        """Recover authority or fence a missing attempt against delayed creation."""
        response = self._request('recover_create', memory_max_bytes=self.memory_max_bytes)
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
        self.unit, self.token, self.cgroup_path = unit, token, path

    def control_record(self) -> dict:
        """Lease metadata for exact-attempt recovery; retain with the reservation."""
        return {'action_key': self.action_key, 'nonce': self.nonce,
                'scope_id': self.unit, 'cgroup_path': str(self.cgroup_path),
                'token': self.token, 'socket_path': str(self.socket_path),
                'memory_max_bytes': self.memory_max_bytes}

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
                                        memory_peak_bytes=0, oom_kill=0)
        record = {
            'action_key': self.action_key, 'nonce': self.nonce,
            'host': socket.gethostname(), 'scope_unit': self.unit,
            'sampled_unix': time.time(), 'wall_seconds': time.monotonic() - self.started,
            **direct, 'memory_max_bytes': self.memory_max_bytes,
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
