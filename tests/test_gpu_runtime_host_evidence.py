"""Reviewer reproductions. No real Docker daemon, CDI or broker is changed."""
import os
from types import SimpleNamespace

import pytest
from prismabuild import gpu_capacity

from prismabuild import pool
from test_gpu_container_runtime import ERRORS, MISSING, daemon_row, launch_row, runtime_authority
from test_gpu_runtime_daemon_events import LaunchEventDaemon
from test_resource_broker import create, container_ticket

__all__ = ['runtime_authority']
_REAL_DRIVER_FAILURE = gpu_capacity.nvidia_driver_failure
NVIDIA_ERROR = ('OCI runtime create failed: nvidia-container-cli: '
                'initialization error: nvml error: driver not loaded')


def test_disabled_cleanup_preserves_main_removal_arguments(monkeypatch):
    calls = []
    def removed(argv, **kwargs):
        calls.append(argv)
        return SimpleNamespace(returncode=0, stdout='d' * 64 + '\n', stderr='')
    monkeypatch.setattr(pool.subprocess, 'run', removed)
    pool._docker_remove_containers(['d' * 64])
    assert calls == [[pool.DOCKER, 'rm', '-f', 'd' * 64]], calls


def test_invalid_entrypoint_text_must_not_be_a_host_fault(runtime_authority, monkeypatch):
    _, authority, backend, runtime = runtime_authority
    _, record = create(authority)
    ticket = container_ticket(authority, record, monkeypatch)
    error = ('failed to create task for container: failed to create shim task: '
             'OCI runtime create failed: runc create failed: unable to start container process: '
             'exec: "nvidia-container-cli: initialization error: nvml error: driver not loaded": '
             'executable file not found in $PATH: unknown')
    row = daemon_row(record, error)
    row['Config']['Labels']['prismabuild.shim'] = ticket
    backend.inspect_container = lambda cid: row
    response = authority.handle(os.getuid(), os.getpid(), {
        'op': 'container_outcome', 'scope_id': record['scope_id'], 'ticket': ticket,
        'container_id': row['Id']})
    assert not runtime.state['latched'], response


def test_create_remove_plus_caller_supplied_error_cannot_fence(runtime_authority, monkeypatch):
    broker, authority, backend, runtime = runtime_authority
    _, record = create(authority)
    ticket = container_ticket(authority, record, monkeypatch)
    row = daemon_row(record)  # No daemon error and no payload start.
    row['Config']['Labels']['prismabuild.shim'] = ticket
    daemon = LaunchEventDaemon(broker, backend, row, ticket, removed=True)
    try:
        authority.launch_watches[ticket] = daemon.watch
        daemon.emit('create')
        assert daemon.inspected.wait(5)
        daemon.emit('destroy')
        response = authority.handle(os.getuid(), os.getpid(), {
            'op': 'container_outcome', 'scope_id': record['scope_id'], 'ticket': ticket,
            'container_id': row['Id'],
            'daemon_stderr': 'docker: Error response from daemon: OCI runtime create failed: '
                             'nvidia-container-cli: initialization error: nvml error: driver not loaded'})
        assert not runtime.state['latched'], response
    finally:
        daemon.close()


def test_disabled_shim_stop_preserves_main_removal_arguments(monkeypatch):
    import importlib.machinery
    import importlib.util
    from pathlib import Path
    from prismabuild import gpu_capacity
    path = Path(__file__).resolve().parents[1] / 'tools/fleet/docker'
    loader = importlib.machinery.SourceFileLoader('cleanup_runtime_shim', str(path))
    shim = importlib.util.module_from_spec(importlib.util.spec_from_loader(loader.name, loader))
    loader.exec_module(shim)
    monkeypatch.setattr(gpu_capacity, 'runtime_fence_enabled', lambda: False)
    calls = []
    stop = shim._Stop('/usr/bin/docker', [], {}, 1, 1)
    monkeypatch.setattr(stop, 'call', lambda args: (calls.append(args) or (0, '', '')))
    assert stop.remove('d' * 64)
    assert calls == [['rm', '-f', 'd' * 64]]


def test_legacy_capacity_error_keeps_main_status_output():
    from test_pbstatus import pbstatus
    node = {'node': 'fixture', 'state': 'live'}
    expected = pbstatus.pool_node_lines([node])
    node['observed_detail'] = {'gpu_capacity_error': 'legacy telemetry unavailable'}
    assert pbstatus.pool_node_lines([node]) == expected


def test_nvidia_diagnostic_requires_independent_host_failure(runtime_authority, monkeypatch):
    from prismabuild import gpu_capacity
    _, authority, backend, runtime = runtime_authority
    _, record = create(authority)
    ticket = container_ticket(authority, record, monkeypatch)
    row = daemon_row(record, 'OCI runtime create failed: nvidia-container-cli: '
                     'initialization error: nvml error: driver not loaded')
    row['Config']['Labels']['prismabuild.shim'] = ticket
    backend.inspect_container = lambda cid: row
    checks = []
    monkeypatch.setattr(gpu_capacity, 'nvidia_driver_failure', lambda: checks.append('queried'))
    reply = authority.handle(os.getuid(), os.getpid(), {'op': 'container_outcome',
        'scope_id': record['scope_id'], 'ticket': ticket, 'container_id': row['Id']})
    assert checks == ['queried']
    assert reply['launch_outcome']['kind'] == 'launch_error'
    assert not runtime.state['latched']


def test_removed_nvidia_launch_with_fabricated_error_and_healthy_host_cannot_fence(runtime_authority, monkeypatch):
    from prismabuild import gpu_capacity
    broker, authority, backend, runtime = runtime_authority
    _, record = create(authority)
    ticket = container_ticket(authority, record, monkeypatch)
    row = daemon_row(record)
    row['Config']['Labels']['prismabuild.shim'] = ticket
    daemon = LaunchEventDaemon(broker, backend, row, ticket, removed=True, nvidia_injection=True)
    checks = []
    monkeypatch.setattr(gpu_capacity, 'nvidia_driver_failure', lambda: checks.append('queried'))
    try:
        authority.launch_watches[ticket] = daemon.watch
        daemon.emit('create')
        daemon.emit('destroy')
        reply = authority.handle(os.getuid(), os.getpid(), {'op': 'container_outcome',
            'scope_id': record['scope_id'], 'ticket': ticket, 'container_id': row['Id'],
            'daemon_stderr': 'docker: Error response from daemon: OCI runtime create failed: '
                             'nvidia-container-cli: initialization error: nvml error: driver not loaded'})
        assert checks == ['queried']
        assert reply['launch_outcome']['kind'] == 'launch_error'
        assert not runtime.state['latched']
    finally:
        daemon.close()


@pytest.mark.parametrize('quote', ['"', "'", '`'])
@pytest.mark.parametrize('field', ['exec', 'args', 'labels', 'env'])
def test_quoted_payload_fields_cannot_select_a_host_check(monkeypatch, quote, field):
    def forbidden():
        raise AssertionError('payload text selected a driver check')
    monkeypatch.setattr(gpu_capacity, 'nvidia_driver_failure', forbidden)
    error = ('OCI runtime create failed: ' + field + ': ' + quote +
             'nvidia-container-cli: initialization error: nvml error: driver not loaded' + quote)
    assert gpu_capacity.container_launch_outcome(launch_row(error), cdi_sources=set()).kind == 'launch_error'


@pytest.mark.parametrize('injected', [False, True])
def test_independent_driver_failure_requires_nvidia_injection(monkeypatch, injected):
    checks = []
    def failed_query():
        checks.append(True)
        return {'kind': 'nvidia_driver_query_failed', 'returncode': 9}
    monkeypatch.setattr(gpu_capacity, 'nvidia_driver_failure', failed_query)
    row = launch_row('OCI runtime create failed: nvidia-container-cli: '
                     'initialization error: nvml error: driver not loaded')
    if not injected:
        row['HostConfig'] = {}
    outcome = gpu_capacity.container_launch_outcome(row, cdi_sources=set())
    assert outcome.kind == ('host_fault' if injected else 'launch_error')
    assert checks == ([True] if injected else [])
    if injected:
        assert outcome.host_check['returncode'] == 9


@pytest.mark.parametrize('result', ['success', 'failed', 'missing', 'timeout', 'interrupted', 'untrusted'])
def test_driver_verification_uses_its_own_bounded_query(tmp_path, monkeypatch, result):
    import subprocess
    binary = tmp_path / 'nvidia-smi'
    binary.write_bytes(b'fixture')
    monkeypatch.setattr(gpu_capacity, '_root_owned_regular_fd', lambda path: os.open(binary, os.O_RDONLY))
    if result == 'untrusted':
        def untrusted_binary(path):
            raise ValueError('untrusted binary')
        monkeypatch.setattr(gpu_capacity, '_root_owned_regular_fd', untrusted_binary)
    calls = []
    def host_query(argv, **kwargs):
        calls.append((argv, kwargs))
        if result == 'missing':
            raise FileNotFoundError()
        if result == 'timeout':
            raise subprocess.TimeoutExpired(argv, 2)
        return SimpleNamespace(returncode=9 if result == 'failed' else -9 if result == 'interrupted' else 0)
    monkeypatch.setattr(gpu_capacity.subprocess, 'run', host_query)
    check = gpu_capacity.nvidia_driver_failure()
    assert (check is not None) is (result in {'success', 'failed', 'interrupted'})
    if check is not None:
        assert (check['kind'] == 'nvidia_driver_query_failed') is (result == 'failed')
    if result == 'untrusted':
        assert not calls
    else:
        assert calls[0][0] == [gpu_capacity.NVIDIA_SMI, '--query-gpu=uuid', '--format=csv,noheader']
        assert calls[0][1]['timeout'] == 2


def test_cdi_lookup_permission_errors_do_not_prove_a_missing_source(monkeypatch):
    original = os.stat
    def denied(path, *args, **kwargs):
        if str(path) == MISSING:
            raise PermissionError('unreadable source')
        return original(path, *args, **kwargs)
    monkeypatch.setattr(gpu_capacity.os, 'stat', denied)
    assert gpu_capacity.container_launch_outcome(launch_row(ERRORS[0]['daemon_error']),
                                                cdi_sources={MISSING}).kind == 'launch_error'


@pytest.mark.parametrize('untrusted', ['owner', 'writable', 'symlink'])
def test_untrusted_cdi_specs_cannot_supply_host_fault_evidence(
        runtime_authority, monkeypatch, untrusted):
    from pathlib import Path
    _, authority, backend, runtime = runtime_authority
    spec = runtime.cdi_dir / 'nvidia.yaml'
    if untrusted == 'symlink':
        target = spec.with_suffix('.target')
        spec.rename(target)
        spec.symlink_to(target)
    else:
        original = os.fstat
        def bad_spec_metadata(fd):
            info = original(fd)
            if Path(os.readlink(f'/proc/self/fd/{fd}')) == spec:
                fields = list(info)
                if untrusted == 'owner': fields[4] = 1000
                else: fields[0] |= 0o020
                return os.stat_result(fields)
            return info
        monkeypatch.setattr(os, 'fstat', bad_spec_metadata)
    _, record = create(authority)
    ticket = container_ticket(authority, record, monkeypatch)
    row = daemon_row(record, ERRORS[0]['daemon_error'])
    row['Config']['Labels']['prismabuild.shim'] = ticket
    backend.inspect_container = lambda cid: row
    reply = authority.handle(os.getuid(), os.getpid(), {'op': 'container_outcome',
        'scope_id': record['scope_id'], 'ticket': ticket, 'container_id': row['Id']})
    assert reply['launch_outcome']['kind'] == 'launch_error'
    assert not runtime.state['latched']


@pytest.mark.parametrize('removed', [False, True], ids=['retained', 'autoremove'])
@pytest.mark.parametrize('status', [0, 2, 3, 4, 6, 8, 9, 10, 12, 13, 14, 15, 17, 255, -9])
def test_only_documented_driver_faults_fence_and_all_query_statuses_are_retained(
        runtime_authority, tmp_path, monkeypatch, removed, status):
    broker, authority, backend, runtime = runtime_authority
    _, record = create(authority)
    ticket = container_ticket(authority, record, monkeypatch)
    row = daemon_row(record, '' if removed else NVIDIA_ERROR)
    row['Config']['Labels']['prismabuild.shim'] = ticket
    binary = tmp_path / 'trusted-nvidia-smi'
    binary.write_bytes(b'not executed')
    monkeypatch.setattr(gpu_capacity, '_root_owned_regular_fd',
                        lambda path: os.open(binary, os.O_RDONLY))
    monkeypatch.setattr(gpu_capacity, 'nvidia_driver_failure', _REAL_DRIVER_FAILURE)
    monkeypatch.setattr(gpu_capacity.subprocess, 'run', lambda *a, **kw: SimpleNamespace(
        returncode=status, stdout='', stderr='independent query diagnostic'))
    daemon = None
    try:
        if removed:
            daemon = LaunchEventDaemon(broker, backend, row, ticket,
                                      removed=True, nvidia_injection=True)
            authority.launch_watches[ticket] = daemon.watch
            daemon.emit('create')
            assert daemon.inspected.wait(5)
            daemon.emit('destroy')
        else:
            backend.inspect_container = lambda cid: row
        reply = authority.handle(os.getuid(), os.getpid(), {
            'op': 'container_outcome', 'scope_id': record['scope_id'],
            'ticket': ticket, 'container_id': row['Id'],
            'daemon_stderr': 'docker: Error response from daemon: ' + NVIDIA_ERROR})
        fault = status in {8, 9, 10, 12, 14, 15}
        assert runtime.state['latched'] is fault, reply
        outcome = reply['launch_outcome']
        assert outcome['kind'] == ('host_fault' if fault else 'launch_error')
        assert outcome['host_check']['returncode'] == status
        if fault:
            assert runtime.state['evidence']['host_check']['returncode'] == status
    finally:
        if daemon is not None:
            daemon.close()


@pytest.mark.parametrize('driver', ['', 'nvidia', 'other'])
@pytest.mark.parametrize('status', [0, 4, 9])
@pytest.mark.parametrize('removed', [False, True], ids=['retained', 'autoremove'])
def test_gpus_device_requests_require_independent_nvidia_fault_evidence(
        runtime_authority, monkeypatch, driver, status, removed):
    broker, authority, backend, runtime = runtime_authority
    _, record = create(authority)
    ticket = container_ticket(authority, record, monkeypatch)
    checks = []
    def query():
        checks.append(True)
        return {'kind': 'nvidia_driver_query_failed' if status == 9
                else 'nvidia_driver_query_unavailable', 'returncode': status}
    monkeypatch.setattr(gpu_capacity, 'nvidia_driver_failure', query)
    row = daemon_row(record, NVIDIA_ERROR)
    row['HostConfig'].update(Runtime='runc', DeviceRequests=[{
        'Driver': driver, 'Count': -1, 'DeviceIDs': None,
        'Capabilities': [['gpu']], 'Options': {}}])
    row['Config']['Labels']['prismabuild.shim'] = ticket
    daemon = None
    try:
        if removed:
            daemon = LaunchEventDaemon(broker, backend, row, ticket,
                                      removed=True, nvidia_injection=driver in {'', 'nvidia'})
            authority.launch_watches[ticket] = daemon.watch
            daemon.emit('create')
            assert daemon.inspected.wait(5)
            daemon.emit('destroy')
        else:
            backend.inspect_container = lambda cid: row
        reply = authority.handle(os.getuid(), os.getpid(), {
            'op': 'container_outcome', 'scope_id': record['scope_id'],
            'ticket': ticket, 'container_id': row['Id'],
            'daemon_stderr': 'docker: Error response from daemon: ' + NVIDIA_ERROR})
        fault = driver in {'', 'nvidia'} and status == 9
        assert runtime.state['latched'] is fault, reply
        assert checks == ([True] if driver in {'', 'nvidia'} else [])
    finally:
        if daemon is not None:
            daemon.close()
