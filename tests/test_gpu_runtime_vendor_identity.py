"""#1895: NVIDIA query failure is not NVIDIA vendor identity."""
import importlib.machinery
import importlib.util
import os
from pathlib import Path
import stat
from types import SimpleNamespace

import pytest
from prismabuild import gpu_capacity
from test_gpu_container_runtime import ERRORS, daemon_row, runtime_authority
from test_gpu_runtime_daemon_events import LaunchEventDaemon
from test_resource_broker import create, container_ticket

__all__ = ['runtime_authority']
_REAL_DRIVER_FAILURE = gpu_capacity.nvidia_driver_failure
NVIDIA_ERROR = ('OCI runtime create failed: nvidia-container-cli: '
                'initialization error: nvml error: driver not loaded')


def vendor_test_shim():
    path = Path(__file__).resolve().parents[1] / 'tools/fleet/docker'
    loader = importlib.machinery.SourceFileLoader('vendor_runtime_shim', str(path))
    shim = importlib.util.module_from_spec(importlib.util.spec_from_loader(loader.name, loader))
    loader.exec_module(shim)
    return shim


@pytest.mark.parametrize('snapshot,runtime_name,nvidia_host', [
    (False, 'amd', False),
    (False, 'runc', False),
    (True, 'amd', False),
    (True, 'amd', True),
    (True, 'runc', False),
    (True, 'runc', True),
    (False, 'runc', True),
])
def test_default_driver_needs_independent_vendor_identity_before_query_failure_can_fence(
        runtime_authority, tmp_path, monkeypatch, snapshot, runtime_name, nvidia_host):
    broker, authority, backend, runtime = runtime_authority
    _, record = create(authority)
    ticket = container_ticket(authority, record, monkeypatch)
    row = daemon_row(record)  # No actual daemon failure; only a fabricated caller hint.
    row['HostConfig'].update(Runtime=runtime_name, DeviceRequests=[{
        'Driver': '', 'Count': -1, 'DeviceIDs': None,
        'Capabilities': [['gpu']], 'Options': {}}])
    row['Config']['Labels']['prismabuild.shim'] = ticket
    daemon = LaunchEventDaemon(broker, backend, row, ticket,
                              removed=not snapshot, nvidia_injection=True)
    binary = tmp_path / 'trusted-nvidia-smi-fixture'
    binary.write_bytes(b'not executed')
    monkeypatch.setattr(gpu_capacity, '_root_owned_regular_fd',
                        lambda path: os.open(binary, os.O_RDONLY))
    monkeypatch.setattr(gpu_capacity, 'nvidia_driver_failure', _REAL_DRIVER_FAILURE)
    # This new seam models trusted device evidence, independently of the query.
    monkeypatch.setattr(gpu_capacity, 'nvidia_host_present', lambda: nvidia_host, raising=False)
    queries = []
    def driver_query(*args, **kwargs):
        queries.append(True)
        return SimpleNamespace(returncode=9, stdout='', stderr='NVIDIA driver is not loaded')
    monkeypatch.setattr(gpu_capacity.subprocess, 'run', driver_query)
    try:
        authority.launch_watches[ticket] = daemon.watch
        daemon.emit('create')
        assert daemon.inspected.wait(5)
        daemon.emit('destroy')
        def removed(cid):
            raise ValueError('No such container')
        backend.inspect_container = removed
        reply = authority.handle(os.getuid(), os.getpid(), {
            'op': 'container_outcome', 'scope_id': record['scope_id'],
            'ticket': ticket, 'container_id': row['Id'],
            'daemon_stderr': 'docker: Error response from daemon: ' + NVIDIA_ERROR})
        fault = nvidia_host and runtime_name == 'runc'
        assert runtime.state['latched'] is fault, reply
        assert reply['launch_outcome']['kind'] == ('host_fault' if fault else 'launch_error')
        assert queries == ([True] if fault else [])
        if snapshot:
            assert daemon.watch.row['HostConfig']['Runtime'] == runtime_name
    finally:
        daemon.close()


@pytest.mark.parametrize('runtime_name', ['amd', 'rocm'])
@pytest.mark.parametrize('driver', ['', 'nvidia', 'cdi'])
def test_other_vendor_runtime_excludes_nvidia_requests(monkeypatch, runtime_name, driver):
    monkeypatch.setattr(gpu_capacity, 'nvidia_host_present', lambda: True, raising=False)
    row = {'HostConfig': {'Runtime': runtime_name, 'DeviceRequests': [{
        'Driver': driver, 'DeviceIDs': ['nvidia.com/gpu=all'], 'Capabilities': [['gpu']]}]}}
    assert gpu_capacity.container_requests_nvidia(row) is False


@pytest.mark.parametrize('value,expected', [
    ('driver=cdi,device=nvidia.com/gpu=all', True),
    ('driver=cdi,"device=nvidia.com/gpu=0,nvidia.com/gpu=1"', True),
    ('driver=cdi,"device=amd.com/gpu=0,nvidia.com/gpu=1"', True),
    ('driver=cdi,device=amd.com/gpu=all', False),
])
def test_gpus_cdi_intent_preserves_device_ids_and_quoted_lists(value, expected):
    shim = vendor_test_shim()
    intent = {}
    shim._container_options(['--gpus', value, 'image'], runtime_intent=intent)
    assert shim._nvidia_injection_intent(intent) is expected


@pytest.mark.parametrize('value', [
    'driver=cdi,device=nvidia.com/gpu=all',
    'driver=cdi,"device=nvidia.com/gpu=0,nvidia.com/gpu=1"',
])
def test_cdi_gpus_autoremove_fences_a_verified_missing_mount(
        runtime_authority, monkeypatch, value):
    broker, authority, backend, runtime = runtime_authority
    _, record = create(authority)
    ticket = container_ticket(authority, record, monkeypatch)
    shim = vendor_test_shim()
    intent = {}
    shim._container_options(['--rm', '--gpus', value, 'image'], runtime_intent=intent)
    monkeypatch.setattr(gpu_capacity, 'nvidia_host_present', lambda: True, raising=False)
    row = daemon_row(record)
    row['Config']['Labels']['prismabuild.shim'] = ticket
    daemon = LaunchEventDaemon(broker, backend, row, ticket, removed=True,
                              nvidia_injection=shim._nvidia_injection_intent(intent))
    try:
        authority.launch_watches[ticket] = daemon.watch
        daemon.emit('create')
        assert daemon.inspected.wait(5)
        daemon.emit('destroy')
        reply = authority.handle(os.getuid(), os.getpid(), {
            'op': 'container_outcome', 'scope_id': record['scope_id'],
            'ticket': ticket, 'container_id': row['Id'],
            'daemon_stderr': 'docker: Error response from daemon: ' + ERRORS[0]['daemon_error']})
        assert runtime.state['latched'], reply
        assert reply['launch_outcome']['subtype'] == 'cdi_mount_missing'
    finally:
        daemon.close()


@pytest.mark.parametrize('node_type,owner,device,expected', [
    (stat.S_IFCHR, 0, (195, 255), True),
    (stat.S_IFCHR, 1000, (195, 255), False),
    (stat.S_IFREG, 0, (195, 255), False),
    (stat.S_IFLNK, 0, (195, 255), False),
    (stat.S_IFCHR, 0, (1, 3), False),
    (stat.S_IFCHR, 0, (195, 0), False),
])
def test_host_identity_checks_the_trusted_control_device_without_opening_it(
        tmp_path, monkeypatch, node_type, owner, device, expected):
    directory = tmp_path / 'dev'
    directory.mkdir()
    path = directory / 'nvidiactl'
    path.write_bytes(b'fixture, never opened as a device')
    monkeypatch.setattr(gpu_capacity, 'NVIDIA_CONTROL_DEVICE', path)
    original_fstat = os.fstat
    def root_directory_metadata(fd):
        info = original_fstat(fd)
        values = list(info)
        values[4] = 0
        values[0] &= ~0o022
        return os.stat_result(values)
    monkeypatch.setattr(os, 'fstat', root_directory_metadata)
    original_stat = os.stat
    observed = []
    def device_metadata(name, *args, **kwargs):
        if name != path.name or 'dir_fd' not in kwargs:
            return original_stat(name, *args, **kwargs)
        assert kwargs['follow_symlinks'] is False
        parent = kwargs['dir_fd']
        assert Path(os.readlink(f'/proc/self/fd/{parent}')) == directory
        observed.append(parent)
        return SimpleNamespace(st_mode=node_type | 0o666, st_uid=owner,
                               st_rdev=os.makedev(*device))
    monkeypatch.setattr(os, 'stat', device_metadata)
    def forbidden_query():
        raise AssertionError('driver query cannot prove host identity')
    monkeypatch.setattr(gpu_capacity, 'nvidia_driver_failure', forbidden_query)
    assert gpu_capacity.nvidia_host_present() is expected
    assert len(observed) == 1
    with pytest.raises(OSError):
        original_fstat(observed[0])


@pytest.mark.parametrize('failure', ['missing', 'unreadable', 'unsafe_parent'])
def test_unavailable_or_untrusted_control_device_cannot_prove_vendor(tmp_path, monkeypatch, failure):
    monkeypatch.setattr(gpu_capacity, 'NVIDIA_CONTROL_DEVICE', tmp_path / 'nvidiactl')
    def unavailable_directory(path):
        if failure == 'unsafe_parent':
            raise ValueError('parent writable outside root')
        raise FileNotFoundError() if failure == 'missing' else PermissionError()
    monkeypatch.setattr(gpu_capacity, '_root_owned_directory_fd', unavailable_directory)
    assert gpu_capacity.nvidia_host_present() is False


def test_unverified_default_gpu_intent_stays_a_candidate(monkeypatch):
    row = {'HostConfig': {'Runtime': 'runc', 'DeviceRequests': [
        {'Driver': '', 'Capabilities': [['gpu']]}]}}
    monkeypatch.setattr(gpu_capacity, 'nvidia_host_present', lambda: False)
    assert gpu_capacity.container_nvidia_intent(row) == 'candidate'
    assert gpu_capacity.container_requests_nvidia(row) is False
    # The shim cannot establish host identity from an action's device namespace.
    assert vendor_test_shim()._nvidia_injection_intent({'gpus': ['all']}) is True
