"""#1895: daemon launch faults fence the host, never payload stderr."""
import json
from pathlib import Path

import pytest
from prismabuild import gpu_capacity

ERRORS = json.loads((Path(__file__).parent / 'fixtures/gpu_container_runtime_1895.json').read_text())
MISSING = '/usr/lib/aarch64-linux-gnu/libnvidia-egl-wayland.so.1.1.21'


def launch_row(error, *, started=False):
    return {'HostConfig': {'DeviceRequests': [{'Driver': 'cdi', 'DeviceIDs': ['nvidia.com/gpu=all']}]},
            'State': {'Error': error, 'StartedAt': '2026-10-21T12:00:00Z' if started else '0001-01-01T00:00:00Z',
                      'Running': False, 'ExitCode': 127 if error else 0}}


@pytest.mark.parametrize('evidence', ERRORS, ids=lambda value: value['action_key'][:12])
def test_recorded_missing_cdi_source_is_a_never_started_host_fault(evidence):
    outcome = gpu_capacity.container_launch_outcome(launch_row(evidence['daemon_error']),
                                                   cdi_sources={MISSING}, source_exists=lambda _: False)
    assert outcome.kind == 'host_fault'
    assert outcome.subtype == 'cdi_mount_missing'
    assert outcome.payload_started is False
    assert outcome.daemon_error == evidence['daemon_error']


@pytest.mark.parametrize('error', [
    'OCI runtime create failed: nvidia-container-cli: initialization error: nvml error: driver not loaded: unknown',
    'OCI runtime create failed: nvidia-container-runtime: initialization error: failed to initialize NVML',
    'OCI runtime create failed: nvidia-container-cli: device error: GPU-123: unknown device',
])
def test_nvidia_runtime_initialization_is_a_host_fault(error):
    outcome = gpu_capacity.container_launch_outcome(launch_row(error), cdi_sources=set())
    assert outcome.kind == 'host_fault'
    assert outcome.subtype == 'nvidia_initialization_failed'


@pytest.mark.parametrize('change,expected', [
    ({'started': True}, 'payload_exit'),
    ({'sources': set()}, 'launch_error'),
    ({'exists': True}, 'launch_error'),
    ({'error': 'OCI runtime create failed: permission denied'}, 'launch_error'),
    ({'error': 'nvidia-container-cli: initialization error: no driver'}, 'launch_error'),
    ({'error': 'OCI runtime create failed: executable file not found'}, 'launch_error'),
    ({'error': ''}, 'unknown'),
])
def test_payload_and_unproven_launch_errors_do_not_fence(change, expected):
    row = launch_row(change.get('error', ERRORS[0]['daemon_error']), started=change.get('started', False))
    outcome = gpu_capacity.container_launch_outcome(row, cdi_sources=change.get('sources', {MISSING}),
                                                   source_exists=lambda _: change.get('exists', False))
    assert outcome.kind == expected
    assert outcome.subtype is None


def test_cdi_spec_reader_accepts_generated_yaml_and_json_without_rewriting(tmp_path):
    yaml = tmp_path / 'nvidia.yaml'
    yaml.write_text(f'kind: nvidia.com/gpu\ncontainerEdits:\n  mounts:\n    - hostPath: {MISSING}\n')
    (tmp_path / 'other.json').write_text(json.dumps({'devices': [{'containerEdits': {
        'mounts': [{'hostPath': '/some/device/library'}]}}]}))
    before = yaml.read_bytes()
    assert gpu_capacity.cdi_mount_sources(tmp_path) == {MISSING, '/some/device/library'}
    assert yaml.read_bytes() == before


@pytest.fixture
def runtime_authority(tmp_path, monkeypatch):
    import os
    from functools import partial
    from test_resource_broker import module, Backend
    broker = module()
    backend = Backend()
    authority = broker.Authority(tmp_path / 'run/jobs', os.getuid(), backend, max_memory_bytes=1024**3)
    cdi = tmp_path / 'cdi'
    cdi.mkdir()
    (cdi / 'nvidia.yaml').write_text(f'containerEdits:\n  mounts:\n    - hostPath: {MISSING}\n')
    runtime = broker.GPUContainerRuntime(gpu_capacity, tmp_path / 'durable/gpu.json',
                                         tmp_path / 'run/gpu.json', authority.lock, cdi_dir=cdi)
    authority.runtime = runtime
    monkeypatch.setattr(gpu_capacity, 'container_launch_outcome',
                        partial(gpu_capacity.container_launch_outcome, source_exists=lambda _: False))
    yield broker, authority, backend, runtime
    runtime.close()


def daemon_row(record, error='', *, started=False, probe=None):
    row = launch_row(error, started=started)
    row.update(Id='d'*64, Config={'Labels': {'prismabuild.scope': record['scope_id']},
                                 'Image': gpu_capacity.RUNTIME_PROBE_IMAGE,
                                 'Entrypoint': ['/bin/true'], 'Cmd': None},
               HostConfig={'CgroupParent': record['scope_id'], 'DeviceRequests': [
                   {'Driver': 'cdi', 'DeviceIDs': ['nvidia.com/gpu=all']}]})
    if probe:
        row['Config']['Labels'][gpu_capacity.RUNTIME_PROBE_LABEL] = probe
    return row


def test_broker_latches_durable_evidence_from_exact_daemon_container(runtime_authority, monkeypatch):
    import os
    from test_resource_broker import create, container_ticket, auth
    broker, authority, backend, runtime = runtime_authority
    request, record = create(authority)
    ticket = container_ticket(authority, record, monkeypatch)
    row = daemon_row(record, ERRORS[0]['daemon_error'])
    backend.inspect_container = lambda cid: row
    reply = authority.handle(os.getuid(), os.getpid(), {'op': 'container_outcome',
        'scope_id': record['scope_id'], 'ticket': ticket, 'container_id': row['Id'], 'remove': False})
    assert reply['launch_outcome']['kind'] == 'host_fault'
    for path in (runtime.durable, runtime.mirror):
        state = json.loads(path.read_text())
        assert state['latched'] is True
        assert state['subtype'] == 'cdi_mount_missing'
        assert state['evidence']['action_key'] == record['action_key']
        assert state['evidence']['attempt'] == record['nonce']
        assert state['evidence']['boot_id'] == runtime.boot_id
        assert state['evidence']['unix_time'] > 0
        assert state['evidence']['daemon_error'] == ERRORS[0]['daemon_error']
    assert authority.handle(os.getuid(), os.getpid(), auth(request, record, 'status'))['host_fault']['subtype'] == 'cdi_mount_missing'
    restored = broker.GPUContainerRuntime(gpu_capacity, runtime.durable, runtime.mirror, authority.lock,
                                          cdi_dir=runtime.cdi_dir)
    try:
        assert restored.state['latched'] is True
    finally:
        restored.close()


@pytest.mark.parametrize('alter', ['id', 'scope', 'parent', 'ticket'])
def test_broker_rejects_unbound_daemon_state(runtime_authority, monkeypatch, alter):
    import os
    from test_resource_broker import create, container_ticket
    _, authority, backend, runtime = runtime_authority
    _, record = create(authority)
    ticket = container_ticket(authority, record, monkeypatch)
    row = daemon_row(record, ERRORS[0]['daemon_error'])
    if alter == 'id': row['Id'] = 'e'*64
    if alter == 'scope': row['Config']['Labels']['prismabuild.scope'] = 'other'
    if alter == 'parent': row['HostConfig']['CgroupParent'] = 'other'
    if alter == 'ticket': ticket = 'f'*64
    backend.inspect_container = lambda cid: row
    with pytest.raises(PermissionError):
        authority.handle(os.getuid(), os.getpid(), {'op': 'container_outcome',
            'scope_id': record['scope_id'], 'ticket': ticket, 'container_id': 'd'*64, 'remove': False})
    assert runtime.state['latched'] is False


def test_event_probe_failure_stays_latched_and_only_current_success_clears(runtime_authority):
    _, _, _, runtime = runtime_authority
    record = {'action_key': 'a'*64, 'nonce': 'b'*32, 'scope_id': 'scope'}
    fault = gpu_capacity.container_launch_outcome(launch_row(ERRORS[0]['daemon_error']), cdi_sources={MISSING})
    runtime.latch(fault, record)
    first = runtime.event('cdi_changed')
    runtime.observe(daemon_row(record, ERRORS[0]['daemon_error'], probe=first), record)
    assert runtime.state['latched'] is True
    assert 'probe_token' not in runtime.state
    assert runtime.state['last_probe']['reason'] == 'readiness_launch_failed'
    second = runtime.event('cdi_changed')
    current = runtime.event('watch_overflow')
    runtime.observe(daemon_row(record, started=True, probe=second), record)
    assert runtime.state['latched'] is True
    runtime.observe(daemon_row(record, started=True, probe=current), record)
    assert runtime.state['latched'] is False
    assert runtime.state['cleared']['event']['reason'] == 'watch_overflow'
    assert runtime.state['evidence']['action_key'] == record['action_key']
    assert second != current


@pytest.mark.parametrize('change', ['image', 'entrypoint', 'cmd', 'gpu', 'exit', 'running'])
def test_probe_success_must_be_the_actual_pinned_cdi_launch(runtime_authority, change):
    _, _, _, runtime = runtime_authority
    record = {'action_key': 'a'*64, 'nonce': 'b'*32, 'scope_id': 'scope'}
    token = runtime.event('watch_restart')
    row = daemon_row(record, started=True, probe=token)
    if change == 'image': row['Config']['Image'] = 'mutable:latest'
    if change == 'entrypoint': row['Config']['Entrypoint'] = ['/bin/sh']
    if change == 'cmd': row['Config']['Cmd'] = ['-c', 'exit 0']
    if change == 'gpu': row['HostConfig']['DeviceRequests'] = []
    if change == 'exit': row['State']['ExitCode'] = 1
    if change == 'running': row['State']['Running'] = True
    runtime.observe(row, record)
    assert runtime.state['latched'] is True
    assert runtime.state['last_probe']['reason'] == 'readiness_launch_failed'


def test_watch_restart_revalidates_once_then_waits_for_event(runtime_authority):
    import os
    _, _, _, runtime = runtime_authority
    calls = []
    def submit(token):
        calls.append(token)
        os.write(runtime.stop_write, b'x')
    runtime._watch(submit)
    assert len(calls) == 1
    assert runtime.state['probe_event']['reason'] == 'watch_restart'


def test_local_inotify_survives_atomic_nvidia_yaml_replacement(tmp_path):
    import os
    directory = tmp_path / 'cdi'
    directory.mkdir()
    target = directory / 'nvidia.yaml'
    target.write_text('old')
    watch = gpu_capacity.CDIChangeWatch(directory)
    stop_read, stop_write = os.pipe()
    try:
        temporary = directory / 'nvidia.tmp'
        temporary.write_text('new')
        temporary.replace(target)
        assert watch.next_change(stop_read) == 'cdi_changed'
        temporary.write_text('next')
        temporary.replace(target)
        assert watch.next_change(stop_read) == 'cdi_changed'
        assert target.read_text() == 'next'
    finally:
        watch.close()
        os.close(stop_read)
        os.close(stop_write)


def test_inotify_overflow_revalidates_as_unknown(tmp_path, monkeypatch):
    import os
    import select
    import struct
    directory = tmp_path / 'cdi'
    directory.mkdir()
    watch = gpu_capacity.CDIChangeWatch(directory)
    try:
        with monkeypatch.context() as patch:
            patch.setattr(select, 'select', lambda *args: ([watch.fd], [], []))
            patch.setattr(os, 'read', lambda *args: struct.pack('iIII', -1, 0x4000, 0, 0))
            assert watch.next_change(-99) == 'watch_overflow'
    finally:
        watch.close()


def test_missing_bind_mount_without_nvidia_injection_is_not_a_host_fault():
    row = launch_row(ERRORS[0]['daemon_error'])
    row['HostConfig'] = {}
    assert gpu_capacity.container_launch_outcome(row, cdi_sources={MISSING},
        source_exists=lambda _: False).kind == 'launch_error'


def test_probe_submission_declares_normal_resources_and_drops_root(runtime_authority, monkeypatch):
    import os
    from types import SimpleNamespace
    broker, _, _, runtime = runtime_authority
    token = runtime.event('cdi_changed')
    calls = []
    def submit(argv, **kwargs):
        calls.append((argv, kwargs))
        return SimpleNamespace(returncode=0, stdout='queued fixture', stderr='')
    monkeypatch.setattr(broker.subprocess, 'run', submit)
    runtime.submit_probe(os.getuid(), token)
    argv, kwargs = calls[0]
    index = argv.index('/mnt/shared/prismabuild-fleet/repo/tools/pbrun.py')
    command = argv[index:]
    assert command[command.index('--cpus') + 1] == '1'
    assert command[command.index('--demand') + 1] == 'mem_gb=1'
    assert command[command.index('--gpu-memory-gb') + 1] == '1'
    assert command[command.index('--timeout-s') + 1] == '30'
    assert '--here' in command and '--gpu' in command and '--exclusive' in command
    assert command[command.index('--container-image') + 1] == gpu_capacity.RUNTIME_PROBE_IMAGE
    assert command[command.index('--') + 1:] == gpu_capacity.runtime_probe_command()
    assert kwargs['user'] == os.getuid()
    assert kwargs['extra_groups'] == []
    assert runtime.state['probe_submission']['returncode'] == 0


def test_event_consumer_coalesces_superseded_pending_probes(runtime_authority):
    _, _, _, runtime = runtime_authority
    old = runtime.event('cdi_changed')
    current = runtime.event('cdi_changed')
    runtime.pending.put(old)
    runtime.pending.put(current)
    runtime.pending.put(None)
    submitted = []
    runtime._submit_events(submitted.append)
    assert submitted == [current]


def test_inotify_recovers_when_cdi_directory_is_recreated(tmp_path):
    import os
    directory = tmp_path / 'cdi'
    directory.mkdir()
    watch = gpu_capacity.CDIChangeWatch(directory)
    stop_read, stop_write = os.pipe()
    try:
        directory.rmdir()
        directory.mkdir()
        assert watch.next_change(stop_read) in {'cdi_changed', 'watch_replaced'}
        (directory / 'nvidia.yaml').write_text('new')
        assert watch.next_change(stop_read) == 'cdi_changed'
    finally:
        watch.close()
        os.close(stop_read)
        os.close(stop_write)


def test_typed_outcome_separates_payload_exit_from_a_running_payload():
    row = launch_row(ERRORS[0]['daemon_error'], started=True)
    outcome = gpu_capacity.container_launch_outcome(row, cdi_sources=set())
    assert outcome.kind == 'payload_exit' and outcome.payload_returncode == 127
    row['State']['Running'] = True
    outcome = gpu_capacity.container_launch_outcome(row, cdi_sources=set())
    assert outcome.kind == 'payload_started' and outcome.payload_returncode is None


@pytest.mark.parametrize('running,owner_matches', [(False, True), (True, True), (True, False)])
def test_deferred_auto_removal_waits_for_exit_and_rechecks_scope(runtime_authority, monkeypatch, running, owner_matches):
    from types import SimpleNamespace
    broker, _, _, _ = runtime_authority
    backend = broker.SystemdBackend()
    calls = []
    row = {'State': {'Running': False}, 'HostConfig': {'CgroupParent': 'owned' if owner_matches else 'other'}}
    monkeypatch.setattr(backend, 'inspect_container', lambda cid: row)
    def daemon(argv, **kwargs):
        calls.append(argv[-2:])
        return SimpleNamespace(returncode=0)
    class InlineThread:
        def __init__(self, *, target, **kwargs): self.target = target
        def start(self): self.target()
    with monkeypatch.context() as patch:
        patch.setattr(broker.subprocess, 'run', daemon)
        patch.setattr(broker.threading, 'Thread', InlineThread)
        backend.remove_after_exit('d'*64, 'owned', running=running)
    assert any(call[0] == 'wait' for call in calls) is running
    assert any(call == ['-v', 'd'*64] for call in calls) is owner_matches
