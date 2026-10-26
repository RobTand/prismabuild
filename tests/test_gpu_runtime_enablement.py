"""Default-off host opt-in must leave legacy execution and admission intact."""
import os
import pytest
from prismabuild import gpu_capacity, gpu_runtime_gate
from test_gpu_container_runtime import runtime_authority

from test_pool_resource_scope import scoped

__all__ = ['runtime_authority', 'scoped']


def test_default_off_does_not_read_a_fence(monkeypatch):
    monkeypatch.setattr(gpu_capacity, 'runtime_fence_enabled', lambda: False, raising=False)
    from prismabuild import adaptive_gpu
    def forbidden(*args): raise AssertionError('off path read the fence')
    monkeypatch.setattr(adaptive_gpu, 'trusted_sample', forbidden)
    assert gpu_runtime_gate.runtime_gate_state() is None


def test_disabled_broker_keeps_legacy_container_protocol(runtime_authority, monkeypatch):
    from test_resource_broker import create, container_ticket
    _, authority, _, _ = runtime_authority
    monkeypatch.setattr(gpu_capacity, 'runtime_fence_enabled', lambda: False, raising=False)
    _, record = create(authority)
    replies = []
    original = authority.handle
    def capture(*args):
        result = original(*args)
        replies.append(result)
        return result
    monkeypatch.setattr(authority, 'handle', capture)
    container_ticket(authority, record, monkeypatch)
    assert 'launch_outcome_protocol' not in replies[-1]


@pytest.mark.parametrize('enabled', [False, True])
@pytest.mark.parametrize('old_broker', [False, True])
@pytest.mark.parametrize('old_shim', [False, True])
@pytest.mark.parametrize('status', [0, 127])
def test_mixed_shim_broker_healthy_launches_keep_legacy_bytes(
        runtime_authority, tmp_path, monkeypatch, enabled, old_broker, old_shim, status):
    import importlib.machinery
    import importlib.util
    from pathlib import Path
    import sys
    from types import SimpleNamespace
    from test_gpu_container_runtime import daemon_row
    from test_resource_broker import create
    broker, authority, backend, runtime = runtime_authority
    root = Path(__file__).resolve().parents[1]
    legacy = (root / 'tests/fixtures/gpu_runtime_legacy_1895.txt').read_text()
    monkeypatch.setattr(gpu_capacity, 'runtime_fence_enabled', lambda: enabled)
    loader = importlib.machinery.SourceFileLoader('mixed_runtime_shim', str(root / 'tools/fleet/docker'))
    shim = importlib.util.module_from_spec(importlib.util.spec_from_loader(loader.name, loader))
    loader.exec_module(shim)
    original_main = shim.main
    originals = {name: getattr(shim, name) for name in ('_has_reserved_label', '_container_options')}
    exec(compile(legacy, '<main-before-1895>', 'exec'), shim.__dict__)
    legacy_main = shim.main
    legacy_helpers = {name: getattr(shim, name) for name in originals}
    if old_broker:
        context = dict(vars(broker))
        exec(compile(legacy, '<main-before-1895>', 'exec'), context)
        monkeypatch.setattr(authority, 'container', context['legacy_container'].__get__(authority))
    _, record = create(authority)
    scope = record['scope_id']
    original_read = Path.read_text
    monkeypatch.setattr(Path, 'read_text', lambda path, *a, **kw:
                        f'0::/prismabuild.slice/{scope}/payload\n'
                        if str(path) == f'/proc/{os.getpid()}/cgroup' else original_read(path, *a, **kw))
    monkeypatch.setattr(broker.secrets, 'token_hex', lambda n: 'b' * (n*2))
    monkeypatch.setattr(shim.os, 'urandom', lambda n: b'\x01' * n)
    monkeypatch.setattr(shim, '_resource_scope', lambda: (scope, 1024**3))
    monkeypatch.setattr(shim, '_local_daemon', lambda *a: (['--host', 'unix:///run/docker.sock'], dict(os.environ)))
    monkeypatch.setattr(shim, '_refuse_unaccounted_work', lambda *a, **kw: None)
    monkeypatch.setattr(shim, '_slurm_job_id', lambda: None)
    monkeypatch.setattr(broker, 'DockerLaunchWatch', lambda *a, **kw: SimpleNamespace(close=lambda: None))
    row = daemon_row(record, started=True)
    row['State']['ExitCode'] = status
    row['Config']['Labels']['prismabuild.shim'] = 'b'*64
    backend.inspect_container = lambda cid: row
    cid = tmp_path / 'cid'
    cid.write_text('d'*64)
    marker = tmp_path / 'marker'
    monkeypatch.setenv(shim.OWNER_ENV, 'a'*64)
    monkeypatch.setenv(shim.MARKER_ENV, str(marker))
    monkeypatch.setattr(sys, 'argv', ['docker', 'run', '--rm', '--device=nvidia.com/gpu=all',
                                    '--cidfile', str(cid), 'image', 'exit', str(status)])
    calls, launches = [], []
    def rpc(scope_id, op, **fields):
        calls.append((op, fields))
        return authority.handle(os.getuid(), os.getpid(), {'op': op, 'scope_id': scope_id, **fields})
    monkeypatch.setattr(shim, '_container_broker', rpc)
    def relay(real, argv, environment, **kwargs):
        launches.append((list(argv), {k: str(v) if isinstance(v, Path) else v for k, v in kwargs.items()}))
        return status
    monkeypatch.setattr(shim, '_run_relayed', relay)
    if not old_shim:
        for name, fn in originals.items(): monkeypatch.setattr(shim, name, fn)
    selected_main = legacy_main if old_shim else original_main
    assert selected_main() == status
    assert runtime.state['latched'] is False
    assert marker.read_bytes() == b'a'*64 + b'\n'
    assert cid.read_bytes() == b'd'*64
    typed = enabled and not old_broker and not old_shim
    assert [op for op, _ in calls] == ['container_begin', *(['container_watch', 'container_outcome'] if typed else []),
                                      *(['container_end'] if status == 0 else [])]
    if typed:
        assert calls[1][1]['nvidia_injection'] is True
        assert 'prismabuild.shim=' + 'b'*64 in launches[0][0]
    else:
        # Run the pinned main implementation against the same broker and
        # compare complete Docker argv, relay kwargs, marker and cidfile bytes.
        candidate = launches.pop()
        calls.clear()
        for name, fn in legacy_helpers.items(): monkeypatch.setattr(shim, name, fn)
        assert legacy_main() == status
        assert launches == [candidate]
        assert marker.read_bytes() == b'a'*64 + b'\n'
        assert cid.read_bytes() == b'd'*64


@pytest.mark.parametrize('enabled', [False, True])
@pytest.mark.parametrize('latched', [False, True])
def test_new_workers_with_legacy_siblings_keep_cpu_and_healthy_gpu_claims(
        tmp_path, monkeypatch, enabled, latched):
    import json
    from pathlib import Path
    from prismabuild import adaptive_gpu, pool
    from admitted_queue_fixture import AdmittedQueueFixture
    state = {'schema': gpu_capacity.RUNTIME_SCHEMA, 'latched': latched,
             'reason': gpu_capacity.RUNTIME_REASON, 'subtype': 'cdi_mount_missing'}
    monkeypatch.setattr(gpu_capacity, 'runtime_fence_enabled', lambda: enabled)
    monkeypatch.setattr(adaptive_gpu, 'trusted_sample', lambda path=None: state)
    queue = AdmittedQueueFixture(pool.PoolQueue(tmp_path / 'queue'),
        capacity={'cpu': 2, 'mem_gb': 4, 'gpu': 1}, default_demand={'cpu': 1, 'mem_gb': 1})
    queue.announce(host='new', has_gpu=True, tags=[], capacity={'cpu': 2, 'gpu': 1},
                   observed_capacity={'cpu': 2, 'gpu': 1})
    offer = json.loads((queue.root / 'workers/new.json').read_text())
    assert offer['observed_capacity'] == {'cpu': 2, 'gpu': 0 if enabled and latched else 1}
    for key, resources in [('a'*64, {'cpu': 1, 'mem_gb': 1}), ('b'*64, {'cpu': 1, 'mem_gb': 1, 'gpu': 1})]:
        queue.publish(action_key=key, cas_root=str(tmp_path / 'cas'), checkout_root=str(tmp_path),
                      worker_script='/worker.py', resources=resources)
    first = queue.claim(has_gpu=True)
    claimed = queue.claim(has_gpu=True)
    if enabled and latched:
        assert first['action_key'] == 'a'*64
        assert claimed is None
        # A legacy sibling has no reader of the new mirror. This intentionally
        # pins execution compatibility rather than claiming rollout protection.
        with monkeypatch.context() as old:
            old.setattr(gpu_runtime_gate, 'runtime_gate_state', lambda: None)
            sibling = pool.PoolQueue(queue.root)
            assert sibling.claim(has_gpu=True, capacity={'cpu': 2, 'mem_gb': 4, 'gpu': 1})['action_key'] == 'b'*64
    else:
        assert {first['action_key'], claimed['action_key']} == {'a'*64, 'b'*64}


@pytest.mark.parametrize('value,mode,uid,symlink,expected', [
    (None, 0o644, 0, False, False), (b'enabled\n', 0o644, 0, False, True),
    (b'enabled\n', 0o666, 0, False, False), (b'enabled\n', 0o644, 1000, False, False),
    (b'true\n', 0o644, 0, False, False), (b'enabled\n', 0o644, 0, True, False),
])
def test_host_opt_in_requires_exact_root_owned_regular_file(tmp_path, monkeypatch, value, mode, uid, symlink, expected):
    flag = tmp_path / 'enabled'
    monkeypatch.setattr(gpu_capacity, 'RUNTIME_ENABLE', flag)
    if value is not None:
        flag.write_bytes(value)
        flag.chmod(mode)
        if symlink:
            flag.rename(tmp_path / 'target')
            flag.symlink_to(tmp_path / 'target')
    import stat
    original = os.fstat
    def info(fd):
        actual = original(fd)
        values = list(actual)
        if stat.S_ISDIR(actual.st_mode):
            values[0], values[4] = 0o40755, 0
        else:
            values[4] = uid
        return os.stat_result(values)
    monkeypatch.setattr(os, 'fstat', info)
    monkeypatch.setenv('PRISMABUILD_GPU_RUNTIME_FENCE', 'enabled')
    try:
        assert gpu_capacity.runtime_fence_enabled() is expected
    finally:
        monkeypatch.undo()


def test_off_ending_does_not_request_typed_broker_state(scoped, monkeypatch):
    from prismabuild import resource_scope
    from test_pool_resource_scope import _process
    monkeypatch.setattr(gpu_capacity, 'runtime_fence_enabled', lambda: False)
    queue, item, calls = scoped
    _process(monkeypatch, queue, item, calls, returncode=1)
    def forbidden(self): raise AssertionError('off path requested a typed outcome')
    monkeypatch.setattr(resource_scope.ResourceScope, 'container_host_fault', forbidden)
    outcome = queue.execute(item, containment=True)
    assert outcome['status'] == 'failed'
    assert 'host_fault' not in outcome
    assert 'container_launch_outcome_error' not in outcome


@pytest.mark.parametrize('argv,expected', [
    (['--device=nvidia.com/gpu=all', 'image'], {'devices': ['nvidia.com/gpu=all']}),
    (['--device', 'nvidia.com/gpu=all', 'image'], {'devices': ['nvidia.com/gpu=all']}),
    (['--runtime=nvidia', '--runtime=runc', 'image'], {'runtime': 'runc'}),
    (['--gpus=all', 'image'], {'gpus': ['all']}),
    (['--gpus', 'driver=nvidia,count=all', 'image'], {'gpus': ['driver=nvidia,count=all']}),
    (['--entrypoint', '--gpus=all', 'image'], {}),
    (['image', '--gpus=all'], {}),
    (['--entrypoint', '--device=nvidia.com/gpu=all', 'image'], {}),
    (['image', '--device=nvidia.com/gpu=all'], {}),
    (['--', 'image', '--runtime=nvidia'], {}),
])
def test_runtime_intent_uses_the_existing_argv_parser(argv, expected):
    import importlib.machinery
    import importlib.util
    from pathlib import Path
    path = Path(__file__).resolve().parents[1] / 'tools/fleet/docker'
    loader = importlib.machinery.SourceFileLoader('intent_shim', str(path))
    shim = importlib.util.module_from_spec(importlib.util.spec_from_loader(loader.name, loader))
    loader.exec_module(shim)
    intent = {}
    baseline = shim._container_options(argv)
    assert shim._container_options(argv, runtime_intent=intent) == baseline
    assert intent == expected


@pytest.mark.parametrize('enabled', [False, True])
def test_readiness_label_is_reserved_only_when_enabled(tmp_path, monkeypatch, enabled):
    import importlib.machinery
    import importlib.util
    from pathlib import Path
    loader = importlib.machinery.SourceFileLoader('label_shim', str(Path(__file__).resolve().parents[1] / 'tools/fleet/docker'))
    shim = importlib.util.module_from_spec(importlib.util.spec_from_loader(loader.name, loader))
    loader.exec_module(shim)
    monkeypatch.setattr(gpu_capacity, 'runtime_fence_enabled', lambda: enabled)
    assert shim._has_reserved_label(['--label', 'prismabuild.runtime-readiness=caller']) is enabled
    assert shim._has_reserved_label(['--label', 'prismabuild.shim=caller'])
