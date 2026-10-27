"""Exercise #1895 through real claim, offer, shim and ending paths with fakes."""
import importlib.machinery
import importlib.util
import itertools
import json
import os
from pathlib import Path
import socket
import sys

import pytest
from prismabuild import adaptive_cpu, adaptive_gpu, adaptive_snapshot, census, core, gpu_capacity, gpu_runtime_gate, movement_actions, pool
from admitted_queue_fixture import AdmittedQueueFixture
from test_pool_resource_scope import scoped, _process


def test_sibling_claimants_deny_gpu_by_name_but_cpu_claims_continue(tmp_path, monkeypatch):
    state = {'schema': gpu_capacity.RUNTIME_SCHEMA, 'latched': True,
             'reason': gpu_capacity.RUNTIME_REASON, 'subtype': 'cdi_mount_missing'}
    monkeypatch.setattr(gpu_runtime_gate, 'runtime_gate_state', lambda: state)
    queue = AdmittedQueueFixture(pool.PoolQueue(tmp_path / 'queue'),
        capacity={'cpu': 2, 'mem_gb': 4, 'gpu': 1}, default_demand={'cpu': 1, 'mem_gb': 1})
    for key, resources, priority in [('a'*64, {'cpu': 1, 'gpu': 1, 'mem_gb': 1}, 1),
                                      ('b'*64, {'cpu': 1, 'mem_gb': 1}, 0)]:
        queue.publish(action_key=key, cas_root=str(tmp_path / 'cas'), checkout_root=str(tmp_path),
                      worker_script='/worker.py', resources=resources, priority=priority)
    item = queue.claim(has_gpu=True)
    assert item['action_key'] == 'b'*64
    sibling = pool.PoolQueue(queue.root)
    assert sibling.claim(has_gpu=True, **simulated_admission(sibling, simulated_capacity({'cpu': 2, 'mem_gb': 4, 'gpu': 1}))) is None
    local = adaptive_cpu.local_state_base(queue.ledger().base)
    records = adaptive_cpu.read_json(local / pool.CLAIM_DENIALS)['records']
    denial = next(row for row in records.values() if row['action_key'] == 'a'*64)
    assert denial['reason'] == gpu_capacity.RUNTIME_REASON
    assert denial['evidence']['subtype'] == 'cdi_mount_missing'
    adaptive_snapshot.copy_snapshot(local, queue.ledger().base / 'adaptive')
    jobs = census.action_rows(queue.root).value['jobs']
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'tools/fleet'))
    import pbstatus
    assert gpu_capacity.RUNTIME_REASON in '\n'.join(pbstatus.pool_job_lines(jobs, {}))
    state['latched'] = False
    monkeypatch.setattr(gpu_runtime_gate, 'runtime_gate_state', lambda: None)
    assert sibling.claim(has_gpu=True, **simulated_admission(sibling, simulated_capacity({'cpu': 2, 'mem_gb': 4, 'gpu': 1})))['action_key'] == 'a'*64


def test_each_offer_masks_gpu_immediately_and_preserves_cpu_and_configured_bounds(tmp_path, monkeypatch):
    gate = {'latched': True, 'reason': gpu_capacity.RUNTIME_REASON, 'subtype': 'cdi_mount_missing'}
    monkeypatch.setattr(gpu_runtime_gate, 'runtime_gate_state', lambda: gate)
    queue = pool.PoolQueue(tmp_path / 'queue')
    queue.announce(host='fixture', has_gpu=True, tags=[], capacity={'cpu': 4, 'gpu': 1},
                   observed_capacity={'cpu': 3, 'gpu': 1})
    offer = json.loads((queue.root / 'workers/fixture.json').read_text())
    assert offer['capacity'] == {'cpu': 4, 'gpu': 1}
    assert offer['observed_capacity'] == {'cpu': 3, 'gpu': 0}
    assert offer['observed_detail'][gpu_capacity.RUNTIME_REASON] == gate
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'tools/fleet'))
    import pbstatus
    assert gpu_capacity.RUNTIME_REASON in '\n'.join(pbstatus.pool_node_lines([{
        'node': 'fixture', 'state': 'live', 'observed_detail': offer['observed_detail']}]))


@pytest.mark.parametrize('change', [None, 'command', 'task', 'cpu', 'host', 'image', 'timeout', 'token', 'path', 'env', 'budget', 'exclusive', 'threads'])
def test_readiness_exception_binds_the_sealed_recipe(tmp_path, change):
    (tmp_path / 'source.py').write_text('')
    command = gpu_capacity.runtime_probe_command()
    params = {'command': command, 'demand': {'cpu': 1, 'mem_gb': 16, 'gpu': 1},
              'placement': {'required_tags': [socket.gethostname()]},
              'container_images': [gpu_capacity.RUNTIME_PROBE_IMAGE], 'execution_timeout_s': 30.0,
              'gpu_memory_gb': 1.0, 'gpu_exclusive': True}
    prefix = str(Path(__file__).resolve().parents[1] / 'tools/fleet')
    env = {'PATH': prefix + ':/usr/local/bin:/usr/bin:/bin', gpu_capacity.RUNTIME_PROBE_ENV: 'e'*64,
           'OMP_NUM_THREADS': '1', 'MKL_NUM_THREADS': '1', 'OPENBLAS_NUM_THREADS': '1'}
    argv = movement_actions.standard_capture_argv(command, 'result.log', path_prefix=prefix)
    if change == 'command': params['command'] = ['docker', 'run', 'evil']
    if change == 'task': argv += ['unbound']
    if change == 'cpu': params['demand']['cpu'] = 2
    if change == 'host': params['placement']['required_tags'] = ['other-host']
    if change == 'image': params['container_images'] = ['mutable:latest']
    if change == 'timeout': params['execution_timeout_s'] = 300.0
    if change == 'token': env.pop(gpu_capacity.RUNTIME_PROBE_ENV)
    if change == 'path': env['PATH'] = '/evil:/usr/bin'
    if change == 'env': env['BASH_ENV'] = '/evil/script'
    if change == 'budget': params['gpu_memory_gb'] = 2.0
    if change == 'exclusive': params['gpu_exclusive'] = False
    if change == 'threads': env['OMP_NUM_THREADS'] = '2'
    action = core.seal_action({'schema': core.ACTION_SCHEMA_V2,
        'task': {'definition_id': 'fleet/pbrun', 'definition_version': 'v1', 'task_class': 'generation',
                 'determinism': 'deterministic', 'artifact_family': 'generic', 'artifact_kind': 'generic',
                 'argv': argv, 'working_directory': '.', 'result_path': 'result.log'},
        'inputs': [], 'code_closure': core.build_code_closure(tmp_path, ['source.py']), 'params': params,
        'environment': {'variables': env, 'toolchain': {}},
        'execution_scope': {'portability': 'portable', 'platform_key': None, 'host_class': None}})
    cas = core.PrismaBuildCAS(tmp_path / 'cas')
    cas.publish_action_request(action)
    token = gpu_runtime_gate.runtime_probe_identity({'action_key': action['action_key'], 'cas_root': str(cas.root)})
    assert token == ('e'*64 if change is None else None)


def test_broker_host_fault_survives_failed_ending_and_charges_attempt(scoped, monkeypatch):
    from prismabuild import resource_scope
    monkeypatch.setattr(gpu_capacity, 'runtime_fence_enabled', lambda: True)
    queue, item, calls = scoped
    _process(monkeypatch, queue, item, calls, returncode=1)
    monkeypatch.setattr(resource_scope.ResourceScope, 'container_host_fault', lambda self: {
        'reason': gpu_capacity.RUNTIME_REASON, 'subtype': 'cdi_mount_missing', 'attempt': self.nonce})
    outcome = queue.execute(item, containment=True)
    assert outcome['status'] == 'failed'
    assert outcome['termination_reason'] == 'host_fault: gpu_container_runtime_unready/cdi_mount_missing'
    path = queue.records.item_path(pool.record_store.CLAIMED, item['action_key'])
    record = json.loads(path.read_text())
    record['max_attempts'] = item['max_attempts'] = 1
    queue.records.replace_record_at(path, record)
    terminal = queue.finish(item['action_key'], status='failed', detail=outcome, claim_snapshot=item)
    filed = json.loads(terminal.read_text())
    assert filed['status'] == 'failed'
    assert filed['attempts'] == 1
    assert filed['detail']['termination_reason'] == outcome['termination_reason']


@pytest.mark.parametrize('gpu_args,nvidia', [
    ([], False),
    (['--gpus', 'all'], True),
    (['--gpus=driver=nvidia,count=all'], True),
    (['--gpus', '"device=0,1",capabilities=utility'], True),
    (['--gpus', 'driver=other,count=all'], False),
    (['--gpus', 'driver=other', '--gpus', 'all'], True),
    (['--entrypoint', '--gpus=all'], False),
])
@pytest.mark.parametrize('status', [0, 127])
def test_shim_preserves_native_auto_removal_and_binds_each_launch(tmp_path, monkeypatch, status, gpu_args, nvidia):
    monkeypatch.setattr(gpu_capacity, 'runtime_fence_enabled', lambda: True)
    shim_path = Path(__file__).resolve().parents[1] / 'tools/fleet/docker'
    loader = importlib.machinery.SourceFileLoader('runtime_shim_test', str(shim_path))
    spec = importlib.util.spec_from_loader(loader.name, loader)
    shim = importlib.util.module_from_spec(spec)
    loader.exec_module(shim)
    cidfile = tmp_path / 'cid'
    cidfile.write_text('d'*64)
    monkeypatch.setattr(shim, '_resource_scope', lambda: ('prismabuild-job'+'a'*32+'.slice', 1024**3))
    monkeypatch.setattr(shim, '_local_daemon', lambda *args: (['--host', 'unix:///run/docker.sock'], dict(os.environ)))
    monkeypatch.setattr(shim, '_refuse_unaccounted_work', lambda *args, **kwargs: None)
    calls = []
    def broker(scope, op, **fields):
        calls.append((op, fields))
        return {'ticket': 'b'*64, 'launch_outcome_protocol': 2}
    monkeypatch.setattr(shim, '_container_broker', broker)
    def launched(real, argv, *args, **kwargs):
        assert '--rm' in argv
        assert 'prismabuild.shim=' + 'b'*64 in argv
        # A spoofed daemon error on inherited stdout cannot be classified.
        return status
    monkeypatch.setattr(shim, '_run_relayed', launched)
    monkeypatch.setenv(shim.OWNER_ENV, 'a'*64)
    monkeypatch.setenv(shim.MARKER_ENV, str(tmp_path / 'marker'))
    monkeypatch.setattr(sys, 'argv', ['docker', 'run', '--rm', *gpu_args, '--cidfile', str(cidfile), 'image'])
    assert shim.main() == status
    assert calls[1] == ('container_watch', {'ticket': 'b'*64, 'nvidia_injection': nvidia})
    assert calls[2] == ('container_outcome', {'ticket': 'b'*64, 'container_id': 'd'*64, 'daemon_stderr': ''})
    assert [op for op, _ in calls] == ['container_begin', 'container_watch', 'container_outcome', *(['container_end'] if status == 0 else [])]


@pytest.mark.parametrize('scoped', [{'execution_timeout_s': 1e-9}], indirect=True)
def test_host_fault_evidence_preserves_an_explicit_deadline(scoped, monkeypatch):
    from prismabuild import resource_scope
    monkeypatch.setattr(gpu_capacity, 'runtime_fence_enabled', lambda: True)
    queue, item, calls = scoped
    _process(monkeypatch, queue, item, calls, ticks=1)
    monkeypatch.setattr(resource_scope.ResourceScope, 'container_host_fault', lambda self: {
        'reason': gpu_capacity.RUNTIME_REASON, 'subtype': 'cdi_mount_missing', 'attempt': self.nonce})
    ticks = itertools.count()
    clock = pool.ExecutionClock(lambda: float(next(ticks)), lambda: 1_000_000.0)
    outcome = queue.execute(item, containment=True, timeout_s=1e-9, clock=clock)
    assert outcome['status'] == 'timeout'
    assert outcome['termination_reason'] == 'execution_deadline'
    assert outcome['host_fault']['subtype'] == 'cdi_mount_missing'


def test_host_fault_evidence_preserves_a_stall_without_a_sealed_deadline(scoped, monkeypatch):
    from prismabuild import resource_scope
    monkeypatch.setattr(gpu_capacity, 'runtime_fence_enabled', lambda: True)
    queue, item, calls = scoped
    _process(monkeypatch, queue, item, calls, ticks=1)
    monkeypatch.setattr(resource_scope.ResourceScope, 'container_host_fault', lambda self: {
        'reason': gpu_capacity.RUNTIME_REASON, 'subtype': 'cdi_mount_missing', 'attempt': self.nonce})
    ticks = itertools.count()
    clock = pool.ExecutionClock(lambda: float(next(ticks)), lambda: 1_000_000.0)
    outcome = queue.execute(item, containment=True, timeout_s=1e-9, clock=clock)
    assert outcome['status'] == 'timeout'
    assert outcome['termination_reason'] == 'no_progress'
    assert outcome['host_fault']['subtype'] == 'cdi_mount_missing'


def test_broker_recipe_survives_real_client_sealing_and_gpu_claim(tmp_path, monkeypatch, capsys):
    import pbrun
    from submission_support import patch_store
    from test_pbrun_detach import _checkout, _queue, _one_json_line
    from test_resource_broker import module
    from prismabuild import submission
    work = _checkout(tmp_path)
    from test_pbrun_declares_container_images import _offer_images
    q = _queue(tmp_path)
    _offer_images(q, gpu_capacity.RUNTIME_PROBE_IMAGE)
    (tmp_path / 'run').mkdir()
    monkeypatch.setattr(socket, 'gethostname', lambda: 'sparky')
    store = patch_store(monkeypatch, root=tmp_path, runtime_root=Path(__file__).resolve().parents[1])
    captured = []
    broker = module()
    runtime = broker.GPUContainerRuntime(gpu_capacity, tmp_path / 'durable/gpu.json',
        tmp_path / 'run/gpu.json', __import__('threading').RLock(), cdi_dir=tmp_path / 'cdi')
    try:
        def capture(argv, **kwargs):
            from types import SimpleNamespace
            captured.append(argv)
            return SimpleNamespace(returncode=0, stdout='', stderr='')
        with monkeypatch.context() as patch:
            patch.setattr(broker.subprocess, 'run', capture)
            runtime.submit_probe(os.getuid(), 'e'*64)
        command = captured[0]
        command = command[command.index('/mnt/shared/prismabuild-fleet/repo/tools/pbrun.py')+1:]
        command[command.index('--cwd')+1] = str(work)
        command.remove('--detach')
        command.insert(0, '--seal-only')
        assert pbrun.main(command) == 0
        sealed = _one_json_line(capsys.readouterr())
        item = {'action_key': sealed['action_key'], 'cas_root': str(store.cas_root)}
        assert gpu_runtime_gate.runtime_probe_identity(item) == 'e'*64
        assert pbrun.main(['--publish-sealed', sealed['action_key'], '--detach']) == 0
        capsys.readouterr()
        monkeypatch.setattr(gpu_runtime_gate, 'runtime_gate_state', lambda: {
            'latched': True, 'reason': gpu_capacity.RUNTIME_REASON})
        claim_args = dict(tags=['sparky', 'gb10', 'container-image-v1'], has_gpu=True,
                          observed_images=[gpu_capacity.RUNTIME_PROBE_IMAGE])
        assert q.claim(**simulated_admission(q, simulated_capacity({'cpu': 1, 'mem_gb': 8, 'gpu': 1})), **claim_args) is None
        claimed = q.claim(**simulated_admission(q, simulated_capacity({'cpu': 1, 'mem_gb': 32, 'gpu': 1})), **claim_args)
        assert claimed is not None, adaptive_cpu.read_json(
            adaptive_cpu.local_state_base(q.ledger().base) / pool.CLAIM_DENIALS)
        assert claimed['action_key'] == sealed['action_key']
        assert claimed['resources'] == {'cpu': 1, 'mem_gb': 16, 'gpu': 1}
    finally:
        runtime.close()
