"""The lead's complete-attempt GPU judgment, including final observation failures."""
import json
import os
import stat
from pathlib import Path
from types import SimpleNamespace

from dataclasses import replace

import pytest

from prismabuild import core as pb, pool
from test_gpu_review_r2 import generic_gpu, _job, _RealGuardGpu, MIB  # noqa: F401
from test_resource_broker import authority, monitored  # noqa: F401


@pytest.mark.parametrize('uuids,failed,covered,final,state,reason', [
    (['GPU-earlier'], False, True, True, 'used', 'attributed_gpu_uuid'),
    (['GPU-final'], False, False, True, 'used', 'attributed_gpu_uuid'),
    (['GPU-earlier'], True, True, False, 'used', 'attributed_gpu_uuid'),
    ([], False, True, True, 'zero', 'successful_payload_sample'),
    ([], True, True, False, 'unobserved', 'final_observation_failed'),
    ([], True, True, True, 'unobserved', 'sampler_error'),
    ([], False, False, True, 'unobserved', 'no_sample_during_lifetime'),
    ([], False, False, False, 'unobserved', 'final_observation_failed'),
])
def test_complete_attempt_core_and_terminal_agree(generic_gpu, monkeypatch,
                                                 uuids, failed, covered, final, state, reason):
    queue, item, action, cas, checkout = generic_gpu
    telemetry = {'gpu_devices_used': len(uuids), 'gpu_uuids': uuids,
                 'gpu_evidence': {'sample_failed': failed, 'live_sample': covered,
                                  'final_succeeded': final}}
    monkeypatch.setattr(pb, '_gpu_broker_request', lambda *a, **k: {
        'resource_telemetry': telemetry, 'nonce': os.environ[pb.ACTION_NONCE_ENV],
        'scope_id': os.environ[pb.ACTION_SCOPE_ENV]})
    if state == 'zero':
        with pytest.raises(pb.LocalActionError, match='zero distinct GPU UUIDs'):
            pb.run_local_action(action, cas_root=cas.root, checkout_root=checkout,
                                resource_demand=queue.demand_of(item))
        assert cas.lookup(action) is None
    else:
        result = pb.run_local_action(action, cas_root=cas.root, checkout_root=checkout,
                                     resource_demand=queue.demand_of(item))
        observed = result['receipt']['resource_telemetry']
        assert (observed['gpu_use'], observed['gpu_use_reason']) == (state, reason)
    monkeypatch.setattr(queue, 'cleanup_action_containers', lambda *a, **k: {
        'complete': True, 'resource_scope': {'telemetry': telemetry}})
    path = queue.finish(item['action_key'], status='executed', detail={'returncode': 0},
                        claim_snapshot=item)
    terminal = json.loads(path.read_text())
    assert terminal['status'] == ('failed' if state == 'zero' else 'executed')
    assert (terminal['detail']['gpu_use'], terminal['detail']['gpu_use_reason']) == (state, reason)


@pytest.mark.parametrize('first,last,covered,state,reason', [
    ('zero', 'zero', True, 'zero', 'successful_payload_sample'),
    ('zero', 'used', False, 'used', 'attributed_gpu_uuid'),
    ('used', 'timeout', True, 'used', 'attributed_gpu_uuid'),
    ('zero', 'timeout', True, 'unobserved', 'final_observation_failed'),
    ('timeout', 'zero', True, 'unobserved', 'sampler_error'),
    ('zero', 'zero', False, 'unobserved', 'no_sample_during_lifetime'),
])
def test_broker_retains_complete_history_and_final_sample(monitored, monkeypatch,
                                                          first, last, covered, state, reason):
    import subprocess
    a, backend, rows, _, monitor = monitored
    request, record, path = rows[0]
    choice = [first]
    gpu = _RealGuardGpu(lambda: [_job(record, path, gpu_bytes=MIB if choice[0] == 'used' else 0)])
    collect = gpu.collect

    def sample(*args, **kwargs):
        if choice[0] == 'timeout':
            raise subprocess.TimeoutExpired('nvidia-smi', 1.)
        return collect(*args, **kwargs)

    monkeypatch.setattr(gpu, 'collect', sample)
    live = type(monitor)(a, gpu)
    monkeypatch.setattr(live, '_payload_live', lambda r: covered)
    live.poll_once()
    choice[0] = last
    live.observe_gpu(record['scope_id'])
    evidence = a.records[record['scope_id']]['resource_telemetry']
    judgment = pb.gpu_execution_judgment(evidence)
    assert (judgment['gpu_use'], judgment['gpu_use_reason']) == (state, reason)
    mirror = a.gpu_evidence_dir / (record['scope_id'] + '.json')
    stored = json.loads(mirror.read_text())
    assert set(stored) == {'action_key', 'nonce', 'scope_id', 'resource_telemetry'}
    assert stored['resource_telemetry'] == evidence
    assert stat.S_IMODE(mirror.stat().st_mode) == 0o444
    assert backend.stopped == []


@pytest.mark.parametrize('uuids,state', [([], 'unobserved'), (['GPU-earlier'], 'used')])
def test_failed_rpc_recovers_only_trusted_prior_evidence(generic_gpu, monkeypatch, uuids, state):
    queue, item, action, cas, checkout = generic_gpu
    previous = {'gpu_devices_used': len(uuids), 'gpu_uuids': uuids,
                'gpu_evidence': {'sample_failed': False, 'live_sample': True, 'final_succeeded': None}}
    monkeypatch.setattr(pb, '_read_broker_gpu_evidence', lambda a: previous)
    result = pb.run_local_action(action, cas_root=cas.root, checkout_root=checkout,
                                 resource_demand=queue.demand_of(item))
    observed = result['receipt']['resource_telemetry']
    assert observed['gpu_use'] == state
    assert observed['gpu_use_reason'] == ('attributed_gpu_uuid' if uuids else 'final_observation_failed')
    # Pool recovers a different sample after publication; the bound receipt
    # remains the attempt's final judgment, including earlier positive use.
    monkeypatch.setattr(queue, 'cleanup_action_containers', lambda *a, **k: {
        'complete': True, 'resource_scope': {'telemetry': {
            'gpu_devices_used': 0, 'gpu_uuids': [], 'gpu_evidence': {
                'sample_failed': False, 'live_sample': True, 'final_succeeded': True}}}})
    claimed = queue.records.item_path(pool.record_store.CLAIMED, item['action_key'])
    record = json.loads(claimed.read_text())
    record['resource_scope'] = {key: os.environ[env] for key, env in
                               [('nonce', pb.ACTION_NONCE_ENV), ('scope_id', pb.ACTION_SCOPE_ENV)]}
    queue.records.replace_record_at(claimed, record)
    path = queue.finish(item['action_key'], status='executed', detail={'returncode': 0}, claim_snapshot=record)
    terminal = json.loads(path.read_text())
    assert terminal['status'] == 'executed'
    assert terminal['detail']['resource_telemetry'] == observed


@pytest.mark.parametrize('fault', [None, 'owner', 'writable_parent', 'symlink', 'attempt', 'oversize'])
def test_broker_evidence_read_requires_protected_ancestry_and_attempt(generic_gpu, tmp_path, monkeypatch, fault):
    queue, item, action, cas, checkout = generic_gpu
    directory = tmp_path / 'gpu-observations'
    directory.mkdir()
    scope = os.environ[pb.ACTION_SCOPE_ENV]
    path = directory / (scope + '.json')
    evidence = {'gpu_devices_used': 1, 'gpu_uuids': ['GPU-root'], 'gpu_evidence': {
        'sample_failed': False, 'live_sample': True, 'final_succeeded': None}}
    record = {'action_key': action['action_key'], 'scope_id': scope,
              'nonce': 'b' * 32 if fault == 'attempt' else os.environ[pb.ACTION_NONCE_ENV],
              'resource_telemetry': evidence}
    path.write_text('x' * 65537 if fault == 'oversize' else json.dumps(record))
    path.chmod(0o444)
    if fault == 'symlink':
        target = directory / 'other.json'
        path.rename(target)
        path.symlink_to(target)
    monkeypatch.setenv('PRISMABUILD_RESOURCE_BROKER_SOCKET', str(tmp_path / 'resources.sock'))
    original = os.fstat

    def ownership(descriptor):
        # CPU fixture models root metadata; no real-root qualification claim.
        info = original(descriptor)
        mode = info.st_mode & ~0o022
        parent = stat.S_ISDIR(mode)
        attrs = {key: getattr(info, key) for key in dir(info) if key.startswith('st_')}
        attrs.update(st_uid=1000 if fault == 'owner' and not parent else 0,
                     st_mode=mode | (0o020 if fault == 'writable_parent' and parent else 0))
        return SimpleNamespace(**attrs)

    with monkeypatch.context() as patch:
        patch.setattr(os, 'fstat', ownership)
        if fault is None:
            assert pb._read_broker_gpu_evidence(action) == evidence
        else:
            with pytest.raises((pb.ActionContractError, OSError)):
                pb._read_broker_gpu_evidence(action)


def test_failed_process_sample_cannot_invent_used(monitored, monkeypatch):
    a, backend, rows, _, monitor = monitored
    request, record, path = rows[0]
    gpu = _RealGuardGpu(lambda: [_job(record, path, gpu_bytes=MIB)])
    collect = gpu.collect
    monkeypatch.setattr(gpu, 'collect', lambda *a, **k: replace(collect(*a, **k), gpu_query_complete=False))
    live = type(monitor)(a, gpu)
    monkeypatch.setattr(live, '_payload_live', lambda row: True)
    live.observe_gpu(record['scope_id'])
    observed = pb.gpu_execution_judgment(a.records[record['scope_id']]['resource_telemetry'])
    assert observed['gpu_use'] == 'unobserved' and observed['gpu_uuids'] == []
