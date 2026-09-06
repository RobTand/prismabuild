"""Identical hardware has identical policy evidence; unknown counters cannot lend."""
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import time

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from prismabuild import gpu_capacity as gc


def raw(*, name='NVIDIA GB10', gpu='GPU-a', draw='4.75', limit='[N/A]',
        total='[N/A]', free='[N/A]', used='[N/A]', flags=None):
    row = dict(zip(gc.FIELDS, [name, gpu, draw, limit, limit, '208', '3003',
                             total, free, used, '0x0000000000000004',
                             *(['Not Active'] * len(gc.THROTTLE_FIELDS))]))
    for key, value in (flags or {}).items():
        row['clocks_event_reasons.' + key] = value
    return ', '.join(row[field] for field in gc.FIELDS) + '\n'


def query(monkeypatch, output):
    def run(argv, **kwargs):
        assert argv[0] == '/usr/bin/nvidia-smi'
        assert kwargs['timeout'] <= 5
        assert not any('utilization.gpu' in word for word in argv)
        return subprocess.CompletedProcess(argv, 0, output, '')
    monkeypatch.setattr(gc.subprocess, 'run', run)


def fixture(tmp_path):
    proc = tmp_path / 'proc'
    (proc / 'pressure').mkdir(parents=True)
    (proc / 'pressure/cpu').write_text('some avg10=0.0 total=0\nfull avg10=0.0 total=0\n')
    sid = 'prismabuild-job' + 'a' * 32 + '.slice'
    record = {'action_key': 'a' * 64, 'nonce': 'b' * 32, 'cgroup_identity': [1, 2],
              'memory_max_bytes': 4096, 'token': 'SECRET_MUST_NOT_APPEAR'}
    job = {'scope_id': sid, 'cgroup_identity': [1, 2], 'budget_bytes': 4096,
           'host_bytes': 1024, 'gpu_reported_bytes': 512, 'gpu_lower_bound_bytes': 512,
           'lower_bound_bytes': 1024, 'upper_bound_bytes': 1536, 'complete': True,
           'processes': [], 'errors': []}
    sample = {'sampled_unix': time.time(), 'jobs': [job], 'gpu_query_complete': True,
              'host_total_bytes': 16384, 'host_available_bytes': 8192,
              'psi_some_avg10': 0., 'psi_full_avg10': 0., 'foreign_processes': [], 'errors': []}
    return proc, sid, {sid: record}, sample


def test_both_gb10s_use_same_hardware_reference_without_inventing_gpu_limit(monkeypatch):
    query(monkeypatch, raw(gpu='GPU-sparky') + raw(gpu='GPU-sparklina', draw='4.55'))
    devices, errors = gc.devices()
    assert not errors and len(devices) == 2
    for device in devices:
        assert device['power_limit_w'] is None
        assert device['power_reference_w'] == 140
        assert device['power_reference_scope'] == 'soc_tdp'
        assert device['memory_domain'] == 'shared_system'
        assert device['memory_total_bytes'] is None
        assert device['complete'] and not device['limited']
    comparable = lambda d: {k: v for k, v in d.items() if k not in {'uuid', 'power_w'}}
    assert comparable(devices[0]) == comparable(devices[1])


def test_desktop_vram_remains_separate_from_system_ram(monkeypatch):
    query(monkeypatch, raw(name='NVIDIA GeForce RTX 4090', limit='450',
                          total='24576', free='20000', used='4576'))
    device = gc.devices()[0][0]
    assert device['memory_domain'] == 'discrete'
    assert device['memory_total_bytes'] == 24576 * gc.MIB
    assert device['memory_free_bytes'] == 20000 * gc.MIB
    assert device['power_reference_w'] == 450
    assert device['power_reference_scope'] == 'gpu_power_limit'


@pytest.mark.parametrize('options', [dict(name='Unknown SoC', limit='140', total='128000', free='64000', used='64000'),
                                     dict(draw='nan'), dict(draw='[N/A]'),
                                     dict(flags={'hw_thermal_slowdown': '[N/A]'})])
def test_unknown_hardware_or_missing_critical_counters_is_incomplete(monkeypatch, options):
    query(monkeypatch, raw(**options))
    device = gc.devices()[0][0]
    assert not device['complete']


def test_named_thermal_limit_is_not_treated_as_idle(monkeypatch):
    query(monkeypatch, raw(flags={'hw_thermal_slowdown': 'Active', 'gpu_idle': 'Not Active'}))
    assert gc.devices()[0][0]['limited'] is True
    query(monkeypatch, raw(flags={'gpu_idle': 'Active'}))
    assert gc.devices()[0][0]['limited'] is False


def test_bounded_query_errors_remain_unknown(monkeypatch):
    def timeout(argv, **kwargs):
        raise subprocess.TimeoutExpired(argv, kwargs['timeout'])
    monkeypatch.setattr(gc.subprocess, 'run', timeout)
    assert gc.devices()[0] == []
    assert 'TimeoutExpired' in gc.devices()[1][0]


def test_public_snapshot_joins_exact_attempt_without_authority_secrets(tmp_path, monkeypatch):
    proc, sid, records, sample = fixture(tmp_path)
    query(monkeypatch, raw())
    public = gc.collect(sample, records, proc_root=proc)
    assert public['complete'] and public['attributed']
    assert public['jobs'][0]['action_key'] == 'a' * 64
    assert public['jobs'][0]['nonce'] == 'b' * 32
    assert public['sampled_unix'] == sample['sampled_unix']
    assert 'SECRET' not in json.dumps(public)
    assert public['sample_id'] != gc.collect(sample, records, proc_root=proc)['sample_id']


@pytest.mark.parametrize('fault', ['foreign_missing', 'stale', 'future', 'scope_missing',
                                   'scope_new', 'nonce_bad', 'inode_changed', 'incomplete', 'budget_changed'])
def test_missing_or_mismatched_job_authority_refuses_credit(tmp_path, monkeypatch, fault):
    proc, sid, records, sample = fixture(tmp_path)
    query(monkeypatch, raw())
    if fault == 'foreign_missing': sample.pop('foreign_processes')
    elif fault == 'stale': sample['sampled_unix'] -= 10
    elif fault == 'future': sample['sampled_unix'] += 10
    elif fault == 'scope_missing': records.clear()
    elif fault == 'scope_new': records['another'] = dict(records[sid])
    elif fault == 'nonce_bad': records[sid]['nonce'] = 'bad'
    elif fault == 'inode_changed': records[sid]['cgroup_identity'] = [1, 3]
    elif fault == 'incomplete': sample['jobs'][0]['complete'] = False
    elif fault == 'budget_changed': records[sid]['memory_max_bytes'] += 1
    result = gc.collect(sample, records, proc_root=proc)
    assert not result['complete'] and not result['attributed']


def test_foreign_zero_byte_context_still_appears(tmp_path, monkeypatch):
    proc, sid, records, sample = fixture(tmp_path)
    query(monkeypatch, raw())
    sample['foreign_processes'] = [{'pid': 123, 'used_bytes': 0, 'gpu_uuid': 'GPU-a'}]
    result = gc.collect(sample, records, proc_root=proc)
    assert result['complete'] and result['foreign_process_count'] == 1
    assert result['foreign_processes'][0]['pid'] == 123


def test_idle_broker_can_publish_a_complete_coldstart_sample(tmp_path, monkeypatch):
    proc, sid, records, sample = fixture(tmp_path)
    query(monkeypatch, raw())
    sample['jobs'] = []
    result = gc.collect(sample, {}, proc_root=proc)
    assert result['complete'] and result['jobs'] == []


def test_device_query_can_be_reused_before_memory_collection(tmp_path, monkeypatch):
    proc, sid, records, sample = fixture(tmp_path)
    query(monkeypatch, raw())
    readings = gc.devices()
    monkeypatch.setattr(gc.subprocess, 'run', lambda *a, **k: pytest.fail('duplicate device query'))
    assert gc.collect(sample, records, proc_root=proc, device_readings=readings)['complete']


def test_broker_publishes_root_readable_idle_sample_and_reuses_device_query(tmp_path, monkeypatch):
    from types import SimpleNamespace
    import threading
    broker_path = Path(__file__).resolve().parents[1] / 'tools/fleet/resource_broker.py'
    spec = importlib.util.spec_from_file_location('gpu_capacity_broker_test', broker_path)
    broker = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(broker)
    proc, sid, records, sample = fixture(tmp_path)
    sample['jobs'] = []
    query(monkeypatch, raw())
    order = []

    class Memory:
        def Guard(self): return self
        def observe(self, snapshot): return []
        def collect(self, scopes, *, timeout_s, gpu_memory_domains):
            order.append('memory')
            assert scopes == []
            assert gpu_memory_domains == {'GPU-a': 'shared_system'}
            return SimpleNamespace(as_dict=lambda: sample)

    class Capacity:
        def devices(self, *, timeout_s):
            order.append('devices')
            return gc.devices(timeout_s=timeout_s)
        def collect(self, snapshot, records, **kwargs):
            order.append('publish')
            return gc.collect(snapshot, records, proc_root=proc, **kwargs)

    state = tmp_path / 'run/jobs'
    state.mkdir(parents=True)
    authority = SimpleNamespace(state_dir=state, records={}, lock=threading.RLock())
    monitor = broker.ResourceMonitor(authority, Memory(), capacity_module=Capacity())
    monitor.poll_once()
    public = state.parent / 'gpu-capacity.json'
    assert json.loads(public.read_text())['complete']
    assert public.stat().st_mode & 0o777 == 0o644
    assert (state / 'monitor.status').stat().st_mode & 0o777 == 0o600
    assert order == ['devices', 'memory', 'publish']


def test_public_freshness_uses_older_power_sample(tmp_path, monkeypatch):
    proc, sid, records, sample = fixture(tmp_path)
    query(monkeypatch, raw())
    readings = gc.devices()
    readings[0][0]['sampled_unix'] = sample['sampled_unix'] - 2
    public = gc.collect(sample, records, proc_root=proc, device_readings=readings)
    assert public['sampled_unix'] == sample['sampled_unix'] - 2
    assert public['jobs'][0]['sampled_unix'] == sample['sampled_unix']
