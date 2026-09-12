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
        def collect(self, scopes, *, timeout_s, gpu_memory_domains, gpu_devices):
            order.append('memory')
            assert scopes == []
            assert gpu_memory_domains == {'GPU-a': 'shared_system'}
            # Which reader can attribute a process is a property of the device,
            # so the census gets the one device query the broker already made.
            assert [device['uuid'] for device in gpu_devices] == ['GPU-a']
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


ROCMINFO_REPORT = """
Agent 1
*******
  Name:                    AMD Ryzen 7 9800X3D 8-Core Processor
  Uuid:                    CPU-XX
  Marketing Name:          AMD Ryzen 7 9800X3D 8-Core Processor
  Device Type:             CPU
  Compute Unit:            16
  Wavefront Size:          0(0x0)
  Pool Info:
    Pool 1
      Segment:                 GLOBAL; FLAGS: FINE GRAINED
      Size:                    28737164(0x1b68e0c) KB
      Allocatable:             TRUE
Agent 2
*******
  Name:                    gfx1201
  Uuid:                    GPU-9c30c352a59e5b7a
  Marketing Name:          AMD Radeon RX 9070 XT
  Device Type:             GPU
  Max Clock Freq. (MHz):   2400
  Compute Unit:            64
  Wavefront Size:          32(0x20)
  Workgroup Max Size:      1024(0x400)
  Workgroup Max Size per Dimension:
    x                        1024(0x400)
  Pool Info:
    Pool 1
      Segment:                 GLOBAL; FLAGS: COARSE GRAINED
      Size:                    16577056(0xfcf220) KB
      Allocatable:             TRUE
    Pool 2
      Segment:                 GROUP
      Size:                    64(0x40) KB
*** Done ***
"""

VRAM_BYTES = 16577056 * 1024
HIP_REPORT = {'device_count': 1, 'memory_total_bytes': VRAM_BYTES,
              'memory_free_bytes': 15 * 1024**3, 'integrated': 0, 'warp_size': 32,
              'clock_rate_khz': 2400000, 'max_threads_per_block': 1024}


def amd(monkeypatch, tmp_path, *, report=ROCMINFO_REPORT, probe=None, error=None):
    """Install an AMD box: rocminfo present, NVML absent, both readers faked."""
    present = tmp_path / 'rocminfo'
    present.write_text('')
    monkeypatch.setattr(gc, 'ROCMINFO', str(present))

    def unavailable(argv, **kwargs):
        raise FileNotFoundError(2, 'No such file or directory')

    monkeypatch.setattr(gc.subprocess, 'run', unavailable)
    monkeypatch.setattr(gc, '_run_rocminfo', lambda timeout_s: (report, None))
    monkeypatch.setattr(gc, '_run_hip_probe',
                        lambda timeout_s: (None, error) if error else (dict(probe or HIP_REPORT), None))


def test_amd_gpu_is_published_from_two_agreeing_runtime_readers(monkeypatch, tmp_path):
    amd(monkeypatch, tmp_path)
    devices, errors = gc.devices()

    assert not errors and len(devices) == 1
    device = devices[0]
    assert device['uuid'] == 'GPU-9c30c352a59e5b7a'
    assert device['vendor'] == 'amd'
    assert device['telemetry_class'] == gc.TELEMETRY_MEMORY_ONLY
    assert device['memory_domain'] == 'discrete'
    assert device['memory_total_bytes'] == VRAM_BYTES
    assert device['memory_free_bytes'] == 15 * 1024**3
    assert device['memory_used_bytes'] == VRAM_BYTES - 15 * 1024**3
    assert device['compute_units'] == 64 and device['architecture'] == 'gfx1201'
    assert device['max_sm_clock_mhz'] == 2400.
    # Absent, not zero: a zero draw would read as an idle device.
    assert device['power_w'] is None and device['power_reference_w'] is None
    assert device['sm_clock_mhz'] is None and device['limited'] is None
    assert device['complete'] is True


def test_the_first_global_pool_belongs_to_the_cpu_and_is_not_vram(monkeypatch, tmp_path):
    amd(monkeypatch, tmp_path)
    device = gc.devices()[0][0]

    # 28737164 KB is host RAM on the CPU agent. A reader keyed on pool order
    # rather than on ``Device Type: GPU`` over-reports this card by 1.7x and
    # would admit work that cannot fit in it.
    assert device['memory_total_bytes'] == VRAM_BYTES
    assert device['memory_total_bytes'] != 28737164 * 1024


@pytest.mark.parametrize('field,value', [
    ('memory_total_bytes', VRAM_BYTES // 2), ('warp_size', 64),
    ('clock_rate_khz', 1800000), ('max_threads_per_block', 256)])
def test_disagreeing_readers_publish_nothing(monkeypatch, tmp_path, field, value):
    # Every one of these is visible to both readers. Disagreement means one of
    # them is describing another device, or an attribute was renumbered under
    # the constant this reads, and neither can be published from.
    amd(monkeypatch, tmp_path, probe={**HIP_REPORT, field: value})
    devices, errors = gc.devices()

    assert devices == []
    assert any('disagree on ' + field in error for error in errors)


@pytest.mark.parametrize('integrated,domain', [(0, 'discrete'), (1, 'shared_system')])
def test_the_runtime_states_the_memory_domain_rather_than_the_name(monkeypatch, tmp_path,
                                                                   integrated, domain):
    amd(monkeypatch, tmp_path, probe={**HIP_REPORT, 'integrated': integrated})
    assert gc.devices()[0][0]['memory_domain'] == domain


def test_an_unreadable_integration_attribute_is_incomplete(monkeypatch, tmp_path):
    amd(monkeypatch, tmp_path, probe={**HIP_REPORT, 'integrated': -1})
    device, errors = gc.devices()

    assert device[0]['memory_domain'] == 'unknown'
    assert device[0]['complete'] is False and errors


def test_more_than_one_amd_gpu_agent_refuses(monkeypatch, tmp_path):
    doubled = ROCMINFO_REPORT + ROCMINFO_REPORT[ROCMINFO_REPORT.index('Agent 2'):].replace(
        'GPU-9c30c352a59e5b7a', 'GPU-second')
    amd(monkeypatch, tmp_path, report=doubled)
    devices, errors = gc.devices()

    assert devices == []
    assert any('expected exactly one' in error for error in errors)


def test_a_failed_amd_memory_query_publishes_nothing(monkeypatch, tmp_path):
    amd(monkeypatch, tmp_path, error='AMD memory query unavailable: FileNotFoundError')
    devices, errors = gc.devices()

    assert devices == []
    assert 'AMD memory query unavailable: FileNotFoundError' in errors


def test_a_host_without_rocminfo_never_launches_the_amd_reader(monkeypatch, tmp_path):
    monkeypatch.setattr(gc, 'ROCMINFO', str(tmp_path / 'absent'))
    calls = []
    monkeypatch.setattr(gc, '_run_rocminfo', lambda timeout_s: calls.append(1) or ('', None))

    def unavailable(argv, **kwargs):
        raise FileNotFoundError(2, 'No such file or directory')

    monkeypatch.setattr(gc.subprocess, 'run', unavailable)
    devices, errors = gc.devices()

    assert devices == [] and not calls
    assert 'GPU device query unavailable: FileNotFoundError' in errors
