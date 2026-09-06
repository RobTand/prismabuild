"""Probe GPU concurrency from broker evidence, never discount memory budgets.

The single physical GPU is shared by generation actions, independently of old
slot counts. Host admission's existing lock serializes decisions and metadata;
no process is stopped when load rises. Fresh evidence permits one bounded cold
start; missing evidence refuses admission. Measurements, explicit exclusivity and ambiguous legacy requests stay
exclusive. This controller currently qualifies single-device workers only.
"""
from __future__ import annotations

import json
import math
import os
from pathlib import Path
import stat
import time

from . import adaptive_cpu

METADATA = '.gpu.json'
SAMPLE_PATH = Path('/run/prismabuild/gpu-capacity.json')
MAX_SAMPLE_AGE_S = 5.0
SETTLE_S = 2.0
MAX_ACTIONS = 256
GIB = 1024 ** 3


def _number(value):
    return type(value) in (int, float) and math.isfinite(value) and value >= 0


def action_contract(item, demand):
    """Read identity and exclusivity from the sealed action, not queue hints."""
    from . import core
    shape, measurement = adaptive_cpu.action_identity(item)
    key = str(item['action_key'])
    raw = adaptive_cpu.read_json(Path(str(item['cas_root'])) / 'requests' / key[:2] / f'{key}.json')
    try:
        action = core.validate_action(raw)
        if action['action_key'] != key:
            raise ValueError('action key mismatch')
        params = action.get('params', {})
        # Old single-slot exclusive and ordinary requests are indistinguishable.
        # Explicit false is the new shared-generation contract.
        exclusive = params.get('gpu_exclusive') is not False
        budget = params.get('gpu_memory_gb', demand.get('mem_gb', 0))
        if not _number(budget) or budget <= 0:
            raise ValueError('invalid GPU memory budget')
        return shape, measurement, exclusive or int(demand.get('gpu', 0)) > 1, int(budget * GIB)
    except (TypeError, ValueError, KeyError):
        return None, measurement, True, int(demand.get('mem_gb', 0)) * GIB


def trusted_sample(path=SAMPLE_PATH):
    """Read only a root-owned, non-writable public broker snapshot."""
    try:
        path = Path(path)
        for parent in (path.parent, *path.parent.parents):
            info = parent.lstat()
            if not stat.S_ISDIR(info.st_mode) or info.st_uid != 0 or info.st_mode & 0o022:
                return {}
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        with os.fdopen(descriptor) as stream:
            info = os.fstat(stream.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_uid != 0 or info.st_mode & 0o022:
                return {}
            value = json.load(stream)
        return value if isinstance(value, dict) else {}
    except (OSError, ValueError):
        return {}


class Controller:
    def __init__(self, ledger):
        self.ledger = ledger
        self.base = ledger.base / 'adaptive'
        self._sample = None

    def sample(self):
        return trusted_sample()

    def decision(self, item, demand):
        """Return durable reservation evidence, or None to leave work queued."""
        if not demand.get('gpu'):
            return {}
        now = time.time()
        if self._sample is None:
            self._sample = self.sample()
        sample = self._sample
        shape, measurement, exclusive, budget = action_contract(item, demand)
        holders = []
        for holder in self.ledger.held_dir.iterdir():
            if not holder.is_dir():
                continue
            meta = adaptive_cpu.read_json(holder / METADATA)
            if meta or any(holder.glob('gpu-*')):
                holders.append((holder, meta))
        if len(holders) >= MAX_ACTIONS:
            return None
        # Unknown/private/legacy reservations remain exclusive until released.
        if holders and (exclusive or measurement or any(not m or m.get('exclusive')
                                                         or m.get('measurement') for _, m in holders)):
            return None
        fresh = (_number(sample.get('sampled_unix'))
                 and 0 <= now - sample['sampled_unix'] <= MAX_SAMPLE_AGE_S)
        devices = sample.get('devices', [])
        valid = (fresh and sample.get('schema') == 'prismabuild.gpu_capacity.v1'
                 and sample.get('complete') is True and sample.get('attributed') is True
                 and isinstance(sample.get('sample_id'), str) and bool(sample['sample_id'])
                 and isinstance(devices, list) and len(devices) == 1
                 and isinstance(devices[0], dict)
                 and isinstance(sample.get('foreign_processes'), list)
                 and isinstance(sample.get('jobs'), list))
        device = devices[0] if valid else {}
        host_fields = ('host_total_bytes', 'host_available_bytes', 'memory_pressure_some',
                       'memory_pressure_full', 'cpu_pressure_some')
        valid = (valid and all(_number(sample.get(k)) for k in host_fields)
                 and 0 < sample['host_total_bytes'] >= sample['host_available_bytes']
                 and all(sample[k] <= 100 for k in host_fields if 'pressure' in k))
        reference = device.get('power_limit_w')
        if not _number(reference) or not reference:
            # The GB10 reference is a SoC envelope, explicitly not a measured
            # GPU-only limit. Low ratio is a probe permission, never saturation
            # certification; CPU pressure and attribution remain independent.
            reference = (device.get('power_reference_w')
                         if device.get('power_reference_scope') == 'soc_tdp' else None)
        valid = (valid and _number(device.get('power_w')) and _number(reference)
                 and reference > 0 and isinstance(device.get('uuid'), str) and bool(device['uuid'])
                 and device.get('memory_domain') in ('shared_system', 'unified', 'discrete'))
        state = adaptive_cpu.read_json(self.base / 'gpu-state.json')
        low = False
        if valid:
            reserve = max(2 * GIB, .02 * sample['host_total_bytes'])
            pressure = (sample['memory_pressure_some'] >= 1.
                        or sample['memory_pressure_full'] >= .1
                        or sample['cpu_pressure_some'] >= 10.
                        or sample['host_available_bytes'] < reserve + demand.get('mem_gb', 0) * GIB)
            # Idle clock gating (0x4) is normal. Thermal, power and external
            # slowdown are congestion even if the sampled power has fallen.
            limited = device.get('limited')
            valid = valid and type(limited) is bool
            congested = (pressure or bool(sample['foreign_processes'])
                         or device['power_w'] >= .80 * reference or limited is True)
            low = valid and not congested and device['power_w'] <= .65 * reference
            if sample['sample_id'] != state.get('sample_id'):
                continuous = 0 < sample['sampled_unix'] - state.get('sampled_unix', 0) <= MAX_SAMPLE_AGE_S
                state.update(sample_id=sample['sample_id'], sampled_unix=sample['sampled_unix'],
                             low_samples=min(3, state.get('low_samples', 0) + 1) if low and continuous else int(low))
                adaptive_cpu.write_json(self.base / 'gpu-state.json', state)
            if congested:
                return None
            if device.get('memory_domain') == 'discrete':
                fields = ('memory_total_bytes', 'memory_free_bytes', 'memory_used_bytes')
                if not all(_number(device.get(k)) for k in fields):
                    return None
                total = device['memory_total_bytes']
                used = sum(m.get('gpu_memory_budget_bytes', 0) for _, m in holders)
                if (not total or budget <= 0 or used + budget > total
                        or device['memory_free_bytes'] < budget
                        or device['memory_free_bytes'] + device['memory_used_bytes'] > total):
                    return None
        if not valid:
            return None
        if measurement and (not valid or not low or sample['foreign_processes']):
            return None
        if holders:
            if (not valid or not low or not shape or state.get('low_samples', 0) < 2
                    or state.get('consumed_sample_id') == sample['sample_id']
                    or sample['sampled_unix'] <= state.get('consumed_sampled_unix', 0)):
                return None
            jobs = {job.get('action_key'): job for job in sample['jobs'] if isinstance(job, dict)}
            for holder, meta in holders:
                record = adaptive_cpu.read_json(self.ledger.base / 'telemetry' / f'{holder.name}.json')
                job = jobs.get(holder.name, {})
                if (not meta.get('shape') or record.get('complete') is not True
                        or job.get('complete') is not True
                        or job.get('nonce') != record.get('nonce') or not job.get('nonce')
                        or job.get('scope_id') != record.get('scope_unit')
                        or not _number(record.get('sampled_unix'))
                        or not 0 <= now - record['sampled_unix'] <= MAX_SAMPLE_AGE_S
                        or record['sampled_unix'] < meta['admitted_unix']
                        or sample['sampled_unix'] < meta['admitted_unix'] + SETTLE_S):
                    return None
        return {'declared_gpu': int(demand['gpu']), 'exclusive': exclusive,
                'measurement': measurement, 'shape': shape, 'admitted_unix': now,
                'probe': bool(holders), 'sample_id': sample.get('sample_id'),
                'sampled_unix': sample.get('sampled_unix'),
                'gpu_memory_budget_bytes': budget, 'device_uuid': device.get('uuid'),
                'memory_domain': device.get('memory_domain', 'unknown')}

    def reserve_probe(self, metadata):
        """Spend sample before mutation; a crash can lose credit, never reuse it."""
        if metadata.get('probe'):
            state = adaptive_cpu.read_json(self.base / 'gpu-state.json')
            state['consumed_sample_id'] = metadata['sample_id']
            state['consumed_sampled_unix'] = metadata['sampled_unix']
            adaptive_cpu.write_json(self.base / 'gpu-state.json', state)
