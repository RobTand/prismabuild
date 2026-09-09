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
import statistics
import time
import uuid

from . import adaptive_cpu

METADATA = '.gpu.json'
SAMPLE_PATH = Path('/run/prismabuild/gpu-capacity.json')
MAX_SAMPLE_AGE_S = 5.0
SETTLE_S = 2.0
MAX_ACTIONS = 256
FEEDBACK_SAMPLES = 3
FEEDBACK_WINDOW = 6
GIB = 1024 ** 3


def memory_budget_bytes(value):
    """Convert GiB to kernel-representable positive bytes without float overflow."""
    # Comparing before multiplication also handles huge ints, infinities and NaN.
    # The upper bound is exclusive: 2**33 GiB is one byte above signed int64.
    if type(value) not in (int, float) or not 0 < value < 2**33:
        raise ValueError('GPU budget must represent between 1 and 9223372036854775807 bytes')
    result = int(value * GIB)
    if not 0 < result <= 2**63 - 1:
        raise ValueError('GPU budget must represent between 1 and 9223372036854775807 bytes')
    return result


def _number(value):
    return type(value) in (int, float) and math.isfinite(value) and value >= 0


def _power_estimate(rows):
    values = [row['power_w'] for row in rows]
    mean = statistics.mean(values)
    error = statistics.stdev(values) / math.sqrt(len(values)) if len(values) > 1 else 0.
    return mean, error


def _separated_power(before, after):
    """Require response above observed sample noise and a relative deadband.

    This is a conservative activity proxy, not a throughput measurement or a
    model-specific power ceiling. The deadband prevents tiny power movements
    from authorizing endless probes on a saturated plateau.
    """
    old, old_error = _power_estimate(before)
    new, new_error = _power_estimate(after)
    margin = max(.03 * max(old, new), 2 * math.hypot(old_error, new_error))
    return new - old, margin


def observe_feedback(state, sample, members):
    """Update probe response under the host lock; return permission to probe.

    Saturation survives an individual holder's departure: remaining work may
    still sustain the same plateau, so concurrency can fall naturally. A
    sustained activity change or the end of the whole GPU busy period reopens
    exploration under the current headroom gates. No holder is killed,
    migrated or stripped of its reservation.
    """
    device = sample['devices'][0]
    if not members:
        state.pop('power_feedback', None)
        state['power_window'] = []
        state['power_members'] = []
        return True
    if state.get('power_members') != members:
        state['power_window'] = []
        state['power_members'] = members
    window = [row for row in state.get('power_window', [])
              if 0 <= sample['sampled_unix'] - row['sampled_unix'] <= MAX_SAMPLE_AGE_S]
    state['power_window'] = window
    row = {'sample_id': sample['sample_id'], 'sampled_unix': sample['sampled_unix'],
           'power_w': device['power_w'], 'sm_clock_mhz': device.get('sm_clock_mhz')}
    if not window or row['sampled_unix'] - window[-1]['sampled_unix'] >= 1.:
        window = (window + [row])[-FEEDBACK_WINDOW:]
        state['power_window'] = window
    feedback = state.get('power_feedback')
    if not feedback:
        return True
    if feedback['device_uuid'] != device['uuid']:
        # A device identity change invalidates the baseline, not its memory
        # reservation. Ordinary attribution and memory checks still apply.
        state.pop('power_feedback', None)
        return True
    if feedback['status'] == 'plateau':
        recent = [r for r in window if r['sampled_unix'] > feedback['evaluated_unix']]
        if len(recent) >= FEEDBACK_SAMPLES:
            difference, margin = _separated_power(feedback['observed'], recent)
            # A rise also invalidates the old phase: an idle startup plateau
            # cannot establish saturation for a later active workload.
            if abs(difference) > margin:
                state.pop('power_feedback', None)
                return True
        return False
    if members != feedback['expected_members']:
        # A failed acquisition or an exit before the response window completed
        # cannot support a controlled before/after inference.
        state.pop('power_feedback', None)
        return True
    recent = [r for r in window if r['sampled_unix'] >= feedback['admitted_unix'] + SETTLE_S]
    if len(recent) < FEEDBACK_SAMPLES:
        return False
    difference, margin = _separated_power(feedback['baseline'], recent)
    if abs(difference) > margin:
        # A sustained fall is a phase change, not evidence that adding work
        # reached a stable saturation plateau.
        state.pop('power_feedback', None)
        return True
    feedback.update(status='plateau', observed=recent,
                    evaluated_unix=sample['sampled_unix'], power_delta_w=difference,
                    uncertainty_margin_w=margin)
    state['power_feedback'] = feedback
    return False


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
        budget_bytes = memory_budget_bytes(budget)
        return shape, measurement, exclusive or int(demand.get('gpu', 0)) > 1, budget_bytes
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
    """Probe state is host-local authority, published as a diagnostic copy.

    ``gpu-state.json`` is read and rewritten on every decision under the host
    admission lock. It lives beside the CPU controller's state under
    ``PRISMABUILD_BOX_STATE_ROOT`` and reaches ``reservations/<host>/adaptive/``
    only through the same independent publisher, after admission is released,
    when a ``publisher`` (the CPU controller) is given. A shared copy is never
    read back: a cold host relearns ``low_samples`` from its own samples and
    holds no probe feedback, which refuses probes until fresh evidence exists.
    """

    def __init__(self, ledger, publisher=None):
        self.ledger = ledger
        self.base = adaptive_cpu.local_state_base(ledger.base)
        self._publisher = publisher
        self._sample = None

    def _write_state(self, state):
        if self._publisher is not None:
            self._publisher.write_state('gpu-state.json', state)
        else:
            adaptive_cpu.write_json(self.base / 'gpu-state.json', state)

    def sample(self):
        return trusted_sample()

    def decision(self, item, demand, *, contract=None):
        """Decide under admission; callers may pre-read the sealed contract."""
        if not demand.get('gpu'):
            return {}
        now = time.time()
        if self._sample is None:
            self._sample = self.sample()
        sample = self._sample
        shape, measurement, exclusive, budget = (
            action_contract(item, demand) if contract is None else contract)
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
                 and device.get('memory_domain') in ('shared_system', 'discrete'))
        state = adaptive_cpu.read_json(self.base / 'gpu-state.json')
        low = False
        feedback_allowed = False
        members = sorted(f"{holder.name}:{meta.get('admitted_unix')}" for holder, meta in holders)
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
            feedback_allowed = observe_feedback(state, sample, members)
            if sample['sample_id'] != state.get('sample_id'):
                continuous = 0 < sample['sampled_unix'] - state.get('sampled_unix', 0) <= MAX_SAMPLE_AGE_S
                state.update(sample_id=sample['sample_id'], sampled_unix=sample['sampled_unix'],
                             low_samples=min(3, state.get('low_samples', 0) + 1) if low and continuous else int(low))
            self._write_state(state)
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
            if (not valid or not low or not shape or not feedback_allowed or state.get('low_samples', 0) < 2
                    or state.get('consumed_sample_id') == sample['sample_id']
                    or sample['sampled_unix'] <= state.get('consumed_sampled_unix', 0)):
                return None
            jobs = {job.get('action_key'): job for job in sample['jobs'] if isinstance(job, dict)}
            for holder, meta in holders:
                record = adaptive_cpu.read_json(self.base / 'telemetry' / f'{holder.name}.json')
                job = jobs.get(holder.name, {})
                if (not meta.get('shape') or meta.get('device_uuid') != device['uuid']
                        or record.get('complete') is not True
                        or job.get('complete') is not True
                        or job.get('nonce') != record.get('nonce') or not job.get('nonce')
                        or job.get('scope_id') != record.get('scope_unit')
                        or not _number(record.get('sampled_unix'))
                        or not 0 <= now - record['sampled_unix'] <= MAX_SAMPLE_AGE_S
                        or record['sampled_unix'] < meta['admitted_unix']
                        or sample['sampled_unix'] < meta['admitted_unix'] + SETTLE_S):
                    return None
        return {'declared_gpu': int(demand['gpu']), 'exclusive': exclusive,
                'action_key': str(item['action_key']), 'members_before': members,
                'measurement': measurement, 'shape': shape, 'admitted_unix': now,
                'probe': bool(holders), 'sample_id': sample.get('sample_id'),
                'sampled_unix': sample.get('sampled_unix'),
                'gpu_memory_budget_bytes': budget, 'device_uuid': device.get('uuid'),
                'memory_domain': device.get('memory_domain', 'unknown')}

    def reserve_probe(self, metadata):
        """Spend sample before claiming work; a crash can lose credit, never reuse it."""
        if metadata.get('probe'):
            state = adaptive_cpu.read_json(self.base / 'gpu-state.json')
            previous = {key: state[key] for key in (
                'consumed_sample_id', 'consumed_sampled_unix') if key in state}
            probe_id = uuid.uuid4().hex
            state['consumed_probe_id'] = probe_id
            state['consumed_sample_id'] = metadata['sample_id']
            state['consumed_sampled_unix'] = metadata['sampled_unix']
            state['power_feedback'] = {
                'status': 'pending', 'device_uuid': metadata['device_uuid'],
                'baseline': state['power_window'], 'admitted_unix': metadata['admitted_unix'],
                'expected_members': sorted(metadata['members_before'] + [
                    f"{metadata['action_key']}:{metadata['admitted_unix']}"])}
            self._write_state(state)
            return {'probe_id': probe_id, 'sample_id': metadata['sample_id'],
                    'sampled_unix': metadata['sampled_unix'], 'previous': previous,
                    'feedback': state['power_feedback']}

    def return_probe(self, ticket):
        """Return an unlaunched probe under admission exclusion, once, if still owned.

        Restore only consumption, preserving intervening observations. A newer
        probe owns its credit even when it consumed the very same sample.
        Callers use this only after ordinary abandonment, never uncertain errors
        or completion of launched work. Crash/restart has no return authority.
        """
        if not ticket:
            return
        probe_id = ticket.pop('probe_id', None)  # Retire authority before any I/O.
        if not probe_id:
            return
        state = adaptive_cpu.read_json(self.base / 'gpu-state.json')
        if (state.get('consumed_probe_id') != probe_id
                or state.get('consumed_sample_id') != ticket['sample_id']
                or state.get('consumed_sampled_unix') != ticket['sampled_unix']):
            return
        state.pop('consumed_probe_id')
        for key in ('consumed_sample_id', 'consumed_sampled_unix'):
            state.pop(key, None)
        state.update(ticket['previous'])
        # Decision may already have invalidated or updated the feedback. Never
        # resurrect its predecessor or overwrite a later power observation.
        if state.get('power_feedback') == ticket['feedback']:
            state.pop('power_feedback', None)
        self._write_state(state)
