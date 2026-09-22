"""Probe GPU concurrency from broker evidence, never discount memory budgets.

The single physical GPU is shared by generation actions, independently of old
slot counts. Host admission's existing lock serializes decisions and metadata;
no process is stopped when load rises. Fresh evidence permits one bounded cold
start; missing evidence refuses admission. Measurements, explicit exclusivity and ambiguous legacy requests stay
exclusive. This controller currently qualifies single-device workers only.

Every power ratio here divides a GPU-only reading by a GPU-only reference. A
device that publishes no programmable power limit gets that reference from
``admission_power_reference``: the highest draw this host has sampled from it,
floored by a declared per-device capacity fact. The vendor SoC envelope stays
in the sample for display and provenance and is never a denominator.
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


#: ``gpu_capacity.TELEMETRY_MEMORY_ONLY``. Spelled here rather than imported so
#: the controller reads one published sample field and never a producer module.
MEMORY_ONLY_TELEMETRY = 'memory_only'

#: Narrow first-job exception for an idle GB10 held at its SW power cap.
#: A GB10 at 4 W idle reports ``sw_power_cap Active`` (mask 0x4) with no
#: thermal/HW flag, while ``gpu_idle`` (mask 0x1) is the distinct "nothing
#: running" reason per the NVML clocks-event-reasons reference and the Sep-20
#: Sparklina captures (mask 0x4 ⇔ sw_power_cap, mask 0x0 ⇔ neither).  The
#: exception admits one first generation job under a hardware-enforced SW cap
#: without loosening any resource budget; sharing/measurement stay closed.
SW_CAP_IDLE_CLOCK_FRACTION = 0.10
SW_CAP_IDLE_POWER_FRACTION = 0.65
SW_CAP_IDLE_DEVICE_NAME = 'NVIDIA GB10'
SW_CAP_IDLE_BIT = 0x4
SW_CAP_GPU_IDLE_BIT = 0x1
#: Every limiter that counts toward ``gpu_capacity.limited`` except the SW cap
#: itself.  All must read False for the exception; ``gpu_idle`` is not a
#: limiter and may read either way (recorded, not gated).
SW_CAP_OTHER_LIMITERS = ('hw_slowdown', 'hw_thermal_slowdown',
                         'hw_power_brake_slowdown', 'sw_thermal_slowdown',
                         'sync_boost')

#: Declared GPU-only power capacity per device model, in watts.  It is the
#: admission reference floor for a device whose driver publishes no
#: programmable power limit, and it is a declared capacity fact: one value per
#: device name, owned by the fleet owner, changed on this one line.  It is not
#: a published vendor envelope and it is deliberately not derived from one:
#: GB10's 140 W figure is the whole-SoC TDP and includes CPU power, so it
#: cannot serve as the denominator of a GPU-only reading.  A device model with
#: no entry here and no driver limit has no admission reference at all, and its
#: sample stays invalid, which refuses.
DECLARED_GPU_POWER_REFERENCE_W = {'NVIDIA GB10': 100.0}

#: Where the declared numbers come from, carried into every decision record so
#: a reader can tell which reference a refusal was made against.
DECLARED_GPU_POWER_REFERENCE_SOURCE = (
    'declared fleet capacity fact (PrismaBuild #806): the fleet owner\'s '
    'observed GB10 GPU ceiling, 100 W (2026-09-21). A floor only: Netdata '
    'nvidia_smi.gpu_power_draw maxima over 2026-08-28..2026-09-21 were '
    'sparky 114 W and sparklina 106 W, and a measured peak above the floor '
    'replaces it')
MEASURED_GPU_POWER_REFERENCE_SOURCE = (
    'highest GPU-only power.draw this host has sampled from this device, '
    "ratcheted in the host-local gpu-state.json 'power_peaks' record")


def admission_power_reference(device, state):
    """Return ``(watts, scope, source)`` for the reference admission divides by.

    The driver's own programmable power limit is admission grade and is used
    unchanged.  GB10 publishes no such limit, and the 140 W figure it publishes
    instead is the SoC TDP: a whole-module number that includes CPU power,
    while ``power_w`` is a GPU-only reading.  Dividing one by the other is not
    a saturation fraction, so that scope is kept for display and provenance and
    is never the denominator here.  In its place admission reads the highest
    GPU-only draw this host has actually sampled from this device, floored by
    the declared capacity fact above so an idle history cannot authorize
    anything.  ``(None, None, None)`` means there is no reference, which leaves
    the sample invalid and refuses.
    """
    limit = device.get('power_limit_w')
    if _number(limit) and limit:
        return float(limit), 'gpu_power_limit', device.get('power_reference_source')
    if device.get('power_reference_scope') != 'soc_tdp':
        return None, None, None
    declared = DECLARED_GPU_POWER_REFERENCE_W.get(device.get('name'))
    if not _number(declared) or not declared:
        return None, None, None
    peak = (state.get('power_peaks') or {}).get(device.get('uuid'))
    if _number(peak) and peak > declared:
        return float(peak), 'measured_peak', MEASURED_GPU_POWER_REFERENCE_SOURCE
    return float(declared), 'declared_fallback', DECLARED_GPU_POWER_REFERENCE_SOURCE


def reporting_power_reference(device, state):
    """Return ``(watts, scope, source)`` for a reference a *reader* divides by.

    Receipts, metrics and the cross-resource placement proxy publish the same
    GPU-only ``power_w`` admission reads, so they divide it by the same
    reference: this defers to ``admission_power_reference`` and adds nothing
    to it.  One number, one meaning -- a receipt and a refusal on the same box
    at the same moment cannot disagree about what full looks like.

    Reporting differs from admission in one direction only.  Admission refuses
    a device it has no GPU-only reference for, because admitting on an unknown
    ceiling is the decision that costs something; a reader has nothing to
    refuse, so a device that publishes only its vendor SoC envelope is still
    described -- under the ``soc_tdp`` scope, which says the denominator
    covers CPU power the numerator does not.  That is a labelled fallback a
    reader can discount, and every caller here carries the scope beside the
    ratio so it can be.  The scope allow-list is admission's: a reference
    nobody scoped is still no reference at all.
    """
    watts, scope, source = admission_power_reference(device, state)
    if _number(watts) and watts > 0:
        return watts, scope, source
    if device.get('power_reference_scope') != 'soc_tdp':
        return None, None, None
    published = device.get('power_reference_w')
    if not _number(published) or not published:
        return None, None, None
    return float(published), 'soc_tdp', device.get('power_reference_source')


def host_local_power_state(ledger_base):
    """Admission's own state, for a reader that wants only its sampled peaks.

    The measured reference lives in one place -- the host-local
    ``gpu-state.json`` admission ratchets under its own lock -- so a receipt
    or a placement reading reads that record rather than keeping a second one
    that could drift from it.  Total by construction: no ledger, no base, an
    unreadable directory or a torn file all mean "no peaks recorded here",
    which leaves ``reporting_power_reference`` on the declared floor.  That is
    still a GPU-only, labelled reference, so a reader never falls back to the
    SoC envelope because a file was missing.  Reporting is the caller here and
    reporting has nothing to refuse, so this answers rather than raises.
    """
    if not ledger_base:
        return {}
    try:
        return adaptive_cpu.read_json(
            adaptive_cpu.local_state_base(ledger_base) / 'gpu-state.json')
    except Exception:                                          # noqa: BLE001
        return {}


def record_power_peak(state, device):
    """Ratchet the highest GPU-only draw sampled from this device.

    The published SoC envelope is a legitimate ceiling on a GPU-only reading,
    so a sample above it is refused rather than ratcheted: one implausible
    ``power.draw`` must not raise the admission reference permanently.  The
    ratchet has no decay window; whether it should acquire one is an owner
    decision and not invented here.  Recorded after the decision it could
    otherwise have influenced, so a sample never widens its own headroom.
    """
    identity, power = device.get('uuid'), device.get('power_w')
    if not isinstance(identity, str) or not identity or not _number(power):
        return
    ceiling = device.get('power_reference_w')
    if _number(ceiling) and ceiling and power > ceiling:
        return
    peaks = state.get('power_peaks')
    if not isinstance(peaks, dict):
        peaks = state['power_peaks'] = {}
    if not _number(peaks.get(identity)) or power > peaks[identity]:
        peaks[identity] = float(power)


def sw_cap_idle_first_job(sample, device, reference, *, holders, measurement,
                          pressure, reference_scope=None, reference_source=None):
    """Whether an idle SW-capped GB10 may admit its first generation job.

    Returns ``(eligible, diagnosis)``.  ``diagnosis`` always carries the
    threshold and the observations the decision was made on, so both the
    admit and the refuse paths record them.  Any missing clock, missing
    limiter breakdown, unknown mask bit, measurement request, holder,
    broker job, foreign process, pressure, or non-GB10/non-SoC telemetry
    denies the exception (returns False) and the caller keeps the existing
    ``host_or_device_congested`` refusal.
    """
    diagnosis = {
        'clock_threshold_fraction': SW_CAP_IDLE_CLOCK_FRACTION,
        'power_gate_fraction': SW_CAP_IDLE_POWER_FRACTION,
        'device_name': device.get('uuid') and device.get('name'),
        'memory_domain': device.get('memory_domain'),
        'power_reference_scope': device.get('power_reference_scope'),
        'power_w': device.get('power_w'),
        'power_reference_w': reference,
        'admission_reference_scope': reference_scope,
        'admission_reference_source': reference_source,
        'sm_clock_mhz': device.get('sm_clock_mhz'),
        'max_sm_clock_mhz': device.get('max_sm_clock_mhz'),
        'throttle_reasons': device.get('throttle_reasons'),
        'throttle_active_mask': device.get('throttle_active_mask'),
        'limited': device.get('limited'),
        'holders': len(holders),
        'broker_jobs': len(sample.get('jobs') or []),
        'foreign_processes': list(sample.get('foreign_processes') or []),
        'measurement': bool(measurement),
        'pressure': bool(pressure),
    }

    def deny(reason):
        diagnosis['exception_reason'] = reason
        return False, diagnosis

    if device.get('name') != SW_CAP_IDLE_DEVICE_NAME:
        return deny('not_gb10')
    if device.get('memory_domain') != 'shared_system':
        return deny('not_shared_system')
    if device.get('power_reference_scope') != 'soc_tdp':
        return deny('not_soc_tdp')
    if measurement:
        return deny('measurement_never_excepted')
    if holders:
        return deny('holders_present_no_sharing_exception')
    if list(sample.get('jobs') or []):
        return deny('broker_jobs_present')
    if list(sample.get('foreign_processes') or []):
        return deny('foreign_processes_present')
    if pressure:
        return deny('host_pressure')
    if device.get('limited') is not True:
        return deny('not_limited')
    power = device.get('power_w')
    if not _number(power) or not _number(reference) or not reference:
        return deny('power_or_reference_unknown')
    diagnosis['power_ratio'] = power / reference
    if power > SW_CAP_IDLE_POWER_FRACTION * reference:
        return deny('power_above_idle_gate')
    sm = device.get('sm_clock_mhz')
    max_sm = device.get('max_sm_clock_mhz')
    if not _number(sm) or not _number(max_sm) or not max_sm:
        return deny('clock_unknown')
    diagnosis['clock_ratio'] = sm / max_sm
    if sm > SW_CAP_IDLE_CLOCK_FRACTION * max_sm:
        return deny('clock_above_idle_threshold')
    reasons = device.get('throttle_reasons')
    if not isinstance(reasons, dict):
        return deny('throttle_reasons_missing')
    if reasons.get('sw_power_cap') is not True:
        return deny('sw_power_cap_not_active')
    for key in SW_CAP_OTHER_LIMITERS:
        if reasons.get(key) is not False:
            return deny(f'other_limiter_not_false:{key}')
    if not isinstance(reasons.get('gpu_idle'), bool):
        return deny('gpu_idle_unknown')
    for key, value in reasons.items():
        if key in ('gpu_idle', 'sw_power_cap'):
            continue
        if value is True:
            return deny(f'unexpected_limiter_active:{key}')
    mask = device.get('throttle_active_mask')
    if not isinstance(mask, int) or isinstance(mask, bool):
        return deny('mask_unknown')
    diagnosis['mask'] = mask
    if not mask & SW_CAP_IDLE_BIT:
        return deny('sw_cap_bit_not_set')
    if mask & ~(SW_CAP_IDLE_BIT | SW_CAP_GPU_IDLE_BIT):
        return deny('unknown_mask_bits')
    if bool(mask & SW_CAP_GPU_IDLE_BIT) != bool(reasons.get('gpu_idle')):
        return deny('mask_idle_bit_mismatches_reason')
    diagnosis['exception_reason'] = 'sw_cap_idle_first_job'
    return True, diagnosis


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
        self.last_decision = {"reason": "not_evaluated"}
        if not demand.get('gpu'):
            self.last_decision = {"reason": "no_gpu_demand"}
            return {}
        now = time.time()
        if self._sample is None:
            self._sample = self.sample()
        sample = self._sample
        def refuse(reason, **values):
            # Saved by the claimant after admission unlocks; this is the exact
            # broker sample already used here, never a diagnostic resample.
            self.last_decision = {"reason": reason, "sample": sample, **values}
            return None
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
            return refuse("max_actions", holders=len(holders))
        # Unknown/private/legacy reservations remain exclusive until released.
        if holders and (exclusive or measurement or any(not m or m.get('exclusive')
                                                         or m.get('measurement') for _, m in holders)):
            return refuse("exclusive_holder", holders=len(holders), exclusive=exclusive,
                          measurement=measurement)
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
        # Probe state is read before the reference because the reference is
        # derived from it: the published SoC envelope is display provenance,
        # and admission divides by the measured peak or the declared floor.
        # Low ratio is a probe permission, never saturation certification;
        # CPU pressure and attribution remain independent.
        state = adaptive_cpu.read_json(self.base / 'gpu-state.json')
        reference, reference_scope, reference_source = admission_power_reference(device, state)
        # A device that declares it has no saturation instrument is admitted on
        # the evidence it does carry -- identity, memory domain, free VRAM,
        # foreign holders and host pressure -- and is never granted the two
        # permissions that instrument exists to authorize. Absent power without
        # that declaration stays invalid, so this narrows one declared class of
        # device rather than weakening the contract for every sample.
        memory_only = device.get('telemetry_class') == MEMORY_ONLY_TELEMETRY
        valid = (valid and isinstance(device.get('uuid'), str) and bool(device['uuid'])
                 and device.get('memory_domain') in ('shared_system', 'discrete')
                 and (memory_only or (_number(device.get('power_w')) and _number(reference)
                                      and reference > 0)))
        low = False
        feedback_allowed = False
        sw_cap_exception = None
        members = sorted(f"{holder.name}:{meta.get('admitted_unix')}" for holder, meta in holders)
        if valid:
            reserve = max(2 * GIB, .02 * sample['host_total_bytes'])
            pressure = (sample['memory_pressure_some'] >= 1.
                        or sample['memory_pressure_full'] >= .1
                        or sample['cpu_pressure_some'] >= 10.
                        or sample['host_available_bytes'] < reserve + demand.get('mem_gb', 0) * GIB)
            # ``gpu_idle`` (clocks dropping because nothing runs) is normal.
            # Software power cap, thermal, power-brake, HW slowdown, SW thermal
            # and sync-boost slowdown are congestion even if the sampled power
            # has fallen.  Observed Sep-20 on Sparklina: mask 0x4 tracks
            # ``sw_power_cap Active`` with ``gpu_idle Not Active`` at 4.3 W idle,
            # per https://docs.nvidia.com/deploy/nvml-api/api/group__nvmlClocksEventReasons.html
            # (GpuIdle = nothing running; SwPowerCap = clocks optimized not to
            # exceed power limits).
            limited = device.get('limited')
            if memory_only:
                # Without a power series there is no observable plateau, so
                # ``low`` stays false: no sharing probe and no measurement
                # action, and the device runs one attributed job at a time.
                congested = pressure or bool(sample['foreign_processes'])
                low = False
            else:
                valid = valid and type(limited) is bool
                congested = (pressure or bool(sample['foreign_processes'])
                             or device['power_w'] >= .80 * reference or limited is True)
                low = valid and not congested and device['power_w'] <= .65 * reference
                feedback_allowed = observe_feedback(state, sample, members)
                record_power_peak(state, device)
            if sample['sample_id'] != state.get('sample_id'):
                continuous = 0 < sample['sampled_unix'] - state.get('sampled_unix', 0) <= MAX_SAMPLE_AGE_S
                state.update(sample_id=sample['sample_id'], sampled_unix=sample['sampled_unix'],
                             low_samples=min(3, state.get('low_samples', 0) + 1) if low and continuous else int(low))
            self._write_state(state)
            if congested and not memory_only:
                eligible, sw_cap_exception = sw_cap_idle_first_job(
                    sample, device, reference, holders=holders,
                    measurement=measurement, pressure=pressure,
                    reference_scope=reference_scope, reference_source=reference_source)
                if eligible:
                    # First generation job only: the SW cap is a clock policy
                    # at idle power, not saturation evidence.  ``low`` stays
                    # False so measurement still refuses and sharing probes
                    # still need genuinely free samples; budgets, pressure,
                    # foreign and thermal/HW gates above are unchanged.
                    congested = False
                elif sw_cap_exception is None:
                    sw_cap_exception = {'exception_reason': 'not_evaluated'}
            if congested:
                return refuse("host_or_device_congested", pressure=pressure,
                              foreign_processes=sample['foreign_processes'],
                              power_w=device.get('power_w'), power_reference_w=reference,
                              power_reference_scope=reference_scope,
                              power_reference_source=reference_source,
                              limited=limited,
                              sw_cap_idle_exception=sw_cap_exception)
            if device.get('memory_domain') == 'discrete':
                fields = ('memory_total_bytes', 'memory_free_bytes', 'memory_used_bytes')
                if not all(_number(device.get(k)) for k in fields):
                    return refuse("gpu_memory_sample_invalid", device=device)
                total = device['memory_total_bytes']
                used = sum(m.get('gpu_memory_budget_bytes', 0) for _, m in holders)
                if (not total or budget <= 0 or used + budget > total
                        or device['memory_free_bytes'] < budget
                        or device['memory_free_bytes'] + device['memory_used_bytes'] > total):
                    return refuse("gpu_memory_budget", memory_total_bytes=total,
                                  memory_free_bytes=device['memory_free_bytes'],
                                  requested_budget_bytes=budget, held_budget_bytes=used)
        if not valid:
            return refuse("sample_invalid_or_stale")
        if measurement and (not valid or not low or sample['foreign_processes']):
            return refuse("measurement_device_not_idle", low=low,
                          foreign_processes=sample['foreign_processes'])
        if holders:
            if (not valid or not low or not shape or not feedback_allowed or state.get('low_samples', 0) < 2
                    or state.get('consumed_sample_id') == sample['sample_id']
                    or sample['sampled_unix'] <= state.get('consumed_sampled_unix', 0)):
                return refuse("sharing_probe_not_authorized", low=low, shape=shape,
                              feedback_allowed=feedback_allowed,
                              low_samples=state.get('low_samples', 0),
                              consumed_sample_id=state.get('consumed_sample_id'))
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
                    return refuse("holder_telemetry_unavailable", holder=holder.name)
        self.last_decision = {"reason": "admitted", "sample": sample,
                                "power_reference_w": reference,
                                "power_reference_scope": reference_scope,
                                "power_reference_source": reference_source,
                                "sw_cap_idle_exception": sw_cap_exception}
        return {'declared_gpu': int(demand['gpu']), 'exclusive': exclusive,
                'action_key': str(item['action_key']), 'members_before': members,
                'measurement': measurement, 'shape': shape, 'admitted_unix': now,
                'probe': bool(holders), 'sample_id': sample.get('sample_id'),
                'sampled_unix': sample.get('sampled_unix'),
                'gpu_memory_budget_bytes': budget, 'device_uuid': device.get('uuid'),
                'memory_domain': device.get('memory_domain', 'unknown'),
                'sw_cap_idle_exception': sw_cap_exception}

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
