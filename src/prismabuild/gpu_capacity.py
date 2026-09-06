"""Root-published GPU admission evidence, without host-specific slot ceilings.

GB10's driver reports GPU power but no programmable power limit. Its published
140 W SoC TDP includes CPU power, so that reference is explicitly identified
and must not be mistaken for a measured GPU saturation point. Admission probes
combine this evidence with attributed residency, memory and host pressure.
"""
from __future__ import annotations

import math
from pathlib import Path
import re
import socket
import subprocess
import time
import uuid

SCHEMA = 'prismabuild.gpu_capacity.v1'
GB10_POWER_SOURCE = 'https://docs.nvidia.com/dgx/dgx-spark/hardware.html'
THROTTLE_FIELDS = ('gpu_idle', 'sw_power_cap', 'hw_slowdown',
                   'hw_thermal_slowdown', 'hw_power_brake_slowdown',
                   'sw_thermal_slowdown', 'sync_boost')
FIELDS = ('name', 'uuid', 'power.draw', 'enforced.power.limit', 'power.limit',
          'clocks.current.sm', 'clocks.max.sm', 'memory.total', 'memory.free',
          'memory.used', 'clocks_event_reasons.active',
          *(f'clocks_event_reasons.{name}' for name in THROTTLE_FIELDS))
MIB = 1024**2
DISCRETE_NAME = re.compile(r'^(?:NVIDIA )?(?:GeForce|RTX|Quadro|Tesla|TITAN|A100|A30|A40|A10|H100|H200|B100|B200|L4|L40|V100|T4)(?:[ -]|$)')


def _number(value, *, positive=False):
    try:
        result = float(value)
        if not math.isfinite(result) or result < 0 or positive and result <= 0:
            return None
        return result
    except (TypeError, ValueError):
        return None


def _flag(value):
    return {'Active': True, 'Not Active': False}.get(value)


def devices(*, timeout_s=1.0):
    """Read device-level power/clock/memory counters with one bounded query."""
    if isinstance(timeout_s, bool) or not math.isfinite(timeout_s) or not 0 < timeout_s <= 5:
        raise ValueError('GPU telemetry timeout must be in (0, 5] seconds')
    sampled_unix = time.time()
    try:
        result = subprocess.run(['/usr/bin/nvidia-smi', '--query-gpu=' + ','.join(FIELDS),
                                 '--format=csv,noheader,nounits'],
                                capture_output=True, text=True, timeout=timeout_s, check=False)
        if result.returncode:
            return [], [f'GPU device query exited {result.returncode}']
    except (OSError, subprocess.SubprocessError) as exc:
        return [], [f'GPU device query unavailable: {type(exc).__name__}']
    found, errors, identities = [], [], set()
    for line in result.stdout.splitlines():
        cells = [part.strip() for part in line.split(',')]
        if len(cells) != len(FIELDS):
            return [], ['malformed GPU device query']
        row = dict(zip(FIELDS, cells))
        identity = row['uuid']
        if not identity.startswith('GPU-') or identity in identities:
            return [], ['missing or duplicate GPU identity']
        identities.add(identity)
        name = row['name']
        limit = (_number(row['enforced.power.limit'], positive=True)
                 or _number(row['power.limit'], positive=True))
        reference = limit
        reference_scope = 'gpu_power_limit' if limit is not None else None
        reference_source = 'nvidia-smi:enforced.power.limit,power.limit' if limit else None
        total, free, used = [_number(row['memory.' + key]) for key in ('total', 'free', 'used')]
        if name == 'NVIDIA GB10':
            domain = 'shared_system'
            if reference is None:
                reference, reference_scope, reference_source = 140., 'soc_tdp', GB10_POWER_SOURCE
        elif (DISCRETE_NAME.match(name) and total is not None and total > 0 and free is not None and used is not None
              and free <= total and used <= total and free + used <= total + 1):
            # These are the driver's reportable framebuffer counters. Memory
            # reservation still remains separate from the host cgroup budget.
            domain = 'discrete'
        else:
            domain = 'unknown'
        throttle = {key: _flag(row['clocks_event_reasons.' + key]) for key in THROTTLE_FIELDS}
        try:
            mask = int(row['clocks_event_reasons.active'], 16)
        except ValueError:
            mask = None
        device = {
            'uuid': identity, 'name': name, 'sampled_unix': sampled_unix,
            'power_w': _number(row['power.draw']),
            'power_limit_w': limit, 'power_reference_w': reference, 'power_measurement_scope': 'gpu',
            'power_reference_scope': reference_scope, 'power_reference_source': reference_source,
            'sm_clock_mhz': _number(row['clocks.current.sm']),
            'max_sm_clock_mhz': _number(row['clocks.max.sm'], positive=True),
            'memory_domain': domain,
            'memory_total_bytes': int(total * MIB) if total is not None else None,
            'memory_free_bytes': int(free * MIB) if free is not None else None,
            'memory_used_bytes': int(used * MIB) if used is not None else None,
            'throttle_active_mask': mask, 'throttle_reasons': throttle,
            'limited': (any(throttle[key] for key in THROTTLE_FIELDS if key != 'gpu_idle')
                        if all(value is not None for value in throttle.values()) else None),
        }
        device['complete'] = (device['power_w'] is not None and reference is not None
                              and domain != 'unknown' and device['limited'] is not None
                              and device['sm_clock_mhz'] is not None
                              and device['max_sm_clock_mhz'] is not None)
        if not device['complete']:
            errors.append(f'incomplete GPU capacity counters: {identity}')
        found.append(device)
    if not found:
        errors.append('no GPU devices reported')
    return found, errors


def _cpu_pressure(proc_root):
    try:
        values = {}
        for line in (proc_root / 'pressure/cpu').read_text().splitlines():
            key, *fields = line.split()
            row = dict(field.split('=', 1) for field in fields)
            value = _number(row.get('avg10'))
            if value is None or value > 100:
                raise ValueError('invalid CPU pressure')
            values[key] = value
        return values['some'], values['full']
    except (OSError, ValueError, KeyError):
        return None, None


def collect(snapshot, records, *, timeout_s=1.0, proc_root=Path('/proc'), device_readings=None):
    """Join one exact GPU-memory census with broker authority and device power.

    ``snapshot`` is gpu_memory.Snapshot.as_dict(); ``records`` contains only
    active, kernel-verified broker attempts. No caller environment or queue
    file supplies these identities. Returned data contains no authority tokens.
    All pressure values are Linux PSI avg10 percentages, in [0, 100].
    """
    started = time.time()
    hardware, errors = device_readings if device_readings is not None else devices(timeout_s=timeout_s)
    errors = list(errors)
    cpu_some, cpu_full = _cpu_pressure(Path(proc_root))
    if cpu_some is None or cpu_full is None:
        errors.append('CPU pressure unavailable')
    jobs, attributed = [], bool(snapshot.get('gpu_query_complete'))
    seen = set()
    for job in snapshot.get('jobs', ()):
        scope = job.get('scope_id')
        record = records.get(scope)
        if (scope in seen or not record
                or list(record.get('cgroup_identity') or []) != list(job.get('cgroup_identity') or [])
                or not re.fullmatch('[0-9a-f]{64}', str(record.get('action_key', '')))
                or not re.fullmatch('[0-9a-f]{32}', str(record.get('nonce', '')))):
            attributed = False
            errors.append(f'GPU scope has no matching current broker attempt: {scope}')
            continue
        seen.add(scope)
        # Explicit allowlist: the broker record also holds a stop token which
        # must never reach the world-readable host admission evidence.
        public = {key: job.get(key) for key in (
            'scope_id', 'cgroup_identity', 'budget_bytes', 'host_bytes',
            'gpu_reported_bytes', 'gpu_lower_bound_bytes', 'lower_bound_bytes',
            'upper_bound_bytes', 'gpu_budget_bytes', 'memory_domain',
            'system_lower_bound_bytes', 'complete', 'processes', 'errors')}
        public.update(action_key=record['action_key'], nonce=record['nonce'],
                      gpu_budget_bytes=record.get('gpu_memory_max_bytes', record['memory_max_bytes']),
                      sampled_unix=snapshot.get('sampled_unix'))
        if not job.get('complete') or record.get('memory_max_bytes') != job.get('budget_bytes'):
            attributed = False
        jobs.append(public)
    if seen != set(records):
        attributed = False
        errors.append('GPU snapshot does not cover every active broker scope')
    foreign = snapshot.get('foreign_processes')
    if not isinstance(foreign, (list, tuple)):
        # Older gpu_memory did not preserve foreign PIDs. Zero reported bytes
        # then did not prove the absence of a foreign GPU context.
        foreign = []
        attributed = False
        errors.append('foreign GPU process inventory unavailable')
    errors.extend(snapshot.get('errors') or ())
    timestamp = snapshot.get('sampled_unix')
    if _number(timestamp, positive=True) is None or not 0 <= started - timestamp <= 5:
        attributed = False
        errors.append('GPU memory snapshot is stale or has a future clock')
    host_fields = {
        'host_total_bytes': snapshot.get('host_total_bytes'),
        'host_available_bytes': snapshot.get('host_available_bytes'),
        'memory_pressure_some': snapshot.get('psi_some_avg10'),
        'memory_pressure_full': snapshot.get('psi_full_avg10'),
        'cpu_pressure_some': cpu_some, 'cpu_pressure_full': cpu_full,
    }
    pressure_ok = all(_number(host_fields[key]) is not None and host_fields[key] <= 100
                      for key in ('memory_pressure_some', 'memory_pressure_full',
                                  'cpu_pressure_some', 'cpu_pressure_full'))
    host_ok = (_number(host_fields['host_total_bytes'], positive=True) is not None
               and _number(host_fields['host_available_bytes']) is not None
               and host_fields['host_available_bytes'] <= host_fields['host_total_bytes'])
    return {
        'schema': SCHEMA, 'host': socket.gethostname(), 'sample_id': uuid.uuid4().hex,
        'sampled_unix': min([timestamp] + [device['sampled_unix'] for device in hardware])
                        if _number(timestamp, positive=True) is not None else timestamp,
        'published_unix': time.time(),
        'complete': bool(hardware) and all(d['complete'] for d in hardware)
                    and attributed and pressure_ok and host_ok and not errors,
        'attributed': attributed, 'devices': hardware, **host_fields,
        'foreign_processes': list(foreign), 'foreign_process_count': len(foreign),
        'jobs': jobs, 'errors': errors,
    }
