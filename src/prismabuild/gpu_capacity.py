"""Root-published GPU admission evidence, without host-specific slot ceilings.

GB10's driver reports GPU power but no programmable power limit. Its published
140 W SoC TDP includes CPU power, so that reference is explicitly identified
and must not be mistaken for a measured GPU saturation point. Admission probes
combine this evidence with attributed residency, memory and host pressure.
"""
from __future__ import annotations

import json
import math
import os
from pathlib import Path
import re
import socket
import subprocess
import sys
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

#: What a device record's counters actually cover. ``power_and_clocks`` is the
#: NVML contract: draw, a reference, current/max clocks and throttle reasons,
#: which is what authorizes the sharing probe. ``memory_only`` is a device whose
#: runtime publishes identity and memory and no saturation instrument at all;
#: it is a declaration in the sample, never an absent field a reader may guess
#: a value for, and the admission controller narrows what such a device may do.
TELEMETRY_POWER_AND_CLOCKS = 'power_and_clocks'
TELEMETRY_MEMORY_ONLY = 'memory_only'

NVIDIA_SMI = '/usr/bin/nvidia-smi'
ROCMINFO = '/usr/bin/rocminfo'
HIP_LIBRARY = '/opt/rocm/lib/libamdhip64.so'

#: ``hipDeviceAttribute_t`` values inside HIP's CUDA-compatible block, whose
#: ordering AMD fixes so the numbers match CUDA's. They are read out of the
#: installed header rather than guessed, and every one of them is cross-checked
#: against the same quantity in ``rocminfo`` before a device is published, so a
#: future renumbering fails the check and refuses instead of misreporting.
HIP_ATTRIBUTE_INTEGRATED = 16
HIP_ATTRIBUTE_WARP_SIZE = 87
HIP_ATTRIBUTE_CLOCK_RATE_KHZ = 5
HIP_ATTRIBUTE_MAX_THREADS_PER_BLOCK = 56

#: Runs in a short-lived subprocess. A HIP context opened inside the broker
#: would hold the device node open for the daemon's lifetime and then appear in
#: its own foreign-handle census.
HIP_PROBE_SOURCE = """
import ctypes, json, sys
try:
    lib = ctypes.CDLL(sys.argv[1])
    if lib.hipInit(0) != 0:
        raise OSError('hipInit failed')
    out = {}
    count = ctypes.c_int()
    if lib.hipGetDeviceCount(ctypes.byref(count)) != 0:
        raise OSError('hipGetDeviceCount failed')
    out['device_count'] = count.value
    free, total = ctypes.c_size_t(), ctypes.c_size_t()
    if lib.hipMemGetInfo(ctypes.byref(free), ctypes.byref(total)) != 0:
        raise OSError('hipMemGetInfo failed')
    out['memory_free_bytes'] = free.value
    out['memory_total_bytes'] = total.value
    for name, attribute in json.loads(sys.argv[2]).items():
        value = ctypes.c_int(-1)
        if lib.hipDeviceGetAttribute(ctypes.byref(value), ctypes.c_int(attribute),
                                     ctypes.c_int(0)) != 0:
            raise OSError('hipDeviceGetAttribute failed: ' + name)
        out[name] = value.value
    print(json.dumps(out))
except Exception as exc:
    print(json.dumps({'error': type(exc).__name__}))
"""

_ROCMINFO_AGENT = re.compile(r'Agent \d+\Z')
_ROCMINFO_FIELDS = frozenset((
    'Name', 'Uuid', 'Marketing Name', 'Device Type', 'Compute Unit',
    'Wavefront Size', 'Max Clock Freq. (MHz)', 'Workgroup Max Size'))


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


def _nvidia_devices(timeout_s):
    """Read device-level power/clock/memory counters with one bounded query."""
    sampled_unix = time.time()
    try:
        result = subprocess.run([NVIDIA_SMI, '--query-gpu=' + ','.join(FIELDS),
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
            'vendor': 'nvidia', 'telemetry_class': TELEMETRY_POWER_AND_CLOCKS,
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


def _parse_rocminfo(text):
    """Split one ``rocminfo`` report into agents; pools keep their segment."""
    agents, current, segment = [], None, None
    for raw in text.splitlines():
        line = raw.strip()
        if _ROCMINFO_AGENT.fullmatch(line):
            current, segment = {'pools': []}, None
            agents.append(current)
            continue
        if current is None or ':' not in line:
            continue
        key, _, value = line.partition(':')
        key, value = key.strip(), value.strip()
        if key == 'Segment':
            segment = value
        elif key == 'Size' and segment is not None:
            current['pools'].append((segment, value))
            segment = None
        elif key in _ROCMINFO_FIELDS:
            current.setdefault(key, value)
    return agents


def _leading_int(value):
    """``16577056(0xfcf220) KB`` and ``32(0x20)`` both carry one number."""
    match = re.match(r'\s*(\d+)', str(value))
    return int(match.group(1)) if match else None


def _trusted_reader(path):
    """Refuse a device reader anyone but root can rewrite.

    This module runs inside the root broker and both AMD readers are executed
    or loaded as root: ``rocminfo`` as a subprocess and ``libamdhip64.so``
    through ctypes. A packaged ROCm installs both root-owned; a writable one is
    a reader whose answers, and whose code, some other account chooses.
    """
    try:
        entry = os.stat(path)
    except OSError as exc:
        return f'AMD reader unavailable: {type(exc).__name__}'
    if entry.st_uid != 0 or entry.st_mode & 0o022:
        return f'AMD reader is writable outside root: {path}'
    return None


def _run_rocminfo(timeout_s):
    untrusted = _trusted_reader(ROCMINFO)
    if untrusted:
        return None, untrusted
    try:
        result = subprocess.run([ROCMINFO], capture_output=True, text=True,
                                timeout=timeout_s, check=False)
    except (OSError, subprocess.SubprocessError) as exc:
        return None, f'AMD device query unavailable: {type(exc).__name__}'
    if result.returncode:
        return None, f'AMD device query exited {result.returncode}'
    return result.stdout, None


def _run_hip_probe(timeout_s):
    """Ask the HIP runtime for device count, free/total VRAM and attributes."""
    attributes = {'integrated': HIP_ATTRIBUTE_INTEGRATED,
                  'warp_size': HIP_ATTRIBUTE_WARP_SIZE,
                  'clock_rate_khz': HIP_ATTRIBUTE_CLOCK_RATE_KHZ,
                  'max_threads_per_block': HIP_ATTRIBUTE_MAX_THREADS_PER_BLOCK}
    untrusted = _trusted_reader(HIP_LIBRARY)
    if untrusted:
        return None, untrusted
    try:
        result = subprocess.run([sys.executable, '-c', HIP_PROBE_SOURCE, HIP_LIBRARY,
                                 json.dumps(attributes)], capture_output=True,
                                text=True, timeout=timeout_s, check=False)
    except (OSError, subprocess.SubprocessError) as exc:
        return None, f'AMD memory query unavailable: {type(exc).__name__}'
    if result.returncode:
        return None, f'AMD memory query exited {result.returncode}'
    try:
        payload = json.loads(result.stdout)
    except ValueError:
        return None, 'malformed AMD memory query'
    if not isinstance(payload, dict) or payload.get('error'):
        return None, f"AMD memory query failed: {(payload or {}).get('error')}"
    return payload, None


def _amd_devices(timeout_s):
    """Publish one AMD GPU from two independent runtime sources that agree.

    ``rocminfo`` answers the HSA agent question -- identity, architecture, CU
    count, wavefront, peak clock and the VRAM pool -- and the HIP runtime
    answers the live memory question the agent report cannot. Four quantities
    are visible to both, and all four must match before anything is published:
    disagreement means one of the two readers is describing a different device
    or a renumbered attribute, and neither is a thing to publish a capacity
    claim from. The VRAM total in particular is the landmine this refuses --
    the first ``GLOBAL`` pool in a ``rocminfo`` report belongs to the CPU agent
    and is host RAM, so a reader keyed on pool order over-reports VRAM.
    """
    sampled_unix = time.time()
    text, error = _run_rocminfo(timeout_s)
    if error:
        return [], [error]
    agents = [agent for agent in _parse_rocminfo(text)
              if agent.get('Device Type') == 'GPU']
    if len(agents) != 1:
        # One HIP ordinal is all the probe reads. Two GPU agents would need a
        # per-ordinal probe and a proven agent-to-ordinal mapping.
        return [], [f'AMD GPU agents reported: {len(agents)}, expected exactly one']
    agent = agents[0]
    identity = agent.get('Uuid', '')
    if not isinstance(identity, str) or not identity.startswith('GPU-'):
        return [], ['missing AMD GPU identity']
    vram = None
    for segment, size in agent['pools']:
        if segment.startswith('GLOBAL') and 'COARSE GRAINED' in segment:
            kilobytes = _leading_int(size)
            vram = kilobytes * 1024 if kilobytes else None
            break
    wavefront = _leading_int(agent.get('Wavefront Size'))
    clock_mhz = _leading_int(agent.get('Max Clock Freq. (MHz)'))
    workgroup = _leading_int(agent.get('Workgroup Max Size'))
    units = _leading_int(agent.get('Compute Unit'))
    if not all((vram, wavefront, clock_mhz, workgroup, units)):
        return [], ['incomplete AMD GPU agent report']
    probe, error = _run_hip_probe(timeout_s)
    if error:
        return [], [error]
    if probe.get('device_count') != 1:
        return [], [f"AMD HIP devices reported: {probe.get('device_count')}, expected exactly one"]
    agreed = (('memory_total_bytes', vram), ('warp_size', wavefront),
              ('clock_rate_khz', clock_mhz * 1000), ('max_threads_per_block', workgroup))
    for field, expected in agreed:
        if probe.get(field) != expected:
            return [], [f'AMD device readers disagree on {field}: '
                        f'{probe.get(field)} and {expected}']
    free = probe.get('memory_free_bytes')
    if not isinstance(free, int) or not 0 <= free <= vram:
        return [], ['AMD free VRAM is unreadable or out of bounds']
    domain = {0: 'discrete', 1: 'shared_system'}.get(probe.get('integrated'), 'unknown')
    device = {
        'uuid': identity, 'name': agent.get('Marketing Name') or agent.get('Name'),
        'sampled_unix': sampled_unix,
        'vendor': 'amd', 'telemetry_class': TELEMETRY_MEMORY_ONLY,
        'architecture': agent.get('Name'), 'compute_units': units,
        # No amdgpu driver reaches this runtime under WSL2, and the HSA agent
        # report carries no power or live clock at all, so these stay absent
        # and ``telemetry_class`` says so rather than a zero implying idle.
        'power_w': None, 'power_limit_w': None, 'power_reference_w': None,
        'power_measurement_scope': None, 'power_reference_scope': None,
        'power_reference_source': None,
        'sm_clock_mhz': None, 'max_sm_clock_mhz': float(clock_mhz),
        'memory_domain': domain,
        'memory_total_bytes': vram, 'memory_free_bytes': free,
        'memory_used_bytes': vram - free,
        'memory_source': 'rocminfo:agent_pool+hip:hipMemGetInfo',
        'throttle_active_mask': None,
        'throttle_reasons': {name: None for name in THROTTLE_FIELDS},
        'limited': None,
    }
    device['complete'] = domain in ('discrete', 'shared_system')
    errors = [] if device['complete'] else [f'unknown AMD memory domain: {identity}']
    return [device], errors


def devices(*, timeout_s=1.0):
    """Read every physical device this host can honestly describe.

    NVML answers first because it is the only reader that carries the power and
    clock counters the sharing controller needs. The AMD reader runs only when
    NVML found nothing and ``rocminfo`` is actually installed, so a host with
    neither spends one stat rather than a second failed process launch.
    """
    if isinstance(timeout_s, bool) or not math.isfinite(timeout_s) or not 0 < timeout_s <= 5:
        raise ValueError('GPU telemetry timeout must be in (0, 5] seconds')
    found, errors = _nvidia_devices(timeout_s)
    if found or not os.path.exists(ROCMINFO):
        return found, errors
    amd_found, amd_errors = _amd_devices(timeout_s)
    if amd_found:
        return amd_found, amd_errors
    return [], errors + amd_errors


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
            'system_lower_bound_bytes', 'complete', 'gpu_budget_enforceable',
            'processes', 'errors')}
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
        # Both are declarations about the census that produced this sample, not
        # measurements: what the foreign inventory could see, and whether the
        # per-scope GPU allowance is enforceable at all on this hardware. A
        # reader that wants either fact must find it stated here rather than
        # infer it from an empty list or an absent byte count.
        'foreign_inventory_scope': snapshot.get('foreign_inventory_scope'),
        'gpu_process_bytes': bool(snapshot.get('gpu_process_bytes', True)),
        'jobs': jobs, 'errors': errors,
    }
