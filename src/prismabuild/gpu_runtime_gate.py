"""Consumers of the broker's host-wide container-runtime fence (#1895)."""
from pathlib import Path
import re
import socket

from . import gpu_capacity


def runtime_gate_state():
    from .adaptive_gpu import trusted_sample
    value = trusted_sample(gpu_capacity.RUNTIME_GATE)
    # Absence supports old brokers during an expand/converge rollout. A present
    # unreadable or malformed record is unknown, and denies GPUs only.
    if not value and not Path(gpu_capacity.RUNTIME_GATE).exists():
        return None
    if value.get('schema') != gpu_capacity.RUNTIME_SCHEMA or type(value.get('latched')) is not bool:
        return {'latched': True, 'reason': gpu_capacity.RUNTIME_REASON, 'subtype': 'state_unknown'}
    return value if value['latched'] else None


def runtime_probe_identity(item):
    """A fence exception authorizes only the exact sealed, host-pinned probe."""
    from . import action_result, adaptive_cpu, core
    try:
        key = str(item['action_key'])
        raw = adaptive_cpu.read_json(Path(item['cas_root']) / 'requests' / key[:2] / f'{key}.json')
        action = core.validate_action(raw)
        action_result.bind_standard_capture_command(action)
        params = action['params']
        env = action['environment']['variables']
        token = env.get(gpu_capacity.RUNTIME_PROBE_ENV)
        runtime = Path(__file__).resolve().parents[2]
        path = env.get('PATH', '')
        expected_paths = {f"{runtime / 'tools'}:/usr/local/bin:/usr/bin:/bin",
                          f"{runtime / 'tools/fleet'}:/usr/local/bin:/usr/bin:/bin"}
        allowed_env = {'HOME', 'TMPDIR', 'TRITON_CACHE_DIR', 'PATH', 'LANG', 'LC_ALL',
                       'OMP_NUM_THREADS', 'MKL_NUM_THREADS', 'OPENBLAS_NUM_THREADS',
                       'PRISMABUILD_CONTAINER_OWNER', 'PRISMABUILD_CONTAINER_MARKER',
                       gpu_capacity.RUNTIME_PROBE_ENV}
        if (action['action_key'] == key and params['command'] == gpu_capacity.runtime_probe_command()
                and params['demand'] == {'cpu': 1, 'mem_gb': 1, 'gpu': 1}
                and socket.gethostname() in params['placement']['required_tags']
                and params.get('container_images') == [gpu_capacity.RUNTIME_PROBE_IMAGE]
                and params.get('execution_timeout_s') == 30.0
                and params.get('gpu_exclusive') is True and params.get('gpu_memory_gb') == 1.0
                and all(env.get(name) == '1' for name in ('OMP_NUM_THREADS', 'MKL_NUM_THREADS', 'OPENBLAS_NUM_THREADS'))
                and path in expected_paths and set(env) <= allowed_env
                and isinstance(token, str) and re.fullmatch('[a-f0-9]{64}', token)):
            return token
    except (OSError, ValueError, TypeError, KeyError, action_result.ActionResultError):
        pass
    return None


def runtime_claim_fence(probe_token=None):
    # The caller supplies only a verified exact probe recipe. Old queued probes
    # may finish after another CDI event; only the current token can CLEAR the
    # broker fence. Letting stale read-only probes finish avoids stranded rows.
    gate = runtime_gate_state()
    if gate is not None and not probe_token:
        return gate
    return None


def runtime_fenced_offer(capacity, detail):
    gate = runtime_gate_state()
    if gate is None:
        return capacity, detail
    return {**capacity, 'gpu': 0}, {**detail, gpu_capacity.RUNTIME_REASON: gate,
                                  'gpu_capacity_error': gpu_capacity.RUNTIME_REASON}
