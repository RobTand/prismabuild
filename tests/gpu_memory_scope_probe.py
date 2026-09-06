"""Admitted real daemon GPU stop proof; no caller-issued stop before verdict."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import secrets
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from prismabuild import gpu_capacity, gpu_memory as gm
from prismabuild.resource_scope import ResourceScope


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--explicit-gpu-cap', action='store_true',
                        help='prove a 1 GiB GPU cap below a 4 GiB shared-RAM cap')
    args = parser.parse_args()
    memory_max = (4 if args.explicit_gpu_cap else 1) * gm.GIB
    gpu_kwargs = {'gpu_memory_max_bytes': gm.GIB} if args.explicit_gpu_cap else {}
    verdict = None
    with tempfile.TemporaryDirectory(prefix='pb-gpu-scope-proof-') as directory:
        base = Path(directory)
        owned = []
        processes = []
        try:
            for name, mib in [('healthy', 128), ('offender', 1536)]:
                nonce = secrets.token_hex(16)
                key = hashlib.sha256((name + nonce).encode()).hexdigest()
                scope = ResourceScope(key, nonce, memory_max, base / (name + '.json'), **gpu_kwargs)
                scope.create()
                owned.append(scope)
                ready = base / (name + '.ready')
                progress = base / (name + '.progress')
                program = ('import torch,time,pathlib,os; torch.set_num_threads(1); '
                           f'x=torch.ones({mib}*1024**2,dtype=torch.uint8,device="cuda"); '
                           'torch.cuda.synchronize(); '
                           f'pathlib.Path({str(ready)!r}).write_text(str(os.getpid())); '
                           'deadline=time.monotonic()+45; step=0\n'
                           'while time.monotonic()<deadline:\n'
                           f' pathlib.Path({str(progress)!r}).write_text(str(step)); '
                           'step+=1; time.sleep(0.2)\n')
                processes.append(subprocess.Popen(scope.wrap_argv([sys.executable, '-c', program]),
                                                  stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                                  text=True))
            deadline = time.monotonic() + 25
            while not all((base / (name + '.ready')).exists() for name in ('healthy', 'offender')):
                if time.monotonic() >= deadline or any(p.poll() is not None for p in processes):
                    raise RuntimeError('CUDA scoped children did not become ready')
                time.sleep(0.2)
            specs = [gm.Scope(s.unit, s.cgroup_path, s.memory_max_bytes,
                              gpu_budget_bytes=s.gpu_memory_max_bytes) for s in owned]
            devices, errors = gpu_capacity.devices()
            domains = {device['uuid']: device['memory_domain'] for device in devices}
            if args.explicit_gpu_cap:
                assert not errors and set(domains.values()) == {'shared_system'}, (devices, errors)
            deadline = time.monotonic() + 25
            last_sample = 0.
            while True:
                offender_status = owned[1]._request('status')
                healthy_status = owned[0]._request('status')
                assert processes[0].poll() is None, 'healthy scope was stopped'
                assert 'stopped_unix' not in healthy_status, healthy_status
                if offender_status.get('stop_reason'):
                    break
                if time.monotonic() >= deadline:
                    raise AssertionError('daemon did not stop the GPU overbudget scope')
                if time.monotonic() - last_sample >= 1:
                    print(json.dumps({'sample': gm.collect(specs, gpu_memory_domains=domains).as_dict()}), flush=True)
                    last_sample = time.monotonic()
                time.sleep(0.25)
            reason = 'gpu_memory_budget_exceeded' if args.explicit_gpu_cap else 'memory_budget_exceeded'
            assert offender_status['stop_reason'] == reason, offender_status
            decision = offender_status['termination_evidence']
            assert decision['scope_id'] == owned[1].unit, decision
            assert decision['reason'] == reason, decision
            assert decision['evidence']['consecutive_samples'] >= 2, decision
            evidence = decision['evidence']['job']
            assert evidence['gpu_lower_bound_bytes'] > evidence['gpu_budget_bytes'], evidence
            if args.explicit_gpu_cap:
                assert evidence['memory_domain'] == 'shared_system', evidence
                assert evidence['budget_bytes'] == 4 * gm.GIB, evidence
                assert evidence['gpu_budget_bytes'] == gm.GIB, evidence
                assert evidence['system_lower_bound_bytes'] < evidence['budget_bytes'], evidence
                assert int((owned[1].cgroup_path / 'memory.max').read_text()) == 4 * gm.GIB
                assert healthy_status['memory_max_bytes'] == 4 * gm.GIB, healthy_status
                assert healthy_status['gpu_memory_max_bytes'] == gm.GIB, healthy_status
            else:
                assert evidence['lower_bound_bytes'] > evidence['budget_bytes'], evidence
            offender_output = processes[1].communicate(timeout=10)
            assert processes[1].returncode == 137, offender_output
            progress = base / 'healthy.progress'
            before_progress = int(progress.read_text())
            time.sleep(1)
            after_progress = int(progress.read_text())
            assert processes[0].poll() is None and after_progress > before_progress
            # Broker capability tokens belong only to this client's lifetime.
            for status in (offender_status, healthy_status):
                status.pop('token', None)
            verdict = {'verdict': 'automatic_daemon_gpu_budget_stop',
                              'explicit_gpu_cap': args.explicit_gpu_cap,
                              'devices': devices,
                              'offender_status': offender_status, 'healthy_status': healthy_status,
                              'healthy_progress_before': before_progress,
                              'healthy_progress_after': after_progress,
                              'offender_returncode': processes[1].returncode,
                              'offender_output': offender_output}
        finally:
            for scope in reversed(owned):
                try:
                    scope.terminate_owned('bounded qualification cleanup')
                except Exception as exc:
                    print('cleanup stop: ' + repr(exc), file=sys.stderr)
            for process in processes:
                process.communicate(timeout=10)
            for scope in reversed(owned):
                deadline = time.monotonic() + 10
                while True:
                    try:
                        scope._request('release')
                        break
                    except OSError:
                        if time.monotonic() >= deadline:
                            raise
                        time.sleep(.2)
                assert not scope.cgroup_path.exists(), scope.cgroup_path
        assert verdict is not None
        verdict['exact_scopes_removed'] = [scope.unit for scope in owned]
        print(json.dumps(verdict), flush=True)


if __name__ == '__main__':
    main()
