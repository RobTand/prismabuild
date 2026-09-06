"""Admitted real daemon GPU stop proof; no caller-issued stop before verdict."""
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
from prismabuild import gpu_memory as gm
from prismabuild.resource_scope import ResourceScope


def main():
    with tempfile.TemporaryDirectory(prefix='pb-gpu-scope-proof-') as directory:
        base = Path(directory)
        owned = []
        processes = []
        try:
            for name, mib in [('healthy', 128), ('offender', 1536)]:
                nonce = secrets.token_hex(16)
                key = hashlib.sha256((name + nonce).encode()).hexdigest()
                scope = ResourceScope(key, nonce, gm.GIB, base / (name + '.json'))
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
            specs = [gm.Scope(s.unit, s.cgroup_path, s.memory_max_bytes) for s in owned]
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
                    print(json.dumps({'sample': gm.collect(specs).as_dict()}), flush=True)
                    last_sample = time.monotonic()
                time.sleep(0.25)
            assert offender_status['stop_reason'] == 'memory_budget_exceeded', offender_status
            decision = offender_status['termination_evidence']
            assert decision['scope_id'] == owned[1].unit, decision
            assert decision['reason'] == 'memory_budget_exceeded', decision
            assert decision['evidence']['consecutive_samples'] >= 2, decision
            evidence = decision['evidence']['job']
            assert evidence['lower_bound_bytes'] > evidence['budget_bytes'], evidence
            assert evidence['gpu_lower_bound_bytes'] > evidence['budget_bytes'], evidence
            offender_output = processes[1].communicate(timeout=10)
            assert processes[1].returncode == 137, offender_output
            progress = base / 'healthy.progress'
            before_progress = int(progress.read_text())
            time.sleep(1)
            after_progress = int(progress.read_text())
            assert processes[0].poll() is None and after_progress > before_progress
            print(json.dumps({'verdict': 'automatic_daemon_gpu_budget_stop',
                              'offender_status': offender_status, 'healthy_status': healthy_status,
                              'healthy_progress_before': before_progress,
                              'healthy_progress_after': after_progress,
                              'offender_returncode': processes[1].returncode,
                              'offender_output': offender_output}), flush=True)
        finally:
            for scope in reversed(owned):
                try:
                    scope.terminate_owned('bounded qualification cleanup')
                except Exception as exc:
                    print('cleanup stop: ' + repr(exc), file=sys.stderr)
            for process in processes:
                process.communicate(timeout=10)
            for scope in reversed(owned):
                scope._request('release')


if __name__ == '__main__':
    main()
