"""Admitted bounded CUDA/scope qualification, two jobs and selective stop."""
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
                program = ('import torch,time,pathlib,os; torch.set_num_threads(1); '
                           f'x=torch.ones({mib}*1024**2,dtype=torch.uint8,device="cuda"); '
                           'torch.cuda.synchronize(); '
                           f'pathlib.Path({str(ready)!r}).write_text(str(os.getpid())); '
                           'time.sleep(45)')
                processes.append(subprocess.Popen(scope.wrap_argv([sys.executable, '-c', program]),
                                                  stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                                  text=True))
            deadline = time.monotonic() + 25
            while not all((base / (name + '.ready')).exists() for name in ('healthy', 'offender')):
                if time.monotonic() >= deadline or any(p.poll() is not None for p in processes):
                    raise RuntimeError('CUDA scoped children did not become ready')
                time.sleep(0.2)
            specs = [gm.Scope(s.unit, s.cgroup_path, s.memory_max_bytes) for s in owned]
            guard = gm.Guard()
            decision = None
            for _ in range(4):
                sample = gm.collect(specs)
                print(json.dumps({'sample': sample.as_dict()}), flush=True)
                decisions = guard.observe(sample)
                if decisions:
                    assert len(decisions) == 1 and decisions[0].scope_id == owned[1].unit, decisions
                    decision = decisions[0]
                    break
                time.sleep(1)
            assert decision is not None, 'GPU overbudget scope was not identified'
            assert decision.reason == 'memory_budget_exceeded'
            owned[1].terminate_owned(decision.reason)
            offender_output = processes[1].communicate(timeout=10)
            assert processes[1].returncode != 0
            assert processes[0].poll() is None, 'healthy scope was stopped'
            print(json.dumps({'verdict': 'selective_gpu_budget_stop', 'decision': decision.as_dict(),
                              'healthy_alive': True, 'offender_returncode': processes[1].returncode,
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
