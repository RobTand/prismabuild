#!/usr/bin/env python3
"""Admitted-only, bounded proof: one greedy job dies while a healthy job lives.

Uses two 96 MiB job scopes and at most four 56 MiB allocations. The Docker case
puts two individually small containers under one aggregate parent. No test
attempts to consume host headroom. Run through the published pbrun entrypoint.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'src'))
from prismabuild.resource_scope import ResourceScope

LIMIT = 96 * 1024**2
PAYLOAD = "import time; time.sleep(5); memory=bytearray(56*1024**2); time.sleep(30)"
HEALTHY = "import time; memory=bytearray(8*1024**2); end=time.monotonic()+.15\nwhile time.monotonic()<end: pass\nprint('healthy ready',flush=True); time.sleep(30)"


def qualify(mode: str, image: str | None, directory: Path) -> dict:
    scopes = [ResourceScope(hashlib.sha256(uuid.uuid4().bytes).hexdigest(),
                            uuid.uuid4().hex, LIMIT, directory / f'{mode}-{name}.json')
              for name in ['healthy', 'greedy']]
    created = []
    processes = []
    try:
        for scope in scopes:
            scope.create()
            created.append(scope)
        healthy, greedy = scopes
        process = subprocess.Popen(healthy.wrap_argv([sys.executable, '-c', HEALTHY]),
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        processes.append(process)
        assert process.stdout.readline().strip() == 'healthy ready', 'healthy payload failed to start'
        if mode == 'direct':
            for _ in range(2):
                processes.append(subprocess.Popen(greedy.wrap_argv([sys.executable, '-c', PAYLOAD]),
                                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True))
        else:
            shim = Path(__file__).resolve().parent / 'docker'
            owner = hashlib.sha256(uuid.uuid4().bytes).hexdigest()
            env = dict(os.environ, PRISMABUILD_CONTAINER_OWNER=owner,
                       PRISMABUILD_CONTAINER_MARKER=str(directory / 'docker.used'))
            # Each CLI is in the greedy scope before it asks the daemon. The
            # shim transfers that kernel ancestor to Docker's systemd parent.
            for _ in range(2):
                processes.append(subprocess.Popen(greedy.wrap_argv([str(shim), 'run', '--detach',
                    '--entrypoint', '/usr/bin/python3', image, '-c', PAYLOAD]),
                    env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True))
        container_ids = []
        if mode == 'docker':
            for child in processes[1:]:
                output, error = child.communicate(timeout=10)
                assert child.returncode == 0, error
                container_ids.append(output.strip())
            result = subprocess.run(['/usr/bin/docker', '--host', 'unix:///var/run/docker.sock',
                                     'inspect', *container_ids],
                                    capture_output=True, text=True, check=True)
            containers = json.loads(result.stdout)
            assert len(containers) == 2
            assert all(row['HostConfig']['CgroupParent'] == greedy.unit for row in containers), containers
            assert all(row['State']['Running'] for row in containers), containers
        deadline = time.monotonic() + 20
        samples = []
        while time.monotonic() < deadline:
            sample = greedy.sample()
            samples.append(sample)
            if sample['oom_kill']:
                break
            if mode == 'direct' and all(proc.poll() is not None for proc in processes[1:]):
                break
            time.sleep(.1)
        evidence = greedy.sample()
        well_behaved = healthy.sample()
        assert evidence['complete'], evidence
        assert evidence['oom_kill'] > 0, {
            'sample': evidence, 'children': [(proc.poll(), proc.stderr.read() if proc.poll() is not None else '')
                                            for proc in processes[1:]]}
        assert processes[0].poll() is None, 'healthy job was killed by another job OOM'
        assert well_behaved['oom_kill'] == 0, well_behaved
        assert well_behaved['cpu_seconds'] > .05, well_behaved
        return {'mode': mode, 'greedy': evidence, 'healthy': well_behaved,
                'healthy_survived': True, 'samples': len(samples), 'container_ids': container_ids}
    finally:
        for scope in created:
            scope.terminate_owned('disposable qualification cleanup')
            # Removal is exact immutable scope label, never a loose name match.
            if mode == 'docker':
                prefix = ['/usr/bin/docker', '--host', 'unix:///var/run/docker.sock']
                found = subprocess.run([*prefix, 'ps', '-aq', '--no-trunc', '--filter',
                                        f'label=prismabuild.scope={scope.unit}'],
                                       capture_output=True, text=True, check=True)
                ids = found.stdout.split()
                if ids:
                    subprocess.run([*prefix, 'rm', '-f', *ids], check=True, capture_output=True)
            deadline = time.monotonic() + 5
            while True:
                try:
                    scope.release()
                    break
                except OSError as exc:
                    if 'populated' not in str(exc) or time.monotonic() >= deadline:
                        raise
                    time.sleep(.05)
        for process in processes:
            process.communicate(timeout=5)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--docker-image', help='local image containing /usr/bin/python3')
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix='pb-scope-proof-') as directory:
        reports = [qualify('direct', None, Path(directory))]
        if args.docker_image:
            reports.append(qualify('docker', args.docker_image, Path(directory)))
        print(json.dumps({'resource_scope_qualification': reports}, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
