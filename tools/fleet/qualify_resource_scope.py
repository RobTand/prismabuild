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
HEALTHY = """import json, os, pathlib, socket, stat, sys, time
memory=bytearray(8*1024**2)
end=time.monotonic()+.15
while time.monotonic()<end: pass
probe=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
probe.close()
created=pathlib.Path(sys.argv[2])
created.touch()
status=dict(line.split(':',1) for line in pathlib.Path('/proc/self/status').read_text().splitlines())
print(json.dumps({'ready': True, 'uids': os.getresuid(), 'gids': os.getresgid(),
 'no_new_privs': int(status['NoNewPrivs']),
 'oom_score_adj': int(pathlib.Path('/proc/self/oom_score_adj').read_text()),
 'host_tmp_visible': pathlib.Path(sys.argv[1]).read_text() == 'visible',
 'inet_socket_created': True, 'affinity': sorted(os.sched_getaffinity(0)),
 'created_file_mode': stat.S_IMODE(created.stat().st_mode)}), flush=True)
time.sleep(30)
"""


def qualify(mode: str, image: str | None, directory: Path) -> dict:
    scopes = [ResourceScope(hashlib.sha256(uuid.uuid4().bytes).hexdigest(),
                            uuid.uuid4().hex, LIMIT, directory / f'{mode}-{name}.json')
              for name in ['healthy', 'greedy']]
    created = []
    processes = []
    host_tmp = tempfile.NamedTemporaryFile(mode='w', prefix='pb-scope-host-visible-', dir='/tmp')
    host_tmp.write('visible')
    host_tmp.flush()
    mode_file = Path(host_tmp.name + '.mode')
    expected_mask = int(next(line.split()[1] for line in Path('/proc/self/status').read_text().splitlines()
                             if line.startswith('Umask:')), 8)
    try:
        for scope in scopes:
            scope.create()
            created.append(scope)
        healthy, greedy = scopes
        process = subprocess.Popen(healthy.wrap_argv([sys.executable, '-c', HEALTHY, host_tmp.name, str(mode_file)]),
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        processes.append(process)
        line = process.stdout.readline()
        assert line.strip(), 'healthy payload failed to start: ' + process.stderr.read()
        privilege = json.loads(line)
        assert privilege == {'ready': True, 'uids': [1000]*3, 'gids': [1000]*3,
                             'no_new_privs': 1, 'oom_score_adj': 0,
                             'host_tmp_visible': True, 'inet_socket_created': True,
                             'affinity': sorted(os.sched_getaffinity(0)),
                             'created_file_mode': 0o666 & ~expected_mask}, privilege
        container_ids = []
        if mode == 'direct':
            driver = """import subprocess, sys
children=[subprocess.Popen([sys.executable, '-c', sys.argv[1]]) for _ in range(2)]
for child in children: child.wait()
"""
            processes.append(subprocess.Popen(greedy.wrap_argv([sys.executable, '-c', driver, PAYLOAD]),
                                               stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True))
        else:
            shim = Path(__file__).resolve().parent / 'docker'
            owner = hashlib.sha256(uuid.uuid4().bytes).hexdigest()
            env = dict(os.environ, PRISMABUILD_CONTAINER_OWNER=owner,
                       PRISMABUILD_CONTAINER_MARKER=str(directory / 'docker.used'))
            driver = """import json, subprocess, sys, time
ids=[]
for _ in range(2):
 result=subprocess.run([sys.argv[1], 'run', '--detach', '--entrypoint', '/usr/bin/python3',
                        sys.argv[2], '-c', sys.argv[3]], capture_output=True, text=True)
 if result.returncode: raise RuntimeError(result.stderr)
 ids.append(result.stdout.strip())
print(json.dumps(ids), flush=True)
time.sleep(30)
"""
            child = subprocess.Popen(greedy.wrap_argv([sys.executable, '-c', driver,
                str(shim), image, PAYLOAD]), env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            processes.append(child)
            output = child.stdout.readline()
            assert output.strip(), child.stderr.read()
            container_ids = json.loads(output)
            result = subprocess.run(['/usr/bin/docker', '--host', 'unix:///var/run/docker.sock',
                                     'inspect', *container_ids],
                                    capture_output=True, text=True, check=True)
            containers = json.loads(result.stdout)
            assert len(containers) == 2
            assert all(row['HostConfig']['CgroupParent'] == greedy.unit for row in containers), containers
            assert all(row['State']['Running'] for row in containers), containers
        kernel_oom_group = int((greedy.cgroup_path / 'memory.oom.group').read_text())
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
        stopped_deadline = time.monotonic() + 5
        while True:
            stopped = all(child.poll() is not None for child in processes[1:])
            if container_ids:
                states = subprocess.run(['/usr/bin/docker', '--host', 'unix:///var/run/docker.sock',
                                         'inspect', *container_ids], capture_output=True, text=True, check=True)
                stopped = stopped and all(not row['State']['Running'] for row in json.loads(states.stdout))
            if stopped or time.monotonic() >= stopped_deadline:
                break
            time.sleep(.1)
        assert stopped, 'greedy scope still has running work five seconds after its kernel OOM'
        assert processes[0].poll() is None, 'healthy job was killed by another job OOM' 
        assert well_behaved['oom_kill'] == 0, well_behaved
        assert well_behaved['cpu_seconds'] > .05, well_behaved
        return {'mode': mode, 'greedy': evidence, 'healthy': well_behaved,
                'healthy_survived': True, 'samples': len(samples), 'container_ids': container_ids,
                'payload_privilege': privilege, 'whole_job_stopped': stopped,
                'kernel_oom_group_after_creation': kernel_oom_group}
    finally:
        host_tmp.close()
        mode_file.unlink(missing_ok=True)
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
