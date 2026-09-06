"""Smoke harnesses pin existing images and keep builds out of the daemon."""
from pathlib import Path
import json
import os
import subprocess
import pytest

ROOT = Path(__file__).resolve().parents[1]
IMAGE = 'sha256:' + 'a'*64


def _environment(tmp_path, image=IMAGE):
    binary = tmp_path / 'bin'
    binary.mkdir()
    docker = binary / 'docker'
    docker.write_text('''#!/usr/bin/python3
import json, os, pathlib, sys
with pathlib.Path(os.environ['CALLS']).open('a') as output:
 output.write(json.dumps(sys.argv[1:])+'\\n')
if sys.argv[1:3] == ['image', 'inspect']:
 print(os.environ['IMAGE_ID'])
elif sys.argv[1] == 'build':
 sys.exit(99)
''')
    docker.chmod(0o755)
    return dict(os.environ, PATH=str(binary)+':'+os.defpath, CALLS=str(tmp_path/'calls.json'),
                IMAGE_ID=image, PB_SMOKE_RUN_ROOT=str(tmp_path/'runs'), PB_SMOKE_PREBUILT_IMAGE_ID=IMAGE)


def test_single_node_uses_verified_id_without_a_build(tmp_path):
    env = _environment(tmp_path)
    result = subprocess.run(['bash', str(ROOT/'fleet/slurm/smoke/run.sh')], env=env,
                            capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, result.stderr
    calls = [json.loads(line) for line in Path(env['CALLS']).read_text().splitlines()]
    assert calls[0] == ['image', 'inspect', '--format', '{{.Id}}', IMAGE]
    assert len(calls) == 2 and calls[1][0] == 'run'
    assert IMAGE in calls[1]
    assert not any(call[0] == 'build' for call in calls)


@pytest.mark.parametrize('script', ['run.sh', 'multinode/run.sh'])
@pytest.mark.parametrize('bad', ['mutable:tag', 'sha256:'+'b'*64])
def test_harness_refuses_unverified_prebuilt_image_before_work(tmp_path, script, bad):
    env = _environment(tmp_path, image=bad)
    if bad == 'mutable:tag':
        env['PB_SMOKE_PREBUILT_IMAGE_ID'] = bad
    env['PB_SMOKE3_PREBUILT_IMAGE_ID'] = env['PB_SMOKE_PREBUILT_IMAGE_ID']
    result = subprocess.run(['bash', str(ROOT/'fleet/slurm/smoke'/script)], env=env,
                            capture_output=True, text=True, timeout=10)
    assert result.returncode == 2
    calls = [json.loads(line) for line in Path(env['CALLS']).read_text().splitlines()] if Path(env['CALLS']).exists() else []
    assert not any(call[0] in {'build', 'run', 'rmi'} for call in calls)


def test_smoke_shell_syntax_including_accounted_bootstrap():
    for relative in ['run.sh', 'image.sh', 'multinode/run.sh', 'multinode/boot.sh']:
        result = subprocess.run(['bash', '-n', str(ROOT/'fleet/slurm/smoke'/relative)],
                                capture_output=True, text=True)
        assert result.returncode == 0, result.stderr
