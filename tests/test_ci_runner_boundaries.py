"""Exercise runner scripts with synthetic credentials and offline tool boundaries."""
import configparser
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
RUNNERS = ROOT / 'deploy/ci-runners'
IMAGE = 'sha256:' + 'a' * 64
JIT = 'synthetic-one-job-config'


@pytest.fixture
def host(tmp_path):
    bin_dir = tmp_path / 'bin'
    bin_dir.mkdir()
    env = {'HOME': str(tmp_path), 'PATH': f'{bin_dir}:/usr/bin:/bin',
           'CALLS': str(tmp_path / 'calls.jsonl'), 'PB_CI_IMAGE': IMAGE,
           'TEST_HOST': 'dl380g10', 'TEST_ARCH': 'x86_64'}
    stub = '''import json, os, sys
from pathlib import Path
name = Path(sys.argv[0]).name
args = sys.argv[1:]
with open(os.environ['CALLS'], 'a') as log:
    log.write(json.dumps([name, *args]) + '\\n')
if name in ('gh', 'ssh'):
    if os.environ.get('FAIL_REGISTRATION'):
        print('sensitive-response', file=sys.stderr)
        sys.exit(42)
    print(os.environ.get('JIT_RESPONSE', 'synthetic-one-job-config'))
elif name == 'hostname': print(os.environ['TEST_HOST'])
elif name == 'uname': print(os.environ['TEST_ARCH'])
elif name == 'id': print('1000')
elif name == 'free': print('Mem: 121 9 57 0 57 112')
elif name == 'podman':
    if args[0] == 'info': print('true')
    elif args[:2] == ['image', 'inspect']: print('sha256:' + 'a' * 64)
    elif args[0] == 'run':
        assert sys.stdin.read() == 'synthetic-one-job-config\\n'
        assert 'ACTIONS_RUNNER_INPUT_JITCONFIG' not in os.environ
'''
    for name in ('gh', 'ssh', 'podman', 'systemctl', 'systemd-notify',
                 'hostname', 'uname', 'id', 'free'):
        path = bin_dir / name
        path.write_text(f'#!{sys.executable}\n' + stub)
        path.chmod(0o700)
    return tmp_path, env


def run(host, script, *args, input=None):
    return subprocess.run(['bash', str(RUNNERS / script), *args], env=host[1],
                          input=input, capture_output=True, text=True, timeout=30)


def calls(host):
    path = host[0] / 'calls.jsonl'
    return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []


@pytest.mark.parametrize(('name', 'arch', 'role', 'label', 'memory'), [
    ('dl380g10', 'x86_64', 'pb-suite', 'x64', '6g'),
    ('sparky', 'aarch64', 'pb-gate', 'arm64', '8g'),
    ('sparklina', 'aarch64', 'pb-gate', 'arm64', '8g'),
])
def test_registration_transport_isolation_and_credential_handoff(host, name, arch, role, label, memory):
    host[1].update(TEST_HOST=name, TEST_ARCH=arch, HTTPS_PROXY='sensitive-proxy')
    result = run(host, 'runner-slot.sh', '1')
    assert result.returncode == 0, result.stderr
    assert JIT not in result.stdout + result.stderr
    commands = calls(host)
    registration = next(c for c in commands if c[0] in ('gh', 'ssh'))
    import shlex
    remote = shlex.split(registration[-1]) if registration[0] == 'ssh' else registration
    assert registration[0] == ('gh' if arch == 'x86_64' else 'ssh')
    if arch == 'aarch64':
        assert registration[1:4] == ['-o', 'BatchMode=yes', 'dl380g10']
    assert 'repos/RobTand/prismabuild/actions/runners/generate-jitconfig' in remote
    assert {f'labels[]={v}' for v in ('self-hosted', 'linux', label, 'pb-fleet', role)} <= set(remote)
    runner = next(v for v in remote if v.startswith('name='))
    assert runner.startswith(f'name=pb-{name}-slot-1-')
    container = next(c for c in commands if c[:2] == ['podman', 'run'])
    assert container[container.index('--name') + 1] == 'pb-ci-runner-1'
    assert container[-2] == IMAGE
    assert container[-1], 'the entrypoint must receive the service CPU mask'
    assert {'--rm', '--interactive', '--pull=never', '--network=private', '--pid=private',
            '--ipc=private', '--http-proxy=false', '--sdnotify=ignore',
            '--cap-drop=all', '--security-opt=no-new-privileges', '--pids-limit=1024',
            f'--memory={memory}', f'--memory-swap={memory}'} <= set(container)
    assert not any(a.startswith(('--volume', '--mount', '--env', '--device', '--privileged'))
                   for a in container)
    if arch == 'x86_64':
        assert '--cpus=4' in container
        assert container[container.index('--cpuset-cpus') + 1] == '40-79'
    else:
        assert '--cpuset-cpus' not in container
    notify = ['systemd-notify', 'RESTART_RESET=1']
    assert commands.index(notify) < commands.index(container)


@pytest.mark.parametrize('response', ['', 'null', 'bad input', 'one\ntwo'])
def test_invalid_jit_never_starts_container_or_resets_backoff(host, response):
    host[1]['JIT_RESPONSE'] = response
    result = run(host, 'runner-slot.sh', '1')
    assert result.returncode != 0
    assert not any(c[0] in ('podman', 'systemd-notify') for c in calls(host))


@pytest.mark.parametrize('name,arch', [('dl380g10', 'x86_64'), ('sparky', 'aarch64')])
def test_registration_failure_reports_only_exit_status(host, name, arch):
    host[1].update(TEST_HOST=name, TEST_ARCH=arch, FAIL_REGISTRATION='1', GH_DEBUG='api')
    result = run(host, 'runner-slot.sh', '1')
    assert result.returncode != 0
    assert 'exit 42' in result.stderr
    assert 'sensitive-response' not in result.stdout + result.stderr
    assert not any(c[0] in ('podman', 'systemd-notify') for c in calls(host))


@pytest.mark.parametrize('slot,image', [('3', IMAGE), ('1', 'localhost/pb-ci-runner:mutable')])
def test_invalid_slot_or_mutable_image_refuses_before_registration(host, slot, image):
    host[1]['PB_CI_IMAGE'] = image
    assert run(host, 'runner-slot.sh', slot).returncode != 0
    assert not any(c[0] in ('gh', 'ssh', 'podman') for c in calls(host))


def test_entrypoint_keeps_jit_out_of_listener_argv(host):
    listener = host[0] / 'listener'
    listener.write_text(f'#!{sys.executable}\nimport os,sys,json\n'
                        'print(json.dumps([sys.argv[1:], os.environ.get("ACTIONS_RUNNER_INPUT_JITCONFIG")]))\n')
    listener.chmod(0o700)
    entrypoint = host[0] / 'entrypoint.sh'
    entrypoint.write_text((RUNNERS / 'runner-entrypoint.sh').read_text().replace(
        '/home/runner/bin/Runner.Listener', str(listener)))
    result = subprocess.run(['bash', str(entrypoint)], env=host[1], input=JIT + '\n',
                            capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == [['run'], JIT]


@pytest.mark.parametrize(('name', 'arch', 'affinities'), [
    ('dl380g10', 'x86_64', ['40-79', '40-79']),
    ('sparky', 'aarch64', ['0-4', '10-14']),
])
def test_installer_is_separate_idempotent_and_uses_systemd_affinity(host, name, arch, affinities):
    host[1].update(TEST_HOST=name, TEST_ARCH=arch)
    sentinel = host[0] / '.config/tl3-ci-runners/runner.env'
    sentinel.parent.mkdir(parents=True)
    sentinel.write_text('untouched\n')
    for _ in range(2):
        result = run(host, 'install.sh', '2')
        assert result.returncode == 0, result.stderr
    assert sentinel.read_text() == 'untouched\n'
    for slot, affinity in enumerate(affinities, 1):
        config = host[0] / f'.config/pb-ci-runners/slot-{slot}.env'
        assert config.read_text() == f'PB_CI_IMAGE={IMAGE}\n'
        assert config.stat().st_mode & 0o777 == 0o600
        dropin = host[0] / f'.config/systemd/user/pb-ci-runner@{slot}.service.d/placement.conf'
        assert f'CPUAffinity={affinity}' in dropin.read_text()
    unit = configparser.ConfigParser(interpolation=None)
    unit.read(host[0] / '.config/systemd/user/pb-ci-runner@.service')
    assert unit['Unit']['StartLimitIntervalSec'] == '0'
    assert unit['Service']['Restart'] == 'always'
    assert unit['Service']['RestartMaxDelaySec'] == '5min'
    assert unit['Service']['NotifyAccess'] == 'all'
    assert unit['Service']['EnvironmentFile'] == '%h/.config/pb-ci-runners/slot-%i.env'
    commands = calls(host)
    assert not any('restart' in c or 'sudo' in c for c in commands)
    assert not any('tl3-' in ' '.join(c) for c in commands)
    enabled = [c[-1] for c in commands if c[0] == 'systemctl' and 'enable' in c]
    assert enabled == [f'pb-ci-runner@{i}.service' for i in (1, 2, 1, 2)]


def test_ssh_broker_accepts_only_pb_gate_registration(host):
    import shlex
    args = ['gh', 'api', '--method', 'POST',
            'repos/RobTand/prismabuild/actions/runners/generate-jitconfig',
            '-f', 'name=pb-sparklina-slot-1-12345678-1234-1234-1234-123456789abc',
            '-F', 'runner_group_id=1', '-f', 'work_folder=_work',
            *[a for label in ('self-hosted', 'linux', 'arm64', 'pb-fleet', 'pb-gate')
              for a in ('-f', f'labels[]={label}')], '--jq', '.encoded_jit_config']
    host[1]['SSH_ORIGINAL_COMMAND'] = shlex.join(args)
    broker = RUNNERS / 'jit-broker.py'
    result = subprocess.run([sys.executable, '-I', str(broker)], env=host[1],
                            capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr
    assert result.stdout == JIT + '\n'
    assert calls(host) == [args]
    (host[0] / 'calls.jsonl').unlink()
    for command in ('sh', shlex.join(args).replace('prismabuild/', 'another-repo/'),
                    shlex.join(args) + '; cat private-file'):
        host[1]['SSH_ORIGINAL_COMMAND'] = command
        result = subprocess.run([sys.executable, '-I', str(broker)], env=host[1],
                                capture_output=True, text=True, timeout=30)
        assert result.returncode != 0
        assert not calls(host)


def test_entrypoint_restores_the_admitted_cpu_mask(host):
    cpu = min(os.sched_getaffinity(0))
    listener = host[0] / 'affinity-listener'
    listener.write_text(f'#!{sys.executable}\nimport json,os\n'
                        'print(json.dumps(sorted(os.sched_getaffinity(0))))\n')
    listener.chmod(0o700)
    entrypoint = host[0] / 'affinity-entrypoint.sh'
    entrypoint.write_text((RUNNERS / 'runner-entrypoint.sh').read_text().replace(
        '/home/runner/bin/Runner.Listener', str(listener)))
    result = subprocess.run(['bash', str(entrypoint), str(cpu)], env=host[1],
                            input=JIT + '\n', capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == [cpu]


def test_successful_registration_resets_only_its_pb_unit_on_older_systemd(host):
    result = run(host, 'runner-slot.sh', '2')
    assert result.returncode == 0, result.stderr
    commands = calls(host)
    reset = ['systemctl', '--user', 'reset-failed', 'pb-ci-runner@2.service']
    container = next(c for c in commands if c[:2] == ['podman', 'run'])
    assert commands.index(reset) < commands.index(container)
    (host[0] / 'calls.jsonl').unlink()
    host[1]['FAIL_REGISTRATION'] = '1'
    assert run(host, 'runner-slot.sh', '2').returncode != 0
    assert not any(c[0] in ('systemctl', 'systemd-notify') for c in calls(host))
