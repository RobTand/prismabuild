"""Native C26 observations. Run only inside an admitted outer PB action."""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys

from pbtest_shard_output import admitted_child, shard_environment
from test_pbtest_reconciles_outcomes import pbtest
from test_pbmergeq import config, mq

ROOT = Path(__file__).resolve().parents[1]
REAL_POPEN = subprocess.Popen
PREFLIGHT = 'pbtest-preflight: '

RELAY = r'''
import json, os, select, signal, subprocess, sys
mode, evidence = sys.argv[1:3]
read_fd, write_fd = os.pipe()
env = dict(os.environ, C26_READY_FD=str(write_fd), PYTHONUNBUFFERED='1')
child = subprocess.Popen(sys.argv[3:], stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT, env=env,
                         start_new_session=True, pass_fds=(write_fd,))
os.close(write_fd)
try:
    if mode == 'timeout':
        ready, _, _ = select.select([read_fd], [], [], 30)
        assert ready, 'native unconfigure hook did not signal readiness'
        assert os.read(read_fd, 64) == b'unconfigure\n'
        os.killpg(child.pid, signal.SIGKILL)
    output, _ = child.communicate(timeout=60)
finally:
    os.close(read_fd)
    if child.poll() is None:
        os.killpg(child.pid, signal.SIGKILL)
        child.communicate(timeout=10)
open(evidence, 'wb').write(output)
if os.environ.get('C26_FAST_SIGNAL') and 'tests/test_b_fast.py' in sys.argv[3:]:
    with open(os.environ['C26_FAST_SIGNAL'], 'wb', buffering=0) as stream:
        stream.write(b'finished\n')
if mode == 'combined':
    lines = output.decode().splitlines()
    positions = [i for i, line in enumerate(lines) if line.startswith('pbtest-outcomes: ')]
    assert positions, 'native final outcomes missing before corruption'
    position = positions[-1]
    value = json.loads(lines[position][len('pbtest-outcomes: '):])
    value['collected'] += [value['collected'][0], 'tests/test_one.py::lost']
    value['reports'].append(['tests/test_one.py::foreign', 'call', 'passed', None, None])
    value['file_selection'] = {'files': ['tests/test_one.py'], 'ignored': ['tests/test_one.py']}
    value['file_selection_error'] = 'controlled inconsistent evidence'
    lines[position] = 'pbtest-outcomes: ' + json.dumps(value)
    output = ('\n'.join(lines) + '\n').encode()
sys.stdout.buffer.write(output)
sys.stdout.buffer.flush()
print('c26-relay-child-exit: ' + str(child.returncode), flush=True)
if mode == 'timeout':
    assert child.returncode == -signal.SIGKILL
    print('pbrun: timeout on controlled-c26-child', flush=True)
    sys.exit(1)
expected = {'failure': 1, 'crash': 7}.get(mode, 0)
assert child.returncode == expected, 'native payload exit differed from the control'
sys.exit(1 if mode == 'nonzero' else child.returncode)
'''

EXIT_BLOCK = '''
import os, threading

def pytest_unconfigure(config):
    os.write(int(os.environ['C26_READY_FD']), b'unconfigure\\n')
    threading.Event().wait()
'''


def repository(path: Path, tests: dict[str, str], *, conftest: str = '') -> Path:
    path.mkdir(parents=True)
    (path / 'pytest.ini').write_text('[pytest]\n')
    for relative, source in tests.items():
        target = path / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(source)
    if conftest:
        (path / 'conftest.py').write_text(conftest)
    for args in (['init', '-q'], ['config', 'user.name', 'C26 fixture'],
                 ['config', 'user.email', 'c26@example.invalid'],
                 ['add', '-A'], ['commit', '-qm', 'native fixture']):
        done = subprocess.run(['git', *args], cwd=path, capture_output=True, text=True,
                              timeout=30)
        assert done.returncode == 0, done.stderr
    return path


def dispatch(checkout: Path, monkeypatch, *, mode: str = 'normal', shards: int = 1,
             pytest_args: list[str] | None = None) -> tuple[int, Path, list[dict]]:
    report = checkout.parent / 'report.json'
    scratch = checkout.parent / 'scratch'
    scratch.mkdir(exist_ok=True)
    launched = []

    def payload(command, **kwargs):
        if str(pbtest.PBRUN) not in [str(part) for part in command]:
            return REAL_POPEN(command, **kwargs)
        argv, environment = admitted_child(command)
        evidence = checkout.parent / f'native-{len(launched)}.log'
        launched.append(evidence)
        # The relay changes a real process ending, not the policy result.
        inherited = kwargs.pop('env', None)
        if inherited is not None:
            environment = {**inherited, **shard_environment(command)}
        return REAL_POPEN([sys.executable, '-c', RELAY, mode, str(evidence), *argv],
                          cwd=checkout, env=environment, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(pbtest.subprocess, 'Popen', payload)
        patch.setattr(sys, 'argv', ['pbtest.py', '--checkout', str(checkout),
            '--python', sys.executable, '--threads-per-shard', '1',
            '--workers-per-shard', '1', '--tmpdir', str(scratch), '--shards', str(shards),
            '--pytest-args', json.dumps(pytest_args or []), '--json', str(report), 'tests'])
        code = pbtest.main()
    assert launched, 'the generated native payload was never launched'
    rows = json.loads(report.read_text())
    assert len(rows) == shards
    assert all(path.is_file() and path.stat().st_size for path in launched)
    return code, report, rows


def consume(tmp_path: Path, report: Path, files: list[str], code: int):
    runner = mq.Runner(config(tmp_path), mq.Store(tmp_path / 'mq-state'))
    log = tmp_path / 'client.log'
    log.write_text('native producer observations are in report.json\n')
    invocation = None
    companion = Path(str(report) + '.progress.json')
    if companion.exists():
        state = json.loads(companion.read_text())
        invocation = {'run_id': state['run_id'], 'launch_entrypoint': state['launch_entrypoint'],
                      'generation': str(ROOT)}
    return runner.read_result(report, files, log=log, returncode=code, invocation=invocation)


def record(output: str, prefix: str = 'pbtest-outcomes: ') -> dict:
    rows = [json.loads(line[len(prefix):]) for line in output.splitlines()
            if line.startswith(prefix)]
    assert rows, f'native producer omitted {prefix!r}'
    return rows[-1]

