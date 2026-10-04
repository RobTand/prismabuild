"""The generated producer seals its interpreter, environment and queue row."""
from __future__ import annotations

import json
import shutil
from pathlib import Path
import sys

import pytest

from test_pbtest import pbtest
from test_pbrun_detach import _checkout, pbrun
from prismabuild import core as pb, pool
from pbtest_shard_output import ShardProcess, shard_output_for

PUBLISHED = Path('/mnt/shared/prismabuild-fleet/repo')


def _generated(root, monkeypatch, *, extra=(), python=sys.executable):
    work = _checkout(root)
    (work / 'tests').mkdir()
    (work / 'tests/test_one.py').write_text('def test_one(): pass\n')
    calls = []

    class Finished(ShardProcess):
        returncode = 0
        def __init__(self, command):
            self.output = shard_output_for(command)
        def communicate(self):
            return self.output, None

    def capture(command, **kwargs):
        calls.append(command)
        return Finished(command)

    with monkeypatch.context() as scoped:
        scoped.setattr(pbtest, 'RUNTIME_ROOT', PUBLISHED)
        scoped.setattr(pbtest.subprocess, 'Popen', capture)
        scoped.setattr(pbtest, 'interpreter_refusal', lambda *a, **k: (None, None))
        scoped.setattr(sys, 'argv', ['pbtest.py', '--checkout', str(work),
                                   '--python', python, '--shards', '1',
                                   '--threads-per-shard', '1', '--timeout-s', '120',
                                   *extra, 'tests'])
        assert pbtest.main() == 0
    return work, calls[0]


def _sealed(root, monkeypatch, *, extra=(), python=sys.executable):
    work, command = _generated(root, monkeypatch, extra=extra, python=python)
    queue = pool.PoolQueue(root / 'pb-queue')
    queue.announce(host='qualified', tags=['x86', pb.INTERPRETER_TAG],
                   interpreters=[python], has_gpu=False,
                   capacity={'cpu': 8, 'mem_gb': 16}, timeout_ceiling_s=300)
    with monkeypatch.context() as scoped:
        scoped.setattr(pbrun, 'SH', root)
        scoped.setattr(pbrun, 'RUNTIME_ROOT', PUBLISHED)
        scoped.setattr(sys, 'argv', [*command[1:2], '--detach', *command[2:]])
        assert pbrun.main() == 0
    requests = list((root / 'cas/requests').rglob('*.json'))
    assert len(requests) == 1
    action = json.loads(requests[0].read_text())
    item = json.loads(queue.item_path(pool.READY, action['action_key']).read_text())
    return action, item, queue


def test_generated_request_and_published_row_bind_the_same_interpreter(tmp_path,
                                                                     monkeypatch):
    action, row, _ = _sealed(tmp_path, monkeypatch)
    assert action['params']['command'][0] == sys.executable
    assert action['params']['interpreter'] == sys.executable
    assert row['interpreter'] == sys.executable
    assert pb.INTERPRETER_TAG in row['tags']
    assert action['params']['placement']['required_tags'] == []


def test_environment_values_are_sealed_as_values_not_interpreter_arguments(tmp_path,
                                                                         monkeypatch):
    directory = tmp_path / 'worker scratch with spaces'
    action, row, _ = _sealed(tmp_path, monkeypatch, extra=(
        '--tmpdir', str(directory), '--pytest-args', '[]', '--cpus-per-shard', '4'))
    variables = action['environment']['variables']
    assert variables['TMPDIR'] == str(directory)
    assert variables['PYTEST_ADDOPTS'] == ''
    assert variables['PYTHONPATH'] == 'src:experiments'
    for key in ('OMP_NUM_THREADS', 'MKL_NUM_THREADS', 'OPENBLAS_NUM_THREADS',
                'TORCH_NUM_THREADS'):
        assert variables[key] == '1'
    assert float(variables[pbtest.test_bound_contract.TIMEOUT_ENV]) == 90
    assert row['resources']['cpu'] == 4
    assert not any(s.startswith(('TMPDIR=', 'OMP_NUM_THREADS=', 'PYTHONPATH='))
                   for s in action['params']['command'])


def test_untagged_generated_row_matches_all_and_only_capable_architectures(tmp_path,
                                                                       monkeypatch):
    _, row, queue = _sealed(tmp_path, monkeypatch)
    for host, tags, answers in [
            ('arm', [pb.INTERPRETER_TAG, 'gb10'], [sys.executable]),
            ('x86', [pb.INTERPRETER_TAG, 'x86'], [sys.executable]),
            ('missing', [pb.INTERPRETER_TAG], []),
            ('legacy', [], None)]:
        queue.announce(host=host, tags=tags, interpreters=answers,
                       has_gpu=False, capacity={'cpu': 8, 'mem_gb': 16})
    assert set(queue.placeable_hosts(row)) == {'qualified', 'arm', 'x86'}


def test_explicit_architecture_constraint_survives_the_interpreter_requirement(tmp_path,
                                                                            monkeypatch):
    action, row, queue = _sealed(tmp_path, monkeypatch, extra=('--tag', 'x86'))
    queue.announce(host='arm', tags=[pb.INTERPRETER_TAG, 'gb10'],
                   interpreters=[sys.executable], has_gpu=False,
                   capacity={'cpu': 8, 'mem_gb': 16})
    assert action['params']['placement']['required_tags'] == ['x86']
    assert row['tags'] == [pb.INTERPRETER_TAG, 'x86']
    assert queue.placeable_hosts(row) == ['qualified']


def test_preflight_prices_the_same_cpu_demand_as_the_generated_request(tmp_path, monkeypatch):
    captured = []
    def refusal(queue, python, **kwargs):
        captured.append(kwargs['resources'])
        return None, None
    # _generated suppresses the live read. Capture the call through its own
    # dispatcher fixture instead of observing a mutable fleet.
    from test_pbtest_reserves_its_threads import _dispatch, pbtest as subject
    monkeypatch.setattr(subject, 'interpreter_refusal', refusal)
    code, _ = _dispatch(tmp_path, monkeypatch, ['--cpus-per-shard', '4'])
    assert code == 0 and captured == [{'cpu': 4, 'mem_gb': 3}]


@pytest.mark.parametrize('answers,absent,verdict', [
    ([sys.executable], [], 'present'),
    ([], [sys.executable], 'absent'),
    (None, [], 'unknown'),
])
def test_published_request_keeps_the_existing_three_state_path_verdict(
        tmp_path, monkeypatch, answers, absent, verdict):
    _, row, queue = _sealed(tmp_path, monkeypatch)
    queue.announce(host='qualified', tags=[pb.INTERPRETER_TAG],
                   interpreters=answers, has_gpu=False,
                   capacity={'cpu': 8, 'mem_gb': 16})
    path = queue.root / 'workers/qualified.json'
    document = json.loads(path.read_text())
    document['interpreters_absent'] = absent
    path.write_text(json.dumps(document))
    assert queue.interpreter_placement_verdict(row, sys.executable) == verdict
    assert queue.placeable_hosts(row) == (['qualified'] if verdict == 'present' else [])


def test_legacy_worker_cannot_claim_the_generated_naming_row(tmp_path, monkeypatch):
    _, row, queue = _sealed(tmp_path, monkeypatch)
    assert queue.claim(capacity={'cpu': 8, 'mem_gb': 16}, tags=['x86']) is None
    assert queue.item_path(pool.READY, row['action_key']).exists()


def test_interpreter_removed_after_publication_is_refused_at_claim(tmp_path, monkeypatch):
    interpreter = tmp_path / 'python-fixture'
    # A native interpreter is a toolchain member; a shell-script stand-in
    # would correctly trip the existing checkout-owned-script gate first.
    shutil.copy2(Path(sys.executable).resolve(), interpreter)
    _, row, queue = _sealed(tmp_path, monkeypatch, python=str(interpreter))
    interpreter.unlink()
    assert queue.claim(capacity={'cpu': 8, 'mem_gb': 16},
                       tags=[pb.INTERPRETER_TAG]) is None
    from test_interpreter_placement import _denials
    denial = _denials(queue)[-1]
    assert denial['reason'] == 'interpreter_not_present'
    assert denial['evidence']['interpreter'] == str(interpreter)
    assert queue.item_path(pool.READY, row['action_key']).exists()


@pytest.mark.parametrize('python', ['python3', 'relative/bin/python', '/bin/sh'])
def test_unsupported_interpreter_spellings_refuse_before_fanout(tmp_path, monkeypatch, python):
    # The existing pbrun declaration recognizes absolute python* entries only;
    # the producer must not advertise a fence for an unrecognized command.
    work = _checkout(tmp_path)
    monkeypatch.setattr(sys, 'argv', ['pbtest.py', '--checkout', str(work), '--python', python])
    calls = []
    monkeypatch.setattr(pbtest.subprocess, 'Popen', lambda *a, **k: calls.append(a))
    assert pbtest.main() == 2 and calls == []
