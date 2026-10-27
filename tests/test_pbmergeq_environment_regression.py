"""Failure attribution requires comparable observations, including actual workers."""

import dataclasses

import pytest

from test_pbmergeq import FakeRunner, entries, make_queue, mq, statuses


@pytest.mark.parametrize('different_run', ['base', 'rerun'])
def test_a_different_worker_cannot_make_an_unchanged_failure_new_or_flaky(tmp_path, different_run):
    queue, origin, github, _ = make_queue(tmp_path)
    origin.pr(1, {'readme.txt': 'documentation only\n'})
    node = 'tests/test_a.py::environment_sensitive'

    class DifferentWorkers(FakeRunner):
        def run_many(self, batch, outdir, jobs):
            results = super().run_many(batch, outdir, jobs)
            for name, checkout, files in jobs:
                # The same source fails on x86 and passes on GB10. The gate's
                # independent submissions change only the actual worker.
                worker = ('sparky' if name == different_run
                          or different_run == 'rerun' and name == 'base' else 'dl380g10')
                result = results[name]
                results[name] = dataclasses.replace(result,
                    failed={node} if worker == 'dl380g10' else set(),
                    runtime={**mq.checkout_runtime(queue.cfg, checkout), 'worker': worker},
                    actions=[f'{name}-{worker}'],
                    environments={f: {'identity': worker, 'host': worker} for f in files})
            return results

    queue.runner = DifferentWorkers()
    batch = queue.run_batch([{**entries(1)[0], 'attempts': 1}])

    assert batch['verdict'] == 'attribution-inconclusive', batch
    assert batch['runs']['candidate']['failed'] == [node]
    assert 'culprit' not in batch
    assert statuses(github) == []
    assert queue.store.state['flakes'] == {}
    assert queue.store.state['queue'][0]['attempts'] == 1


def test_missing_environment_evidence_retains_failure_without_charging_the_member(tmp_path):
    queue, origin, github, _ = make_queue(tmp_path)
    origin.pr(1, {'fail-new': 'tests/test_a.py::broken\n'})

    class NoEnvironment(FakeRunner):
        def run_many(self, batch, outdir, jobs):
            return {name: dataclasses.replace(result, environments={})
                    for name, result in super().run_many(batch, outdir, jobs).items()}

    queue.runner = NoEnvironment()
    batch = queue.run_batch(entries(1))
    assert batch['verdict'] == 'attribution-inconclusive'
    assert batch['runs']['candidate']['failed'] == ['tests/test_a.py::broken']
    assert batch['attribution']['files'] == ['tests/test_a.py']
    assert statuses(github) == [] and queue.store.state['flakes'] == {}
    assert queue.store.state['queue'][0]['attempts'] == 0


def test_a_prefix_on_a_different_worker_cannot_identify_a_culprit(tmp_path):
    queue, origin, github, _ = make_queue(tmp_path)
    origin.pr(1, {'readme.txt': 'documentation only\n'})
    origin.pr(2, {'fail-new': 'tests/test_a.py::broken\n'})

    class DifferentPrefix(FakeRunner):
        def run_many(self, batch, outdir, jobs):
            results = super().run_many(batch, outdir, jobs)
            for name, _, files in jobs:
                if name.startswith('prefix'):
                    results[name].environments = {f: {'identity': 'another-worker'} for f in files}
            return results

    queue.runner = DifferentPrefix()
    batch = queue.run_batch(entries(1, 2))
    assert batch['verdict'] == 'attribution-inconclusive'
    assert 'culprit' not in batch
    assert batch['new'] == ['tests/test_a.py::broken']
    assert statuses(github) == []
    assert [entry['attempts'] for entry in queue.store.state['queue']] == [0, 0]


def test_a_cached_base_from_another_worker_is_observed_again(tmp_path):
    from test_pbmergeq import git

    queue, origin, github, runner = make_queue(tmp_path)
    (origin.work / 'fail-main').write_text('tests/test_a.py::old\n')
    git('add', '-A', cwd=origin.work)
    git('commit', '-q', '-m', 'red main', cwd=origin.work)
    origin.pr(1, {'one.txt': 'one\n'})
    assert queue.run_batch(entries(1))['verdict'] == 'green'
    origin.pr(2, {'two.txt': 'two\n'})

    class NewWorker(FakeRunner):
        def run_many(self, batch, outdir, jobs):
            results = super().run_many(batch, outdir, jobs)
            for result in results.values():
                result.environments = {f: {'identity': 'new-worker'} for f in result.files}
            return results

    queue.runner = runner = NewWorker()
    batch = queue.run_batch(entries(2))
    assert batch['verdict'] == 'green'
    assert batch['shared'] == ['tests/test_a.py::old']
    assert ('base', ['tests/test_a.py']) in runner.runs


def test_legacy_node_only_baselines_do_not_authorize_attribution(tmp_path):
    import json

    store = mq.Store(tmp_path)
    store.baseline_path('legacy').write_text(json.dumps({'tests/a.py': ['tests/a.py::old']}))
    assert store.baseline('legacy') == {}


def test_flake_filing_reuses_an_explicit_exact_node_owner_and_retains_evidence(tmp_path, monkeypatch):
    import json
    import subprocess

    from test_pbmergeq import config

    cfg = config(tmp_path)
    store = mq.Store(cfg.state_dir)
    github = mq.GitHub(cfg, store, dry=False)
    node = 'tests/test_a.py::wobbly'
    calls = []

    def existing_owner(command, **kwargs):
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, json.dumps([{
            'number': 42, 'title': 'Known fixture defect',
            'body': f'Owns `{node}` and its related failures.',
            'url': 'https://github.com/RobTand/proj/issues/42'}]), '')

    monkeypatch.setattr(mq, 'call_tool', existing_owner)
    assert github.file_flake('b1', node, 'candidate', {'host': 'worker'}) == (
        'https://github.com/RobTand/proj/issues/42')
    assert len(calls) == 1 and calls[0][:3] == ['gh', 'issue', 'list']


def test_a_new_flake_issue_contains_source_worker_and_action_evidence(tmp_path, monkeypatch):
    import subprocess
    from test_pbmergeq import config

    cfg = config(tmp_path)
    store = mq.Store(cfg.state_dir)
    github = mq.GitHub(cfg, store, dry=False)
    bodies = []

    def create(command, **kwargs):
        if command[:3] == ['gh', 'issue', 'list']:
            return subprocess.CompletedProcess(command, 0, '[]', '')
        bodies.append(kwargs['stdin'])
        return subprocess.CompletedProcess(command, 0, 'https://github.com/RobTand/proj/issues/43', '')

    monkeypatch.setattr(mq, 'call_tool', create)
    github.file_flake('b1', 'tests/test_a.py::wobbly', 'candidate-source',
                      {'worker': 'dl380g10', 'action_key': 'native-action', 'base': 'base-source'})
    assert len(bodies) == 1
    for fact in ['candidate-source', 'base-source', 'dl380g10', 'native-action', 'RobTand/proj']:
        assert fact in bodies[0]


@pytest.mark.parametrize('field', ['worker', 'platform', 'runtime', 'interpreter', 'pin', 'environment', 'demand'])
def test_environment_equivalence_requires_the_observed_runtime_and_test_contract(field):
    import copy
    from prismabuild.pbmergeq_attribution import environment_identity

    producer = {'worker_id': 'dl380g10', 'platform_key': 'x86-platform', 'host_class': 'cpu',
        'evidence': {'hostname': 'dl380g10', 'system': 'Linux', 'machine': 'x86_64',
                     'libc': 'glibc', 'accelerators': []},
        'runtime': {'runtime_sha256': 'runtime-a'}, 'executable': {'sha256': 'shell-a'},
        'toolchain': {'declared': {}, 'verified': {}}}
    interpreter = {'path': '/py', 'sha256': 'python-a'}
    runtime = {'python': '/py', 'pins': {'sdk': 'a' * 40}, 'pin_sources': {}, 'pbtest': '/published/pbtest.py'}
    request = {'environment': {'variables': {'OMP_NUM_THREADS': '1'}}, 'params': {'demand': {'cpu': 1}}}
    before = environment_identity(producer, interpreter, runtime, {}, request)
    producer, interpreter, runtime, request = copy.deepcopy((producer, interpreter, runtime, request))
    if field == 'worker':
        producer['worker_id'] = 'sparky'
    elif field == 'platform':
        producer['platform_key'] = 'arm-platform'
    elif field == 'runtime':
        producer['runtime']['runtime_sha256'] = 'runtime-b'
    elif field == 'interpreter':
        interpreter['sha256'] = 'python-b'
    elif field == 'pin':
        runtime['pins']['sdk'] = 'b' * 40
    elif field == 'environment':
        request['environment']['variables']['OMP_NUM_THREADS'] = '2'
    else:
        request['params']['demand']['cpu'] = 2
    assert environment_identity(producer, interpreter, runtime, {}, request) != before
