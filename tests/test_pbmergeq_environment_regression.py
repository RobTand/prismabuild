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


def test_a_mixed_batch_files_no_flake_when_prefix_environments_differ(tmp_path):
    queue, origin, github, _ = make_queue(tmp_path)
    origin.pr(1, {'fail-flaky': 'flaky:tests/test_a.py::wobbly\n'})
    origin.pr(2, {'fail-new': 'tests/test_b.py::broken\n'})

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
    assert batch['runs']['candidate']['failed'] == [
        'tests/test_a.py::wobbly', 'tests/test_b.py::broken']
    assert batch['new'] == ['tests/test_b.py::broken']
    assert batch['flakes'] == ['tests/test_a.py::wobbly']
    assert 'culprit' not in batch
    assert github.calls == []
    assert queue.store.state['flakes'] == {}
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


@pytest.mark.parametrize('field', ['worker', 'platform', 'runtime', 'interpreter', 'pin', 'environment', 'demand', 'pytest_options'])
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
    request = {'environment': {'variables': {'OMP_NUM_THREADS': '1'}}, 'params': {'demand': {'cpu': 1}, 'command': ['/py', '-c', 'recorder',
        '{"files": ["tests/test_a.py"]}', '-q', 'tests/test_a.py']}}
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
    elif field == 'pytest_options':
        request['params']['command'][4] = '-v'
    else:
        request['params']['demand']['cpu'] = 2
    assert environment_identity(producer, interpreter, runtime, {}, request) != before


@pytest.mark.parametrize('case', ['matched', 'forged_outcome', 'wrong_source', 'wrong_python', 'unbound',
    'unrelated', 'wrong_recorder', 'wrong_files', 'wrong_paths', 'wrong_options',
    'wrong_workers', 'inherited_addopts', 'wrong_roots'])
def test_native_ending_is_the_authority_for_each_report_environment(tmp_path, monkeypatch, case):
    import json
    import os
    import sys
    from types import SimpleNamespace
    from prismabuild import core, pool, submission_frontend
    from prismabuild import _measurement_reservation as reservation
    from test_core import _two_commit_checkout
    from test_pbmergeq import config, pbtest_outcomes, shard
    from test_verified_action_result import _standard_action
    from pbmcp_fixture import settle_publishers

    for key in ('PRISMABUILD_ACTION_NONCE', 'PRISMABUILD_ACTION_SCOPE', 'PRISMABUILD_READER_HELPER_ROOT'):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setattr(pool.socket, 'gethostname', lambda: 'fixture-host')
    monkeypatch.setattr(pool.cpu_admission, 'action_identity', lambda item: ('environment-fixture', False))
    monkeypatch.setattr(reservation.CensusReader, '_spawn_owned',
                        lambda self, directory, section, read, budget_s: read())
    checkout, parent, commit, branch = _two_commit_checkout(tmp_path)
    pbtest = mq.load_published_module(mq.Path(mq.__file__).with_name('pbtest.py'))
    cfg = dataclasses.replace(config(tmp_path, pbtest_args=['--timeout-s', '60',
        '--workers-per-shard', '2', '--pytest-args', '["--durations=1"]']),
        test_python=sys.executable)
    files = ['tests/test_a.py']
    selection = {'files': files, 'roots': [['tests/test_a.py', 'file']]}
    options = ['-q', '--no-header', '-p', 'no:cacheprovider', '-o', 'addopts=',
               '--durations', '1', '-n', '2']
    command = [*pbtest.shard_entry(sys.executable, checkout, collection=True),
               json.dumps(selection), *options, *files]
    if case == 'unrelated':
        command = [sys.executable, '-c', 'raise SystemExit(7)']
    elif case == 'wrong_recorder':
        command[2] += '\n# a different recorder program\n'
    elif case == 'wrong_files':
        command[3] = json.dumps({**selection, 'files': ['tests/test_b.py']})
    elif case == 'wrong_roots':
        command[3] = json.dumps({**selection, 'roots': [['tests', 'directory']]})
    elif case == 'wrong_paths':
        command[-1] = 'tests/test_b.py'
    elif case == 'wrong_options':
        command[-4:-1] = ['-k', 'another_test', '-n', '2']
    elif case == 'wrong_workers':
        command[-2] = '3'
    action = _standard_action(checkout, command=command)
    # pb-history-ok: Commits only the temporary checkout created by _two_commit_checkout.
    from test_pbmergeq import git
    git('add', '-A', cwd=checkout)
    git('commit', '-qm', 'action closure', cwd=checkout)
    parent = commit
    commit = git('rev-parse', 'HEAD', cwd=checkout)
    bundle = tmp_path / 'snapshot.bundle'
    git('bundle', 'create', str(bundle), 'HEAD', cwd=checkout)
    cas = core.PrismaBuildCAS(tmp_path / 'cas')
    entry, _ = cas.ingest_input(bundle, input_id=core.PBRUN_CHECKOUT_SNAPSHOT_INPUT_ID)
    body = {key: value for key, value in action.items() if key != 'action_key'}
    body['inputs'] = [entry]
    body['params'].update(interpreter=sys.executable, demand={'cpu': 1, 'mem_gb': 1},
        checkout_snapshot={'schema': core.PBRUN_CHECKOUT_SNAPSHOT_SCHEMA_V2,
            'commit': commit, 'parent': parent, 'refs': {}, 'subdirectory': '.', 'input': entry})
    body['environment']['variables'].update({
        pbtest_outcomes.RECORD_BINDING_ENV: '1', 'TMPDIR': pbtest.fleet_roster.LEGACY_ACTION_TMPDIR,
        'PYTEST_ADDOPTS': '' if case != 'inherited_addopts' else '-k another_test'})
    action = core.seal_action(body)
    cas.publish_action_request(action)
    producer = core.preflight_action(action, cas_root=cas.root, checkout_root=checkout)
    queue = pool.PoolQueue(tmp_path / 'pb-queue')
    monkeypatch.setattr(queue, 'offers', lambda: [])
    queue.records.ensure_layout()
    key = action['action_key']
    queue.publish(action_key=key, cas_root=cas.root, checkout_root=checkout,
        worker_script=tmp_path / 'worker.py', resources={'cpu': 1, 'mem_gb': 1}, max_attempts=1)
    assert queue.claim(tags=[], capacity={'cpu': 4, 'mem_gb': 4}) is not None
    node = 'tests/test_a.py::broken'
    row = shard(['tests/test_a.py'], rows=[(node, 'call', 'failed', None, None)])
    record = pbtest_outcomes.parse(row['output'])
    nonce = 'a' * 64
    record['run_nonce'] = nonce
    output = (pbtest_outcomes.preflight_record(0, None, run_nonce=nonce) + '\n1 failed in 1.0s\n'
              + pbtest_outcomes.PREFIX + json.dumps(record) + '\n')
    queue.finish(key, status='failed', detail={'returncode': 1, 'action_returncode': 7,
        'stdout': output, 'stderr': '', 'producer_attestation': producer,
        'producer_interpreter': core.identify_executable(sys.executable)})
    settle_publishers()
    row.update(shard=0, action_key=key, record_binding=1, output=output, receipt_path=None,
               environment={'identity': 'untrusted report assertion'})
    runtime = {'python': sys.executable, 'pins': {}, 'pin_sources': {}, 'pbtest': str(cfg.pbtest),
               'source': [parent, 'tree']}
    if case == 'forged_outcome':
        row['output'] = output.replace(node, 'tests/test_a.py::different')
    elif case == 'wrong_source':
        runtime['source'] = ['b' * 40, 'tree']
    elif case == 'wrong_python':
        runtime['python'] = '/different/python'
    elif case == 'unbound':
        row.pop('record_binding')
    monkeypatch.setattr(submission_frontend, 'store_for', lambda path: SimpleNamespace(root=tmp_path))
    runner = mq.Runner(cfg, mq.Store(cfg.state_dir))
    observations, errors = runner.observe_environments([row], runtime, checkout=checkout, files=files)
    if case == 'matched':
        assert errors == []
        assert observations['tests/test_a.py']['host'] == 'fixture-host'
        assert observations['tests/test_a.py']['action_key'] == key
        assert observations['tests/test_a.py']['receipt_sha256'] is None
        assert observations['tests/test_a.py']['identity'] != 'untrusted report assertion'
    else:
        assert observations == {}
        assert len(errors) == 1
