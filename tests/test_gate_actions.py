"""A missing verdict retries once; a test failure is final and cannot be overwritten."""
import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location('gate_actions', ROOT / 'tools/ci/gate_actions.py')
actions = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(actions)

PLAN = {'mode': 'full', 'base_sha': 'base', 'head_sha': 'head',
        'population': {'version': 'population'},
        'selected': [{'path': 'a.py'}, {'path': 'b.py'}]}
ASSIGNMENTS = [{'index': 0, 'files': ['a.py']}, {'index': 1, 'files': ['b.py']}]


def record(path, verdict='pass', **changes):
    return dict({'files': [path], 'verdict': verdict,
                 'pytest_exit_code': 0 if verdict == 'pass' else 1,
                 'population_version': 'population', 'base_sha': 'base',
                 'head_sha': 'head', 'mode': 'full'}, **changes)


def test_missing_verdict_retries_only_the_missing_shard():
    result = actions.assess(PLAN, ASSIGNMENTS, {1: {0: record('a.py')}})
    assert result['retry_indices'] == [1]
    assert result['missing_indices'] == [1]
    assert result['exit_code'] == 75


def test_test_failure_is_never_retried_or_overwritten_by_a_later_pass():
    result = actions.assess(PLAN, ASSIGNMENTS, {
        1: {0: record('a.py', 'fail'), 1: record('b.py')},
        2: {0: record('a.py')}})
    assert result['retry_indices'] == []
    assert result['failed_indices'] == [0]
    assert result['effective_attempts'] == {0: 1, 1: 1}
    assert result['exit_code'] == 1


def test_second_attempt_can_supply_a_missing_verdict():
    result = actions.assess(PLAN, ASSIGNMENTS, {
        1: {0: record('a.py'), 1: record('b.py', verdict=None, pytest_exit_code=3)},
        2: {1: record('b.py')}})
    assert result['exit_code'] == 0
    assert result['effective_attempts'] == {0: 1, 1: 2}
    assert result['missing_indices'] == []


def test_no_verdict_after_retry_cannot_pass():
    result = actions.assess(PLAN, ASSIGNMENTS, {1: {}, 2: {0: record('a.py')}})
    assert result['exit_code'] == 75 and result['missing_indices'] == [1]


@pytest.mark.parametrize('changes', [
    {'head_sha': 'stale'}, {'base_sha': 'other'}, {'population_version': 'other'},
    {'files': ['a.py', 'a.py']}, {'files': ['b.py']}, {'mode': 'selection'},
    {'pytest_exit_code': 1}, {'pytest_exit_code': False}, {'files': [None]}, {'verdict': []},
])
def test_invalid_or_stale_result_is_not_a_verdict(changes):
    result = actions.assess(PLAN, ASSIGNMENTS, {
        1: {0: record('a.py', **changes), 1: record('b.py')}})
    assert result['retry_indices'] == [0] and result['exit_code'] == 75


@pytest.mark.parametrize('assignments', [
    [{'index': 0, 'files': ['a.py']}],
    [{'index': 0, 'files': ['a.py']}, {'index': 1, 'files': ['a.py', 'b.py']}],
    [{'index': 0, 'files': ['a.py', 'b.py', 'outside.py']}],
    [{'index': 0, 'files': ['a.py']}, {'index': 0, 'files': ['b.py']}],
])
def test_incomplete_or_duplicate_assignments_are_refused(assignments):
    with pytest.raises(ValueError):
        actions.assess(PLAN, assignments, {1: {}, 2: {}})


def cli_fixture(tmp_path):
    import json
    plan = tmp_path / 'plan.json'; plan.write_text(json.dumps(PLAN))
    durations = tmp_path / 'durations.json'; durations.write_text('{"files":{"a.py":3,"b.py":1}}')
    matrix = tmp_path / 'matrix.json'; matrix.write_text(json.dumps({'shards': 2, 'include': [
        {'index': 0, 'runner': 'ubuntu-latest', 'files': 1, 'estimated_seconds': 3},
        {'index': 1, 'runner': 'ubuntu-24.04-arm', 'files': 1, 'estimated_seconds': 1}]}))
    return ['--plan', str(plan), '--matrix', str(matrix), '--durations', str(durations),
            '--artifacts', str(tmp_path / 'artifacts'), '--prefix', 'run-7',
            '--output', str(tmp_path / 'output.json')]


def cli(*arguments):
    import subprocess
    import sys
    return subprocess.run([sys.executable, str(ROOT / 'tools/ci/gate_actions.py'), *arguments],
                          text=True, capture_output=True, check=False)


def save_result(tmp_path, index, verdict='pass', attempt=1):
    import json
    directory = tmp_path / 'artifacts' / f'run-7-a{attempt}-{index}'
    directory.mkdir(parents=True)
    directory.joinpath('result.json').write_text(json.dumps(record('a.py' if index == 0 else 'b.py', verdict)))
    directory.joinpath('junit.xml').write_text('<testsuites/>')


def test_retry_cli_uses_artifact_prefix_and_never_retries_recorded_failure(tmp_path):
    import json
    arguments = cli_fixture(tmp_path)
    save_result(tmp_path, 0, 'fail')
    run = cli('retries', *arguments)
    assert run.returncode == 0, run.stderr
    assert 'retry=true' in run.stdout
    assert [row['index'] for row in json.loads((tmp_path / 'output.json').read_text())['include']] == [1]


def test_finish_cli_fails_without_records_instead_of_accepting_an_empty_download(tmp_path):
    run = cli('finish', *cli_fixture(tmp_path))
    assert run.returncode == 75
    assert 'NO_VERDICT' in run.stdout


def test_finish_cli_exports_only_effective_results(tmp_path):
    import json
    arguments = cli_fixture(tmp_path)
    save_result(tmp_path, 0)
    save_result(tmp_path, 1, attempt=2)
    run = cli('finish', *arguments, '--effective-output', str(tmp_path / 'effective'))
    assert run.returncode == 0, run.stderr
    assert json.loads((tmp_path / 'output.json').read_text())['effective_attempts'] == {'0': 1, '1': 2}
    assert (tmp_path / 'effective/0/junit.xml').is_file()
    assert json.loads((tmp_path / 'effective/1/result.json').read_text())['files'] == ['b.py']
