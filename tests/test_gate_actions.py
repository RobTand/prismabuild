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
    {'pytest_exit_code': 1}, {'pytest_exit_code': False},
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
