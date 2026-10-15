"""The shared boundary judges corruptions of one actual native recorder log."""
from __future__ import annotations

import copy
import json
import subprocess
import sys

import pytest

from c26_native_support import ROOT, pbtest, repository


@pytest.fixture(scope='module')
def native(tmp_path_factory):
    checkout = repository(tmp_path_factory.mktemp('verdict-vectors') / 'suite', {
        'tests/test_one.py': 'def test_one():\n    pass\ndef test_two():\n    pass\n'})
    done = subprocess.run([*pbtest.shard_entry(sys.executable, checkout), '-q', 'tests/test_one.py'],
                          cwd=checkout, capture_output=True, text=True, timeout=30)
    assert done.returncode == 0, done.stdout + done.stderr
    return {'shard': 0, 'files': ['tests/test_one.py'], 'returncode': 0,
            'timed_out': False, 'output': done.stdout + done.stderr}


def rewrite(row, prefix, change):
    lines = row['output'].splitlines()
    at = next(i for i, line in enumerate(lines) if line.startswith(prefix))
    value = json.loads(lines[at][len(prefix):])
    change(value)
    lines[at] = prefix + json.dumps(value)
    row['output'] = '\n'.join(lines) + '\n'


@pytest.mark.parametrize('damage', ['summary', 'duplicate', 'foreign', 'never-ran',
    'selection', 'completion-roster', 'completion-duplicate', 'completion-invalid',
    'reports-invalid', 'collected-invalid', 'returncode-invalid', 'timeout', 'signal'])
def test_all_corruptions_leave_unknown_coverage(native, damage):
    owner = pbtest.pbtest_outcomes
    row = copy.deepcopy(native)
    if damage == 'summary':
        row['output'] = row['output'].replace('2 passed in ', '3 passed in ')
    elif damage == 'duplicate':
        rewrite(row, owner.PREFIX, lambda r: r['collected'].append(r['collected'][0]))
    elif damage == 'foreign':
        rewrite(row, owner.PREFIX, lambda r: r['reports'].append(
            ['tests/test_one.py::foreign', 'call', 'passed', None, None]))
    elif damage == 'never-ran':
        rewrite(row, owner.PREFIX, lambda r: r['collected'].append('tests/test_one.py::lost'))
    elif damage == 'selection':
        rewrite(row, owner.PREFIX, lambda r: r.update(file_selection={'files': row['files'], 'ignored': row['files']}))
    elif damage == 'completion-roster':
        rewrite(row, owner.COMPLETION_PREFIX, lambda r: r['collected'].pop())
    elif damage == 'completion-duplicate':
        rewrite(row, owner.COMPLETION_PREFIX, lambda r: r.update(duplicate_teardown=True))
    elif damage == 'completion-invalid':
        rewrite(row, owner.COMPLETION_PREFIX, lambda r: r.update(collected=None))
    elif damage == 'reports-invalid':
        rewrite(row, owner.PREFIX, lambda r: r.update(reports=[[]]))
    elif damage == 'collected-invalid':
        rewrite(row, owner.PREFIX, lambda r: r.update(collected=[None]))
    elif damage == 'returncode-invalid':
        row['returncode'] = False
    elif damage == 'timeout':
        row['timed_out'] = True
    else:
        row['returncode'] = -9
    verdict = owner.judge_shards([row], requested_files=native['files'])
    assert not verdict['green'] and verdict['problems']
    assert verdict['unobserved_files'] == native['files']


def test_absent_historical_completion_remains_valid(native):
    owner = pbtest.pbtest_outcomes
    row = copy.deepcopy(native)
    row['output'] = '\n'.join(line for line in row['output'].splitlines()
                              if not line.startswith(owner.COMPLETION_PREFIX))
    verdict = owner.judge_shards([row], requested_files=native['files'])
    assert verdict['green'] and not verdict['unobserved_files']
    assert verdict['shards'][0]['completion']['status'] == 'unknown'


def test_cross_shard_population_names_both_unknown_files(native):
    other = copy.deepcopy(native)
    other.update(shard=1, files=['tests/test_other.py'])
    verdict = pbtest.pbtest_outcomes.judge_shards([native, other],
        requested_files=[*native['files'], *other['files']])
    assert verdict['unobserved_files'] == sorted([*native['files'], *other['files']])
    assert any(p['code'] == 'cross_shard_collection' for p in verdict['problems'])


def test_actual_code_failure_keeps_failure_and_unknown_distinct(native):
    row = copy.deepcopy(native)
    owner = pbtest.pbtest_outcomes
    rewrite(row, owner.PREFIX, lambda r: r['reports'][0].__setitem__(2, 'failed'))
    row['output'] = row['output'].replace('2 passed in ', '1 failed, 1 passed in ')
    row['returncode'] = 1
    verdict = owner.judge_shards([row], requested_files=native['files'])
    assert verdict['failing_nodes'] == ['tests/test_one.py::test_one']
    assert not verdict['unobserved_files'] and not verdict['green']
    row['returncode'] = 7
    unknown = owner.judge_shards([row], requested_files=native['files'])
    assert unknown['failing_nodes'] == verdict['failing_nodes']
    assert unknown['unobserved_files'] == native['files']
