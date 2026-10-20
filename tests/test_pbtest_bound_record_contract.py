"""Versioned record readers require the harness's independent association."""
from __future__ import annotations

import copy
import json
import subprocess
import sys

import pytest

from c26_native_support import pbtest, repository


@pytest.fixture(params=[[], ['-n', '2'], ['--collect-only']], ids=['serial', 'xdist', 'collection'])
def bound(request, tmp_path):
    checkout = repository(tmp_path / 'suite', {
        'tests/test_one.py': 'def test_one():\n    pass\n'})
    spec = {'files': ['tests/test_one.py'], 'roots': [['tests', 'directory']],
            'record_binding': 'expected-shard-command'}
    argv = [*pbtest.shard_entry(sys.executable, checkout, collection=True),
            json.dumps(spec), '-q', *request.param, 'tests/test_one.py']
    done = subprocess.run(argv, cwd=checkout, capture_output=True, text=True, timeout=30)
    assert done.returncode == 0, done.stdout + done.stderr
    return {'shard': 0, 'files': spec['files'], 'returncode': 0,
            'record_binding': pbtest.pbtest_outcomes.binding_from_argv(argv),
            'output': done.stdout}, bool(request.param == ['--collect-only'])


def test_bound_readers_replay_and_required_population(bound, tmp_path):
    row, collect_only = bound
    owner = pbtest.pbtest_outcomes
    record = owner.parse(row['output'], row['record_binding'])
    assert record['schema'] == 'prismabuild.pbtest_outcomes.v2'
    assert owner.parse(row['output']) is None
    assert owner.parse(row['output'], 'foreign-shard') is None
    forged = copy.deepcopy(record)
    forged.update(record_binding='foreign-shard', collect_only=True, collected=[])
    row['output'] += '\n' + owner.PREFIX + json.dumps(forged) + '\n'
    row['output'] += owner.COMPLETION_PREFIX + '{broken json}\n'
    row['output'] += owner.COMPLETION_PREFIX + json.dumps({
        'schema': owner.COMPLETION_SCHEMA, 'collect_only': True, 'collected': []}) + '\n'
    # Persist the independent metadata exactly as downstream consumers receive it.
    replay = json.loads(json.dumps([row]))
    verdict = owner.judge_shards(replay, requested_files=row['files'])
    assert verdict['green'], verdict
    assert verdict['shards'][0]['coverage'] == ('collection_only' if collect_only else 'executed')
    strict = owner.judge_shards(replay, requested_files=row['files'],
                               required_nodes=['tests/test_one.py::test_one'])
    assert strict['green'] is not collect_only
    history = tmp_path / 'history.json'
    history.write_text(json.dumps(replay))
    samples, problems = pbtest.load_history([str(history)])
    assert not problems
    assert isinstance(samples, dict)


def test_unbound_completion_presence_does_not_require_completion_validation(bound):
    row, _ = bound
    owner = pbtest.pbtest_outcomes
    row['output'] = '\n'.join(line for line in row['output'].splitlines()
                              if not line.startswith(owner.COMPLETION_PREFIX))
    row['output'] += '\n' + owner.COMPLETION_PREFIX + json.dumps({
        'schema': owner.COMPLETION_SCHEMA, 'collected': []}) + '\n'
    verdict = owner.judge_shards([row], requested_files=row['files'])
    assert verdict['green'], verdict
    assert verdict['shards'][0]['completion']['status'] == 'unknown'
