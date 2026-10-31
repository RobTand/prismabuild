"""The shared shard interface preserves coverage and balances deterministic costs."""
import importlib.util
import itertools
import json
from pathlib import Path
import subprocess
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location('gate_partition', ROOT / 'tools/ci/gate_partition.py')
partition = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(partition)


def test_longest_first_assignment_is_complete_balanced_and_order_independent():
    files = ['a', 'b', 'c', 'd']
    costs = {'a': 8, 'b': 5, 'c': 5, 'd': 2}
    expected = [
        {'index': 0, 'files': ['a', 'd'], 'estimated_seconds': 10.0, 'unknown_files': []},
        {'index': 1, 'files': ['b', 'c'], 'estimated_seconds': 10.0, 'unknown_files': []},
    ]
    for order in itertools.permutations(files):
        assert partition.shards(order, costs, 2, unknown_seconds=3) == expected


def test_equal_costs_break_ties_by_path_then_shard_index():
    result = partition.shards(['d', 'b', 'c', 'a'], dict.fromkeys('abcd', 1), 3,
                              unknown_seconds=2)
    assert [row['files'] for row in result] == [['a', 'd'], ['b'], ['c']]


def test_unknown_cost_is_nearest_rank_p90_and_recorded_explicitly():
    costs = {str(i): i for i in range(1, 11)}
    fallback = partition.default_unknown_seconds(costs)
    assert fallback == 9
    result = partition.shards(['new', '1'], costs, 2, unknown_seconds=fallback)
    assert result[0] == {'index': 0, 'files': ['new'], 'estimated_seconds': 9.0,
                         'unknown_files': ['new']}
    assert result[1]['files'] == ['1']


def test_empty_or_zero_only_inventory_has_a_positive_unknown_default():
    assert partition.default_unknown_seconds({}) > 0
    assert partition.default_unknown_seconds({'zero': 0}) > 0


def test_empty_shards_are_retained():
    rows = partition.shards(['only'], {'only': 4}, 3, unknown_seconds=1)
    assert rows == [
        {'index': 0, 'files': ['only'], 'estimated_seconds': 4.0, 'unknown_files': []},
        {'index': 1, 'files': [], 'estimated_seconds': 0.0, 'unknown_files': []},
        {'index': 2, 'files': [], 'estimated_seconds': 0.0, 'unknown_files': []},
    ]
    assert len(partition.shards([], {}, 3, unknown_seconds=1)) == 3


@pytest.mark.parametrize('count', [0, -1, True, 1.5])
def test_invalid_count_is_refused(count):
    with pytest.raises(ValueError):
        partition.shards(['a'], {'a': 1}, count, unknown_seconds=1)


@pytest.mark.parametrize('bad', [-1, float('inf'), float('nan'), True, False])
def test_invalid_duration_is_refused_even_when_file_is_not_selected(bad):
    with pytest.raises(ValueError):
        partition.shards(['a'], {'a': 1, 'other': bad}, 2, unknown_seconds=1)
    with pytest.raises(ValueError):
        partition.default_unknown_seconds({'other': bad})
    with pytest.raises(ValueError):
        partition.shards(['a'], {}, 2, unknown_seconds=bad)


def test_duplicate_files_are_refused():
    with pytest.raises(ValueError, match='duplicate'):
        partition.shards(['a', 'a'], {'a': 1}, 2, unknown_seconds=1)


def invoke(*args):
    return subprocess.run([sys.executable, str(ROOT / 'tools/ci/gate_partition.py'), *args],
                          text=True, capture_output=True, check=False)


def test_matrix_and_shard_commands_agree_with_unknowns_and_blank_lines(tmp_path):
    files = tmp_path / 'files.txt'
    files.write_text('tests/c.py\n\ntests/a.py\ntests/new file.py\ntests/b.py\n')
    durations = tmp_path / 'durations.json'
    durations.write_text(json.dumps({'files': {'tests/a.py': 10, 'tests/b.py': 7, 'tests/c.py': 1}}))
    matrix = tmp_path / 'matrix.json'
    common = ['--files-from', str(files), '--durations', str(durations), '--shards', '3']
    run = invoke('matrix', *common, '--runners', 'ubuntu-latest,ubuntu-24.04-arm',
                 '--output', str(matrix))
    assert run.returncode == 0, run.stderr
    data = json.loads(matrix.read_text())
    assert data['schema'] == 'prismabuild.gate_matrix.v1'
    assert data['shards'] == 3 and data['total_estimated_seconds'] == 28
    assert [row['runner'] for row in data['include']] == [
        'ubuntu-latest', 'ubuntu-24.04-arm', 'ubuntu-latest']
    observed = []
    for row in data['include']:
        output = tmp_path / f"shard-{row['index']}.txt"
        run = invoke('shard', *common, '--index', str(row['index']), '--output-files', str(output))
        assert run.returncode == 0, run.stderr
        assigned = output.read_text().splitlines()
        assert len(assigned) == row['files']
        assert assigned == sorted(assigned)
        observed.extend(assigned)
    assert sorted(observed) == ['tests/a.py', 'tests/b.py', 'tests/c.py', 'tests/new file.py']
    assert data['include'][0]['estimated_seconds'] == 10


def test_cli_explicit_unknown_cost_and_invalid_assignment(tmp_path):
    files = tmp_path / 'files.txt'; files.write_text('new.py\n')
    durations = tmp_path / 'durations.json'; durations.write_text('{"files": {}}')
    output = tmp_path / 'matrix.json'
    common = ['--files-from', str(files), '--durations', str(durations), '--shards', '2']
    run = invoke('matrix', *common, '--unknown-seconds', '12', '--runners', 'ubuntu-latest',
                 '--output', str(output))
    assert run.returncode == 0, run.stderr
    assert json.loads(output.read_text())['total_estimated_seconds'] == 12
    run = invoke('shard', *common, '--index', '2', '--output-files', str(tmp_path / 'bad.txt'))
    assert run.returncode != 0
    files.write_text('new.py\nnew.py\n')
    run = invoke('matrix', *common, '--runners', 'ubuntu-latest', '--output', str(output))
    assert run.returncode != 0 and 'duplicate' in run.stderr.lower()
