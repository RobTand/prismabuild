"""Environment qualification retains immutable PB node selections."""
import copy

import pytest

from prismabuild import pbtest_collection
from test_pbmergeq import mq
from test_pbmergeq_environment_regression import native_environment


FILE = 'tests/test_a.py'
NODE = FILE + '::broken'
GROUPS = {FILE: {'index': 0, 'count': 2, 'durations': {NODE: 2.0}}}


def test_shard_arguments_use_the_collection_owners_canonical_encoding():
    pbtest = mq.load_published_module(mq.Path(mq.__file__).with_name('pbtest.py'))
    spec = {'files': [FILE], 'roots': [['tests', 'directory']], 'node_groups': GROUPS}
    arguments = pbtest.shard_arguments(spec, ['--durations=3'], 0,
        explicit_options=['-o', 'addopts='], pytest_workers=['-n', '2'])
    assert arguments == [pbtest_collection.encode_selection(spec), '-q', '--no-header',
        '-p', 'no:cacheprovider', '-o', 'addopts=', '--durations=3', '-n', '2', FILE]


def test_native_compressed_node_selection_qualifies_and_retains_requested_nodes(tmp_path, monkeypatch):
    native = native_environment(tmp_path, monkeypatch, node_groups=copy.deepcopy(GROUPS))
    observations, errors = native.runner.observe_environments(
        native.rows, native.runtime, checkout=native.checkout, files=native.files)
    assert errors == []
    observation = observations[FILE]
    assert observation['action_key'] == native.key
    assert observation['selection'] == {'files': [FILE], 'roots': [[FILE, 'file']],
                                        'node_groups': GROUPS}
    assert observation['collected'] == [NODE]


@pytest.mark.parametrize('damage', ['group', 'encoding'])
def test_a_mismatched_or_corrupt_node_selection_is_inconclusive(tmp_path, monkeypatch, damage):
    native = native_environment(tmp_path, monkeypatch, node_groups=copy.deepcopy(GROUPS))
    if damage == 'group':
        native.rows[0]['node_groups'][FILE]['index'] = 1
    else:
        # Fault the temporary immutable request after proving the real parser accepted it.
        request = native.cas.root / 'requests' / native.key[:2] / (native.key + '.json')
        request.chmod(0o644)
        request.write_text('corrupt sealed request')
    observations, errors = native.runner.observe_environments(
        native.rows, native.runtime, checkout=native.checkout, files=native.files)
    assert observations == {}
    assert len(errors) == 1


@pytest.mark.parametrize('damage', [None, 'worker', 'duplicate', 'plan', 'refused'])
def test_grouped_file_attribution_requires_every_native_environment(damage):
    from prismabuild.pbmergeq_attribution import file_environments

    parts = [{'identity': 'worker-a', 'action_key': str(index),
              'selection': {'files': [FILE], 'roots': [['tests', 'directory']],
                            'node_groups': {FILE: {**GROUPS[FILE], 'index': index}}},
              'collected': [FILE + '::test_' + str(index)]} for index in range(2)]
    if damage == 'worker':
        parts[1]['identity'] = 'worker-b'
    elif damage == 'duplicate':
        parts[1]['selection']['node_groups'][FILE]['index'] = 0
    elif damage == 'plan':
        parts[1]['selection']['node_groups'][FILE]['durations'] = {NODE: 3.0}
    observations, errors = file_environments({FILE: parts}, {FILE} if damage == 'refused' else set())
    if damage is None:
        assert errors == []
        assert observations[FILE]['parts'] == parts
        assert [part['action_key'] for part in observations[FILE]['parts']] == ['0', '1']
    else:
        assert observations == {}
        assert bool(errors) == (damage != 'refused')
