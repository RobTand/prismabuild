"""CPU coverage and trustworthy verdicts for the off-queue suite."""
import importlib.util
from pathlib import Path
import xml.etree.ElementTree as ET

import pytest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('full_suite_shard', ROOT / 'tools/ci/full_suite_shard.py')
shard = importlib.util.module_from_spec(spec)
spec.loader.exec_module(shard)


def test_duration_partition_is_order_independent_and_covers_once():
    files = ['d.py', 'c.py', 'b.py', 'a.py', 'unknown.py']
    durations = {'a.py': 9, 'b.py': 8, 'c.py': 3, 'd.py': 2}
    groups = shard.partition(files, durations, 2)
    assert groups == [['a.py', 'd.py', 'unknown.py'], ['b.py', 'c.py']]
    assert shard.partition(list(reversed(files)), durations, 2) == groups
    assert sorted(f for group in groups for f in group) == sorted(files)


def test_selected_files_exclude_only_authoritative_root_and_gpu_populations():
    import runpy
    lanes = runpy.run_path(str(ROOT / 'tools/verify.py'))['populations'](ROOT)
    selected = shard.selected_files(ROOT)
    assert set(selected) == set(lanes['fast'] + lanes['integration'] + lanes['two_cpu'])
    assert not (set(selected) & set(lanes['root'] + lanes['gpu']))
    assert 'tests/test_full_suite_shard.py' in selected


def test_invalid_shard_count_refused():
    with pytest.raises(ValueError, match='positive'):
        shard.partition(['a.py'], {}, 0)


def test_junit_retains_exact_parameterized_nodeid_and_failure(tmp_path):
    xml = tmp_path / 'junit.xml'
    xml.write_text('''<testsuites><testsuite tests="2" failures="1">
      <testcase classname="tests.test_a.TestA" name="test_x[a.b]" time="1.25">
        <properties><property name="nodeid" value="tests/test_a.py::TestA::test_x[a.b]"/></properties>
        <failure message="wrong">assert 1 == 2</failure>
      </testcase><testcase classname="tests.test_a" name="test_ok" time="0.5">
        <properties><property name="nodeid" value="tests/test_a.py::test_ok"/></properties>
      </testcase></testsuite></testsuites>''')
    rows = shard.read_junit(xml)
    assert rows['tests/test_a.py::TestA::test_x[a.b]'] == {
        'outcome': 'failed', 'duration': 1.25, 'failure': 'assert 1 == 2'}
    assert rows['tests/test_a.py::test_ok']['outcome'] == 'passed'


@pytest.mark.parametrize('code,rows,expected', [
    (0, {'x': {'outcome': 'passed'}}, True),
    (1, {'x': {'outcome': 'failed'}}, True),
    (2, {'x': {'outcome': 'failed'}}, True),
    (3, {'x': {'outcome': 'passed'}}, False), (0, {}, False), (5, {}, False),
])
def test_retry_only_without_test_verdict(code, rows, expected):
    assert shard.has_verdict(code, rows) is expected


def test_real_pytest_plugin_preserves_parameter_id_under_xdist(tmp_path):
    import os
    import subprocess
    import sys
    (tmp_path / 'test_example.py').write_text('''import pytest
@pytest.mark.parametrize('value', ['a.b', 'c::d'])
def test_value(value):
    assert value == 'a.b'
''')
    xml = tmp_path / 'junit.xml'
    env = dict(os.environ, PYTHONPATH=str(ROOT / 'tools/ci'), PYTEST_DISABLE_PLUGIN_AUTOLOAD='1')
    done = subprocess.run([sys.executable, '-m', 'pytest', '-p', 'xdist.plugin', '-p',
        'full_suite_shard', '-n', '2', '--junitxml', str(xml), str(tmp_path / 'test_example.py')],
        cwd=tmp_path, env=env, capture_output=True, text=True, timeout=60)
    assert done.returncode == 1, done.stdout + done.stderr
    rows = shard.read_junit(xml)
    assert set(rows) == {'test_example.py::test_value[a.b]', 'test_example.py::test_value[c::d]'}
    assert rows['test_example.py::test_value[a.b]']['outcome'] == 'passed'
    assert rows['test_example.py::test_value[c::d]']['outcome'] == 'failed'


def test_infrastructure_retry_does_not_rerun_a_failed_test(tmp_path, monkeypatch):
    import json
    previous = tmp_path / 'previous'
    previous.mkdir()
    (previous / 'result.json').write_text(json.dumps({'shard': 1, 'verdict': True,
        'tests': {'x': {'outcome': 'failed'}}}))
    def unexpected(*args, **kwargs):
        raise AssertionError('pytest ran again after a test verdict')
    monkeypatch.setattr(shard.subprocess, 'run', unexpected)
    assert shard.main(['--shard', '1', '--previous', str(previous),
                       '--output', str(tmp_path / 'output')]) == 0
    assert not (tmp_path / 'output').exists()
