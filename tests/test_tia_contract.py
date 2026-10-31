"""Function impact contracts, including fixture and child-process attribution."""
from __future__ import annotations

import gzip
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]


def load(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / 'tools/ci' / f'{name}.py')
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def test_diff_ignores_comments_and_formatting():
    diff = load('tia_diff').compare('def f(x):\n return x + 1\n',
                                    '# comment\ndef f( x ):\n    return x+1\n')
    assert diff['changed'] == []
    assert diff['module_changed'] is False


def test_diff_nested_decorator_signature_and_rename():
    before = 'X = 1\nclass C:\n @wrap\n def f(self, x=1):\n  def inner():\n   return 1\n  return inner()\ndef old():\n return 3\n'
    after = before.replace('x=1', 'x=2').replace('return 1', 'return 2').replace('old()', 'new()')
    diff = load('tia_diff').compare(before, after)
    assert diff['changed'] == ['C.f', 'C.f.<locals>.inner']
    assert diff['added'] == ['new']
    assert diff['deleted'] == ['old']
    assert diff['renamed'] == [{'before': 'old', 'after': 'new'}]
    assert diff['module_changed'] is True


@pytest.mark.parametrize('before,after', [
    ('X = 1\ndef f():\n return 1\n', 'X = 2\ndef f():\n return 1\n'),
    ('class C(A):\n def f(self):\n  pass\n', 'class C(B):\n def f(self):\n  pass\n'),
    ('def f():\n pass\n', '@decorate\ndef f():\n pass\n'),
])
def test_definition_time_changes_use_module_fallback(before, after):
    assert load('tia_diff').compare(before, after)['module_changed'] is True


def test_map_reproducible_and_rejects_mixed_sources(tmp_path):
    mod = load('tia_map')
    record = {'schema': 'prismabuild.tia_record.v1', 'source_commit': 'a' * 40,
              'python': '3.12.8', 'files': {'src/a.py': 'abc'},
              'tests': {'tests/test_a.py::test_a': ['src/a.py::f', 'src/a.py::f']}}
    source = tmp_path / 'record.json'
    source.write_text(json.dumps(record))
    first, second = tmp_path / 'one.gz', tmp_path / 'two.gz'
    mod.write_map([source], first)
    mod.write_map([source], second)
    assert first.read_bytes() == second.read_bytes()
    result = json.loads(gzip.decompress(first.read_bytes()))
    assert result['functions'] == ['src/a.py::f']
    assert result['tests'] == {'tests/test_a.py::test_a': [0]}
    record['source_commit'] = 'b' * 40
    other = tmp_path / 'other.json'
    other.write_text(json.dumps(record))
    with pytest.raises(ValueError, match='source'):
        mod.write_map([source, other], second)


def selection(tmp_path, *, changed=('f',), module=False, hashes=None, mapped=True):
    mod = load('tia_select')
    path = 'src/a.py'
    mapping = {'schema': 'prismabuild.tia_map.v1', 'source_commit': 'a' * 40,
               'python': ['3.12.8'], 'files': {path: 'abc'},
               'functions': [path + '::f'] if mapped else [],
               'tests': {'tests/test_a.py::test_f': [0]} if mapped else {}}
    diff = {'python': {path: {'changed': list(changed), 'added': [], 'deleted': [],
                            'module_changed': module}}, 'non_python': []}
    calls = []
    def static(paths):
        calls.append(paths)
        return {'selected': [{'path': 'tests/test_static.py', 'reasons': ['direct import']} ]}
    plan = mod.select(mapping, diff, file_hashes=hashes or {path: 'abc'},
                      fast=['tests/test_fast.py'], changed_tests=['tests/test_new.py'],
                      population=['tests/test_a.py', 'tests/test_static.py', 'tests/test_fast.py', 'tests/test_new.py'],
                      static=static)
    return plan, calls


def test_selection_function_and_mandatory_tiers(tmp_path):
    plan, calls = selection(tmp_path)
    assert [row['path'] for row in plan['selected']] == ['tests/test_a.py', 'tests/test_fast.py', 'tests/test_new.py']
    assert calls == []
    assert plan['targets'] == ['tests/test_a.py::test_f', 'tests/test_fast.py', 'tests/test_new.py']
    assert all(row['reasons'] for row in plan['selected'])


@pytest.mark.parametrize('kwargs,reason', [
    ({'changed': ('new',)}, 'unmapped function'),
    ({'hashes': {'src/a.py': 'different'}}, 'stale source'),
    ({'module': True}, 'module change'),
])
def test_selection_conservative_fallbacks(tmp_path, kwargs, reason):
    plan, calls = selection(tmp_path, **kwargs)
    assert calls == [['src/a.py']]
    assert 'tests/test_static.py' in [row['path'] for row in plan['selected']]
    assert any(reason in why for row in plan['selected'] for why in row['reasons'])


def test_recorder_real_pytest_shared_fixture_and_child(tmp_path):
    root = tmp_path / 'repo'
    (root / 'src').mkdir(parents=True)
    (root / 'tests').mkdir()
    (root / 'src/example.py').write_text('def fixture_work():\n return 4\ndef direct():\n return 7\ndef child():\n return 9\n')
    (root / 'tests/test_example.py').write_text('''import subprocess, sys
import pytest
from example import fixture_work, direct
@pytest.fixture(scope="session")
def shared():
 return fixture_work()
def test_one(shared):
 assert shared == 4
 assert direct() == 7
 subprocess.run([sys.executable, '-c', 'from example import child; assert child() == 9'], check=True)
def test_two(shared):
 assert shared == 4
 assert direct() == 7
''')
    output = tmp_path / 'records'
    env = dict(os.environ, PYTHONPATH=os.pathsep.join([str(ROOT / 'tools/ci'), str(root / 'src')]),
               PB_TIA_ROOT=str(root), PB_TIA_OUTPUT=str(output), PB_TIA_COMMIT='a' * 40,
               PYTEST_DISABLE_PLUGIN_AUTOLOAD='1', PYTEST_PLUGINS='')
    result = subprocess.run([sys.executable, '-m', 'pytest', '-p', 'tia_record', '-q', '--rootdir=' + str(root), str(root / 'tests')],
                            cwd=root, env=env, text=True, capture_output=True, timeout=120)
    assert result.returncode == 0, result.stdout + result.stderr
    records = [json.loads(path.read_text()) for path in output.glob('*.json')]
    tests = {}
    for record in records:
        for node, functions in record['tests'].items():
            tests.setdefault(node, set()).update(functions)
    assert tests['tests/test_example.py::test_one'] >= {'src/example.py::fixture_work', 'src/example.py::direct', 'src/example.py::child'}
    assert tests['tests/test_example.py::test_two'] >= {'src/example.py::fixture_work', 'src/example.py::direct'}
    assert not any('<module>' in function for values in tests.values() for function in values)
