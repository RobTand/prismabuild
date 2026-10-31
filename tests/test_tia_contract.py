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
sys.path.insert(0, str(ROOT / 'tools/ci'))


def load_tia_module(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / 'tools/ci' / f'{name}.py')
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def test_diff_ignores_comments_and_formatting():
    diff = load_tia_module('tia_diff').compare_functions('def f(x):\n return x + 1\n',
                                    '# comment\ndef f( x ):\n    return x+1\n')
    assert diff['changed'] == []
    assert diff['module_changed'] is False


def test_diff_nested_decorator_signature_and_rename():
    before = 'X = 1\nclass C:\n @wrap\n def f(self, x=1):\n  def inner():\n   return 1\n  return inner()\ndef old():\n return 3\n'
    after = before.replace('x=1', 'x=2').replace('return 1', 'return 2').replace('old()', 'new()')
    diff = load_tia_module('tia_diff').compare_functions(before, after)
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
    assert load_tia_module('tia_diff').compare_functions(before, after)['module_changed'] is True


def test_map_reproducible_and_rejects_mixed_sources(tmp_path):
    mod = load_tia_module('tia_map')
    record = {'schema': 'prismabuild.tia_record.v1', 'source_commit': 'a' * 40,
              'python': '3.12.8', 'unattributed': [], 'files': {'src/a.py': 'abc'},
              'tests': {'tests/test_a.py::test_a': ['src/a.py::f', 'src/a.py::f']}}
    source = tmp_path / 'record.json'
    source.write_text(json.dumps(record))
    first, second = tmp_path / 'one.gz', tmp_path / 'two.gz'
    mod.write_impact_map([source], first)
    mod.write_impact_map([source], second)
    assert first.read_bytes() == second.read_bytes()
    result = json.loads(gzip.decompress(first.read_bytes()))
    assert result['functions'] == ['src/a.py::f']
    assert result['tests'] == {'tests/test_a.py::test_a': [0]}
    record['source_commit'] = 'b' * 40
    other = tmp_path / 'other.json'
    other.write_text(json.dumps(record))
    with pytest.raises(ValueError, match='source'):
        mod.write_impact_map([source, other], second)


def selection(tmp_path, *, changed=('f',), module=False, hashes=None, mapped=True, unattributed=()):
    mod = load_tia_module('tia_select')
    path = 'src/a.py'
    mapping = {'schema': 'prismabuild.tia_map.v1', 'source_commit': 'a' * 40,
               'python': ['3.12.8'], 'files': {path: 'abc'},
               'functions': [path + '::f'] if mapped else [], 'unattributed': list(unattributed),
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
    (root / 'src/example.py').write_text('def import_work():\n return 11\nVALUE = import_work()\ndef fixture_work():\n return 4\ndef direct():\n return 7\ndef child():\n return 9\n')
    (root / 'tests/test_example.py').write_text('''import os, subprocess, sys
from pathlib import Path
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
def test_nested_pytest(tmp_path):
 nested = tmp_path / 'nested'
 nested.mkdir()
 (nested / 'test_child.py').write_text('from example import child\\n' + 'def test_ok(): assert child() == 9')
 env = dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[1] / 'src'))
 run = subprocess.run([sys.executable, '-m', 'pytest', '-q', str(nested)], cwd=nested, env=env, capture_output=True, text=True)
 assert run.returncode == 0, run.stdout + run.stderr
''')
    output = tmp_path / 'records'
    env = dict(os.environ, PYTHONPATH=os.pathsep.join([str(ROOT / 'tools/ci'), str(root / 'src')]),
               PB_TIA_ROOT=str(root), PB_TIA_OUTPUT=str(output), PB_TIA_COMMIT='a' * 40,
               PYTEST_DISABLE_PLUGIN_AUTOLOAD='1', PYTEST_PLUGINS='tia_record')
    env.pop('PB_TIA_NODE', None)
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
    assert 'src/example.py::import_work' in {function for record in records for function in record.get('unattributed', [])}
    assert not any('<module>' in function for values in tests.values() for function in values)


def test_non_python_uses_static_rule(tmp_path):
    mod = load_tia_module('tia_select')
    seen = []
    def static(paths):
        seen.extend(paths)
        return {'selected': [{'path': 'tests/test_data.py', 'reasons': ['literal path reference']}]}
    result = mod.select({'functions': [], 'tests': {}, 'files': {}, 'source_commit': 'a' * 40},
                        {'python': {}, 'non_python': ['data/schema.json']}, file_hashes={},
                        fast=[], changed_tests=[], population=['tests/test_data.py'], static=static)
    assert seen == ['data/schema.json']
    assert result['targets'] == ['tests/test_data.py']
    assert 'non-Python path rule' in result['selected'][0]['reasons'][0]


def test_stale_source_keeps_dynamic_import_consumers(tmp_path):
    result, _ = selection(tmp_path, hashes={'src/a.py': 'new'})
    assert result['targets'] == ['tests/test_a.py', 'tests/test_fast.py', 'tests/test_new.py', 'tests/test_static.py']


def test_record_schema_rejects_out_of_range_function_indices(tmp_path):
    path = tmp_path / 'bad.gz'
    path.write_bytes(gzip.compress(json.dumps({'schema': 'prismabuild.tia_map.v1',
                     'functions': ['src/a.py::f'], 'tests': {'tests/test_a.py::test_a': [1]}}).encode()))
    with pytest.raises(ValueError, match='indices'):
        load_tia_module('tia_map').read_impact_map(path)


def test_mutation_sample_preserves_bytes_and_function_identity(tmp_path):
    mod = load_tia_module('tia_evaluate')
    directory = tmp_path / 'src/prismabuild'
    directory.mkdir(parents=True)
    path = directory / 'example.py'
    path.write_text('# é\ndef choose(x):\n    if x == 3:\n        return 7\n    return 9\n')
    mapping = {'files': {'src/prismabuild/example.py': mod.raw_sha256(path.read_bytes())},
               'functions': ['src/prismabuild/example.py::choose']}
    rows = list(mod.mutations(tmp_path, mapping))
    assert len(rows) == 1
    assert rows[0]['function'] == 'choose'
    assert rows[0]['kind'] == 'condition'
    assert rows[0]['replacement'] == 'if not x == 3:\n        return 7'
    assert rows[0]['diff']['changed'] == ['choose']
    assert rows[0]['diff']['module_changed'] is False


def test_import_time_function_calls_require_static_fallback(tmp_path):
    result, calls = selection(tmp_path, unattributed=['src/a.py::f'])
    assert calls == [['src/a.py']]
    assert result['targets'] == ['tests/test_a.py', 'tests/test_fast.py', 'tests/test_new.py', 'tests/test_static.py']
    assert any('outside test' in why for row in result['selected'] for why in row['reasons'])


def test_recorder_does_not_retain_transient_code_objects(tmp_path):
    import gc
    import weakref
    mod = load_tia_module('tia_record')
    recorder = mod.Recorder(tmp_path, tmp_path / 'out', 'a' * 40)
    try:
        code = compile('value = 41', 'generated.py', 'exec')
        reference = weakref.ref(code)
        recorder.key(code)
        del code
        gc.collect()
        assert reference() is None
    finally:
        recorder.close()


def test_unmapped_test_retains_static_import_and_path_rules():
    mod = load_tia_module('tia_select')
    mapping = {'source_commit': 'a' * 40, 'files': {'src/a.py': 'abc'},
               'functions': ['src/a.py::f'], 'unattributed': [],
               'tests': {'tests/test_a.py::test_f': [0]},
               'unmapped': {'tests/test_archive.py': 'bubblewrap cannot mount /proc in admitted PrismaBuild actions; exit 78'}}
    def static(paths):
        return {'selected': ([{'path': 'tests/test_archive.py', 'reasons': ['direct import: src/a.py']}]
                             if 'src/a.py' in paths or 'data/golden.json' in paths else [])}
    args = {'file_hashes': {'src/a.py': 'abc'}, 'fast': [], 'changed_tests': [],
            'population': ['tests/test_a.py', 'tests/test_archive.py'], 'static': static}
    changes = {'python': {'src/a.py': {'changed': ['f'], 'added': [], 'deleted': [], 'module_changed': False}}, 'non_python': []}
    selected = mod.select(mapping, changes, **args)
    assert selected['targets'] == ['tests/test_a.py::test_f', 'tests/test_archive.py']
    assert any('unmapped test' in why for row in selected['selected'] for why in row['reasons'])
    unrelated = mod.select(mapping, {'python': {}, 'non_python': ['docs/readme.md']}, **args)
    assert unrelated['targets'] == []
    literal = mod.select(mapping, {'python': {}, 'non_python': ['data/golden.json']}, **args)
    assert literal['targets'] == ['tests/test_archive.py']


def test_child_reuses_parent_source_catalog(tmp_path, monkeypatch):
    mod = load_tia_module('tia_record')
    (tmp_path / 'src').mkdir()
    (tmp_path / 'src/example.py').write_text('def work(): return 1')
    catalog = tmp_path / 'catalog.json'
    catalog.write_text(json.dumps({'root': str(tmp_path), 'files': {
        str(tmp_path / 'src/example.py'): ['src/example.py', 'known-parent-hash']}}))
    monkeypatch.setenv('PB_TIA_CATALOG', str(catalog))
    def reject_walk(*args, **kwargs):
        raise AssertionError('child must reuse prehashed parent catalog')
    monkeypatch.setattr(Path, 'rglob', reject_walk)
    recorder = mod.Recorder(tmp_path, tmp_path / 'records', 'a' * 40)
    try:
        code = compile('def work(): return 1', str(tmp_path / 'src/example.py'), 'exec')
        function = next(value for value in code.co_consts if hasattr(value, 'co_qualname'))
        assert recorder.key(function) == 'src/example.py::work'
        assert recorder.files == {'src/example.py': 'known-parent-hash'}
    finally:
        recorder.close()


def test_source_readers_and_their_consumers_keep_static_coverage():
    mod = load_tia_module('tia_select')
    mapping = {'source_commit': 'a' * 40, 'files': {'src/a.py': 'abc'},
               'functions': ['src/a.py::f'], 'unattributed': [],
               'tests': {'tests/test_direct.py::test_f': [0], 'tests/test_reader.py::test_ast': [],
                         'tests/test_consumer.py::test_value': [], 'tests/test_other.py::test_unused': []}}
    def static(paths):
        return {'selected': [
            {'path': 'tests/test_reader.py', 'reasons': ['literal path reference: src/a.py']},
            {'path': 'tests/test_consumer.py', 'reasons': ['helper dependency: tests/test_reader.py']},
            {'path': 'tests/test_other.py', 'reasons': ['direct import: src/a.py']}]}
    result = mod.select(mapping, {'python': {'src/a.py': {'changed': ['f'], 'added': [], 'deleted': [],
                        'module_changed': False}}, 'non_python': []}, file_hashes={'src/a.py': 'abc'},
                        fast=[], changed_tests=[], population=['tests/test_direct.py', 'tests/test_reader.py',
                        'tests/test_consumer.py', 'tests/test_other.py'], static=static)
    assert result['targets'] == ['tests/test_consumer.py', 'tests/test_direct.py::test_f', 'tests/test_reader.py']
    assert any('source reader' in reason for row in result['selected'] for reason in row['reasons'])
