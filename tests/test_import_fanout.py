"""PrismaBuild's two import restrictions use Bazel library dependency edges."""
import importlib.util
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location('import_fanout', ROOT / 'tools/ci/import_fanout.py')
fanout = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(fanout)


def test_committed_baseline_has_no_stored_edge_lists():
    baseline = json.loads((ROOT / 'tests/fixtures/import_fanout_baseline.json').read_text())
    assert 'test_to_test_imports' not in baseline
    assert 'conftest_source_imports' not in baseline

    def check(value, path):
        assert not isinstance(value, list), f'baseline must not store lists at {path}'
        if isinstance(value, dict):
            for key, child in value.items():
                check(child, f'{path}.{key}')

    check(baseline, 'baseline')


def test_bazel_library_edges_preserve_the_two_import_rules():
    edges = {'//tests:a_lib': {'//tests:a.py', '//tests:b_lib'},
             '//tests:b_lib': {'//tests:test_b.py'},
             '//tests:conftest': {'//tests:conftest.py', '//src/prismabuild:core'},
             '//src/prismabuild:core': {'//src/prismabuild:core.py'},
             '//tests:test_a': {'//tests:a_lib', '//tests:b_lib'}}
    owners = {'//tests:a_lib': {'//tests:a.py'}, '//tests:b_lib': {'//tests:test_b.py'},
              '//tests:conftest': {'//tests:conftest.py'},
              '//src/prismabuild:core': {'//src/prismabuild:core.py'}}
    imports = fanout.bazel_imports(edges, owners)
    assert imports == {'test_to_test_imports': ['tests/a.py -> tests/test_b.py'],
                       'conftest_source_imports': ['tests/conftest.py -> src/prismabuild/core.py']}
    before = {key: [] for key in imports}
    errors = fanout.bazel_regressions(imports, before)
    assert errors == ['new conftest_source_imports entry: tests/conftest.py -> src/prismabuild/core.py',
                      'new test_to_test_imports entry: tests/a.py -> tests/test_b.py']


def test_bazel_data_edges_and_test_target_ownership_are_not_imports():
    edges = {'//tests:a_lib': {'//tests:test_b.py'},
             '//tests:test_a': {'//tests:a_lib', '//tests:b_lib'},
             '//tests:b_lib': {'//tests:test_b.py'}}
    assert fanout.bazel_imports(edges, {'//tests:a_lib': {'//tests:test_a.py'},
                                       '//tests:b_lib': {'//tests:test_b.py'}}) == {
        'test_to_test_imports': [], 'conftest_source_imports': []}


def test_conftest_and_test_import_removals_do_not_fail():
    old = {'conftest_source_imports': ['conftest.py -> tools/a.py'],
           'test_to_test_imports': ['tests/a.py -> tests/test_b.py']}
    assert fanout.bazel_regressions({key: [] for key in old}, old) == []


def test_a_cycle_library_preserves_internal_test_imports():
    imports = fanout.bazel_imports({'//tests:cycle': set()},
                                   {'//tests:cycle': {'//tests:test_a.py', '//tests:test_b.py'}})
    assert imports['test_to_test_imports'] == ['tests/test_a.py -> tests/test_b.py',
                                              'tests/test_b.py -> tests/test_a.py']


def test_compatibility_imports_do_not_load_gate_control_code(monkeypatch):
    import builtins
    import sys
    monkeypatch.delitem(sys.modules, 'tools.ci.bazel_imports', raising=False)
    original_import = builtins.__import__
    def import_module(name, *args, **kwargs):
        if name.startswith('tools.ci.') and name != 'tools.ci.bazel_imports':
            raise AssertionError('pure import rules must not load gate control/configuration code')
        return original_import(name, *args, **kwargs)
    monkeypatch.setattr(builtins, '__import__', import_module)
    spec = importlib.util.spec_from_file_location('isolated_import_rules', ROOT / 'tools/ci/import_fanout.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert module.bazel_imports({'//tests:a': set()}, {}) == {
        'test_to_test_imports': [], 'conftest_source_imports': []}
