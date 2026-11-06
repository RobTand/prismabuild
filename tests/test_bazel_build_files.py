"""The BUILD generator turns the gate selector's evidence into a valid Bazel graph."""
from __future__ import annotations

import importlib.util
from pathlib import Path
import subprocess
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location('bazel_build_files', ROOT / 'tools/bazel/build_files.py')
build_files = importlib.util.module_from_spec(SPEC)
sys.modules['bazel_build_files'] = build_files
SPEC.loader.exec_module(build_files)
gate_select = build_files.import_from_path('gate_select_for_bazel_tests', ROOT / 'tools/ci/gate_select.py')


def evidence(tmp_path, sources):
    for path, text in sources.items():
        (tmp_path / path).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / path).write_text(text)
    return build_files.Evidence(tmp_path, sorted(sources), gate_select, gate_select.gate_paths)


def test_an_import_cycle_becomes_one_strongly_connected_component():
    edges = {'a': {'b'}, 'b': {'c'}, 'c': {'a'}, 'd': {'a'}}
    assert build_files.strongly_connected(['a', 'b', 'c', 'd'], edges) == [['a', 'b', 'c'], ['d']]


def test_import_statements_anywhere_are_edges_and_bare_strings_are_not(tmp_path):
    found = evidence(tmp_path, {
        'tests/test_a.py': ('import helper\n'
                            'def test_x():\n    from prismabuild import core\n'
                            "PATCHED = ('prismabuild.pool', 'pbrun')\n"),
        'tests/test_b.py': ("import importlib\nNAMES = ('prismabuild.pool', 'pbrun')\n"
                            'MODULES = [importlib.import_module(name) for name in NAMES]\n'),
        'tests/helper.py': '', 'src/prismabuild/core.py': '', 'src/prismabuild/pool.py': '',
        'tools/fleet/pbrun.py': ''})
    modules, _ = found.of('tests/test_a.py')
    assert modules == {'tests/helper.py', 'src/prismabuild/core.py'}
    assert found.test_modules('tests/test_a.py') == set()
    # A test that imports by name gets the modules it names, on its own target only.
    assert found.of('tests/test_b.py')[0] == set()
    assert found.test_modules('tests/test_b.py') == {'src/prismabuild/pool.py', 'tools/fleet/pbrun.py'}


def test_a_script_run_by_path_brings_its_imports_to_the_test(tmp_path):
    found = evidence(tmp_path, {
        'tests/test_shim.py': ("from pathlib import Path\n"
                               "SHIM = Path(__file__).resolve().parents[1] / 'tools/fleet/docker'\n"),
        'tools/fleet/docker': '#!/usr/bin/python3\nfrom prismabuild import resource_scope\n',
        'tools/worker.py': ("import subprocess, sys\nfrom pathlib import Path\n"
                            "EXEC = Path(__file__).resolve().parent / 'fleet/resource_exec.py'\n"
                            "def run():\n    subprocess.run([sys.executable, str(EXEC)], check=True)\n"),
        'tools/named.py': ("from pathlib import Path\n"
                           "EXEC = Path(__file__).resolve().parent / 'fleet/resource_exec.py'\n"
                           "print('would run', EXEC)\n"),
        'tools/fleet/resource_exec.py': 'from prismabuild import pool\n',
        'src/prismabuild/resource_scope.py': '', 'src/prismabuild/pool.py': ''})
    assert found.run_by_path({'tests/test_shim.py'}) == {'tools/fleet/docker', 'src/prismabuild/resource_scope.py'}
    # A path that reaches a launch counts; the same path only named does not.
    assert found.run_by_path({'tools/worker.py'}) == {'tools/fleet/resource_exec.py'}
    assert found.run_by_path({'tools/named.py'}) == set()
    assert found.of('tools/worker.py')[0] == set()


def test_bare_file_names_count_only_when_a_path_is_built_from_them():
    tree = build_files.ast.parse(
        "SCRIPTS = ('worker_loop.py', 'tier_loop.py')\n"
        "DEPENDENCIES = ('resource_broker.py',)\n"
        "def busy(name):\n    return name in SCRIPTS\n"
        "def copy(source, target):\n    for dependency in DEPENDENCIES:\n"
        "        (target / dependency).write_bytes((source / dependency).read_bytes())\n")
    assert build_files.path_built_names(tree) == {'resource_broker.py'}


def test_a_test_importing_another_test_depends_on_its_library_not_its_test():
    assert build_files.library_name('tests/test_b.py') == 'test_b_lib'
    assert build_files.library_name('tests/helper.py') == 'helper'


def test_snippets_dynamic_imports_and_pytest_plugins_are_edges(tmp_path):
    found = evidence(tmp_path, {
        'tests/test_a.py': ('import importlib, textwrap\n'
                            'CHILD = textwrap.dedent("""\n    from prismabuild import pool\n    pool.run()\n""")\n'
                            "SCRIPT = f'from queue_host import install; install({1!r})'\n"
                            "importlib.import_module('prismabuild.core')\n"),
        'conftest.py': 'pytest_plugins = ("prismabuild.bound",)\n',
        'tests/queue_host.py': '', 'src/prismabuild/pool.py': '', 'src/prismabuild/core.py': '',
        'src/prismabuild/bound.py': ''})
    assert found.of('tests/test_a.py')[0] == {'src/prismabuild/pool.py', 'src/prismabuild/core.py',
                                             'tests/queue_host.py'}
    assert found.of('conftest.py')[0] == {'src/prismabuild/bound.py'}


def test_paths_to_tools_are_edges_and_paths_to_tests_or_globs_are_data(tmp_path):
    found = evidence(tmp_path, {
        'tests/test_a.py': ('from pathlib import Path\nROOT = Path(__file__).resolve().parents[1]\n'
                            "TOOL = ROOT / 'tools/fleet/pbrun.py'\nOTHER = ROOT / 'tests/test_b.py'\n"
                            "FIXTURES = sorted((ROOT / 'tests/fixtures').glob('*.json'))\n"),
        'tests/test_b.py': '', 'tools/fleet/pbrun.py': '',
        'tests/fixtures/one.json': '', 'tests/fixtures/two.json': ''})
    modules, data = found.of('tests/test_a.py')
    assert modules == {'tools/fleet/pbrun.py'}
    assert {'tests/test_b.py', 'tests/fixtures/one.json', 'tests/fixtures/two.json'} <= data


def test_a_nested_tool_module_resolves_from_a_sys_path_root(tmp_path):
    found = evidence(tmp_path, {'tests/test_a.py': 'from pbcanary_legs import leg3\n',
                                'tools/fleet/pbcanary_legs/__init__.py': '', 'tools/fleet/pbcanary_legs/leg3.py': ''})
    assert found.of('tests/test_a.py')[0] == {'tools/fleet/pbcanary_legs/__init__.py',
                                             'tools/fleet/pbcanary_legs/leg3.py'}


def test_production_tree_reads_reach_only_tests_that_import_the_module(tmp_path):
    found = evidence(tmp_path, {
        'tools/scan.py': ('from pathlib import Path\nROOT = Path(__file__).resolve().parents[1]\n'
                          "TABLE = Path(__file__).with_name('table.json')\n"
                          "FILES = sorted((ROOT / 'docs').rglob('*.md'))\n"),
        'tools/table.json': '{}', 'docs/a.md': '', 'docs/b.md': '',
        'tests/test_scan.py': 'import scan\n', 'tests/helper.py': 'import scan\n'})
    modules, data = found.of('tools/scan.py')
    assert data - {'tools/scan.py'} == {'tools/table.json'}
    assert found.test_data('tests/test_scan.py', found.of('tests/test_scan.py')[0]) >= {'docs/a.md', 'docs/b.md'}


def test_conftest_contributes_imports_and_exact_paths_only(tmp_path):
    found = evidence(tmp_path, {
        'tests/conftest.py': ('from pathlib import Path\nimport helper\n'
                              'def pytest_terminal_summary(terminalreporter):\n'
                              '    root = Path(__file__).resolve().parents[1]\n'
                              '    for name in terminalreporter.files:\n        (root / name).read_bytes()\n'),
        'tests/helper.py': '', 'docs/a.md': ''})
    modules, data = found.of('tests/conftest.py')
    assert modules == {'tests/helper.py'} and data <= {'tests/conftest.py'}


def test_whole_directories_compress_to_one_filegroup_and_the_rest_stay_files():
    files = ['tests/fixtures/a.json', 'tests/fixtures/b.json', 'tests/test_x.py', 'docs/one.md', 'docs/two.md']
    trees = build_files.Trees(files)
    labels = trees.labels({'tests/fixtures/a.json', 'tests/fixtures/b.json', 'docs/one.md'}, 'tests')
    assert labels == ['//:docs/one.md', ':files/fixtures']
    assert trees.used == {'tests': {'fixtures'}}


PLAN = {'selected': [{'path': 'tests/test_in.py'}, {'path': 'tests/test_slow.py'}],
        'deselect': ['tests/test_in.py::test_flaky'],
        'deferred': [{'path': 'tests/test_root.py', 'lane': 'root'},
                     {'path': 'tests/test_archival.py', 'issue': 1997, 'kind': 'environment_lanes'}]}


def test_population_files_keep_their_deselects_and_others_are_manual_with_owner():
    attributes = build_files.test_attributes('tests/test_in.py', PLAN, {})
    assert attributes['args'] == ['--deselect=tests/test_in.py::test_flaky']
    assert 'tags' not in attributes
    assert build_files.test_attributes('tests/test_root.py', PLAN, {})['tags'] == ['gate-lane-root', 'manual']
    assert build_files.test_attributes('tests/test_archival.py', PLAN, {})['tags'] == [
        'gate-issue-1997', 'manual']


def test_a_test_file_outside_the_plan_refuses_generation():
    with pytest.raises(build_files.GenerationError, match='tests/test_unknown.py'):
        build_files.test_attributes('tests/test_unknown.py', PLAN, {})


def test_long_files_are_sharded_and_given_a_long_timeout_only_when_needed():
    slow = build_files.test_attributes('tests/test_slow.py', PLAN, {'tests/test_slow.py': 170.2})
    assert slow['shard_count'] == 5
    assert slow['timeout'] == 'moderate'
    quick = build_files.test_attributes('tests/test_in.py', PLAN, {'tests/test_in.py': 3.0})
    assert 'shard_count' not in quick and quick['timeout'] == 'moderate'


def test_two_targets_with_one_name_refuse_generation():
    with pytest.raises(build_files.GenerationError, match='x_lib'):
        build_files.render_package('tests', [('x_lib', 'pb_py_library', {}), ('x_lib', 'pb_py_library', {})])


def checkout(root, files, tracked):
    """A Git checkout holding these files, with only the tracked ones in the index."""
    subprocess.run(['git', 'init', '-q', str(root)], check=True)
    for path, text in files.items():
        (root / path).parent.mkdir(parents=True, exist_ok=True)
        (root / path).write_text(text)
    if tracked:
        subprocess.run(['git', 'add', '--', *tracked], cwd=root, check=True)


GENERATED = {'tests': build_files.HEADER + 'tests\n', '': build_files.HEADER + 'root\n'}


def check(root, monkeypatch):
    monkeypatch.setattr(build_files, 'generate', lambda root: dict(GENERATED))
    with pytest.raises(SystemExit) as stopped:
        build_files.main(['--check', '--root', str(root)])
    return str(stopped.value)


def test_check_names_each_stale_build_file_and_the_fix(tmp_path, monkeypatch, capsys):
    checkout(tmp_path, {'tests/BUILD.bazel': 'old\n'}, ['tests/BUILD.bazel'])
    monkeypatch.setattr(build_files, 'generate', lambda root: {'tests': 'new\n', '': 'root\n'})
    with pytest.raises(SystemExit) as stopped:
        build_files.main(['--check', '--root', str(tmp_path)])
    assert 'BUILD.bazel' in str(stopped.value) and 'tests/BUILD.bazel' in str(stopped.value)
    assert 'tools/bazel/generate.sh' in str(stopped.value)
    assert (tmp_path / 'tests/BUILD.bazel').read_text() == 'old\n'


def test_check_passes_when_every_generated_file_is_tracked_and_current(tmp_path, monkeypatch):
    files = {'tests/BUILD.bazel': GENERATED['tests'], 'BUILD.bazel': GENERATED[''],
             'tools/bazel/BUILD.bazel': 'handwritten\n', 'vendor/BUILD.bazel': 'handwritten elsewhere\n'}
    checkout(tmp_path, files, ['tests/BUILD.bazel', 'BUILD.bazel', 'tools/bazel/BUILD.bazel'])
    monkeypatch.setattr(build_files, 'generate', lambda root: dict(GENERATED))
    assert build_files.main(['--check', '--root', str(tmp_path)]) == 0


def test_check_fails_on_a_committed_build_file_the_generator_no_longer_writes(tmp_path, monkeypatch):
    files = {'tests/BUILD.bazel': GENERATED['tests'], 'BUILD.bazel': GENERATED[''],
             'tests/retired/BUILD.bazel': build_files.HEADER + 'retired\n'}
    checkout(tmp_path, files, list(files))
    message = check(tmp_path, monkeypatch)
    assert 'no longer generated: tests/retired/BUILD.bazel' in message and build_files.FIX in message


def test_check_fails_on_a_current_generated_build_file_git_does_not_track(tmp_path, monkeypatch):
    checkout(tmp_path, {'tests/BUILD.bazel': GENERATED['tests'], 'BUILD.bazel': GENERATED['']}, ['BUILD.bazel'])
    message = check(tmp_path, monkeypatch)
    assert 'not tracked by git: tests/BUILD.bazel' in message and 'stale' not in message


def test_generation_removes_build_files_it_no_longer_writes(tmp_path, monkeypatch):
    files = {'tests/retired/BUILD.bazel': build_files.HEADER + 'retired\n', 'tools/bazel/BUILD.bazel': 'handwritten\n'}
    checkout(tmp_path, files, list(files))
    monkeypatch.setattr(build_files, 'generate', lambda root: dict(GENERATED))
    assert build_files.main(['--root', str(tmp_path)]) == 0
    assert not (tmp_path / 'tests/retired/BUILD.bazel').exists()
    assert (tmp_path / 'tools/bazel/BUILD.bazel').read_text() == 'handwritten\n'
    assert (tmp_path / 'tests/BUILD.bazel').read_text() == GENERATED['tests']


def test_committed_build_files_carry_the_generated_header():
    for path in [ROOT / 'BUILD.bazel', ROOT / 'src/prismabuild/BUILD.bazel', ROOT / 'tools/BUILD.bazel',
                 ROOT / 'tests/BUILD.bazel']:
        assert path.read_text().startswith(build_files.HEADER), path
