"""The merge gate explains coverage and never silently drops relevant tests."""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import subprocess
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location('gate_select', ROOT / 'tools/ci/gate_select.py')
select = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(select)


def tree(root):
    sources = {
        'tools/verify.py': "FAST = ('tests/test_fast.py',)\nGPU = ('tests/test_gpu.py',)\nTWO_CPU = ('tests/test_pair.py',)\n",
        'tests/proof_execution.json': json.dumps({'root': {'files': {'tests/test_root.py': {}}}}),
        'tests/receipt_execution.json': json.dumps({'root_files': {}}),
        'tests/test_fast.py': 'def test_fast(): assert 2 + 2 == 4\n',
        'tests/test_direct.py': 'from prismabuild import pool as owner\n',
        'tests/test_nested.py': 'import prismabuild.pool.capacity\n',
        'tests/test_indirect.py': 'from prismabuild import helper\n',
        'tests/test_tool.py': "import pbrun\n",
        'tests/test_dynamic.py': "import importlib\nm = importlib.import_module('pbrun')\n",
        'tests/test_path.py': "from pathlib import Path\np = Path('tools/fleet/pbrun.py')\n",
        'tests/test_parts.py': "from pathlib import Path\np = Path('tools') / 'fleet' / 'pbrun.py'\n",
        'tests/test_asset.py': "p = 'deploy/pool.json'\n",
        'tests/test_gpu.py': 'import prismabuild.pool\n',
        'tests/test_pair.py': 'import prismabuild.pool\n',
        'tests/test_root.py': 'import prismabuild.pool\n',
        'tests/support.py': 'VALUE = 1\n',
        'src/prismabuild/pool.py': 'VALUE = 1\n',
        'src/prismabuild/helper.py': 'from prismabuild import pool\n',
        'tools/fleet/pbrun.py': 'VALUE = 1\n',
    }
    for name, content in sources.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
    return root


def plan(root, changed, **kwargs):
    return select.create_plan(root, changed, durations={'source': 'fixture', 'files': {}},
                              quarantine={'entries': [], 'cost_exclusions': []}, **kwargs)


def selected(result):
    return {row['path'] for row in result['selected']}


def test_fast_plus_direct_importers_without_transitive_fanout(tmp_path):
    result = plan(tree(tmp_path), ['src/prismabuild/pool.py'])
    assert selected(result) == {'tests/test_fast.py', 'tests/test_direct.py',
                                'tests/test_nested.py'}
    reasons = {row['path']: row['reasons'] for row in result['selected']}
    assert 'fast tier' in reasons['tests/test_fast.py']
    assert any('src/prismabuild/pool.py' in reason for reason in reasons['tests/test_direct.py'])
    deferred = {row['path']: row for row in result['deferred']}
    assert deferred['tests/test_gpu.py']['lane'] == 'gpu'
    assert deferred['tests/test_pair.py']['lane'] == 'two_cpu'
    assert 'gpu' in deferred['tests/test_gpu.py']['reason'].lower()
    assert 'root' in deferred['tests/test_root.py']['reason'].lower()
    assert result['estimate']['wall_seconds'] is None
    assert result['estimate']['missing_files'] == sorted(selected(result))


def test_tools_loaded_by_import_name_or_path(tmp_path):
    result = plan(tree(tmp_path), ['tools/fleet/pbrun.py'])
    assert selected(result) == {'tests/test_fast.py', 'tests/test_tool.py',
                                'tests/test_dynamic.py', 'tests/test_path.py', 'tests/test_parts.py'}


def test_changed_tests_and_non_python_path_references(tmp_path):
    result = plan(tree(tmp_path), ['tests/test_indirect.py', 'deploy/pool.json', 'tests/test_deleted.py'])
    assert selected(result) == {'tests/test_fast.py', 'tests/test_indirect.py', 'tests/test_asset.py'}


@pytest.mark.parametrize('changed', ['conftest.py', 'tests/conftest.py', 'pyproject.toml',
                                    'tools/verify.py', 'tests/proof_execution.json',
                                    'tests/receipt_execution.json',
                                    'tools/ci/gate_select.py',
                                    'tests/test_gate_select.py'])
def test_shared_configuration_and_helpers_select_whole_population(tmp_path, changed):
    result = plan(tree(tmp_path), [changed])
    assert len(result['selected']) == result['population']['size'] == 9


def test_quarantine_and_cost_exclusions_are_owned_and_node_ids_stay_precise(tmp_path):
    root = tree(tmp_path)
    result = select.create_plan(root, ['src/prismabuild/pool.py'],
        durations={'source': 'fixture', 'files': {'tests/test_fast.py': 3, 'tests/test_direct.py': 7,
            'tests/test_nested.py': 2, 'tests/test_pair.py': 4}},
        quarantine={'entries': [
            {'target': 'tests/test_direct.py::test_bad[param]', 'reason': 'observed flake', 'issue': 9001},
            {'target': 'tests/test_nested.py', 'reason': 'broken at baseline', 'issue': 9001}],
            'cost_exclusions': [{'target': 'tests/test_pair.py', 'reason': 'measured cost', 'issue': 9001}]})
    assert selected(result) == {'tests/test_fast.py', 'tests/test_direct.py'}
    assert result['deselect'] == ['tests/test_direct.py::test_bad[param]']
    assert result['estimate']['wall_seconds'] == 7
    assert result['estimate']['serial_seconds'] == 10
    assert result['population']['version']
    assert {(row['path'], row['issue']) for row in result['deferred']} >= {
        ('tests/test_nested.py', 9001), ('tests/test_pair.py', 9001),
        ('tests/test_direct.py::test_bad[param]', 9001)}


def test_invalid_or_unowned_exclusion_refuses_instead_of_hiding_coverage(tmp_path):
    root = tree(tmp_path)
    for entry in ({'target': 'tests/test_direct.py', 'reason': 'broken'},
                  {'target': '../test_escape.py', 'reason': 'broken', 'issue': 9001}):
        with pytest.raises(ValueError):
            select.create_plan(root, [], durations={'files': {}},
                               quarantine={'entries': [entry], 'cost_exclusions': []})


def test_population_version_changes_when_policy_changes(tmp_path):
    root = tree(tmp_path)
    first = plan(root, [])['population']['version']
    (root / 'tests/test_future.py').write_text('def test_future(): assert 3 * 3 == 9\n')
    assert plan(root, [])['population']['version'] != first


def test_git_changes_include_rename_source_and_destination(tmp_path):
    root = tree(tmp_path)
    def git(*args):
        return subprocess.check_output(['git', *args], cwd=root, text=True).strip()
    git('init', '-q')
    git('add', '.')
    git('-c', 'user.name=Gate fixture', '-c', 'user.email=fixture@example.invalid',
        'commit', '-qm', 'base')
    base = git('rev-parse', 'HEAD')
    git('mv', 'src/prismabuild/pool.py', 'src/prismabuild/renamed.py')
    git('-c', 'user.name=Gate fixture', '-c', 'user.email=fixture@example.invalid',
        'commit', '-qam', 'rename')
    head = git('rev-parse', 'HEAD')
    assert select.changed_files(root, base, head) == ['src/prismabuild/pool.py', 'src/prismabuild/renamed.py']
    with pytest.raises(ValueError):
        select.changed_files(root, '--output=unsafe', head)


def test_root_relative_path_joins_select_file_loader_tests(tmp_path):
    root = tree(tmp_path)
    (root / 'tests/test_root_join.py').write_text(
        "from pathlib import Path\nROOT = Path(__file__).parents[1]\n"
        "tool = ROOT / 'tools' / 'fleet' / 'pbrun.py'\n")
    (root / 'tests/test_joinpath.py').write_text(
        "from pathlib import Path\nROOT = Path(__file__).parents[1]\n"
        "tool = ROOT.joinpath('tools', 'fleet', 'pbrun.py')\n")
    assert selected(plan(root, ['tools/fleet/pbrun.py'])) >= {
        'tests/test_root_join.py', 'tests/test_joinpath.py'}


def test_root_relative_asset_joins_are_direct_evidence(tmp_path):
    root = tree(tmp_path)
    (root / 'tests/test_asset_join.py').write_text(
        "from pathlib import Path\nROOT = Path(__file__).parents[1]\n"
        "asset = ROOT / 'deploy' / 'pool.json'\n")
    assert 'tests/test_asset_join.py' in selected(plan(root, ['deploy/pool.json']))


def test_imported_test_fixture_selects_its_importers_without_global_fanout(tmp_path):
    root = tree(tmp_path)
    (root / 'tests/test_fixture_consumer.py').write_text('from test_indirect import fixture_builder\n')
    assert selected(plan(root, ['tests/test_indirect.py'])) == {
        'tests/test_fast.py', 'tests/test_indirect.py', 'tests/test_fixture_consumer.py'}


def test_helper_closure_handles_cycles_relative_imports_and_literal_assets(tmp_path):
    root = tree(tmp_path)
    files = {
        'tests/support.py': "from .helpers import bridge\nasset = 'deploy/helper.json'\n",
        'tests/helpers/bridge.py': 'from .. import support\n',
        'tests/test_consumer.py': 'from tests.helpers import bridge\n',
        'tests/test_loader.py': "p = 'tests/support.py'\n",
    }
    for name, source in files.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(source)
    expected = {'tests/test_fast.py', 'tests/test_consumer.py', 'tests/test_loader.py'}
    assert selected(plan(root, ['tests/support.py'])) == expected
    assert selected(plan(root, ['deploy/helper.json'])) == expected


def test_deleted_helper_still_selects_its_importers(tmp_path):
    root = tree(tmp_path)
    (root / 'tests/test_consumer.py').write_text('from removed_helper import fixture\n')
    assert selected(plan(root, ['tests/removed_helper.py'])) == {
        'tests/test_fast.py', 'tests/test_consumer.py'}


def test_unused_helpers_and_gate_documentation_do_not_expand_selection(tmp_path):
    root = tree(tmp_path)
    assert selected(plan(root, ['tests/support.py', 'docs/ci_gate.md'])) == {'tests/test_fast.py'}


def test_full_queue_plan_retains_population_and_records_shadow_selection(tmp_path):
    root = tree(tmp_path)
    result = plan(root, ['src/prismabuild/pool.py'], mode='full')
    assert len(result['selected']) == result['population']['size'] == 9
    assert {row['path'] for row in result['shadow_selected']} == {
        'tests/test_fast.py', 'tests/test_direct.py', 'tests/test_nested.py'}
    assert result['mode'] == 'full'


def test_readiness_tracker_is_not_a_quarantine_owner(tmp_path):
    with pytest.raises(ValueError, match='owner'):
        select.create_plan(tree(tmp_path), [], durations={'files': {}},
            quarantine={'entries': [{'target': 'tests/test_direct.py', 'reason': 'broken', 'issue': 2428}]})
