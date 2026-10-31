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
                                'tests/test_nested.py', 'tests/test_pair.py'}
    reasons = {row['path']: row['reasons'] for row in result['selected']}
    assert 'fast tier' in reasons['tests/test_fast.py']
    assert any('src/prismabuild/pool.py' in reason for reason in reasons['tests/test_direct.py'])
    deferred = {row['path']: row for row in result['deferred']}
    assert deferred['tests/test_gpu.py']['issue']
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
                                    'tests/support.py', 'tools/verify.py',
                                    'tools/ci/gate_select.py', '.github/workflows/gate.yml'])
def test_shared_configuration_and_helpers_select_whole_population(tmp_path, changed):
    result = plan(tree(tmp_path), [changed])
    assert len(result['selected']) == result['population']['size'] == 10


def test_quarantine_and_cost_exclusions_are_owned_and_node_ids_stay_precise(tmp_path):
    root = tree(tmp_path)
    result = select.create_plan(root, ['src/prismabuild/pool.py'],
        durations={'source': 'fixture', 'files': {'tests/test_fast.py': 3, 'tests/test_direct.py': 7,
            'tests/test_nested.py': 2, 'tests/test_pair.py': 4}},
        quarantine={'entries': [
            {'target': 'tests/test_direct.py::test_bad[param]', 'reason': 'observed flake', 'issue': 2428},
            {'target': 'tests/test_nested.py', 'reason': 'broken at baseline', 'issue': 2428}],
            'cost_exclusions': [{'target': 'tests/test_pair.py', 'reason': 'measured cost', 'issue': 2428}]})
    assert selected(result) == {'tests/test_fast.py', 'tests/test_direct.py'}
    assert result['deselect'] == ['tests/test_direct.py::test_bad[param]']
    assert result['estimate']['wall_seconds'] == 7
    assert result['estimate']['serial_seconds'] == 10
    assert result['population']['version']
    assert {(row['path'], row['issue']) for row in result['deferred']} >= {
        ('tests/test_nested.py', 2428), ('tests/test_pair.py', 2428),
        ('tests/test_direct.py::test_bad[param]', 2428)}


def test_invalid_or_unowned_exclusion_refuses_instead_of_hiding_coverage(tmp_path):
    root = tree(tmp_path)
    for entry in ({'target': 'tests/test_direct.py', 'reason': 'broken'},
                  {'target': '../test_escape.py', 'reason': 'broken', 'issue': 2428}):
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
