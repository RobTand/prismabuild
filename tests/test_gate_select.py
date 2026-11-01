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


def test_fast_plus_direct_and_transitive_production_importers(tmp_path):
    result = plan(tree(tmp_path), ['src/prismabuild/pool.py'])
    assert selected(result) == {'tests/test_fast.py', 'tests/test_direct.py',
                                'tests/test_nested.py', 'tests/test_indirect.py'}
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
                                    '.github/workflows/gate.yml',
                                    'tools/ci/gate_select.py',
                                    'tests/test_gate_select.py'])
def test_shared_configuration_and_helpers_select_whole_population(tmp_path, changed):
    result = plan(tree(tmp_path), [changed])
    assert len(result['selected']) == result['population']['size'] == 9


def test_quarantine_and_cost_exclusions_are_owned_and_node_ids_stay_precise(tmp_path):
    root = tree(tmp_path)
    result = select.create_plan(root, ['src/prismabuild/pool.py'],
        durations={'source': 'fixture', 'files': {'tests/test_fast.py': 3, 'tests/test_direct.py': 7,
            'tests/test_nested.py': 2, 'tests/test_pair.py': 4, 'tests/test_indirect.py': 2}},
        quarantine={'entries': [
            {'target': 'tests/test_direct.py::test_bad[param]', 'reason': 'observed flake', 'issue': 9001},
            {'target': 'tests/test_nested.py', 'reason': 'broken at baseline', 'issue': 9001}],
            'cost_exclusions': [{'target': 'tests/test_pair.py', 'reason': 'measured cost', 'issue': 9001}]})
    assert selected(result) == {'tests/test_fast.py', 'tests/test_direct.py', 'tests/test_indirect.py'}
    assert result['deselect'] == ['tests/test_direct.py::test_bad[param]']
    assert result['estimate']['wall_seconds'] == 7
    assert result['estimate']['serial_seconds'] == 12
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
        'tests/test_fast.py', 'tests/test_direct.py', 'tests/test_nested.py',
        'tests/test_indirect.py'}
    assert result['mode'] == 'full'


def test_readiness_tracker_is_not_a_quarantine_owner(tmp_path):
    with pytest.raises(ValueError, match='owner'):
        select.create_plan(tree(tmp_path), [], durations={'files': {}},
            quarantine={'entries': [{'target': 'tests/test_direct.py', 'reason': 'broken', 'issue': 2428}]})


def test_always_fast_tests_do_not_seed_helper_change_propagation(tmp_path):
    root = tree(tmp_path)
    (root / 'tests/test_consumer.py').write_text('from test_fast import fixture_builder\n')
    assert selected(plan(root, ['docs/ci_gate.md'])) == {'tests/test_fast.py'}


@pytest.fixture(scope='module')
def repository_path_index(tmp_path_factory):
    # Keep the real source patterns, without rescanning 1,300 unrelated files.
    root = tree(tmp_path_factory.mktemp('repository-path-patterns'))
    for name in (
        'tests/test_pbtest_native_directory_collection_1458.py',
        'tests/test_worker_loop_proves_it_parked.py',
        'tests/test_458_reboot_gate_regression.py',
        'tests/test_needrestart_broker_restart_exclusion.py',
        'tests/test_container_image_inventory.py',
    ):
        target = root / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text((ROOT / name).read_text())
    return root, select.dependency_index(root)


@pytest.mark.parametrize('changed,consumer', [
    ('tools/fleet/pbtest.py', 'tests/test_pbtest_native_directory_collection_1458.py'),
    ('tests/test_worker_loop_proves_it_parked.py', 'tests/test_458_reboot_gate_regression.py'),
    ('src/prismabuild/role_session.py', 'tests/test_458_reboot_gate_regression.py'),
    ('tools/fleet/50-prismabuild-resource-broker.conf', 'tests/test_needrestart_broker_restart_exclusion.py'),
    ('tests/fixtures/container_images/sparky_image_ls.txt', 'tests/test_container_image_inventory.py'),
])
def test_repository_path_loading_regressions(repository_path_index, changed, consumer):
    root, index = repository_path_index
    result = plan(root, [changed], index=index)
    assert consumer in selected(result)


@pytest.mark.parametrize('directory,expression', [
    ('ROOT = Path(__file__).resolve().parents[1]\nFLEET = ROOT / "tools" / "fleet"', 'FLEET / "pbtest.py"'),
    ('FLEET = Path(__file__).parent.parent / "tools/fleet"', 'FLEET.joinpath("pbtest.py")'),
    ('ROOT = Path(__file__).resolve().parents[1]', 'Path(ROOT, "tools", "fleet", "pbtest.py")'),
    ('FLEET = Path(__file__).parent / "../tools/fleet"', 'FLEET / "pbtest.py"'),
    ('import os\nFLEET = os.path.join(os.path.dirname(os.path.dirname(__file__)), "tools", "fleet")',
     'os.path.join(FLEET, "pbtest.py")'),
])
def test_named_directory_and_path_constructor_forms(tmp_path, directory, expression):
    root = tree(tmp_path)
    (root / 'tests/test_bound.py').write_text('from pathlib import Path\n' + directory + '\nP = ' + expression + '\n')
    assert 'tests/test_bound.py' in selected(plan(root, ['tools/fleet/pbtest.py']))
    assert 'tests/test_bound.py' not in selected(plan(root, ['docs/unrelated.md']))


@pytest.mark.parametrize('expression', [
    'Path(__file__).with_name("support.py")',
    'Path(__file__).parent / "support.py"',
    'Path(__file__).parent.joinpath("support.py")',
])
def test_test_relative_path_loaded_helper_closure(tmp_path, expression):
    root = tree(tmp_path)
    (root / 'tests/support.py').write_text('ASSET = "deploy/helper.json"\n')
    (root / 'tests/test_bound.py').write_text('from pathlib import Path\nP = ' + expression + '\n')
    assert 'tests/test_bound.py' in selected(plan(root, ['tests/support.py']))
    assert 'tests/test_bound.py' in selected(plan(root, ['deploy/helper.json']))


@pytest.mark.parametrize('tail', ['f"{box}_image_{name}"', 'name', '"{}_image_{}".format(box, name)',
                                  '"%s_image_%s" % (box, name)', 'box + "_image_" + name'])
def test_unresolved_fixture_names_cover_the_known_directory(tmp_path, tail):
    root = tree(tmp_path)
    (root / 'tests/test_bound.py').write_text(
        'from pathlib import Path\nFIXTURES = Path(__file__).parent / "fixtures" / "container_images"\n'
        'def fixture(box, name):\n    return (FIXTURES / ' + tail + ').read_bytes()\n')
    result = plan(root, ['tests/fixtures/container_images/sparky_image_ls.txt'])
    assert 'tests/test_bound.py' in selected(result)
    assert 'tests/test_bound.py' not in selected(plan(root, ['tests/fixtures/other/input.txt']))


def test_computed_descendant_of_a_directory_with_unresolved_base(tmp_path):
    root = tree(tmp_path)
    (root / 'tests/test_bound.py').write_text(
        'from pathlib import Path as _P\n'
        'FIXTURES = _P(__file__).parent / "fixtures" / "images"\n'
        'def fixture(name):\n    return (FIXTURES / name).read_bytes()\n')
    assert 'tests/test_bound.py' in selected(plan(root, ['tests/fixtures/images/captured.txt']))
    assert 'tests/test_bound.py' not in selected(plan(root, ['tests/fixtures/other/captured.txt']))


def test_owned_environment_lane_preserves_portable_tests_in_the_same_file(tmp_path):
    root = tree(tmp_path)
    quarantine = {'entries': [], 'cost_exclusions': [], 'environment_lanes': [
        {'target': 'tests/test_direct.py::test_needs_broker', 'lane': 'fleet_broker',
         'issue': 2500, 'reason': 'Requires a real PB broker to complete its scope'},
        {'target': 'tests/test_tool.py', 'lane': 'fleet_runtime',
         'issue': 2501, 'reason': 'Requires the published fleet runtime'}]}
    result = select.create_plan(root, ['src/prismabuild/pool.py', 'tools/fleet/pbrun.py'],
        durations={'files': {}}, quarantine=quarantine)
    assert 'tests/test_direct.py' in selected(result)
    assert 'tests/test_tool.py' not in selected(result)
    assert result['deselect'] == ['tests/test_direct.py::test_needs_broker']
    deferred = {row['path']: row for row in result['deferred']}
    assert deferred['tests/test_direct.py::test_needs_broker']['lane'] == 'fleet_broker'
    assert deferred['tests/test_tool.py']['issue'] == 2501


@pytest.mark.parametrize('owner,lane', [(2428, 'fleet_broker'), (None, 'fleet_broker'), (2500, '')])
def test_environment_lanes_require_a_real_owner_and_capability(tmp_path, owner, lane):
    with pytest.raises(ValueError):
        select.create_plan(tree(tmp_path), [], durations={'files': {}}, quarantine={
            'environment_lanes': [{'target': 'tests/test_direct.py', 'lane': lane,
                                   'issue': owner, 'reason': 'Requires the broker'}]})


@pytest.mark.parametrize('production_import', [
    'from prismabuild._gpu_submission_policy import _require_gb10_memory_subset',
    'from ._gpu_submission_policy import _require_gb10_memory_subset',
    'from . import _gpu_submission_policy',
])
def test_private_production_module_reaches_public_importers_and_test_helpers(tmp_path, production_import):
    root = tree(tmp_path)
    files = {
        'src/prismabuild/_gpu_submission_policy.py': 'def _require_gb10_memory_subset(): pass\n',
        'src/prismabuild/manifest.py': production_import + '\n',
        'tools/fleet/pbcampaign.py': 'from prismabuild.manifest import load_manifest\n',
        'tests/gpu_support.py': 'import pbcampaign\n',
        'tests/test_gpu_policy.py': 'from .gpu_support import manifest\n',
        'tests/test_public_manifest.py': 'from prismabuild import manifest\n',
        'tests/test_unrelated.py': 'import unrelated\n',
    }
    for name, source in files.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(source)
    result = plan(root, ['src/prismabuild/_gpu_submission_policy.py'])
    assert selected(result) == {
        'tests/test_fast.py', 'tests/test_gpu_policy.py', 'tests/test_public_manifest.py'}
    reasons = {row['path']: row['reasons'] for row in result['selected']}
    assert any('gpu_support.py' in reason for reason in reasons['tests/test_gpu_policy.py'])


def test_production_import_closure_handles_cycles_deleted_modules_and_parent_imports(tmp_path):
    root = tree(tmp_path)
    files = {
        'src/prismabuild/sub/bridge.py': 'from .. import missing\nfrom . import cycle\n',
        'src/prismabuild/sub/cycle.py': 'from .bridge import caller\n',
        'src/prismabuild/sub/__init__.py': 'from .bridge import caller\n',
        'tests/test_cycle.py': 'from prismabuild.sub import cycle\n',
    }
    for name, source in files.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(source)
    assert selected(plan(root, ['src/prismabuild/missing.py'])) == {
        'tests/test_fast.py', 'tests/test_cycle.py'}


def test_repository_gpu_policy_private_module_selects_public_campaign_consumer(tmp_path):
    root = tree(tmp_path)
    for name in ('src/prismabuild/_gpu_submission_policy.py', 'tools/fleet/pbcampaign.py',
                 'tests/test_gpu_submission_eligibility_2036.py'):
        target = root / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text((ROOT / name).read_text())
    assert 'tests/test_gpu_submission_eligibility_2036.py' in selected(
        plan(root, ['src/prismabuild/_gpu_submission_policy.py']))


@pytest.mark.parametrize('source', [
    'COUNTERS = {"census": 1}\n',
    '"""Example: import census"""\n',
])
def test_production_strings_do_not_create_import_edges_but_test_heuristics_remain(tmp_path, source):
    root = tree(tmp_path)
    (root / 'tools/backlog_census').mkdir()
    (root / 'tools/backlog_census/census.py').write_text('VALUE = 1\n')
    (root / 'src/prismabuild/pool.py').write_text(source)
    (root / 'tests/test_named_string.py').write_text('MODULE = "census"\n')
    (root / 'tests/census_support.py').write_text('MODULE = "census"\n')
    (root / 'tests/test_string_helper.py').write_text('from . import census_support\n')
    assert selected(plan(root, ['tools/backlog_census/census.py'])) == {
        'tests/test_fast.py', 'tests/test_named_string.py', 'tests/test_string_helper.py'}


@pytest.mark.parametrize('source', [
    'import importlib\nmod = importlib.import_module("prismabuild.private")\n',
    'import importlib as loader\nmod = loader.import_module("prismabuild.private")\n',
    'from importlib import import_module as load\nmod = load("prismabuild.private")\n',
    'mod = __import__("prismabuild.private")\n',
    'import importlib\nmod = importlib.import_module(".private", "prismabuild")\n',
    'import importlib\nmod = importlib.import_module(".private", package="prismabuild")\n',
])
def test_recognized_literal_dynamic_production_imports_reach_tests(tmp_path, source):
    root = tree(tmp_path)
    (root / 'src/prismabuild/private.py').write_text('VALUE = 1\n')
    (root / 'src/prismabuild/pool.py').write_text(source)
    assert selected(plan(root, ['src/prismabuild/private.py'])) == {
        'tests/test_fast.py', 'tests/test_direct.py', 'tests/test_nested.py', 'tests/test_indirect.py'}


@pytest.mark.parametrize('loader', [
    'importlib.util.spec_from_file_location("outcomes", OUTCOMES)',
    'importlib.machinery.SourceFileLoader("outcomes", str(OUTCOMES))',
    'runpy.run_path(str(OUTCOMES))',
])
def test_production_sibling_path_loading_reaches_test_helper_consumers(tmp_path, loader):
    root = tree(tmp_path)
    (root / 'tools/fleet/pbtest_outcomes.py').write_text('VALUE = 1\n')
    (root / 'tools/fleet/shape_gate.py').write_text(
        'from pathlib import Path\nimport importlib.util\nimport importlib.machinery\nimport runpy\n'
        'OUTCOMES = Path(__file__).parent.resolve() / "pbtest_outcomes.py"\n'
        'def load():\n    return ' + loader + '\n')
    (root / 'tests/shape_support.py').write_text('from tools.fleet import shape_gate\n')
    (root / 'tests/test_shape_consumer.py').write_text('from .shape_support import load\n')
    assert selected(plan(root, ['tools/fleet/pbtest_outcomes.py'])) == {
        'tests/test_fast.py', 'tests/test_shape_consumer.py'}


def test_repository_outcomes_path_selects_shape_gate_consumer(tmp_path):
    root = tree(tmp_path)
    for name in ('tools/fleet/shape_gate.py', 'tools/fleet/pbtest_outcomes.py'):
        (root / name).write_text((ROOT / name).read_text())
    (root / 'tests/test_shape_consumer.py').write_text('from tools.fleet import shape_gate\n')
    assert 'tests/test_shape_consumer.py' in selected(
        plan(root, ['tools/fleet/pbtest_outcomes.py']))


def test_production_module_and_path_strings_require_a_recognized_loader(tmp_path):
    root = tree(tmp_path)
    (root / 'tools/fleet/pbtest_outcomes.py').write_text('VALUE = 1\n')
    (root / 'src/prismabuild/private.py').write_text('VALUE = 1\n')
    (root / 'src/prismabuild/pool.py').write_text(
        'def store(value): return value\n'
        'VALUES = {"path": "tools/fleet/pbtest_outcomes.py"}\n'
        'store("prismabuild.private")\n')
    assert selected(plan(root, ['tools/fleet/pbtest_outcomes.py', 'src/prismabuild/private.py'])) == {
        'tests/test_fast.py'}


@pytest.mark.parametrize('setup,target,inside', [
    ('', 'str(Path("/tmp") / "config.py")', False),
    ('DIRECTORY = Path("/tmp")\nTARGET = DIRECTORY / "config.py"\n', 'str(TARGET)', False),
    ('DIRECTORY = Path(__file__).parent / "nested"\nTARGET = DIRECTORY / "config.py"\n',
     'str(TARGET)', True),
    ('TARGET = Path(__file__).parent / "config.py"\nTARGET = Path("/tmp") / "config.py"\n',
     'str(TARGET)', False),
])
def test_production_loader_conservatively_includes_external_and_nested_basename_literals(tmp_path, setup, target, inside):
    root = tree(tmp_path)
    (root / 'tools/fleet/config.py').write_text('VALUE = 1\n')
    (root / 'tools/fleet/nested').mkdir()
    (root / 'tools/fleet/nested/config.py').write_text('VALUE = 2\n')
    (root / 'tools/fleet/loader.py').write_text(
        'from pathlib import Path\nimport runpy\n' + setup + 'runpy.run_path(' + target + ')\n')
    (root / 'tests/test_loader_consumer.py').write_text('from tools.fleet import loader\n')
    assert selected(plan(root, ['tools/fleet/config.py'])) == {
        'tests/test_fast.py', 'tests/test_loader_consumer.py'}
    expected = {'tests/test_fast.py', 'tests/test_loader_consumer.py'} if inside else {'tests/test_fast.py'}
    assert selected(plan(root, ['tools/fleet/nested/config.py'])) == expected


@pytest.mark.parametrize('source', [
    'def load():\n    path = Path(__file__).parent / "config.py"\n'
    '    runpy.run_path(str(path))\n'
    'def unrelated(entry):\n    path = Path(entry.path)\n',
    'path = Path(__file__).parent / "config.py"\n'
    'runpy.run_path(str(path))\npath = Path("/tmp") / "config.py"\n',
    'def load(directory):\n    runpy.run_path(str(Path(directory) / "config.py"))\n',
    'def load(directory):\n    path = Path(directory) / "config.py"\n'
    '    runpy.run_path(str(path))\n',
    'def load(flag, directory):\n    path = Path(__file__).parent / "config.py"\n'
    '    if flag:\n        path = Path(directory)\n    runpy.run_path(str(path))\n',
])
def test_production_loader_keeps_evidence_with_other_writes_and_uncertain_values(tmp_path, source):
    root = tree(tmp_path)
    (root / 'tools/fleet/config.py').write_text('VALUE = 1\n')
    (root / 'tools/fleet/loader.py').write_text('from pathlib import Path\nimport runpy\n' + source)
    (root / 'tests/test_loader_consumer.py').write_text('from tools.fleet import loader\n')
    assert selected(plan(root, ['tools/fleet/config.py'])) == {
        'tests/test_fast.py', 'tests/test_loader_consumer.py'}


def test_repository_digest_loader_keeps_its_function_local_core_binding():
    path = 'tools/maintenance/diag_811_e1_checkout_cache.py'
    assert 'src/prismabuild/core.py' in select.dependencies(
        (ROOT / path).read_text(), path, production=True)[1]


@pytest.mark.parametrize('source', [
    pytest.param('path = Path("/tmp") / "config.py"\n'
                 'path, _ = Path(__file__).parent / "config.py", None\n'
                 'runpy.run_path(str(path))\n', id='unpacking'),
    pytest.param('path = Path("/tmp") / "config.py"\n'
                 'for path in [Path(__file__).parent / "config.py"]:\n'
                 '    runpy.run_path(str(path))\n', id='loop-target'),
    pytest.param('path = Path("/tmp") / "config.py"\n'
                 '[runpy.run_path(str(path)) for path in [Path(__file__).parent / "config.py"]]\n',
                 id='comprehension-target'),
    pytest.param('path = Path("/tmp") / "config.py"\n'
                 'with nullcontext(Path(__file__).parent / "config.py") as path:\n'
                 '    runpy.run_path(str(path))\n', id='with-target'),
    pytest.param('path = Path("/tmp") / "config.py"\n'
                 'try:\n    raise LookupError(Path(__file__).parent / "config.py")\n'
                 'except LookupError as path:\n    runpy.run_path(str(path))\n', id='except-name'),
    pytest.param('def outer():\n    path = Path(__file__).parent / "config.py"\n'
                 '    def load():\n        return runpy.run_path(str(path))\n'
                 '    return load\n', id='closure'),
    pytest.param('def outer():\n    path = Path(__file__).parent / "config.py"\n'
                 '    return lambda: runpy.run_path(str(path))\n', id='lambda'),
    pytest.param('def outer():\n    path = Path(__file__).parent / "config.py"\n'
                 '    def middle():\n        def load():\n            return runpy.run_path(str(path))\n'
                 '        return load\n    return middle\n', id='nested-scope'),
])
def test_production_loader_literals_survive_untracked_writes_and_enclosing_scopes(tmp_path, source):
    root = tree(tmp_path)
    (root / 'tools/fleet/config.py').write_text('VALUE = 1\n')
    (root / 'tools/fleet/loader.py').write_text(
        'from pathlib import Path\nfrom contextlib import nullcontext\nimport runpy\n' + source)
    (root / 'tests/test_loader_consumer.py').write_text('from tools.fleet import loader\n')
    assert selected(plan(root, ['tools/fleet/config.py'])) == {
        'tests/test_fast.py', 'tests/test_loader_consumer.py'}


def test_whole_population_trigger_does_not_build_an_unused_import_graph(tmp_path, monkeypatch):
    root = tree(tmp_path)
    expected = plan(root, ['tools/ci/gate_run.py'], mode='full')
    def unexpected_scan(*args):
        raise AssertionError('whole-population planning parsed an unnecessary dependency graph')
    monkeypatch.setattr(select, 'dependency_index', unexpected_scan)
    for mode in ('selection', 'full'):
        actual = plan(root, ['tools/ci/gate_run.py'], mode=mode)
        assert selected(actual) == selected(expected)
        assert {row['path'] for row in actual['shadow_selected']} == selected(expected)
        assert actual['population'] == expected['population']
        assert actual['deferred'] == expected['deferred']
        assert actual['deselect'] == expected['deselect']


@pytest.mark.parametrize('source', [
    pytest.param('path, _ = Path(__file__).parent / "config.py", None\n'
                 'runpy.run_path(str(path))\n', id='unpacking'),
    pytest.param('for path in [Path(__file__).parent / "config.py"]:\n'
                 '    runpy.run_path(str(path))\n', id='loop'),
    pytest.param('[runpy.run_path(str(path)) for path in [Path(__file__).parent / "config.py"]]\n',
                 id='comprehension'),
    pytest.param('with nullcontext(Path(__file__).parent / "config.py") as path:\n'
                 '    runpy.run_path(str(path))\n', id='with'),
])
def test_production_loader_tracks_nonplain_bindings_without_incidental_literals(tmp_path, source):
    root = tree(tmp_path)
    (root / 'tools/fleet/config.py').write_text('VALUE = 1\n')
    (root / 'tools/fleet/loader.py').write_text(
        'from pathlib import Path\nfrom contextlib import nullcontext\nimport runpy\n' + source)
    (root / 'tests/test_loader_consumer.py').write_text('from tools.fleet import loader\n')
    assert selected(plan(root, ['tools/fleet/config.py'])) == {
        'tests/test_fast.py', 'tests/test_loader_consumer.py'}


def test_production_loader_wildcard_matches_only_the_referenced_directory(tmp_path):
    root = tree(tmp_path)
    (root / 'tools/fleet/nested').mkdir()
    (root / 'tools/fleet/nested/config.py').write_text('VALUE = 1\n')
    (root / 'tools/fleet/other').mkdir()
    (root / 'tools/fleet/other/config.py').write_text('VALUE = 2\n')
    (root / 'tools/fleet/loader.py').write_text(
        'from pathlib import Path\nimport runpy\n'
        'def load(filename):\n'
        '    return runpy.run_path(str(Path(__file__).parent / "nested" / filename))\n')
    (root / 'tests/test_loader_consumer.py').write_text('from tools.fleet import loader\n')
    assert selected(plan(root, ['tools/fleet/nested/config.py'])) == {
        'tests/test_fast.py', 'tests/test_loader_consumer.py'}
    assert selected(plan(root, ['tools/fleet/other/config.py'])) == {'tests/test_fast.py'}


@pytest.mark.parametrize('loader,shadow', [
    ('from importlib import import_module as load', 'import json as load'),
    ('import importlib as load', 'import json as load'),
])
def test_dynamic_import_alias_keeps_each_imported_meaning_across_scopes(tmp_path, loader, shadow):
    root = tree(tmp_path)
    (root / 'src/prismabuild/private.py').write_text('VALUE = 1\n')
    call = 'load' if loader.startswith('from ') else 'load.import_module'
    (root / 'src/prismabuild/pool.py').write_text(
        'def load_private():\n    ' + loader + '\n    return ' + call + '("prismabuild.private")\n'
        'def load_json():\n    ' + shadow + '\n    return load.loads("{}")\n')
    assert selected(plan(root, ['src/prismabuild/private.py'])) == {
        'tests/test_fast.py', 'tests/test_direct.py', 'tests/test_nested.py', 'tests/test_indirect.py'}


@pytest.mark.parametrize('source', [
    'import importlib\nmodule = importlib.import_module(name="prismabuild.private")\n',
    'from importlib import import_module as load\nmodule = load(name="prismabuild.private")\n',
    'import importlib\nmodule = importlib.import_module(name=".private", package="prismabuild")\n',
])
def test_literal_keyword_dynamic_imports_reach_production_consumers(tmp_path, source):
    root = tree(tmp_path)
    (root / 'src/prismabuild/private.py').write_text('VALUE = 1\n')
    (root / 'src/prismabuild/pool.py').write_text(source)
    assert selected(plan(root, ['src/prismabuild/private.py'])) == {
        'tests/test_fast.py', 'tests/test_direct.py', 'tests/test_nested.py', 'tests/test_indirect.py'}


@pytest.mark.parametrize('source', [
    pytest.param('directory, filename = Path(__file__).parent / "nested", "config.py"\n'
                 'runpy.run_path(str(directory / filename))\n', id='unpacking'),
    pytest.param('for directory, filename in [(Path(__file__).parent / "nested", "config.py")]:\n'
                 '    runpy.run_path(str(directory / filename))\n', id='loop'),
    pytest.param('[runpy.run_path(str(directory / filename))\n'
                 ' for directory, filename in [(Path(__file__).parent / "nested", "config.py")]]\n',
                 id='comprehension'),
    pytest.param('with nullcontext((Path(__file__).parent / "nested", "config.py")) as (directory, filename):\n'
                 '    runpy.run_path(str(directory / filename))\n', id='with'),
])
def test_production_loader_keeps_split_components_in_compound_bindings(tmp_path, source):
    root = tree(tmp_path)
    (root / 'tools/fleet/nested').mkdir()
    (root / 'tools/fleet/nested/config.py').write_text('VALUE = 1\n')
    (root / 'tools/fleet/loader.py').write_text(
        'from pathlib import Path\nfrom contextlib import nullcontext\nimport runpy\n' + source)
    (root / 'tests/test_loader_consumer.py').write_text('from tools.fleet import loader\n')
    assert selected(plan(root, ['tools/fleet/nested/config.py'])) == {
        'tests/test_fast.py', 'tests/test_loader_consumer.py'}
