"""Runtime loader contracts keep static dependency evidence bounded and complete."""
from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location('loader_contract_gate', ROOT / 'tools/ci/gate_select.py')
gate = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(gate)


@pytest.mark.parametrize('owner,expected', [
    ('src/prismabuild/installed_bootstrap.py', {
        'tools/fleet/shape_gate.py', 'tools/fleet/pbtest_outcomes.py',
        'tools/shape_gate.py', 'tools/pbtest_outcomes.py'}),
    ('tools/fleet/publish_runtime.py', {
        'tools/fleet/shape_gate.py', 'tools/fleet/pbcanary.py', 'tools/fleet/upgrade_client.py'}),
    ('tools/maintenance/broker_read_baseline.py', {'tools/fleet/resource_broker.py'}),
    ('tools/ci/full_suite_workflow.py', {
        'tools/ci/full_suite_report.py', 'tools/ci/gate_select.py', 'tools/ci/gate_partition.py'}),
])
def test_reviewed_python_loader_targets_are_complete_and_bounded(owner, expected):
    _, references = gate.dependencies((ROOT / owner).read_text(), owner, production=True)
    # Include absent flat-layout alternatives, and same-directory unrelated files.
    candidates = {path.relative_to(ROOT).as_posix()
                  for directory in ('src/prismabuild', 'tools')
                  for path in (ROOT / directory).rglob('*.py')} | expected
    actual = {path for path in candidates if gate.gate_paths.matches(path, references)}
    assert actual == expected, {'missing': sorted(expected - actual), 'extra': sorted(actual - expected)}


@pytest.mark.parametrize('layout', ['flat', 'fleet', 'both'])
def test_bootstrap_loads_only_its_two_tools_in_each_supported_layout(tmp_path, layout):
    from prismabuild import installed_bootstrap

    layouts = ('tools', 'tools/fleet') if layout == 'both' else (
        'tools/fleet' if layout == 'fleet' else 'tools',)
    for directory in layouts:
        folder = tmp_path / directory
        folder.mkdir(parents=True, exist_ok=True)
        for name in ('shape_gate', 'pbtest_outcomes'):
            (folder / (name + '.py')).write_text(f'ORIGIN = {directory + "/" + name!r}\n')
    modules = installed_bootstrap._load_qualification_tools(tmp_path)
    chosen = 'tools' if layout == 'flat' else 'tools/fleet'
    assert [module.ORIGIN for module in modules] == [chosen + '/shape_gate', chosen + '/pbtest_outcomes']


def test_nightly_sibling_loader_refuses_an_unlisted_existing_module():
    spec = importlib.util.spec_from_file_location('loader_contract_workflow', ROOT / 'tools/ci/full_suite_workflow.py')
    workflow = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(workflow)
    # gate_paths.py exists beside the allowed modules, but is not a runtime input.
    with pytest.raises(KeyError, match='gate_paths'):
        workflow.sibling('gate_paths')
    assert workflow.report.__file__ == str(ROOT / 'tools/ci/full_suite_report.py')
    assert workflow.gate_select.__file__ == str(ROOT / 'tools/ci/gate_select.py')
    assert workflow.gate_partition.__file__ == str(ROOT / 'tools/ci/gate_partition.py')
