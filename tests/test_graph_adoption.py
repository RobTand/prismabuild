"""The PB adoption grant admits a measured graph transition, never general growth."""
from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location('adoption_bazel_gate', ROOT / 'tools/ci/bazel_gate.py')
gate = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(gate)
CORE_SPEC = importlib.util.spec_from_file_location('adoption_impact_core', ROOT / 'tools/ci/graph_impact.py')
impact = importlib.util.module_from_spec(CORE_SPEC)
CORE_SPEC.loader.exec_module(impact)

ADOPTER_TEST = '//tests:test_1421_steady_fixture_is_bounded'
CLIS = ('//tools:fleet/pbrun', '//tools:fleet/pbwait', '//tools:fleet/pbstatus')
VIEWS = ('//src/prismabuild:operator_view', '//src/prismabuild:public_evidence')


@pytest.fixture
def adoption_workspace():
    def build(*, unrelated=False, overflowing=False, partial=False, cycle=False):
        cli_sources = {name + '.py' for name in CLIS}
        view_sources = {name + '.py' for name in VIEWS}
        extra = '//src/prismabuild:unrecorded.py'
        loc = {source: 1 for source in cli_sources | view_sources | {extra}}
        before_edges = {(ADOPTER_TEST, cli) for cli in CLIS}
        before_edges |= {(name, name + '.py') for name in (*CLIS, *VIEWS)}
        if overflowing:
            # This dependency already exists; only the six recorded import edges
            # are added, but their actual closure now exceeds its recorded delta.
            before_edges.add((VIEWS[0], extra))
        added_rules = {(cli, view) for cli in CLIS for view in VIEWS}
        if partial:
            added_rules.remove((CLIS[0], VIEWS[0]))
        if unrelated:
            added_rules.add((CLIS[0], extra))
        if cycle:
            before_edges.add((VIEWS[0], CLIS[0]))
        added_data = {(ADOPTER_TEST, source) for source in view_sources}
        after_edges = before_edges | added_rules | added_data
        reached = cli_sources | view_sources | ({extra} if unrelated or overflowing else set())
        before = impact.measure({ADOPTER_TEST: cli_sources}, before_edges, loc)
        after = impact.measure({ADOPTER_TEST: reached}, after_edges, loc)
        rules = {ADOPTER_TEST, *CLIS, *VIEWS}
        typed = lambda edges: {
            'rule_to_rule': {(a, b) for a, b in edges if a in rules and b in rules},
            'rule_to_source': {(a, b) for a, b in edges if a in rules and b in loc},
        }
        workspace = gate.Workspace()
        workspace.graph_revisions = ('before', 'after')
        workspace.snapshots = {'base': before, 'head': after}
        workspace.graph_impact = impact
        workspace.graph_inputs = gate.graph_policy_inputs(before, after, typed(before_edges), typed(after_edges))
        workspace.graph_contents = {name: {ADOPTER_TEST: 'unchanged'} for name in ('base', 'head')}
        workspace.graph_baselines = {name: {'graph_impact': impact.baseline_record(before)}
                                     for name in ('base', 'head')}
        workspace.graph_allowlists = {'base': {}, 'head': {}}
        workspace.graph_imports = {'base': {}, 'head': {}}
        workspace.graph_owners = {'base': {}, 'head': {}}
        workspace.explain = lambda target: 'synthetic graph contract'
        return workspace
    return build


def test_recorded_adoption_passes_without_changing_real_metrics(adoption_workspace):
    workspace = adoption_workspace()
    original_edges = workspace.graph_inputs['edge_diff']['added']['rule_to_rule'][:]
    report = workspace.graph_check('before', 'after')
    assert report['errors'] == []
    assert report['fanout_growth']['//src/prismabuild:public_evidence.py'] == 1
    assert report['head']['reach_sizes'][ADOPTER_TEST] == 5
    assert report['edge_diff']['added']['rule_to_rule'] == original_edges


def test_an_edge_outside_the_manifest_still_fails(adoption_workspace):
    report = adoption_workspace(unrelated=True).graph_check('before', 'after')
    assert report['errors']
    assert any('unrecorded' in error for error in report['errors'])


def test_recorded_edge_with_excess_measured_delta_fails(adoption_workspace):
    report = adoption_workspace(overflowing=True).graph_check('before', 'after')
    assert report['errors']
    assert any('recorded reach delta' in error for error in report['errors'])


def test_partial_import_inventory_is_not_an_adoption_grant(adoption_workspace):
    report = adoption_workspace(partial=True).graph_check('before', 'after')
    assert report['errors']


def test_adoption_does_not_admit_a_cycle(adoption_workspace):
    report = adoption_workspace(cycle=True).graph_check('before', 'after')
    assert any('SCC' in error or 'scc_max' in error for error in report['errors'])
