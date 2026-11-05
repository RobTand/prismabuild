"""The PB adoption grant admits a measured graph transition, never general growth."""
from __future__ import annotations

import importlib.util
import json
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
    def build(*, unrelated=False, overflowing=False, partial=False, cycle=False,
              single_cli=None, outside_view=None, outside_only=False,
              only_public=False, prior_public=False):
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
        if single_cli is not None:
            added_rules = {edge for edge in added_rules if edge[0] == single_cli}
        if only_public:
            added_rules = {edge for edge in added_rules if edge[1] == VIEWS[1]}
        if prior_public:
            before_edges |= {(single_cli, VIEWS[1]), (ADOPTER_TEST, VIEWS[1] + '.py')}
            added_rules.remove((single_cli, VIEWS[1]))
        if partial:
            added_rules.remove((CLIS[0], VIEWS[0]))
        if unrelated:
            added_rules.add((CLIS[0], extra))
        if cycle:
            before_edges.add((VIEWS[0], CLIS[0]))
        added_data = {(ADOPTER_TEST, source) for source in view_sources}
        outsider = '//tools:fleet/not_a_recorded_adopter'
        if outside_view is not None:
            before_edges.add((ADOPTER_TEST, outsider))
            outside_views = (outside_view,) if isinstance(outside_view, str) else outside_view
            added_rules |= {(outsider, view) for view in outside_views}
        if only_public:
            view_sources = {VIEWS[1] + '.py'}
            added_data = {(ADOPTER_TEST, source) for source in view_sources}
        after_edges = before_edges | added_rules | added_data
        initial_sources = cli_sources | ({VIEWS[1] + '.py'} if prior_public else set())
        if outside_only:
            before_edges |= {(ADOPTER_TEST, view) for view in VIEWS}
            after_edges = before_edges | {(outsider, outside_view)}
            initial_sources = cli_sources | view_sources
        reached = cli_sources | view_sources | ({extra} if unrelated or overflowing else set())
        before = impact.measure({ADOPTER_TEST: initial_sources}, before_edges, loc)
        after = impact.measure({ADOPTER_TEST: reached}, after_edges, loc)
        rules = {ADOPTER_TEST, outsider, *CLIS, *VIEWS}
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
        workspace.graph_imports = {name: gate.bazel_imports({}, {}) for name in ('base', 'head')}
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


def test_partial_import_inventory_is_an_adoption_grant(adoption_workspace):
    report = adoption_workspace(partial=True).graph_check('before', 'after')
    assert report['errors'] == []
    assert report['adoption']['admitted_edges'] == 7


def test_adoption_does_not_admit_a_cycle(adoption_workspace):
    report = adoption_workspace(cycle=True).graph_check('before', 'after')
    assert any('SCC' in error or 'scc_max' in error for error in report['errors'])


@pytest.fixture
def adoption_policy_modules():
    policy = gate.module_from('adoption_boundary_policy', ROOT / 'tools/ci/graph_adoption.py')
    digests = gate.module_from('adoption_boundary_digests', ROOT / 'src/prismabuild/digest_primitives.py')
    return policy, digests


def test_manifest_payload_and_declared_hash_are_both_pinned(adoption_policy_modules, tmp_path):
    policy, digests = adoption_policy_modules
    source = ROOT / 'tools/ci/public_view_adoption.json'
    document = policy.read_first_adoption_manifest(source, digests)
    assert document['sha256'] == policy.ADOPTION_SHA256
    assert len(document['imports']) == 6
    document['source_deltas'][VIEWS[0] + '.py']['max_added_consumers'] += 1
    path = tmp_path / 'altered.json'
    path.write_text(json.dumps(document))
    with pytest.raises(ValueError, match='reviewed payload'):
        policy.read_first_adoption_manifest(path, digests)
    document['sha256'] = digests.canonical_sha256({k: v for k, v in document.items() if k != 'sha256'})
    path.write_text(json.dumps(document))
    with pytest.raises(ValueError, match='reviewed pin'):
        policy.read_first_adoption_manifest(path, digests)


def test_manifest_rejects_duplicate_keys_and_symlinks(adoption_policy_modules, tmp_path):
    policy, digests = adoption_policy_modules
    duplicate = tmp_path / 'duplicate.json'
    duplicate.write_text('{"sha256":"a","sha256":"b"}')
    with pytest.raises(ValueError, match='duplicate key'):
        policy.read_first_adoption_manifest(duplicate, digests)
    link = tmp_path / 'link.json'
    link.symlink_to(ROOT / 'tools/ci/public_view_adoption.json')
    with pytest.raises(OSError):
        policy.read_first_adoption_manifest(link, digests)


def test_manifest_rejects_oversized_or_missing_files(adoption_policy_modules, tmp_path):
    policy, digests = adoption_policy_modules
    oversized = tmp_path / 'oversized.json'
    oversized.write_bytes(b' ' * (policy.MAX_ADOPTION_BYTES + 1))
    with pytest.raises(ValueError, match='byte bound'):
        policy.read_first_adoption_manifest(oversized, digests)
    with pytest.raises(OSError):
        policy.read_first_adoption_manifest(tmp_path / 'absent.json', digests)


def test_new_consumer_cannot_trade_places_with_recorded_consumers(adoption_workspace):
    workspace = adoption_workspace()
    unexpected = '//tests:not_a_recorded_adoption_consumer'
    for name, graph in workspace.snapshots.items():
        graph['reach'][unexpected] = graph['reach'][ADOPTER_TEST][:]
    report = workspace.graph_check('before', 'after')
    assert report['errors']
    assert any('recorded reach delta' in error or 'unrecorded consumers' in error for error in report['errors'])


@pytest.mark.parametrize('cli', CLIS)
def test_one_recorded_cli_pair_can_adopt_with_its_first_writer(adoption_workspace, cli):
    workspace = adoption_workspace(single_cli=cli)
    report = workspace.graph_check('before', 'after')
    assert report['errors'] == []
    assert report['adoption']['applied'] is True
    assert len(report['edge_diff']['added']['rule_to_rule']) == 2


@pytest.mark.parametrize('cli', CLIS)
def test_one_cli_can_adopt_only_public_evidence(adoption_workspace, cli):
    workspace = adoption_workspace(single_cli=cli, only_public=True)
    report = workspace.graph_check('before', 'after')
    assert report['errors'] == []
    assert report['adoption']['applied'] is True
    assert report['adoption']['admitted_edges'] == 2
    assert report['edge_diff']['added']['rule_to_rule'] == [[cli, VIEWS[1]]]
    assert report['head']['reach_sizes'][ADOPTER_TEST] == 4


@pytest.mark.parametrize('cli', CLIS)
def test_one_cli_can_later_adopt_its_other_recorded_import(adoption_workspace, cli):
    workspace = adoption_workspace(single_cli=cli, prior_public=True)
    assert VIEWS[1] + '.py' in workspace.snapshots['base']['reach'][ADOPTER_TEST]
    report = workspace.graph_check('before', 'after')
    assert report['errors'] == []
    assert report['adoption']['applied'] is True
    assert report['adoption']['admitted_edges'] == 2
    assert report['edge_diff']['added']['rule_to_rule'] == [[cli, VIEWS[0]]]
    assert report['fanout_growth'] == {VIEWS[0] + '.py': 1}


@pytest.mark.parametrize('view', VIEWS)
def test_a_nonparticipating_module_cannot_import_either_view(adoption_workspace, view):
    report = adoption_workspace(single_cli=CLIS[0], outside_view=view).graph_check('before', 'after')
    assert report['errors']
    assert report['adoption']['applied'] is False
    assert any('unrecorded rule_to_rule edge' in error for error in report['errors'])


@pytest.mark.parametrize('cli', CLIS)
def test_each_recorded_pair_refuses_a_delta_over_its_cap(adoption_workspace, cli):
    report = adoption_workspace(single_cli=cli, overflowing=True).graph_check('before', 'after')
    assert report['errors']
    assert report['adoption']['applied'] is False
    assert any('recorded reach delta' in error for error in report['errors'])


@pytest.mark.parametrize('view', VIEWS)
def test_outsider_only_import_is_rejected_even_without_reach_growth(adoption_workspace, view):
    workspace = adoption_workspace(outside_view=view, outside_only=True)
    assert workspace.snapshots['base']['reach'] == workspace.snapshots['head']['reach']
    report = workspace.graph_check('before', 'after')
    assert report['errors']
    assert report['adoption']['applied'] is False
    assert any('unrecorded rule_to_rule edge' in error for error in report['errors'])


def test_an_unrecorded_cli_cannot_adopt_an_import_pair(adoption_workspace):
    report = adoption_workspace(outside_view=VIEWS).graph_check('before', 'after')
    assert report['errors']
    assert report['adoption']['applied'] is False
    assert any('unrecorded rule_to_rule edge' in error for error in report['errors'])


def _direct_grant(policy, *, components_after, removed=()):
    """Call the adoption policy alone, so only its own guards can reject."""
    cli, view = CLIS[2], VIEWS[1]
    manifest = {'sha256': 'direct', 'imports': [[cli, view]],
                'added_edges': {'rule_to_rule': [[cli, view]], 'rule_to_source': []},
                'test_deltas': {}, 'source_deltas': {}}
    inputs = {'edge_diff': {'added': {'rule_to_rule': [[cli, view]], 'rule_to_source': []},
                            'removed': {'rule_to_rule': [list(edge) for edge in removed],
                                        'rule_to_source': []}}}
    six = [f'//big:{n}' for n in range(6)]
    before = {'reach': {}, 'loc': {}, 'components': [six, ['//p:a', '//p:b'], ['//q:a', '//q:b']]}
    after = {'reach': {}, 'loc': {}, 'components': components_after(six)}
    return policy.adoption_structural_inputs(before, after, inputs, manifest)


def test_adoption_guard_rejects_an_scc_merge_below_the_existing_maximum(adoption_policy_modules):
    policy, _ = adoption_policy_modules
    merged = lambda six: [six, ['//p:a', '//p:b', '//q:a', '//q:b']]
    _, decision, errors = _direct_grant(policy, components_after=merged)
    assert decision['applied'] is False
    assert any('SCC growth or merging' in error for error in errors)
    unchanged = lambda six: [six, ['//p:a', '//p:b'], ['//q:a', '//q:b']]
    _, control, control_errors = _direct_grant(policy, components_after=unchanged)
    assert control_errors == [] and control['applied'] is True


def test_adoption_guard_rejects_a_simultaneous_edge_removal(adoption_policy_modules):
    policy, _ = adoption_policy_modules
    unchanged = lambda six: [six, ['//p:a', '//p:b'], ['//q:a', '//q:b']]
    _, decision, errors = _direct_grant(policy, components_after=unchanged,
                                        removed=[('//x:a', '//x:b')])
    assert decision['applied'] is False
    assert any('simultaneous edge removals' in error for error in errors)
