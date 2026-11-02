"""Bazel graph metrics and minimum-only policy on deterministic small graphs."""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location('graph_impact', ROOT / 'tools/ci/graph_impact.py')
impact = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(impact)


def graph(edges=None, roots=None, loc=None, contents=None):
    return impact.Snapshot(edges or {}, roots or {}, loc or {}, contents or {})


def record(snapshot, **overrides):
    return {**impact.baseline_record(impact.measure(snapshot)), **overrides}


def test_metrics_include_unreached_sources_and_weight_lines():
    snapshot = graph({'t1': {'a'}, 't2': set(), 'conftest.py': {'b'}, 'a': {'b'}},
                     {'t1': {'t1', 'conftest.py'}, 't2': {'t2'}}, {'a': 1, 'b': 3, 'c': 2})
    live = impact.measure(snapshot)
    assert live['reach'] == {'t1': ['a', 'b'], 't2': []}
    assert live['dependents'] == {'a': 1, 'b': 1, 'c': 0}
    assert live['u_pct'] == pytest.approx(100 / 3)
    assert live['loc_u_pct'] == pytest.approx(100 / 3)
    assert (live['reach_median'], live['reach_p90'], live['reach_max']) == (1, 2, 2)
    assert impact.measure(graph(snapshot.edges, snapshot.roots, {'a': 1, 'b': 5, 'c': 0}))['loc_u_pct'] == 50
    assert record(snapshot)['reach_budget'] == 2


def test_empty_graph_has_finite_zero_metrics():
    live = impact.measure(graph())
    assert [live[k] for k in ('u_pct', 'loc_u_pct', 'scc_max', 'reach_median', 'reach_p90', 'reach_max')] == [0] * 6


def test_scc_members_and_deep_graph_do_not_need_python_recursion():
    edges = {str(i): {str(i + 1)} for i in range(1500)}
    edges['1500'] = {'1499'}
    live = impact.measure(graph(edges))
    assert live['scc_max'] == 2
    assert live['scc_members'] == ['1499', '1500']


def test_deps_reader_accepts_source_leaves_without_keys_and_normalizes_labels(tmp_path):
    path = tmp_path / 'deps.json'
    path.write_text(json.dumps({'@//tests:t': ['@//pkg:a.py', '@@external//:ignored']}))
    snapshot = impact.read_deps(path, {'//tests:t'}, {'//pkg:a.py': 3})
    assert impact.measure(snapshot)['reach'] == {'//tests:t': ['//pkg:a.py']}
    assert snapshot.edges['//tests:t'] == {'//pkg:a.py'}
    path.write_text('{"//tests:t": "not a list"}')
    with pytest.raises(ValueError, match='dependencies'):
        impact.read_deps(path, {'//tests:t'}, {})


def test_changed_and_new_tests_use_the_recorded_budget_and_bazel_blame():
    before = graph({'t': {'a'}, 'a': {'hub'}}, {'t': {'t'}}, {'a': 1, 'hub': 1}, {'t': 'old'})
    after = graph(before.edges, before.roots, before.loc, {'t': 'changed'})
    errors = impact.compare(before, after, record(before, reach_budget=1),
                            explain=lambda target: f'```mermaid\ngraph TD\n{target} --> hub\n```')
    assert any('t reach 2 exceeds budget 1' in e and 't --> hub' in e for e in errors)
    assert impact.compare(before, before, record(before, reach_budget=1)) == []
    errors = impact.compare(None, after, record(after, reach_budget=0))
    assert any('t reach 2 exceeds budget 0' in e for e in errors)


def test_transitive_reach_change_counts_as_a_changed_test():
    before = graph({'t': {'a'}}, {'t': {'t'}}, {'a': 1, 'b': 1})
    after = graph({'t': {'a'}, 'a': {'b'}}, before.roots, before.loc)
    assert any('t reach 2 exceeds budget 1' in e for e in impact.compare(before, after, record(before)))


def test_uniform_regression_attributes_added_edges_and_source_fanout():
    before = graph({'t': {'a'}}, {'t': {'t'}}, {'a': 1, 'b': 1})
    after = graph({'t': {'a'}, 'a': {'b'}, 'unrelated': {'leaf'}}, before.roots, before.loc)
    errors = impact.compare(before, after, record(before, reach_budget=10))
    error = next(e for e in errors if 'u_pct rose' in e)
    assert 'a -> b' in error and 'b +1' in error
    assert 'unrelated -> leaf' not in error


@pytest.mark.parametrize('edges,added', [
    ({'a': {'b'}, 'b': {'a', 'c'}, 'c': set()}, ('c', 'a')),
    ({'a': {'b'}, 'b': {'a', 'c'}, 'c': {'d'}, 'd': {'c'}}, ('d', 'a')),
    ({'a': {'b'}, 'b': set(), 'x': {'y'}, 'y': {'z'}, 'z': {'x'}}, ('b', 'a')),
])
def test_any_scc_growth_or_merge_fails_even_below_the_largest(edges, added):
    before = graph({**edges, 'unrelated': set()})
    after = graph({**before.edges, added[0]: edges[added[0]] | {added[1]}, 'unrelated': {'leaf'}})
    error = next(e for e in impact.compare(before, after, record(before)) if 'SCC grew or merged' in e)
    assert f'{added[0]} -> {added[1]}' in error
    assert 'unrelated -> leaf' not in error


def test_minimum_baseline_preserves_history_and_never_banks_tolerated_rises():
    old = {'u_pct': 40, 'loc_u_pct': 50, 'scc_max': 4, 'reach_p90': 10}
    baseline = impact.baseline_record(old)
    assert impact.regressions({**old, 'u_pct': 40.5}, baseline) == []
    live = {**old, 'u_pct': 30, 'loc_u_pct': 50.5, 'scc_max': 2, 'reach_p90': 8}
    assert any('u_pct fell' in e for e in impact.stale_entries(live, baseline))
    updated = impact.baseline_record(live, baseline)
    assert (updated['u_pct'], updated['loc_u_pct'], updated['reach_budget']) == (30, 50, 8)


def test_baseline_writer_records_gains_and_refuses_losses_without_writing(tmp_path, capsys):
    path = tmp_path / 'baseline.json'
    path.write_text(json.dumps({'history': 'kept', 'graph_impact': {
        'u_pct': 90, 'loc_u_pct': 90, 'scc_max': 3, 'reach_budget': 4}}))
    deps = tmp_path / 'deps.json'
    deps.write_text('{"t": ["a"]}')
    metadata = tmp_path / 'sources.json'
    metadata.write_text('{"tests": ["t"], "loc": {"a": 1, "b": 1}}')
    args = ['--head-deps', str(deps), '--head-metadata', str(metadata), '--baseline', str(path), '--write-baseline']
    assert impact.main(args) == 0
    written = json.loads(path.read_text())
    assert written['history'] == 'kept'
    assert written['graph_impact']['u_pct'] == 50
    old = path.read_bytes()
    deps.write_text('{"t": ["a", "b"]}')
    assert impact.main(args) == 1
    assert path.read_bytes() == old
    assert 'refusing to record' in capsys.readouterr().err


def test_allowlist_requires_a_reason_and_may_only_shrink():
    assert impact.allowlist_errors({'reach:t': 'integration contract'}, {})
    assert impact.allowlist_errors({'reach:t': ''}, {'reach:t': ''})
    assert impact.allowlist_errors({'reach:t': 'changed'}, {'reach:t': 'original'})
    assert impact.allowlist_errors({}, {'reach:t': 'integration contract'}) == []
    after = graph({'t': {'a'}, 'other': {'a'}}, {'t': {'t'}, 'other': {'other'}}, {'a': 1})
    errors = impact.compare(None, after, record(after, reach_budget=0), allowlist={'reach:t': 'integration'})
    assert not any('t reach ' in e for e in errors)
    assert any('other reach ' in e for e in errors)


def test_summary_has_metrics_budget_and_added_edge_attribution():
    before = graph({'t': {'a'}}, {'t': {'t'}}, {'a': 1})
    after = graph(before.edges, {**before.roots, 'new': {'new'}}, before.loc)
    assert impact.summary(before, after, 1) == (
        'U 100.0% → 50.0%, SCC max 1 → 1, new tests 1 (reach 0 ≤ budget 1), new edges 0, none to a hub')
    assert 'no base available' in impact.summary(None, after, 0)
