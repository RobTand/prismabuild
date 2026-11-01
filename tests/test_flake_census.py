"""A census distinguishes changes across repeats from architecture differences."""
import importlib.util
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location('flake_census', ROOT / 'tools/ci/flake_census.py')
census = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(census)


def samples_for(outcomes):
    return [{'repeat': repeat, 'architecture': arch, 'tests': {
        node: {'outcome': outcome, 'duration': 1, 'failure': 'bad' if outcome == 'failed' else ''}
        for node, outcome in nodes.items()}, 'run_url': f'https://example/run/{arch}/{repeat}'}
        for arch, repeats in outcomes.items() for repeat, nodes in enumerate(repeats)]


def test_counts_flakes_within_each_architecture_and_separates_stable_arch_difference():
    samples = samples_for({
        'x64': [{'flake': 'passed', 'broken': 'failed', 'arch': 'passed'},
                {'flake': 'failed', 'broken': 'failed', 'arch': 'passed'}],
        'arm64': [{'flake': 'passed', 'broken': 'failed', 'arch': 'failed'},
                  {'flake': 'passed', 'broken': 'failed', 'arch': 'failed'}]})
    result = census.classify_samples(samples, 2, [])
    assert result['classification']['intermittent'] == ['flake']
    assert result['classification']['deterministic_failure'] == ['broken']
    assert result['classification']['architecture_difference'] == ['arch']
    assert result['tests']['flake']['counts']['x64'] == {
        'passed': 1, 'failed': 1, 'skipped': 0, 'missing': 0}
    assert result['tests']['flake']['counts']['arm64']['passed'] == 2
    assert result['tests']['flake']['failures'][0]['excerpt'] == 'bad'
    assert result['tests']['flake']['failures'][0]['run_url'] == 'https://example/run/x64/1'


def test_quarantine_keeps_counts_but_only_fully_observed_passes_are_retirement_candidates():
    samples = samples_for({arch: [{'q::good': 'passed', 'q::skip': 'skipped', 'q::flake': 'passed'},
                                 {'q::good': 'passed', 'q::skip': 'passed', 'q::flake': 'failed'}]
                           for arch in ['x64', 'arm64']})
    result = census.classify_samples(samples, 2, [{'target': 'q', 'issue': 123}])
    assert result['classification']['intermittent'] == []
    assert result['classification']['quarantined'] == ['q::flake', 'q::good', 'q::skip']
    assert result['classification']['ready_for_retirement'] == ['q::good']
    assert result['tests']['q::good']['owners'] == [123]
    assert result['tests']['q::flake']['counts']['x64']['failed'] == 1


def test_missing_and_skipped_samples_never_imply_pass_or_retirement():
    result = census.classify_samples(samples_for({
        'x64': [{'q': 'passed'}, {}], 'arm64': [{'q': 'passed'}, {'q': 'skipped'}]}),
        2, [{'target': 'q', 'issue': 123}])
    assert result['classification']['ready_for_retirement'] == []
    assert result['tests']['q']['counts']['x64']['missing'] == 1
    assert result['tests']['q']['counts']['arm64']['skipped'] == 1


@pytest.mark.parametrize('samples', [
    [{'repeat': 0, 'architecture': 'x64', 'tests': {}, 'run_url': 'run'}] * 2,
    [{'repeat': 2, 'architecture': 'x64', 'tests': {}, 'run_url': 'run'}],
    [{'repeat': 0, 'architecture': 'mystery', 'tests': {}, 'run_url': 'run'}],
])
def test_duplicate_or_unexpected_sample_identity_is_refused(samples):
    with pytest.raises(ValueError):
        census.classify_samples(samples, 2, [])


def test_issue_proposal_is_literal_mention_free_and_skips_existing_owner():
    node = 'tests/a.py::test_x[![image](https://bad.invalid) <b> `tick` ' + chr(64) + 'person]'
    samples = samples_for({arch: [{node: 'passed'}, {node: 'failed'}] for arch in ['x64', 'arm64']})
    result = dict(census.classify_samples(samples, 2, []), sha='abc', complete=True, run_url='https://example/run')
    plans = census.census_issue_plans(result, [])
    assert len(plans) == 1
    body = plans[0]['body']
    assert body.startswith(census.report.marker(node) + '\n\n')
    assert chr(64) + 'person' not in body
    assert '\\!\\[image\\]' in body
    assert '&lt;b&gt;' in body
    assert '\\`tick\\`' in body
    assert 'gate_quarantine.json' in body
    assert '"issue": "<new issue number>"' in body
    owner = {'number': 123, 'body': body, 'labels': [{'name': 'full-suite'}]}
    assert census.census_issue_plans(result, [owner]) == []
    assert census.census_issue_plans(dict(result, complete=False), []) == []


class CensusStore:
    def __init__(self):
        self.issues = []
    def pages(self, path, field=None):
        if path == 'labels':
            return [{'name': 'full-suite'}, {'name': 'P2'}]
        return self.issues
    def request(self, path, data):
        assert path == 'issues', 'existing owners must not receive another filing or comment'
        issue = dict(data, number=len(self.issues) + 1, html_url='https://example/issue')
        issue['labels'] = [{'name': label} for label in data['labels']]
        self.issues.append(issue)
        return issue


def test_filing_dedupes_against_latest_open_owners_and_preserves_excerpt_boundaries():
    api = CensusStore()
    a, b = 'tests/a.py::test_a', 'tests/b.py::test_b'
    result = dict(census.classify_samples(samples_for({arch: [
        {a: 'passed', b: 'passed'}, {a: 'failed', b: 'failed'}] for arch in ['x64', 'arm64']}), 2, []),
        sha='abc', complete=True, run_url='run')
    plans = census.census_issue_plans(result, [])
    plans[0]['body'] += '\n\n    ' + census.report.marker(b)
    census.file_new_issues(api, plans)
    census.file_new_issues(api, plans)
    assert len(api.issues) == 2
    assert census.report.owners(api.issues) == {a: 1, b: 2}


def test_collect_refuses_partial_shards_instead_of_inventing_samples(tmp_path):
    manifest = {'sha': 'sha', 'repeats': 2, 'include': [
        {'index': 0, 'repeat': 0, 'architecture': 'x64', 'runner': 'ubuntu-24.04', 'files': ['tests/a.py', 'tests/b.py']}]}
    folder = tmp_path / '0'
    folder.mkdir()
    (folder / 'result.json').write_text(json.dumps({'verdict': 'fail', 'head_sha': 'sha',
        'files': ['tests/a.py', 'tests/b.py'], 'pytest_exit_code': 2, 'budget': {'status': 'unavailable'}}))
    (folder / 'junit.xml').write_text('<testsuite><testcase file="tests/a.py" classname="tests.a" '
        'name="test_x"><failure>bad</failure></testcase></testsuite>')
    with pytest.raises(ValueError, match='complet|cover'):
        census.collect_census(tmp_path, manifest, 'run')


def test_plan_uses_shared_partition_and_covers_both_architectures_every_repeat(tmp_path, monkeypatch):
    (tmp_path / 'tests').mkdir()
    for name in ['a', 'b']:
        (tmp_path / 'tests' / f'test_{name}.py').write_text('')
    ci = tmp_path / 'tools/ci'
    ci.mkdir(parents=True)
    (ci / 'gate_durations.json').write_text('{"files":{"tests/test_a.py":7,"tests/test_b.py":3}}')
    (ci / 'gate_quarantine.json').write_text('{"entries":[{"target":"tests/test_a.py", "issue":123,"reason":"flake"}]}')
    monkeypatch.setattr(census.nightly.gate_select, 'tier_declarations', lambda root: (set(), {}))
    plan, manifest = census.census_manifest(tmp_path, 'sha', 3, 2)
    assert len(manifest['include']) == 12
    for repeat in range(3):
        for arch in ['x64', 'arm64']:
            rows = [r for r in manifest['include'] if r['repeat'] == repeat and r['architecture'] == arch]
            assert [r['files'] for r in rows] == [['tests/test_a.py'], ['tests/test_b.py']]
    assert plan['deselect'] == []
    assert [r['path'] for r in plan['selected']] == ['tests/test_a.py', 'tests/test_b.py']


def test_workflow_freezes_main_and_limits_capacity_and_write_permissions():
    import yaml
    workflow = yaml.safe_load((ROOT / '.github/workflows/flake-census.yml').read_text())
    triggers = workflow.get('on', workflow.get(True))
    assert triggers['workflow_dispatch']['inputs']['repeats']['default'] == 5
    assert workflow['permissions'] == {'contents': 'read'}
    assert workflow['concurrency'] == {'group': 'full-suite-main', 'cancel-in-progress': False}
    samples = workflow['jobs']['samples']
    assert samples['strategy']['max-parallel'] == 4
    assert samples['strategy']['fail-fast'] is False
    checkouts = [step for step in samples['steps'] if step.get('uses', '').startswith('actions/checkout') ]
    assert checkouts[1]['with']['ref'] == '${{ needs.plan.outputs.sha }}'
    assert 'issues' not in samples.get('permissions', {})
    assert workflow['jobs']['report']['permissions']['issues'] == 'write'
    assert workflow['jobs']['report']['if'] == 'always()'


def test_an_unlabeled_open_marker_owner_also_suppresses_creation():
    result = dict(census.classify_samples(samples_for({arch: [{'x': 'passed'}, {'x': 'failed'}]
        for arch in ['x64', 'arm64']}), 2, []), sha='abc', complete=True, run_url='run')
    issue = {'number': 8, 'body': census.report.marker('x'), 'labels': []}
    assert census.census_issue_plans(result, [issue]) == []
