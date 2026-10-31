"""Nightly CI keeps full coverage and reports only complete multi-architecture runs."""
import importlib.util
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('full_suite_workflow', ROOT / 'tools/ci/full_suite_workflow.py')
workflow = importlib.util.module_from_spec(spec)
spec.loader.exec_module(workflow)


def test_population_keeps_quarantine_and_two_cpu_and_excludes_only_root_gpu(tmp_path):
    for name in ['test_plain', 'test_quarantined', 'test_two', 'test_root', 'test_gpu']:
        path = tmp_path / 'tests' / (name + '.py')
        path.parent.mkdir(exist_ok=True)
        path.write_text('')
    lanes = {'tests/test_root.py': {'lane': 'root'}, 'tests/test_gpu.py': {'lane': 'gpu'},
             'tests/test_two.py': {'lane': 'two_cpu'}}
    plan = workflow.cpu_plan(tmp_path, lanes, 'a' * 40)
    assert [r['path'] for r in plan['selected']] == [
        'tests/test_plain.py', 'tests/test_quarantined.py', 'tests/test_two.py']
    assert plan['deselect'] == []


def test_missing_shard_or_no_verdict_refuses_complete_reporting(tmp_path):
    manifest = {'sha': 'a' * 40, 'include': [
        {'index': 0, 'runner': 'ubuntu-latest', 'files': ['tests/a.py']},
        {'index': 1, 'runner': 'ubuntu-24.04-arm', 'files': ['tests/b.py']}]}
    for index, name in enumerate(['a', 'b']):
        path = tmp_path / f'full-suite-shard-{index}'
        path.mkdir()
        (path / 'result.json').write_text(json.dumps({'verdict': 'pass', 'head_sha': 'a' * 40,
                                                     'files': [f'tests/{name}.py']}))
        (path / 'junit.xml').write_text(f'<testsuite><testcase file="tests/{name}.py" classname="tests.{name}" name="test_x"/></testsuite>')
    assert workflow.collect_shards(tmp_path, manifest)['tests/a.py::test_x']['outcome'] == 'passed'
    (tmp_path / 'full-suite-shard-1/result.json').write_text(json.dumps({'verdict': None}))
    with pytest.raises(ValueError, match='verdict|identity'):
        workflow.collect_shards(tmp_path, manifest)
    (tmp_path / 'full-suite-shard-1/result.json').unlink()
    with pytest.raises((ValueError, FileNotFoundError)):
        workflow.collect_shards(tmp_path, manifest)


def test_nightly_workflow_uses_hosted_gate_tools_and_always_reports():
    import yaml
    source = (ROOT / '.github/workflows/full-suite.yml').read_text()
    value = yaml.safe_load(source)
    triggers = value.get('on', value.get(True))
    assert set(triggers) == {'schedule', 'workflow_dispatch'}
    assert value['concurrency']['cancel-in-progress'] is False
    assert value['jobs']['suite']['strategy']['fail-fast'] is False
    assert value['env']['FULL_SUITE_SHARDS'] == '16'
    assert value['jobs']['retry-plan']['needs'] == ['plan', 'suite']
    assert value['jobs']['retry-suite']['strategy']['fail-fast'] is False
    assert value['jobs']['report']['if'] == 'always()'
    assert value['jobs']['report']['permissions']['issues'] == 'write'
    assert 'gate_run.py' in source
    assert 'self-hosted' not in source
    assert 'CircleCI' not in source


def test_previous_results_skips_dry_and_incomplete_runs(monkeypatch):
    class API:
        def pages(self, path, field):
            if '/runs?' in path:
                return [{'id': n, 'run_number': n} for n in [3, 2, 1, 4]]
            number = int(path.split('/')[2])
            return [{'id': number, 'name': 'full-suite-results', 'expired': False}]
    monkeypatch.setattr(workflow, 'artifact_results', lambda api, number: {
        3: {'complete': True, 'dry_run': True},
        2: {'complete': False},
        1: {'complete': True, 'dry_run': False, 'sha': 'previous'}}[number])
    assert workflow.previous_results(API(), 4)['sha'] == 'previous'


def test_signed_artifact_download_drops_authorization(tmp_path, monkeypatch):
    import io
    import urllib.error
    import zipfile
    archive = io.BytesIO()
    with zipfile.ZipFile(archive, 'w') as output:
        output.writestr('results.json', '{"complete": true, "sha": "old"}')
    class Opener:
        def open(self, request, timeout):
            assert request.get_header('Authorization') == 'Bearer test-only-secret'
            raise urllib.error.HTTPError(request.full_url, 302, 'redirect',
                                         {'Location': 'https://example.invalid/public'}, None)
    monkeypatch.setattr(workflow.urllib.request, 'build_opener', lambda *args: Opener())
    def public_download(request, timeout):
        assert request == 'https://example.invalid/public'
        return io.BytesIO(archive.getvalue())
    monkeypatch.setattr(workflow.urllib.request, 'urlopen', public_download)
    api = workflow.report.GitHub('RobTand/prismabuild', 'test-only-secret')
    assert workflow.artifact_results(api, 7)['sha'] == 'old'


def test_plan_uses_real_partition_cli_and_round_robin_runners(tmp_path, monkeypatch):
    (tmp_path / 'tests').mkdir()
    for name in ['a', 'b']:
        (tmp_path / 'tests' / f'test_{name}.py').write_text('')
    durations = tmp_path / 'tools/ci/gate_durations.json'
    durations.parent.mkdir(parents=True)
    durations.write_text(json.dumps({'files': {'tests/test_a.py': 3, 'tests/test_b.py': 7}}))
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(workflow.gate_select, 'tier_declarations', lambda root: (set(), {}))
    monkeypatch.setenv('GITHUB_OUTPUT', str(tmp_path / 'outputs'))
    workflow.main(['plan', '--sha', 'a' * 40, '--shards', '2', '--output', str(tmp_path / 'out')])
    matrix = json.loads((tmp_path / 'outputs').read_text().splitlines()[0].removeprefix('matrix='))
    assert [(row['index'], row['runner']) for row in matrix['include']] == [
        (0, 'ubuntu-24.04'), (1, 'ubuntu-24.04-arm')]
    manifest = json.loads((tmp_path / 'out/manifest.json').read_text())
    assert manifest['include'][0]['files'] == ['tests/test_b.py']
    assert manifest['include'][1]['files'] == ['tests/test_a.py']
    assert manifest['sha'] == 'a' * 40




def test_incomplete_ci_writes_a_result_without_requesting_or_filing_issues(tmp_path, monkeypatch):
    class API:
        def pages(self, *args):
            raise AssertionError('incomplete CI must not look up issues')
        def request(self, *args):
            raise AssertionError('incomplete CI must not file')
    monkeypatch.setattr(workflow.report.GitHub, 'from_environment', lambda repository: API())
    monkeypatch.setattr(workflow, 'previous_results', lambda api, current: {})
    monkeypatch.setenv('GITHUB_REPOSITORY', 'RobTand/prismabuild')
    monkeypatch.setenv('GITHUB_RUN_ID', '42')
    manifest = {'sha': 'a' * 40, 'include': [
        {'index': 0, 'runner': 'ubuntu-latest', 'files': ['tests/a.py']}]}
    output = tmp_path / 'out/results.json'
    assert workflow.write_report(tmp_path / 'missing', manifest, output, False) == 2
    result = json.loads(output.read_text())
    assert result['complete'] is False
    assert result['filed'] == []
    assert result['plans'] == []
    assert result['classification'] == {'new': [], 'known': [], 'quarantined': [], 'recovered': []}


def test_cpu_plan_defers_capability_controls_without_dropping_quarantined_files(tmp_path):
    (tmp_path / 'tests').mkdir()
    for name in ['portable', 'quarantined', 'fleet', 'mixed', 'archival']:
        (tmp_path / 'tests' / f'test_{name}.py').write_text('')
    capabilities = [{'target': 'tests/test_fleet.py', 'lane': 'fleet_runtime', 'issue': 1182},
                    {'target': 'tests/test_mixed.py::test_needs_broker', 'lane': 'fleet_broker', 'issue': 2442}]
    plan = workflow.cpu_plan(tmp_path, {}, 'a' * 40, environment_lanes=capabilities)
    assert 'tests/test_quarantined.py' in [row['path'] for row in plan['selected']]
    assert 'tests/test_fleet.py' not in [row['path'] for row in plan['selected']]
    assert plan['deselect'] == ['tests/test_mixed.py::test_needs_broker']
    assert plan['deferred'] == capabilities
