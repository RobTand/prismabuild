"""A hosted reporter trusts CircleCI run identity and treats artifacts only as data."""
import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('full_suite_bridge', ROOT / 'tools/ci/full_suite_bridge.py')
bridge = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bridge)


def test_status_target_accepts_only_the_same_circleci_project():
    event = {'context': 'ci/circleci: full-suite-report', 'state': 'failure', 'sha': 'a' * 40,
             'target_url': 'https://circleci.com/gh/RobTand/prismabuild/42'}
    assert bridge.event_target(event, 'RobTand/prismabuild') == ('job', 42, 'a' * 40)
    event['target_url'] = 'https://circleci.com/gh/other/repository/42'
    assert bridge.event_target(event, 'RobTand/prismabuild') is None
    event['target_url'] = 'https://evil.example/gh/RobTand/prismabuild/42'
    assert bridge.event_target(event, 'RobTand/prismabuild') is None


def test_pipeline_target_and_checks_are_supported_without_polling():
    event = {'context': 'CircleCI Pipeline', 'state': 'success', 'sha': 'a' * 40,
             'target_url': 'https://app.circleci.com/pipelines/gh/RobTand/prismabuild/9'}
    assert bridge.event_target(event, 'RobTand/prismabuild') == ('pipeline', 9, 'a' * 40)
    event = {'check_run': {'name': 'full-suite-report', 'status': 'completed',
             'head_sha': 'b' * 40, 'details_url': 'https://circleci.com/gh/RobTand/prismabuild/10'}}
    assert bridge.event_target(event, 'RobTand/prismabuild') == ('job', 10, 'b' * 40)


def test_public_pipeline_identity_requires_main_and_the_exact_commit():
    pipeline = {'project_slug': 'gh/RobTand/prismabuild', 'number': 8,
                'vcs': {'branch': 'main', 'revision': 'a' * 40}}
    assert bridge.valid_pipeline(pipeline, 'RobTand/prismabuild', 'a' * 40)
    pipeline['vcs']['branch'] = 'feature'
    assert not bridge.valid_pipeline(pipeline, 'RobTand/prismabuild', 'a' * 40)
    pipeline['vcs']['branch'] = 'main'
    assert not bridge.valid_pipeline(pipeline, 'RobTand/prismabuild', 'b' * 40)


def test_manifest_requires_complete_artifacts_and_safe_relative_xml_paths():
    manifest = {'schema': 1, 'sha': 'a' * 40, 'branch': 'main', 'complete': True,
                'junit': ['full-suite/junit/0.xml', 'full-suite/junit/1.xml']}
    assert bridge.manifest_files(manifest, 'a' * 40) == manifest['junit']
    manifest['complete'] = False
    with pytest.raises(ValueError, match='complete'):
        bridge.manifest_files(manifest, 'a' * 40)
    manifest['complete'] = True
    manifest['junit'] = ['../outside.xml']
    with pytest.raises(ValueError, match='relative'):
        bridge.manifest_files(manifest, 'a' * 40)


def test_old_and_duplicate_circleci_runs_cannot_replace_the_newer_baseline():
    history = {'circleci_pipeline_number': 8}
    assert bridge.newer_than_history(9, history)
    assert not bridge.newer_than_history(8, history)
    assert not bridge.newer_than_history(7, history)
    assert bridge.newer_than_history(1, {})


def test_bridge_runs_reporter_offline_with_public_xml_as_data(tmp_path, monkeypatch):
    import json
    sha = 'a' * 40
    event = tmp_path / 'event.json'
    event.write_text(json.dumps({'inputs': {'job_number': '42', 'sha': sha}}))
    output = tmp_path / 'out/results.json'
    class Circle:
        def __init__(self, repository):
            pass
        def resolve(self, kind, number):
            return {'number': 42}, {'number': 9, 'project_slug': 'gh/RobTand/prismabuild',
                                  'vcs': {'branch': 'main', 'revision': sha}}
        def junit(self, number, revision, directory):
            (directory / 'tests.xml').write_text('''<testsuite><testcase file="tests/a.py"
                classname="tests.a" name="test_x"><failure>wrong value</failure></testcase></testsuite>''')
    monkeypatch.setattr(bridge, 'CircleCI', Circle)
    monkeypatch.setattr(bridge, 'previous_results', lambda *args: {})
    monkeypatch.setattr(bridge.report.GitHub, 'from_environment', lambda repository: object())
    monkeypatch.setenv('GITHUB_REPOSITORY', 'RobTand/prismabuild')
    monkeypatch.setenv('GITHUB_RUN_NUMBER', '7')
    monkeypatch.delenv('GITHUB_TOKEN', raising=False)
    assert bridge.main(['--event', str(event), '--output', str(output), '--dry-run']) == 1
    result = json.loads(output.read_text())
    assert result['classification']['new'] == ['tests/a.py::test_x']
    assert result['circleci_pipeline_number'] == 9
    assert result['filed'] == []
    assert result['plans'][0]['title'] == '[P2] Full suite: tests/a.py::test_x fails on main'


def test_hosted_bridge_has_no_test_or_fleet_execution_steps():
    import yaml
    workflow = yaml.safe_load((ROOT / '.github/workflows/full-suite-report.yml').read_text())
    triggers = workflow.get('on', workflow.get(True))
    assert set(triggers) == {'status', 'check_run', 'workflow_dispatch'}
    job = workflow['jobs']['report']
    assert job['runs-on'] == 'ubuntu-latest'
    checkout = next(s for s in job['steps'] if s.get('uses', '').startswith('actions/checkout'))
    assert checkout['with']['ref'] == 'main'
    source = (ROOT / '.github/workflows/full-suite-report.yml').read_text()
    assert 'self-hosted' not in source
    assert 'pytest' not in source
    assert 'CIRCLE_TOKEN' not in source
