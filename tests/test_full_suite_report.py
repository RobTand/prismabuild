"""Issue decisions use test history, exact owners, and actual recoveries."""
import importlib.util
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('full_suite_report', ROOT / 'tools/ci/full_suite_report.py')
report = importlib.util.module_from_spec(spec)
spec.loader.exec_module(report)


def row(outcome, failure='assert 1 == 2'):
    return {'outcome': outcome, 'duration': 1, 'failure': failure}


def test_classify_new_known_recovered_and_quarantined():
    before = {'new': row('passed'), 'known': row('failed'), 'fixed': row('failed'),
              'skipped': row('failed')}
    now = {'new': row('failed'), 'known': row('failed'), 'fixed': row('passed'),
           'added': row('failed'), 'skip': row('failed'), 'skipped': row('skipped')}
    assert report.classify(now, before, {'skip'}) == {
        'new': ['added', 'new'], 'known': ['known'], 'recovered': ['fixed'],
        'quarantined': ['skip']}


def test_owner_requires_exact_hidden_marker_and_full_suite_label():
    issues = [
        {'number': 1, 'body': '<!-- full-suite: tests/a.py::test_xy -->', 'labels': [{'name': 'full-suite'}]},
        {'number': 2, 'body': '<!-- full-suite: tests/a.py::test_x -->', 'labels': []},
        {'number': 3, 'body': '<!-- full-suite: tests/a.py::test_x -->', 'labels': [{'name': 'full-suite'}]},
    ]
    assert report.owners(issues) == {'tests/a.py::test_xy': 1, 'tests/a.py::test_x': 3}


def test_recurrence_comments_on_owner_and_recovery_does_not_close():
    plan = report.issue_plan({'x': row('failed'), 'fixed': row('passed')},
                             {'x': row('passed'), 'fixed': row('failed')}, set(),
                             {'x': 4, 'fixed': 5}, 'old', 'new', [], 'https://example/run')
    assert [(p['kind'], p.get('number')) for p in plan] == [('comment', 4), ('comment', 5)]
    assert 'passed at new' in plan[1]['body']


def test_twenty_failures_file_individual_issues_and_twenty_one_file_one_summary():
    now = {f'tests/a.py::test_{n}': row('failed') for n in range(21)}
    plan = report.issue_plan(now, {}, set(), {}, 'old', 'new', [], 'https://example/run')
    assert len(plan) == 1
    assert plan[0]['kind'] == 'issue'
    assert '21 new failures' in plan[0]['title']
    assert '<!-- full-suite: tests/a.py::test_20 -->' in plan[0]['body']
    del now['tests/a.py::test_20']
    assert len(report.issue_plan(now, {}, set(), {}, 'old', 'new', [], 'run')) == 20


def test_issue_contains_range_prs_reproducer_and_no_mentions():
    plan = report.issue_plan({'tests/a.py::test_x': row('failed', 'hello @someone\nsecret?')},
        {}, set(), {}, 'abc', 'def', [{'number': 42, 'title': 'Fix @person behavior'}], 'https://example/run')
    issue = plan[0]
    assert issue['title'] == '[P2] Full suite: tests/a.py::test_x fails on main'
    assert 'abc...def' in issue['body']
    assert '#42' in issue['body']
    assert 'pbtest.py' in issue['body']
    assert '--priority -10' in issue['body']
    assert not re.search(r'@[A-Za-z0-9_]', issue['body'])


def test_quarantine_file_and_node_markers_suppress_only_matching_tests():
    now = {'tests/a.py::test_x': row('failed'), 'tests/a.py::test_y': row('failed'),
           'tests/b.py::test_x': row('failed')}
    assert report.classify(now, {}, {'tests/a.py'})['new'] == ['tests/b.py::test_x']


def test_previous_run_is_completed_older_main_run_and_never_current():
    runs = [{'id': 7, 'run_number': 7, 'head_branch': 'main', 'status': 'completed'},
            {'id': 6, 'run_number': 6, 'head_branch': 'feature', 'status': 'completed'},
            {'id': 5, 'run_number': 5, 'head_branch': 'main', 'status': 'in_progress'},
            {'id': 4, 'run_number': 4, 'head_branch': 'main', 'status': 'completed'}]
    assert report.previous_run(runs, 7, 7)['id'] == 4


def test_missing_shards_are_infrastructure_not_mass_recovery(tmp_path):
    import json
    (tmp_path / 'one').mkdir()
    (tmp_path / 'one/result.json').write_text(json.dumps({
        'shard': 0, 'verdict': True, 'tests': {'x': row('passed')}, 'wall_seconds': 2}))
    result = report.collect_results(tmp_path, 2)
    assert result['complete'] is False
    assert result['missing_shards'] == [1]
    assert result['tests']['x']['outcome'] == 'passed'


class IssueStore:
    """An in-memory REST resource with persistent issues and comments."""
    def __init__(self):
        self.issues = []
        self.comments = {}
        self.labels = [{'name': 'P2'}]

    def pages(self, path, field=None):
        if path == 'labels':
            return self.labels
        if path.startswith('issues?'):
            return self.issues
        if path.endswith('/comments'):
            return self.comments.get(int(path.split('/')[1]), [])
        raise AssertionError(path)

    def request(self, path, data):
        if path == 'labels':
            self.labels.append(data)
            return data
        if path == 'issues':
            number = len(self.issues) + 1
            issue = dict(data, number=number, html_url=f'https://example/issues/{number}')
            issue['labels'] = [{'name': name} for name in data['labels']]
            self.issues.append(issue)
            return issue
        if path.endswith('/comments'):
            number = int(path.split('/')[1])
            comment = dict(data, html_url=f'https://example/issues/{number}#comment')
            self.comments.setdefault(number, []).append(comment)
            return comment
        raise AssertionError(path)


def test_rest_path_files_one_owner_and_comments_on_recovery_idempotently():
    api = IssueStore()
    now = {'tests/a.py::test_x': row('failed')}
    report.apply_plan(api, report.issue_plan(now, {}, set(), {}, 'old', 'bad', [], 'run/1'))
    assert len(api.issues) == 1
    assert [l['name'] for l in api.issues[0]['labels']] == ['full-suite', 'P2']
    fixed = {'tests/a.py::test_x': row('passed')}
    plan = report.issue_plan(fixed, now, set(), report.owners(api.issues), 'bad', 'fixed', [], 'run/2')
    report.apply_plan(api, plan)
    report.apply_plan(api, plan)
    assert len(api.comments[1]) == 1
    assert 'passed at fixed' in api.comments[1][0]['body']
    assert len(api.issues) == 1


def test_rerun_after_filing_before_upload_reuses_owner():
    api = IssueStore()
    plan = report.issue_plan({'x': row('failed')}, {}, set(), {}, 'old', 'bad', [], 'run/1')
    report.apply_plan(api, plan)
    report.apply_plan(api, plan)
    report.apply_plan(api, plan)
    assert len(api.issues) == 1
    assert len(api.comments[1]) == 1


def test_prior_incomplete_run_falls_back_to_complete_history():
    class History:
        def pages(self, path, field):
            if 'workflows/' in path:
                return [{'id': n, 'run_number': n, 'head_branch': 'main', 'status': 'completed'}
                        for n in (3, 2)]
            return [{'id': int(path.split('/')[2]), 'name': 'full-suite-results', 'expired': False}]
        def artifact(self, artifact_id):
            return {'complete': artifact_id == 2, 'sha': f'sha{artifact_id}', 'tests': {}}
    assert report.load_history(History(), 4, 4)['sha'] == 'sha2'


def test_quarantine_reads_gate_records(tmp_path):
    import json
    path = tmp_path / 'quarantine.json'
    path.write_text(json.dumps({'tests': [{'nodeid': 'tests/a.py::test_x', 'issue': 123}]}))
    assert report.quarantine_entries(path) == {'tests/a.py::test_x'}


def test_summary_reuses_owners_but_still_files_unowned_failures():
    api = IssueStore()
    report.apply_plan(api, report.issue_plan({'x': row('failed')}, {}, set(), {}, 'a', 'b', [], 'run/0'))
    now = {**{'x': row('failed')}, **{f'new{n}': row('failed') for n in range(21)}}
    report.apply_plan(api, report.issue_plan(now, {}, set(), {}, 'a', 'b', [], 'run/1'))
    assert len(api.issues) == 2
    assert report.owners(api.issues)['new20'] == 2
    assert report.owners(api.issues)['x'] == 1


def test_retry_result_replaces_unverdictable_partial_results(tmp_path):
    import json
    for attempt, verdict, outcome in ((1, False, 'passed'), (2, True, 'failed')):
        directory = tmp_path / f'attempt-{attempt}'
        directory.mkdir()
        (directory / 'result.json').write_text(json.dumps({'shard': 0, 'verdict': verdict,
            'tests': {'x': row(outcome)}, 'wall_seconds': 2}))
    result = report.collect_results(tmp_path, 1)
    assert result['complete'] is True
    assert result['tests']['x']['outcome'] == 'failed'


def test_last_passing_sha_survives_a_skipped_baseline():
    before = {'x': dict(row('skipped'), last_pass_sha='last-good')}
    plan = report.issue_plan({'x': row('failed')}, before, set(), {}, 'previous', 'bad', [], 'run')
    assert 'last-good...bad' in plan[0]['body']


def test_workflow_keeps_suite_off_gate_and_privileges_in_report():
    import yaml
    workflow = yaml.safe_load((ROOT / '.github/workflows/full-suite.yml').read_text())
    triggers = workflow.get('on', workflow.get(True))
    assert set(triggers) == {'push', 'schedule', 'workflow_dispatch'}
    assert triggers['schedule'] == [{'cron': '23 2 * * *'}]
    assert workflow['concurrency'] == {'group': 'full-suite-main', 'cancel-in-progress': False}
    for name in ('shards', 'retry'):
        job = workflow['jobs'][name]
        assert job['runs-on'] == ['self-hosted', 'pb-fleet', 'pb-suite']
        assert job['strategy']['matrix']['shard'] == [0, 1, 2, 3]
    report_job = workflow['jobs']['report']
    assert report_job['runs-on'] == 'ubuntu-latest'
    assert report_job['permissions']['issues'] == 'write'
    assert report_job['if'] == 'always()'
    assert set(report_job['needs']) >= {'shards', 'retry'}
    assert 'issues' not in workflow['permissions']


def test_artifact_redirect_drops_authorization_before_downloading(monkeypatch):
    import io
    import json
    import urllib.error
    import zipfile
    archive = io.BytesIO()
    with zipfile.ZipFile(archive, 'w') as zipped:
        zipped.writestr('results.json', json.dumps({'complete': True, 'sha': 'old', 'tests': {}}))
    downloads = []
    class Opener:
        def open(self, request, timeout):
            assert request.get_header('Authorization') == 'Bearer unit-test-secret'
            raise urllib.error.HTTPError(request.full_url, 302, 'redirect',
                {'Location': 'https://artifact.example/signed'}, None)
    def download(request, timeout):
        downloads.append(request)
        return io.BytesIO(archive.getvalue())
    monkeypatch.setattr(report.urllib.request, 'build_opener', lambda *args: Opener())
    monkeypatch.setattr(report.urllib.request, 'urlopen', download)
    result = report.GitHub('RobTand/prismabuild', 'unit-test-secret').artifact(42)
    assert result == {'complete': True, 'sha': 'old', 'tests': {}}
    assert downloads == ['https://artifact.example/signed']


def test_rest_payload_scrubs_runtime_token(monkeypatch):
    import io
    import json
    payloads = []
    def send(request, timeout):
        payloads.append(json.loads(request.data))
        return io.BytesIO(b'{"number": 7}')
    monkeypatch.setattr(report.urllib.request, 'urlopen', send)
    result = report.GitHub('RobTand/prismabuild', 'unit-test-secret').request(
        'issues', {'body': 'failure contained unit-test-secret'})
    assert result['number'] == 7
    assert payloads == [{'body': 'failure contained [REDACTED]'}]
