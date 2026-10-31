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




def test_last_passing_sha_survives_a_skipped_baseline():
    before = {'x': dict(row('skipped'), last_pass_sha='last-good')}
    plan = report.issue_plan({'x': row('failed')}, before, set(), {}, 'previous', 'bad', [], 'run')
    assert 'last-good...bad' in plan[0]['body']






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




def test_pull_request_cannot_own_a_test_failure():
    issue = {'number': 8, 'body': '<!-- full-suite: tests/a.py::test_x -->',
             'labels': [{'name': 'full-suite'}], 'pull_request': {'url': 'example'}}
    assert report.owners([issue]) == {}


def test_plain_junit_uses_file_and_class_to_keep_reproducible_pytest_ids():
    xml = '''<testsuites><testsuite tests="3" failures="1" skipped="1">
      <testcase file="tests/test_a.py" classname="tests.test_a.TestA" name="test_x[a.b]" time="1.25">
        <failure message="wrong">assert 1 == 2</failure>
      </testcase><testcase file="tests/test_a.py" classname="tests.test_a" name="test_ok" time="0.5"/>
      <testcase file="tests/test_a.py" classname="tests.test_a" name="test_skip"><skipped/></testcase>
    </testsuite></testsuites>'''
    rows = report.parse_junit(xml)
    assert rows['tests/test_a.py::TestA::test_x[a.b]'] == {
        'outcome': 'failed', 'duration': 1.25, 'failure': 'assert 1 == 2'}
    assert rows['tests/test_a.py::test_ok']['outcome'] == 'passed'
    assert rows['tests/test_a.py::test_skip']['outcome'] == 'skipped'


def test_exact_nodeid_property_takes_precedence_over_lossy_junit_classname():
    xml = '''<testsuite><testcase classname="ambiguous.class" name="test_x[a.b]" time="2">
      <properties><property name="nodeid" value="tests/a.b.py::custom_class::test_x[a.b]"/></properties>
      <error>setup failed</error></testcase></testsuite>'''
    assert report.parse_junit(xml)['tests/a.b.py::custom_class::test_x[a.b]']['outcome'] == 'failed'


def test_ambiguous_junit_missing_file_and_exact_id_refuses():
    import pytest
    with pytest.raises(ValueError, match='junit_family=legacy'):
        report.parse_junit('<testsuite><testcase classname="a.C" name="test_x"/></testsuite>')


def test_directory_collects_nested_junit_files_once_and_rejects_duplicate_nodes(tmp_path):
    import pytest
    (tmp_path / 'first').mkdir()
    (tmp_path / 'second').mkdir()
    xml = '<testsuite><testcase file="tests/a.py" classname="tests.a" name="test_x"/></testsuite>'
    (tmp_path / 'first/junit.xml').write_text(xml)
    result = report.read_results(tmp_path)
    assert list(result) == ['tests/a.py::test_x']
    (tmp_path / 'second/junit.xml').write_text(xml)
    with pytest.raises(ValueError, match='multiple'):
        report.read_results(tmp_path)


def test_dry_run_baseline_is_refused_instead_of_hiding_unreported_failures():
    import pytest
    with pytest.raises(ValueError, match='reported'):
        report.build_report({'x': row('failed')}, {'complete': True, 'dry_run': True, 'sha': 'old',
            'tests': {'x': row('failed')}}, set(), [], 'new', [], 'run')


def test_incomplete_input_does_not_file_or_claim_recovery():
    result = report.build_report({'x': row('passed')},
        {'complete': True, 'sha': 'old', 'tests': {'x': row('failed')}}, set(),
        [{'number': 1, 'labels': [{'name': 'full-suite'}], 'body': '<!-- full-suite: x -->'}],
        'new', [], 'run', complete=False)
    assert result['complete'] is False
    assert result['plans'] == []
    assert result['classification']['recovered'] == []


def test_cli_dry_run_needs_no_ci_environment_or_token(tmp_path, monkeypatch):
    import json
    inputs = tmp_path / 'junit'
    inputs.mkdir()
    (inputs / 'junit.xml').write_text('''<testsuite><testcase file="tests/a.py" classname="tests.a"
        name="test_x"><failure>new failure</failure></testcase></testsuite>''')
    previous = tmp_path / 'previous.json'
    previous.write_text(json.dumps({'complete': True, 'sha': 'old', 'tests': {}}))
    output = tmp_path / 'results.json'
    for name in ('GITHUB_TOKEN', 'GITHUB_SHA', 'GITHUB_RUN_ID', 'GITHUB_REPOSITORY'):
        monkeypatch.delenv(name, raising=False)
    assert report.main(['--input', str(inputs), '--previous', str(previous), '--output', str(output),
        '--sha', 'new', '--run-url', 'https://circleci.example/run', '--dry-run']) == 1
    result = json.loads(output.read_text())
    assert result['classification']['new'] == ['tests/a.py::test_x']
    assert result['filed'] == []
    assert result['plans'][0]['title'] == '[P2] Full suite: tests/a.py::test_x fails on main'


def test_real_pytest_legacy_junit_preserves_class_and_parameter(tmp_path):
    import os
    import subprocess
    import sys
    (tmp_path / 'test_sample.py').write_text('''import pytest
class TestBehavior:
    @pytest.mark.parametrize('value', ['a.b'])
    def test_value(self, value):
        assert value == 'expected'
''')
    output = tmp_path / 'junit.xml'
    done = subprocess.run([sys.executable, '-m', 'pytest', '--rootdir', str(tmp_path),
        '-o', 'junit_family=legacy', '--junitxml', str(output), 'test_sample.py'], cwd=tmp_path,
        env=dict(os.environ, PYTEST_DISABLE_PLUGIN_AUTOLOAD='1'), capture_output=True, text=True, timeout=60)
    assert done.returncode == 1, done.stdout + done.stderr
    rows = report.parse_junit(output.read_bytes())
    assert list(rows) == ['test_sample.py::TestBehavior::test_value[a.b]']
    assert rows['test_sample.py::TestBehavior::test_value[a.b]']['outcome'] == 'failed'


def test_junit_collection_error_keeps_its_file_identity():
    rows = report.parse_junit('''<testsuite><testcase name="tests/test_import.py" classname="">
        <error>ImportError: unavailable dependency</error></testcase></testsuite>''')
    assert rows['tests/test_import.py']['outcome'] == 'failed'
