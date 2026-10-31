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
