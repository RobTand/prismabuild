"""The metadata-only check validates issue links and the pull request branch."""
from pathlib import Path
import json
import os
import shutil
import subprocess

import pytest
import yaml

WORKFLOW = Path(__file__).resolve().parents[1] / '.github/workflows/issue-link.yml'
NODE = shutil.which('node')

HARNESS = """
const scenario = JSON.parse(process.env.PB202_SCENARIO);
const core = {
    failed: null,
    info() {},
    setFailed(message) { this.failed = message; },
};
const github = {
    rest: {issues: {get: async ({issue_number}) => {
        const issue = scenario.issues[String(issue_number)];
        if (!issue) { const error = new Error('Not Found'); error.status = 404; throw error; }
        return {data: issue};
    }}},
    graphql: async () => ({repository: {pullRequest: {
        closingIssuesReferences: {nodes: scenario.sidebar}}}}),
};
const context = {repo: {owner: 'RobTand', repo: 'prismabuild'},
                 payload: {pull_request: {
                     number: 1, body: scenario.body,
                     head: {ref: scenario.branch},
                     created_at: scenario.created_at,
                 }}};
(async () => {
    await (async function () { SCRIPT })();
    console.log(JSON.stringify({failed: core.failed}));
})();
"""

HERE = {'nameWithOwner': 'RobTand/prismabuild'}
ELSEWHERE = {'nameWithOwner': 'RobTand/prismaquant'}
ISSUE = {'number': 202}
A_PULL = {'number': 203, 'pull_request': {}}


def run(scenario):
    scenario = {'branch': 'ig/legacy', 'created_at': '2026-10-09T17:00:00Z', **scenario}
    script = yaml.safe_load(WORKFLOW.read_text())['jobs']['issue-link']['steps'][0]['with']['script']
    harness = HARNESS.replace('SCRIPT', script)
    result = subprocess.run([NODE, '-e', harness], text=True, capture_output=True,
                            env={**os.environ, 'PB202_SCENARIO': json.dumps(scenario)})
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)['failed']


@pytest.mark.skipif(NODE is None, reason='node is not installed on this worker')
@pytest.mark.parametrize('body,issues,sidebar,accepted', [
    ('Refs #202', {'202': ISSUE}, [], True),
    ('Fixes #202', {'202': ISSUE}, [], True),
    ('Closes #202', {'202': ISSUE}, [], True),
    ('Resolves #202', {'202': ISSUE}, [], True),
    ('Refs https://github.com/RobTand/prismabuild/issues/202', {'202': ISSUE}, [], True),
    ('No link at all.', {'202': ISSUE}, [], False),
    ('Refs #999', {'202': ISSUE}, [], False),
    ('Refs #203', {'203': A_PULL}, [], False),
    ('Mentions #202 without a keyword', {'202': ISSUE}, [], False),
    ('No body keyword.', {}, [{'number': 202, 'repository': HERE}], True),
    ('No body keyword.', {}, [{'number': 202, 'repository': ELSEWHERE}], False),
    ('Refs #999', {}, [{'number': 202, 'repository': HERE}], True),
])
def test_issue_link_decision(body, issues, sidebar, accepted):
    failed = run({'body': body, 'issues': issues, 'sidebar': sidebar})
    assert (failed is None) is accepted
    if not accepted:
        assert 'Refs #123' in failed and 'sidebar' in failed


@pytest.mark.skipif(NODE is None, reason='node is not installed on this worker')
@pytest.mark.parametrize('branch,body,issues,accepted', [
    ('prismabuild-123', 'Refs #123', {'123': {'number': 123}}, True),
    ('prismabuild-123-gpu', 'Refs #123', {'123': {'number': 123}}, True),
    ('prismabuild-123-gpu-2', 'Refs #123', {'123': {'number': 123}}, True),
    ('prismabuild-123', 'Fixes https://github.com/RobTand/prismabuild/issues/123',
     {'123': {'number': 123}}, True),
    ('prismabuild-123', 'Refs RobTand/prismabuild#123', {'123': {'number': 123}}, True),
    ('prismabuild-123', 'Refs #202. Refs #123',
     {'202': ISSUE, '123': {'number': 123}}, True),
    ('fix/something', 'Refs #123', {'123': {'number': 123}}, False),
    ('prismabuild-124', 'Refs #123', {'123': {'number': 123}}, False),
    ('prismabuild-123-', 'Refs #123', {'123': {'number': 123}}, False),
    ('prismabuild-123-GPU', 'Refs #123', {'123': {'number': 123}}, False),
    ('prismabuild-123-_gpu', 'Refs #123', {'123': {'number': 123}}, False),
    ('prismabuild-123', 'Refs #202. Refs #123',
     {'202': ISSUE, '123': {'number': 123, 'pull_request': {}}}, False),
    ('prismabuild-123', 'Refs #202. Refs #123', {'202': ISSUE}, False),
    ('prismabuild-123', 'Refs RobTand/prismaquant#123', {'123': {'number': 123}}, False),
    ('prismabuild-123', 'Mentions #123 without a keyword', {'123': {'number': 123}}, False),
])
def test_branch_requires_matching_actual_body_issue(branch, body, issues, accepted):
    failed = run({'branch': branch, 'body': body, 'issues': issues,
                  'sidebar': [{'number': 123, 'repository': HERE}]})
    assert (failed is None) is accepted
    if not accepted:
        assert 'branch naming rule' in failed
        assert 'prismabuild-<issue>' in failed


@pytest.mark.skipif(NODE is None, reason='node is not installed on this worker')
@pytest.mark.parametrize('branch,created_at,accepted', [
    ('ig/train/x-9', '2026-10-09T17:00:00Z', True),
    ('ig/older-pipeline', '2026-10-10T00:00:00Z', True),
    ('release', '2026-10-09T17:00:00Z', True),
    ('release/1.0', '2026-10-10T00:00:00Z', True),
    ('release-1.0', '2026-10-10T00:00:00Z', True),
    ('fix/something', '2026-10-09T16:59:59Z', True),
    ('fix/something', '2026-10-09T17:00:00Z', False),
    ('fix/something', '2026-10-09T17:00:01Z', False),
])
def test_branch_exemptions_and_cutoff(branch, created_at, accepted):
    # A sidebar issue still satisfies the existing policy for exempt branches.
    failed = run({'branch': branch, 'created_at': created_at, 'body': '',
                  'issues': {}, 'sidebar': [{'number': 123, 'repository': HERE}]})
    assert (failed is None) is accepted
    if not accepted:
        assert 'branch naming rule' in failed
        assert 'prismabuild-<issue>' in failed


@pytest.mark.skipif(NODE is None, reason='node is not installed on this worker')
@pytest.mark.parametrize('branch,created_at', [
    ('ig/train/x-9', '2026-10-09T17:00:00Z'),
    ('release/1.0', '2026-10-09T17:00:00Z'),
    ('fix/something', '2026-10-09T16:59:59Z'),
])
@pytest.mark.parametrize('body,issues', [
    ('No issue link.', {}),
    ('Refs #999', {}),
    ('Refs #203', {'203': A_PULL}),
])
def test_exempt_branches_still_require_actual_issues(branch, created_at, body, issues):
    failed = run({'branch': branch, 'created_at': created_at, 'body': body,
                  'issues': issues, 'sidebar': []})
    assert 'Refs #123' in failed and 'sidebar' in failed


@pytest.mark.skipif(NODE is None, reason='node is not installed on this worker')
def test_subject_branch_rejects_sidebar_only_issue():
    failed = run({'branch': 'prismabuild-123', 'body': '', 'issues': {},
                  'sidebar': [{'number': 123, 'repository': HERE}]})
    assert 'branch naming rule' in failed
    assert 'prismabuild-123' in failed


def test_action_is_pinned_to_a_full_commit_sha():
    step = yaml.safe_load(WORKFLOW.read_text())['jobs']['issue-link']['steps'][0]
    _, ref = step['uses'].split('@')
    assert len(ref) == 40, f'{ref} is not a 40-character commit SHA'
    assert all(character in '0123456789abcdef' for character in ref)
