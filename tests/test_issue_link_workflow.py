"""The issue-link check accepts a body reference or a sidebar link, and nothing else."""
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
                 payload: {pull_request: {number: 1, body: scenario.body}}};
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


def test_action_is_pinned_to_a_full_commit_sha():
    step = yaml.safe_load(WORKFLOW.read_text())['jobs']['issue-link']['steps'][0]
    _, ref = step['uses'].split('@')
    assert len(ref) == 40, f'{ref} is not a 40-character commit SHA'
    assert all(character in '0123456789abcdef' for character in ref)
