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

HERE = {'nameWithOwner': 'RobTand/prismabuild'}
ELSEWHERE = {'nameWithOwner': 'RobTand/prismaquant'}
ISSUE = {'number': 202}
A_PULL = {'number': 203, 'pull_request': {}}


def run(scenario):
    from tools.ci.issue_link import issue_link_verdict

    scenario = {'branch': 'ig/legacy', 'created_at': '2026-10-09T17:00:00Z', **scenario}
    pull = {'number': 1, 'body': scenario['body'], 'head': {'ref': scenario['branch']},
            'created_at': scenario['created_at']}
    verdict = issue_link_verdict(
        pull, 'RobTand/prismabuild',
        lambda number: scenario['issues'].get(str(number)),
        lambda number: None, lambda number: scenario['sidebar'],
    )
    return None if verdict.allowed else verdict.reason


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


@pytest.mark.parametrize('branch,body,issues,accepted', [
    ('prismabuild-123', 'Refs #123', {'123': {'number': 123}}, True),
    ('tl/release-operation', 'Refs #123', {'123': {'number': 123}}, True),
    ('tlx/release-operation', 'Refs #123', {'123': {'number': 123}}, False),
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


@pytest.mark.parametrize('branch,created_at,accepted', [
    ('ig/train/x-9', '2026-10-09T17:00:00Z', True),
    ('ig/older-pipeline', '2026-10-10T00:00:00Z', True),
    ('tl/release-operation', '2026-10-10T00:00:00Z', True),
    ('tlx/release-operation', '2026-10-10T00:00:00Z', False),
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


@pytest.mark.parametrize('branch,created_at', [
    ('ig/train/x-9', '2026-10-09T17:00:00Z'),
    ('tl/release-operation', '2026-10-10T00:00:00Z'),
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


def test_workflow_keeps_trigger_runner_name_and_read_permissions():
    workflow = yaml.safe_load(WORKFLOW.read_text())
    trigger = workflow.get('on', workflow.get(True))
    assert trigger == {'pull_request': {'types': ['opened', 'edited', 'synchronize', 'reopened', 'ready_for_review']}}
    assert workflow['permissions'] == {'contents': 'read', 'issues': 'read', 'pull-requests': 'read'}
    assert list(workflow['jobs']) == ['issue-link']
    assert workflow['jobs']['issue-link']['runs-on'] == 'ubuntu-latest'


@pytest.mark.skipif(NODE is None, reason='node is not installed on this worker')
@pytest.mark.parametrize('body,accepted', [('Refs #202', True), ('Refs #999', False)])
def test_workflow_downloads_and_runs_python_evaluator(tmp_path, body, accepted):
    event = tmp_path / 'event.json'
    event.write_text(json.dumps({'pull_request': {
        'number': 1, 'body': body, 'head': {'ref': 'prismabuild-202'},
        'created_at': '2026-10-10T00:00:00Z',
    }}))
    # Install the fake HTTP boundary before the downloaded helper imports urlopen.
    fake_api = r'''import io
from urllib.error import HTTPError
import urllib.request
def fixture_urlopen(request, **kwargs):
    if request.full_url == 'https://api.github.com/repos/RobTand/prismabuild/issues/202':
        return io.BytesIO(b'{"number":202}')
    raise HTTPError(request.full_url, 404, 'Not Found', {}, None)
urllib.request.urlopen = fixture_urlopen
'''
    evaluator = WORKFLOW.parents[2] / 'tools/ci/issue_link.py'
    source = evaluator.read_text().replace('from __future__ import annotations\n', '')
    helper_source = tmp_path / 'helper-source.py'
    helper_source.write_text(fake_api + source)
    script = yaml.safe_load(WORKFLOW.read_text())['jobs']['issue-link']['steps'][0]['with']['script']
    harness = r'''
const fs = require('fs');
const child = require('child_process');
const core = {failed: null, info() {}, setFailed(message) { this.failed = message; }};
const context = {repo: {owner: 'RobTand', repo: 'prismabuild'}, sha: 'integrated-sha'};
const github = {rest: {repos: {getContent: async ({ref, path}) => {
    if (ref !== 'integrated-sha' || path !== 'tools/ci/issue_link.py') throw new Error('Wrong evaluator source');
    const data = fs.readFileSync(process.env.HELPER_SOURCE);
    return {data: {type: 'file', encoding: 'base64', size: data.length, content: data.toString('base64')}};
}}}};
const exec = {getExecOutput: async (command, args) => {
    if (command !== 'python3' || args[0] !== '-I') throw new Error('Python must be isolated');
    const result = child.spawnSync(command, args, {encoding: 'utf8', env: process.env});
    if (result.error) throw result.error;
    return {exitCode: result.status, stdout: result.stdout, stderr: result.stderr};
}};
(async () => {
    await (async function () { SCRIPT })();
    console.log(JSON.stringify({failed: core.failed}));
})();
'''.replace('SCRIPT', script)
    result = subprocess.run([NODE, '-e', harness], text=True, capture_output=True,
                            env={**os.environ, 'RUNNER_TEMP': str(tmp_path),
                                 'GITHUB_EVENT_PATH': str(event), 'GITHUB_REPOSITORY': 'RobTand/prismabuild',
                                 'GITHUB_TOKEN': 'fixture-token', 'HELPER_SOURCE': str(helper_source)})
    assert result.returncode == 0, result.stderr
    failed = json.loads(result.stdout)['failed']
    assert (failed is None) is accepted
    if not accepted:
        # The check's annotation carries the policy reason itself, as it did
        # before the evaluator moved to Python.
        assert failed.startswith('This pull request violates the branch naming rule (rule 12). ')
        assert 'for example prismabuild-999.' in failed
