"""The Merge gate workflow keeps the check names and queue contract Mergify and TL3 read."""
from __future__ import annotations

from pathlib import Path
import re

import yaml

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = yaml.safe_load((ROOT / '.github/workflows/gate.yml').read_text())
HOSTED_LANE = yaml.safe_load((ROOT / '.github/actions/hosted-lane/action.yml').read_text())
MERGIFY = yaml.safe_load((ROOT / '.mergify.yml').read_text())
JOBS = WORKFLOW['jobs']
BATCH_DRAFT = ("github.event.pull_request.head.repo.full_name == github.repository && "
               "startsWith(github.head_ref, 'mergify/merge-queue/') && "
               "github.event.pull_request.user.login == 'mergify[bot]' && "
               "github.event.pull_request.user.id == 37929162")


def steps(job):
    return JOBS[job]['steps']


def test_label_changes_do_not_rerun_the_gate_and_main_pushes_run_post_merge():
    triggers = WORKFLOW[True] if True in WORKFLOW else WORKFLOW['on']
    assert set(triggers['pull_request']['types']) == {'opened', 'synchronize', 'reopened'}
    assert triggers['push']['branches'] == ['main']
    assert JOBS['post-merge']['if'] == "github.event_name == 'push'"
    run = next(step['run'] for step in steps('post-merge') if 'remote_gate.py main' in step.get('run', ''))
    assert '--base "$BEFORE" --head "$AFTER"' in run


def test_required_check_names_and_who_produces_them_are_unchanged():
    assert JOBS['pr-gate']['name'] == 'pr-gate' and JOBS['gate']['name'] == 'gate'
    assert f'!({BATCH_DRAFT})' in JOBS['pr-gate']['if']
    assert f'&& ({BATCH_DRAFT})' in JOBS['gate']['if']
    for job in ('pr-gate', 'gate'):
        assert JOBS[job]['needs'] == ['bazel', 'hosted']
        assert any('remote_gate.py aggregate' in step.get('run', '') for step in steps(job))
        checkout = next(step for step in steps(job) if 'actions/checkout@' in step.get('uses', ''))
        sparse = checkout['with'].get('sparse-checkout', '').split()
        assert not sparse or {'tools/ci', 'tools/bazel'} <= set(sparse), 'the aggregate reads both'
    names = {job.get('name', key) for key, job in JOBS.items()}
    assert not {name for name in names if name.startswith('shard-')}


def test_every_third_party_action_is_pinned_by_commit():
    uses = [step['uses'] for job in JOBS.values() for step in job.get('steps', []) if 'uses' in step]
    uses += [step['uses'] for step in HOSTED_LANE['runs']['steps'] if 'uses' in step]
    for use in uses:
        if use.startswith('./'):
            continue
        assert re.fullmatch(r'[\w.-]+/[\w.-]+@[0-9a-f]{40}', use), use


def test_scopes_reach_mergify_or_the_gate_fails():
    bazel = steps('bazel')
    refs = next(step for step in bazel if step.get('id') == 'refs')
    assert 'Mergifyio/gha-mergify-ci@' in refs['uses'] and refs['with']['action'] == 'scopes-git-refs'
    token = next(step for step in bazel if step.get('id') == 'token')
    assert 'test -n "$MERGIFY_TOKEN"' in token['run'] and 'exit 1' in token['run']
    upload = next(step for step in bazel if step.get('id') == 'upload')
    assert upload['with']['action'] == 'scopes-upload' and 'MERGIFY_TOKEN' in upload['with']['token']
    assert "steps.token.outcome == 'success'" in upload['if']
    uploaded = next(step for step in bazel if step.get('id') == 'uploaded')
    assert uploaded['if'] == 'always()' and 'exit 1' in uploaded['run']


def test_ordinary_pull_requests_test_the_merge_commit_and_drafts_their_head():
    target = next(step for step in steps('bazel') if step.get('id') == 'target')
    assert 'MERGE: ${{ github.sha }}' in yaml.safe_dump(target) or target['env']['MERGE'] == '${{ github.sha }}'
    assert 'git rev-parse "$MERGE^1"' in target['run']
    assert 'base="$REFS_BASE" head="$REFS_HEAD"' in target['run']


def test_the_hosted_lane_runs_selected_files_with_the_old_gate_tooling():
    lane = yaml.safe_dump(HOSTED_LANE)
    for needed in ('tools/ci/gate_requirements.txt', 'tools/ci/gate_select.py', 'tools/ci/gate_setup.py',
                   'tools/ci/gate_run.py'):
        assert needed in lane
    assert 'needs' not in JOBS['hosted'], 'the hosted lane must not wait for the Bazel job'
    assert any('remote_gate.py hosted-files' in step.get('run', '') for step in steps('hosted'))
    matrix = JOBS['post-merge-hosted']['strategy']['matrix']['runner']
    assert matrix == ['ubuntu-24.04', 'ubuntu-24.04-arm']


def test_the_queue_stays_serial_with_scopes_and_its_tl3_route():
    assert MERGIFY['scopes'] == {'source': {'manual': None}}
    queue = MERGIFY['merge_queue']
    assert queue['mode'] == 'serial' and queue['max_parallel_checks'] == 2
    rules = {rule['name']: rule for rule in MERGIFY['queue_rules']}
    assert list(rules) == ['release', 'default']
    for rule in rules.values():
        assert rule['merge_conditions'] == ['check-success = gate']
        assert rule['merge_method'] == 'merge' and rule['max_checks_retries'] == 0
    assert 'check-success = pr-gate' in rules['release']['queue_conditions']
    names = [rule['name'] for rule in MERGIFY['pull_request_rules']]
    assert 'Queue TL3-approved PRs' in names
    assert 'direct_merge' not in yaml.safe_dump(MERGIFY)
