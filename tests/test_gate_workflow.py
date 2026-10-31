"""The workflow admits trusted heads and retries infrastructure, never failed tests."""
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def workflow():
    return yaml.safe_load((ROOT / '.github/workflows/gate.yml').read_text())


def test_gate_has_no_self_hosted_job_for_ordinary_or_fork_prs():
    jobs = workflow()['jobs']
    condition = jobs['attempt_1']['if']
    assert 'github.event.pull_request.head.repo.full_name == github.repository' in condition
    assert "startsWith(github.head_ref, 'mergify/merge-queue/')" in condition
    assert "github.event.pull_request.user.login == 'mergify[bot]'" in condition
    assert 'github.event.pull_request.user.id == 37929162' in condition
    assert "github.event.pull_request.user.type == 'Bot'" in condition
    assert "'gate-canary'" in condition
    assert jobs['attempt_1']['runs-on'] == ['self-hosted', 'pb-fleet', 'pb-gate']
    assert jobs['attempt_2']['runs-on'] == jobs['attempt_1']['runs-on']
    assert jobs['gate']['name'] == 'gate'


def test_retry_is_once_and_only_without_a_verdict():
    jobs = workflow()['jobs']
    assert set(jobs) == {'attempt_1', 'attempt_2', 'gate'}
    assert 'always()' in jobs['attempt_2']['if']
    assert "needs.attempt_1.outputs.verdict == ''" in jobs['attempt_2']['if']
    assert "needs.attempt_1.result != 'skipped'" in jobs['attempt_2']['if']
    assert 'steps.tests.outputs.verdict' in jobs['attempt_1']['outputs']['verdict']
    assert 'steps.tests.outputs.verdict' in jobs['attempt_2']['outputs']['verdict']
    assert jobs['gate']['needs'] == ['attempt_1', 'attempt_2']
    assert 'always()' in jobs['gate']['if']


def test_attempts_share_exact_steps_and_checkout_the_integrated_head():
    config = workflow()
    assert config['permissions'] == {'contents': 'read'}
    steps = config['jobs']['attempt_1']['steps']
    assert steps == config['jobs']['attempt_2']['steps']
    checkout = next(step for step in steps if step.get('uses', '').startswith('actions/checkout'))
    assert checkout['with']['ref'] == '${{ github.event.pull_request.head.sha }}'
    assert checkout['with']['persist-credentials'] is False
    assert checkout['with']['fetch-depth'] == 1
    selection = next(step for step in steps if step.get('id') == 'selection')
    assert 'github.event.pull_request.base.sha' in selection['env']['BASE_SHA']
    assert 'github.event.pull_request.head.sha' in selection['env']['HEAD_SHA']
    assert '--base' in selection['run'] and '--head' in selection['run']
    assert any(step.get('uses', '').startswith('actions/upload-artifact') for step in steps)
    assert not any('setup-python' in step.get('uses', '') for step in steps)
