"""CircleCI routes branch pushes without using TL3 runners or exceeding four queue tasks."""
from pathlib import Path
import re

import yaml

ROOT = Path(__file__).resolve().parents[1]


def config():
    return yaml.safe_load((ROOT / '.circleci/config.yml').read_text())


def test_circleci_classes_parallelism_and_status_names():
    jobs = config()['jobs']
    assert set(jobs) == {'pr-gate', 'queue-gate-heavy', 'queue-gate-light'}
    assert jobs['pr-gate']['resource_class'] == 'robtand/fast-arm64'
    assert jobs['queue-gate-heavy']['resource_class'] == 'robtand/fast-arm64'
    assert jobs['queue-gate-light']['resource_class'] == 'robtand/slow-x64'
    assert sum(jobs[name]['parallelism'] for name in ('queue-gate-heavy', 'queue-gate-light')) == 4
    assert all(job['machine'] is True for job in jobs.values())
    assert 'robtand/fast-x64' not in (ROOT / '.circleci/config.yml').read_text()


def test_branch_filters_separate_prs_main_and_full_queue_canaries():
    flows = config()['workflows']
    ignored = flows['pull-requests']['jobs'][0]['pr-gate']['filters']['branches']['ignore']
    queue = flows['merge-queue']['jobs'][0]['queue-gate-heavy']['filters']['branches']['only']
    def matches(pattern, branch):
        return re.fullmatch(pattern[1:-1], branch) if pattern.startswith('/') else pattern == branch
    for branch in ('main', 'mergify/merge-queue/batch', 'gate-canary/trial'):
        assert any(matches(pattern, branch) for pattern in ignored)
    assert not any(matches(pattern, 'prismabuild-2428-gate') for pattern in ignored)
    assert matches(queue, 'mergify/merge-queue/batch') and matches(queue, 'gate-canary/trial')
    assert not matches(queue, 'main') and not matches(queue, 'prismabuild-2428-gate')


def test_dependencies_retries_and_circleci_timing_contract():
    text = (ROOT / '.circleci/config.yml').read_text()
    assert 'pytest==9.1.1' in text and 'pytest-xdist==3.8.0' in text and 'PyYAML==6.0.3' in text
    assert 'for attempt in 1 2 3' in text
    assert '--split-by=timings --timings-type=filename' in text
    assert '--workers 4' in text
    assert 'store_test_results' in text and 'store_artifacts' in text
    assert 'NO_VERDICT' in text
