"""CircleCI routes branch pushes without using TL3 runners or exceeding four queue tasks."""
from pathlib import Path
import re

import pytest

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


def run_command(tmp_path, command, *, plan_exit=0, runner_exit=0):
    """Execute the configured shell, replacing external services with local commands."""
    import os
    import subprocess
    bin_dir = tmp_path / 'bin'
    bin_dir.mkdir()
    programs = {
        'uv': '#!/bin/bash\nif [ "$1 $2" = "python find" ]; then echo /unused/python; exit 0; fi\nexit 9\n',
        'git': '#!/bin/bash\nif [ "$1" != fetch ]; then printf "%040d\\n" 1; fi\n',
        'python': '#!/bin/bash\ncase "$1" in\n'
                  ' *gate_select.py) if [ "$PLAN_EXIT" != 0 ]; then exit "$PLAN_EXIT"; fi; '
                  'printf "tests/test_selected.py\\n" > gate-artifacts/population.txt;;\n'
                  ' *gate_run.py) exit "$RUNNER_EXIT";;\nesac\n',
    }
    for name, source in programs.items():
        path = bin_dir / name
        path.write_text(source)
        path.chmod(0o755)
    environment = dict(os.environ, PATH=str(bin_dir) + os.pathsep + os.environ['PATH'],
                       BASH_ENV=str(tmp_path / 'bash-env'), PLAN_EXIT=str(plan_exit), RUNNER_EXIT=str(runner_exit))
    return subprocess.run(['bash', '-c', command], cwd=tmp_path, env=environment,
                          text=True, capture_output=True, check=False)


def execution_command():
    command = config()['commands']['gate-execute']['steps'][0]['run']['command']
    return command.replace('<< parameters.mode >>', 'selection').replace('<< parameters.lane >>', 'selection')


def test_environment_setup_failure_exits_no_verdict(tmp_path):
    command = config()['commands']['gate-setup']['steps'][1]['run']['command']
    result = run_command(tmp_path, command)
    assert result.returncode == 75
    assert 'NO_VERDICT' in result.stdout


def test_planning_failure_exits_no_verdict(tmp_path):
    result = run_command(tmp_path, execution_command(), plan_exit=1)
    assert result.returncode == 75
    assert 'NO_VERDICT' in result.stdout


@pytest.mark.parametrize('code,marker', [(0, 'GATE_PASS'), (1, 'GATE_TEST_FAILURE'), (75, 'NO_VERDICT')])
def test_explicit_runner_verdict_is_preserved(tmp_path, code, marker):
    result = run_command(tmp_path, execution_command(), runner_exit=code)
    assert result.returncode == code
    assert marker in result.stdout
    assert 'planning or execution infrastructure failed' not in result.stdout
