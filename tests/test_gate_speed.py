"""Bound CI presentation and share balanced coverage with large PR selections."""
import importlib.util
from pathlib import Path
import subprocess

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]


def load(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / 'tools/ci' / (name + '.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_selection_summary_is_bounded_and_links_complete_artifact():
    selector = load('gate_select')
    rows = [{'path': f'tests/test_{i:04d}.py',
             'reasons': ['direct import: ' + 'module/' * 2000, 'fast tier'],
             'estimated_seconds': 1} for i in range(2000)]
    plan = {'population': {'version': 'v1', 'size': 2000}, 'selected': rows,
            'estimate': {'wall_seconds': 400},
            'deferred': [{'path': f'tests/other_{i:04d}.py', 'reason': 'lane ' * 2000,
                          'issue': 1997} for i in range(1000)]}
    text = selector.summary(plan)
    assert len(text.encode()) < 128 * 1024
    assert 'Selected 2000 of 2000' in text
    assert 'tests/test_0000.py' in text and 'tests/test_1999.py' not in text
    assert 'direct import: 2000' in text and 'fast tier: 2000' in text
    assert 'plan.json' in text and '1950 more selected' in text
    assert len(plan['selected']) == 2000 and len(plan['deferred']) == 1000


@pytest.mark.parametrize('seconds,count', [(360, 1), (360.01, 16), (4800, 16)])
def test_pr_selection_uses_one_shard_only_below_the_wall_time_threshold(seconds, count):
    partition = load('gate_partition')
    files = ['slow.py', *[f'fast_{i}.py' for i in range(15)]]
    assert partition.selection_shard_count(files, dict.fromkeys(files, seconds / len(files)), 16,
                                            workers=4, single_shard_seconds=90) == count


def test_unknown_pr_files_are_not_free_when_deciding_to_fan_out():
    partition = load('gate_partition')
    assert partition.selection_shard_count([f'new_{i}.py' for i in range(16)], {'known.py': 400}, 16,
                                            workers=4, single_shard_seconds=90) == 16


def test_required_pr_gate_waits_for_the_shared_shards_and_retry():
    jobs = yaml.safe_load((ROOT / '.github/workflows/gate.yml').read_text())['jobs']
    assert {'plan', 'shards', 'retry-plan', 'retry-shards'} <= set(jobs['pr-gate']['needs'])
    assert jobs['pr-gate']['name'] == 'pr-gate'
    plan_script = '\n'.join(step.get('run', '') for step in jobs['plan']['steps'])
    assert '--mode' in plan_script and '--single-shard-seconds' in plan_script


def test_old_pr_head_is_a_final_failure_before_dependency_setup(tmp_path):
    jobs = yaml.safe_load((ROOT / '.github/workflows/gate.yml').read_text())['jobs']
    checks = [step for step in jobs['plan']['steps'] if step.get('id') == 'head-contract']
    assert len(checks) == 1, 'head compatibility must be checked before setup-python caching'
    steps = jobs['plan']['steps']
    assert steps.index(checks[0]) < next(i for i, step in enumerate(steps)
                                       if step.get('uses', '').startswith('actions/setup-python@'))
    output = tmp_path / 'output'
    result = subprocess.run(['bash', '-c', checks[0]['run']], cwd=tmp_path,
                            env={'GITHUB_OUTPUT': str(output), 'PATH': '/usr/bin:/bin'},
                            text=True, capture_output=True, check=False)
    assert result.returncode == 1
    assert 'merge origin/main' in result.stdout
    assert 'compatible=false' in output.read_text()
    assert 'NO_VERDICT' not in result.stdout


def test_pr_shard_count_uses_file_packing_and_avoids_empty_shards():
    partition = load('gate_partition')
    assert partition.selection_shard_count(['long.py', 'small.py'],
        {'long.py': 100, 'small.py': 1}, 16, workers=4, single_shard_seconds=90) == 2
    assert partition.selection_shard_count(['long.py', 'zero.py'],
        {'long.py': 400, 'zero.py': 0}, 16, workers=4, single_shard_seconds=90) == 1
