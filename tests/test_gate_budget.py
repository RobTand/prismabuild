"""CI costs may shrink; complete test outcomes cannot hide missing cost evidence."""
import importlib.util
import json
from pathlib import Path
import subprocess
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location('gate_run', ROOT / 'tools/ci/gate_run.py')
runner = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(runner)


def policy(root, seconds=20, slow=None):
    target = root / 'tools/ci/gate_budget.json'
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps({'seconds': seconds, 'slow': slow or {}}))


def budget_run(tmp_path, xml, *, base=None, pytest_code=0):
    plan = {'selected': [{'path': 'tests/test_sample.py'}], 'deselect': [], 'base_sha': base}
    def execute(*args, **kwargs):
        (tmp_path / 'junit.xml').write_text(xml)
        return SimpleNamespace(returncode=pytest_code)
    code = runner.run_plan(plan, root=tmp_path, output=tmp_path / 'result.json',
                           junit=tmp_path / 'junit.xml', execute=execute, affinity={1})
    return code, json.loads((tmp_path / 'result.json').read_text())


def case(seconds='21', *, classname='tests.test_sample', name='test_cost', time_attr=True):
    timing = f' time="{seconds}"' if time_attr else ''
    return (f'<testsuites><testsuite><testcase file="tests/test_sample.py" '
            f'classname="{classname}" name="{name}"{timing}/></testsuite></testsuites>')


def commit_target(tmp_path):
    def git(*args):
        return subprocess.check_output(['git', '-C', str(tmp_path), *args], text=True).strip()
    git('init', '-q')
    git('add', '.')
    git('-c', 'user.name=Gate fixture', '-c', 'user.email=gate@example.invalid', 'commit', '-qm', 'Target')
    return git('rev-parse', 'HEAD')


def test_overrun_is_a_final_failure_even_when_pytest_passed(tmp_path, capsys):
    policy(tmp_path)
    code, result = budget_run(tmp_path, case())
    assert code == 1
    assert result['verdict'] == 'fail' and result['pytest_exit_code'] == 0
    assert result['budget']['status'] == 'fail'
    assert result['budget']['overruns'] == [
        {'nodeid': 'tests/test_sample.py::test_cost', 'seconds': 21.0, 'budget_seconds': 20}]
    assert 'tests/test_sample.py::test_cost' in capsys.readouterr().out


@pytest.mark.parametrize('seconds,slow', [
    (21, {}), (20, {'tests/test_sample.py::test_cost': 40}),
    (20, {'tests/test_sample.py::test_known': 41}),
])
def test_growth_is_checked_against_the_actual_target_commit(tmp_path, seconds, slow):
    policy(tmp_path, slow={'tests/test_sample.py::test_known': 40})
    base = commit_target(tmp_path)
    policy(tmp_path, seconds, slow)
    code, result = budget_run(tmp_path, case('0'), base=base)
    assert code == 1
    assert result['budget']['status'] == 'fail'
    assert 'grow' in result['budget']['reason']
    assert result['pytest_exit_code'] is None


def test_shrink_and_removed_exceptions_are_allowed(tmp_path):
    policy(tmp_path, slow={'tests/test_sample.py::test_known': 40, 'removed': 60})
    base = commit_target(tmp_path)
    policy(tmp_path, 10, {'tests/test_sample.py::test_known': 30})
    code, result = budget_run(tmp_path, case('10'), base=base)
    assert code == 0
    assert result['budget']['status'] == 'pass'
    assert result['budget']['baseline_sha'] == base


def test_absent_target_policy_bootstraps_but_missing_target_object_does_not(tmp_path):
    (tmp_path / 'tracked').write_text('baseline predates the policy')
    base = commit_target(tmp_path)
    policy(tmp_path)
    assert budget_run(tmp_path, case('1'), base=base)[0] == 0
    code, result = budget_run(tmp_path, case('1'), base='f' * 40)
    assert code == 75 and result['verdict'] is None
    assert 'target' in result['infrastructure_reason']


@pytest.mark.parametrize('xml', [
    case(time_attr=False), case('nan'), case('-1'), case('inf'),
    '<testsuites/>', '<not-xml',
    '<testsuites><testcase name="test_cost" time="1"/></testsuites>',
])
def test_missing_or_invalid_timing_is_no_verdict(tmp_path, xml):
    policy(tmp_path)
    code, result = budget_run(tmp_path, xml)
    assert code == 75 and result['verdict'] is None
    assert 'infrastructure_reason' in result


def test_known_test_failure_is_never_retried_even_if_junit_is_missing(tmp_path):
    policy(tmp_path)
    code, result = budget_run(tmp_path, '<not-xml', pytest_code=1)
    assert code == 1 and result['verdict'] == 'fail'
    assert result['pytest_exit_code'] == 1
    assert result['budget']['status'] == 'unavailable'


def test_class_and_parameter_id_use_the_precise_exception(tmp_path):
    policy(tmp_path, slow={'tests/test_sample.py::TestCosts::test_cost[a.b]': 42})
    code, result = budget_run(tmp_path, case('41', classname='tests.test_sample.TestCosts',
                                          name='test_cost[a.b]'))
    assert code == 0 and result['budget']['status'] == 'pass'


@pytest.mark.parametrize('seconds', [True, 0, -1, float('nan'), float('inf')])
def test_invalid_policy_limits_fail_closed(tmp_path, seconds):
    policy(tmp_path, seconds)
    code, result = budget_run(tmp_path, case('0'))
    assert code == 1 and result['budget']['status'] == 'fail'
