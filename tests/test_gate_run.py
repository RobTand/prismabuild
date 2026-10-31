"""Test outcomes are final; an absent outcome alone permits an infrastructure retry."""
from __future__ import annotations

import importlib.util
import json
import os
import xml.etree.ElementTree as ET
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location('gate_run', ROOT / 'tools/ci/gate_run.py')
runner = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(runner)


@pytest.mark.parametrize('code,verdict', [(0, 'pass'), (1, 'fail')])
def test_completed_pytest_writes_a_final_verdict(tmp_path, code, verdict):
    calls = []
    def execute(command, **kwargs):
        calls.append((command, kwargs))
        return SimpleNamespace(returncode=code)
    result = runner.run_plan({'selected': [{'path': 'tests/test_one.py'}],
                             'deselect': ['tests/test_one.py::test_bad']},
                            root=tmp_path, output=tmp_path / 'output', junit=tmp_path / 'junit.xml',
                            execute=execute, affinity={11, 12, 13, 14, 15})
    assert result == (0 if code == 0 else 1)
    assert json.loads((tmp_path / 'output').read_text())['verdict'] == verdict
    command, kwargs = calls[0]
    assert command[command.index('-n') + 1] == '5'
    assert '--deselect=tests/test_one.py::test_bad' in command
    assert command[-1] == 'tests/test_one.py'
    assert kwargs['env']['PRISMABUILD_TEST_TIMEOUT_S'] == '60'
    assert kwargs['env']['OMP_NUM_THREADS'] == kwargs['env']['OPENBLAS_NUM_THREADS'] == '1'


@pytest.mark.parametrize('code', [2, 3, 4, 5])
def test_pytest_without_complete_test_verdict_is_infrastructure(tmp_path, code):
    result = runner.run_plan({'selected': [{'path': 'tests/test_one.py'}], 'deselect': []},
        root=tmp_path, output=tmp_path / 'output', junit=tmp_path / 'junit.xml',
        execute=lambda *a, **k: SimpleNamespace(returncode=code), affinity={1})
    assert result == 75
    assert json.loads((tmp_path / 'output').read_text())['verdict'] is None


def test_launch_failure_leaves_no_verdict(tmp_path):
    def unavailable(*args, **kwargs):
        raise OSError('interpreter unavailable')
    result = runner.run_plan({'selected': [{'path': 'tests/test_one.py'}], 'deselect': []},
                        root=tmp_path, output=tmp_path / 'output', junit=tmp_path / 'junit.xml',
                        execute=unavailable, affinity={1})
    assert result == 75
    assert json.loads((tmp_path / 'output').read_text())['verdict'] is None


def test_killed_pytest_leaves_no_verdict(tmp_path):
    result = runner.run_plan({'selected': [{'path': 'tests/test_one.py'}], 'deselect': []},
                        root=tmp_path, output=tmp_path / 'output', junit=tmp_path / 'junit.xml',
                        execute=lambda *a, **k: SimpleNamespace(returncode=-9), affinity={1})
    assert result == 75
    assert json.loads((tmp_path / 'output').read_text())['verdict'] is None


def test_empty_selection_cannot_default_to_running_the_entire_suite(tmp_path):
    result = runner.run_plan({'selected': [], 'deselect': []}, root=tmp_path,
                        output=tmp_path / 'output', junit=tmp_path / 'junit.xml',
                        execute=lambda *a, **k: pytest.fail('must not start pytest'), affinity={1})
    assert result == 75


def test_runner_uses_assigned_files_and_job_cpus_with_circleci_junit(tmp_path):
    (tmp_path / 'tests').mkdir()
    (tmp_path / 'pytest.ini').write_text('[pytest]\n')
    (tmp_path / 'tests/test_sample.py').write_text(
        'import math\ndef test_math(): assert math.isqrt(81) == 8\n')
    result = runner.run_plan({'selected': [{'path': 'tests/test_sample.py'},
                                         {'path': 'tests/test_unassigned.py'}], 'deselect': []},
        files=['tests/test_sample.py'], workers=1, root=tmp_path,
        output=tmp_path / 'result.json', junit=tmp_path / 'junit.xml')
    assert result == 1
    record = json.loads((tmp_path / 'result.json').read_text())
    assert record['files'] == ['tests/test_sample.py'] and record['verdict'] == 'fail'
    cases = list(ET.parse(tmp_path / 'junit.xml').iter('testcase'))
    assert len(cases) == 1 and cases[0].get('file') == 'tests/test_sample.py'
    assert cases[0].find('failure') is not None


def test_runner_rejects_files_outside_the_plan_before_launch(tmp_path):
    result = runner.run_plan({'selected': [{'path': 'tests/test_one.py'}], 'deselect': []},
        files=['tests/test_other.py'], root=tmp_path, output=tmp_path / 'output',
        junit=tmp_path / 'junit.xml', execute=lambda *a, **k: pytest.fail('must not launch'))
    assert result == 75


def test_newline_assignment_preserves_a_path_containing_spaces(tmp_path):
    (tmp_path / 'tests').mkdir()
    (tmp_path / 'pytest.ini').write_text('[pytest]\n')
    filename = 'tests/test with spaces.py'
    (tmp_path / filename).write_text('def test_value(): assert 7 * 6 == 42\n')
    (tmp_path / 'plan.json').write_text(json.dumps({'selected': [{'path': filename}], 'deselect': []}))
    (tmp_path / 'assigned.txt').write_text(filename + '\n')
    result = runner.main(['--plan', str(tmp_path / 'plan.json'), '--root', str(tmp_path),
        '--files-from', str(tmp_path / 'assigned.txt'), '--workers', '1',
        '--result', str(tmp_path / 'result.json'), '--junitxml', str(tmp_path / 'junit.xml')])
    assert result == 0
    assert json.loads((tmp_path / 'result.json').read_text())['files'] == [filename]
