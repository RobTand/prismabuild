"""Test outcomes are final; an absent outcome alone permits an infrastructure retry."""
from __future__ import annotations

import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location('gate_run', ROOT / 'tools/ci/gate_run.py')
runner = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(runner)


@pytest.mark.parametrize('code,verdict', [(0, 'pass'), (1, 'fail'), (2, 'fail'), (3, 'fail'), (4, 'fail'), (5, 'fail')])
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
    assert (tmp_path / 'output').read_text() == f'verdict={verdict}\n'
    command, kwargs = calls[0]
    assert command[command.index('-n') + 1] == '5'
    assert '--deselect=tests/test_one.py::test_bad' in command
    assert command[-1] == 'tests/test_one.py'
    assert kwargs['env']['PRISMABUILD_TEST_TIMEOUT_S'] == '60'
    assert kwargs['env']['OMP_NUM_THREADS'] == kwargs['env']['OPENBLAS_NUM_THREADS'] == '1'


def test_launch_failure_leaves_no_verdict(tmp_path):
    def unavailable(*args, **kwargs):
        raise OSError('interpreter unavailable')
    with pytest.raises(OSError):
        runner.run_plan({'selected': [{'path': 'tests/test_one.py'}], 'deselect': []},
                        root=tmp_path, output=tmp_path / 'output', junit=tmp_path / 'junit.xml',
                        execute=unavailable, affinity={1})
    assert not (tmp_path / 'output').exists()


def test_killed_pytest_leaves_no_verdict(tmp_path):
    with pytest.raises(RuntimeError, match='signal'):
        runner.run_plan({'selected': [{'path': 'tests/test_one.py'}], 'deselect': []},
                        root=tmp_path, output=tmp_path / 'output', junit=tmp_path / 'junit.xml',
                        execute=lambda *a, **k: SimpleNamespace(returncode=-9), affinity={1})
    assert not (tmp_path / 'output').exists()


def test_empty_selection_cannot_default_to_running_the_entire_suite(tmp_path):
    with pytest.raises(ValueError, match='empty'):
        runner.run_plan({'selected': [], 'deselect': []}, root=tmp_path,
                        output=tmp_path / 'output', junit=tmp_path / 'junit.xml',
                        execute=lambda *a, **k: pytest.fail('must not start pytest'), affinity={1})
