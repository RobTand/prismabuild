"""All 27 main traces, with a portable, PB-captured immutable reference."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
REFERENCE = ROOT / 'tests/fixtures/cpu_lending_main_43234d0f.json'
REFERENCE_SHA256 = 'a2d44e3a66c6e2d73b5b441ff406a77591964a2d8e7600a9be37df49a58d2a2c'
BASE = json.loads(REFERENCE.read_bytes())
SCENARIOS = sorted(BASE['outcomes'])


def run_trace(tmp_path, scenario, ramp_cap=0):
    builder = scenario.startswith('builder_')
    name = scenario.removeprefix('builder_')
    fixture = ROOT / 'tests/compat/support' / (
        'cpu_lending_cap0_fixture.py' if builder else 'cpu_lending_review_cap0_fixture.py')
    tmp_path.mkdir(parents=True, exist_ok=True)
    command = [sys.executable, str(fixture), str(tmp_path), name]
    if ramp_cap:
        injection = """import runpy,sys
from prismabuild import adaptive_cpu
original = adaptive_cpu.Controller.__init__
def configured(self, *args, **kwargs):
    kwargs['ramp_cap'] = 2
    original(self, *args, **kwargs)
adaptive_cpu.Controller.__init__ = configured
sys.argv = sys.argv[1:]
runpy.run_path(sys.argv[0], run_name='__main__')
"""
        command = [sys.executable, '-c', injection, str(fixture), str(tmp_path), name]
    result = subprocess.run(command,
        cwd=tmp_path, env=dict(os.environ, PYTHONPATH=str(ROOT / 'src'),
                              PRISMABUILD_BOX_STATE_ROOT=str(tmp_path / 'box-state')),
        capture_output=True, text=True, timeout=600)
    assert result.returncode == 0, result.stderr[-2000:]
    value = json.loads(result.stdout)
    return value if builder else value['steps']


def test_main_reference_is_pinned_to_adopted_source_and_fixtures():
    assert hashlib.sha256(REFERENCE.read_bytes()).hexdigest() == REFERENCE_SHA256
    assert BASE['main_commit'] == '43234d0fbd42d6ed76da38658cb9f447ed19a606'
    assert len(SCENARIOS) == 27
    for name, expected in BASE['fixture_sha256'].items():
        assert hashlib.sha256((ROOT / 'tests/compat/support' / name).read_bytes()).hexdigest() == expected


def test_capture_reference_works_without_git_history_or_executable(tmp_path):
    checkout = tmp_path / 'snapshot'
    shutil.copytree(ROOT / 'src', checkout / 'src')
    for name in ('compat/support', 'fixtures'):
        shutil.copytree(ROOT / 'tests' / name, checkout / 'tests' / name)
    shutil.copyfile(ROOT / 'tests/history_inputs.py', checkout / 'tests/history_inputs.py')
    empty = tmp_path / 'empty-path'
    empty.mkdir()
    result = subprocess.run([sys.executable,
        str(checkout / 'tests/compat/support/capture_cpu_lending_main.py')], cwd=checkout,
        env=dict(os.environ, PATH=str(empty), PYTHONPATH=str(checkout / 'src'),
                 PRISMABUILD_BOX_STATE_ROOT=str(tmp_path / 'box-state')),
        text=True, capture_output=True, timeout=600)
    assert result.returncode == 0, result.stdout[-4000:] + result.stderr[-2000:]
    captured = json.loads(result.stdout.removeprefix('CPU_MAIN_REFERENCE '))
    assert captured == {key: value for key, value in BASE.items() if key != 'capture'}


@pytest.mark.parametrize('scenario', SCENARIOS)
def test_cap_zero_matches_main(tmp_path, scenario):
    assert run_trace(tmp_path, scenario) == BASE['outcomes'][scenario]


def test_b4_multislot_gpu_borrow_survives_five_admission_passes(tmp_path):
    scenario = 's20_multislot_gpu_row_borrows_cpu'
    for attempt in range(5):
        trace = run_trace(tmp_path / str(attempt), scenario)
        assert trace == BASE['outcomes'][scenario]
        candidate = dict(trace)['multislot_gpu_row']
        assert candidate['acquired'] and candidate['borrowed'] == [0, 1]
        assert 'acquire_error' not in candidate


def test_b3_builder_cases_work_without_git_history_or_git_executable(tmp_path):
    checkout = tmp_path / 'snapshot'
    shutil.copytree(ROOT / 'src', checkout / 'src')
    support = checkout / 'tests/compat/support'
    support.mkdir(parents=True)
    shutil.copyfile(ROOT / 'tests/compat/support/cpu_lending_cap0_fixture.py', support / 'cpu_lending_cap0_fixture.py')
    shutil.copytree(ROOT / 'tests/fixtures', checkout / 'tests/fixtures')
    shutil.copyfile(Path(__file__), checkout / 'tests/test_cpu_lending_main_equivalence.py')
    empty = tmp_path / 'empty-path'
    empty.mkdir()
    result = subprocess.run([sys.executable, '-m', 'pytest', '-q', *[
        str(checkout / 'tests/test_cpu_lending_main_equivalence.py')
        + '::test_cap_zero_matches_main[builder_' + name + ']'
        for name in ('ordinary', 'wide', 'busy', 'unknown', 'measurement')]], cwd=checkout,
        env=dict(os.environ, PATH=str(empty), PYTHONPATH=str(checkout / 'src'),
                 PRISMABUILD_BOX_STATE_ROOT=str(tmp_path / 'box-state')),
        text=True, capture_output=True, timeout=600)
    assert result.returncode == 0, result.stdout[-4000:] + result.stderr[-2000:]
    assert '5 passed' in result.stdout


@pytest.mark.parametrize('scenario', SCENARIOS)
def test_undeclared_matches_main_at_positive_cap(tmp_path, scenario):
    assert run_trace(tmp_path, scenario, ramp_cap=2) == BASE['outcomes'][scenario]
