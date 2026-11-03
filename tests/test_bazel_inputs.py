"""Derived Bazel inputs fail CI when stale, and the runner reads hosted-style JUnit."""
from __future__ import annotations

import importlib.util
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, ROOT / path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


bazel_generated = load('bazel_generated', 'tools/ci/bazel_generated.py')
requirements = load('bazel_requirements', 'tools/bazel/requirements.py')
python_runner = load('bazel_python_runner', 'tools/bazel/python_runner.py')


def test_every_generated_input_has_a_check_and_one_fix():
    commands = [' '.join(map(str, command)) for command in bazel_generated.commands()]
    assert any('tools/bazel/requirements.py --check' in command for command in commands)
    assert any('tools/bazel/build_files.py --check' in command for command in commands)


def test_stale_inputs_report_every_failure_and_the_fix(tmp_path):
    failing = [[sys.executable, '-c', 'import sys; print("stale A"); sys.exit(1)'],
               [sys.executable, '-c', 'pass'],
               [sys.executable, '-c', 'import sys; print("stale B"); sys.exit(1)']]
    with pytest.raises(RuntimeError) as failed:
        bazel_generated.check(failing, root=tmp_path)
    assert 'stale A' in str(failed.value) and 'stale B' in str(failed.value)
    assert 'Fix them all with: tools/bazel/generate.sh' in str(failed.value)


def test_requirements_follow_the_hosted_gate_pins():
    source = (ROOT / requirements.SOURCE).read_text()
    assert requirements.problems(source, (ROOT / requirements.EXPORT).read_text()) == []


def test_a_changed_pin_or_source_is_drift():
    source = 'pytest==9.1.1\nPyYAML==6.0.3\n'
    export = requirements.header(source) + 'pytest==9.1.1 \\\n    --hash=sha256:aa\npyyaml==6.0.3 \\\n'
    assert requirements.problems(source, export) == []
    bumped = source.replace('9.1.1', '9.2.0')
    found = requirements.problems(bumped, export)
    assert any('was not generated from the current' in problem for problem in found)
    assert any('pins pytest==9.2.0' in problem for problem in found)


def test_outcomes_read_hosted_style_junit(tmp_path):
    report = tmp_path / 'junit.xml'
    report.write_text(
        '<testsuites><testsuite>'
        '<testcase file="tests/test_a.py" classname="tests.test_a.TestX" name="test_one" time="1"/>'
        '<testcase file="tests/test_a.py" classname="tests.test_a" name="test_two" time="1"><skipped/></testcase>'
        '<testcase file="tests/test_a.py" classname="tests.test_a" name="test_three" time="1"><failure/></testcase>'
        '</testsuite></testsuites>')
    assert python_runner.outcomes(report) == {
        'tests/test_a.py::TestX::test_one': 'passed',
        'tests/test_a.py::test_two': 'skipped',
        'tests/test_a.py::test_three': 'failed'}


def test_shards_partition_the_collected_tests(tmp_path):
    module = tmp_path / 'test_many.py'
    module.write_text(''.join(f'def test_{index}():\n    pass\n' for index in range(7)))
    (tmp_path / 'bazel_shard.py').write_text(python_runner.SHARD_PLUGIN)
    seen = []
    for index in range(3):
        environment = dict(os.environ, PB_BAZEL_SHARD_INDEX=str(index), PB_BAZEL_TOTAL_SHARDS='3',
                           PYTHONPATH=str(tmp_path))
        result = subprocess.run([sys.executable, '-m', 'pytest', str(module), '-p', 'bazel_shard',
                                 '-q', '--collect-only', '-p', 'no:cacheprovider'],
                                cwd=tmp_path, env=environment, capture_output=True, text=True, check=True)
        seen.append({line for line in result.stdout.splitlines() if '::test_' in line})
    assert sum(map(len, seen)) == 7
    assert set().union(*seen) == {f'test_many.py::test_{index}' for index in range(7)}


def test_the_runner_keeps_the_gate_environment():
    runtime = load('bazel_runtime', 'tools/bazel/runtime.py')
    gate_run = (ROOT / 'tools/ci/gate_run.py').read_text()
    assert "PRISMABUILD_TEST_TIMEOUT_S='300'" in gate_run
    assert runtime.GATE_ENVIRONMENT['PRISMABUILD_TEST_TIMEOUT_S'] == '300'
    for name in ('OMP_NUM_THREADS', 'MKL_NUM_THREADS', 'OPENBLAS_NUM_THREADS'):
        assert f"{name}='1'" in gate_run and runtime.GATE_ENVIRONMENT[name] == '1'
    for name in ('GIT_AUTHOR_NAME', 'GIT_AUTHOR_EMAIL', 'GIT_COMMITTER_NAME', 'GIT_COMMITTER_EMAIL'):
        assert runtime.GATE_ENVIRONMENT[name]


runtime = load('bazel_runtime_lanes', 'tools/bazel/runtime.py')


def test_pytest_sees_the_gate_environment_without_bazels(tmp_path):
    environ = {'PATH': '/usr/bin', 'USER': 'ci', 'TEST_TMPDIR': '/x', 'RUNFILES_DIR': '/r',
               'BASH_FUNC_rlocation%%': '() { :; }', 'XML_OUTPUT_FILE': '/t.xml', 'PYTHONSAFEPATH': '1',
               'LD_PRELOAD': '/evil.so', 'PB_BAZEL_LANE': 'runner'}
    environment = runtime.gate_environment(environ, tmp_path, tmp_path / 'scratch')
    assert environment['USER'] == 'ci' and environment['PATH'] == '/usr/bin'
    for name in ('TEST_TMPDIR', 'RUNFILES_DIR', 'BASH_FUNC_rlocation%%', 'XML_OUTPUT_FILE', 'PYTHONSAFEPATH',
                 'LD_PRELOAD'):
        assert name not in environment
    assert environment['TMPDIR'] == environment['HOME'] == str(tmp_path / 'scratch')
    assert environment['PYTHONPATH'].split(os.pathsep)[:2] == [str(tmp_path / 'tools/ci'), str(tmp_path / 'src')]
    assert environment['PRISMABUILD_TEST_TIMEOUT_S'] == '300'


def test_short_scratch_climbs_to_the_top_writable_directory_of_the_same_filesystem(tmp_path):
    private = tmp_path / 'a/very/deep/test/tmpdir'
    private.mkdir(parents=True)
    scratch = runtime.short_scratch(private)
    try:
        assert scratch.stat().st_dev == private.stat().st_dev
        assert len(str(scratch)) < len(str(private))
        assert scratch.parent in private.parents
    finally:
        scratch.rmdir()


@pytest.mark.parametrize('machine', ['aarch64', 'x86_64'])
def test_tests_read_a_private_copy_of_the_interpreter(tmp_path, monkeypatch, machine):
    # Input-cache hardlinks change metadata while other actions link and unlink
    # the same interpreter. Every architecture needs an independent inode.
    original = Path(getattr(sys, '_base_executable', sys.executable)).resolve()
    monkeypatch.setenv('TEST_TMPDIR', str(tmp_path))
    monkeypatch.delenv('PB_BAZEL_LANE', raising=False)
    monkeypatch.setattr(sys, 'executable', sys.executable)
    monkeypatch.setattr('platform.machine', lambda: machine)
    copies = []
    copyfile = runtime.shutil.copyfile

    def measured_copy(source, destination, *args, **kwargs):
        started = time.perf_counter()
        result = copyfile(source, destination, *args, **kwargs)
        copies.append({'bytes': Path(destination).stat().st_size,
                       'seconds': time.perf_counter() - started})
        return result

    monkeypatch.setattr(runtime.shutil, 'copyfile', measured_copy)
    scratch, environment = runtime.prepare_remote_test(tmp_path)
    scratch.rmdir()
    python = tmp_path / 'python-environment/bin/python'
    print('PB_INTERPRETER_COPY ' + json.dumps({'machine': machine, 'copies': copies}))
    assert Path(sys.executable) == python
    assert not python.is_symlink()
    assert not python.samefile(original)
    with original.open('rb') as source, python.open('rb') as private:
        assert hashlib.file_digest(private, 'sha256').digest() == hashlib.file_digest(source, 'sha256').digest()
    assert subprocess.run([python, '-c', 'import json'], env=environment, check=False).returncode == 0


def test_runner_lane_tests_need_a_git_checkout(tmp_path):
    checkout = tmp_path / 'checkout'
    (checkout / '.git').mkdir(parents=True)
    (checkout / 'conftest.py').write_text('')
    runfiles = tmp_path / 'runfiles'
    runfiles.mkdir()
    (runfiles / 'conftest.py').symlink_to(checkout / 'conftest.py')
    assert runtime.checkout_of(runfiles) == checkout.resolve()
    (checkout / '.git').rmdir()
    with pytest.raises(RuntimeError, match='needs a git checkout'):
        runtime.checkout_of(runfiles)


OVERRUN = [{'nodeid': 'tests/test_a.py::test_one', 'seconds': 138.0, 'budget_seconds': 20.0}]


@pytest.mark.parametrize('pytest_code, seconds', [(0, 20.5), (0, 138.0), (1, 138.0)])
def test_runner_warns_for_passing_overruns_and_preserves_failures(
        tmp_path, monkeypatch, capsys, pytest_code, seconds):
    config = tmp_path / 'tools/ci/gate_budget.json'
    config.parent.mkdir(parents=True)
    config.write_text(json.dumps({'seconds': 20, 'slow': {}}))
    private, outputs = tmp_path / 'private', tmp_path / 'outputs'
    private.mkdir()
    scratch = tmp_path / 'scratch'
    scratch.mkdir()
    gate_run = python_runner.gate_run_of(ROOT)
    monkeypatch.setattr(python_runner, 'gate_run_of', lambda root: gate_run)
    monkeypatch.setattr(python_runner, 'prepare_remote_test', lambda root: (scratch, {}))
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv('TEST_TMPDIR', str(private))
    monkeypatch.setenv('TEST_UNDECLARED_OUTPUTS_DIR', str(outputs))
    monkeypatch.delenv('XML_OUTPUT_FILE', raising=False)
    monkeypatch.delenv('TEST_TOTAL_SHARDS', raising=False)
    monkeypatch.delenv('PB_BAZEL_LANE', raising=False)
    monkeypatch.setattr(sys, 'argv', ['python_runner.py', 'tests/test_a.py'])

    def execute(command, **kwargs):
        outputs.mkdir()
        failure = '<failure message="wrong result"/>' if pytest_code else ''
        (outputs / 'junit.xml').write_text(
            '<testsuites><testsuite><testcase file="tests/test_a.py" classname="tests.test_a" '
            f'name="test_one" time="{seconds}">' + failure + '</testcase></testsuite></testsuites>')
        return SimpleNamespace(returncode=pytest_code)

    monkeypatch.setattr(python_runner.subprocess, 'run', execute)
    assert python_runner.main() == pytest_code
    assert not (outputs / 'budget-overrun.txt').exists()
    if pytest_code:
        assert not (outputs / 'budget-warnings.json').exists()
        assert not (outputs / 'inventory.json').exists()
    else:
        assert json.loads((outputs / 'budget-warnings.json').read_text()) == [
            {**OVERRUN[0], 'seconds': seconds}]
        assert json.loads((outputs / 'inventory.json').read_text())['nodes'] == {
            'tests/test_a.py::test_one': 'passed'}
        assert f'{seconds:.3f}s > 20.000s' in capsys.readouterr().err


def test_the_runner_measures_budgets_only_after_every_test_passed(tmp_path, monkeypatch):
    seen = []
    monkeypatch.setattr(python_runner, 'budget_overruns', lambda *args: seen.append(args) or OVERRUN)
    assert python_runner.overruns_after(1, {'tests/test_a.py::test_one': 'failed'}, tmp_path, tmp_path,
                                        'tests/test_a.py') == []
    assert python_runner.overruns_after(0, {}, tmp_path, tmp_path, 'tests/test_a.py') == []
    assert seen == []
    assert python_runner.overruns_after(0, {'tests/test_a.py::test_one': 'passed'}, tmp_path, tmp_path,
                                        'tests/test_a.py') == OVERRUN


def test_the_gate_collects_the_warning_the_runner_writes(tmp_path):
    outputs = tmp_path / 'bazel-testlogs/tests/test_a/test.outputs'
    python_runner.settle(0, {'tests/test_a.py::test_one': 'passed'}, OVERRUN, outputs,
                         'tests/test_a.py')
    gate = load('bazel_gate_for_runner', 'tools/ci/bazel_gate.py').Workspace(root=tmp_path)
    assert gate.budget_warnings(['//tests:test_a']) == [{'label': '//tests:test_a', **OVERRUN[0]}]
