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


def test_gate_child_environment_builder_keeps_only_allowed_names():
    builder = load('gate_env_unit', 'tools/ci/gate_env.py')
    source = {'PATH': '/usr/bin', 'HOME': '/home/ci', 'TMPDIR': '/tmp/ci',
              'LANG': 'C.UTF-8', 'LC_ALL': 'C', 'PYTHONPATH': '/code/src',
              'GITHUB_SHA': 'abc', 'PRISMABUILD_TEST_TIMEOUT_S': '300',
              'GATE_PROFILE_STARTUP': '1', 'UNRELATED': 'private',
              'LD_PRELOAD': '/untrusted.so', 'BUILDBUDDY_API_KEY': 'fake-secret-value',
              'MERGIFY_TOKEN': 'fake-secret-value', 'GITHUB_TOKEN': 'fake-secret-value',
              'GITHUB_OTHER_TOKEN': 'fake-secret-value', 'GATE_API_KEY': 'fake-secret-value',
              'PRISMABUILD_OTHER_TOKEN': 'fake-secret-value'}
    environment = builder.child_environment(source)
    assert set(environment) == {'PATH', 'HOME', 'TMPDIR', 'LANG', 'LC_ALL', 'PYTHONPATH',
                                'GITHUB_SHA', 'PRISMABUILD_TEST_TIMEOUT_S', 'GATE_PROFILE_STARTUP'}
    assert environment['PATH'] == '/usr/bin'
    assert environment['GITHUB_SHA'] == 'abc'
    assert environment['GATE_PROFILE_STARTUP'] == '1'
    assert source['UNRELATED'] == 'private'
    assert not any(name.endswith(('_API_KEY', '_TOKEN')) for name in environment)


def test_real_gate_child_call_sites_never_inherit_secrets(tmp_path, monkeypatch):
    import ast
    import inspect
    from types import SimpleNamespace

    paths = ['tools/ci/bazel_gate.py', 'tools/ci/gate_run.py',
             'tools/ci/bazel_generated.py', 'tools/ci/gate_select.py',
             'tools/ci/gate_revision.py', 'tools/ci/gate_setup.py',
             'tools/bazel/build_files.py', 'tools/ci/bazel_parity.py']
    modules = [load('secret_probe_' + Path(path).stem, path) for path in paths]
    gate, runner, generated, select, revision, setup, build_files, parity = modules
    expected = set()
    for path in paths:
        for node in ast.walk(ast.parse((ROOT / path).read_text())):
            if isinstance(node, ast.Call) and ast.unparse(node.func) in {
                    'subprocess.run', 'subprocess.Popen', 'subprocess.check_output', 'execute'}:
                expected.add((str((ROOT / path).resolve()), node.lineno))
    observed = set()
    ambient = {'PATH': '/usr/bin', 'HOME': str(tmp_path), 'GITHUB_SHA': 'abc',
               'PRISMABUILD_TEST_TIMEOUT_S': '300', 'GATE_PROFILE_STARTUP': '1',
               'PYTHONPATH': str(ROOT / 'src')}
    for name in ('BUILDBUDDY_API_KEY', 'MERGIFY_TOKEN', 'GITHUB_TOKEN',
                 'GITHUB_OTHER_TOKEN', 'PRISMABUILD_OTHER_API_KEY', 'GATE_OTHER_TOKEN'):
        ambient[name] = 'fake-secret-value'
    monkeypatch.setattr(os, 'environ', ambient)
    budget = tmp_path / 'tools/ci/gate_budget.json'
    budget.parent.mkdir(parents=True)
    budget.write_text('{"seconds":20,"slow":{}}')
    junit = tmp_path / 'junit.xml'
    junit.write_text('<testsuite><testcase file="tests/test_one.py" classname="tests.test_one" '
                     'name="test_one" time="0.01"/></testsuite>')

    def capture(command, **kwargs):
        frame = inspect.currentframe().f_back
        observed.add((str(Path(frame.f_code.co_filename).resolve()), frame.f_lineno))
        # Assert on names only, even if this regression fails with a real runner env.
        if 'env' not in kwargs or kwargs['env'] is None:
            pytest.fail('child inherited the ambient environment: ' + str(command[0]))
        names = set(kwargs['env'])
        if Path(str(command[0])).name in {'bazel', 'bb'}:
            assert kwargs['env']['BUILDBUDDY_API_KEY'] == 'fake-secret-value'
        else:
            forbidden = sorted(name for name in names if name.endswith(('_API_KEY', '_TOKEN')))
            assert not forbidden, 'forbidden child environment names: ' + ', '.join(forbidden)
            assert kwargs['env']['PATH'] == '/usr/bin'
        stdout = '' if kwargs.get('text') else b''
        if command[:2] == ['git', 'ls-files']:
            stdout = b''
        elif command[:2] == ['git', 'ls-tree']:
            stdout = 'budget exists'
        elif command[:2] == ['git', 'show']:
            stdout = budget.read_text()
        elif command[:2] == ['git', 'rev-parse']:
            stdout = 'a' * 40 + '\n'
        return SimpleNamespace(returncode=0, stdout=stdout, wait=lambda: 0)

    monkeypatch.setattr(subprocess, 'run', capture)
    monkeypatch.setattr(subprocess, 'Popen', capture)
    workspace = gate.Workspace(ROOT)
    for command in [('git', 'status'), ('python3', 'tool.py'), ('java', '-jar', 'diff.jar'),
                    ('bash', '-c', 'true'), ('bazel', 'query'), ('/opt/bin/bb', 'version')]:
        workspace.run(*command)
    workspace.ensure_commit('a' * 40)
    workspace.start_generated_checks()
    generated.check(root=tmp_path)
    build_files.repository_files(tmp_path)
    def capture_output(command, **kwargs):
        caller = inspect.currentframe().f_back
        observed.add((str(Path(caller.f_code.co_filename).resolve()), caller.f_lineno))
        return capture(command, **kwargs).stdout
    monkeypatch.setattr(subprocess, 'check_output', capture_output)
    monkeypatch.setattr(parity, 'population', lambda: [])
    monkeypatch.setattr(parity, 'compare_static', lambda *args: {})
    assert parity.main(['static']) == 0
    runner._budget_policy(tmp_path, 'a' * 40)
    assert runner.run_plan({'selected': [{'path': 'tests/test_one.py'}], 'deselect': []},
                           root=tmp_path, output=tmp_path / 'result.json', junit=junit,
                           execute=subprocess.run, affinity={1}) == 0
    select.changed_files(tmp_path, 'a' * 40, 'b' * 40)
    revision.gate_revisions(tmp_path, 'selection', 'b' * 40, 'a' * 40)
    setup.install_bubblewrap(subprocess.run)
    # Cover apt's recovery calls with an explicit failure, without sleeping.
    first = True
    def fail_install_once(command, **kwargs):
        nonlocal first
        result = capture(command, **kwargs)
        # The wrapper is outside production; record the actual execute call too.
        caller = inspect.currentframe().f_back
        observed.add((str(Path(caller.f_code.co_filename).resolve()), caller.f_lineno))
        if first:
            first = False
            raise subprocess.CalledProcessError(1, command)
        return result
    setup.install_bubblewrap(fail_install_once)
    monkeypatch.setattr(setup.shutil, 'which', lambda name: '/usr/bin/bwrap')
    monkeypatch.setattr(setup.os, 'getuid', lambda: 1000)
    monkeypatch.setattr(setup.os, 'getgid', lambda: 1000)
    # Avoid machine state: the relocation operates only on private temp paths.
    original = tmp_path / 'python'
    original.mkdir()
    setup.relocate_python(original, tmp_path / 'relocated', subprocess.run)
    real_stat = Path.stat
    def different_device(path, *args, **kwargs):
        result = real_stat(path, *args, **kwargs)
        if path == original:
            values = list(result)
            values[2] += 1
            return os.stat_result(values)
        return result
    monkeypatch.setattr(Path, 'stat', different_device)
    setup.relocate_python(original, tmp_path / 'other-device', subprocess.run)
    monkeypatch.setattr(Path, 'stat', real_stat)
    monkeypatch.setattr(setup, 'relocate_python', lambda *args: None)
    partition = load('secret_probe_partition', 'tools/ci/gate_partition.py')
    files = sorted(partition.SANDBOX_TEST_FILES | setup.ROOT_OWNED_INTERPRETER_FILES)
    setup.prepare_assigned_gate(files, execute=subprocess.run)
    # Wrapper frames aren't call sites. Every real subprocess site must execute.
    assert expected <= observed, sorted((Path(path).name, line) for path, line in expected - observed)


def test_runner_shell_launcher_uses_the_allowlist(monkeypatch):
    builder = load('gate_env_launcher', 'tools/ci/gate_env.py')
    monkeypatch.setattr(os, 'environ', {'PATH': '/usr/bin', 'HOME': '/home/ci',
                                      'BUILDBUDDY_API_KEY': 'fake-secret-value',
                                      'GITHUB_TOKEN': 'fake-secret-value'})
    calls = []
    def execute(file, argv, env):
        calls.append(file)
        assert argv == ['bash', '-c', 'true']
        assert set(env) == {'PATH', 'HOME'}
        assert env['PATH'] == '/usr/bin'
    monkeypatch.setattr(os, 'execvpe', execute)
    builder.main(['bash', '-c', 'true'])
    assert calls == ['bash']
