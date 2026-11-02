"""Quarantine is reasoned, target-scoped, and tied to open deflake issues."""
import ast
import importlib.util
import io
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from test_bazel_test_sizing import starlark

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def quarantine():
    namespace = starlark('tools/bazel/nonhermetic.bzl')
    assert 'QUARANTINE' in namespace, 'the issue-backed quarantine table is missing'
    return namespace['QUARANTINE']


def test_entries_have_an_issue_a_reason_and_an_existing_target(quarantine):
    build = ast.parse((ROOT / 'tests/BUILD.bazel').read_text())
    targets = {'//tests:' + ast.literal_eval(keyword.value)
               for node in build.body if isinstance(node, ast.Expr)
               and isinstance(node.value, ast.Call) and isinstance(node.value.func, ast.Name)
               and node.value.func.id == 'pb_py_test'
               for keyword in node.value.keywords if keyword.arg == 'name'}
    for label, entry in quarantine.items():
        assert label in targets, label
        assert type(entry['issue']) is int and entry['issue'] > 0, label
        assert isinstance(entry['reason'], str) and entry['reason'].strip(), label


def test_quarantine_covers_the_authorized_gate_failures(quarantine):
    # Remove each expectation alongside its entry when the deflake issue closes.
    assert len(quarantine) == 23
    assert len({entry['issue'] for entry in quarantine.values()}) == 19
    expected = {
        '//tests:test_output_reconciliation_cycle': 2670,
        '//tests:test_queue_bell': 2671,
        '//tests:test_a_produced_namespace_is_not_a_consumer': 2672,
    }
    assert {label: quarantine[label]['issue'] for label in expected} == expected


def test_only_quarantined_targets_are_flaky(quarantine):
    captured = []
    native = SimpleNamespace(package_name=lambda: 'tests', package_relative_label=str)
    bindings = starlark('tools/bazel/nonhermetic.bzl', native=native)
    bindings.update(native=native, py_test=lambda **kwargs: captured.append(kwargs),
                    TEST_INPUTS={}, TEST_SIZING={}, all_requirements=[])
    macro = starlark('tools/bazel/python.bzl', **bindings)['pb_py_test']
    for node in ast.parse((ROOT / 'tests/BUILD.bazel').read_text()).body:
        if not isinstance(node, ast.Expr) or not isinstance(node.value, ast.Call):
            continue
        call = node.value
        if not isinstance(call.func, ast.Name) or call.func.id != 'pb_py_test':
            continue
        attributes = {kw.arg: ast.literal_eval(kw.value) for kw in call.keywords
                      if kw.arg in ('name', 'path', 'library', 'flaky')}
        macro(**attributes)
    marked = {'//tests:' + target['name'] for target in captured if target.get('flaky', False)}
    assert marked == set(quarantine)
    # Direct rule attributes must not bypass the macro's table.
    for path in ('BUILD.bazel', 'tests/BUILD.bazel', 'tools/BUILD.bazel',
                 'src/prismabuild/BUILD.bazel', 'tools/bazel/BUILD.bazel'):
        for node in ast.walk(ast.parse((ROOT / path).read_text())):
            if isinstance(node, ast.Call):
                for keyword in node.keywords:
                    if keyword.arg == 'flaky':
                        assert ast.literal_eval(keyword.value) is False, path


@pytest.fixture
def checker():
    spec = importlib.util.spec_from_file_location('bazel_quarantine', ROOT / 'tools/ci/bazel_quarantine.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_controller_reads_the_same_literal_table(checker, quarantine):
    assert checker.read_quarantine(ROOT / 'tools/bazel/nonhermetic.bzl') == quarantine


@pytest.mark.parametrize('state', ['open', 'closed', 'unknown'])
def test_issue_state_check_is_deduplicated_and_fails_closed(checker, state):
    requests = []

    def opened(request, timeout):
        requests.append(request)
        number = int(request.full_url.rsplit('/', 1)[1])
        return io.BytesIO(json.dumps({'number': number, 'state': state}).encode())

    table = {'//tests:test_one': {'issue': 123, 'reason': 'a census deadline'},
             '//tests:test_two': {'issue': 123, 'reason': 'the same census deadline'},
             '//tests:test_three': {'issue': 456, 'reason': 'an expired CPU sample'}}
    failures = checker.check_issues(table, 'RobTand/prismabuild', 'test-only-token', urlopen=opened)
    assert [request.full_url for request in requests] == [
        'https://api.github.com/repos/RobTand/prismabuild/issues/123',
        'https://api.github.com/repos/RobTand/prismabuild/issues/456']
    assert all(request.get_header('Authorization') == 'Bearer test-only-token' for request in requests)
    if state == 'open':
        assert failures == []
    else:
        assert len(failures) == 2
        assert '#123' in failures[0] and '#456' in failures[1]
        assert state in failures[0]


def test_api_errors_fail_without_echoing_the_token(checker):
    def unavailable(request, timeout):
        raise OSError('sensitive-test-token')

    failures = checker.check_issues({'//tests:test_one': {'issue': 123, 'reason': 'deadline'}},
                                    'RobTand/prismabuild', 'sensitive-test-token', urlopen=unavailable)
    assert len(failures) == 1 and '#123' in failures[0]
    assert 'sensitive-test-token' not in failures[0]


def test_controller_scrubs_credentials_before_requests(checker, monkeypatch, tmp_path):
    table = tmp_path / 'nonhermetic.bzl'
    table.write_text('QUARANTINE = {"//tests:test_one": {"issue": 123, "reason": "deadline"}}\n')
    monkeypatch.setenv('GITHUB_TOKEN', 'test-only-token')
    monkeypatch.setenv('BUILDBUDDY_API_KEY', 'test-only-buildbuddy')
    monkeypatch.setenv('MERGIFY_TOKEN', 'test-only-mergify')
    monkeypatch.setenv('OTHER_API_KEY', 'test-only-other')
    monkeypatch.setenv('OTHER_TOKEN', 'test-only-other-token')
    monkeypatch.setenv('GITHUB_REPOSITORY', 'RobTand/prismabuild')
    observed = []

    def opened(request, timeout):
        for name in ('GITHUB_TOKEN', 'BUILDBUDDY_API_KEY', 'MERGIFY_TOKEN', 'OTHER_API_KEY', 'OTHER_TOKEN'):
            assert name not in checker.os.environ, name
        observed.append(request.full_url)
        return io.BytesIO(b'{"number":123,"state":"open"}')

    monkeypatch.setattr(checker.urllib.request, 'urlopen', opened)
    assert checker.main(['--table', str(table)]) == 0
    assert observed == ['https://api.github.com/repos/RobTand/prismabuild/issues/123']


def test_controller_requires_its_own_job_token(checker, monkeypatch):
    monkeypatch.delenv('GITHUB_TOKEN', raising=False)
    assert checker.main([]) == 1
