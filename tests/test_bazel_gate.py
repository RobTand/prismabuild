"""Selection must fail closed: any doubt about what to test fails the gate."""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location('bazel_gate', ROOT / 'tools/ci/bazel_gate.py')
bazel_gate = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(bazel_gate)

TESTS = {'//tests:test_core', '//tests:test_pbrun', '//tests:test_fixture_reader'}
HOSTED = {'//tests:test_pbrun_placement'}
USED = {'src/prismabuild/core.py', 'tools/fleet/pbrun.py', 'tests/fixtures/one.json',
        'tests/test_core.py', 'tests/BUILD.bazel', 'src/prismabuild/BUILD.bazel', 'docs/policy.md'}


def plan(changed, impacted=(), deleted=(), used=USED, tests=TESTS, hosted=HOSTED):
    return bazel_gate.gate_decision(set(changed), set(deleted), set(impacted), set(used), set(tests), set(hosted))


class FakeWorkspace:
    """Records calls; answers from canned git and Bazel facts."""

    def __init__(self, changed, impacted=(), statuses=None, diff_error=None, build_ok=True,
                 inputs=(), budgets=()):
        self.changed, self.impacted, self.statuses = set(changed), set(impacted), statuses or {}
        self.diff_error, self.build_ok, self.inputs, self.budgets = diff_error, build_ok, inputs, budgets
        self.ran, self.built = [], []

    def ensure_commit(self, sha):
        if not sha:
            raise bazel_gate.GateError('empty base or head')

    def changes(self, base, head):
        return set(self.changed), set()

    def bazel_diff(self, base, head):
        if self.diff_error:
            raise bazel_gate.GateError(self.diff_error)
        return set(self.impacted)

    def checkout(self, sha):
        pass

    def tests(self):
        return set(TESTS)

    def used_sources(self):
        return set(USED)

    def hosted_tests(self):
        return set(HOSTED)

    def test(self, targets, config, metadata, log_name):
        self.ran.append((config, list(targets)))
        return {label: self.statuses.get(label, 'PASSED') for label in targets}

    def build(self, targets, config, metadata, log_name):
        self.built.append((config, list(targets)))
        return self.build_ok

    def inputs_check(self):
        return list(self.inputs)

    def budget_check(self, base):
        return list(self.budgets)


def run(workspace, tmp_path, monkeypatch, mode='pr', base='a' * 40):
    monkeypatch.setenv('BUILDBUDDY_ARTIFACTS_DIRECTORY', str(tmp_path))
    status = bazel_gate.main([mode, '--base', base, '--head', 'b' * 40], workspace=workspace)
    return status, json.loads((tmp_path / bazel_gate.RESULT).read_text())


def test_impacted_hosted_lane_targets_go_to_the_hosted_lane_and_are_never_built():
    decision = plan(['tools/fleet/pbrun.py'], ['//tools:fleet/pbrun', '//tests:test_pbrun',
                                               '//tests:test_pbrun_placement'])
    assert decision['selected'] == ['//tests:test_pbrun']
    assert decision['hosted'] == ['//tests:test_pbrun_placement']
    assert decision['build'] == ['//tools:fleet/pbrun']
    assert plan(['MODULE.bazel'])['hosted'] == sorted(HOSTED)


def test_impacted_tests_are_selected_and_packages_become_scopes():
    decision = plan(['src/prismabuild/core.py'], ['//src/prismabuild:core', '//tests:test_core'])
    assert decision['selected'] == ['//tests:test_core']
    assert decision['scopes'] == ['//src/prismabuild', '//tests']
    assert decision['build'] == ['//src/prismabuild:core']
    assert not decision['all_scopes']


@pytest.mark.parametrize('path', ['MODULE.bazel', 'MODULE.bazel.lock', '.bazelrc', '.bazelversion',
                                  '.bazelignore', 'BUILD.bazel', 'tools/bazel/python.bzl',
                                  'tools/bazel/requirements.txt', '.github/workflows/gate.yml',
                                  'tools/ci/gate_requirements.txt', 'tools/ci/bazel_gate.py',
                                  'tools/ci/gate_select.py'])
def test_build_lock_and_workflow_changes_test_everything(path):
    decision = plan([path, 'src/prismabuild/core.py'])
    assert decision['selection'] == 'fallback'
    assert decision['selected'] == sorted(TESTS)
    assert decision['build'] == ['//...'] and decision['all_scopes']


def test_an_empty_diff_fails_closed():
    with pytest.raises(bazel_gate.GateError, match='empty diff'):
        plan([])


def test_an_unmapped_changed_file_fails_naming_it_and_the_fix():
    with pytest.raises(bazel_gate.GateError) as failed:
        plan(['tests/fixtures/new.json'], ['//tests:test_core'])
    assert 'tests/fixtures/new.json' in str(failed.value)
    assert 'tools/bazel/generate.sh' in str(failed.value)


def test_a_relevant_change_with_nothing_impacted_fails_closed():
    with pytest.raises(bazel_gate.GateError, match='no impacted targets'):
        plan(['src/prismabuild/core.py'], [])


def test_only_unread_allowlisted_files_select_nothing():
    decision = plan(['.mergify.yml'])
    assert decision['selection'] == 'irrelevant' and decision['selected'] == []
    assert bazel_gate.needs_no_diff({'.mergify.yml'}, set(), USED)
    assert not bazel_gate.needs_no_diff({'docs/policy.md'}, set(), USED)


def test_a_documented_add_regenerate_commit_change_selects_the_new_test():
    # docs/bazel.md: add tests/test_new.py, run tools/bazel/generate.sh, commit both.
    used = USED | {'tests/test_new.py'}
    tests = TESTS | {'//tests:test_new'}
    decision = plan(['tests/test_new.py', 'tests/BUILD.bazel'],
                    ['//tests:test_new', '//tests:test_new_lib'], used=used, tests=tests)
    assert decision['selection'] == 'impacted'
    assert decision['selected'] == ['//tests:test_new']


def test_a_build_file_edit_that_moves_no_rule_hash_tests_everything():
    decision = plan(['tests/BUILD.bazel'], [])
    assert decision['selection'] == 'fallback' and decision['selected'] == sorted(TESTS)
    with pytest.raises(bazel_gate.GateError, match='no impacted targets'):
        plan(['tests/BUILD.bazel', 'src/prismabuild/core.py'], [])


def test_used_sources_include_build_files(monkeypatch):
    workspace = bazel_gate.Workspace(root=ROOT)
    answers = {'kind("source file", deps(//...))': ['//src/prismabuild:core.py', '@@rules_python+//:x.py'],
               'buildfiles(//...)': ['//tests:BUILD.bazel', '//tools/bazel:python.bzl',
                                     '@@rules_python+//python:defs.bzl']}
    monkeypatch.setattr(workspace, 'query', lambda expression: answers[expression])
    assert workspace.used_sources() == {'src/prismabuild/core.py', 'tests/BUILD.bazel',
                                        'tools/bazel/python.bzl'}


def test_a_bazel_diff_error_fails_the_gate(tmp_path, monkeypatch):
    status, result = run(FakeWorkspace(['src/prismabuild/core.py'], diff_error='bazel-diff exited 1'),
                         tmp_path, monkeypatch)
    assert status == 1 and 'bazel-diff exited 1' in result['error']


def test_an_empty_base_fails_the_gate(tmp_path, monkeypatch):
    status, result = run(FakeWorkspace(['src/prismabuild/core.py']), tmp_path, monkeypatch, base='')
    assert status == 1 and 'empty base' in result['error']


def test_a_failing_or_missing_test_status_fails(tmp_path, monkeypatch):
    workspace = FakeWorkspace(['src/prismabuild/core.py'], ['//tests:test_core', '//tests:test_pbrun'],
                              statuses={'//tests:test_core': 'FAILED'})
    status, result = run(workspace, tmp_path, monkeypatch)
    assert status == 1 and result['failed'] == ['//tests:test_core']
    workspace = FakeWorkspace(['src/prismabuild/core.py'], ['//tests:test_core'])
    workspace.test = lambda targets, *rest: {}
    status, result = run(workspace, tmp_path, monkeypatch)
    assert status == 1 and result['failed'] == ['//tests:test_core']


def test_impacted_non_test_targets_are_built_and_a_build_failure_fails(tmp_path, monkeypatch):
    workspace = FakeWorkspace(['src/prismabuild/core.py'], ['//src/prismabuild:core', '//tests:test_core'],
                              build_ok=False)
    status, result = run(workspace, tmp_path, monkeypatch)
    assert workspace.built == [('remote', ['//src/prismabuild:core'])]
    assert status == 1 and any('bazel build' in problem for problem in result['inputs'])


def test_stale_inputs_or_growing_budgets_fail_a_passing_run(tmp_path, monkeypatch):
    for workspace in [FakeWorkspace(['src/prismabuild/core.py'], ['//tests:test_core'], inputs=['parity']),
                      FakeWorkspace(['src/prismabuild/core.py'], ['//tests:test_core'], budgets=['grew'])]:
        status, result = run(workspace, tmp_path, monkeypatch)
        assert status == 1 and result['verdict'] == 'pass' and result['inputs']


def test_main_mode_tests_everything_on_both_architectures_and_names_misses(tmp_path, monkeypatch):
    workspace = FakeWorkspace(['src/prismabuild/core.py'], ['//tests:test_core'],
                              statuses={'//tests:test_pbrun': 'FAILED'})
    status, result = run(workspace, tmp_path, monkeypatch, mode='main')
    assert [config for config, _ in workspace.ran] == ['remote', 'remote-arm64']
    assert all(targets == sorted(TESTS) for _, targets in workspace.ran)
    assert result['architectures']['amd64']['missed'] == ['//tests:test_pbrun']
    assert status == 1


def test_remote_executor_configs_are_named_in_bazelrc():
    config = (ROOT / '.bazelrc').read_text()
    for name in bazel_gate.CONFIGS.values():
        assert f'build:{name} --remote_executor=' in config
