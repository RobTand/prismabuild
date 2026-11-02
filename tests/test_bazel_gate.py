"""Selection must fail closed: any doubt about what to test fails the gate."""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import zipfile

import pytest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location('bazel_gate', ROOT / 'tools/ci/bazel_gate.py')
bazel_gate = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(bazel_gate)

TESTS = {'//tests:test_core', '//tests:test_pbrun', '//tests:test_fixture_reader'}
HOSTED = {'//tests:test_pbrun_placement'}
# Tests BuildBuddy's arm64 executors can't host, by nonhermetic.bzl tag.
ARM64_EXCLUDED = {'runner': {'//tests:test_pbrun'}, 'hosted-arm64': {'//tests:test_fixture_reader'}}
USED = {'src/prismabuild/core.py', 'tools/fleet/pbrun.py', 'tests/fixtures/one.json',
        'tests/test_core.py', 'tests/BUILD.bazel', 'src/prismabuild/BUILD.bazel', 'docs/policy.md'}


def plan(changed, impacted=(), deleted=(), used=USED, tests=TESTS, hosted=HOSTED):
    return bazel_gate.gate_decision(set(changed), set(deleted), set(impacted), set(used), set(tests), set(hosted))


class FakeWorkspace:
    """Records calls; answers from canned git and Bazel facts."""

    def __init__(self, changed, impacted=(), statuses=None, diff_error=None, build_ok=True,
                 inputs=(), budgets=(), broken=(), warnings=()):
        self.changed, self.impacted, self.statuses = set(changed), set(impacted), statuses or {}
        self.diff_error, self.build_ok, self.inputs, self.budgets = diff_error, build_ok, inputs, budgets
        # Configs whose invocation fails before any test runs.
        self.broken = set(broken)
        self.warnings = list(warnings)
        self.ran, self.built, self.extras = [], [], []
        self.graph_revisions, self.diff_revisions = [], []
        self.graph_errors = []

    def ensure_commit(self, sha):
        if not sha:
            raise bazel_gate.GateError('empty base or head')

    def changes(self, base, head):
        return set(self.changed), set()

    def graph_check(self, base, head):
        self.graph_revisions.extend([base, head])
        return {"summary": "U 20.0% → 20.0%, SCC max 1 → 1",
                "base": {"u_pct": 20}, "head": {"u_pct": 20}, "errors": self.graph_errors}

    def bazel_diff(self, base, head):
        self.diff_revisions.extend([base, head])
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

    def arm64_excluded(self):
        return {tag: set(labels) for tag, labels in ARM64_EXCLUDED.items()}

    def test(self, targets, config, metadata, log_name, extra=()):
        self.ran.append((config, list(targets)))
        self.extras.append(list(extra))
        if config in self.broken:
            return {'statuses': {}, 'invocation': f'{config}-id',
                    'error': '(15:48:14) ERROR: no matching toolchains found'}
        return {'statuses': {label: self.statuses.get(label, 'PASSED') for label in targets},
                'invocation': f'{config}-id', 'error': None}

    def budget_warnings(self, labels):
        return [row for row in self.warnings if row["label"] in labels]

    def build(self, targets, config, metadata, log_name):
        self.built.append((config, list(targets)))
        return self.build_ok

    def start_generated_checks(self):
        self.ran.append(('checks', []))
        return ['started']

    def inputs_check(self, started=None):
        assert started == ['started'] or started is None
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
                                  'tools/ci/gate_env.py', 'tools/ci/gate_requirements.txt', 'tools/ci/bazel_gate.py',
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
    assert result['graph_impact']['head']['u_pct'] == 20


def test_an_empty_base_fails_the_gate(tmp_path, monkeypatch, capsys):
    status, result = run(FakeWorkspace(['src/prismabuild/core.py']), tmp_path, monkeypatch, base='')
    assert status == 1 and 'empty base' in result['error']
    assert 'U unavailable' in capsys.readouterr().out
    assert result['graph_impact']['errors']


def test_a_failing_or_missing_test_status_fails(tmp_path, monkeypatch):
    workspace = FakeWorkspace(['src/prismabuild/core.py'], ['//tests:test_core', '//tests:test_pbrun'],
                              statuses={'//tests:test_core': 'FAILED'})
    status, result = run(workspace, tmp_path, monkeypatch)
    assert status == 1 and result['failed'] == ['//tests:test_core']
    workspace = FakeWorkspace(['src/prismabuild/core.py'], ['//tests:test_core'])
    workspace.test = lambda targets, *rest: {'statuses': {}, 'invocation': 'id', 'error': None}
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


def test_generated_input_checks_start_before_the_tests_and_finish_after(tmp_path, monkeypatch):
    workspace = FakeWorkspace(['src/prismabuild/core.py'], ['//tests:test_core'])
    status, result = run(workspace, tmp_path, monkeypatch)
    assert [config for config, _ in workspace.ran] == ['checks', 'remote']
    assert status == 0 and set(result['durations']) == {'selection', 'tests', 'total'}


def test_main_mode_tests_everything_on_both_architectures_and_names_misses(tmp_path, monkeypatch):
    workspace = FakeWorkspace(['src/prismabuild/core.py'], ['//tests:test_core'],
                              statuses={'//tests:test_pbrun': 'FAILED'})
    status, result = run(workspace, tmp_path, monkeypatch, mode='main')
    assert [config for config, _ in workspace.ran if config != 'checks'] == ['remote', 'remote-arm64']
    assert dict(workspace.ran)['remote'] == sorted(TESTS)
    assert result['architectures']['amd64']['missed'] == ['//tests:test_pbrun']
    assert status == 1


def test_main_mode_runs_on_arm64_only_what_its_executors_can_host_and_records_the_rest(tmp_path, monkeypatch):
    workspace = FakeWorkspace(['src/prismabuild/core.py'], ['//tests:test_core'],
                              statuses={'//tests:test_pbrun': 'FAILED'})
    status, result = run(workspace, tmp_path, monkeypatch, mode='main')
    assert dict(workspace.ran)['remote-arm64'] == ['//tests:test_core']
    arm64, amd64 = result['architectures']['arm64'], result['architectures']['amd64']
    assert arm64['tests'] == 1 and arm64['failed'] == []
    assert arm64['excluded'] == {'runner': 1, 'hosted-arm64': 1}
    assert arm64['excluded_tests'] == {'runner': ['//tests:test_pbrun'],
                                       'hosted-arm64': ['//tests:test_fixture_reader']}
    assert amd64['tests'] == len(TESTS) and amd64['excluded'] == {}
    assert bazel_gate.ARM64_EXCLUDED == tuple(ARM64_EXCLUDED)


def test_an_architecture_whose_invocation_ran_no_test_is_an_error_not_failures(tmp_path, monkeypatch):
    workspace = FakeWorkspace(['src/prismabuild/core.py'], ['//tests:test_core'], broken={'remote-arm64'})
    status, result = run(workspace, tmp_path, monkeypatch, mode='main')
    arm64 = result['architectures']['arm64']
    assert arm64['verdict'] == 'error' and arm64['failed'] == [] and arm64['missed'] == []
    assert 'remote-arm64-id' in arm64['error'] and 'no matching toolchains found' in arm64['error']
    assert result['architectures']['amd64']['verdict'] == 'pass'
    assert status == 1


def bep(*events):
    return '\n'.join(json.dumps(event) for event in events) + '\n'


def test_build_events_give_statuses_the_invocation_and_the_first_error_line():
    started = {'id': {'started': {}}, 'started': {'uuid': 'b8bda915-aeb6-44e3-9232-a3073a498838'}}
    progress = {'id': {'progress': {'opaqueCount': 1}},
                'progress': {'stderr': '(15:48:13) INFO: Analyzed 1 target\n'
                                       '(15:48:14) ERROR: /x/BUILD.bazel:3:8: no matching toolchains\n'
                                       '(15:48:14) ERROR: No test targets were found\n'}}
    run = bazel_gate.bep_run(bep(started, progress))
    assert run == {'statuses': {}, 'invocation': 'b8bda915-aeb6-44e3-9232-a3073a498838',
                   'error': '(15:48:14) ERROR: /x/BUILD.bazel:3:8: no matching toolchains'}
    summary = {'id': {'testSummary': {'label': '//tests:test_core'}},
               'testSummary': {'overallStatus': 'PASSED'}}
    assert bazel_gate.bep_run(bep(started, summary))['statuses'] == {'//tests:test_core': 'PASSED'}
    assert bazel_gate.bep_run('') == {'statuses': {}, 'invocation': None, 'error': None}


WARNING = {'label': '//tests:test_core', 'nodeid': 'tests/test_core.py::test_one',
           'seconds': 138.0, 'budget_seconds': 20.0}


@pytest.mark.parametrize('mode', ['pr', 'main'])
def test_budget_warnings_are_recorded_without_failing_or_rerunning(mode, tmp_path, monkeypatch):
    workspace = FakeWorkspace(['src/prismabuild/core.py'], ['//tests:test_core'], warnings=[WARNING])
    status, result = run(workspace, tmp_path, monkeypatch, mode=mode)
    assert status == 0
    if mode == 'pr':
        assert result['verdict'] == 'pass' and result['failed'] == []
        assert result['budget_warnings'] == [WARNING]
    else:
        assert all(not row['failed'] for row in result['architectures'].values())
        assert result['budget_warnings'] == [
            {**WARNING, 'architecture': 'amd64'}, {**WARNING, 'architecture': 'arm64'}]
    assert [config for config, _ in workspace.ran if config != 'checks'] == (
        ['remote'] if mode == 'pr' else ['remote', 'remote-arm64'])


@pytest.mark.parametrize('mode', ['pr', 'main'])
@pytest.mark.parametrize('status', ['TIMEOUT', 'FAILED'])
def test_failures_stay_failed_and_never_start_a_second_test(mode, status, tmp_path, monkeypatch):
    workspace = FakeWorkspace(['src/prismabuild/core.py'], ['//tests:test_core'],
                              statuses={'//tests:test_core': status})
    code, result = run(workspace, tmp_path, monkeypatch, mode=mode)
    assert code == 1
    if mode == 'pr':
        assert result['failed'] == ['//tests:test_core']
        assert workspace.ran == [('checks', []), ('remote', ['//tests:test_core'])]
    else:
        assert [config for config, _ in workspace.ran] == ['remote', 'remote-arm64']
        assert all(row['failed'] == ['//tests:test_core'] for row in result['architectures'].values())


def _outputs(root, label, rows=None, shard=None, zipped=False):
    package, _, name = label.removeprefix('//').partition(':')
    outputs = root / 'bazel-testlogs' / package / name / (f'{shard}/test.outputs' if shard else 'test.outputs')
    outputs.mkdir(parents=True)
    files = {'inventory.json': '{}'}
    if rows is not None:
        files['budget-warnings.json'] = json.dumps(rows)
    if zipped:
        with zipfile.ZipFile(outputs / 'outputs.zip', 'w') as archive:
            for file, content in files.items():
                archive.writestr(file, content)
    else:
        for file, content in files.items():
            (outputs / file).write_text(content)


def test_warnings_are_collected_from_plain_zipped_and_sharded_outputs(tmp_path):
    row = {key: value for key, value in WARNING.items() if key != 'label'}
    _outputs(tmp_path, '//tests:plain', [row])
    _outputs(tmp_path, '//tests:zipped', [row], zipped=True)
    _outputs(tmp_path, '//tests:sharded', [row], shard='shard_1_of_2')
    _outputs(tmp_path, '//tests:sharded', shard='shard_2_of_2', zipped=True)
    _outputs(tmp_path, '//tests:clean', zipped=True)
    workspace = bazel_gate.Workspace(root=tmp_path)
    assert workspace.budget_warnings(['//tests:plain', '//tests:zipped', '//tests:sharded',
                                      '//tests:clean', '//tests:missing']) == [
        {'label': '//tests:plain', **row}, {'label': '//tests:zipped', **row},
        {'label': '//tests:sharded', **row}]


def test_remote_executor_configs_are_named_in_bazelrc():
    config = (ROOT / '.bazelrc').read_text()
    for name in bazel_gate.CONFIGS.values():
        assert f'build:{name} --remote_executor=' in config


@pytest.mark.parametrize('mode', ['pr', 'main'])
@pytest.mark.parametrize('changed,impacted', [
    (['MODULE.bazel'], []), (['.github/workflows/gate.yml'], []),
    (['.mergify.yml'], []), (['tests/BUILD.bazel'], []),
    (['src/prismabuild/core.py'], ['//tests:test_core']),
])
def test_both_graphs_are_checked_even_when_selection_skips_diff(mode, changed, impacted, tmp_path, monkeypatch, capsys):
    workspace = FakeWorkspace(changed, impacted)
    status, result = run(workspace, tmp_path, monkeypatch, mode=mode)
    assert workspace.graph_revisions == ['a' * 40, 'b' * 40]
    assert result['graph_impact']['head']['u_pct'] == 20
    assert 'U 20.0% → 20.0%, SCC max 1 → 1' in capsys.readouterr().out
    assert status == 0


@pytest.mark.parametrize('mode', ['pr', 'main'])
def test_graph_failures_reach_inputs_with_mermaid_and_still_print_summary(mode, tmp_path, monkeypatch, capsys):
    workspace = FakeWorkspace(['MODULE.bazel'])
    workspace.graph_errors = ['test reach 99 exceeds budget 2\n```mermaid\ngraph TD; t --> hub\n```']
    status, result = run(workspace, tmp_path, monkeypatch, mode=mode)
    assert status == 1
    assert workspace.graph_errors[0] in result['inputs']
    assert 't --> hub' in result['graph_impact']['errors'][0]
    assert 'U 20.0% → 20.0%' in capsys.readouterr().out


def test_hash_generation_requests_a_revision_specific_dependency_file():
    generate, impacted = bazel_gate.diff_commands('/tmp/bazel-diff-v70', '/repo', 'bazel',
                                           Path('/tmp/base.json'), Path('/tmp/head.json'), '/tmp/impacted')
    command = generate(Path('/tmp/base.json'))
    assert command[command.index('--depEdgesFile') + 1] == '/tmp/base-deps.json'
    assert command[:2] == ['/tmp/bazel-diff-v70', 'generate-hashes']
    assert impacted[:2] == ['/tmp/bazel-diff-v70', 'get-impacted-targets']
    assert '-jar' not in command + impacted


def test_bazel_blame_uses_explain_not_a_second_path_finder(tmp_path, monkeypatch):
    workspace = bazel_gate.Workspace(root=tmp_path)
    workspace.hashes = {'base': Path('/tmp/base.json'), 'head': Path('/tmp/head.json')}
    workspace.deps = {'head': Path('/tmp/head-deps.json')}
    monkeypatch.setattr(workspace, 'diff_tool', lambda: '/tmp/bazel-diff-v70')
    calls = []
    def run_command(*args, **kwargs):
        calls.append(args)
        return 'graph TD; test --> hub'
    monkeypatch.setattr(workspace, 'run', run_command)
    assert 'graph TD; test --> hub' in workspace.explain('//tests:test_x')
    assert calls == [('/tmp/bazel-diff-v70', 'explain', '-sh', '/tmp/base.json',
                      '-fh', '/tmp/head.json', '-d', '/tmp/head-deps.json',
                      '--target', '//tests:test_x', '--format', 'mermaid')]


def test_streamed_query_adds_missing_source_edges_and_records_library_owners(tmp_path):
    rows = [
        {'sourceFile': {'name': '//pkg:a.py'}},
        {'sourceFile': {'name': '@@external//:a.py'}},
        {'rule': {'name': '//pkg:a', 'ruleInput': ['//pkg:a.py', '@@external//:a.py'],
                  'attribute': [{'name': 'srcs', 'stringListValue': ['//pkg:a.py']}]}}]
    edges, owners = bazel_gate.source_edges({'//tests:t': {'//pkg:a'}, '//pkg:a': set()}, rows)
    assert edges == {'//tests:t': {'//pkg:a'}, '//pkg:a': {'//pkg:a.py'}}
    assert owners == {'//pkg:a': {'//pkg:a.py'}}


def test_graph_baseline_may_not_be_loosened_or_deleted():
    old = {'u_pct': 20, 'loc_u_pct': 30, 'scc_max': 1, 'reach_budget': 9}
    assert bazel_gate.graph_baseline_errors({**old, 'u_pct': 21}, old)
    assert bazel_gate.graph_baseline_errors({}, old)
    assert bazel_gate.graph_baseline_errors({**old, 'u_pct': 19, 'reach_budget': 8}, old) == []


def test_workspace_reuses_both_graphs_and_fingerprints_only_a_tests_source_and_imports(tmp_path, monkeypatch):
    (tmp_path / 'tests/fixtures').mkdir(parents=True)
    (tmp_path / 'tests/test_t.py').write_text('unchanged\n')
    (tmp_path / 'tests/fixtures/import_fanout_baseline.json').write_text('{}')
    (tmp_path / 'bazel-diff-v70.0.2-linux-amd64').write_bytes(b'fixture binary')
    monkeypatch.setattr(bazel_gate.tempfile, 'gettempdir', lambda: str(tmp_path))
    monkeypatch.setattr(bazel_gate, 'bazel_diff_digest', lambda data: bazel_gate.BAZEL_DIFF_SHA256)
    workspace = bazel_gate.Workspace(root=tmp_path)
    real_module_from = bazel_gate.module_from
    def graph_module(name, path):
        if name != 'graph_impact_for_gate':
            pytest.fail('gate graph checks must not deepen reach through the legacy scanner path')
        return real_module_from(name, path)
    monkeypatch.setattr(bazel_gate, 'module_from', graph_module)
    current, generated = [], []
    monkeypatch.setattr(workspace, 'checkout', lambda sha: current.append(sha))
    monkeypatch.setattr(workspace, 'tests', lambda: {'//tests:test_t'})
    def query(expression):
        if expression.startswith('kind("source file"'):
            return [{'sourceFile': {'name': '//tests:test_t.py'}}]
        return [{'rule': {'name': '//tests:test_t_lib', 'ruleClass': 'py_library',
                          'ruleInput': ['//tests:test_t.py'],
                          'attribute': [{'name': 'srcs', 'stringListValue': ['//tests:test_t.py']}]}}]
    monkeypatch.setattr(workspace, 'streamed_query', query)
    def command(*args, **kwargs):
        assert args[0] == str(tmp_path / 'bazel-diff-v70.0.2-linux-amd64')
        if 'generate-hashes' in args:
            generated.append(current[-1])
            output = Path(args[-1])
            output.write_text(json.dumps({'hashes': {
                '//tests:test_t': f'Rule#transitive-{current[-1]}~own-test',
                '//tests:test_t_lib': f'Rule#transitive-{current[-1]}~own-library',
                '//tests:test_t.py': 'SourceFile#same~same'}}))
            Path(args[args.index('--depEdgesFile') + 1]).write_text(json.dumps({
                '//tests:test_t': ['//tests:test_t_lib'], '//tests:test_t_lib': []}))
        elif 'get-impacted-targets' in args:
            Path(args[args.index('--output') + 1]).write_text('//tests:test_t\n')
        return ''
    monkeypatch.setattr(workspace, 'run', command)
    def unexpected_download(*args, **kwargs):
        pytest.fail('the cached binary must serve generation and selection')
    monkeypatch.setattr(bazel_gate.urllib.request, 'urlopen', unexpected_download)
    workspace.ensure_graphs('base-sha', 'head-sha')
    assert workspace.bazel_diff('base-sha', 'head-sha') == {'//tests:test_t'}
    assert generated == ['base-sha', 'head-sha']
    assert workspace.graph_contents['base']['//tests:test_t'] == {
        'source': 'SourceFile#same~same', 'imports': [], 'deps': ['//tests:test_t_lib']}
    assert workspace.graph_contents['head']['//tests:test_t'] == {
        'source': 'SourceFile#same~same', 'imports': [], 'deps': ['//tests:test_t_lib']}
    assert workspace.snapshots['head']['reach'] == {
        '//tests:test_t': ['//tests:test_t.py']}



def test_single_diff_binary_is_checksum_pinned_and_cached(tmp_path, monkeypatch):
    from io import BytesIO
    monkeypatch.setattr(bazel_gate.tempfile, 'gettempdir', lambda: str(tmp_path))
    monkeypatch.setattr(bazel_gate, 'bazel_diff_digest', lambda data: bazel_gate.BAZEL_DIFF_SHA256)
    downloads = []
    def fetch(url, **kwargs):
        downloads.append(url)
        return BytesIO(b'native diff tool')
    monkeypatch.setattr(bazel_gate.urllib.request, 'urlopen', fetch)
    workspace = bazel_gate.Workspace(root=tmp_path)
    assert downloads == []
    binary = Path(workspace.diff_tool())
    assert binary.read_bytes() == b'native diff tool'
    assert binary.stat().st_mode & 0o111
    assert Path(workspace.diff_tool()) == binary
    assert downloads == [bazel_gate.BAZEL_DIFF_URL]
    assert '/v70.0.2/bazel-diff-rust-linux-amd64' in downloads[0]
    assert bazel_gate.BAZEL_DIFF_SHA256 == '44226a1d1d5239abc8bc6158de1f886fcbbf58e44479ec178df8c01e7e32102e'


def test_a_diff_checksum_mismatch_refuses_the_binary(tmp_path, monkeypatch):
    from io import BytesIO
    monkeypatch.setattr(bazel_gate.tempfile, 'gettempdir', lambda: str(tmp_path))
    monkeypatch.setattr(bazel_gate, 'bazel_diff_digest', lambda data: 'wrong checksum')
    monkeypatch.setattr(bazel_gate.urllib.request, 'urlopen', lambda *args, **kwargs: BytesIO(b'wrong tool'))
    workspace = bazel_gate.Workspace(root=tmp_path)
    with pytest.raises(bazel_gate.GateError, match='checksum mismatch'):
        workspace.diff_tool()
    assert not (tmp_path / 'bazel-diff-v70.0.2-linux-amd64').exists()



def test_gate_metrics_record_counts_without_repeating_full_source_closures():
    measured = {'u_pct': 25, 'reach': {'t': ['a', 'b']},
                'components': [['a'], ['b', 'c']], 'edges': [['t', 'a']], 'loc': {'a': 1}, 'scc_max': 2, 'scc_members': ['b', 'c']}
    record = bazel_gate.graph_metrics(measured)
    assert record == {'u_pct': 25, 'reach_sizes': {'t': 2}, 'cycles': [['b', 'c']],
                      'scc_max': 2, 'scc_members': ['b', 'c']}
    assert measured['reach']['t'] == ['a', 'b']


def test_adapter_produces_packages_base_maxima_and_both_typed_edge_directions():
    before = {'reach': {'//pkg:old': ['//pkg:a.py', '//pkg:b.py'], '//other:t': ['//pkg:a.py']},
              'test_packages': {'//pkg:old': '//pkg', '//other:t': '//other'}}
    after = {'reach': {'//pkg:old': ['//pkg:a.py'], '//new:t': ['//pkg:c.py']},
             'test_packages': {'//pkg:old': '//pkg', '//new:t': '//new'}}
    old_edges = {'rule_to_rule': {('//pkg:old', '//pkg:lib')},
                 'rule_to_source': {('//pkg:lib', '//pkg:b.py')}}
    new_edges = {'rule_to_rule': {('//pkg:old', '//pkg:newlib')},
                 'rule_to_source': {('//pkg:newlib', '//pkg:c.py')}}
    assert bazel_gate.graph_policy_inputs(before, after, old_edges, new_edges) == {
        'test_packages': {'//pkg:old': '//pkg', '//new:t': '//new'},
        'base_package_max_reach': {'//pkg': 2, '//other': 1},
        'edge_diff': {'added': {'rule_to_rule': [['//pkg:old', '//pkg:newlib']],
                                'rule_to_source': [['//pkg:newlib', '//pkg:c.py']]},
                      'removed': {'rule_to_rule': [['//pkg:old', '//pkg:lib']],
                                  'rule_to_source': [['//pkg:lib', '//pkg:b.py']]}}}


def test_edge_types_use_bazel_kinds_instead_of_file_extensions():
    edges = {'//pkg:t': {'//pkg:generated.py', '//pkg:source', '//pkg:lib.py'},
             '//pkg:source': {'//pkg:lib.py'}}
    assert bazel_gate.typed_edges(edges, {'//pkg:t', '//pkg:lib.py'}, {'//pkg:source'}) == {
        'rule_to_rule': {('//pkg:t', '//pkg:lib.py')},
        'rule_to_source': {('//pkg:t', '//pkg:source')}}


def test_graph_only_baseline_writer_keeps_history_and_never_runs_test_populations(tmp_path, monkeypatch, capsys):
    impact = bazel_gate.module_from('core_for_baseline_test', ROOT / 'tools/ci/graph_impact.py')
    workspace = bazel_gate.Workspace(root=tmp_path)
    snapshot = impact.measure({'//pkg:t': {'//pkg:a'}}, {('//pkg:t', '//pkg:a')}, {'//pkg:a': 1})
    baseline = impact.baseline_record(snapshot)
    baseline.pop('edge_groups')
    baseline['figures']['u_pct'] = 200
    workspace.snapshots = {'base': snapshot, 'head': snapshot}
    workspace.graph_impact = impact
    workspace.graph_baselines = {'head': {'history': ['preserved'], 'graph_impact': baseline}}
    calls = []
    def check(base, head, *, record_gains=False):
        calls.append(record_gains)
        return {'summary': 'U 100.0% → 100.0%, SCC max 1 → 1', 'errors': []}
    monkeypatch.setattr(workspace, 'graph_check', check)
    monkeypatch.setattr(workspace, 'ensure_commit', lambda sha: None)
    monkeypatch.setattr(workspace, 'run', lambda *args, **kwargs: pytest.fail('graph mode must not run test populations'))
    monkeypatch.setenv('BUILDBUDDY_ARTIFACTS_DIRECTORY', str(tmp_path / 'artifacts'))
    path = tmp_path / 'tests/fixtures/import_fanout_baseline.json'
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(workspace.graph_baselines['head']))
    assert bazel_gate.main(['graph', '--base', 'base', '--head', 'head', '--write-baseline'], workspace) == 0
    written = json.loads(path.read_text())
    assert written['history'] == ['preserved']
    assert written['graph_impact']['figures']['u_pct'] == 100
    assert 'edge_groups' not in written['graph_impact']
    assert calls == [True]
    assert 'U 100.0% → 100.0%' in capsys.readouterr().out
    original = path.read_bytes()
    monkeypatch.setattr(workspace, 'graph_check', lambda *args, **kwargs: {'summary': 'loss', 'errors': ['SCC grew']})
    assert bazel_gate.main(['graph', '--base', 'base', '--head', 'head', '--write-baseline'], workspace) == 1
    assert path.read_bytes() == original


def test_native_cycle_blame_targets_the_reported_cycle_even_without_a_test_path():
    head = {'reach': {'//tests:unrelated': []}, 'loc': {},
            'edges': [['//new:a', '//new:b'], ['//new:b', '//new:a']]}
    assert bazel_gate.blame_target('SCC grew or merged (2 nodes): //new:a, //new:b', head, []) == '//new:a'
    head['reach']['//tests:affected'] = []
    assert bazel_gate.blame_target('existing test reach grew: //tests:affected +1', head, []) == '//tests:affected'


@pytest.mark.parametrize('stored', [
    {'test_to_test_imports': ['tests/test_a.py -> tests/test_b.py']},
    {'test_to_test_imports': []},
    {'conftest_source_imports': ['conftest.py -> src/prismabuild/core.py']},
    {'conftest_source_imports': []},
    {'test_to_test_imports': [], 'conftest_source_imports': []},
    {'future_relationships': ['//pkg:a -> //pkg:b']},
    {'future_relationships': [['//pkg:a', '//pkg:b']]},
    {'future_edges': []},
    {'graph_impact': {'edge_groups': {'test_to_test': []}}},
    {'metadata': {'future_relationships': [['//pkg:a', '//pkg:b']]}},
])
def test_baseline_writer_refuses_stored_edges_in_head_without_writing(tmp_path, monkeypatch, stored):
    impact = bazel_gate.module_from('core_for_edge_baseline_test', ROOT / 'tools/ci/graph_impact.py')
    snapshot = impact.measure({'//pkg:t': {'//pkg:a'}}, {('//pkg:t', '//pkg:a')}, {'//pkg:a': 1})
    baseline = impact.baseline_record(snapshot)
    baseline.pop('edge_groups')
    head = {'history': ['preserved'], 'graph_impact': baseline, **stored}
    head['graph_impact'] = {**baseline, **head['graph_impact']}
    workspace = bazel_gate.Workspace(root=tmp_path)
    workspace.snapshots = {'head': snapshot}
    workspace.graph_impact = impact
    workspace.graph_baselines = {'head': head}
    monkeypatch.setattr(workspace, 'ensure_commit', lambda sha: None)
    monkeypatch.setattr(workspace, 'graph_check', lambda *args, **kwargs: {
        'summary': 'U 100.0% → 100.0%, SCC max 1 → 1', 'errors': []})
    monkeypatch.setattr(workspace, 'run', lambda *args, **kwargs: pytest.fail('graph mode ran a population'))
    output = tmp_path / 'artifacts'
    monkeypatch.setenv('BUILDBUDDY_ARTIFACTS_DIRECTORY', str(output))
    path = tmp_path / 'tests/fixtures/import_fanout_baseline.json'
    path.parent.mkdir(parents=True)
    original = json.dumps(head)
    path.write_text(original)

    assert bazel_gate.main(['graph', '--base', 'base', '--head', 'head', '--write-baseline'], workspace) == 1
    result = json.loads((output / bazel_gate.RESULT).read_text())
    assert 'stored edge lists' in result['error']
    assert path.read_text() == original
    assert not (output / path.name).exists()


def test_prismabuild_baseline_cannot_enable_transitional_test_imports():
    baseline = bazel_gate.graph_baseline({'u_pct': 20, 'loc_u_pct': 30, 'scc_max': 1, 'reach_budget': 9})
    assert any('strict' in error for error in bazel_gate.graph_baseline_errors(
        {**baseline, 'test_to_test': 'reach'}, baseline))


def test_raw_bazel_diagnostics_stay_outside_uploaded_results(tmp_path):
    root, upload = tmp_path / 'checkout', tmp_path / 'upload'
    root.mkdir()
    logs = ['pr-bazel-remote-grpc.log', 'main-amd64-execution_log.binpb.zst']
    for name in logs:
        (root / name).write_bytes(b'private diagnostic')
    bazel_gate.write_result({'verdict': 'pass'}, upload, root=root)
    assert {path.name for path in upload.iterdir()} == {'gate-result.json'}
    assert all((root / name).read_bytes() == b'private diagnostic' for name in logs)
