#!/usr/bin/env python3
"""The Bazel merge gate. Runs on a BuildBuddy Remote Bazel runner.

pr mode selects the tests a change can affect, runs them on remote execution
and reports Mergify scopes. main mode runs every test on amd64 and arm64 after
a merge and records whether the merged change's own selection would have
caught each failure. Both modes write gate-result.json to the BuildBuddy
artifacts directory, and the GitHub job reads it back. Any detection failure
is a gate failure, never an empty pass.
"""
import argparse
from fnmatch import fnmatch
import importlib.util
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
import tempfile
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[2]
SCHEMA = 'prismabuild.bazel-gate.v1'
RESULT = 'gate-result.json'

BAZEL_DIFF_URL = 'https://github.com/Tinder/bazel-diff/releases/download/v42.0.0/bazel-diff_deploy.jar'
BAZEL_DIFF_SHA256 = '0170b70fe2f24477ab056c11f5c3240d8b45a1dca5ab73ad252c096679c5500c'

# A change to any of these can move every target: test everything. Patterns
# without '/' name root files. The selector's evidence code decides the graph.
FALLBACK = [
    'MODULE.bazel', 'MODULE.bazel.lock', '.bazelrc', '.bazelversion', '.bazelignore', 'BUILD.bazel',
    'tools/bazel/*', '.github/workflows/*', 'tools/ci/bazel_gate.py', 'tools/ci/remote_gate.py',
    'tools/ci/gate_requirements.txt', 'tools/ci/gate_select.py', 'tools/ci/gate_paths.py',
]
# Files no build or test reads. Only these may change without selecting a target.
IRRELEVANT = ['.mergify.yml', '.github/*.md', '.github/ISSUE_TEMPLATE/*', '.github/CODEOWNERS']
CONFIGS = {'amd64': 'remote', 'arm64': 'remote-arm64'}
# .bazelrc's default names; test and build runs prefix their own.
DIAGNOSTICS = ['bazel-remote-grpc.log', 'execution_log.binpb.zst']
TESTS = 'tests(//...)'
HOSTED = 'gate-lane-hosted'


def bazel_diff_digest(data):
    """PrismaBuild's one digest owner (#1328), loaded by path."""
    return module_from('bazel_gate_digests', ROOT / 'src/prismabuild/digest_primitives.py').raw_sha256(data)


class GateError(RuntimeError):
    """Detection could not establish what to test. The gate fails."""


# fnmatch's * also crosses '/', so 'tools/bazel/*' covers the whole tree.
def fallback_files(changed):
    return sorted(path for path in changed if any(
        fnmatch(path, pattern) if '/' in pattern else path == pattern for pattern in FALLBACK))


def irrelevant(path):
    return any(fnmatch(path, pattern) for pattern in IRRELEVANT)


def build_graph(path):
    return Path(path).name in ('BUILD.bazel', 'BUILD') or path.endswith('.bzl')


def needs_no_diff(changed, deleted, used_sources):
    """Only unread, allowlisted files changed; bazel-diff has nothing to find."""
    return not deleted and all(irrelevant(path) and path not in used_sources for path in changed)


def diff_commands(java, jar, root, bazel, base_hashes, head_hashes, impacted):
    """bazel-diff v42 command lines: a hash generator, then the impacted query."""
    def generate(output):
        return [str(java), '-jar', str(jar), 'generate-hashes', '--workspacePath', str(root),
                '--bazelPath', str(bazel), '--excludeExternalTargets', '--includeTargetType',
                str(output)]
    return generate, [str(java), '-jar', str(jar), 'get-impacted-targets',
                      '--workspacePath', str(root), '--bazelPath', str(bazel),
                      '--targetType', 'Rule', '--startingHashes', str(base_hashes),
                      '--finalHashes', str(head_hashes), '--output', str(impacted)]


def everything(reason, tests, hosted):
    return {'selection': 'fallback', 'reason': sorted(reason), 'selected': sorted(tests),
            'hosted': sorted(hosted), 'build': ['//...'], 'scopes': [], 'all_scopes': True}


def gate_decision(changed, deleted, impacted, used_sources, tests, hosted=frozenset()):
    """Decide what to test. Pure; every input comes from git or Bazel.

    tests are the targets Bazel runs; hosted are the manual targets the
    workflow's hosted lane runs (tools/bazel/nonhermetic.bzl).
    """
    if not changed and not deleted:
        raise GateError('no changed files between base and head; refusing to pass an empty diff')
    forced = fallback_files(changed | deleted)
    if forced:
        return everything(forced, tests, hosted)
    unmapped = sorted(path for path in changed - deleted
                      if path not in used_sources and not irrelevant(path))
    if unmapped:
        raise GateError('changed files that no Bazel target reads: ' + ', '.join(unmapped)
                        + '. Run tools/bazel/generate.sh and commit what it writes (docs/bazel.md), '
                        'or add the pattern to IRRELEVANT in tools/ci/bazel_gate.py if nothing can read them.')
    relevant = [path for path in changed | deleted if not irrelevant(path)]
    if not impacted and relevant and all(build_graph(path) for path in relevant):
        # A BUILD or .bzl edit that changes no rule hash (formatting, comments): test everything.
        return everything(relevant, tests, hosted)
    if not impacted and relevant:
        raise GateError('bazel-diff found no impacted targets for build-relevant changes')
    selected = sorted(label for label in impacted if label in tests)
    scopes = sorted({label.split(':', 1)[0] for label in impacted if label.startswith('//')})
    # Impacted targets no test depends on would otherwise never be built.
    build = sorted(label for label in impacted if label not in tests and label not in hosted)
    return {'selection': 'impacted' if impacted else 'irrelevant', 'reason': [], 'selected': selected,
            'hosted': sorted(label for label in impacted if label in hosted), 'build': build,
            'scopes': scopes, 'all_scopes': False}


def verdict_of(selected, statuses):
    """Every selected test must report PASSED; a missing status fails."""
    failed = sorted(label for label in selected if statuses.get(label) != 'PASSED')
    return ('fail' if failed else 'pass' if selected else 'skipped'), failed


def test_statuses(bep_path):
    statuses = {}
    if not bep_path.exists():
        return statuses
    for line in bep_path.read_text().splitlines():
        event = json.loads(line)
        summary = event.get('id', {}).get('testSummary')
        if summary:
            statuses[summary['label']] = event.get('testSummary', {}).get('overallStatus', 'NO_STATUS')
    return statuses


def canonical_label(label):
    return label[1:] if label.startswith('@//') else label.removeprefix('@@')


def module_from(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class Workspace:
    """Git and Bazel on the runner. Every failure raises GateError."""

    def __init__(self, root=ROOT, bazel='bazel'):
        self.root, self.bazel = root, bazel

    def run(self, *command, capture=True, check=True):
        print('$ ' + shlex.join(map(str, command)), flush=True)
        result = subprocess.run(list(map(str, command)), cwd=self.root, text=True,
                                stdout=subprocess.PIPE if capture else None)
        if check and result.returncode:
            raise GateError(f'{shlex.join(map(str, command))} exited {result.returncode}')
        return result.stdout if capture else result.returncode

    def ensure_commit(self, sha):
        if not sha:
            raise GateError('empty base or head; the queue-aware refs were unavailable')
        present = subprocess.run(['git', 'cat-file', '-e', f'{sha}^{{commit}}'], cwd=self.root,
                                 stderr=subprocess.DEVNULL)
        if present.returncode:
            self.run('git', 'fetch', '--quiet', '--no-tags', 'origin', sha)

    def changes(self, base, head):
        output = self.run('git', 'diff', '--name-status', '--no-renames', '-z', base, head)
        parts = output.split('\0')
        changed, deleted = set(), set()
        for status, path in zip(parts[0::2], parts[1::2]):
            (deleted if status.startswith('D') else changed).add(path)
        return changed, deleted

    def checkout(self, sha):
        self.run('git', 'checkout', '--quiet', '--force', '--end-of-options', sha, '--')

    def query(self, expression):
        return self.run(self.bazel, 'query', expression, '--output=label', '--keep_going=false').split()

    def tests(self):
        """The population; manual targets are tests outside the hosted gate."""
        return set(map(canonical_label, self.query(f'{TESTS} except attr(tags, "\\bmanual\\b", //...)')))

    def hosted_tests(self):
        """Manual targets the workflow's hosted lane runs instead of Bazel."""
        return set(map(canonical_label, self.query(f'attr(tags, "\\b{HOSTED}\\b", {TESTS})')))

    def used_sources(self):
        # BUILD and .bzl files define targets rather than being read by one; Bazel still reads them.
        labels = self.query('kind("source file", deps(//...))') + self.query('buildfiles(//...)')
        return {label.removeprefix('//').replace(':', '/').lstrip('/') for label in map(canonical_label, labels)
                if label.startswith('//')}

    def bazel_diff(self, base, head):
        jar = Path(tempfile.gettempdir(), 'bazel-diff-v42.0.0.jar')
        if not jar.exists() or bazel_diff_digest(jar.read_bytes()) != BAZEL_DIFF_SHA256:
            with urllib.request.urlopen(BAZEL_DIFF_URL, timeout=120) as response:
                data = response.read()
            if bazel_diff_digest(data) != BAZEL_DIFF_SHA256:
                raise GateError('bazel-diff jar checksum mismatch')
            jar.write_bytes(data)
        java = Path(self.run(self.bazel, 'info', 'java-home').strip(), 'bin/java')
        hashes = {name: Path(tempfile.gettempdir(), f'{name}-hashes.json') for name in ['base', 'head']}
        impacted = Path(tempfile.gettempdir(), 'impacted.txt')
        impacted.unlink(missing_ok=True)
        generate, query = diff_commands(java, jar, self.root, self.bazel,
                                        hashes['base'], hashes['head'], impacted)
        for name, sha in [('base', base), ('head', head)]:
            self.checkout(sha)
            self.run(*generate(hashes[name]))
        self.run(*query)
        return {canonical_label(line) for line in impacted.read_text().split() if line}

    def test(self, targets, config, metadata, log_name):
        """Run targets; return per-label status. Build failures leave labels without status."""
        if not targets:
            return {}
        patterns = Path(tempfile.gettempdir(), f'{log_name}-targets.txt')
        patterns.write_text('\n'.join(targets) + '\n')
        bep = Path(tempfile.gettempdir(), f'{log_name}-bep.json')
        bep.unlink(missing_ok=True)
        self.run(self.bazel, 'test', f'--config={config}', '--config=ci', '--keep_going',
                 f'--target_pattern_file={patterns}', f'--build_event_json_file={bep}',
                 *diagnostic_logs(log_name),
                 *[f'--build_metadata={key}={value}' for key, value in metadata.items()],
                 capture=False, check=False)
        return test_statuses(bep)

    def build(self, targets, config, metadata, log_name):
        """Build targets; True when every one built."""
        if not targets:
            return True
        patterns = Path(tempfile.gettempdir(), f'{log_name}-build-targets.txt')
        patterns.write_text('\n'.join(targets) + '\n')
        return not self.run(self.bazel, 'build', f'--config={config}', '--config=ci', '--keep_going',
                            f'--target_pattern_file={patterns}', *diagnostic_logs(f'{log_name}-build'),
                            *[f'--build_metadata={key}={value}' for key, value in metadata.items()],
                            capture=False, check=False)

    def budget_check(self, base):
        """The hosted gate refused budgets that grow against the target; so does this one."""
        gate_run = module_from('gate_run_for_bazel_gate', self.root / 'tools/ci/gate_run.py')
        try:
            gate_run._budget_policy(self.root, base)
        except (gate_run.BudgetFailure, RuntimeError, subprocess.CalledProcessError) as error:
            return [f'per-test budgets: {error}']
        return []

    def start_generated_checks(self):
        """Start the generated-input checks, which need no Bazel server, so they
        run while the tests do. Returns (command, process) pairs for inputs_check."""
        generated = module_from('bazel_generated_for_gate', self.root / 'tools/ci/bazel_generated.py')
        started = []
        for command in generated.commands():
            print('$ ' + shlex.join(map(str, command)) + ' &', flush=True)
            started.append((command, subprocess.Popen(list(map(str, command)), cwd=self.root)))
        return started

    def inputs_check(self, started=None):
        """Population parity on the head tree, then the generated-input checks:
        those started earlier are awaited, the rest run now."""
        problems = []
        parity = [sys.executable, 'tools/ci/bazel_parity.py', 'static', '--bazel', self.bazel]
        if self.run(*parity, capture=False, check=False):
            problems.append(shlex.join(map(str, parity)))
        for command, process in self.start_generated_checks() if started is None else started:
            if process.wait():
                problems.append(shlex.join(map(str, command)))
        return problems


def diagnostic_logs(prefix):
    """Per-invocation names, so later helper invocations can't overwrite these logs."""
    return [f'--remote_grpc_log={prefix}-{DIAGNOSTICS[0]}',
            f'--execution_log_compact_file={prefix}-{DIAGNOSTICS[1]}']


def write_result(result, directory, root=ROOT):
    directory.mkdir(parents=True, exist_ok=True)
    (directory / RESULT).write_text(json.dumps(result, indent=2) + '\n')
    for suffix in DIAGNOSTICS:
        for log in root.glob(f'*{suffix}'):
            log.replace(directory / log.name)


def gate_pr(workspace, base, head, metadata):
    started = time.monotonic()
    for sha in [base, head]:
        workspace.ensure_commit(sha)
    changed, deleted = workspace.changes(base, head)
    workspace.checkout(head)
    used = workspace.used_sources()
    forced = fallback_files(changed | deleted)
    skip = forced or needs_no_diff(changed, deleted, used)
    impacted = set() if skip else workspace.bazel_diff(base, head)
    workspace.checkout(head)
    tests, hosted = workspace.tests(), workspace.hosted_tests()
    decision = gate_decision(changed, deleted, impacted, used, tests, hosted)
    selected_seconds = time.monotonic() - started
    # The generated-input checks need no Bazel server: they run beside the tests.
    checks = workspace.start_generated_checks()
    statuses = workspace.test(decision['selected'], CONFIGS['amd64'], metadata, 'pr')
    tested_seconds = time.monotonic() - started
    verdict, failed = verdict_of(decision['selected'], statuses)
    inputs = workspace.inputs_check(checks) + workspace.budget_check(base)
    if not workspace.build(decision['build'], CONFIGS['amd64'], metadata, 'pr'):
        inputs.append('bazel build of impacted targets failed: ' + ' '.join(decision['build']))
    return {**decision, 'verdict': verdict, 'failed': failed, 'inputs': inputs,
            'changed': sorted(changed | deleted),
            'durations': {'selection': round(selected_seconds, 1),
                          'tests': round(tested_seconds - selected_seconds, 1),
                          'total': round(time.monotonic() - started, 1)}}


def gate_main(workspace, base, head, metadata):
    """Full run after a merge, on both architectures, plus what selection would have run."""
    started = time.monotonic()
    for sha in [base, head]:
        workspace.ensure_commit(sha)
    changed, deleted = workspace.changes(base, head)
    try:
        forced = fallback_files(changed | deleted)
        impacted = set() if forced else workspace.bazel_diff(base, head)
        workspace.checkout(head)
        tests = workspace.tests()
        selection = gate_decision(changed, deleted, impacted, workspace.used_sources(), tests,
                                  workspace.hosted_tests())
    except GateError as error:
        workspace.checkout(head)
        tests = workspace.tests()
        selection = {'selection': 'error', 'reason': [str(error)], 'selected': sorted(tests)}
    population = sorted(tests)
    architectures = {}
    for architecture, config in CONFIGS.items():
        begun = time.monotonic()
        statuses = workspace.test(population, config, metadata, f'main-{architecture}')
        verdict, failed = verdict_of(population, statuses)
        architectures[architecture] = {
            'verdict': verdict, 'failed': failed,
            'missed': sorted(label for label in failed if label not in selection['selected']),
            'seconds': round(time.monotonic() - begun, 1)}
    return {'selection': selection['selection'], 'reason': selection['reason'],
            'selected': selection['selected'], 'tests': len(population),
            'hosted': sorted(workspace.hosted_tests()),
            'architectures': architectures, 'inputs': workspace.inputs_check(),
            'changed': sorted(changed | deleted),
            'durations': {'total': round(time.monotonic() - started, 1)}}


def passed(result):
    if result.get('error') or result.get('inputs'):
        return False
    if result['mode'] == 'main':
        return all(not arch['failed'] for arch in result['architectures'].values())
    return result['verdict'] != 'fail'


def main(argv=None, workspace=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=['pr', 'main'])
    parser.add_argument('--base', required=True)
    parser.add_argument('--head', required=True)
    parser.add_argument('--branch', default='')
    args = parser.parse_args(argv)
    output = Path(os.environ.get('BUILDBUDDY_ARTIFACTS_DIRECTORY', ROOT / 'gate-artifacts'))
    metadata = {'COMMIT_SHA': args.head, 'BRANCH_NAME': args.branch}
    result = {'schema': SCHEMA, 'mode': args.mode, 'base': args.base, 'head': args.head}
    try:
        gate = gate_pr if args.mode == 'pr' else gate_main
        result.update(gate(workspace or Workspace(), args.base, args.head, metadata))
        result['error'] = None
    except GateError as error:
        result['error'] = str(error)
    write_result(result, output)
    print(json.dumps({key: result.get(key) for key in ['selection', 'error', 'verdict', 'inputs']}))
    return 0 if passed(result) else 1


if __name__ == '__main__':
    sys.exit(main())
