#!/usr/bin/env python3
"""The Bazel merge gate. Runs on a BuildBuddy Remote Bazel runner.

pr mode selects the tests a change can affect, runs them on remote execution
and reports Mergify scopes. main mode runs every test on amd64, and on arm64
every test its executors can host, after a merge and records whether the
merged change's own selection would have caught each failure. Both modes write gate-result.json to the BuildBuddy
artifacts directory, and the GitHub job reads it back. Any detection failure
is a gate failure, never an empty pass.
"""
import argparse
from fnmatch import fnmatch
import importlib.util
import json
import math
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys
import tempfile
import time
import urllib.request
import zipfile

ROOT = Path(__file__).resolve().parents[2]
SCHEMA = 'prismabuild.bazel-gate.v1'
RESULT = 'gate-result.json'

BAZEL_DIFF_URL = 'https://github.com/Tinder/bazel-diff/releases/download/v42.0.0/bazel-diff_deploy.jar'
BAZEL_DIFF_SHA256 = '0170b70fe2f24477ab056c11f5c3240d8b45a1dca5ab73ad252c096679c5500c'
BAZEL_EXPLAIN_URL = 'https://github.com/Tinder/bazel-diff/releases/download/v70.0.2/bazel-diff-rust-linux-amd64'
BAZEL_EXPLAIN_SHA256 = '44226a1d1d5239abc8bc6158de1f886fcbbf58e44479ec178df8c01e7e32102e'

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
# nonhermetic.bzl lanes BuildBuddy's arm64 executors can't host: the Remote
# Bazel runner is amd64 only, and the executors keep every writable path on
# tmpfs. main mode leaves them out of its arm64 pass and records them; the
# post-merge hosted lane runs their files on ubuntu-24.04-arm.
ARM64_EXCLUDED = ('runner', 'hosted-arm64')
# With --show_timestamps, Bazel prefixes each stderr line with the time.
ERROR_LINE = re.compile(r'(\(\d\d:\d\d:\d\d\) )?ERROR: ')


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
                '--depEdgesFile', str(Path(output).with_name(Path(output).stem + '-deps.json')),
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


def bep_run(text):
    """Per-label test status, the invocation ID and Bazel's first ERROR line, from a JSON BEP file."""
    statuses, invocation, error = {}, None, None
    for line in text.splitlines():
        event = json.loads(line)
        if 'started' in event.get('id', {}):
            invocation = event.get('started', {}).get('uuid')
        summary = event.get('id', {}).get('testSummary')
        if summary:
            statuses[summary['label']] = event.get('testSummary', {}).get('overallStatus', 'NO_STATUS')
        if error is None:
            error = next((output for output in event.get('progress', {}).get('stderr', '').splitlines()
                          if ERROR_LINE.match(output)), None)
    return {'statuses': statuses, 'invocation': invocation, 'error': error}


def architecture_result(population, run, selected):
    """One architecture's verdict. An invocation that ran no test failed as a
    whole, before any test: an error naming it, not a failure per label."""
    if population and not run['statuses']:
        return {'verdict': 'error', 'failed': [], 'missed': [],
                'error': f"invocation {run['invocation'] or '(unknown)'} ran no tests: "
                         f"{run['error'] or 'its build events hold no ERROR line'}"}
    verdict, failed = verdict_of(population, run['statuses'])
    return {'verdict': verdict, 'failed': failed,
            'missed': sorted(label for label in failed if label not in selected)}


def arm64_population(population, excluded):
    """The tests arm64 executors run, and the rest by nonhermetic.bzl lane."""
    left_out = {tag: sorted(set(population) & labels) for tag, labels in excluded.items()}
    skipped = {label for labels in left_out.values() for label in labels}
    return sorted(label for label in population if label not in skipped), left_out


def has_output(outputs, name):
    """Whether a test.outputs directory holds name, as a file or inside outputs.zip."""
    if (outputs / name).is_file():
        return True
    archive = outputs / 'outputs.zip'
    if not archive.is_file():
        return False
    with zipfile.ZipFile(archive) as zipped:
        return name in zipped.namelist()


def rerun_budget_only(workspace, labels, statuses, config, metadata, log_name):
    """Rerun once, uncached, the targets that failed only a time budget. A second
    overrun fails; real failures are never retried."""
    failed = [label for label in labels if statuses.get(label) != 'PASSED']
    budget = sorted(workspace.budget_only(failed, statuses))
    if not budget:
        return statuses, {}
    again = workspace.test(budget, config, metadata, f'{log_name}-budget-rerun',
                           extra=['--nocache_test_results'])['statuses']
    reruns = {label: again.get(label, 'NO_STATUS') for label in budget}
    return {**statuses, **reruns}, reruns


def canonical_label(label):
    return label[1:] if label.startswith('@//') else label.removeprefix('@@')


def module_from(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def source_edges(edges, rows):
    """Restore source inputs omitted by bazel-diff v42, using Bazel metadata."""
    sources = {canonical_label(row['sourceFile']['name']) for row in rows if 'sourceFile' in row
               and canonical_label(row['sourceFile']['name']).startswith('//')}
    result, owners = {node: set(deps) for node, deps in edges.items()}, {}
    for row in rows:
        rule = row.get('rule')
        if not rule:
            continue
        label = canonical_label(rule['name'])
        if label not in result:
            continue
        result[label].update(canonical_label(dep) for dep in rule.get('ruleInput', ())
                             if canonical_label(dep) in sources)
        kind = rule.get('ruleClass', 'py_library')
        if kind.endswith('_test') or kind == 'filegroup':
            continue
        srcs = {canonical_label(dep) for attr in rule.get('attribute', ()) if attr['name'] == 'srcs'
                for dep in attr.get('stringListValue', ()) if canonical_label(dep) in sources}
        if srcs:
            owners[label] = srcs
    return result, owners


def graph_metrics(measured):
    """Keep the gate record small; full closures can be read from the graph artifacts."""
    return {**{key: value for key, value in measured.items() if key not in ('reach', 'sccs')},
            'reach_sizes': {test: len(files) for test, files in measured['reach'].items()},
            'cycles': [group for group in measured['sccs'] if len(group) > 1]}


def graph_baseline_errors(current, previous):
    errors = []
    for key in ('u_pct', 'loc_u_pct', 'scc_max', 'reach_budget'):
        value = current.get(key)
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
            errors.append(f'graph baseline needs a finite nonnegative {key}')
        elif key in previous and value > previous[key]:
            errors.append(f'graph baseline may only tighten: {key} ({previous[key]} -> {value})')
    return errors


class Workspace:
    """Git and Bazel on the runner. Every failure raises GateError."""

    def __init__(self, root=ROOT, bazel='bazel'):
        self.root, self.bazel = root, bazel
        self.graph_revisions = None

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

    def arm64_excluded(self):
        """Tests in lanes arm64 executors can't host, by nonhermetic.bzl tag."""
        return {tag: set(map(canonical_label, self.query(f'attr(tags, "\\b{tag}\\b", {TESTS})')))
                for tag in ARM64_EXCLUDED}

    def used_sources(self):
        # BUILD and .bzl files define targets rather than being read by one; Bazel still reads them.
        labels = self.query('kind("source file", deps(//...))') + self.query('buildfiles(//...)')
        return {label.removeprefix('//').replace(':', '/').lstrip('/') for label in map(canonical_label, labels)
                if label.startswith('//')}

    def ensure_graphs(self, base, head):
        """Freeze both revisions once, even when selection tests everything."""
        if self.graph_revisions == (base, head):
            return
        impact = module_from('graph_impact_for_gate', ROOT / 'tools/ci/graph_impact.py')
        fanout = module_from('import_fanout_for_gate', ROOT / 'tools/ci/import_fanout.py')
        jar = Path(tempfile.gettempdir(), 'bazel-diff-v42.0.0.jar')
        if not jar.exists() or bazel_diff_digest(jar.read_bytes()) != BAZEL_DIFF_SHA256:
            with urllib.request.urlopen(BAZEL_DIFF_URL, timeout=120) as response:
                data = response.read()
            if bazel_diff_digest(data) != BAZEL_DIFF_SHA256:
                raise GateError('bazel-diff jar checksum mismatch')
            jar.write_bytes(data)
        self.diff_java = str(Path(self.run(self.bazel, 'info', 'java-home').strip(), 'bin/java'))
        self.diff_jar = jar
        self.graph_directory = tempfile.TemporaryDirectory(prefix='graph-impact-')
        directory = Path(self.graph_directory.name)
        self.hashes = {name: directory / f'{name}.json' for name in ('base', 'head')}
        self.deps = {name: directory / f'{name}-deps.json' for name in ('base', 'head')}
        self.impacted_path = directory / 'impacted.txt'
        generate, self.impact_command = diff_commands(self.diff_java, jar, self.root, self.bazel,
                                                       self.hashes['base'], self.hashes['head'],
                                                       self.impacted_path)
        self.snapshots, self.graph_baselines, self.graph_allowlists, self.graph_imports = {}, {}, {}, {}
        self.graph_owners = {}
        try:
            for name, sha in [('base', base), ('head', head)]:
                self.checkout(sha)
                self.run(*generate(self.hashes[name]))
                raw = json.loads(self.deps[name].read_text())
                edges = {canonical_label(node): {canonical_label(dep) for dep in deps
                                                 if canonical_label(dep).startswith('//')}
                         for node, deps in raw.items() if canonical_label(node).startswith('//')}
                source_rows = self.streamed_query('kind("source file", deps(tests(//...)))')
                # The source-only query cannot say which rule owns each file.
                # One batched rule query supplies source ownership and inputs.
                rule_rows = self.streamed_query('kind(".* rule", //...)')
                edges, owners = source_edges(edges, source_rows + rule_rows)
                loc = {}
                for row in source_rows:
                    label = canonical_label(row['sourceFile']['name'])
                    if label.startswith('//'):
                        path = self.root / label.removeprefix('//').replace(':', '/').lstrip('/')
                        loc[label] = len(path.read_bytes().splitlines())
                tests = self.tests()
                if tests - edges.keys():
                    raise GateError('graph missing test labels: ' + ', '.join(sorted(tests - edges.keys())))
                hashes = json.loads(self.hashes[name].read_text())['hashes']
                library_imports = {
                    canonical_label(row['rule']['name']): sorted(
                        canonical_label(dep) for attr in row['rule'].get('attribute', ())
                        if attr['name'] == 'deps' for dep in attr.get('stringListValue', ())
                        if canonical_label(dep).startswith('//'))
                    for row in rule_rows if 'rule' in row}
                contents = {test: {
                    'source': hashes.get(test + '.py'),
                    'imports': library_imports.get(test + '_lib', []),
                } for test in tests}
                self.deps[name].write_text(json.dumps({node: sorted(deps) for node, deps in edges.items()}))
                metadata = {'tests': sorted(tests), 'loc': loc, 'contents': contents}
                (directory / f'{name}-metadata.json').write_text(json.dumps(metadata))
                self.snapshots[name] = impact.Snapshot(edges, {test: {test} for test in tests}, loc, contents)
                self.graph_owners[name] = owners
                self.graph_imports[name] = fanout.bazel_imports(edges, owners)
                baseline = self.root / 'tests/fixtures/import_fanout_baseline.json'
                self.graph_baselines[name] = json.loads(baseline.read_text())
                if not isinstance(self.graph_baselines[name].get('graph_impact', {}), dict):
                    raise GateError('graph_impact baseline must be an object')
                allowlist = self.root / 'tests/fixtures/graph_impact_allowlist.json'
                self.graph_allowlists[name] = json.loads(allowlist.read_text()) if allowlist.exists() else {}
                if not isinstance(self.graph_allowlists[name], dict):
                    raise GateError('graph impact allowlist must be an object')
        except (OSError, ValueError, KeyError, TypeError) as error:
            raise GateError(f'graph impact inputs: {error}') from error
        finally:
            self.checkout(head)
        artifacts = os.environ.get('BUILDBUDDY_ARTIFACTS_DIRECTORY')
        if artifacts:
            output = Path(artifacts) / 'graph-impact'
            output.mkdir(parents=True, exist_ok=True)
            for path in directory.glob('*.json'):
                (output / path.name).write_bytes(path.read_bytes())
        self.graph_revisions = (base, head)
        self.graph_impact, self.import_fanout = impact, fanout

    def streamed_query(self, expression):
        return [json.loads(line) for line in self.run(self.bazel, 'query', expression,
                '--output=streamed_jsonproto', '--keep_going=false').splitlines() if line.strip()]

    def bazel_diff(self, base, head):
        self.ensure_graphs(base, head)
        self.run(*self.impact_command)
        return {canonical_label(line) for line in self.impacted_path.read_text().split() if line}

    def explain_tool(self):
        """v70 reads v42 hashes; it is used only for native explanations."""
        binary = Path(tempfile.gettempdir(), 'bazel-diff-explain-v70.0.2-linux-amd64')
        if not binary.exists() or bazel_diff_digest(binary.read_bytes()) != BAZEL_EXPLAIN_SHA256:
            with urllib.request.urlopen(BAZEL_EXPLAIN_URL, timeout=120) as response:
                data = response.read()
            if bazel_diff_digest(data) != BAZEL_EXPLAIN_SHA256:
                raise GateError('bazel-diff explain binary checksum mismatch')
            binary.write_bytes(data)
        binary.chmod(0o755)
        return str(binary)

    def explain(self, target):
        try:
            output = self.run(self.explain_tool(), 'explain',
                              '-sh', str(self.hashes['base']), '-fh', str(self.hashes['head']),
                              '-d', str(self.deps['head']), '--target', target, '--format', 'mermaid')
        except (GateError, OSError) as error:
            return f'bazel-diff blame unavailable: {error}'
        return '```mermaid\n' + output.strip() + '\n```'

    def graph_check(self, base, head):
        try:
            self.ensure_graphs(base, head)
            impact = self.graph_impact
            before, after = self.snapshots['base'], self.snapshots['head']
            previous = self.graph_baselines['base'].get('graph_impact', {})
            baseline = self.graph_baselines['head'].get('graph_impact', {})
            policy_errors = graph_baseline_errors(baseline, previous) if baseline else []
            policy_errors += impact.allowlist_errors(self.graph_allowlists['head'], self.graph_allowlists['base'])
            live = impact.measure(after)
            if not baseline:
                baseline = impact.baseline_record(impact.measure(before))
                policy_errors.append('graph impact baseline missing: record graph_impact in import_fanout_baseline.json')
            if graph_baseline_errors(baseline, {}):
                baseline = impact.baseline_record(impact.measure(before))
            errors = impact.compare(before, after, baseline, self.graph_allowlists['head'], self.explain)
            if policy_errors:
                target = '//tools:ci/bazel_gate' if '//tools:ci/bazel_gate' in after.edges else next(iter(after.roots))
                blame = self.explain(target)
                errors.extend(error + '\n' + blame for error in policy_errors)
            for error in self.import_fanout.bazel_regressions(self.graph_imports['head'], self.graph_imports['base']):
                # Blame the new edge's consuming library, whose source spelling
                # is printed by the existing import policy.
                path = error.split('entry: ', 1)[1].split(' -> ', 1)[0]
                label = next(node for node, sources in sorted(self.graph_owners['head'].items())
                             if any(source.removeprefix('//').replace(':', '/').lstrip('/') == path
                                    for source in sources))
                key = error.split(' entry:', 1)[0].removeprefix('new ')
                entry = error.split('entry: ', 1)[1]
                if f'{key}:{entry}' not in self.graph_allowlists['head']:
                    errors.append(error + '\n' + self.explain(label))
            return {'summary': impact.summary(before, after, baseline['reach_budget']),
                    'base': graph_metrics(impact.measure(before)), 'head': graph_metrics(live),
                    'added_edges': sorted(impact.changes(before, after)[0]),
                    'fanout_growth': impact.changes(before, after)[1], 'errors': errors}
        except GateError as error:
            return {'summary': 'U unavailable, SCC max unavailable: ' + str(error), 'errors': [str(error)]}

    def test(self, targets, config, metadata, log_name, extra=()):
        """Run targets; return bep_run's statuses, invocation and first ERROR line.
        Build failures leave labels without status."""
        if not targets:
            return bep_run('')
        patterns = Path(tempfile.gettempdir(), f'{log_name}-targets.txt')
        patterns.write_text('\n'.join(targets) + '\n')
        bep = Path(tempfile.gettempdir(), f'{log_name}-bep.json')
        bep.unlink(missing_ok=True)
        self.run(self.bazel, 'test', f'--config={config}', '--config=ci', '--keep_going',
                 f'--target_pattern_file={patterns}', f'--build_event_json_file={bep}',
                 *diagnostic_logs(log_name), *extra,
                 *[f'--build_metadata={key}={value}' for key, value in metadata.items()],
                 capture=False, check=False)
        return bep_run(bep.read_text() if bep.exists() else '')

    def budget_only(self, labels, statuses):
        """Failed labels to rerun once: a Bazel TIMEOUT, or the runner's budget marker
        in every shard that did not pass. A shard that passed left inventory.json."""
        marker = module_from('gate_run_for_bazel_gate', self.root / 'tools/ci/gate_run.py').BUDGET_MARKER
        found = set()
        for label in labels:
            if statuses.get(label) == 'TIMEOUT':
                found.add(label)
                continue
            package, _, name = label.removeprefix('//').partition(':')
            logs = self.root / 'bazel-testlogs' / package / name
            runs = sorted(logs.glob('shard_*_of_*/test.outputs')) or [logs / 'test.outputs']
            marked = [has_output(run, marker) for run in runs]
            if any(marked) and all(mark or has_output(run, 'inventory.json')
                                   for mark, run in zip(marked, runs)):
                found.add(label)
        return found

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
    graph_impact = workspace.graph_check(base, head)
    print(graph_impact['summary'], flush=True)
    try:
        forced = fallback_files(changed | deleted)
        skip = forced or needs_no_diff(changed, deleted, used)
        impacted = set() if skip else workspace.bazel_diff(base, head)
        workspace.checkout(head)
        tests, hosted = workspace.tests(), workspace.hosted_tests()
        decision = gate_decision(changed, deleted, impacted, used, tests, hosted)
    except GateError as error:
        return {'error': str(error), 'inputs': graph_impact['errors'], 'graph_impact': graph_impact}
    selected_seconds = time.monotonic() - started
    # The generated-input checks need no Bazel server: they run beside the tests.
    checks = workspace.start_generated_checks()
    statuses = workspace.test(decision['selected'], CONFIGS['amd64'], metadata, 'pr')['statuses']
    statuses, reruns = rerun_budget_only(workspace, decision['selected'], statuses, CONFIGS['amd64'],
                                         metadata, 'pr')
    tested_seconds = time.monotonic() - started
    verdict, failed = verdict_of(decision['selected'], statuses)
    inputs = workspace.inputs_check(checks) + workspace.budget_check(base) + graph_impact['errors']
    if not workspace.build(decision['build'], CONFIGS['amd64'], metadata, 'pr'):
        inputs.append('bazel build of impacted targets failed: ' + ' '.join(decision['build']))
    return {**decision, 'verdict': verdict, 'failed': failed, 'inputs': inputs, 'budget_reruns': reruns,
            'graph_impact': graph_impact,
            'changed': sorted(changed | deleted),
            'durations': {'selection': round(selected_seconds, 1),
                          'tests': round(tested_seconds - selected_seconds, 1),
                          'total': round(time.monotonic() - started, 1)}}


def gate_main(workspace, base, head, metadata):
    """Full run after a merge, on both architectures, plus what selection would have run.
    arm64 leaves out the lanes in ARM64_EXCLUDED and records them."""
    started = time.monotonic()
    for sha in [base, head]:
        workspace.ensure_commit(sha)
    changed, deleted = workspace.changes(base, head)
    graph_impact = workspace.graph_check(base, head)
    print(graph_impact['summary'], flush=True)
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
    runnable = {'amd64': (population, {}), 'arm64': arm64_population(population, workspace.arm64_excluded())}
    architectures = {}
    for architecture, config in CONFIGS.items():
        begun = time.monotonic()
        targets, excluded = runnable[architecture]
        run = workspace.test(targets, config, metadata, f'main-{architecture}')
        run['statuses'], reruns = rerun_budget_only(workspace, targets, run['statuses'], config, metadata,
                                                   f'main-{architecture}')
        architectures[architecture] = {
            **architecture_result(targets, run, selection['selected']), 'invocation': run['invocation'],
            'tests': len(targets), 'excluded': {tag: len(labels) for tag, labels in excluded.items()},
            'excluded_tests': excluded, 'budget_reruns': reruns,
            'seconds': round(time.monotonic() - begun, 1)}
    return {'selection': selection['selection'], 'reason': selection['reason'],
            'selected': selection['selected'], 'tests': len(population),
            'hosted': sorted(workspace.hosted_tests()),
            'architectures': architectures, 'inputs': workspace.inputs_check() + graph_impact['errors'],
            'graph_impact': graph_impact,
            'changed': sorted(changed | deleted),
            'durations': {'total': round(time.monotonic() - started, 1)}}


def passed(result):
    if result.get('error') or result.get('inputs'):
        return False
    if result['mode'] == 'main':
        return all(not arch['failed'] and not arch.get('error') for arch in result['architectures'].values())
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
        result.setdefault('error', None)
    except GateError as error:
        result['error'] = str(error)
        report = {'summary': 'U unavailable, SCC max unavailable: ' + str(error), 'errors': [str(error)]}
        result['graph_impact'] = report
        print(report['summary'], flush=True)
    write_result(result, output)
    print(json.dumps({key: result.get(key) for key in ['selection', 'error', 'verdict', 'inputs']}))
    return 0 if passed(result) else 1


if __name__ == '__main__':
    sys.exit(main())
