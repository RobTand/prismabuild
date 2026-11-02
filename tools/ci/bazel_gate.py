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
import signal
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.request
import zipfile

ROOT = Path(__file__).resolve().parents[2]
SCHEMA = 'prismabuild.bazel-gate.v1'
RESULT = 'gate-result.json'

BAZEL_DIFF_URL = 'https://github.com/Tinder/bazel-diff/releases/download/v70.0.2/bazel-diff-rust-linux-amd64'
BAZEL_DIFF_SHA256 = '44226a1d1d5239abc8bc6158de1f886fcbbf58e44479ec178df8c01e7e32102e'

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


def diff_commands(binary, root, bazel, base_hashes, head_hashes, impacted):
    """bazel-diff v70.0.2 command lines: a hash generator, then the impacted query."""
    def generate(output):
        return [str(binary), 'generate-hashes', '--workspacePath', str(root),
                '--bazelPath', str(bazel), '--excludeExternalTargets', '--includeTargetType',
                '--depEdgesFile', str(Path(output).with_name(Path(output).stem + '-deps.json')),
                str(output)]
    return generate, [str(binary), 'get-impacted-targets',
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


class BepStream:
    """Decode complete JSON lines only. Keep partial UTF-8 and JSON between reads."""

    def __init__(self):
        self.pending = b''

    def feed(self, data, *, final=False):
        self.pending += data
        while b'\n' in self.pending:
            line, self.pending = self.pending.split(b'\n', 1)
            if line.strip():
                yield json.loads(line)
        if final and self.pending.strip():
            # SIGINT may leave the last event incomplete. Complete earlier events
            # remain evidence; selected targets without a result still fail.
            try:
                event = json.loads(self.pending)
            except (ValueError, UnicodeDecodeError):
                return
            self.pending = b''
            yield event


def update_bep(run, event):
    identity = event.get('id', {})
    if 'started' in identity:
        run['invocation'] = event.get('started', {}).get('uuid')
    attempt = identity.get('testResult')
    if attempt:
        # A failed attempt remains visible if cancellation prevents its summary.
        label = canonical_label(attempt['label'])
        status = event.get('testResult', {}).get('status', 'NO_STATUS')
        if status != 'PASSED':
            run['statuses'][label] = status
    summary = identity.get('testSummary')
    if summary:
        run['statuses'][canonical_label(summary['label'])] = event.get(
            'testSummary', {}).get('overallStatus', 'NO_STATUS')
    if run['error'] is None:
        run['error'] = next((line for line in event.get('progress', {}).get('stderr', '').splitlines()
                             if ERROR_LINE.match(line)), None)


def bep_run(text):
    """Per-label status, invocation ID and first ERROR, using the streaming decoder."""
    run = {'statuses': {}, 'invocation': None, 'error': None}
    for event in BepStream().feed(text.encode(), final=True):
        update_bep(run, event)
    return run


class FailFast:
    """Failure policy and PR grace. Clock and marker lookup are injectable.

    BEP announces result/action IDs before publishing their completion. These
    include queued work, so draining the snapshot is a conservative bound on
    work in flight, with an unconditional 60-second maximum grace.
    """

    def __init__(self, mode, quarantine, budget_marker, clock):
        self.mode, self.quarantine = mode, quarantine
        self.budget_marker, self.clock = budget_marker, clock
        self.failure, self.deadline = None, None
        self.pending, self.draining = set(), set()
        self.ran = set()

    @staticmethod
    def action_id(identity):
        if 'testResult' in identity or 'actionCompleted' in identity:
            return json.dumps(identity, sort_keys=True)
        return None

    def observe(self, event):
        identity = event.get('id', {})
        completed = self.action_id(identity)
        self.pending.discard(completed)
        self.draining.discard(completed)
        for child in event.get('children', []):
            action = self.action_id(child)
            if action:
                self.pending.add(action)
        attempt = identity.get('testResult')
        if attempt:
            self.ran.add(canonical_label(attempt['label']))
        if self.failure or self.mode in ('main', 'off'):
            return
        reason, label = None, None
        if attempt and event.get('testResult', {}).get('status') == 'FAILED':
            label = canonical_label(attempt['label'])
            if not self.budget_marker(label, event):
                reason = 'FAILED'
        for key, payload in [('targetCompleted', 'completed'), ('actionCompleted', 'action')]:
            if key in identity and not event.get(payload, {}).get('success', False):
                # Bazel's failed TestRunner action is classified by testResult,
                # where TIMEOUT and the budget marker are available.
                if event.get(payload, {}).get('type') != 'TestRunner':
                    label = canonical_label(identity[key].get('label', '(build)'))
                    reason = 'BUILD_FAILURE'
        if event.get('aborted', {}).get('reason') in ('ANALYSIS_FAILURE', 'LOADING_FAILURE'):
            label = next((canonical_label(value['label']) for value in identity.values()
                          if isinstance(value, dict) and 'label' in value), '(build)')
            reason = 'BUILD_FAILURE'
        if reason and label not in self.quarantine:
            self.failure = {'label': label, 'reason': reason}
            self.deadline = self.clock() + 60
            self.draining = self.pending.copy()

    def cancel_due(self):
        return self.failure is not None and (self.mode == 'batch' or
                self.mode == 'pr' and (not self.draining or self.clock() >= self.deadline))


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


_import_rules = module_from('bazel_import_rules_for_gate', ROOT / 'tools/ci/bazel_imports.py')
bazel_imports = _import_rules.bazel_imports
bazel_regressions = _import_rules.bazel_regressions


def source_edges(edges, rows):
    """Attach source inputs and library ownership from Bazel metadata."""
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


def typed_edges(edges, rules, sources):
    """Classify only queried Bazel rules and source files, including extensionless ones."""
    return {kind: {(node, dep) for node, deps in edges.items() if node in rules
                   for dep in deps if dep in targets}
            for kind, targets in [('rule_to_rule', rules), ('rule_to_source', sources)]}


def graph_policy_inputs(before, after, old_edges, new_edges):
    maxima = {}
    for test, files in before['reach'].items():
        package = before['test_packages'][test]
        maxima[package] = max(maxima.get(package, 0), len(files))
    return {'test_packages': after['test_packages'], 'base_package_max_reach': maxima,
            'edge_diff': {direction: {kind: [list(edge) for edge in sorted(left[kind] - right[kind])]
                                      for kind in ('rule_to_rule', 'rule_to_source')}
                          for direction, left, right in [('added', new_edges, old_edges),
                                                         ('removed', old_edges, new_edges)]}}


def graph_metrics(measured):
    """Keep closures, raw edges and source weights in graph artifacts, not the gate record."""
    return {**{key: value for key, value in measured.items()
               if key not in ('reach', 'components', 'edges', 'loc', 'edge_diff',
                              'test_packages', 'base_package_max_reach')},
            'reach_sizes': {test: len(files) for test, files in measured['reach'].items()},
            'cycles': [group for group in measured['components'] if len(group) > 1]}


def graph_baseline(record):
    """Migrate the original flat graph limits while preserving their recorded minima."""
    if not record or record.get('schema') == 'graph-impact.v1':
        return record
    return {'schema': 'graph-impact.v1',
            'figures': {key: record[key] for key in ('u_pct', 'loc_u_pct', 'scc_max') if key in record},
            'tolerances': {'u_pct': 1.0, 'loc_u_pct': 1.0, 'scc_max': 0},
            'reach_budget': record.get('reach_budget'), 'edge_groups': {}}


def refuse_stored_edge_lists(value, path='baseline'):
    """Persist figures and history; edge rules always compare the two live graphs."""
    if isinstance(value, dict):
        for key, child in value.items():
            edge_key = key in ('edges', 'imports') or key.endswith(('_edges', '_imports'))
            edge_rows = isinstance(child, list) and any(
                isinstance(row, str) and (' -> ' in row or ' → ' in row)
                or isinstance(row, list) and len(row) == 2 and all(isinstance(node, str) for node in row)
                for row in child)
            if key in ('test_to_test_imports', 'conftest_source_imports') or (
                    isinstance(child, list) and (edge_key or edge_rows or path.endswith('.edge_groups'))):
                raise GateError(f'baseline refuses stored edge lists: {path}.{key}; use the base-against-head graph diff')
            refuse_stored_edge_lists(child, f'{path}.{key}')
    elif isinstance(value, list):
        for index, child in enumerate(value):
            refuse_stored_edge_lists(child, f'{path}[{index}]')


def graph_baseline_errors(current, previous):
    current, previous = graph_baseline(current), graph_baseline(previous)
    errors = []
    for key in ('u_pct', 'loc_u_pct', 'scc_max', 'reach_budget'):
        value = current.get('reach_budget') if key == 'reach_budget' else current.get('figures', {}).get(key)
        old = previous.get('reach_budget') if key == 'reach_budget' else previous.get('figures', {}).get(key)
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
            errors.append(f'graph baseline needs a finite nonnegative {key}')
        elif old is not None and value > old:
            errors.append(f'graph baseline may only tighten: {key} ({old} -> {value})')
    if current.get('test_to_test', 'strict') != 'strict':
        errors.append('PrismaBuild test_to_test policy must stay strict')
    if current.get('schema') != 'graph-impact.v1':
        errors.append('graph baseline schema must be graph-impact.v1')
    if current.get('tolerances') != {'u_pct': 1.0, 'loc_u_pct': 1.0, 'scc_max': 0}:
        errors.append('graph baseline tolerances changed')
    for group, entries in current.get('edge_groups', {}).items():
        if set(entries) - set(previous.get('edge_groups', {}).get(group, ())):
            errors.append(f'graph baseline {group} exceptions may only shrink')
    return errors


def blame_target(error, head, affected):
    """Select a reported label; bazel-diff alone explains its dependency paths."""
    labels = {word.rstrip(')') for word in error.replace(',', ' ').replace(';', ' ').split()
              if word.startswith('//')}
    tests = set(head['reach'])
    nodes = {node for edge in head['edges'] for node in edge}
    rules = nodes - head['loc'].keys()
    for candidates in (labels & tests, set(affected), labels & rules, labels & nodes, tests):
        if candidates:
            return min(candidates)
    return '//tools:ci/bazel_gate'


def child_environment(command):
    """Bazel owns runner auth. Non-Bazel children cannot inherit credentials."""
    return {name: value for name, value in os.environ.items()
            if Path(str(command[0])).name in ('bazel', 'bazelisk')
            or not (name.endswith(('_API_KEY', '_TOKEN')) or name == 'GITHUB_TOKEN')}


class Workspace:
    """Git and Bazel on the runner. Every failure raises GateError."""

    def __init__(self, root=ROOT, bazel='bazel', *, clock=time.monotonic, process_factory=subprocess.Popen):
        self.root, self.bazel = root, bazel
        self.clock, self.process_factory = clock, process_factory
        self.graph_revisions = None

    def run(self, *command, capture=True, check=True):
        print('$ ' + shlex.join(map(str, command)), flush=True)
        result = subprocess.run(list(map(str, command)), cwd=self.root, text=True,
                                env=child_environment(command),
                                stdout=subprocess.PIPE if capture else None)
        if check and result.returncode:
            raise GateError(f'{shlex.join(map(str, command))} exited {result.returncode}')
        return result.stdout if capture else result.returncode

    def ensure_commit(self, sha):
        if not sha:
            raise GateError('empty base or head; the queue-aware refs were unavailable')
        present = subprocess.run(['git', 'cat-file', '-e', f'{sha}^{{commit}}'], cwd=self.root,
                                 stderr=subprocess.DEVNULL, env=child_environment(['git']))
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
        binary = self.diff_tool()
        self.graph_directory = tempfile.TemporaryDirectory(prefix='graph-impact-')
        directory = Path(self.graph_directory.name)
        self.hashes = {name: directory / f'{name}.json' for name in ('base', 'head')}
        self.deps = {name: directory / f'{name}-deps.json' for name in ('base', 'head')}
        self.impacted_path = directory / 'impacted.txt'
        generate, self.impact_command = diff_commands(binary, self.root, self.bazel,
                                                       self.hashes['base'], self.hashes['head'],
                                                       self.impacted_path)
        self.snapshots, self.graph_baselines, self.graph_allowlists, self.graph_imports = {}, {}, {}, {}
        self.graph_owners, self.graph_contents, self.graph_edge_types = {}, {}, {}
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
                    'deps': sorted(edges[test]),
                } for test in tests}
                self.deps[name].write_text(json.dumps({node: sorted(deps) for node, deps in edges.items()}))
                metadata = {'tests': sorted(tests), 'loc': loc, 'contents': contents,
                            'test_packages': {test: test.rsplit(':', 1)[0] for test in tests}}
                (directory / f'{name}-metadata.json').write_text(json.dumps(metadata))
                packages = {test: test.rsplit(':', 1)[0] for test in tests}
                self.snapshots[name] = impact.measure(
                    *impact.read_deps(self.deps[name], tests, loc, loc=loc), test_packages=packages)
                self.graph_contents[name] = contents
                rules = {canonical_label(row['rule']['name']) for row in rule_rows if 'rule' in row}
                sources = set(loc) | {canonical_label(label) for label, value in hashes.items()
                                      if isinstance(value, str) and value.startswith('SourceFile#')}
                self.graph_edge_types[name] = typed_edges(edges, rules, sources)
                self.graph_owners[name] = owners
                self.graph_imports[name] = bazel_imports(edges, owners)
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
        self.graph_inputs = graph_policy_inputs(self.snapshots['base'], self.snapshots['head'],
                                                self.graph_edge_types['base'], self.graph_edge_types['head'])
        self.snapshots['head'].update(self.graph_inputs)
        self.graph_revisions = (base, head)
        self.graph_impact = impact

    def streamed_query(self, expression):
        return [json.loads(line) for line in self.run(self.bazel, 'query', expression,
                '--output=streamed_jsonproto', '--keep_going=false').splitlines() if line.strip()]

    def bazel_diff(self, base, head):
        self.ensure_graphs(base, head)
        self.run(*self.impact_command)
        return {canonical_label(line) for line in self.impacted_path.read_text().split() if line}

    def diff_tool(self):
        """One checksum-pinned native tool for generation, selection and blame."""
        binary = Path(tempfile.gettempdir(), 'bazel-diff-v70.0.2-linux-amd64')
        if not binary.exists() or bazel_diff_digest(binary.read_bytes()) != BAZEL_DIFF_SHA256:
            with urllib.request.urlopen(BAZEL_DIFF_URL, timeout=120) as response:
                data = response.read()
            if bazel_diff_digest(data) != BAZEL_DIFF_SHA256:
                raise GateError('bazel-diff binary checksum mismatch')
            binary.write_bytes(data)
        binary.chmod(0o755)
        return str(binary)

    def explain(self, target):
        try:
            output = self.run(self.diff_tool(), 'explain',
                              '-sh', str(self.hashes['base']), '-fh', str(self.hashes['head']),
                              '-d', str(self.deps['head']), '--target', target, '--format', 'mermaid')
        except (GateError, OSError) as error:
            return f'bazel-diff blame unavailable: {error}'
        return '```mermaid\n' + output.strip() + '\n```'

    def graph_check(self, base, head, *, record_gains=False):
        try:
            self.ensure_graphs(base, head)
            impact = self.graph_impact
            before, after = self.snapshots['base'], self.snapshots['head']
            previous = graph_baseline(self.graph_baselines['base'].get('graph_impact', {}))
            baseline = graph_baseline(self.graph_baselines['head'].get('graph_impact', {}))
            policy_errors = graph_baseline_errors(baseline, previous) if baseline else []
            policy_errors += impact.allowlist_errors(self.graph_allowlists['head'], self.graph_allowlists['base'])
            live = after
            if not baseline:
                baseline = impact.baseline_record(before)
                policy_errors.append('graph impact baseline missing: record graph_impact in import_fanout_baseline.json')
            if graph_baseline_errors(baseline, {}):
                baseline = impact.baseline_record(before)
            changed = {test for test, fingerprint in self.graph_contents['head'].items()
                       if fingerprint != self.graph_contents['base'].get(test)}
            errors = impact.compare(before, after, baseline, changed_tests=changed,
                                    allowlist=self.graph_allowlists['head'], record_gains=record_gains,
                                    **self.graph_inputs)
            affected = sorted(test for test, files in after['reach'].items()
                              if files != before['reach'].get(test) or test in changed)
            diagrams = {}
            for index, error in enumerate(errors):
                target = blame_target(error, after, affected)
                if target not in diagrams:
                    diagrams[target] = self.explain(target)
                errors[index] = error + '\n' + diagrams[target]
            if policy_errors:
                target = next(iter(sorted(after['reach'])), '//tools:ci/bazel_gate')
                blame = self.explain(target)
                errors.extend(error + '\n' + blame for error in policy_errors)
            for error in bazel_regressions(self.graph_imports['head'], self.graph_imports['base']):
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
                    'base': graph_metrics(before), 'head': graph_metrics(live),
                    'edge_diff': self.graph_inputs['edge_diff'],
                    'base_package_max_reach': self.graph_inputs['base_package_max_reach'],
                    'fanout_growth': {source: count - before['dependents'].get(source, 0)
                                      for source, count in after['dependents'].items()
                                      if count > before['dependents'].get(source, 0)}, 'errors': errors}
        except GateError as error:
            return {'summary': 'U unavailable, SCC max unavailable: ' + str(error), 'errors': [str(error)]}

    def test(self, targets, config, metadata, log_name, extra=(), *, fail_fast='off', quarantine=None):
        """Watch a private BEP stream while the Bazel client owns remote work."""
        if not targets:
            return bep_run('')
        if quarantine is None:
            quarantine = set()
            if fail_fast != 'off':
                try:
                    loader = module_from('quarantine_for_gate', self.root / 'tools/ci/bazel_quarantine.py')
                    quarantine = set(loader.read_quarantine(self.root / 'tools/bazel/nonhermetic.bzl'))
                except (OSError, ValueError, SyntaxError, TypeError) as error:
                    raise GateError('cannot read Bazel quarantine table') from error
        with tempfile.TemporaryDirectory(prefix='bazel-gate-') as directory:
            patterns, path = Path(directory) / 'targets.txt', Path(directory) / 'bep.json'
            patterns.write_text('\n'.join(targets) + '\n')
            command = [self.bazel, 'test', f'--config={config}', '--config=ci', '--keep_going',
                       f'--target_pattern_file={patterns}', f'--build_event_json_file={path}',
                       *diagnostic_logs(str(Path(directory) / log_name)), *extra,
                       *[f'--build_metadata={key}={value}' for key, value in metadata.items()]]
            print('$ ' + shlex.join(command), flush=True)
            stream, run, offset = BepStream(), bep_run(''), 0
            watch = FailFast(fail_fast, quarantine, self.budget_marker, self.clock)
            process = self.process_factory(command, cwd=self.root)
            aborted = None
            try:
                while True:
                    ended = process.poll() is not None
                    if path.exists():
                        with path.open('rb') as handle:
                            handle.seek(offset)
                            chunk = handle.read()
                            offset = handle.tell()
                    else:
                        chunk = b''
                    for event in stream.feed(chunk, final=ended):
                        update_bep(run, event)
                        watch.observe(event)
                        if not ended and aborted is None and watch.cancel_due():
                            process.send_signal(signal.SIGINT)
                            aborted = dict(watch.failure)
                    if ended:
                        break
                    if aborted is None and watch.cancel_due():
                        process.send_signal(signal.SIGINT)
                        aborted = dict(watch.failure)
                    try:
                        process.wait(timeout=0.1)
                    except subprocess.TimeoutExpired:
                        pass
            finally:
                if process.poll() is None:
                    process.send_signal(signal.SIGINT)
                    process.wait()
            if aborted:
                run.update(aborted_early=aborted, not_run=sorted(set(targets) - watch.ran))
            return run

    def budget_marker(self, label, event):
        """Inspect this result's shard, not other unfinished shards of the target.

        ci's remote_download_regex materializes test.outputs before testResult.
        A budget-only marker is distinct from an assertion failure (#2637).
        """
        package, _, name = label.removeprefix('//').partition(':')
        logs = self.root / 'bazel-testlogs' / package / name
        shard = event['id']['testResult'].get('shard', 0)
        outputs = list(logs.glob(f'shard_{shard + 1}_of_*/test.outputs')) or [logs / 'test.outputs']
        marker = module_from('gate_run_for_bazel_gate', self.root / 'tools/ci/gate_run.py').BUDGET_MARKER
        return any(has_output(path, marker) for path in outputs)

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
            started.append((command, subprocess.Popen(list(map(str, command)), cwd=self.root,
                                                       env=child_environment(command))))
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


def gate_pr(workspace, base, head, metadata, *, fail_fast='pr'):
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
    run = workspace.test(decision['selected'], CONFIGS['amd64'], metadata, 'pr', fail_fast=fail_fast)
    statuses, reruns = run['statuses'], {}
    if not run.get('aborted_early'):
        statuses, reruns = rerun_budget_only(workspace, decision['selected'], statuses, CONFIGS['amd64'],
                                             metadata, 'pr')
    tested_seconds = time.monotonic() - started
    verdict, failed = verdict_of(decision['selected'], statuses)
    inputs = workspace.inputs_check(checks) + workspace.budget_check(base) + graph_impact['errors']
    if run.get('aborted_early'):
        verdict = 'fail'
    elif not workspace.build(decision['build'], CONFIGS['amd64'], metadata, 'pr'):
        inputs.append('bazel build of impacted targets failed: ' + ' '.join(decision['build']))
    return {**decision, **{key: run[key] for key in ('aborted_early', 'not_run') if key in run},
            'verdict': verdict, 'failed': failed, 'inputs': inputs, 'budget_reruns': reruns,
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
    if result.get('error') or result.get('inputs') or result.get('aborted_early'):
        return False
    if result['mode'] == 'main':
        return all(not arch['failed'] and not arch.get('error') for arch in result['architectures'].values())
    return result['verdict'] != 'fail'


def main(argv=None, workspace=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=['pr', 'main', 'graph'])
    parser.add_argument('--write-baseline', action='store_true', help='graph mode only: record gains')
    parser.add_argument('--base', required=True)
    parser.add_argument('--head', required=True)
    parser.add_argument('--branch', default='')
    parser.add_argument('--fail-fast', choices=['batch', 'pr', 'off'], default='pr')
    args = parser.parse_args(argv)
    if args.write_baseline and args.mode != 'graph':
        parser.error('--write-baseline requires graph mode')
    output = Path(os.environ.get('BUILDBUDDY_ARTIFACTS_DIRECTORY', ROOT / 'gate-artifacts'))
    metadata = {'COMMIT_SHA': args.head, 'BRANCH_NAME': args.branch}
    result = {'schema': SCHEMA, 'mode': args.mode, 'base': args.base, 'head': args.head}
    try:
        workspace = workspace or Workspace()
        if args.mode == 'graph':
            workspace.ensure_commit(args.base)
            workspace.ensure_commit(args.head)
            report = workspace.graph_check(args.base, args.head, record_gains=args.write_baseline)
            print(report['summary'], flush=True)
            result.update(graph_impact=report, inputs=report['errors'],
                          verdict='fail' if report['errors'] else 'pass')
            if args.write_baseline and not report['errors']:
                baseline = dict(workspace.graph_baselines['head'])
                refuse_stored_edge_lists(baseline)
                baseline['graph_impact'] = workspace.graph_impact.baseline_record(
                    workspace.snapshots['head'], graph_baseline(baseline.get('graph_impact', {})))
                baseline['graph_impact'].pop('edge_groups', None)
                refuse_stored_edge_lists(baseline)
                path = workspace.root / 'tests/fixtures/import_fanout_baseline.json'
                path.write_text(json.dumps(baseline, indent=2) + '\n')
                output.mkdir(parents=True, exist_ok=True)
                (output / path.name).write_bytes(path.read_bytes())
        else:
            if args.mode == 'pr':
                result.update(gate_pr(workspace, args.base, args.head, metadata, fail_fast=args.fail_fast))
            else:
                result.update(gate_main(workspace, args.base, args.head, metadata))
        result.setdefault('error', None)
    except GateError as error:
        result['error'] = str(error)
        report = {'summary': 'U unavailable, SCC max unavailable: ' + str(error), 'errors': [str(error)]}
        result['graph_impact'] = report
        print(report['summary'], flush=True)
    write_result(result, output)
    print(json.dumps({key: result.get(key) for key in ['selection', 'error', 'verdict', 'inputs']}))
    return 0 if passed(result) else 1


def scan_output():
    """CI uploads only scanned summaries; bbrun wraps its own final scan."""
    if os.environ.get('BBRUN_FINAL_SCAN') == '1':
        return
    output = Path(os.environ.get('BUILDBUDDY_ARTIFACTS_DIRECTORY', ROOT / 'gate-artifacts'))
    output.mkdir(parents=True, exist_ok=True)
    try:
        scanner = module_from('secret_scan_for_gate', ROOT / 'tools/ci/check_no_secret_leak.py')
        code = scanner.main(['--require', 'BUILDBUDDY_API_KEY', '--env', 'MERGIFY_TOKEN',
                             '--env', 'GITHUB_TOKEN', str(output)])
    except BaseException:
        shutil.rmtree(output)
        output.mkdir(parents=True)
        raise
    if code:
        shutil.rmtree(output)
        output.mkdir(parents=True)
        raise SystemExit(75)


if __name__ == '__main__':
    try:
        code = main()
    finally:
        scan_output()
    sys.exit(code)
