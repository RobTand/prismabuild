#!/usr/bin/env python3
"""Reproducible TIA evaluation. Execute all commands inside PrismaBuild actions.

The campaign producer emits one independent row per mutation. It never places
workers or dispatches tests itself. Map runs use pbtest's partitioner.
"""
from __future__ import annotations

import argparse
import ast
import base64
import copy
import gzip
import json
import os
import shutil
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import xml.etree.ElementTree as ET

sys.path.insert(0, str(Path(__file__).resolve().parent))

from tia_select import load_tia_module, plan, prepare_selection
from tia_map import read_impact_map, write_impact_map
from tia_diff import analyze_changes, compare_functions, raw_sha256, source

try:
    from .tia_bytes import raw_sha256, compact_ascii_json_bytes, sorted_json
except ImportError:
    from tia_bytes import raw_sha256, compact_ascii_json_bytes, sorted_json


def extract(reports: list[Path], output: Path):
    """Extract raw records only from complete pbtest shard verdicts."""
    with tempfile.TemporaryDirectory(dir=Path.cwd()) as scratch:
        paths = []
        for report in reports:
            for row in json.loads(report.read_text()):
                if not row['verdict']['green'] or not row.get('receipt_path'):
                    raise ValueError(f"map shard not green: {row['action_key']}")
                for line in row['output'].splitlines():
                    if 'tia-record: ' in line:
                        records = json.loads(gzip.decompress(base64.b64decode(line.split('tia-record: ', 1)[1])))
                        for record in records:
                            path = Path(scratch) / f'{len(paths)}.json'
                            path.write_text(json.dumps(record))
                            paths.append(path)
        result = write_impact_map(paths, output, unmapped=json.loads(os.environ.get('PB_TIA_UNMAPPED', '{}')),
                           duration_updates=json.loads(os.environ.get('PB_TIA_EXTRA_DURATIONS', '{}')),
                           baseline_failures=json.loads(os.environ.get('PB_TIA_BASELINE_FAILURES', '[]')))
    print(json.dumps({'tests': len(result['tests']), 'functions': len(result['functions']),
                      'bytes': output.stat().st_size, 'source_commit': result['source_commit']}))


def extract_cas(inputs: list[dict], output: Path):
    with tempfile.TemporaryDirectory(dir=Path.cwd()) as temporary:
        paths = []
        for entry in inputs:
            payload = Path(entry['blob_path']).read_bytes()
            if raw_sha256(payload) != entry['blob_sha256']:
                raise ValueError('recorded result or terminal-log hash mismatch')
            found = False
            for line in payload.decode().splitlines():
                if 'tia-record: ' in line:
                    found = True
                    records = json.loads(gzip.decompress(base64.b64decode(line.split('tia-record: ', 1)[1])))
                    for record in records:
                        path = Path(temporary) / f'{len(paths)}.json'
                        path.write_text(json.dumps(record))
                        paths.append(path)
            if not found:
                raise ValueError(f"missing recorder output: {entry['action_key']}")
        result = write_impact_map(paths, output, unmapped=json.loads(os.environ.get('PB_TIA_UNMAPPED', '{}')),
                           duration_updates=json.loads(os.environ.get('PB_TIA_EXTRA_DURATIONS', '{}')),
                           baseline_failures=json.loads(os.environ.get('PB_TIA_BASELINE_FAILURES', '[]')))
    print('tia-map: ' + base64.b64encode(output.read_bytes()).decode(), flush=True)
    print(json.dumps({'tests': len(result['tests']), 'functions': len(result['functions']),
                      'bytes': output.stat().st_size, 'source_commit': result['source_commit']}), flush=True)


def replay_impact(root: Path, gate_dir: Path, mapping: dict, history: dict) -> dict:
    gate_replay = load_tia_module(gate_dir / 'gate_replay.py', 'tia_gate_replay')
    # Reuse its population, static rule, estimates and nearest-rank statistics.
    excluded = set(mapping.get('unmapped', {}))
    context = prepare_selection(root, gate_dir, exclude=excluded, duration_updates=mapping.get('duration_updates'))
    # The existing replay has no input-injection arguments. Adapt its reads for this call,
    # preserving its selector, estimates, replay loop and percentile calculation unchanged.
    original_index = gate_replay.select.dependency_index
    original_json = gate_replay.select.read_json
    gate_replay.select.dependency_index = lambda directory: {name: value for name, value in original_index(directory).items() if name not in excluded}
    gate_replay.select.read_json = lambda path: context['durations'] if path.name == 'gate_durations.json' else original_json(gate_dir / path.name if path.name == 'gate_quarantine.json' else path)
    try:
        static = gate_replay.replay(root, history)
    finally:
        gate_replay.select.dependency_index = original_index
        gate_replay.select.read_json = original_json
    rows = []
    for pull in history['pulls']:
        merge = pull['merge_commit_sha']
        parent = subprocess.check_output(['git', 'rev-parse', merge + '^'], cwd=root, text=True).strip()
        changes = analyze_changes(root, parent, merge, merge_base=False)
        selected = plan(root, gate_dir, mapping, changes, context=context)
        rows.append({'number': pull['number'], 'selected_files': len(selected['selected']),
                     'wall_seconds': selected['estimate']['wall_seconds'],
                     'serial_seconds': selected['estimate']['serial_seconds'],
                     'fallbacks': selected['fallbacks']})
    return {'schema': 'prismabuild.tia_replay.v1', 'static': static, 'evaluation_excluded': mapping.get('unmapped', {}),
            'tia': {'summary': gate_replay.summarize(rows), 'pulls': rows},
            'method': 'same current population and gate estimates; AST diffs of merge parent to merge; '
                      'historical source hashes must match the current map, otherwise static fallback'}


def mutations(root: Path, mapping: dict, *, paths: set[str] | None = None):
    """One deterministic operator flip per function, including both requested trees."""
    known = set(mapping['functions'])
    for path in sorted(mapping['files']):
        if paths is not None and path not in paths:
            continue
        if not path.startswith(('src/prismabuild/', 'tools/fleet/')):
            continue
        original = (root / path).read_text()
        if raw_sha256(original.encode()) != mapping['files'][path]:
            continue
        tree = ast.parse(original)
        def visit(node, prefix=''):
            for child in ast.iter_child_nodes(node):
                if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    name = prefix + child.name
                    if path + '::' + name in known:
                        # Do not mutate a nested function under its parent's identity.
                        pending = list(child.body)
                        while pending:
                            candidate = pending.pop(0)
                            if isinstance(candidate, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                                continue
                            replacement = copy.deepcopy(candidate)
                            kind = None
                            if isinstance(candidate, ast.Compare) and candidate.ops:
                                flips = {ast.Eq: ast.NotEq, ast.NotEq: ast.Eq, ast.Lt: ast.GtE,
                                         ast.LtE: ast.Gt, ast.Gt: ast.LtE, ast.GtE: ast.Lt,
                                         ast.Is: ast.IsNot, ast.IsNot: ast.Is, ast.In: ast.NotIn, ast.NotIn: ast.In}
                                if type(candidate.ops[0]) in flips:
                                    replacement.ops[0] = flips[type(candidate.ops[0])]()
                                    kind = 'comparison'
                            elif isinstance(candidate, ast.BinOp) and isinstance(candidate.op, (ast.Add, ast.Sub, ast.Mult)):
                                replacement.op = ast.Sub() if isinstance(candidate.op, ast.Add) else ast.Add()
                                kind = 'arithmetic'
                            elif isinstance(candidate, ast.If):
                                replacement.test = ast.UnaryOp(op=ast.Not(), operand=replacement.test)
                                kind = 'condition'
                            elif isinstance(candidate, ast.Return) and candidate.value is not None:
                                replacement.value = ast.UnaryOp(op=ast.Not(), operand=replacement.value)
                                kind = 'return'
                            if kind:
                                lines = original.encode().splitlines(keepends=True)
                                start = sum(map(len, lines[:candidate.lineno - 1])) + candidate.col_offset
                                end = sum(map(len, lines[:candidate.end_lineno - 1])) + candidate.end_col_offset
                                # unparse emits multiline statements with no parent indentation.
                                changed = ast.unparse(replacement).replace('\n', '\n' + ' ' * candidate.col_offset)
                                mutant = (original.encode()[:start] + changed.encode() + original.encode()[end:]).decode()
                                try:
                                    ast.parse(mutant)
                                except SyntaxError:
                                    continue
                                yield {'path': path, 'function': name, 'kind': kind, 'line': candidate.lineno,
                                       'before': raw_sha256(original.encode()), 'start': start, 'end': end,
                                       'replacement': changed, 'diff': compare_functions(original, mutant)}
                                break
                            pending.extend(ast.iter_child_nodes(candidate))
                    yield from visit(child, name + '.<locals>.')
                elif isinstance(child, ast.ClassDef):
                    yield from visit(child, prefix + child.name + '.')
                else:
                    yield from visit(child, prefix)
        yield from visit(tree)


def campaign(root: Path, gate_dir: Path, mapping: dict, map_path: Path, output: Path, count: int):
    gate = load_tia_module(gate_dir / 'gate_select.py', 'tia_campaign_gate')
    durations = prepare_selection(root, gate_dir, duration_updates=mapping.get('duration_updates'))['durations']
    quarantine = gate.read_json(gate_dir / 'gate_quarantine.json')
    index = gate.dependency_index(root)
    graph = gate.helper_importers(index, [])
    costs = {}
    for path in sorted(mapping['files']):
        if path.startswith(('src/prismabuild/', 'tools/fleet/')):
            selected = gate.create_plan(root, [path], durations=durations,
                                        quarantine=quarantine, index=index, helper_graph=graph)
            cost = selected['estimate']['serial_seconds']
            if cost is not None and cost <= 1200:
                costs[path] = cost
    candidates = [(costs[mutation['path']], mutation['path'], mutation['function'], mutation)
                  for mutation in mutations(root, mapping, paths=set(costs))]
    candidates.sort(key=lambda item: item[:3])
    groups = [[row[-1] for row in candidates if row[1].startswith(prefix)]
              for prefix in ('src/prismabuild/', 'tools/fleet/')]
    chosen = []
    while len(chosen) < count and any(groups):
        for group in groups:
            if group and len(chosen) < count:
                chosen.append(group.pop(0))
    if len(chosen) < count:
        raise ValueError(f'only {len(chosen)} eligible mutations, need {count}')
    rows = []
    for number, mutation in enumerate(chosen):
        ids = [i for i, name in enumerate(mapping['functions']) if name.startswith(mutation['path'] + '::')]
        remap = {old: new for new, old in enumerate(ids)}
        tests = {}
        for node, values in mapping['tests'].items():
            relevant = {remap[i] for i in values if i in remap}
            key = node if relevant else node.split('::', 1)[0] + '::recorded'
            tests.setdefault(key, set()).update(relevant)
        subset = dict(mapping, functions=[mapping['functions'][i] for i in ids],
                      tests={node: sorted(values) for node, values in tests.items()},
                      files={mutation['path']: mapping['files'][mutation['path']]},
                      unattributed=[name for name in mapping['unattributed'] if name.startswith(mutation['path'] + '::')])
        packed_map = base64.b64encode(gzip.compress(json.dumps(subset).encode(), mtime=0)).decode()
        rows.append({'cwd': str(root), 'argv': ['/home/rob/venvs/pb-cpu/bin/python',
                     Path(__file__).resolve().relative_to(root).as_posix(), 'mutation', '--mutation', json.dumps(mutation),
                     '--gate-dir', str(gate_dir.relative_to(root))],
                     'demand': {'cpu': 1, 'mem_gb': 4}, 'priority': -10, 'anywhere': True, 'timeout_s': 6000,
                     'env': {'OMP_NUM_THREADS': '1', 'MKL_NUM_THREADS': '1', 'OPENBLAS_NUM_THREADS': '1',
                             'PB_TIA_MAP': packed_map, 'PB_TIA_MUTATION': str(number)}})
    output.write_text(sorted_json(rows) + '\n')
    print('tia-campaign: ' + base64.b64encode(output.read_bytes()).decode(), flush=True)
    print(json.dumps({'mutations': len(rows), 'src': sum(m['path'].startswith('src/') for m in chosen),
                      'fleet': sum(m['path'].startswith('tools/') for m in chosen)}))


def pytest_run(root: Path, files: list[str], deselect: list[str], directory: Path, label: str) -> dict:
    xml = directory / (label + '.xml')
    env = dict(os.environ, PYTHONPATH=str(root / 'src') + os.pathsep + str(root),
               PYTEST_ADDOPTS='', PYTHONDONTWRITEBYTECODE='1')
    for key in list(env):
        if key.startswith('PB_TIA_') or key == 'PYTEST_PLUGINS':
            env.pop(key, None)
    command = [sys.executable, '-m', 'pytest', '-q', '-p', 'no:cacheprovider',
               '--junitxml=' + str(xml),
               *['--deselect=' + node for node in deselect], *files]
    start = time.monotonic()
    complete = True
    scratch_root = os.environ.get('PB_TIA_SCRATCH_ROOT', '/tmp')
    if not Path(scratch_root).is_absolute():
        raise ValueError('PB_TIA_SCRATCH_ROOT must be absolute')
    with tempfile.TemporaryDirectory(prefix='pbm.', dir=scratch_root) as scratch:
        temporary = Path(scratch) / 'tmp'
        temporary.mkdir()
        env['TMPDIR'] = str(temporary)
        command.insert(7, '--basetemp=' + str(Path(scratch) / 't'))
        try:
            run = subprocess.run(command, cwd=root, env=env, capture_output=True, text=True, timeout=1800)
            code, output = run.returncode, run.stdout + run.stderr
        except subprocess.TimeoutExpired as error:
            code, complete = 124, False
            output = ''.join(value.decode(errors='replace') if isinstance(value, bytes) else value
                             for value in (error.stdout, error.stderr) if value)
        (directory / (label + '.log')).write_text(output)
    failures, errors, tests, skips = [], [], 0, 0
    if xml.exists():
        for case in ET.parse(xml).iter('testcase'):
            node = case.get('classname', '') + '::' + case.get('name', '')
            tests += 1
            if case.find('failure') is not None:
                failures.append(node)
            if case.find('error') is not None:
                errors.append(node)
            skips += case.find('skipped') is not None
    if code:
        print('tia-arm-log: ' + base64.b64encode(gzip.compress(output.encode(), mtime=0)).decode(), flush=True)
    result = {'returncode': code, 'complete': complete and code in (0, 1, 2) and xml.exists(),
              'failures': sorted(failures), 'errors': sorted(errors), 'tests': tests, 'skipped': skips,
              'files': len({target.split('::', 1)[0] for target in files}), 'targets': len(files),
              'elapsed_s': time.monotonic() - start, 'tail': output[-4000:] if code else ''}
    print('tia-arm: ' + sorted_json({'label': label, **{key: result[key] for key in
          ('returncode', 'complete', 'tests', 'elapsed_s')}}), flush=True)
    return result


def run_mutation(root: Path, gate_dir: Path, mutation: dict) -> dict:
    mapping = json.loads(gzip.decompress(base64.b64decode(os.environ['PB_TIA_MAP'])))
    path = mutation['path']
    original = (root / path).read_bytes()
    if raw_sha256(original) != mutation['before']:
        raise ValueError('mutation source hash mismatch')
    diff = {'python': {path: mutation['diff']}, 'non_python': [], 'base_hashes': {path: mutation['before']}}
    selected = plan(root, gate_dir, mapping, diff)
    gate = load_tia_module(gate_dir / 'gate_select.py', 'tia_mutation_gate')
    static = gate.create_plan(root, [path], durations=gate.read_json(gate_dir / 'gate_durations.json'),
                              quarantine=gate.read_json(gate_dir / 'gate_quarantine.json'))
    excluded = set(mapping.get('unmapped', {}))
    tia_files = [row['path'] for row in selected['selected'] if row['path'] not in excluded]
    static_files = [row['path'] for row in static['selected'] if row['path'] not in excluded]
    tia_targets = [target for target in selected['targets'] if target.split('::', 1)[0] not in excluded]
    with tempfile.TemporaryDirectory(dir=Path.cwd()) as temporary:
        directory = Path(temporary)
        baseline = pytest_run(root, sorted(set(tia_files + static_files)), static['deselect'], directory, 'baseline')
        baseline_bad = set(baseline['failures'] + baseline['errors'])
        known_baseline = set()
        for entry in mapping.get('baseline_failures', []):
            file, name = entry['nodeid'].split('::', 1)
            known_baseline.add(file.removesuffix('.py').replace('/', '.') + '::' + name)
        baseline_valid = (baseline['complete'] and baseline['returncode'] in (0, 1)
                          and baseline_bad <= known_baseline)
        if not baseline_valid:
            skipped = {'returncode': None, 'complete': False, 'failures': [], 'errors': [],
                       'tests': 0, 'skipped': 0, 'files': 0, 'reason': 'invalid baseline'}
            return {'mutation': mutation, 'baseline': baseline, 'tia': skipped, 'static': skipped,
                    'tia_killed': False, 'static_killed': False, 'valid': False,
                    'known_baseline_failures': sorted(baseline_bad & known_baseline),
                    'tia_plan': selected, 'evaluation_excluded': mapping.get('unmapped', {})}
        changed = original[:mutation['start']] + mutation['replacement'].encode() + original[mutation['end']:]
        try:
            (root / path).write_bytes(changed)
            # Avoid reading a same-size, same-second bytecode cache after mutating a comparison.
            for cache in (root / path).parent.glob('__pycache__/' + Path(path).stem + '.*.pyc'):
                cache.unlink()
            tia = pytest_run(root, tia_targets, static['deselect'], directory, 'tia')
            full = pytest_run(root, static_files, static['deselect'], directory, 'static')
        finally:
            (root / path).write_bytes(original)
    def killed(result):
        return result['complete'] and bool(set(result['failures'] + result['errors']) - baseline_bad)
    return {'mutation': mutation, 'baseline': baseline, 'tia': tia, 'static': full,
            'tia_killed': killed(tia), 'static_killed': killed(full),
            'valid': baseline_valid and tia['complete'] and full['complete'],
            'known_baseline_failures': sorted(baseline_bad & known_baseline),
            'tia_plan': selected, 'evaluation_excluded': mapping.get('unmapped', {})}


def overhead(root: Path, gate_dir: Path) -> dict:
    gate = load_tia_module(gate_dir / 'gate_select.py', 'tia_overhead_gate')
    fast, _ = gate.tier_declarations(root)
    arms = []
    with tempfile.TemporaryDirectory(dir=Path.cwd()) as temporary:
        for repeat in range(5):
            for recorded in (False, True):
                output = Path(temporary) / f'{repeat}-{int(recorded)}'
                env = dict(os.environ, PYTHONPATH=os.pathsep.join([str(Path(__file__).resolve().parent), str(root / 'src'), str(root)]),
                           PYTEST_ADDOPTS='', PYTEST_PLUGINS='')
                for key in list(env):
                    if key.startswith('PB_TIA_'):
                        env.pop(key)
                command = [sys.executable, '-m', 'pytest', '-q', '-p', 'no:cacheprovider', *sorted(fast)]
                if recorded:
                    env.update(PB_TIA_ROOT=str(root), PB_TIA_OUTPUT=str(output),
                               PB_TIA_COMMIT=subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=root, text=True).strip())
                    command[3:3] = ['-p', 'tia_record']
                load_before = os.getloadavg()
                start = time.monotonic()
                with tempfile.TemporaryDirectory(prefix='pbo.', dir=os.environ.get('PB_TIA_SCRATCH_ROOT', '/tmp')) as scratch:
                    env['TMPDIR'] = scratch
                    command.insert(6, '--basetemp=' + str(Path(scratch) / 't'))
                    run = subprocess.run(command, env=env, cwd=root, capture_output=True, text=True, timeout=1800)
                elapsed = time.monotonic() - start
                arms.append({'repeat': repeat, 'recorded': recorded, 'elapsed_s': elapsed,
                             'returncode': run.returncode, 'load_before': load_before, 'load_after': os.getloadavg(),
                             'summary': run.stdout.splitlines()[-1:]})
                if run.returncode:
                    raise RuntimeError(run.stdout + run.stderr)
    return {'schema': 'prismabuild.tia_overhead.v1', 'python': sys.version,
            'files': sorted(fast), 'arms': arms,
            'method': 'five interleaved plain/recorded pairs in one PB measurement action; includes pytest startup'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=['extract', 'replay', 'campaign', 'mutation', 'overhead', 'extract-cas'])
    parser.add_argument('--root', type=Path, default=Path.cwd())
    parser.add_argument('--gate-dir', type=Path, default=Path('tools/ci'))
    parser.add_argument('--map', type=Path)
    parser.add_argument('--reports', type=Path, nargs='+')
    parser.add_argument('--history', type=Path)
    parser.add_argument('--output', type=Path)
    parser.add_argument('--count', type=int, default=100)
    parser.add_argument('--mutation')
    args = parser.parse_args()
    root = args.root.resolve()
    gate_dir = args.gate_dir.resolve()
    if args.mode == 'extract-cas':
        extract_cas(json.loads(os.environ['PB_TIA_CAS_INPUTS']), args.output)
    elif args.mode == 'extract':
        extract(args.reports, args.output)
    elif args.mode == 'campaign':
        campaign(root, gate_dir, read_impact_map(args.map), args.map, args.output, args.count)
    else:
        result = (overhead(root, gate_dir) if args.mode == 'overhead' else
                  run_mutation(root, gate_dir, json.loads(args.mutation)) if args.mode == 'mutation'
                  else replay_impact(root, gate_dir, read_impact_map(args.map), json.loads(args.history.read_text())))
        rendered = sorted_json(result)
        if args.output:
            args.output.write_text(rendered + '\n')
        print('tia-evaluation: ' + rendered, flush=True)
        if args.mode == 'mutation' and not result['valid']:
            raise SystemExit(1)


if __name__ == '__main__':
    main()
