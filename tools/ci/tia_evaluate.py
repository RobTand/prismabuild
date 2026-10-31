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
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import xml.etree.ElementTree as ET

from tia_select import load, plan
from tia_map import read_map, write_map
from tia_diff import analyze, compare, digest, source


def extract(reports: list[Path], output: Path):
    """Extract raw records only from complete pbtest shard verdicts."""
    with tempfile.TemporaryDirectory() as scratch:
        paths = []
        for report in reports:
            for row in json.loads(report.read_text()):
                if not row['verdict']['green'] or not row.get('receipt_path'):
                    raise ValueError(f"map shard not green: {row['action_key']}")
                for line in row['output'].splitlines():
                    if line.startswith('tia-record: '):
                        records = json.loads(gzip.decompress(base64.b64decode(line.split(': ', 1)[1])))
                        for record in records:
                            path = Path(scratch) / f'{len(paths)}.json'
                            path.write_text(json.dumps(record))
                            paths.append(path)
        result = write_map(paths, output)
    print(json.dumps({'tests': len(result['tests']), 'functions': len(result['functions']),
                      'bytes': output.stat().st_size, 'source_commit': result['source_commit']}))


def replay(root: Path, gate_dir: Path, mapping: dict, history: dict) -> dict:
    gate_replay = load(gate_dir / 'gate_replay.py', 'tia_gate_replay')
    # Reuse its population, static rule, estimates and nearest-rank statistics.
    static = gate_replay.replay(root, history)
    rows = []
    for pull in history['pulls']:
        merge = pull['merge_commit_sha']
        parent = subprocess.check_output(['git', 'rev-parse', merge + '^'], cwd=root, text=True).strip()
        changes = analyze(root, parent, merge, merge_base=False)
        selected = plan(root, gate_dir, mapping, changes)
        rows.append({'number': pull['number'], 'selected_files': len(selected['selected']),
                     'wall_seconds': selected['estimate']['wall_seconds'],
                     'serial_seconds': selected['estimate']['serial_seconds'],
                     'fallbacks': selected['fallbacks']})
    return {'schema': 'prismabuild.tia_replay.v1', 'static': static,
            'tia': {'summary': gate_replay.summarize(rows), 'pulls': rows},
            'method': 'same current population and gate estimates; AST diffs of merge parent to merge; '
                      'historical source hashes must match the current map, otherwise static fallback'}


def mutations(root: Path, mapping: dict):
    """One deterministic operator flip per function, including both requested trees."""
    known = set(mapping['functions'])
    for path in sorted(mapping['files']):
        if not path.startswith(('src/prismabuild/', 'tools/fleet/')):
            continue
        original = (root / path).read_text()
        if digest(original.encode()) != mapping['files'][path]:
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
                                       'before': digest(original.encode()), 'start': start, 'end': end,
                                       'replacement': changed, 'diff': compare(original, mutant)}
                                break
                            pending.extend(ast.iter_child_nodes(candidate))
                    yield from visit(child, name + '.<locals>.')
                elif isinstance(child, ast.ClassDef):
                    yield from visit(child, prefix + child.name + '.')
                else:
                    yield from visit(child, prefix)
        yield from visit(tree)


def campaign(root: Path, gate_dir: Path, mapping: dict, map_path: Path, output: Path, count: int):
    gate = load(gate_dir / 'gate_select.py', 'tia_campaign_gate')
    durations = gate.read_json(gate_dir / 'gate_durations.json')
    quarantine = gate.read_json(gate_dir / 'gate_quarantine.json')
    index = gate.dependency_index(root)
    graph = gate.helper_importers(index, [])
    candidates = []
    for mutation in mutations(root, mapping):
        selected = gate.create_plan(root, [mutation['path']], durations=durations,
                                    quarantine=quarantine, index=index, helper_graph=graph)
        cost = selected['estimate']['serial_seconds']
        if cost is not None and cost <= 30:
            candidates.append((cost, mutation['path'], mutation['function'], mutation))
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
            tests.setdefault(node.split('::', 1)[0] + '::recorded', set()).update(remap[i] for i in values if i in remap)
        subset = dict(mapping, functions=[mapping['functions'][i] for i in ids],
                      tests={node: sorted(values) for node, values in tests.items()},
                      files={mutation['path']: mapping['files'][mutation['path']]})
        packed_map = base64.b64encode(gzip.compress(json.dumps(subset).encode(), mtime=0)).decode()
        rows.append({'cwd': str(root), 'argv': ['/home/rob/venvs/pb-cpu/bin/python',
                     'tools/ci/tia_evaluate.py', 'mutation', '--mutation', json.dumps(mutation),
                     '--gate-dir', str(gate_dir.relative_to(root))],
                     'demand': {'cpu': 1, 'mem_gb': 4}, 'priority': -10, 'timeout_s': 900,
                     'env': {'OMP_NUM_THREADS': '1', 'MKL_NUM_THREADS': '1', 'OPENBLAS_NUM_THREADS': '1',
                             'PB_TIA_MAP': packed_map, 'PB_TIA_MUTATION': str(number)}})
    output.write_text(json.dumps(rows, sort_keys=True, indent=2) + '\n')
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
               '--junitxml=' + str(xml), *['--deselect=' + node for node in deselect], *files]
    start = time.monotonic()
    try:
        run = subprocess.run(command, cwd=root, env=env, capture_output=True, text=True, timeout=300)
    except subprocess.TimeoutExpired:
        return {'returncode': 124, 'complete': False, 'failures': [], 'files': len(files)}
    (directory / (label + '.log')).write_text(run.stdout + run.stderr)
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
    return {'returncode': run.returncode, 'complete': run.returncode in (0, 1),
            'failures': sorted(failures), 'errors': sorted(errors), 'tests': tests, 'skipped': skips,
            'files': len(files), 'elapsed_s': time.monotonic() - start,
            'tail': (run.stdout + run.stderr)[-4000:] if run.returncode else ''}


def run_mutation(root: Path, gate_dir: Path, mutation: dict) -> dict:
    mapping = json.loads(gzip.decompress(base64.b64decode(os.environ['PB_TIA_MAP'])))
    path = mutation['path']
    original = (root / path).read_bytes()
    if digest(original) != mutation['before']:
        raise ValueError('mutation source hash mismatch')
    diff = {'python': {path: mutation['diff']}, 'non_python': [], 'base_hashes': {path: mutation['before']}}
    selected = plan(root, gate_dir, mapping, diff)
    gate = load(gate_dir / 'gate_select.py', 'tia_mutation_gate')
    static = gate.create_plan(root, [path], durations=gate.read_json(gate_dir / 'gate_durations.json'),
                              quarantine=gate.read_json(gate_dir / 'gate_quarantine.json'))
    tia_files = [row['path'] for row in selected['selected']]
    static_files = [row['path'] for row in static['selected']]
    with tempfile.TemporaryDirectory() as temporary:
        directory = Path(temporary)
        baseline = pytest_run(root, sorted(set(tia_files + static_files)), static['deselect'], directory, 'baseline')
        changed = original[:mutation['start']] + mutation['replacement'].encode() + original[mutation['end']:]
        try:
            (root / path).write_bytes(changed)
            # Avoid reading a same-size, same-second bytecode cache after mutating a comparison.
            for cache in (root / path).parent.glob('__pycache__/' + Path(path).stem + '.*.pyc'):
                cache.unlink()
            tia = pytest_run(root, tia_files, static['deselect'], directory, 'tia')
            full = pytest_run(root, static_files, static['deselect'], directory, 'static')
        finally:
            (root / path).write_bytes(original)
    baseline_bad = set(baseline['failures'] + baseline['errors'])
    def killed(result):
        return result['complete'] and bool(set(result['failures'] + result['errors']) - baseline_bad)
    return {'mutation': mutation, 'baseline': baseline, 'tia': tia, 'static': full,
            'tia_killed': killed(tia), 'static_killed': killed(full),
            'valid': baseline['returncode'] == 0 and tia['complete'] and full['complete'],
            'tia_plan': selected}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=['extract', 'replay', 'campaign', 'mutation'])
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
    if args.mode == 'extract':
        extract(args.reports, args.output)
    elif args.mode == 'campaign':
        campaign(root, gate_dir, read_map(args.map), args.map, args.output, args.count)
    else:
        result = (run_mutation(root, gate_dir, json.loads(args.mutation)) if args.mode == 'mutation'
                  else replay(root, gate_dir, read_map(args.map), json.loads(args.history.read_text())))
        rendered = json.dumps(result, sort_keys=True)
        if args.output:
            args.output.write_text(rendered + '\n')
        print('tia-evaluation: ' + rendered, flush=True)
        if args.mode == 'mutation' and not result['valid']:
            raise SystemExit(1)


if __name__ == '__main__':
    main()
