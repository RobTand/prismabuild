#!/usr/bin/env python3
"""Run the CPU full-suite population on one four-CPU CI slot."""
from __future__ import annotations

import argparse
import json
import math
import os
from pathlib import Path
import runpy
import subprocess
import sys
import time
import xml.etree.ElementTree as ET


def selected_files(root: Path) -> list[str]:
    lanes = runpy.run_path(str(root / 'tools/verify.py'))['populations'](root)
    return sorted(name for lane, files in lanes.items() if lane not in {'root', 'gpu'}
                  for name in files)


def partition(files: list[str], durations: dict[str, float], count: int) -> list[list[str]]:
    """Longest first, with lexical and shard-index tie breaks. Unknown files cost 1s."""
    if count < 1:
        raise ValueError('shard count must be positive')
    groups = [[] for _ in range(count)]
    loads = [0.0] * count
    costs = {name: float(durations.get(name, 1)) for name in files}
    if any(not math.isfinite(cost) or cost < 0 for cost in costs.values()):
        raise ValueError('durations must be finite and nonnegative')
    for name in sorted(set(files), key=lambda name: (-costs[name], name)):
        index = min(range(count), key=lambda index: (loads[index], index))
        groups[index].append(name)
        loads[index] += costs[name]
    return [sorted(group) for group in groups]


def pytest_collection_modifyitems(items):
    """JUnit's classname loses path and parameter punctuation. Preserve pytest's ID."""
    for item in items:
        item.user_properties.append(('nodeid', item.nodeid))


def read_junit(path: Path) -> dict[str, dict]:
    tests = {}
    for case in ET.parse(path).iter('testcase'):
        node = next((p.get('value') for p in case.findall('./properties/property')
                     if p.get('name') == 'nodeid'), None)
        if node is None:
            # Collection errors have no item properties. Pytest names the file.
            node = case.get('name', '')
            if not node.startswith('tests/'):
                raise ValueError('JUnit testcase is missing its exact nodeid')
        failure = case.find('failure')
        if failure is None:
            failure = case.find('error')
        outcome = 'failed' if failure is not None else (
            'skipped' if case.find('skipped') is not None else 'passed')
        row = {'outcome': outcome, 'duration': float(case.get('time', '0')),
               'failure': '' if failure is None else (failure.text or failure.get('message', ''))}
        if node in tests:
            prior = tests[node]
            row['duration'] += prior['duration']
            if prior['outcome'] == 'failed':
                row['outcome'] = 'failed'
                row['failure'] = prior['failure'] + '\n' + row['failure']
        tests[node] = row
    return tests


def has_verdict(code: int, rows: dict[str, dict]) -> bool:
    return bool(rows) and (code == 0 or any(r['outcome'] == 'failed' for r in rows.values()))


def file_durations(results: dict) -> dict[str, float]:
    durations = {}
    for node, row in results.get('tests', {}).items():
        name = node.split('::', 1)[0]
        durations[name] = durations.get(name, 0) + row['duration']
    return durations


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--shard', type=int, required=True)
    parser.add_argument('--shards', type=int, default=4)
    parser.add_argument('--workers', type=int, default=4)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--history', type=Path)
    parser.add_argument('--previous', type=Path, help='Attempt-one artifact; skip if it has a verdict')
    args = parser.parse_args(argv)
    if not 0 <= args.shard < args.shards or not 1 <= args.workers <= 4:
        parser.error('invalid shard index or worker count')
    if args.previous and (args.previous / 'result.json').exists():
        prior = json.loads((args.previous / 'result.json').read_text())
        if prior['shard'] != args.shard:
            raise ValueError('attempt-one shard does not match')
        if prior['verdict']:
            print(f'Shard {args.shard} already has a test verdict; no retry.')
            return 0
    root = Path(__file__).resolve().parents[2]
    history = json.loads(args.history.read_text()) if args.history and args.history.exists() else {}
    files = partition(selected_files(root), file_durations(history), args.shards)[args.shard]
    args.output.mkdir(parents=True, exist_ok=True)
    xml = args.output.resolve() / 'junit.xml'
    env = dict(os.environ, OMP_NUM_THREADS='1', MKL_NUM_THREADS='1', OPENBLAS_NUM_THREADS='1',
               PRISMABUILD_TEST_TIMEOUT_S='300')
    env['PYTHONPATH'] = os.pathsep.join([str(root / 'tools/ci'), str(root / 'src'),
                                       env.get('PYTHONPATH', '')])
    start = time.monotonic()
    code, rows, error = 0, {}, ''
    if files:
        command = [sys.executable, '-m', 'pytest', '-p', 'full_suite_shard', '-n', str(args.workers),
                   '--dist', 'loadfile', '--continue-on-collection-errors', '--junitxml', str(xml), '--durations', '30', *files]
        try:
            code = subprocess.run(command, cwd=root, env=env, check=False).returncode
            rows = read_junit(xml)
        except (OSError, ValueError, ET.ParseError) as exc:
            error = f'{type(exc).__name__}: {exc}'
    verdict = has_verdict(code, rows) if files else True
    result = {'shard': args.shard, 'verdict': verdict, 'exit_code': code, 'tests': rows,
              'files': files, 'wall_seconds': round(time.monotonic() - start, 3), 'error': error}
    (args.output / 'result.json').write_text(json.dumps(result, sort_keys=True) + '\n')
    print(json.dumps({key: value for key, value in result.items() if key not in {'tests', 'files'}}))
    return 0 if verdict else 2


if __name__ == '__main__':
    raise SystemExit(main())
