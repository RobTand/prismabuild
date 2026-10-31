#!/usr/bin/env python3
"""Run any CI-assigned gate files. Exit 0 pass, 1 test failure, 75 no verdict."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys

NO_VERDICT = 75


def run_plan(plan: dict, *, root: Path, output: Path, junit: Path,
             files: list[str] | None = None, workers: int | None = None,
             execute=subprocess.run, affinity=None) -> int:
    record = {'schema': 'prismabuild.gate_result.v1',
              'population_version': plan.get('population', {}).get('version'),
              'mode': plan.get('mode'), 'base_sha': plan.get('base_sha'), 'head_sha': plan.get('head_sha'), 'files': [], 'verdict': None, 'pytest_exit_code': None}
    try:
        eligible = {row['path'] for row in plan['selected']}
        files = sorted(eligible) if files is None else files
        if not files:
            raise ValueError('empty gate assignment cannot run pytest')
        if len(files) != len(set(files)) or set(files) - eligible:
            raise ValueError('assignment contains duplicate files or files outside the plan')
        record['files'] = files
        cpus = os.sched_getaffinity(0) if affinity is None else affinity
        workers = len(cpus) if workers is None else workers
        if isinstance(workers, bool) or not isinstance(workers, int) or workers < 1 or not cpus:
            raise ValueError('job needs a positive worker count and assigned CPUs')
        workers = min(workers, len(cpus))
        record['workers'] = workers
        environment = dict(os.environ, PRISMABUILD_TEST_TIMEOUT_S='300',
                           OMP_NUM_THREADS='1', MKL_NUM_THREADS='1', OPENBLAS_NUM_THREADS='1')
        # setup-python exports loader overrides; proof capture must see the actual
        # installed interpreter rather than an ambient dynamic-loader override.
        environment = {name: value for name, value in environment.items() if not name.startswith('LD_')}
        junit.parent.mkdir(parents=True, exist_ok=True)
        command = [sys.executable, '-m', 'pytest', '-n', str(workers), '--dist=loadfile',
                   '-q', '-ra', '--junitxml=' + str(junit), '-o', 'junit_family=xunit1',
                   *('--deselect=' + target for target in plan['deselect']
                     if target.split('::', 1)[0] in files), *files]
        result = execute(command, cwd=root, env=environment, check=False)
        record['pytest_exit_code'] = result.returncode
        if result.returncode not in {0, 1}:
            raise RuntimeError(f'pytest exited {result.returncode}, without a complete test verdict')
        record['verdict'] = 'pass' if result.returncode == 0 else 'fail'
        code = 0 if record['verdict'] == 'pass' else 1
    except (OSError, ValueError, RuntimeError) as error:
        record['infrastructure_reason'] = str(error)
        code = NO_VERDICT
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(record, indent=2) + '\n')
    return code


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--plan', type=Path, required=True)
    parser.add_argument('--root', type=Path, default=Path.cwd())
    parser.add_argument('--result', type=Path, required=True)
    parser.add_argument('--junitxml', type=Path, required=True)
    parser.add_argument('--workers', type=int, help='resource-class CPU allocation; clamped to inherited affinity')
    assigned = parser.add_mutually_exclusive_group()
    assigned.add_argument('--files-from', type=Path, help='one assigned repository-relative file per line')
    assigned.add_argument('--files', nargs='+', help='explicit assigned test files')
    args = parser.parse_args(argv)
    try:
        plan = json.loads(args.plan.read_text())
        files = ([line.strip() for line in args.files_from.read_text().splitlines() if line.strip()]
                 if args.files_from else args.files)
        return run_plan(plan, root=args.root, output=args.result, junit=args.junitxml,
                        files=files, workers=args.workers)
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(f'gate: no verdict: {error}', file=sys.stderr)
        return NO_VERDICT


if __name__ == '__main__':
    raise SystemExit(main())
