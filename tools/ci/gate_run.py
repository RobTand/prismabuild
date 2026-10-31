#!/usr/bin/env python3
"""Run the selected files on the CI slot and write a final pytest verdict."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys


def run_plan(plan: dict, *, root: Path, output: Path, junit: Path,
             execute=subprocess.run, affinity=None) -> int:
    files = [row['path'] for row in plan['selected']]
    if not files:
        raise ValueError('empty gate selection cannot run pytest')
    cpus = os.sched_getaffinity(0) if affinity is None else affinity
    workers = min(5, len(cpus))
    if not workers:
        raise ValueError('CI slot has no assigned CPU')
    environment = dict(os.environ, PRISMABUILD_TEST_TIMEOUT_S='60',
                       OMP_NUM_THREADS='1', MKL_NUM_THREADS='1', OPENBLAS_NUM_THREADS='1')
    command = [sys.executable, '-m', 'pytest', '-n', str(workers), '--dist=loadfile',
               '-q', '-ra', '--junitxml=' + str(junit),
               *('--deselect=' + target for target in plan['deselect']), *files]
    result = execute(command, cwd=root, env=environment, check=False)
    if result.returncode < 0:
        raise RuntimeError(f'pytest ended on signal {-result.returncode}, without a verdict')
    verdict = 'pass' if result.returncode == 0 else 'fail'
    # Write before any artifact or summary operation that might fail independently.
    with output.open('a') as handle:
        handle.write(f'verdict={verdict}\n')
    return 0 if verdict == 'pass' else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--plan', type=Path, required=True)
    parser.add_argument('--root', type=Path, default=Path.cwd())
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--junit', type=Path, required=True)
    args = parser.parse_args(argv)
    return run_plan(json.loads(args.plan.read_text()), root=args.root,
                    output=args.output, junit=args.junit)


if __name__ == '__main__':
    raise SystemExit(main())
