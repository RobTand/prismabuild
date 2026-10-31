#!/usr/bin/env python3
"""Partition a complete gate plan into duration lanes without dropping files."""
from __future__ import annotations

import argparse
import json
import math
from pathlib import Path


def partition(plan: dict, durations: dict, threshold: float) -> dict:
    if not math.isfinite(threshold) or threshold < 0:
        raise ValueError('threshold must be finite and nonnegative')
    costs = durations['files']
    lanes = {'heavy': [], 'light': []}
    unknown = []
    for row in plan['selected']:
        path = row['path']
        cost = costs.get(path)
        if cost is not None and (isinstance(cost, bool) or not isinstance(cost, (int, float)) or
                                 not math.isfinite(cost) or cost < 0):
            raise ValueError('invalid duration for ' + path)
        if cost is None:
            unknown.append(path)
        lanes['heavy' if cost is None or cost >= threshold else 'light'].append(path)
    return {**{name: sorted(files) for name, files in lanes.items()},
            'threshold_seconds': threshold, 'unknown_files': sorted(unknown),
            **{name + '_cpu_seconds': None if set(files) & set(unknown) else
               round(sum(costs[path] for path in files), 3) for name, files in lanes.items()}}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--plan', type=Path, required=True)
    parser.add_argument('--durations', type=Path, default=Path('tools/ci/gate_durations.json'))
    parser.add_argument('--threshold', type=float, required=True)
    parser.add_argument('--lane', choices=['heavy', 'light'], required=True)
    parser.add_argument('--files-output', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args(argv)
    result = partition(json.loads(args.plan.read_text()), json.loads(args.durations.read_text()), args.threshold)
    args.output.write_text(json.dumps(result, sort_keys=True, indent=2) + '\n')
    args.files_output.write_text(''.join(path + '\n' for path in result[args.lane]))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
