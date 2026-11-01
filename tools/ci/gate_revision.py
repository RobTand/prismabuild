#!/usr/bin/env python3
"""Resolve the exact checkout and selection base for hosted gate jobs."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
import subprocess
import sys


def resolve(root: Path, mode: str, base: str, head: str) -> dict:
    """Ordinary PRs diff the merge's first parent; full gates keep event refs."""
    if mode not in {'selection', 'full'}:
        raise ValueError('gate mode must be selection or full')
    if not all(re.fullmatch(r'[0-9a-fA-F]{40,64}', sha) for sha in (base, head)):
        raise ValueError('gate revisions must be full Git object IDs')

    def git(*arguments):
        return subprocess.run(['git', *arguments], cwd=root, text=True,
                              capture_output=True, check=True).stdout.strip()

    if git('rev-parse', 'HEAD') != head.lower():
        raise ValueError('gate checkout does not match its requested head')
    selected_base = git('rev-parse', '--verify', head + '^1' if mode == 'selection'
                        else base + '^{commit}')
    return {'mode': mode, 'base_sha': selected_base, 'head_sha': head.lower(),
            'base_source': 'merge_first_parent' if mode == 'selection' else 'pr_event'}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path.cwd())
    parser.add_argument('--mode', choices=['selection', 'full'], required=True)
    parser.add_argument('--base', required=True, help='pull_request event base SHA')
    parser.add_argument('--head', required=True, help='immutable checked-out SHA')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        result = resolve(args.root, args.mode, args.base, args.head)
        args.output.write_text(json.dumps(result, indent=2) + '\n')
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        print('NO_VERDICT: gate revisions unavailable: ' + str(error), file=sys.stderr)
        return 75
    print('base_sha=' + result['base_sha'])
    print('head_sha=' + result['head_sha'])
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
