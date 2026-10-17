#!/usr/bin/env python3
"""Replay C13's exact-base mbox into a new owned worktree, or verify its replay.

Git-only evidence: this does not collect tests, import product code or execute
build steps. A second invocation checks the existing clean tree without applying
the patches again. Existing worktrees, branches and dirty files are preserved.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess

BASE = 'd2f92f5ff533489182dddaf286feb54afde6f34a'


def c13_replay_git(root, *args):
    return subprocess.check_output(['git', '-C', str(root), *args], text=True).strip()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--head', required=True)
    parser.add_argument('--mbox', type=Path, required=True)
    parser.add_argument('--worktree', type=Path, required=True)
    parser.add_argument('--out', type=Path, required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    expected = c13_replay_git(root, 'rev-parse', args.head + '^{tree}')
    mbox, destination = args.mbox.resolve(strict=True), args.worktree.absolute()
    created = not destination.exists()
    if created:
        subprocess.run(['git', '-C', str(root), 'worktree', 'add', '--detach',
                        str(destination), BASE], check=True)
        subprocess.run(['git', '-C', str(destination), 'am', str(mbox)], check=True)
    actual = c13_replay_git(destination, 'rev-parse', 'HEAD^{tree}')
    clean = not c13_replay_git(destination, 'status', '--porcelain')
    result = dict(base=BASE, expected_head=args.head, expected_tree=expected,
                  replay_head=c13_replay_git(destination, 'rev-parse', 'HEAD'),
                  replay_tree=actual, worktree=str(destination), created=created,
                  clean=clean, tree_equal=actual == expected,
                  qualified=clean and actual == expected,
                  credit='Git replay only; no runtime or test credit')
    with args.out.open('x') as stream:
        json.dump(result, stream, indent=2)
        stream.write('\n')
    print(json.dumps(result))
    raise SystemExit(0 if result['qualified'] else 1)


if __name__ == '__main__':
    main()
