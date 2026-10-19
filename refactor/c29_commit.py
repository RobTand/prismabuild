#!/usr/bin/env python3
"""Commit C29 source moves with a private index, then bounded caller batches.

Only run in an owned checkout based on C29's exact source pin. Source files
are never changed. A moved old path is absent for one commit, so Git can
recognize the relocation before the literal entry wrapper is recreated.
"""
import argparse
import json
import os
from pathlib import Path
import subprocess
import tempfile

from c29_move import BASE, MOVES


def c29_commit_units(root, report):
    def command(*args, env=None, data=None):
        return subprocess.check_output(['git', '-c', 'core.hooksPath=/dev/null', '-C', str(root), *args],
                                       env=env, input=data, text=True).strip()
    start = command('rev-parse', 'HEAD')
    command('merge-base', '--is-ancestor', BASE, start)
    branch = command('symbolic-ref', 'HEAD')
    commits = []

    def record(paths, removed, message):
        head = command('rev-parse', 'HEAD')
        with tempfile.TemporaryDirectory(prefix='c29-index-') as scratch:
            env = dict(os.environ, GIT_INDEX_FILE=str(Path(scratch) / 'index'))
            command('read-tree', head, env=env)
            for path in removed:
                command('update-index', '--force-remove', path, env=env)
            for path in paths:
                file = root / path
                oid = command('hash-object', '-w', '--', path)
                mode = '100755' if os.access(file, os.X_OK) else '100644'
                command('update-index', '--add', '--cacheinfo', f'{mode},{oid},{path}', env=env)
            names = command('diff', '--cached', '--name-only', '--no-renames', env=env).splitlines()
            stat = command('diff', '--cached', '--numstat', '--find-renames=50%', env=env)
            lines = sum(sum(map(int, row.split('\t')[:2])) for row in stat.splitlines())
            if not names:
                return
            if len(names) > 6 or lines > 600:
                raise RuntimeError(f'Unbounded commit: {len(names)} paths, {lines} lines\n{stat}')
            command('diff', '--cached', '--check', env=env)
            tree = command('write-tree', env=env)
            commit = command('commit-tree', tree, '-p', head, env=env, data=message + '\n')
            command('update-ref', branch, commit, head)
            commits.append({'head': commit, 'tree': tree, 'paths': names,
                            'changed_lines': lines, 'numstat': stat, 'message': message})
            report.write_text(json.dumps({'base': BASE, 'start': start, 'commits': commits}, indent=2) + '\n')
    for old, new in MOVES.items():
        current = command('ls-tree', 'HEAD', new)
        if not current:
            record([new], [old], f'Move {Path(old).stem} implementation into the package')
            if (root / old).exists():
                record([old], [], f'Keep the {Path(old).stem} entry wrapper')
    # read-tree aligns only the index; every source file stays as authored.
    command('read-tree', 'HEAD')
    dirty = command('diff', '--name-only').splitlines()
    extra = command('ls-files', '--others', '--exclude-standard').splitlines()
    files = sorted(set(dirty + extra))
    for offset in range(0, len(files), 6):
        group = files[offset:offset + 6]
        record([p for p in group if (root / p).exists()],
               [p for p in group if not (root / p).exists()],
               f'Migrate movement package callers {offset // 6 + 1:02d}')
    command('read-tree', 'HEAD')
    report.write_text(json.dumps({'base': BASE, 'start': start, 'commits': commits,
                                 'final_head': command('rev-parse', 'HEAD'),
                                 'final_status': command('status', '--porcelain=v1')}, indent=2) + '\n')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--report', type=Path, required=True)
    args = parser.parse_args()
    c29_commit_units(args.root, args.report)
