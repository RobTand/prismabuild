#!/usr/bin/env python3
"""Freeze source edits into an owned immutable Git pin before PB submission."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import tempfile


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--checkout', type=Path, default=Path.cwd())
    parser.add_argument('--phase', required=True)
    parser.add_argument('--destination', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    root = args.checkout.resolve()
    if not args.phase.replace('-', '').isalnum():
        raise ValueError('phase must be an owned simple name')

    def git(*words, data=None, env=None):
        return subprocess.check_output(['git', '-C', str(root), *words], input=data, env=env).strip().decode()

    parent = git('rev-parse', 'HEAD')
    with tempfile.TemporaryDirectory() as temporary:
        environment = dict(os.environ, GIT_INDEX_FILE=str(Path(temporary) / 'index'),
                           GIT_AUTHOR_DATE='2026-10-17T14:00:00Z',
                           GIT_COMMITTER_DATE='2026-10-17T14:00:00Z')
        git('read-tree', parent, env=environment)
        git('add', '-A', '--', '.', ':(exclude)qualification_*', env=environment)
        tree = git('write-tree', env=environment)
        pin = git('commit-tree', tree, '-p', parent, data=(
            'source: freeze C15 ' + args.phase + '\n').encode(), env=environment)
    ref = 'refs/c15/source/' + args.phase
    previous = subprocess.run(['git', '-C', str(root), 'rev-parse', '--verify', ref], capture_output=True)
    if previous.returncode == 0 and previous.stdout.decode().strip() != pin:
        raise ValueError('phase source is immutable; use a new phase')
    git('update-ref', ref, pin)
    destination = args.destination.absolute()
    if destination.exists():
        actual = subprocess.check_output(['git', '-C', str(destination), 'rev-parse', 'HEAD']).decode().strip()
        if actual != pin or subprocess.check_output(['git', '-C', str(destination), 'status', '--porcelain']):
            raise ValueError('existing frozen checkout differs or is dirty')
    else:
        git('worktree', 'add', '--detach', str(destination), pin)
    report = dict(parent=parent, source=pin, tree=tree, checkout=str(destination), ref=ref,
                  production_source_frozen=True, qualification_inputs='prepared separately')
    args.output.write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report))


if __name__ == '__main__':
    main()
