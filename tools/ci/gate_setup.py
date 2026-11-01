#!/usr/bin/env python3
"""Prepare hosted CI's assigned files; setup failures exit 75, never 1."""
from __future__ import annotations

import argparse
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

NO_VERDICT = 75


def install_bubblewrap(execute):
    """Try the image's indexes first, with one bounded refresh fallback."""
    command = ['sudo', '-n', 'apt-get', '-o', 'Acquire::Retries=0',
               '-o', 'Acquire::http::Timeout=10', '-o', 'Acquire::https::Timeout=10']
    install = ['install', '-y', '--no-install-recommends', 'bubblewrap']
    try:
        execute([*command, *install], check=True, timeout=120)
    except subprocess.CalledProcessError:
        execute([*command, 'update', '-qq'], check=True, timeout=120)
        execute([*command, *install], check=True, timeout=120)


def relocate_python(prefix, target, execute):
    """Move within one filesystem; retain an alias for installed entry points."""
    prefix, target = Path(prefix), Path(target)
    if prefix.resolve() == target.resolve():
        return
    if target.exists():
        raise FileExistsError('unexpected hosted Python destination: ' + str(target))
    target.parent.mkdir(parents=True, exist_ok=True)
    print(f'GATE_SETUP_PYTHON: prefix uid={prefix.stat().st_uid}; '
          f'same filesystem={prefix.stat().st_dev == target.parent.stat().st_dev}', flush=True)
    if prefix.stat().st_dev == target.parent.stat().st_dev:
        execute(['sudo', 'mv', str(prefix), str(target)], check=True)
        # pip/pytest entry points retain their original shebang. setup-python's
        # cache-saving post-step can also still use its recorded interpreter.
        execute(['sudo', 'ln', '-s', str(target), str(prefix)], check=True)
    else:
        execute(['sudo', 'mkdir', '-p', str(target)], check=True)
        execute(['sudo', 'cp', '-a', str(prefix) + '/.', str(target) + '/'], check=True)


def _export(name, value):
    destination = os.environ.get(name)
    if destination:
        with Path(destination).open('a') as handle:
            handle.write(value + '\n')


def prepare(files, *, sandbox_mode='full', execute=subprocess.run):
    spec = importlib.util.spec_from_file_location('gate_partition', Path(__file__).with_name('gate_partition.py'))
    partition = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(partition)
    root = Path(__file__).resolve().parents[2]
    scratch = json.loads((root / 'tools/fleet/fleet_boxes.json').read_text())['action_tmpdir']
    execute(['sudo', 'install', '-d', '-m', '0700', '-o', str(os.getuid()), '-g', str(os.getgid()), scratch], check=True)
    _export('GITHUB_ENV', 'TMPDIR=' + scratch)
    sandbox = bool(set(files) & partition.SANDBOX_TEST_FILES)
    print(f'GATE_SETUP: sandbox={sandbox}; mode={sandbox_mode}; files={len(files)}', flush=True)
    if not sandbox or sandbox_mode == 'never':
        return
    if not shutil.which('bwrap'):
        install_bubblewrap(execute)
    execute(['sudo', '-n', 'sysctl', '-w', 'kernel.apparmor_restrict_unprivileged_userns=0'], check=True)
    execute(['bwrap', '--version'], check=True)
    if sandbox_mode == 'namespace':
        return
    original = Path(sys.base_prefix)
    target = Path('/usr/local/pb-gate-python')
    relocate_python(original, target, execute)
    # Read-only /usr bindings protect the interpreter inside bwrap; recursive
    # ownership changes add no protection to that namespace contract.
    execute(['sudo', 'sh', '-c', "printf '%s\\n' /usr/local/pb-gate-python/lib > /etc/ld.so.conf.d/pb-gate-python.conf"], check=True)
    execute(['sudo', 'ldconfig'], check=True)
    rewrite = '''import os, sys, sysconfig
from pathlib import Path
for config in Path(sysconfig.get_path('stdlib')).glob('_sysconfigdata_*.py'):
    config.write_text(config.read_text().replace(os.environ['GATE_ORIGINAL_PREFIX'], sys.base_prefix))
'''
    execute(['sudo', 'env', 'GATE_ORIGINAL_PREFIX=' + str(original), str(target / 'bin/python'), '-c', rewrite], check=True)
    execute(['env', '-u', 'LD_LIBRARY_PATH', '-u', 'LD_PRELOAD', str(target / 'bin/python'), '--version'], check=True)
    _export('GITHUB_PATH', str(target / 'bin'))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--files-from', type=Path, required=True)
    parser.add_argument('--sandbox-mode', choices=['full', 'never', 'namespace'], default='full',
                        help='never/namespace are explicit dependency-probe modes; gate jobs use full')
    args = parser.parse_args(argv)
    try:
        files = [line.strip() for line in args.files_from.read_text().splitlines() if line.strip()]
        prepare(files, sandbox_mode=args.sandbox_mode)
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        print('NO_VERDICT: ' + str(error), flush=True)
        return NO_VERDICT
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
