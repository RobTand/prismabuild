#!/usr/bin/env python3
"""Allowlisted environments for gate children that do not need Bazel auth."""
import os
import sys

NAMES = frozenset({
    'PATH', 'HOME', 'TMPDIR', 'TMP', 'TEMP', 'LANG', 'TZ', 'USER', 'LOGNAME',
    'SHELL', 'PYTHONPATH', 'SSL_CERT_FILE', 'SSL_CERT_DIR',
    'OMP_NUM_THREADS', 'MKL_NUM_THREADS', 'OPENBLAS_NUM_THREADS',
    'GIT_AUTHOR_NAME', 'GIT_AUTHOR_EMAIL', 'GIT_AUTHOR_DATE',
    'GIT_COMMITTER_NAME', 'GIT_COMMITTER_EMAIL', 'GIT_COMMITTER_DATE',
})
PREFIXES = ('LC_', 'PRISMABUILD_', 'GATE_', 'GITHUB_')


def child_environment(source=None):
    """Select operational settings; secret suffixes take precedence over prefixes."""
    source = os.environ if source is None else source
    return {name: value for name, value in source.items()
            if not name.endswith(('_API_KEY', '_TOKEN'))
            and (name in NAMES or name.startswith(PREFIXES))}


def main(argv=None):
    command = sys.argv[1:] if argv is None else argv
    if not command:
        raise SystemExit('usage: gate_env.py COMMAND [ARG ...]')
    os.execvpe(command[0], command, child_environment())


if __name__ == '__main__':
    main()
