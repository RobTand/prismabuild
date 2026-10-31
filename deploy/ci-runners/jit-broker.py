#!/usr/bin/python3 -I
"""Forced SSH command for PB gate registrations. Never log the JIT response."""
import os
import re
import shlex
import sys


def main():
    try:
        args = shlex.split(os.environ.get('SSH_ORIGINAL_COMMAND', ''))
    except ValueError:
        return 1
    if len(args) < 7 or not re.fullmatch(
        r'name=pb-(sparky|sparklina)-slot-[12]-[a-f0-9]{8}-(?:[a-f0-9]{4}-){3}[a-f0-9]{12}',
        args[6],
    ):
        return 1
    expected = ['gh', 'api', '--method', 'POST',
                'repos/RobTand/prismabuild/actions/runners/generate-jitconfig',
                '-f', args[6], '-F', 'runner_group_id=1', '-f', 'work_folder=_work',
                *[value for label in ('self-hosted', 'linux', 'arm64', 'pb-fleet', 'pb-gate')
                  for value in ('-f', f'labels[]={label}')], '--jq', '.encoded_jit_config']
    if args != expected:
        return 1
    os.environ.pop('GH_DEBUG', None)
    with open(os.devnull, 'w') as sink:
        os.dup2(sink.fileno(), 2)
    os.execvp('gh', expected)


if __name__ == '__main__':
    sys.exit(main())
