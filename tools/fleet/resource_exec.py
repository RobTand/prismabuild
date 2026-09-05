#!/usr/bin/env python3
"""Proxy an authorized PB command to a broker that launches it inside its scope.

The broker receives only ordinary stdin/stdout/stderr descriptors. A fixed
root-owned helper enters its own cgroup and drops privileges before executing
this user argv; no numeric client PID is ever migrated by a privileged broker.
"""
from __future__ import annotations

import argparse
import array
import json
import os
from pathlib import Path
import signal
import socket
import sys

_source = Path(__file__).resolve().parents[1] / 'src'
if not _source.is_dir():
    _source = Path(__file__).resolve().parents[2] / 'src'
sys.path.insert(0, str(_source))
from prismabuild.resource_scope import MAX_MESSAGE_BYTES, broker_request


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--socket', required=True, type=Path)
    parser.add_argument('--action-key', required=True)
    parser.add_argument('--nonce', required=True)
    parser.add_argument('--token', required=True)
    parser.add_argument('argv', nargs=argparse.REMAINDER)
    args = parser.parse_args()
    argv = args.argv[1:] if args.argv[:1] == ['--'] else args.argv
    if not argv:
        parser.error('an executable is required')
    identity = {'action_key': args.action_key, 'nonce': args.nonce, 'token': args.token}

    def terminate(signum, _frame):
        # The actual payload is a broker child, so signal its exact authenticated
        # scope rather than assuming that this proxy is the payload's parent.
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        try:
            broker_request({'op': 'stop', **identity,
                            'reason': f'launcher received signal {signum}'},
                           socket_path=args.socket)
        except (OSError, ValueError) as exc:
            sys.stderr.write(f'PrismaBuild resource stop failed: {exc}\n')
        raise SystemExit(128 + signum)

    signal.signal(signal.SIGTERM, terminate)
    signal.signal(signal.SIGINT, terminate)
    try:
        request = {'op': 'run', **identity, 'argv': argv,
                   'cwd': os.getcwd(), 'env': dict(os.environ),
                   'affinity': sorted(os.sched_getaffinity(0))}
        message = json.dumps(request, separators=(',', ':')).encode() + b'\n'
        if len(message) > MAX_MESSAGE_BYTES:
            raise OSError('resource launch request exceeds 64 KiB')
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
            client.settimeout(10)
            client.connect(str(args.socket))
            sent = client.sendmsg([message], [(socket.SOL_SOCKET, socket.SCM_RIGHTS,
                                              array.array('i', [0, 1, 2]))])
            if sent < len(message):
                client.sendall(message[sent:])
            # The normal response arrives only after the owned payload exits.
            # Action timeout/withdrawal is controlled by the supervising pool.
            client.settimeout(None)
            data = bytearray()
            while b'\n' not in data:
                chunk = client.recv(min(4096, MAX_MESSAGE_BYTES + 1 - len(data)))
                if not chunk:
                    raise OSError('resource broker closed without an execution result')
                data.extend(chunk)
                if len(data) > MAX_MESSAGE_BYTES:
                    raise OSError('resource execution response exceeds 64 KiB')
            response = json.loads(data.split(b'\n', 1)[0])
            if not isinstance(response, dict) or response.get('ok') is not True:
                raise OSError(f'resource broker refused execution: {response}')
            returncode = response.get('returncode')
            if type(returncode) is not int or not -64 <= returncode <= 255:
                raise OSError('resource broker returned an invalid execution status')
            return returncode if returncode >= 0 else 128 - returncode
    except (OSError, ValueError) as exc:
        sys.stderr.write(f'PrismaBuild resource execution refused: {exc}\n')
        return 125


if __name__ == '__main__':
    raise SystemExit(main())
