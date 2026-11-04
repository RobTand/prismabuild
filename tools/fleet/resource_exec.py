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
import re
import signal
import socket
import stat
import sys

PRIVATE_CAPABILITY_FD_PROTOCOL = 1


class CompletionEOF(OSError):
    """The broker closed before sending a complete execution result."""


def read_capability_fd(descriptor: int) -> str:
    """Consume one atomic 64-byte pipe frame, without waiting for a writer."""
    if type(descriptor) is not int or not 3 <= descriptor <= 2**31 - 1:
        raise ValueError('invalid private capability descriptor')
    try:
        if not stat.S_ISFIFO(os.fstat(descriptor).st_mode):
            raise ValueError('invalid private capability descriptor')
        os.set_blocking(descriptor, False)
        frame = os.read(descriptor, 65)
        if re.fullmatch(b'[a-f0-9]{64}', frame) is None:
            raise ValueError('invalid private capability frame')
        return frame.decode('ascii')
    finally:
        os.close(descriptor)


def process_umask() -> int:
    # /proc exposes the inherited mask without mutating process-global state.
    for line in Path('/proc/self/status').read_text().splitlines():
        if line.startswith('Umask:'):
            value = int(line.split()[1], 8)
            if 0 <= value <= 0o777:
                return value
    raise OSError('cannot read this launcher process umask')


def payload_identity_env(env, *, action_key, nonce):
    """Stamp public attempt identity plus the sealed helper root.

    Pure and unit-testable: copies ``env`` and assigns
    ``PRISMABUILD_ACTION_NONCE``/``PRISMABUILD_ACTION_SCOPE`` from the
    exact launch identity (never the broker token) plus
    ``PRISMABUILD_READER_HELPER_ROOT`` as the immutable generation ROOT
    (consumers append ``/src`` themselves; this variable never names it,
    so no layer can double it).  Assignment, not setdefault: an outer
    attempt's variables must not leak into the inner payload -- and when
    a derivation fails its keys are REMOVED, never left holding stale
    outer values while documented as unset.
    """

    payload_env = dict(env)
    try:
        from resource_broker import scope_id as broker_scope_id
    except ImportError:
        broker_scope_id = None  # type: ignore[assignment]
    if broker_scope_id is not None:
        payload_env['PRISMABUILD_ACTION_NONCE'] = nonce
        payload_env['PRISMABUILD_ACTION_SCOPE'] = broker_scope_id(
            action_key, nonce)
    else:
        payload_env.pop('PRISMABUILD_ACTION_NONCE', None)
        payload_env.pop('PRISMABUILD_ACTION_SCOPE', None)
    try:
        from prismabuild.runtime_paths import generation_root as sealed_root
        payload_env['PRISMABUILD_READER_HELPER_ROOT'] = str(
            sealed_root(__file__))
    except (ImportError, OSError):
        payload_env.pop('PRISMABUILD_READER_HELPER_ROOT', None)
    return payload_env


def main() -> int:
    # Resolve source versus published layout only when executing the proxy.
    source = Path(__file__).resolve().parents[1] / 'src'
    if not source.is_dir():
        source = Path(__file__).resolve().parents[2] / 'src'
    sys.path.insert(0, str(source))
    from prismabuild.resource_scope import MAX_MESSAGE_BYTES, broker_request

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--socket', required=True, type=Path, help='Local resource broker Unix socket.')
    parser.add_argument('--action-key', required=True, help='Full action key owning this execution.')
    parser.add_argument('--nonce', required=True, help='Unique 32-hex nonce for this execution attempt.')
    transport = parser.add_mutually_exclusive_group(required=True)
    transport.add_argument('--token', help='Legacy broker capability transport.')
    transport.add_argument('--token-fd', type=int, help='Private capability pipe descriptor.')
    parser.add_argument('argv', nargs=argparse.REMAINDER, help='Command and arguments to execute inside the owned scope; precede with --.')
    args = parser.parse_args()
    argv = args.argv[1:] if args.argv[:1] == ['--'] else args.argv
    if not argv:
        parser.error('an executable is required')
    try:
        token = read_capability_fd(args.token_fd) if args.token_fd is not None else args.token
        if re.fullmatch('[a-f0-9]{64}', token) is None:
            raise ValueError('invalid resource launch capability')
    except (OSError, ValueError):
        sys.stderr.write('PrismaBuild resource execution refused: invalid capability transport\n')
        return 125
    identity = {'action_key': args.action_key, 'nonce': args.nonce, 'token': token}

    def terminate(signum, _frame):
        # The actual payload is a broker child, so signal its exact authenticated
        # scope rather than assuming that this proxy is the payload's parent.
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        try:
            broker_request({'op': 'stop', **identity,
                            'reason': f'launcher received signal {signum}'},
                           socket_path=args.socket)
        except (OSError, ValueError):
            sys.stderr.write('PrismaBuild resource stop failed\n')
        raise SystemExit(128 + signum)

    signal.signal(signal.SIGTERM, terminate)
    signal.signal(signal.SIGINT, terminate)
    try:
        # The payload's public attempt identity, from the exact launch
        # identity this proxy was invoked with (never the broker token):
        # readers downstream bind their pins to the attempt that launched
        # them and compare it to the live claim, so a superseded process
        # can never silently adopt its successor's attempt.  Names match
        # core's protected residency forwarding, which refuses sealed
        # spoofs of either variable.  The sealed helper root rides along
        # so readers import from sealed bytes, never the mutable repo.
        payload_env = payload_identity_env(
            os.environ, action_key=args.action_key, nonce=args.nonce)
        request = {'op': 'run', **identity, 'argv': argv,
                   'cwd': os.getcwd(), 'env': payload_env,
                   'affinity': sorted(os.sched_getaffinity(0)), 'umask': process_umask()}
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
                    raise CompletionEOF
                data.extend(chunk)
                if len(data) > MAX_MESSAGE_BYTES:
                    raise OSError('resource execution response exceeds 64 KiB')
            response = json.loads(data.split(b'\n', 1)[0])
            if not isinstance(response, dict) or response.get('ok') is not True:
                raise OSError('resource broker refused execution')
            returncode = response.get('returncode')
            if type(returncode) is not int or not -64 <= returncode <= 255:
                raise OSError('resource broker returned an invalid execution status')
            return returncode if returncode >= 0 else 128 - returncode
    except CompletionEOF:
        sys.stderr.write('PrismaBuild resource execution refused: resource broker closed '
                         'without an execution result\n')
        return 125
    except (OSError, ValueError):
        sys.stderr.write('PrismaBuild resource execution refused\n')
        return 125


if __name__ == '__main__':
    raise SystemExit(main())
