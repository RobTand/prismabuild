#!/usr/bin/env python3
"""Attach this process to its authorized PB memory slice before executing argv."""
from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'src'))
from prismabuild.resource_scope import broker_request


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
    try:
        broker_request({'op': 'attach', 'action_key': args.action_key,
                        'nonce': args.nonce, 'token': args.token},
                       socket_path=args.socket)
        os.execvpe(argv[0], argv, os.environ)
    except (OSError, ValueError) as exc:
        sys.stderr.write(f'PrismaBuild resource attach refused: {exc}\n')
        return 125
    return 125


if __name__ == '__main__':
    raise SystemExit(main())
