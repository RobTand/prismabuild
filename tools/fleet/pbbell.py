#!/usr/bin/env python3
"""Serve queue change hints from the queue host's local filesystem (#1894)."""
from __future__ import annotations

import argparse
import errno
from pathlib import Path
import signal
import sys

_ENTRYPOINT = Path(__file__).resolve(strict=True)
sys.path.insert(0, str(_ENTRYPOINT.parents[
    2 if _ENTRYPOINT.parent.name == "fleet" else 1] / "src"))
from prismabuild import queue_bell, queue_notifier, role_session


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--queue-root", type=Path,
                        default=Path("/mnt/shared/prismabuild-fleet/pb-queue"))
    parser.add_argument("--listen", action="append",
                        help="bind address; repeat for each private link (default loopback)")
    parser.add_argument("--port", type=int, default=queue_bell.DEFAULT_PORT)
    args = parser.parse_args(argv)
    if not 1 <= args.port <= 65535:
        parser.error("--port must be between 1 and 65535")
    try:
        with role_session.role_singleton(_ENTRYPOINT):
            with role_session.stop_requests(signal.SIGTERM, signal.SIGINT) as stop:
                addresses = [(host, args.port) for host in (args.listen or ["127.0.0.1"])]
                with queue_notifier.BellService(args.queue_root, addresses, on_exit=stop.set):
                    stop.wait()
        return 0
    except (role_session.RoleLockHeld, role_session.RoleLockUnavailable) as exc:
        print(f"pbbell: singleton refused: {exc}", file=sys.stderr, flush=True)
        return role_session.ROLE_SINGLETON_HELD_EXIT
    except OSError as exc:
        print(f"pbbell: {exc}", file=sys.stderr, flush=True)
        return (role_session.ROLE_SINGLETON_HELD_EXIT
                if exc.errno == errno.EADDRINUSE else 1)


if __name__ == "__main__":
    raise SystemExit(main())
