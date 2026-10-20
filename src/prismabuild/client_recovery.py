"""Read-only recovery of one named generation under the original client deadline."""
from __future__ import annotations

import argparse
import math
from pathlib import Path
import re
import sys
import time

from . import action_client as client, action_wait, client_channel, pool


def command(events: client_channel.ClientEventResult, *, deadline: float,
            python: str | None = None) -> list[str] | None:
    identity = events.identity
    if (not isinstance(identity, client.PoolClientIdentity)
            or not events.may_recover(deadline=deadline, now=time.monotonic())):
        return None
    return observe_command(identity, deadline=deadline, python=python)


def observe_command(identity: client.PoolClientIdentity, *, deadline: float,
                    python: str | None = None) -> list[str]:
    """The read-only observer of one published pool generation, until ``deadline``.

    A detached publisher (``pbtest`` since pb-identity) waits on its own
    publication with exactly this program, so its wait is the recovery wait:
    the same reader, notices and rendering as ``pbrun``'s attached tail.
    """
    return [sys.executable if python is None else python, '-m', 'prismabuild.client_recovery',
        '--action-key', identity.action_key, '--published-unix', str(identity.published_unix),
        '--deadline', str(deadline), '--queue-root', str(identity.queue_root),
        '--cas-root', str(identity.cas_root)]


def _recovery_generation(value: str) -> float:
    result = float(value)
    if not math.isfinite(result):
        raise argparse.ArgumentTypeError('the deadline and generation must be finite')
    return result


def _recovery_deadline(value: str) -> float:
    result = float(value)
    if math.isnan(result):
        raise argparse.ArgumentTypeError('the deadline cannot be NaN')
    return result


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--action-key', required=True)
    parser.add_argument('--published-unix', type=_recovery_generation, required=True)
    parser.add_argument('--deadline', type=_recovery_deadline, required=True)
    parser.add_argument('--queue-root', type=Path, required=True)
    parser.add_argument('--cas-root', type=Path, required=True)
    parser.add_argument('--client-events-fd', type=int)
    args = parser.parse_args(argv)
    if re.fullmatch('[a-f0-9]{64}', args.action_key) is None:
        parser.error('recovery requires the exact action key')
    if not args.queue_root.is_absolute() or not args.cas_root.is_absolute():
        parser.error('recovery requires configured absolute store roots')
    identity = client.PoolClientIdentity(args.action_key, args.published_unix,
        args.queue_root, args.cas_root, None, client.ClientArrival.ATTACHED)
    writer = (client_channel.ClientEventWriter(args.client_events_fd, expected_identity=identity)
              if args.client_events_fd is not None else None)
    report = client.ClientReport(identity=identity, on_event=writer.send if writer is not None else None)
    try:
        with action_wait.client_readers():
            if time.monotonic() >= args.deadline:
                return report.finish(client.ClientPatienceExpired(args.action_key, args.published_unix))
            outcome = client.await_pool_outcome(pool.PoolQueue(args.queue_root), args.action_key,
                generation=args.published_unix, patience=action_wait.WaitUntil(args.deadline),
                notice=lambda event: client.report_pool_notice(args.action_key, event))
            client.render_pool_outcome(outcome)
            report.conclude(outcome.ending)
            return outcome.ending.exit_code
    finally:
        if writer is not None:
            writer.close()


if __name__ == '__main__':
    raise SystemExit(main())
