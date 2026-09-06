#!/usr/bin/env python3
"""Retire a worker offer whose box no longer answers to that name.

A worker offer is a claim about a box, refreshed by that box, and it expires:
``PoolQueue.offers`` believes nothing older than ``OFFER_TIMEOUT_S``.  That is
enough for scheduling and not enough for an operator, because the file stays
in ``workers/`` forever.  Every tool that lists the fleet lists it, so a box
renamed months ago keeps a seat at the table, and the reader has to know the
history to know which rows are real.

Before this there was no supported way to take one out.  A record was moved
aside by hand instead, correctly and reversibly, and that is the shape this
tool makes repeatable rather than a new capability.  It is a **rename**, into
``workers-retired/`` beside ``workers/``, which no reader of the offer
directory globs; ``--restore`` puts it back.  Nothing is deleted, because the
one thing that makes a hand-move safe is that it can be undone by the person
who finds out it was wrong.

**The refusal it exists for is retiring a name a loop is still announcing
under.**  ``worker_loop`` re-reads the hostname every poll and announces under
whatever it reads, so "this name is dead" is a claim about the future, and the
only evidence for it is that nothing has refreshed the record.  Three checks,
and each is a different way for the name to still be in use:

* the record is younger than ``LEASE_TIMEOUT_S`` -- a loop wrote it recently,
  so it is announcing now;
* an item in ``claimed/`` names the host -- work is out on this name, and its
  lease is what a reaper will match against;
* the host's ledger holds tokens -- capacity is reserved under this name.

The lease timeout is the threshold rather than the offer timeout, deliberately.
The offer timeout is how long a *scheduler* believes a claim, and a box two
minutes quiet is a box between polls.  The lease timeout is how long the pool
waits before it will take work away from a host, and retiring a record is a
statement of the same kind: this name is not coming back.  Using the shorter
one would retire a healthy box that missed three polls under load.

**The read and the rename are not one operation, so the rename is the read.**
A loop can refresh the record between the check and the move, and on a shared
filesystem it will eventually.  So the sequence is: read, check, rename, then
re-read ``announced_unix`` **from the moved file** and put it back if it does
not match the value the checks were made against.  The moved file is the only
copy a refresh cannot have reached -- a refresh writes to ``workers/<host>.
json`` -- so a mismatch there means the refresh landed before the rename and
the checks were made against bytes that are no longer current.  Checking the
source path again instead would answer a different question, and answer it
about a file the loop is free to recreate.

If a loop announces *after* the rename, the record simply comes back and the
retirement did nothing durable.  That is reported and exits non-zero rather
than being silently called success: the operator asked for a name to stop
appearing, and it is still appearing.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve(strict=True).parent))
from runtime_paths import generation_root  # noqa: E402
RUNTIME_ROOT = generation_root(__file__)
sys.path.insert(0, str(RUNTIME_ROOT / "src"))
from prismabuild import pool  # noqa: E402

#: Beside ``workers/`` rather than inside it, because ``offers()`` globs
#: ``workers/*.json`` and a subdirectory there would be one ``rglob`` away from
#: coming back to life.
RETIRED = "workers-retired"


def _read(path: Path) -> dict | None:
    try:
        text = path.read_text()
    except FileNotFoundError:
        return None
    except OSError:
        # A stale NFS handle is the shared filesystem's way of saying the file
        # moved while we were reading it, which is the same answer as absent.
        return None
    try:
        record = json.loads(text)
    except ValueError:
        return None
    return record if isinstance(record, dict) else None


def _announced(record: dict | None) -> float | None:
    if record is None:
        return None
    value = record.get("announced_unix")
    return float(value) if isinstance(value, (int, float)) else None


def claim_holding(queue, host: str) -> str | None:
    """The action key of a claimed item that names ``host``, if any."""

    directory = queue.dir(pool.CLAIMED)
    if not directory.is_dir():
        return None
    for path in sorted(directory.glob("*.json")):
        record = _read(path)
        if record is None:
            continue
        holder = record.get("claimed_host") or record.get("host")
        if str(holder) == host:
            return path.stem
    return None


def refusals(queue, host: str, record: dict, *, now: float) -> list[str]:
    """Every reason this name is still in use.  Empty means retirable."""

    reasons: list[str] = []
    announced = _announced(record)
    if announced is None:
        # A record with no timestamp cannot be shown to be quiet, and the
        # safe reading of "cannot be shown" is not "is".
        reasons.append("the record carries no announced_unix, so nothing "
                       "about it can be shown to be stale")
    else:
        age = now - announced
        if age < pool.LEASE_TIMEOUT_S:
            reasons.append(
                f"the record was refreshed {age:.0f}s ago, inside the "
                f"{pool.LEASE_TIMEOUT_S:.0f}s lease timeout: a loop is "
                "announcing under this name")
    key = claim_holding(queue, host)
    if key is not None:
        reasons.append(f"claimed/{key} names this host, so work is out on it")
    held = queue.ledger(host).held_keys()
    if held:
        reasons.append(
            f"the host ledger holds {len(held)} reservation(s): "
            + ", ".join(entry[:12] for entry in held[:4]))
    return reasons


def _archive_existing(destination: Path) -> Path | None:
    """Move an older tombstone out of the way, keyed by its own timestamp.

    A name can be retired more than once: the box comes back, announces, is
    renamed again, and goes quiet again.  Refusing the second retirement
    because the first tombstone is in the way sends the operator back to the
    hand-move this tool exists to replace, and ``--restore`` cannot get them
    out either -- it refuses while the live record is there, which in that
    scenario it is.  So the older tombstone is filed under the moment it last
    announced, which is the only thing that distinguishes it from the newer
    one.  Still a rename: nothing is deleted, and the history reads in order.
    """

    if not destination.exists():
        return None
    stamp = _announced(_read(destination))
    label = f"{int(stamp)}" if stamp is not None else "unknown"
    archived = destination.with_name(f"{destination.stem}.{label}.json")
    suffix = 1
    while archived.exists():
        archived = destination.with_name(f"{destination.stem}.{label}.{suffix}.json")
        suffix += 1
    os.rename(destination, archived)
    return archived


def retire(queue, host: str, *, now: float) -> tuple[int, str]:
    """Move the record aside, and put it back if the checks went stale."""

    source = queue.root / pool.WORKERS / f"{host}.json"
    record = _read(source)
    if record is None:
        return 1, f"no worker record for {host}"
    reasons = refusals(queue, host, record, now=now)
    if reasons:
        return 1, f"refused to retire {host}:\n  " + "\n  ".join(reasons)
    checked = _announced(record)
    destination = queue.root / RETIRED / f"{host}.json"
    destination.parent.mkdir(parents=True, exist_ok=True)
    archived = _archive_existing(destination)
    os.rename(source, destination)
    # The moved file is the copy a refresh cannot have reached.  If its
    # timestamp is not the one the checks were made against, the refresh
    # landed first and those checks were made against bytes that are gone.
    moved = _announced(_read(destination))
    if moved != checked:
        os.rename(destination, source)
        return 1, (f"refused to retire {host}: the record changed between the "
                   "check and the move, so a loop is announcing under this "
                   "name; put back unchanged")
    note = f" (the previous tombstone is now {archived.name})" if archived else ""
    if source.exists():
        return 1, (f"retired {host} to {destination}{note}, and the name was "
                   "announced again immediately: a loop is live on this box. "
                   "The record is back in workers/ and the retirement did "
                   "nothing durable.")
    return 0, f"retired {host} to {destination}{note}"


def restore(queue, host: str) -> tuple[int, str]:
    source = queue.root / RETIRED / f"{host}.json"
    destination = queue.root / pool.WORKERS / f"{host}.json"
    if not source.exists():
        return 1, f"no retired record for {host}"
    if destination.exists():
        return 1, (f"{destination} exists: the name is announcing again on "
                   "its own, so there is nothing to restore")
    destination.parent.mkdir(parents=True, exist_ok=True)
    os.rename(source, destination)
    return 0, f"restored {host} to {destination}"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__.split("\n\n")[0],
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "host", help="the worker name to retire, as it appears in workers/")
    parser.add_argument(
        "--root", default=None,
        help="queue root to act on (default: the fleet's pool root)")
    parser.add_argument(
        "--apply", action="store_true",
        help="actually move the record; without it the checks are run and "
             "the outcome is printed")
    parser.add_argument(
        "--restore", action="store_true",
        help="move a retired record back into workers/ instead")
    args = parser.parse_args(argv)

    queue = pool.PoolQueue(args.root) if args.root else pool.PoolQueue()
    if args.restore:
        code, message = restore(queue, args.host)
        print(message)
        return code

    source = queue.root / pool.WORKERS / f"{args.host}.json"
    record = _read(source)
    if record is None:
        print(f"no worker record for {args.host}")
        return 1
    now = time.time()
    reasons = refusals(queue, args.host, record, now=now)
    if reasons:
        print(f"refused to retire {args.host}:\n  " + "\n  ".join(reasons))
        return 1
    age = now - (_announced(record) or now)
    if not args.apply:
        print(f"would retire {args.host} (quiet for {age:.0f}s, no claim, no "
              f"reservation) to {queue.root / RETIRED / f'{args.host}.json'}\n"
              "re-run with --apply")
        return 0
    code, message = retire(queue, args.host, now=now)
    print(message)
    return code


if __name__ == "__main__":
    raise SystemExit(main())
