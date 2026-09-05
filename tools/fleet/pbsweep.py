#!/usr/bin/env python3
"""File the SLURM endings nobody asked about.

The lane files an ending when somebody polls for that key.  ``pbrun`` polls
because it is holding the submission open, ``pbwait`` because an operator
typed the key.  A job that ends while nothing is watching leaves its verdict
in the controller's accounting and never becomes a record: a detached
submission whose waiter died is the ordinary way to produce one, and so is a
``pool_reset`` re-submission, which detaches on purpose.

Every reader of ``pb-queue`` then sees a key with no ending, and nothing else
fills the gap: ``pbwait`` would file it, but only for a key somebody names,
and the keys that need it are exactly the ones nobody is holding.  This is
the process that asks about all of them.  It walks the lane root, compares
each recorded submission against the endings on disk, and files the ones that
are missing.

Three properties are the point.

*It is not a second filing path.*  Every ending it files goes through
``slurm_lane.resume``, the same call ``pbwait`` makes for one key, so a swept
record is byte-identical to a polled one.  A reconcile that wrote its own
record shape would be the defect the pull queue had between ``finish`` and
``reap_stale``.

*It never invents a verdict.*  An ending is filed only from a real recorded
one: the controller's terminal state, an operator's withdrawal marker, or a
receipt in the CAS.  A job the controller knows nothing about, with no
receipt behind it, is reported ``no-verdict`` and files nothing.  Not knowing
is not the same as knowing it failed, and the record would stand.

*Reporting is the default and filing is a flag.*  The report asks the
controller and writes nothing at all, so it is safe against a live fleet::

    pbsweep.py                       # what is missing, and what would be filed
    pbsweep.py --apply               # file it
    pbsweep.py --json                # the same rows, for a script
    pbsweep.py --apply <key> <key>   # reconcile only these

It is safe to run beside a ``pbrun`` polling the same key.  Both sides reach
``slurm_lane.publish_outcome``, which links first at an empty name and
refuses a record of its own generation, so the two produce one record between
them and neither fails.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from collections.abc import Mapping, Sequence

sys.path.insert(0, str(Path(__file__).resolve(strict=True).parent))
from runtime_paths import generation_root  # noqa: E402

RUNTIME_ROOT = generation_root(__file__)
sys.path.insert(0, str(RUNTIME_ROOT / "src"))
from prismabuild import core as pb, slurm_lane as sl  # noqa: E402

#: Where the fleet keeps its store.  The same spelling ``pbrun``, ``pbstatus``
#: and ``pool_reset`` use, and read when the parser is built rather than when
#: this module is defined, so a test that repoints it is obeyed.
SH = Path("/mnt/shared/prismabuild-fleet")

#: How wide a key prints.  Twelve characters is what every fleet log line
#: shows, so it is what an operator has to compare against.
KEY_WIDTH = 12

#: The dispositions that mean an operator has something to do.  A sweep that
#: found nothing to reconcile exits 0; one that left an ending unfiled because
#: it could not establish a verdict says so through its exit status, so a cron
#: entry can notice without parsing the table.
UNRESOLVED = frozenset({sl.NO_VERDICT, sl.UNREACHABLE, sl.NO_ACTION,
                        sl.UNRECORDED})

#: What this exits with when some key could not be resolved.  Distinct from 1,
#: which every fleet tool keeps for "the work failed": nothing here runs work.
UNRESOLVED_EXIT = 3

_COLUMNS = ("KEY", "JOB", "DISPOSITION", "STATUS", "NOTE")


def build_parser() -> argparse.ArgumentParser:
    """This tool's flags, with the roots read when the parser is built."""

    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument(
        "keys", nargs="*",
        help="reconcile only these action keys, named in full (default: every "
             "key the lane root records)")
    ap.add_argument(
        "--apply", action="store_true",
        help="file the missing endings; the default only reports")
    ap.add_argument(
        "--json", action="store_true",
        help="print one JSON object with the rows and the counts, and nothing "
             "else")
    ap.add_argument(
        "--lane-root", default=None,
        help="the SLURM lane root to reconcile (default: "
             f"${sl.LANE_ROOT_ENV}, else {sl.DEFAULT_LANE_ROOT})")
    ap.add_argument(
        "--queue-root", default=str(SH / "pb-queue"),
        help="the queue root holding done/, failed/ and withdrawn/ (default "
             f"{SH / 'pb-queue'})")
    ap.add_argument(
        "--cas-root", default=str(SH / "cas"),
        help=f"the CAS holding the receipts and the sealed requests (default "
             f"{SH / 'cas'})")
    ap.add_argument("--sacct", default="sacct", help=argparse.SUPPRESS)
    ap.add_argument("--scontrol", default="scontrol", help=argparse.SUPPRESS)
    ap.add_argument("--squeue", default="squeue", help=argparse.SUPPRESS)
    ap.add_argument("--sstat", default="sstat", help=argparse.SUPPRESS)
    return ap


def table_lines(rows: Sequence[Mapping[str, object]]) -> list[str]:
    """The reconcile table, one line per key that has something to say.

    Keys whose ending is already filed are counted and not listed.  A live
    lane root holds a directory per action the fleet has ever run, and a table
    that printed a row for each of them would bury the handful that need an
    operator.
    """

    body = [
        (
            str(row.get("action_key") or "")[:KEY_WIDTH],
            str(row.get("job_id") or "-"),
            str(row.get("disposition") or ""),
            str(row.get("status") or "-"),
            str(row.get("note") or ""),
        )
        for row in rows
        if row.get("disposition") not in (sl.ALREADY_FILED, sl.SUPERSEDED)
    ]
    if not body:
        return ["nothing to reconcile: every recorded submission has an ending"]
    widths = [len(head) for head in _COLUMNS]
    for line in body:
        for column, cell in enumerate(line):
            widths[column] = max(widths[column], len(cell))
    lines = ["  ".join(
        head.ljust(widths[column]) for column, head in enumerate(_COLUMNS)
    ).rstrip()]
    for line in body:
        lines.append("  ".join(
            cell.ljust(widths[column]) for column, cell in enumerate(line)
        ).rstrip())
    return lines


def counts(rows: Sequence[Mapping[str, object]]) -> dict[str, int]:
    """How many keys landed in each disposition."""

    tally: dict[str, int] = {}
    for row in rows:
        name = str(row.get("disposition") or "")
        tally[name] = tally.get(name, 0) + 1
    return tally


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    rows = sl.sweep(
        cas=pb.PrismaBuildCAS(Path(args.cas_root)),
        queue_root=Path(args.queue_root),
        root=args.lane_root,
        apply=args.apply,
        keys=args.keys or None,
        sacct=args.sacct,
        scontrol=args.scontrol,
        squeue=args.squeue,
        sstat=args.sstat,
    )
    tally = counts(rows)
    if args.json:
        print(json.dumps({
            "schema": "prismabuild.pbsweep.v1",
            "applied": bool(args.apply),
            "rows": list(rows),
            "counts": tally,
        }, sort_keys=True, indent=1))
    else:
        print(f"{len(rows)} recorded submissions, "
              f"{tally.get(sl.ALREADY_FILED, 0)} already filed")
        print("\n".join(table_lines(rows)))
        if not args.apply and tally.get(sl.MISSING):
            print(f"\n{tally[sl.MISSING]} ending(s) would be filed; "
                  f"re-run with --apply")
    return UNRESOLVED_EXIT if any(
        row.get("disposition") in UNRESOLVED for row in rows
    ) else 0


if __name__ == "__main__":
    raise SystemExit(main())
