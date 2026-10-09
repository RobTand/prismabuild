"""A person's recorded acceptance of the lifetime contract's assumption (#1429).

The payload stop, termination, cleanup, scope settlement and the return of the
host tokens take the time that the kernel, the broker, the disk and the shared
mount take. No component bounds that time, and PrismaBuild never returns tokens
on a timer. A deadline is therefore a release bound only for someone who
accepts :data:`~prismabuild.lifetime_fence.ASSUMPTION`: the bound holds while
those answers are prompt, and a call that never returns delays the measurement
that waits. That decision puts the fleet's measurements at risk. A change set
cannot make it and an agent cannot waive it. A person makes it.

Admission reads the decision from the queue root. Without a valid record each
:data:`~prismabuild.lifetime_fence.PROMPT_PHASES` phase reads UNKNOWN, no
candidate reads a finite bound, and no timed backfill runs
(:func:`prismabuild._measurement_reservation.candidate_release_verdict`). The
record holds the exact statement accepted, who accepted it and the authority
they cite. Another statement (another reserve, other words) is another
decision, so an older record stops applying. ``revoke`` removes the record, and
admission reads UNKNOWN again at once. Neither command touches a running
attempt or a held token.

    python -m prismabuild.lifetime_acceptance accept --by NAME --authority REF
    python -m prismabuild.lifetime_acceptance status
    python -m prismabuild.lifetime_acceptance revoke
"""
from __future__ import annotations

import argparse
from collections.abc import Mapping
import json
import math
import os
from pathlib import Path
import sys
import time

from . import core as pb
from . import lifetime_fence, materialize

#: Versioned record a person's acceptance is filed under.
ACCEPTANCE_SCHEMA_V1 = "prismabuild.lifetime_acceptance.v1"
#: Beside ``filesystem-floor/`` and the other queue-root directories.
ACCEPTANCE_DIR = "lifetime-fence"
ACCEPTANCE_FILE = "acceptance.json"
#: A record is a few hundred bytes; a larger file is not one.
MAX_RECORD_BYTES = 16 * 1024
_FIELDS = frozenset({
    "schema", "contract", "assumption", "accepted_by", "authority", "accepted_unix"})


def acceptance_path(queue_root: str | os.PathLike[str]) -> Path:
    return Path(queue_root) / ACCEPTANCE_DIR / ACCEPTANCE_FILE


def _problem(record: object) -> str | None:
    """Why ``record`` is not an acceptance of the current assumption, or ``None``."""

    if not isinstance(record, Mapping) or set(record) != _FIELDS:
        return "the record does not hold exactly the acceptance fields"
    if record["schema"] != ACCEPTANCE_SCHEMA_V1:
        return "the record is another schema"
    if record["contract"] != lifetime_fence.LIFETIME_SCHEMA_V1:
        return "the record accepts another contract"
    if record["assumption"] != lifetime_fence.ASSUMPTION:
        return "the record accepts another statement than the current assumption"
    for field in ("accepted_by", "authority"):
        value = record[field]
        if not isinstance(value, str) or not value.strip():
            return f"the record names no {field}"
    stamp = record["accepted_unix"]
    if (type(stamp) not in (int, float) or isinstance(stamp, bool)
            or not math.isfinite(float(stamp))):
        return "the record carries no acceptance time"
    return None


def current_acceptance(
    queue_root: str | os.PathLike[str],
) -> tuple[dict[str, object] | None, str | None]:
    """The queue's acceptance record, or ``(None, why)``.

    Fail closed: a record that is missing, unreadable, reached through a link,
    oversized, not strict JSON or not an acceptance of the current assumption
    is no acceptance.
    """

    path = acceptance_path(queue_root)
    try:
        raw = pb._read_regular_file_nofollow(
            path, where="lifetime acceptance record",
            max_bytes=MAX_RECORD_BYTES, replaced_leaf=True)
    except FileNotFoundError:
        return None, "no acceptance record"
    except (OSError, pb.PrismaBuildError) as exc:
        return None, f"the acceptance record cannot be read: {exc}"
    try:
        record = pb._decode_strict_json(raw, where="lifetime acceptance record")
    except pb.PrismaBuildError as exc:
        return None, f"the acceptance record is not strict JSON: {exc}"
    problem = _problem(record)
    if problem is not None:
        return None, problem
    assert isinstance(record, dict)
    return record, None


def assumption_accepted(queue_root: str | os.PathLike[str]) -> bool:
    """Whether a person has accepted the current assumption for this queue."""

    return current_acceptance(queue_root)[0] is not None


def _named(value: str, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must name a person or an explicit authority")
    return value.strip()


def record_acceptance(
    queue_root: str | os.PathLike[str], *, accepted_by: str, authority: str,
    now_unix: float | None = None,
) -> dict[str, object]:
    """File the acceptance of the current assumption, replacing any older one.

    ``authority`` is the explicit user instruction that allows the acceptance:
    a message, a comment or a decision record. An agent's own judgment is not
    an authority.
    """

    record = {
        "schema": ACCEPTANCE_SCHEMA_V1,
        "contract": lifetime_fence.LIFETIME_SCHEMA_V1,
        "assumption": lifetime_fence.ASSUMPTION,
        "accepted_by": _named(accepted_by, "accepted_by"),
        "authority": _named(authority, "authority"),
        "accepted_unix": time.time() if now_unix is None else float(now_unix),
    }
    problem = _problem(record)
    if problem is not None:
        raise ValueError(problem)
    materialize._write_json_atomic(
        acceptance_path(queue_root), record, text="canonical_lf")
    return record


def withdraw_acceptance(queue_root: str | os.PathLike[str]) -> bool:
    """Remove the record. True when there was one."""

    try:
        acceptance_path(queue_root).unlink()
    except FileNotFoundError:
        return False
    return True


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m prismabuild.lifetime_acceptance",
        description="Record, show or withdraw a person's acceptance of the "
                    "lifetime contract's assumption.")
    parser.add_argument("--queue", default="/mnt/shared/prismabuild-fleet/pb-queue")
    commands = parser.add_subparsers(dest="command", required=True)
    accepting = commands.add_parser(
        "accept", help="record the acceptance; admission then reads finite bounds")
    accepting.add_argument("--by", required=True, help="the person who accepts")
    accepting.add_argument(
        "--authority", required=True,
        help="the explicit user instruction that allows it (a message, a comment, "
             "a decision record); an agent's own judgment is not one")
    commands.add_parser("status", help="show the assumption and whether it is accepted")
    commands.add_parser("revoke", help="remove the record; admission reads UNKNOWN again")
    args = parser.parse_args(argv)
    queue = Path(args.queue)
    if args.command == "accept":
        try:
            result: dict[str, object] = {
                "accepted": True,
                "record": record_acceptance(
                    queue, accepted_by=args.by, authority=args.authority)}
        except ValueError as exc:
            parser.error(str(exc))
    elif args.command == "revoke":
        result = {"revoked": withdraw_acceptance(queue)}
    else:
        record, why = current_acceptance(queue)
        result = {"accepted": record is not None, "why_not": why, "record": record,
                  "path": str(acceptance_path(queue)),
                  "assumption": lifetime_fence.ASSUMPTION}
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
