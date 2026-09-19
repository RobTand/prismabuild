"""Presence states for boxes in ``tools/fleet/fleet_boxes.json`` (#606).

A roster box that is gone still vetoes every barrier publish: the attestation
preflight demands the target agent's version from *every* declared box, so one
box 69 h silent and unreachable (``wsl-gpu`` on 2026-09-18) blocks the whole
fleet, and the only exit is a hand-written rolling reason that installs
nothing on the missing box.

The fix is a declared presence, not a liveness inference.  Each box carries
an optional ``status``:

* ``"active"`` (the default when absent): the box is offered, attests, and
  counts toward every barrier quorum.
* ``"retired"``: the box is gone for good; it is excluded from the
  attestation preflight, the barrier roster, and placement (its supervisor
  converges its loops to zero and starts none).
* ``"offline"``: the same exclusion, but the box is expected back; the
  operator clears the state by publishing a roster without it.

A non-active box must say who declared it absent and when: ``status_reason``
(non-blank), ``status_by`` (non-blank) and ``status_unix`` (a finite
number) are all required.  An unknown ``status`` value, or a non-active box
without its provenance, refuses loudly wherever the roster is read -- an
ambiguous presence fails closed, never as "active".

This module is the single reader of those fields.  ``publish_runtime`` (the
barrier side) and ``supervise`` (the placement side) both call it rather than
keeping a second copy of the spelling.
"""

from __future__ import annotations

import math
from collections.abc import Mapping


ACTIVE = "active"
RETIRED = "retired"
OFFLINE = "offline"

#: Every presence a roster box may declare.  Absent means active.
STATUSES = (ACTIVE, RETIRED, OFFLINE)

#: States excluded from barrier quorums, attestation preflight and placement.
ABSENT = (RETIRED, OFFLINE)


class RosterPresenceError(ValueError):
    """A roster box whose presence cannot be established."""


def box_status(key: str, entry: object) -> tuple[str, dict[str, object]]:
    """The presence of roster box ``key``, and its declaration detail.

    Returns ``(status, detail)`` where ``detail`` carries ``reason``, ``by``
    and ``unix`` for a non-active box and is empty for an active one.
    Raises :class:`RosterPresenceError` for an unknown status or a
    non-active box without full provenance -- the caller turns that into the
    refusal its own layer owes (``SystemExit`` for the CLIs).
    """

    status: object = ACTIVE
    if isinstance(entry, Mapping):
        status = entry.get("status", ACTIVE)
    if status not in STATUSES:
        raise RosterPresenceError(
            f"roster box {key!r} declares unknown status {status!r}; "
            f"expected one of {list(STATUSES)}"
        )
    if status == ACTIVE:
        return ACTIVE, {}
    assert isinstance(entry, Mapping)
    reason = entry.get("status_reason")
    by = entry.get("status_by")
    unix = entry.get("status_unix")
    problems = []
    if not isinstance(reason, str) or not reason.strip():
        problems.append("status_reason must be a nonblank string")
    if not isinstance(by, str) or not by.strip():
        problems.append("status_by must be a nonblank string")
    if (not isinstance(unix, (int, float)) or isinstance(unix, bool)
            or not math.isfinite(unix)):
        problems.append("status_unix must be a finite number")
    if problems:
        raise RosterPresenceError(
            f"roster box {key!r} declares status {status!r} without provenance: "
            + "; ".join(problems)
        )
    return str(status), {
        "reason": str(reason).strip(),
        "by": str(by).strip(),
        "unix": float(unix),
    }


def is_absent(key: str, entry: object) -> bool:
    """Whether roster box ``key`` is declared absent (retired or offline)."""

    status, _ = box_status(key, entry)
    return status in ABSENT


def describe_absent(key: str, detail: Mapping[str, object]) -> str:
    """One legible line naming who declared the absence and when."""

    return (f"{key}: declared absent ({detail.get('reason')}) "
            f"by {detail.get('by')} at {detail.get('unix')}")
