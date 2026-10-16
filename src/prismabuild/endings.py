"""Pure ending questions over captured queue evidence."""
from __future__ import annotations

from collections.abc import Mapping, Sequence
import math
from pathlib import Path

_DONE = "done"
_FAILED = "failed"
_WITHDRAWN = "withdrawn"


def publication_generation(value: object) -> float | None:
    """A record's generation as a finite float, or ``None``.

    A huge integer is ``None``, never an ``OverflowError``: hostile durable
    input stays in the pool's fail-closed error vocabulary (#1446).
    """

    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    try:
        number = float(value)
    except OverflowError:
        return None
    return number if math.isfinite(number) else None



def resolve_current(
    readable: Mapping[str, tuple[Path, Mapping[str, object]]],
    unreadable: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    """How this key ended, by generation, over one captured record set.

    Pure: no directory or record is read here, so the result can only
    describe the capture it was handed.  ``done/`` and ``failed/`` are
    one slot each and a key is a content hash, so two generations'
    records can stand together: records filed before #1117 retired the
    earlier row at conclusion, a crash between the new terminal's write
    and the retired row's unlink, an archive write that failed, or a
    terminal filed by a path that never archived.  Readers that each
    walked the directories in their own order disagreed in exactly that
    state (GLM Stage B layer 037: ``pbwait`` read ``executed``, a
    ``failed/``-first census read dead).

    The rule is ordering over validated generations.  The unique newest
    finite ``published_unix`` among the readable candidates answers, and
    the other readable candidates are ``history``.  ``supersedes_terminal``
    is historical evidence the writer filed, not current authority: it
    names the generation a later record retired, and a slot it points
    into may since have been replaced by a newer one, so it never
    decides.  A CONTESTED key generations cannot order -- a missing or
    malformed generation, a tie at the maximum, a third unorderable
    candidate, or a record that could not be read -- is never read as
    success: a readable failure or cancellation stands, or ``state`` is
    ``None`` when even that cannot be named, and ``ambiguous`` is true.

    Nothing is moved, deleted or rewritten.  A lone record is the key's
    ending even when its generation is malformed or absent:
    ``pbrun.terminal_record`` already states that a record carrying no
    generation stands, because staleness cannot be proved of it and
    refusing it would hang a reader on the only account of what
    happened.  The conservative rule above applies to a contest, not to
    a sole record.  This is a point-in-time
    status rule; a waiter bound to one generation keeps
    ``pbrun.outcome_poll`` and ``archived_generation_outcomes``, which
    are exact to that generation.
    """

    def generation(state: str) -> float | None:
        # The one finite-generation owner: an integer past the binary64
        # range is an unorderable generation (``None``), never an escaped
        # ``OverflowError`` out of a status census (#1446).
        return publication_generation(readable[state][1].get("published_unix"))

    present = [state for state in (_DONE, _FAILED, _WITHDRAWN)
               if state in readable]
    unreadable_states = [str(entry["state"]) for entry in unreadable]
    coexisting = present + unreadable_states

    def result(state: str | None,
               *, ambiguous: bool = False) -> dict[str, object]:
        history = [
            {"state": other, "path": readable[other][0],
             "record": readable[other][1], "generation": generation(other)}
            for other in present if other != state
        ]
        if state is None:
            return {
                "state": None, "path": None, "record": None,
                "generation": None, "history": history,
                "unreadable": list(unreadable), "ambiguous": ambiguous,
                "coexisting": coexisting,
            }
        path, record = readable[state]
        return {
            "state": state, "path": path, "record": record,
            "generation": generation(state), "history": history,
            "unreadable": list(unreadable), "ambiguous": ambiguous,
            "coexisting": coexisting,
        }

    def failure() -> str | None:
        return next((state for state in (_FAILED, _WITHDRAWN)
                     if state in readable), None)

    if not coexisting:
        return result(None)
    if len(readable) == 1 and not unreadable:
        return result(present[0])

    orderable = [state for state in present if generation(state) is not None]
    if len(orderable) >= 2:
        generations = {state: generation(state) for state in orderable}
        newest = max(
            value for value in generations.values() if value is not None)
        winners = [state for state, value in generations.items()
                   if value == newest]
        if len(winners) == 1:
            winner = winners[0]
            if winner == _DONE and len(orderable) != len(coexisting):
                # An unorderable or unreadable sibling stands against
                # the success; a unique maximum over every candidate
                # does not.
                return result(failure(), ambiguous=True)
            return result(winner, ambiguous=bool(unreadable))
        failure_winner = next(
            (state for state in winners if state != _DONE), winners[0])
        return result(failure_winner, ambiguous=True)
    if len(orderable) == 1:
        only = orderable[0]
        if only == _DONE and len(coexisting) > 1:
            return result(failure(), ambiguous=True)
        return result(only, ambiguous=len(coexisting) > 1)
    return result(failure() or (present[0] if present else None),
                  ambiguous=len(coexisting) > 1)

