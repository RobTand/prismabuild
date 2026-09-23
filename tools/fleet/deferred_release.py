"""Release deferred consumers once their producers commit (#913).

``pbrun --after PRODUCER:TEMPLATE_ID`` files a consumer before its producer
has run.  The submission freezes everything the submitter's checkout decides
and files it under ``pb-queue/deferred/``; what depends on the producer's
bytes -- the data manifest, the key, the residency window -- does not exist
yet.  This module is the tiers loop's half: once per cycle it looks at every
unreleased submission, and releases the ones whose producers have succeeded:
it resolves the batches each producer's successful attempt committed, seals
the consumer over exactly those, and publishes it as ``pbrun`` would have.

The tick is bounded, contained and quiet:

* **Bounded.**  It stops starting releases once the cycle's deadline passes
  or it has released ``MAX_RELEASES_PER_CYCLE``, and leaves the rest for the
  next cycle.  A release reads PB's own records and ``lstat``s the committed
  origins; it never reads or hashes payload bytes.  The one blob it hashes is
  the consumer's merged manifest, which it writes.
* **Contained.**  A submission that cannot be read, resolved or released is
  reported and kept for an operator.  Nothing a record says ends the loop.
* **Quiet.**  A consumer whose producer is still queued, running or itself
  deferred waits without a line of its own.  A hold worth a person's
  attention -- a failed or withdrawn producer, a generation that is gone --
  prints once per change.  While anything is unreleased, the
  tick prints one summary line per cycle with its wall time; with nothing
  unreleased it prints nothing.

Two generations meet in a release.  The loop's own code seals, so a loop
that is not the published generation (read from the ``repo`` link) releases
nothing and reports it; it restarts on the published generation at its next
cycle.  The generation sealed *into* is the template's: a consumer frozen by
``pbrun --after`` runs under the generation that froze it, exactly as an
ordinary submission sealed just before a publish does.  When that is an
older retained generation, its wrapper must match its receipt
(``pbrun.verify_retained_wrapper``, the ``--as-sealed-by`` check); if it
does not, the consumer is held and reported, never sealed into another.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
import contextlib
import hashlib
import io
import json
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve(strict=True).parent))
import pbrun  # noqa: E402  (puts the generation's src/ on sys.path)

from prismabuild import action_edges  # noqa: E402
from prismabuild import core as pb  # noqa: E402
from prismabuild import pool  # noqa: E402
from prismabuild import produced_output  # noqa: E402

RELEASED_EVENT = action_edges.RELEASED_EVENT
HELD_EVENT = "deferred-held"
REFUSED_EVENT = "deferred-release-refused"
TICK_EVENT = "deferred-release-tick"

#: The most consumers one cycle releases.  Each release can file a residency
#: plan whose first phase the *next* cycle publishes, so the cap bounds how
#: much new window work one burst hands the cycle after it.  The deadline
#: bounds this cycle; the cap bounds the next.
MAX_RELEASES_PER_CYCLE = 8

#: Producer states that wait without a report: the producer may still
#: succeed.  Every other state that is not ``done`` is a hold to report.
_QUIET_STATES = frozenset({"live", "pending"})

#: What a record or a release can raise that the tick reports and keeps.
#: ``SystemExit`` is ``pbrun``'s refusal: its sealing and publication helpers
#: refuse by raising it with the reason as its message.
_CONTAINED = (action_edges.ActionEdgeError, produced_output.ProducedOutputError,
              pool.PoolContractError, pb.PrismaBuildError, OSError, ValueError,
              KeyError, TypeError, SystemExit)

#: The last report printed for each pending id, as a digest.  Submissions are
#: immutable, so the memo lives in this process: after a restart a standing
#: hold prints once more.
_REPORTED: dict[str, str] = {}


def _report_once(memo_key: str, event: dict[str, object]
                 ) -> dict[str, object] | None:
    signature = hashlib.sha256(json.dumps(
        event, sort_keys=True, default=str).encode()).hexdigest()
    if _REPORTED.get(memo_key) == signature:
        return None
    _REPORTED[memo_key] = signature
    return event


def published_runtime_root() -> Path:
    """The generation the ``repo`` link names now."""

    return (pbrun.SH / "repo").resolve(strict=True)


def decide(queue, record: Mapping[str, object]) -> dict[str, object]:
    """Whether one submission can be released now, and against what.

    ``{"state": "release", "producers": [...]}`` when every edge resolves to
    a producer attempt that executed, each with ``{producer, key, nonce,
    template_id, path}``.  Otherwise ``{"state": "wait"}`` while every
    unresolved edge may still succeed, or ``{"state": "hold", "edges":
    [...]}`` naming each edge that will not without an operator.
    """

    resolved: list[dict[str, object]] = []
    held: list[dict[str, object]] = []
    waiting = False
    for edge in record["edges"]:                              # type: ignore[union-attr]
        answer = action_edges.resolve_producer(
            queue, str(edge["producer"]), str(edge["kind"]),
            str(edge["template_id"]))
        state = str(answer["state"])
        if state == "done":
            resolved.append({"producer": edge["producer"],
                             "key": answer["key"], "nonce": answer["nonce"],
                             "template_id": edge["template_id"],
                             "path": answer["path"]})
        elif state in _QUIET_STATES:
            waiting = True
        else:
            held.append({"producer": edge["producer"],
                         "template_id": edge["template_id"], "state": state,
                         "key": answer["key"], "path": answer["path"]})
    if held:
        return {"state": "hold", "edges": held}
    if waiting:
        return {"state": "wait"}
    return {"state": "release", "producers": resolved}


def release_tick(queue, *, deadline: float | None = None,
                 max_releases: int | None = None,
                 clock: Callable[[], float] = time.monotonic
                 ) -> list[dict[str, object]]:
    """One cycle's releases; the events to log.

    ``deadline`` is a ``clock()`` reading after which no release starts; the
    tiers loop passes the end of the current cycle's interval.  ``None``
    means no deadline, for one-shot callers.  ``max_releases`` defaults to
    ``MAX_RELEASES_PER_CYCLE``, read at the call.
    """

    if max_releases is None:
        max_releases = MAX_RELEASES_PER_CYCLE
    started = clock()
    events: list[dict[str, object]] = []
    try:
        pending = action_edges.unreleased_ids(queue.root)
    except OSError as exc:
        event = _report_once("", {"event": REFUSED_EVENT,
                                  "reason": f"deferred-unlistable: {exc}"})
        return [event] if event is not None else []
    if not pending:
        return events
    try:
        published_root = published_runtime_root()
        loop_root = pbrun.RUNTIME_ROOT.resolve(strict=True)
    except OSError as exc:
        event = _report_once("", {"event": REFUSED_EVENT,
                                  "reason": f"published-runtime-unreadable: {exc}"})
        return [event] if event is not None else []
    if loop_root != published_root:
        # This loop's code is not the published generation's, so it cannot
        # say it seals for that generation's contract.  It releases nothing;
        # the loop restarts on the published generation at its next cycle.
        event = _report_once("", {
            "event": REFUSED_EVENT, "reason": "loop-runtime-is-not-published",
            "loop_runtime": str(loop_root), "published_runtime": str(published_root),
            "pending": len(pending)})
        return [event] if event is not None else []
    _REPORTED.pop("", None)
    released = held = waiting = superseded = 0
    carried: list[str] = []
    for pending_id in pending:
        if released >= max_releases or (deadline is not None
                                        and clock() >= deadline):
            carried.append(pending_id)
            continue
        notices = io.StringIO()
        try:
            record = action_edges.read_deferred(queue.root, pending_id)
            if record is None:
                continue
            pinned = action_edges.read_release(queue.root, pending_id)
            if pinned is None:
                if action_edges.read_supersession(queue.root, pending_id) is not None:
                    # Replaced before it was released: edges follow the
                    # successor, and this one is never published.
                    superseded += 1
                    _REPORTED.pop(pending_id, None)
                    continue
                decision = decide(queue, record)
                if decision["state"] == "wait":
                    waiting += 1
                    _REPORTED.pop(pending_id, None)
                    continue
                if decision["state"] == "hold":
                    held += 1
                    event = _report_once(pending_id, {
                        "event": HELD_EVENT, "pending_id": pending_id,
                        "reason": "producer-will-not-succeed",
                        "edges": decision["edges"]})
                    if event is not None:
                        events.append(event)
                    continue
                producers = decision["producers"]
            else:
                # A pinned release is resumed as pinned, whatever became of
                # its producers or the runtime since: its key is decided.
                producers = ()
            # pbrun's notices go to stderr; the tier log is JSON lines, so
            # they travel inside this release's event instead.
            with contextlib.redirect_stderr(notices):
                event = pbrun.release_deferred(
                    queue, pending_id, record, producers=producers)
        except action_edges.RuntimeGenerationUnavailable as exc:
            # Never sealed into another generation instead: held, and the
            # remedy is a resubmission, which freezes under today's.
            held += 1
            event = _report_once(pending_id, {
                "event": HELD_EVENT, "pending_id": pending_id,
                "reason": "runtime-generation-unavailable", "detail": str(exc),
                "remedy": f"resubmit with --supersedes {pending_id}"})
            if event is not None:
                events.append(event)
            continue
        except _CONTAINED as exc:
            held += 1
            refusal = {"event": REFUSED_EVENT, "pending_id": pending_id,
                       "reason": f"{type(exc).__name__}: {exc}"}
            if notices.getvalue():
                refusal["notices"] = notices.getvalue().splitlines()
            event = _report_once(pending_id, refusal)
            if event is not None:
                events.append(event)
            continue
        released += 1
        _REPORTED.pop(pending_id, None)
        if notices.getvalue():
            event = {**event, "notices": notices.getvalue().splitlines()}
        events.append(event)
    events.append({"event": TICK_EVENT, "pending": len(pending),
                   "released": released, "waiting": waiting, "held": held,
                   "superseded": superseded, "carried": len(carried),
                   "elapsed_s": round(clock() - started, 6)})
    return events


__all__ = [
    "HELD_EVENT", "MAX_RELEASES_PER_CYCLE", "REFUSED_EVENT", "RELEASED_EVENT",
    "TICK_EVENT", "decide", "published_runtime_root", "release_tick",
]
