"""One atomic group reservation per prelaunch unit and tier (#1594).

The tier loop is the single writer for one tier. This module takes
the queue and plain values. It never reads the live census itself.

A group holds one unit's whole declared demand under one holder in
one acquire. Receipts under ``prelaunch-groups/<unit>.<tier12>/``
record intent before the take and the conclusion after it. The
ledger owns the tokens; receipts never do. The turn under
``prelaunch-turn/`` orders multi-tier units across tier loops.

States per (unit, tier): unreserved, acquiring, reserved, splitting,
done, short, overfull, unknown. Authority to publish a chunk needs
``committed.json`` with h+m+r equal to the demand.
"""
from __future__ import annotations

import os
import re
import socket
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping, Sequence

from . import pool
from . import storage_tiers
from ._gang import _link_new
from .digest_primitives import raw_sha256
from .materialize import _write_json_atomic

__all__ = [
    "GROUP_DIR", "TURN_DIR", "HOLDER_PREFIX",
    "GroupCensus", "ReconcileOutcome", "PublishOutcome", "TakeTurn",
    "tier_digest", "phase_digest", "holder_name", "group_dir",
    "file_intent", "census", "incremental_need_gib", "reconcile",
    "publish_chunk", "release_unit", "has_holdings",
    "turn_ticket", "current_turn", "take_turn", "record_reserved",
    "finish_turn",
]

#: Receipts for one unit on one tier live here, one directory per pair.
GROUP_DIR = "prelaunch-groups"
#: Turn tickets, epochs and reserved markers live here.
TURN_DIR = "prelaunch-turn"
#: Group holders start here, never with dots, never 64-hex.
HOLDER_PREFIX = "prelaunch-"

INTENT_SCHEMA = "prismabuild.prelaunch_group_intent.v1"
COMMITTED_SCHEMA = "prismabuild.prelaunch_group_committed.v1"
ROLLEDBACK_SCHEMA = "prismabuild.prelaunch_group_rolled_back.v1"
RELEASED_SCHEMA = "prismabuild.prelaunch_group_released.v1"
TICKET_SCHEMA = "prismabuild.prelaunch_turn_ticket.v1"
TURN_SCHEMA = "prismabuild.prelaunch_turn.v1"
RESERVED_SCHEMA = "prismabuild.prelaunch_turn_reserved.v1"
DONE_SCHEMA = "prismabuild.prelaunch_turn_done.v1"

_TIERS = "tickets"
_RESERVED_SUFFIX = ".json"


@dataclass
class GroupCensus:
    """One reading of a group's tokens, receipts and handles."""

    holder: str = ""
    demand_gib: int = 0
    #: Tokens under the holder.
    h: int = 0
    #: Tokens under the holder's private acquisition handles.
    p: int = 0
    #: Named funding tokens held under this group's chunk movers.
    m: int = 0
    #: Tokens the release function already returned for ended chunks.
    r: int = 0
    #: Receipt filename to parsed body, None when unreadable.
    receipts: dict[str, dict | None] = field(default_factory=dict)
    #: Claiming names of this holder no exact parse owns.
    unparsed: list[str] = field(default_factory=list)
    #: Parsed handles as (handle, usec, host, pid, token_count).
    handles: list[tuple[str, int, str, int, int]] = field(default_factory=list)
    #: Movers whose funding record would not read.
    funding_unknown: list[str] = field(default_factory=list)
    #: Receipts that would not read, with their reasons.
    unknown: list[str] = field(default_factory=list)


@dataclass
class ReconcileOutcome:
    """One rule-table pass: state, events, authority and the census."""

    state: str = "unknown"
    events: list[str] = field(default_factory=list)
    authority: bool = False
    census: GroupCensus | None = None


@dataclass
class PublishOutcome:
    """One chunk handoff: status, events, generation and moved count."""

    status: str = "deferred"
    events: list[str] = field(default_factory=list)
    generation: str | None = None
    moved: int = 0


@dataclass
class TakeTurn:
    """One turn creation attempt: epoch, winner, win flag and status."""

    epoch: int | None = None
    unit: str | None = None
    won: bool = False
    status: str = "unknown"


# ------------------------------------------------------------- names


def tier_digest(tier_id: str) -> str:
    """Twelve hex chars naming one tier in file names."""
    return raw_sha256(str(tier_id).encode())[:12]


def phase_digest(phase_names: Sequence[str]) -> str:
    """Twelve hex chars naming one declared phase set."""
    joined = "\x1f".join(str(name) for name in phase_names)
    return raw_sha256(joined.encode())[:12]


def holder_name(unit: str, tier_id: str, phase_names: Sequence[str]) -> str:
    """The deterministic group holder: dot-free by construction."""
    if not isinstance(unit, str) or not unit:
        raise ValueError("a group unit names a consumer or a gang group")
    if "." in unit[:16]:
        raise ValueError("a group unit never carries dots into its holder")
    return (f"{HOLDER_PREFIX}{unit[:16]}-"
            f"{tier_digest(tier_id)}-{phase_digest(phase_names)}")


def group_dir(queue: pool.PoolQueue, unit: str, tier_id: str) -> Path:
    """The receipt directory for one unit on one tier."""
    if (not isinstance(unit, str) or not unit or "/" in unit
            or unit in (".", "..") or len(unit) > 200):
        raise ValueError(f"a group unit must be a plain name, got {unit!r}")
    return queue.root / GROUP_DIR / f"{unit}.{tier_digest(tier_id)}"


# ------------------------------------------------------------- receipts


def _read_receipt(path: Path) -> tuple[str, dict | None, str | None]:
    """Read one receipt as absent, record or unknown with its reason."""
    try:
        raw = pool._read_json(path)
    except OSError as exc:
        return ("unknown", None, f"receipt unreadable: {exc!r}")
    except pool.PoolContractError as exc:
        return ("unknown", None, f"receipt unparsable: {exc}")
    if raw is None:
        try:
            present = path.exists()
        except OSError as exc:
            return ("unknown", None, f"receipt census unreadable: {exc!r}")
        if present:
            return ("unknown", None, "receipt present but empty")
        return ("absent", None, None)
    if not isinstance(raw, Mapping):
        return ("unknown", None, "receipt is not an object")
    return ("record", dict(raw), None)


def _standing_matches(path: Path, payload: Mapping[str, object],
                      keys: Sequence[str]) -> bool:
    """True when the filed record agrees on the semantic keys."""
    status, standing, _ = _read_receipt(path)
    if status != "record" or standing is None:
        return False
    return all(standing.get(key) == payload.get(key) for key in keys)


def _link_or_adopt(path: Path, payload: Mapping[str, object],
                   keys: Sequence[str]) -> str:
    """File no-clobber; adopt or refuse when a record already stands."""
    try:
        if _link_new(path, payload):
            return "filed"
    except OSError:
        return "unknown"
    if _standing_matches(path, payload, keys):
        return "standing"
    return "conflict"


def _check_chunks(chunks: object) -> list[dict]:
    """Validated intent chunk entries, or ValueError for junk."""
    if not isinstance(chunks, (list, tuple)):
        raise ValueError("intent chunks must be a list")
    out = []
    for entry in chunks:
        if not isinstance(entry, Mapping):
            raise ValueError("each intent chunk must be an object")
        mover = entry.get("mover_action_key")
        plan = entry.get("plan_sha256")
        consumer = entry.get("consumer_action_key")
        if not isinstance(mover, str) or not mover:
            raise ValueError("each chunk names its mover")
        try:
            start = int(entry["start_bytes"])  # type: ignore[index]
            end = int(entry["end_bytes"])  # type: ignore[index]
            gib = int(entry["stage_gib"])  # type: ignore[index]
        except (KeyError, TypeError, ValueError):
            raise ValueError("each chunk carries whole start, end and gib")
        if (not isinstance(plan, str) or len(plan) != 64
                or any(c not in "0123456789abcdef" for c in plan)):
            raise ValueError("each chunk carries its 64-hex plan digest")
        if (not isinstance(consumer, str) or len(consumer) != 64
                or any(c not in "0123456789abcdef" for c in consumer)):
            raise ValueError("each chunk carries its 64-hex consumer")
        if end <= start or gib <= 0:
            raise ValueError("each chunk spans a positive range and demand")
        out.append({"mover_action_key": mover, "start_bytes": start,
                    "end_bytes": end, "stage_gib": gib,
                    "plan_sha256": plan, "consumer_action_key": consumer})
    return out


def file_intent(queue: pool.PoolQueue, unit: str, holder: str, tier_id: str,
                demand_gib: int, chunks: Sequence[Mapping[str, object]],
                *, epoch: int | None = None) -> bool:
    """File the write-ahead intent before any ledger step takes tokens."""
    demand = _check_demand(demand_gib)
    checked = _check_chunks(chunks)
    if epoch is not None and (not isinstance(epoch, int) or epoch < 0):
        raise ValueError("a turn epoch is a non-negative whole number")
    payload = {"schema": INTENT_SCHEMA, "unit": str(unit), "holder": str(holder),
               "tier_id": str(tier_id), "demand_gib": demand,
               "chunks": checked, "epoch": epoch, "filed_unix": time.time()}
    result = _link_or_adopt(group_dir(queue, unit, tier_id) / "intent.json",
                            payload, ["unit", "holder", "tier_id",
                                      "demand_gib", "chunks", "epoch"])
    if result == "conflict":
        raise pool.PoolContractError("a different intent already names this group")
    return result in ("filed", "standing")


def standing_intent(queue: pool.PoolQueue, unit: str, tier_id: str) -> dict | None:
    """The filed intent of one group, or None when none is readable.

    A standing intent is immutable, so a reader that builds a unit later
    holds the filed demand and chunks instead of recomputing them from
    evidence that has moved since (a live shared mover, a landed range).
    """
    status, record, _ = _read_receipt(group_dir(queue, unit, tier_id)
                                      / "intent.json")
    return record if status == "record" else None


def _check_demand(demand_gib: object) -> int:
    """A whole non-negative token demand, or ValueError."""
    if isinstance(demand_gib, bool) or not isinstance(demand_gib, int):
        raise ValueError(f"a group demand is whole tokens, got {demand_gib!r}")
    if demand_gib < 0:
        raise ValueError(f"a group demand never goes negative, got {demand_gib}")
    return demand_gib


def _check_movers(chunk_movers: object) -> list[str]:
    """Mover keys as a list, or ValueError for junk."""
    if not isinstance(chunk_movers, (list, tuple)):
        raise ValueError("chunk movers must be a list of keys")
    movers = [str(key) for key in chunk_movers]
    if any(not key for key in movers):
        raise ValueError("chunk mover keys are non-empty strings")
    return movers


# ------------------------------------------------------------- census


def _chunk_binds(intent: dict | None, record: Mapping[str, object],
                 mover: str) -> bool:
    """True when one funding record funds this group's chunk of one mover."""
    if intent is None:
        return False
    chunks = intent.get("chunks")
    if not isinstance(chunks, list):
        return False
    try:
        want_range = (int(record["range_start_bytes"]),  # type: ignore[index]
                      int(record["range_end_bytes"]))  # type: ignore[index]
        want_plan = str(record["plan_sha256"])
    except (KeyError, TypeError, ValueError):
        return False
    for entry in chunks:
        if not isinstance(entry, Mapping):
            continue
        try:
            if (str(entry.get("mover_action_key")) == mover
                    and (int(entry["start_bytes"]),  # type: ignore[index]
                         int(entry["end_bytes"])) == want_range  # type: ignore[index]
                    and str(entry.get("plan_sha256")) == want_plan):
                return True
        except (TypeError, ValueError):
            continue
    return False


def _held_or_unknown(ledger: pool.ResourceLedger, key: str,
                     found: GroupCensus) -> set[str] | None:
    """One holder's token names, or None with the mover marked unknown."""
    try:
        return set(pool.held_names_visible(ledger, key))
    except (OSError, pool.PoolContractError, ValueError):
        found.funding_unknown.append(key)
        return None


def census(queue: pool.PoolQueue, tier_id: str, unit: str, holder: str,
           demand_gib: int, chunk_movers: Sequence[str]) -> GroupCensus:
    """Read one group's holder, handles, bound mover tokens and receipts."""
    demand = _check_demand(demand_gib)
    movers = _check_movers(chunk_movers)
    kind = storage_tiers.capacity_kind_of(tier_id)
    ledger = queue.tier_ledger(tier_id)
    found = GroupCensus(holder=str(holder), demand_gib=demand)
    try:
        found.h = int(ledger.holder_tokens(str(holder)).get(kind, 0))
    except (OSError, pool.PoolContractError, ValueError) as exc:
        found.unknown.append(f"holder census unreadable: {exc!r}")
    try:
        found.handles = ledger.acquisitions_of(str(holder))
        found.p = sum(int(count) for _, _, _, _, count in found.handles)
    except (OSError, pool.PoolContractError, ValueError) as exc:
        found.unknown.append(f"handle census unreadable: {exc!r}")
    try:
        found.unparsed = ledger.unparsed_acquisitions_of(str(holder))
    except (OSError, pool.PoolContractError, ValueError) as exc:
        found.unknown.append(f"handle census unreadable: {exc!r}")
    directory = group_dir(queue, unit, tier_id)
    names = ["intent.json", "committed.json", "released.json"]
    try:
        extra = sorted(p.name for p in directory.iterdir()
                       if p.is_file() and p.name.startswith("rolled-back-")
                       and p.name.endswith(".json"))
    except OSError:
        extra = []
        try:
            present = directory.exists()
        except OSError:
            present = True
        if present:
            found.unknown.append("rolled-back census unreadable")
    for name in names + extra:
        status, body, reason = _read_receipt(directory / name)
        if status == "record":
            found.receipts[name] = body
        elif status == "unknown":
            found.receipts[name] = None
            found.unknown.append(f"{name} unreadable: {reason}")
    intent = found.receipts.get("intent.json")
    for mover in movers:
        try:
            status, record, _ = queue.read_funding_evidence(mover, tier_id)
        except (OSError, pool.PoolContractError, ValueError):
            found.funding_unknown.append(mover)
            continue
        if status != "record" or record is None:
            if status == "unknown":
                found.funding_unknown.append(mover)
            continue
        # A bound token the mover holds is the mover's, whatever the record
        # says: ``publish_chunk`` moves the tokens first and closes the record
        # from ``reserved`` to ``transferring`` after.  A stop between the two
        # (or a deferred update) must not read as a short group, or the
        # reconcile would release the holder's remainder and no pass could
        # repair the record.  A ``reserved`` record whose tokens still sit in
        # the group holder counts zero here: the mover holds none of them.
        if (str(record.get("state")) not in ("reserved", "transferring",
                                             "consumed")
                or not _chunk_binds(intent, record, mover)):
            continue
        held = _held_or_unknown(ledger, mover, found)
        if held is None:
            continue
        tokens = record.get("tokens")
        if not isinstance(tokens, list):
            found.funding_unknown.append(mover)
            continue
        found.m += len({str(name) for name in tokens} & held)
    released = found.receipts.get("released.json")
    if released is not None:
        try:
            total = int(released["holder_released"])  # type: ignore[index]
            per_chunk = released["chunks"]  # type: ignore[index]
            if not isinstance(per_chunk, Mapping):
                raise ValueError("released chunks must be an object")
            for count in per_chunk.values():
                total += int(count)  # type: ignore[arg-type]
            if total < 0:
                raise ValueError("released counts never go negative")
            found.r = total
        except (KeyError, TypeError, ValueError):
            found.receipts["released.json"] = None
            found.unknown.append("released.json unreadable")
    return found


def incremental_need_gib(found: GroupCensus, demand_gib: int) -> int:
    """The gate need: a begun acquisition owns the whole demand already."""
    demand = _check_demand(demand_gib)
    return max(0, demand - (found.h + found.p + found.m))


# ------------------------------------------------------------- reconcile


def _is_me(host: str, pid: int) -> bool:
    """True when one handle names this very process."""
    return host == socket.gethostname() and pid == os.getpid()


def _is_dead(host: str, pid: int) -> bool:
    """True when one handle names a dead process on this host."""
    return host == socket.gethostname() and not pool._process_alive(pid)


def _accounted(found: GroupCensus, demand: int) -> tuple[str, bool]:
    """The committed state from h+m+r against the demand."""
    total = found.h + found.m + found.r
    if total != demand:
        return ("short" if total < demand else "overfull", False)
    if found.m == 0 and found.r == 0:
        return ("reserved", True)
    if found.h == 0:
        return ("done", True)
    return ("splitting", True)


def _commit_receipt(queue: pool.PoolQueue, unit: str, tier_id: str,
                    holder: str, demand: int) -> str:
    """File committed.json no-clobber; adopt, conflict or unknown."""
    payload = {"schema": COMMITTED_SCHEMA, "unit": str(unit),
               "holder": str(holder), "tier_id": str(tier_id),
               "demand_gib": demand, "committed_unix": time.time()}
    return _link_or_adopt(group_dir(queue, unit, tier_id) / "committed.json",
                          payload, ["unit", "holder", "tier_id", "demand_gib"])


def _rollback_receipt(queue: pool.PoolQueue, unit: str, tier_id: str,
                      holder: str, demand: int, found: GroupCensus) -> None:
    """File the next rolled-back-<n>.json for one abandoned attempt."""
    directory = group_dir(queue, unit, tier_id)
    try:
        taken = sorted(int(p.name[len("rolled-back-"):-len(".json")])
                       for p in directory.iterdir()
                       if p.is_file() and p.name.startswith("rolled-back-")
                       and p.name.endswith(".json")
                       and p.name[len("rolled-back-"):-len(".json")].isdigit())
    except OSError:
        taken = []
    number = (taken[-1] + 1) if taken else 0
    payload = {"schema": ROLLEDBACK_SCHEMA, "unit": str(unit),
               "holder": str(holder), "tier_id": str(tier_id),
               "demand_gib": demand, "h_seen": found.h, "p_seen": found.p,
               "rolled_back_unix": time.time()}
    _link_or_adopt(directory / f"rolled-back-{number}.json", payload,
                   ["unit", "holder", "tier_id", "demand_gib"])


def _settle_handles(ledger: pool.ResourceLedger, holder: str,
                    found: GroupCensus, events: list[str]) -> ReconcileOutcome:
    """Commit each acquisition handle this writer owns or a dead one left."""
    live = False
    for handle, _, host, pid, _ in found.handles:
        if _is_me(host, pid) or _is_dead(host, pid):
            try:
                moved = ledger.commit_acquire(str(holder), handle)
            except (OSError, pool.PoolContractError, ValueError):
                return ReconcileOutcome(
                    "unknown", events + ["prelaunch-unknown-evidence"],
                    False, found)
            if moved > 0:
                events.append("prelaunch-acquisition-committed")
        else:
            live = True
    if live:
        events.append("prelaunch-acquisition-in-flight")
    return ReconcileOutcome("acquiring", events, False, found)


def _top_up(ledger: pool.ResourceLedger, kind: str, holder: str,
            demand: int, found: GroupCensus,
            events: list[str]) -> ReconcileOutcome:
    """Begin the acquisition of what a committed group lost.

    The deficit is the demand less every token the census accounts for, so
    the settled group is exactly the filed demand again and the intent is
    never recomputed.  No room is a wait: the next pass asks again.
    """
    deficit = demand - (found.h + found.m + found.r)
    try:
        handle = ledger.begin_acquire(str(holder), {kind: deficit})
    except (OSError, pool.PoolContractError, ValueError):
        return ReconcileOutcome("unknown",
                                events + ["prelaunch-unknown-evidence"],
                                False, found)
    if handle is None:
        return ReconcileOutcome("short", events + ["prelaunch-begin-declined"],
                                False, found)
    return ReconcileOutcome("acquiring",
                            events + ["prelaunch-group-topped-up"],
                            False, found)


def reconcile(queue: pool.PoolQueue, tier_id: str, unit: str, holder: str,
              demand_gib: int, chunk_movers: Sequence[str],
              *, writer_is_me: bool) -> ReconcileOutcome:
    """Run the R1'' rule table once; one action per pass at most."""
    demand = _check_demand(demand_gib)
    movers = _check_movers(chunk_movers)
    kind = storage_tiers.capacity_kind_of(tier_id)
    ledger = queue.tier_ledger(tier_id)
    try:
        found = census(queue, tier_id, unit, holder, demand, movers)
    except (OSError, pool.PoolContractError, ValueError):
        return ReconcileOutcome("unknown", ["prelaunch-unknown-evidence"],
                                False, None)
    events: list[str] = []
    if found.unparsed:
        events.append("prelaunch-handle-unparsed")
    if found.unknown or found.funding_unknown:
        return ReconcileOutcome("unknown",
                                events + ["prelaunch-unknown-evidence"],
                                False, found)
    intent = found.receipts.get("intent.json")
    committed = found.receipts.get("committed.json")
    if intent is None:
        return ReconcileOutcome("unreserved", events + ["prelaunch-no-intent"],
                                False, found)
    if committed is not None:
        state, authority = _accounted(found, demand)
        if not authority and writer_is_me and found.p > 0:
            return _settle_handles(ledger, holder, found, events)
        if not authority and writer_is_me and state == "short" and found.h == 0:
            return _top_up(ledger, kind, holder, demand, found, events)
        if authority:
            return ReconcileOutcome(state, events, True, found)
        try:
            ledger.release(str(holder))
        except (OSError, pool.PoolContractError, ValueError):
            events.append("prelaunch-unknown-evidence")
        events.append("prelaunch-group-short"
                      if state == "short" else "prelaunch-group-overfull")
        return ReconcileOutcome(state, events, False, found)
    if not writer_is_me:
        if found.p > 0:
            return ReconcileOutcome("acquiring",
                                    events + ["prelaunch-not-writer"],
                                    False, found)
        state, _ = _accounted(found, demand)
        passive = state if state in ("short", "overfull") else (
            "reserved" if found.h == demand else "unreserved")
        return ReconcileOutcome(passive, events + ["prelaunch-not-writer"],
                                False, found)
    if found.p > 0:
        return _settle_handles(ledger, holder, found, events)
    if found.h == demand:
        try:
            result = _commit_receipt(queue, unit, tier_id, holder, demand)
        except (OSError, pool.PoolContractError, ValueError):
            return ReconcileOutcome(
                "unknown", events + ["prelaunch-unknown-evidence"],
                False, found)
        if result == "conflict":
            return ReconcileOutcome(
                "unknown", events + ["prelaunch-unknown-evidence"],
                False, found)
        if result == "filed":
            events.append("prelaunch-group-committed")
        state, authority = _accounted(found, demand)
        if authority:
            return ReconcileOutcome(state, events, True, found)
        try:
            ledger.release(str(holder))
        except (OSError, pool.PoolContractError, ValueError):
            events.append("prelaunch-unknown-evidence")
        events.append("prelaunch-group-overfull")
        return ReconcileOutcome("overfull", events, False, found)
    if found.h > 0:
        try:
            ledger.release(str(holder))
        except (OSError, pool.PoolContractError, ValueError):
            return ReconcileOutcome(
                "unknown", events + ["prelaunch-unknown-evidence"],
                False, found)
        try:
            _rollback_receipt(queue, unit, tier_id, holder, demand, found)
        except (OSError, pool.PoolContractError, ValueError):
            return ReconcileOutcome(
                "unknown", events + ["prelaunch-unknown-evidence"],
                False, found)
        return ReconcileOutcome("unreserved",
                                events + ["prelaunch-group-rolled-back"],
                                False, found)
    try:
        handle = ledger.begin_acquire(str(holder), {kind: demand})
    except (OSError, pool.PoolContractError, ValueError):
        return ReconcileOutcome("unknown",
                                events + ["prelaunch-unknown-evidence"],
                                False, found)
    if handle is None:
        return ReconcileOutcome("unreserved",
                                events + ["prelaunch-begin-declined"],
                                False, found)
    # The begun acquisition owns the whole demand in its private handle, so
    # the census this pass reports is read again after the begin: the gate
    # reads need from it, and the tokens have already left free.
    try:
        after = census(queue, tier_id, unit, holder, demand, movers)
    except (OSError, pool.PoolContractError, ValueError):
        after = found
        events.append("prelaunch-unknown-evidence")
    return ReconcileOutcome("acquiring", events + ["prelaunch-group-begun"],
                            False, after)


# ------------------------------------------------------------- funding


def _leg_mover(leg: Mapping[str, object]) -> str:
    """The mover key a leg hands its tokens to, or ValueError."""
    if not isinstance(leg, Mapping):
        raise ValueError("a chunk leg must be an object")
    mover = leg.get("mover_action_key")
    if not isinstance(mover, str) or not mover:
        raise ValueError("a chunk leg names its mover")
    return mover


def _leg_size(leg: Mapping[str, object]) -> tuple[int, int, int]:
    """A leg's (start, end, gib), or ValueError for junk."""
    try:
        start = int(leg["start_bytes"])  # type: ignore[index]
        end = int(leg["end_bytes"])  # type: ignore[index]
        gib = int(leg["stage_gib"])  # type: ignore[index]
    except (KeyError, TypeError, ValueError):
        raise ValueError("a chunk leg carries whole start, end and gib")
    if (isinstance(leg["stage_gib"], bool) or end <= start or gib <= 0):  # type: ignore[index]
        raise ValueError("a chunk leg spans a positive range and demand")
    return (start, end, gib)


def _funding_matches(record: Mapping[str, object], tier_id: str, mover: str,
                     kind: str, consumer: str, digest: str,
                     start: int, end: int, published: float) -> bool:
    """True when one record binds this chunk's exact publication."""
    try:
        row_published = float(record["published_unix"])  # type: ignore[index]
    except (KeyError, TypeError, ValueError):
        return False
    return (str(record.get("tier_id")) == str(tier_id)
            and str(record.get("mover_action_key")) == mover
            and str(record.get("kind")) == kind
            and str(record.get("consumer_action_key")) == consumer
            and str(record.get("plan_sha256")) == digest
            and str(record.get("range_start_bytes")) == str(start)
            and str(record.get("range_end_bytes")) == str(end)
            and row_published == published)


def _record_tokens(record: Mapping[str, object]) -> list[str] | None:
    """The bound token names, or None when the field misbehaves."""
    tokens = record.get("tokens")
    if (not isinstance(tokens, list) or not tokens
            or any(not isinstance(name, str) or not name for name in tokens)):
        return None
    return [str(name) for name in tokens]


def _birth_record(queue: pool.PoolQueue, tier_id: str, mover: str, kind: str,
                  consumer: str, digest: str, start: int, end: int,
                  published: float, tokens: list[str]) -> str | None:
    """File one reserved generation; None when the write loses its race."""
    payload = {"schema": pool.TIER_FUNDING_SCHEMA_V1, "tier_id": str(tier_id),
               "consumer_action_key": consumer, "plan_sha256": digest,
               "mover_action_key": mover, "range_start_bytes": start,
               "range_end_bytes": end, "kind": kind, "tokens": sorted(tokens),
               "generation": uuid.uuid4().hex, "state": "reserved",
               "unix": time.time(), "published_unix": published}
    try:
        queue.write_funding(payload)
    except (OSError, pool.PoolContractError, ValueError):
        return None
    try:
        filed = queue.read_funding(mover, tier_id)
    except (OSError, pool.PoolContractError, ValueError):
        return None
    if filed is None or str(filed.get("state")) != "reserved":
        return None
    generation = filed.get("generation")
    return str(generation) if isinstance(generation, str) else None


def _row_is_ready(queue: pool.PoolQueue, mover: str) -> bool:
    """True while the mover's row waits to be claimed, never once claimed."""
    return (queue.item_path(pool.READY, mover).exists()
            and not queue.item_path(pool.CLAIMED, mover).exists())


def _rotate_record(queue: pool.PoolQueue, mover: str, tier_id: str,
                   expect_generation: str, fresh: Mapping[str, object],
                   *, expect_state: str | None = None) -> str | None:
    """Replace one generation under the mover lock; None on any loss.

    ``expect_state`` also requires, under the lock a claim holds from its
    tier acquire to its consumed-marking, that the record is in that state
    and the row is still READY: a recovery never rotates a live claim.
    """
    with queue.mover_transition_lock(mover, blocking=False) as acquired:
        if not acquired:
            return None
        try:
            status, current, _ = queue.read_funding_evidence(mover, tier_id)
        except (OSError, pool.PoolContractError, ValueError):
            return None
        if (status != "record" or current is None
                or str(current.get("generation")) != expect_generation):
            return None
        if expect_state is not None and (
                str(current.get("state")) != expect_state
                or not _row_is_ready(queue, mover)):
            return None
        try:
            queue._rotate_funding_locked(dict(fresh),
                                         expect_generation=expect_generation)
        except (OSError, pool.PoolContractError, ValueError):
            return None
    return str(fresh["generation"])


def _advance_record(queue: pool.PoolQueue, mover: str, tier_id: str,
                    generation: str) -> bool:
    """Close reserved into transferring; False defers to the next pass."""
    try:
        return bool(queue.advance_funding_state(
            mover, tier_id, expect="reserved", advance_to="transferring",
            generation=generation))
    except (OSError, pool.PoolContractError, ValueError):
        return False


def _fresh_binding(tier_id: str, mover: str, kind: str, consumer: str,
                   digest: str, start: int, end: int, published: float,
                   tokens: list[str]) -> dict[str, object]:
    """One reserved generation binding an exact token set."""
    return {"schema": pool.TIER_FUNDING_SCHEMA_V1, "tier_id": str(tier_id),
            "consumer_action_key": consumer, "plan_sha256": digest,
            "mover_action_key": mover, "range_start_bytes": start,
            "range_end_bytes": end, "kind": kind, "tokens": sorted(tokens),
            "generation": uuid.uuid4().hex, "state": "reserved",
            "unix": time.time(), "published_unix": published}


def _rebind_retained(queue: pool.PoolQueue, ledger: pool.ResourceLedger,
                     tier_id: str, mover: str, kind: str, consumer: str,
                     digest: str, start: int, end: int, published: float,
                     gib: int, holder: str, tokens: list[str],
                     held: set[str], rotate_from: str,
                     expect_state: str | None = None,
                     ) -> tuple[str, set[str], set[str]] | PublishOutcome:
    """Bind a mover's retained tokens and the group's exact remainder.

    Returns the new generation with the retained and the bound token sets,
    or the outcome that ends this pass.  Nothing moves before the rename.
    """
    if held - set(tokens):
        return PublishOutcome("refused", ["prelaunch-mover-occupied"],
                              None, 0)
    prior = set(tokens) & held
    try:
        names = pool.held_names_visible(ledger, str(holder))
    except (OSError, pool.PoolContractError, ValueError):
        return PublishOutcome("deferred", ["prelaunch-unknown-evidence"],
                              None, 0)
    need = gib - len(prior)
    kind_names = sorted(n for n in names if n.startswith(kind + "-"))
    if need < 0 or len(kind_names) < need:
        return PublishOutcome("short", ["prelaunch-funding-short"], None, 0)
    bound = prior | set(kind_names[:need])
    fresh = _fresh_binding(tier_id, mover, kind, consumer, digest,
                           start, end, published, sorted(bound))
    generation = _rotate_record(queue, mover, tier_id, rotate_from, fresh,
                                expect_state=expect_state)
    if generation is None:
        return PublishOutcome("deferred", ["prelaunch-publish-deferred"],
                              None, 0)
    return (generation, prior, bound)


def publish_chunk(queue: pool.PoolQueue, tier_id: str, unit: str, holder: str,
                  plan: Mapping[str, object], leg: Mapping[str, object],
                  mover_row_published_unix: float) -> PublishOutcome:
    """Hand one leg's tokens to its mover and bind them as its fence."""
    from . import residency_plan as plans
    mover = _leg_mover(leg)
    start, end, gib = _leg_size(leg)
    try:
        published = float(mover_row_published_unix)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        raise ValueError("the mover publication time must be a timestamp")
    if published < 0:
        raise ValueError("the mover publication time never goes negative")
    try:
        consumer = str(plan["consumer_action_key"])  # type: ignore[index]
        digest = plans.plan_sha256(plan)
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError(f"the chunk needs its live plan: {exc!r}")
    kind = storage_tiers.capacity_kind_of(tier_id)
    ledger = queue.tier_ledger(tier_id)
    if not (queue.item_path(pool.READY, mover).exists()
            or queue.item_path(pool.CLAIMED, mover).exists()):
        return PublishOutcome("refused", ["prelaunch-mover-unpublished"],
                              None, 0)
    try:
        status, record, _ = queue.read_funding_evidence(mover, tier_id)
    except (OSError, pool.PoolContractError, ValueError):
        return PublishOutcome("deferred", ["prelaunch-unknown-evidence"],
                              None, 0)
    if status == "unknown":
        return PublishOutcome("deferred", ["prelaunch-unknown-evidence"],
                              None, 0)
    generation: str | None = None
    prior: set[str] = set()
    rotate_from: str | None = None
    bound: set[str] | None = None
    if record is not None and _funding_matches(
            record, tier_id, mover, kind, consumer, digest,
            start, end, published):
        tokens = _record_tokens(record)
        generation = (str(record["generation"])
                      if isinstance(record.get("generation"), str) else None)
        if tokens is None or generation is None:
            return PublishOutcome("deferred", ["prelaunch-unknown-evidence"],
                                  None, 0)
        held = _held_or_unknown(ledger, mover, GroupCensus())
        if held is None:
            return PublishOutcome("deferred", ["prelaunch-unknown-evidence"],
                                  None, 0)
        state = str(record.get("state"))
        if state in ("transferring", "consumed") and set(tokens) <= held:
            return PublishOutcome("already", [], generation, 0)
        if state == "transferring" and _row_is_ready(queue, mover):
            # A READY row whose bound tokens left (a recovered group): bind
            # what it still holds and the group's exact remainder, and only
            # while the record is still ``transferring`` under the lock (#1637).
            rebound = _rebind_retained(
                queue, ledger, tier_id, mover, kind, consumer, digest,
                start, end, published, gib, holder, tokens, held,
                generation, expect_state="transferring")
            if isinstance(rebound, PublishOutcome):
                return rebound
            generation, prior, bound = rebound
        else:
            if state == "transferring" or state == "consumed":
                return PublishOutcome("short", ["prelaunch-funding-short"],
                                      generation, 0)
            if state != "reserved":
                return PublishOutcome("deferred",
                                      ["prelaunch-unknown-evidence"], None, 0)
            prior = set(tokens) & held
            bound = set(tokens)
    elif record is not None:
        old = record.get("generation")
        rotate_from = str(old) if isinstance(old, str) else None
        tokens = _record_tokens(record)
        republished = (tokens is not None
                       and _republished(record, tier_id, mover, kind,
                                        consumer, digest, start, end))
        # Movers are keyed by manifest range, so a second capture of one
        # manifest names the first capture's movers.  What the first left is
        # a spent record bound to another consumer and plan.  It covers
        # nothing (only ``transferring`` funds a claim) and no state leaves
        # it, so it is rotated like an older publication of this chunk -- but
        # only when the mover holds no tokens (#1628).  A live record of
        # another consumer is still occupied.
        spent = str(record.get("state")) in ("consumed", "released")
        if rotate_from is None or not (republished or spent):
            return PublishOutcome("refused", ["prelaunch-mover-occupied"],
                                  None, 0)
        held = _held_or_unknown(ledger, mover, GroupCensus())
        if held is None:
            return PublishOutcome("deferred", ["prelaunch-unknown-evidence"],
                                  None, 0)
        if republished:
            rebound = _rebind_retained(
                queue, ledger, tier_id, mover, kind, consumer, digest,
                start, end, published, gib, holder, tokens, held,
                rotate_from)
            if isinstance(rebound, PublishOutcome):
                return rebound
            generation, prior, bound = rebound
            rotate_from = None
        else:
            if held:
                return PublishOutcome("refused", ["prelaunch-mover-occupied"],
                                      None, 0)
            # Establish the new reserved generation BEFORE any token moves,
            # naming tokens still in the group's holder, the way the
            # no-record path below does.  Rotating after the transfer would
            # leave tokens under the mover beside the old spent record when
            # the rotation is deferred or the process stops mid-transfer, and
            # the retry would find a held mover and refuse it for good.  With
            # the binding first, a retry finds a matching record and resumes.
            try:
                names = pool.held_names_visible(ledger, str(holder))
            except (OSError, pool.PoolContractError, ValueError):
                return PublishOutcome("deferred",
                                      ["prelaunch-unknown-evidence"], None, 0)
            kind_names = sorted(n for n in names if n.startswith(kind + "-"))
            if len(kind_names) < gib:
                return PublishOutcome("short", ["prelaunch-funding-short"],
                                      None, 0)
            bound = set(kind_names[:gib])
            fresh = _fresh_binding(tier_id, mover, kind, consumer, digest,
                                   start, end, published, kind_names[:gib])
            generation = _rotate_record(queue, mover, tier_id, rotate_from,
                                        fresh)
            if generation is None:
                return PublishOutcome("deferred",
                                      ["prelaunch-publish-deferred"], None, 0)
            rotate_from = None
            prior = set()
    else:
        held = _held_or_unknown(ledger, mover, GroupCensus())
        if held is None:
            return PublishOutcome("deferred", ["prelaunch-unknown-evidence"],
                                  None, 0)
        if held:
            return PublishOutcome("refused", ["prelaunch-mover-occupied"],
                                  None, 0)
        if generation is None:
            try:
                names = pool.held_names_visible(ledger, str(holder))
            except (OSError, pool.PoolContractError, ValueError):
                return PublishOutcome("deferred",
                                      ["prelaunch-unknown-evidence"], None, 0)
            kind_names = sorted(n for n in names if n.startswith(kind + "-"))
            if len(kind_names) < gib:
                return PublishOutcome("short", ["prelaunch-funding-short"],
                                      None, 0)
            bound = set(kind_names[:gib])
            generation = _birth_record(queue, tier_id, mover, kind, consumer,
                                       digest, start, end, published,
                                       kind_names[:gib])
            if generation is None:
                return PublishOutcome("deferred",
                                      ["prelaunch-publish-deferred"], None, 0)
            try:
                reread = queue.read_funding(mover, tier_id)
            except (OSError, pool.PoolContractError, ValueError):
                reread = None
            tokens = _record_tokens(reread) if reread is not None else None
            if tokens is None or set(tokens) != bound:
                return PublishOutcome("deferred",
                                      ["prelaunch-unknown-evidence"], None, 0)
            prior = set()
    need = gib - len(prior)
    if need > 0:
        try:
            before = set(pool.held_names_visible(ledger, mover))
        except (OSError, pool.PoolContractError, ValueError):
            return PublishOutcome("deferred", ["prelaunch-unknown-evidence"],
                                  generation, 0)
        try:
            moved = int(queue.transfer_tier_reservation_count(
                tier_id, str(holder), mover, need))
        except (OSError, pool.PoolContractError, ValueError):
            return PublishOutcome("deferred", ["prelaunch-unknown-evidence"],
                                  generation, 0)
        if moved < need:
            return PublishOutcome("short", ["prelaunch-funding-short"],
                                  generation, moved)
        try:
            after = set(pool.held_names_visible(ledger, mover))
        except (OSError, pool.PoolContractError, ValueError):
            return PublishOutcome("deferred", ["prelaunch-unknown-evidence"],
                                  generation, 0)
        fresh_names = sorted(n for n in (prior | (after - before))
                             if n.startswith(kind + "-"))
    else:
        moved = 0
        try:
            current = set(pool.held_names_visible(ledger, mover))
        except (OSError, pool.PoolContractError, ValueError):
            return PublishOutcome("deferred", ["prelaunch-unknown-evidence"],
                                  generation, 0)
        fresh_names = sorted(n for n in current if n.startswith(kind + "-"))
    if generation is None:
        return PublishOutcome("deferred", ["prelaunch-unknown-evidence"],
                              None, moved)
    if bound is not None and not bound <= set(fresh_names):
        if len(fresh_names) != gib:
            return PublishOutcome("short", ["prelaunch-funding-short"],
                                  generation, moved)
        fresh = _fresh_binding(tier_id, mover, kind, consumer, digest,
                               start, end, published, fresh_names)
        generation = _rotate_record(queue, mover, tier_id, generation, fresh)
        if generation is None:
            return PublishOutcome("deferred", ["prelaunch-publish-deferred"],
                                  None, moved)
    if _advance_record(queue, mover, tier_id, generation):
        return PublishOutcome("published", ["prelaunch-chunk-published"],
                              generation, moved)
    return PublishOutcome("deferred", ["prelaunch-publish-deferred"],
                          generation, moved)


def _republished(record: Mapping[str, object], tier_id: str, mover: str,
                 kind: str, consumer: str, digest: str,
                 start: int, end: int) -> bool:
    """True when one record binds this chunk under an older publication."""
    return (str(record.get("tier_id")) == str(tier_id)
            and str(record.get("mover_action_key")) == mover
            and str(record.get("kind")) == kind
            and str(record.get("consumer_action_key")) == consumer
            and str(record.get("plan_sha256")) == digest
            and str(record.get("range_start_bytes")) == str(start)
            and str(record.get("range_end_bytes")) == str(end))


# ------------------------------------------------------------- release


def _release_row_state(queue: pool.PoolQueue, mover: str) -> str:
    """One mover's row state: claimed, ready, or gone."""
    if queue.item_path(pool.CLAIMED, mover).exists():
        return "claimed"
    if queue.item_path(pool.READY, mover).exists():
        return "ready"
    return "gone"


def release_unit(queue: pool.PoolQueue, tier_id: str, unit: str, holder: str,
                 chunk_movers: Sequence[str], *, terminal: bool,
                 shared_owned: Sequence[str] = frozenset()) -> list[str]:
    """Release one unit's unsplit remainder and its unconsumed fences."""
    if not terminal:
        return []
    movers = _check_movers(chunk_movers)
    shared = {str(key) for key in shared_owned}
    ledger = queue.tier_ledger(tier_id)
    events: list[str] = []
    directory = group_dir(queue, unit, tier_id)
    status, intent, _ = _read_receipt(directory / "intent.json")
    if status == "unknown":
        events.append("prelaunch-unknown-evidence")
        intent = None
    try:
        holder_freed = int(ledger.release(str(holder)))
    except (OSError, pool.PoolContractError, ValueError):
        holder_freed = 0
        events.append("prelaunch-unknown-evidence")
    chunks: dict[str, int] = {}
    for mover in movers:
        try:
            evidence, record, _ = queue.read_funding_evidence(mover, tier_id)
        except (OSError, pool.PoolContractError, ValueError):
            events.append("prelaunch-unknown-evidence")
            continue
        if evidence != "record" or record is None:
            if evidence == "unknown":
                events.append("prelaunch-unknown-evidence")
            continue
        if (str(record.get("state")) not in ("reserved", "transferring")
                or mover in shared
                or not _chunk_binds(intent, record, mover)):
            continue
        if _release_row_state(queue, mover) == "claimed":
            continue
        generation = record.get("generation")
        try:
            closed = queue.advance_funding_state(
                mover, tier_id, expect=str(record.get("state")),
                advance_to="released",
                generation=(str(generation)
                            if isinstance(generation, str) else None))
        except (OSError, pool.PoolContractError, ValueError):
            closed = False
        if not closed:
            events.append("prelaunch-unknown-evidence")
            continue
        tokens = _record_tokens(record) or []
        if _release_row_state(queue, mover) == "ready":
            try:
                held = set(pool.held_names_visible(ledger, mover))
            except (OSError, pool.PoolContractError, ValueError):
                events.append("prelaunch-unknown-evidence")
                continue
            count = len(set(tokens) & held)
            try:
                ledger.release(mover)
            except (OSError, pool.PoolContractError, ValueError):
                events.append("prelaunch-unknown-evidence")
                continue
            chunks[mover] = count
        else:
            try:
                held = set(pool.held_names_visible(ledger, mover))
            except (OSError, pool.PoolContractError, ValueError):
                held = set()
            chunks[mover] = len(set(tokens) & held)
    status, prior, _ = _read_receipt(directory / "released.json")
    if status == "unknown":
        events.append("prelaunch-unknown-evidence")
        return events + ["prelaunch-group-released"]
    merged: dict[str, int] = {}
    holder_part = holder_freed
    if status == "record" and prior is not None:
        try:
            holder_part += int(prior["holder_released"])  # type: ignore[index]
            old_chunks = prior["chunks"]  # type: ignore[index]
            if not isinstance(old_chunks, Mapping):
                raise ValueError("released chunks must be an object")
            for key, count in old_chunks.items():
                merged[str(key)] = int(count)  # type: ignore[arg-type]
        except (KeyError, TypeError, ValueError):
            return events + ["prelaunch-group-released"]
    for key, count in chunks.items():
        merged[key] = merged.get(key, 0) + count
    payload = {"schema": RELEASED_SCHEMA, "unit": str(unit),
               "holder": str(holder), "tier_id": str(tier_id),
               "holder_released": holder_part, "chunks": merged,
               "released_unix": time.time()}
    try:
        _write_json_atomic(directory / "released.json", payload)
    except OSError:
        events.append("prelaunch-unknown-evidence")
        return events
    return events + ["prelaunch-group-released"]


# ------------------------------------------------------------- holdings


def has_holdings(queue: pool.PoolQueue, unit: str) -> bool:
    """True when one unit still owns tokens or unreleased group property."""
    unit = str(unit)
    try:
        exists = (queue.root / GROUP_DIR).is_dir()
    except OSError:
        return True
    if exists:
        try:
            entries = [p for p in (queue.root / GROUP_DIR).iterdir()
                       if p.is_dir() and p.name.startswith(unit + ".")]
        except FileNotFoundError:
            entries = []
        except OSError:
            return True
        for entry in entries:
            status, intent, _ = _read_receipt(entry / "intent.json")
            if status != "record" or intent is None:
                continue
            try:
                holder = str(intent["holder"])  # type: ignore[index]
                tier = str(intent["tier_id"])  # type: ignore[index]
                demand = int(intent["demand_gib"])  # type: ignore[index]
                raw_chunks = intent["chunks"]  # type: ignore[index]
                movers = [str(e["mover_action_key"])  # type: ignore[index]
                          for e in raw_chunks] if isinstance(
                              raw_chunks, list) else []
            except (KeyError, TypeError, ValueError):
                return True
            try:
                found = census(queue, tier, unit, holder, demand, movers)
            except (OSError, pool.PoolContractError, ValueError):
                return True
            if (found.unknown or found.funding_unknown
                    or found.h + found.p + found.m > 0):
                return True
            if found.receipts.get("committed.json") is not None and found.r < demand:
                return True
    prefix = f"{HOLDER_PREFIX}{unit[:16]}-"
    try:
        base = queue.root / pool.TIER_RESERVATIONS
        tiers = [p for p in base.iterdir() if p.is_dir()]
    except FileNotFoundError:
        tiers = []
    except OSError:
        return True
    for tier in tiers:
        try:
            held = tier / "held"
            names = [p for p in held.iterdir()
                     if p.is_dir() and p.name.startswith(prefix)]
        except FileNotFoundError:
            continue
        except OSError:
            return True
        for holder_dir in names:
            try:
                if any(holder_dir.iterdir()):
                    return True
            except OSError:
                return True
    return False


# ------------------------------------------------------------- turn


def _turn_dir(queue: pool.PoolQueue) -> Path:
    """The directory holding tickets, epochs and markers."""
    return queue.root / TURN_DIR


def _ticket_file(queue: pool.PoolQueue, unit: str) -> Path:
    """The immutable ticket path for one unit."""
    if re.fullmatch(r"[A-Za-z0-9_=@:.-]{1,128}", unit):
        name = unit
    else:
        name = "u-" + raw_sha256(unit.encode())[:32]
    return _turn_dir(queue) / _TIERS / f"{name}.json"


def _check_ticket(ticket: object) -> dict | None:
    """A ranked ticket body, or None when it cannot rank."""
    if not isinstance(ticket, Mapping):
        return None
    unit = ticket.get("unit")
    priority = ticket.get("priority")
    when = ticket.get("published_unix")
    demands = ticket.get("tier_demands")
    if not isinstance(unit, str) or not unit:
        return None
    if not isinstance(priority, int) or isinstance(priority, bool):
        return None
    if (not isinstance(when, (int, float)) or isinstance(when, bool)
            or float(when) < 0):
        return None
    if not isinstance(demands, Mapping) or not demands:
        return None
    clean: dict[str, int] = {}
    for tier, gib in demands.items():
        if not isinstance(tier, str) or not tier:
            return None
        if not isinstance(gib, int) or isinstance(gib, bool) or gib <= 0:
            return None
        clean[tier] = gib
    return {"unit": unit, "priority": priority,
            "published_unix": float(when), "tier_demands": clean}


def turn_ticket(queue: pool.PoolQueue, unit: str, priority: int,
                published_unix: float,
                tier_demands: Mapping[str, int]) -> bool:
    """File one unit's immutable turn ticket, first writer wins."""
    ticket = _check_ticket({"unit": unit, "priority": priority,
                            "published_unix": published_unix,
                            "tier_demands": tier_demands})
    if ticket is None:
        raise ValueError("a turn ticket carries unit, priority, time, demands")
    payload = {"schema": TICKET_SCHEMA, **ticket, "ticket_unix": time.time()}
    result = _link_or_adopt(_ticket_file(queue, str(unit)), payload,
                            ["unit", "priority", "published_unix",
                             "tier_demands"])
    if result == "conflict":
        raise pool.PoolContractError("a different ticket already names this unit")
    return result in ("filed", "standing")


_TURN_FILE = re.compile(r"^turn-(\d+)\.json$")
_RESERVED_FILE = re.compile(r"^turn-(\d+)\.reserved-([0-9a-f]{12})\.json$")
_DONE_FILE = re.compile(r"^turn-(\d+)\.done$")


def current_turn(queue: pool.PoolQueue) -> dict:
    """The latest epoch with its unit, markers and done state."""
    try:
        entries = [p for p in _turn_dir(queue).iterdir() if p.is_file()]
    except OSError:
        return {"status": "none"}
    epochs: dict[int, Path] = {}
    reserved: dict[int, list[Path]] = {}
    done: dict[int, Path] = {}
    for path in entries:
        turn = _TURN_FILE.fullmatch(path.name)
        marker = _RESERVED_FILE.fullmatch(path.name)
        finished = _DONE_FILE.fullmatch(path.name)
        if turn is not None:
            epochs[int(turn.group(1))] = path
        elif marker is not None:
            reserved.setdefault(int(marker.group(1)), []).append(path)
        elif finished is not None:
            done[int(finished.group(1))] = path
    if not epochs:
        return {"status": "none"}
    epoch = max(epochs)
    status, body, _ = _read_receipt(epochs[epoch])
    if status != "record" or body is None:
        return {"status": "unknown", "reason": f"turn-{epoch}.json unreadable"}
    if body.get("epoch") != epoch or not isinstance(body.get("unit"), str):
        return {"status": "unknown", "reason": f"turn-{epoch}.json names nobody"}
    tiers = []
    for path in sorted(reserved.get(epoch, [])):
        marker_status, marker, _ = _read_receipt(path)
        if marker_status != "record" or marker is None:
            return {"status": "unknown",
                    "reason": f"{path.name} unreadable"}
        tier = marker.get("tier_id")
        if not isinstance(tier, str) or not tier:
            return {"status": "unknown",
                    "reason": f"{path.name} names no tier"}
        tiers.append(tier)
    if epoch in done:
        done_status, finished, _ = _read_receipt(done[epoch])
        if done_status != "record" or finished is None:
            return {"status": "unknown",
                    "reason": f"turn-{epoch}.done unreadable"}
        reason = finished.get("reason")
        if not isinstance(reason, str) or not reason:
            return {"status": "unknown",
                    "reason": f"turn-{epoch}.done names no reason"}
        return {"status": "done", "epoch": epoch,
                "unit": body.get("unit"), "reserved": sorted(tiers),
                "reason": reason}
    return {"status": "open", "epoch": epoch, "unit": body.get("unit"),
            "reserved": sorted(tiers)}


def take_turn(queue: pool.PoolQueue,
              tickets_in_rank_order: Sequence[Mapping[str, object]]) -> TakeTurn:
    """Create the next epoch for the best-ranked ticket without holdings."""
    current = current_turn(queue)
    if current["status"] == "unknown":
        return TakeTurn(None, None, False, "unknown")
    if current["status"] == "open":
        return TakeTurn(current["epoch"], current["unit"], False, "standing")
    last = int(current["epoch"]) if current["status"] == "done" else -1
    ranked = sorted((t for t in (_check_ticket(t)
                                for t in tickets_in_rank_order) if t is not None),
                    key=lambda t: (-int(t["priority"]),  # type: ignore[index]
                                   float(t["published_unix"]),  # type: ignore[index]
                                   str(t["unit"])))  # type: ignore[index]
    winner = None
    for ticket in ranked:
        try:
            held = has_holdings(queue, str(ticket["unit"]))
        except (OSError, pool.PoolContractError, ValueError):
            continue
        if not held:
            winner = ticket
            break
    if winner is None:
        return TakeTurn(None, None, False, "no-candidate")
    epoch = last + 1
    payload = {"schema": TURN_SCHEMA, "epoch": epoch,
               "unit": str(winner["unit"]), "ticket": dict(winner),
               "picked_unix": time.time()}
    try:
        if _link_new(_turn_dir(queue) / f"turn-{epoch}.json", payload):
            return TakeTurn(epoch, str(winner["unit"]), True, "created")
    except OSError:
        return TakeTurn(None, None, False, "unknown")
    status, standing, _ = _read_receipt(_turn_dir(queue) / f"turn-{epoch}.json")
    if status == "record" and standing is not None:
        unit = standing.get("unit")
        if isinstance(unit, str) and unit:
            return TakeTurn(epoch, unit, False, "standing")
    return TakeTurn(epoch, None, False, "unknown")


def record_reserved(queue: pool.PoolQueue, epoch: int, tier_id: str) -> bool:
    """Mark one tier's group reserved under one epoch, first writer wins."""
    if not isinstance(epoch, int) or epoch < 0:
        raise ValueError("a turn epoch is a non-negative whole number")
    payload = {"schema": RESERVED_SCHEMA, "epoch": epoch,
               "tier_id": str(tier_id), "reserved_unix": time.time()}
    path = (_turn_dir(queue)
            / f"turn-{epoch}.reserved-{tier_digest(tier_id)}{_RESERVED_SUFFIX}")
    result = _link_or_adopt(path, payload, ["epoch", "tier_id"])
    if result == "conflict":
        raise pool.PoolContractError("this marker already names another tier")
    return result in ("filed", "standing")


def finish_turn(queue: pool.PoolQueue, epoch: int, reason: str) -> bool:
    """Close one epoch with its reason; the next epoch may then open."""
    if not isinstance(epoch, int) or epoch < 0:
        raise ValueError("a turn epoch is a non-negative whole number")
    if not isinstance(reason, str) or not reason:
        raise ValueError("a finished turn names its reason")
    payload = {"schema": DONE_SCHEMA, "epoch": epoch, "reason": reason,
               "done_unix": time.time()}
    result = _link_or_adopt(_turn_dir(queue) / f"turn-{epoch}.done",
                            payload, ["epoch", "reason"])
    if result == "conflict":
        raise pool.PoolContractError("this epoch already closed otherwise")
    return result in ("filed", "standing")
