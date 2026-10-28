"""Source adapters for existing gang clocks, not an admission or wait policy."""
from __future__ import annotations

from dataclasses import dataclass
import math

from . import _gang, numeric_fields
from .host_wait import DeadlineReason, RetryDeadline


@dataclass(frozen=True)
class GangMember:
    group: str
    index: int
    action_key: str
    published_unix: float


@dataclass(frozen=True)
class GangDeadline:
    member: GangMember
    source: str
    deadline: RetryDeadline


def member_deadlines(record, item, *, wall_now, monotonic_now, ready=None,
                     terminal=None, terminal_signature=None, barrier_deadline=None,
                     effective_heartbeat_unix=None, lease_timeout_s=None):
    """Expose individual absolute deadlines from already authoritative evidence.

    The caller retains read_group/sealed-declaration/claim validation. Hints
    cannot supply these documents. The barrier deadline is the original local
    start plus configured skew, not reconnect time. The heartbeat is the
    claim-bound result of PoolQueue._effective_heartbeat, not a raw stamp.
    Missing or invalid proof remains unknown to the caller, not fresh evidence.
    Retire this vector when the member publication or relevant predicate ends.
    """
    from .pool import LEASE_TIMEOUT_S

    declaration = _gang.declaration(item.get("gang"))
    if (declaration is None or declaration["group"] != record["group"]
            or declaration["size"] != record["size"]):
        raise _gang.GangContractError("deadline row does not declare this gang")
    entry = _gang.member(record, item, declaration)
    identity = GangMember(record["group"], entry["index"], entry["action_key"],
                          float(entry["published_unix"]))
    if not all(numeric_fields.is_finite_number(value) for value in (wall_now, monotonic_now)):
        raise ValueError("deadline clock samples must be finite")
    result = []

    def wall_deadline(source, stamp, reason, *, strict=False):
        converted = monotonic_now + (stamp - wall_now)
        if strict:
            converted = max(math.nextafter(converted, math.inf),
                monotonic_now + (math.nextafter(stamp, math.inf) - wall_now))
            if stamp < wall_now:
                converted = min(converted, monotonic_now)
        result.append(GangDeadline(identity, source,
            RetryDeadline(reason, converted)))

    if (ready is not None and ready.get("schema") == _gang.READY_SCHEMA
            and ready.get("group") == identity.group
            and type(ready.get("index")) is int and ready["index"] == identity.index
            and ready.get("action_key") == identity.action_key
            and numeric_fields.is_finite_number(ready.get("ready_unix"))):
        stamp = float(ready["ready_unix"])
        if stamp > wall_now:
            wall_deadline("readiness-visible", stamp, DeadlineReason.EVIDENCE_EXPIRY)
        wall_deadline("readiness", stamp + _gang.READY_FRESH_S,
                      DeadlineReason.EVIDENCE_EXPIRY, strict=True)
    if (terminal_signature and terminal is not None
            and terminal.get("schema") == _gang.TERMINAL_SCHEMA
            and terminal.get("group") == identity.group
            and terminal.get("member") == identity.action_key
            and terminal.get("signature") == terminal_signature
            and numeric_fields.is_finite_number(terminal.get("first_seen_unix"))
            and terminal["first_seen_unix"] <= wall_now):
        wall_deadline("terminal", float(terminal["first_seen_unix"]) + _gang.TERMINAL_CONFIRM_S,
                      DeadlineReason.POLICY_EXPIRY)
    if barrier_deadline is not None:
        result.append(GangDeadline(identity, "skew",
            RetryDeadline(DeadlineReason.POLICY_EXPIRY, barrier_deadline)))
    if effective_heartbeat_unix is not None:
        timeout = LEASE_TIMEOUT_S if lease_timeout_s is None else lease_timeout_s
        if (not numeric_fields.is_finite_number(effective_heartbeat_unix)
                or not numeric_fields.is_finite_number(timeout) or timeout < 0):
            raise ValueError("lease evidence and timeout must be finite")
        wall_deadline("lease", effective_heartbeat_unix + timeout,
                      DeadlineReason.EVIDENCE_EXPIRY, strict=True)
    return tuple(result)
