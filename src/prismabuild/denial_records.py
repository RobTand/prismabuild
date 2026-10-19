"""Pure encoding of latest claim diagnostics and denial transitions."""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType


CLAIM_DENIALS_SCHEMA_V1 = "prismabuild.claim_denials.v1"
DENIAL_TRANSITIONS_SCHEMA_V1 = "prismabuild.denial_transitions.v1"
MAX_CLAIM_DENIALS = 256
MAX_CLAIM_PASS_LOOPS = 64
MAX_DENIAL_TRANSITIONS = 16
MAX_DENIAL_VALUE_DEPTH = 6
MAX_DENIAL_VALUE_ITEMS = 32
MAX_DENIAL_VALUE_TEXT = 256


def _bounded_value(value: object, depth: int = 0) -> object:
    if depth >= MAX_DENIAL_VALUE_DEPTH:
        return "<truncated>"
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, str):
        return value[:MAX_DENIAL_VALUE_TEXT]
    if isinstance(value, Mapping):
        return {str(key)[:MAX_DENIAL_VALUE_TEXT]: _bounded_value(value[key], depth + 1)
                for key in list(sorted(value, key=str))[:MAX_DENIAL_VALUE_ITEMS]}
    if isinstance(value, (list, tuple, set)):
        return [_bounded_value(item, depth + 1)
                for item in list(value)[:MAX_DENIAL_VALUE_ITEMS]]
    return repr(value)[:MAX_DENIAL_VALUE_TEXT]


@dataclass(frozen=True)
class PreparedDenial:
    """Detached wire evidence, ready for its separately observed timestamp."""

    identity: str
    _body: Mapping[str, object]

    @classmethod
    def from_item(
        cls, item: Mapping[str, object], reason: str,
        evidence: Mapping[str, object] | None, *, host: str,
    ) -> PreparedDenial:
        key = str(item.get("action_key", ""))
        identity = f"{key}:{repr(float(item['published_unix']))}"
        evidence = dict(evidence or {})
        if item.get("dependent_of") is not None:
            evidence.setdefault("dependent_of", item.get("dependent_of"))
        body = {
            "action_key": key, "published_unix": float(item["published_unix"]),
            "host": host, "reason": reason, "attempts": item.get("attempts", 0),
            "evidence": _bounded_value(evidence),
        }
        return cls(identity, MappingProxyType(body))

    def stamped(self, denied_unix: float) -> dict[str, object]:
        return {**self._body, "denied_unix": denied_unix}


def transition_document(
    prior: object, *, key: str, generation: float, host: str, reason: str,
    decision_reason: str | None, decision: Mapping[str, object] | None, now: float,
) -> dict[str, object] | None:
    ring = [dict(value) for value in prior.get("transitions", [])
            if isinstance(value, Mapping)] if isinstance(prior, Mapping) else []
    on_host = [value for value in ring
               if value.get("host") == host and value.get("published_unix") == generation]
    verdict = (reason, decision_reason)
    last = max(
        on_host,
        key=lambda value: (value.get("last_unix", value.get("unix", 0.0))
                           if isinstance(value.get("last_unix", value.get("unix")),
                                         (int, float)) else 0.0),
        default=None)
    if last is not None and (last.get("reason"), last.get("decision_reason")) == verdict:
        return None
    repeat = next((value for value in on_host
                   if (value.get("reason"), value.get("decision_reason")) == verdict), None)
    if repeat is not None:
        repeat["count"] = int(repeat.get("count") or 1) + 1
        repeat["last_unix"] = now
    else:
        ring.append({"unix": now, "host": host, "reason": reason,
                     "decision_reason": decision_reason,
                     "decision": (_bounded_value(decision) if isinstance(decision, Mapping) else None),
                     "published_unix": generation, "count": 1, "last_unix": now})
        ring = ring[-MAX_DENIAL_TRANSITIONS:]
    return {"schema": DENIAL_TRANSITIONS_SCHEMA_V1, "action_key": key, "transitions": ring}


def diagnostic_stamp(record: object, field: str) -> float:
    value = record.get(field, 0) if isinstance(record, Mapping) else 0
    return float(value) if isinstance(value, (int, float)) else 0.0


def diagnostic_document(
    existing: Mapping[str, object], denials: Mapping[str, object], summaries: Mapping[str, object],
) -> dict[str, object]:
    document: dict[str, object] = {"schema": CLAIM_DENIALS_SCHEMA_V1}
    for name, updates, stamp, limit in (
            ("records", denials, "denied_unix", MAX_CLAIM_DENIALS),
            ("claim_passes", summaries, "passed_unix", MAX_CLAIM_PASS_LOOPS)):
        prior = existing.get(name)
        records = dict(prior) if isinstance(prior, Mapping) else {}
        for identity, record in updates.items():
            if (identity not in records or diagnostic_stamp(record, stamp)
                    >= diagnostic_stamp(records[identity], stamp)):
                records[identity] = record
        if name == "records" or records or isinstance(prior, Mapping):
            newest = sorted(records.items(), key=lambda entry:
                            diagnostic_stamp(entry[1], stamp), reverse=True)
            document[name] = dict(newest[:limit])
    return document
