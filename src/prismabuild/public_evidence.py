"""Derived, allow-listed public evidence; not receipt verification (#2701).

Only built-in JSON containers and exact scalar types are inspected. Unknown
positions are not searched for apparently useful values. See
``docs/public_evidence.md`` for the deliberately narrow adoption contract.
"""
from __future__ import annotations

import math
import re


_SCHEMA = "prismabuild.public_evidence.v1"
_STATUSES = frozenset({
    "pending", "ready", "running", "succeeded", "failed", "cancelled",
    "withdrawn", "interrupted", "timed_out", "unknown",
})
_HASH = re.compile(r"[0-9a-f]{64}")
_HASH_FIELDS = frozenset({
    "action_key", "action_manifest_sha256", "stdout_sha256", "stderr_sha256",
})
_TIME_FIELDS = frozenset({"started_at", "finished_at", "observed_at"})
_RECORD_FIELDS = _HASH_FIELDS | _TIME_FIELDS | {"status", "returncode", "result", "children"}
_MAX_DEPTH = 16
_MAX_ITEMS = 1024
_MAX_NODES = 4096


def _valid_hash(value: object) -> bool:
    return type(value) is str and _HASH.fullmatch(value) is not None


def _valid_scalar(key: str, value: object) -> bool:
    if key == "status":
        return type(value) is str and value in _STATUSES
    if key == "returncode":
        return type(value) is int and -(2**31) <= value < 2**31
    if key in _HASH_FIELDS:
        return _valid_hash(value)
    if key in _TIME_FIELDS:
        # Unix seconds in the datetime year 1..9999 range, restricted to >= 0.
        return (type(value) in (int, float) and 0 <= value < 253402300800
                and math.isfinite(value))
    return False


def _approved_items(value: dict, fields: frozenset[str] | set[str]) -> dict:
    # Iterate rather than lookup on the untrusted mapping: non-str keys may
    # implement equality against str. Do not call any of their methods.
    return {key: item for key, item in value.items()
            if type(key) is str and key in fields}


def _result(value: object) -> dict:
    if type(value) is not dict or len(value) > _MAX_ITEMS:
        return {}
    result = {}
    for key, item in sorted(_approved_items(value, {"sha256", "bytes"}).items()):
        if ((key == "sha256" and _valid_hash(item))
                or (key == "bytes" and type(item) is int and 0 <= item < 2**63)):
            result[key] = item
    return result


def public_evidence_view(value: object) -> dict[str, object]:
    """Return a fresh, deterministic ``{schema, evidence}`` public projection.

    Records allow only validated scalars, a result descriptor, and children.
    Lists allow only projected records/lists, never scalar strings (argv).
    Invalid values, cycles, empty nested containers and over-limit branches
    are omitted silently. No repr/str fallback or input-bearing diagnostics.
    A view is revalidated, not trusted merely because it has our marker.
    """
    if type(value) is dict and len(value) <= _MAX_ITEMS:
        envelope = _approved_items(value, {"schema", "evidence"})
        schema = envelope.get("schema")
        if type(schema) is str and schema == _SCHEMA:
            value = envelope.get("evidence")

    ancestors: set[int] = set()
    remaining = _MAX_NODES

    def project(node: object, depth: int) -> dict | list:
        nonlocal remaining
        if (depth > _MAX_DEPTH or remaining == 0
                or type(node) not in (dict, list) or len(node) > _MAX_ITEMS
                or id(node) in ancestors):
            return {}
        remaining -= 1
        ancestors.add(id(node))
        try:
            if type(node) is list:
                items = []
                for item in node:
                    child = project(item, depth + 1)
                    if child:
                        items.append(child)
                return items
            record = {}
            for key, item in sorted(_approved_items(node, _RECORD_FIELDS).items()):
                if key == "children":
                    # Only this named edge authorizes recursive record/list data.
                    child = project(item, depth + 1) if type(item) is list else {}
                    if child:
                        record[key] = child
                elif key == "result":
                    descriptor = _result(item)
                    if descriptor:
                        record[key] = descriptor
                elif _valid_scalar(key, item):
                    record[key] = item
            return record
        finally:
            ancestors.remove(id(node))

    return {"schema": _SCHEMA, "evidence": project(value, 0) or {}}
