"""The standard-library-only half of core's digest owner (#1547).

pbtest ships this exact source into target interpreters before its helpers.
Core re-exports the same objects; this is not an alternate digest policy.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path


class PrismaBuildError(RuntimeError):
    """Base class for PrismaBuild core failures."""


class ActionContractError(PrismaBuildError, ValueError):
    """An action, closure, receipt, or worker identity is not exact."""


def _canonical_bytes(value: object) -> bytes:
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"),
                          ensure_ascii=False, allow_nan=False).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ActionContractError("value is not finite canonical JSON data") from exc


def sorted_json(value: object, *, allow_nan: bool = True) -> str:
    """Sorted default JSON text, including the existing non-finite spelling."""
    return json.dumps(value, sort_keys=True, allow_nan=allow_nan)


def _sorted_json_bytes(value: object, *, allow_nan: bool = True) -> bytes:
    """Sorted default JSON without a terminator, retaining its wire spelling."""
    return sorted_json(value, allow_nan=allow_nan).encode("utf-8")


def compact_ascii_json_bytes(value: object) -> bytes:
    """Finite compact JSON with the resident-manifest ASCII escape spelling."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      allow_nan=False).encode("utf-8")


def _sorted_lf_bytes(value: object) -> bytes:
    """Sorted default mapping JSON followed by exactly one LF."""
    return _sorted_json_bytes(dict(value)) + b"\n"


def canonical_sha256(value: object) -> str:
    """SHA-256 of finite compact UTF-8 canonical JSON."""
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def raw_sha256(data: bytes) -> str:
    """SHA-256 of the bytes themselves, without JSON encoding."""
    return hashlib.sha256(data).hexdigest()


# Incremental consumers keep their existing single I/O pass and native constructor.
new_sha256 = hashlib.sha256


def stream_digest(path: str | Path, *, algorithm: str = "sha256",
                  offset: int = 0, length: int | None = None) -> bytes:
    """Read bounded chunks for the existing file and installed-RECORD recipes."""
    digest = hashlib.new(algorithm)
    remaining = length
    with open(path, "rb") as handle:
        if offset:
            handle.seek(offset)
        while True:
            want = 1 << 20 if remaining is None else min(1 << 20, remaining)
            if want <= 0:
                break
            chunk = handle.read(want)
            if not chunk:
                break
            digest.update(chunk)
            if remaining is not None:
                remaining -= len(chunk)
    return digest.digest()


def stream_sha256(path: str | Path, *, offset: int = 0,
                  length: int | None = None) -> str:
    """Chunked SHA-256 of a file range; a short file hashes what it has."""
    return stream_digest(path, offset=offset, length=length).hex()
