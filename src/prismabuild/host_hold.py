"""Operator admission holds, independent of the host's upgrade drain."""
from __future__ import annotations

from dataclasses import dataclass
import math
from pathlib import Path
import re
import time

from . import immutable_files, posix_lock
from .digest_primitives import PrismaBuildError, decode_strict_json

HOST_HOLD_SCHEMA = "prismabuild.host_hold.v1"
MAX_HOLD_SECONDS = 12 * 3600


@dataclass(frozen=True)
class HostHold:
    owner: str | None = None
    reason: str | None = None
    created_unix: float | None = None
    expires_unix: float | None = None
    error: str | None = None
    expired: bool = False

    def fields(self) -> dict[str, object]:
        return dict(owner=self.owner, reason=self.reason,
                    created_unix=self.created_unix, expires_unix=self.expires_unix,
                    error=self.error)


def host_hold_path(queue_root: Path, host: str) -> Path:
    if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", host) is None:
        raise ValueError("host must be a single host name")
    return Path(queue_root) / "holds" / (host + ".json")


def read_host_hold(queue_root: Path, host: str, *, now: float | None = None) -> HostHold | None:
    """Absence opens admission; every other read/shape failure closes it."""
    path = host_hold_path(queue_root, host)
    try:
        raw = immutable_files.read_replaced_record(path, where="operator host hold", max_bytes=16384)
    except FileNotFoundError:
        return None
    except (OSError, PrismaBuildError) as exc:
        return HostHold(error=f"unreadable operator hold: {type(exc).__name__}")
    try:
        body = decode_strict_json(raw, where="operator host hold")
        if not isinstance(body, dict) or body.get("schema") != HOST_HOLD_SCHEMA:
            raise ValueError("invalid hold schema")
        owner, reason = body.get("owner"), body.get("reason")
        if (not isinstance(owner, str) or not owner.strip() or len(owner) > 200
                or not isinstance(reason, str) or not reason.strip() or len(reason) > 1000):
            raise ValueError("hold needs owner and reason")
        created, expires = body.get("created_unix"), body.get("expires_unix")
        if (type(created) not in (int, float) or type(expires) not in (int, float)
                or not math.isfinite(created) or not math.isfinite(expires)
                or created < 0 or not 0 < expires - created <= MAX_HOLD_SECONDS):
            raise ValueError("hold lifetime must be positive and at most 12 hours")
        return HostHold(owner, reason, created, expires,
                        expired=expires <= (time.time() if now is None else now))
    except (ValueError, OverflowError, PrismaBuildError) as exc:
        return HostHold(error=f"malformed operator hold: {exc}")


def change_host_hold(queue_root: Path, host: str, *, owner: str,
                     reason: str | None = None, expires_unix: float | None = None,
                     release: bool = False, force: bool = False,
                     now: float | None = None) -> HostHold | None:
    """Serialize operator mutations so release cannot erase another owner's hold."""
    path = host_hold_path(queue_root, host)
    if not isinstance(owner, str) or not owner.strip() or len(owner) > 200:
        raise ValueError("owner is required and must fit 200 characters")
    if force and (not reason or not reason.strip()):
        raise ValueError("--force requires --reason")
    now = time.time() if now is None else now
    if not release:
        if not reason or not reason.strip() or len(reason) > 1000:
            raise ValueError("reason is required and must fit 1000 characters")
        if (type(expires_unix) not in (int, float) or not math.isfinite(expires_unix)
                or not 0 < expires_unix - now <= MAX_HOLD_SECONDS):
            raise ValueError("expiry must be in the next 12 hours")
    with posix_lock.held(path.with_suffix(".lock")):
        previous = read_host_hold(queue_root, host, now=now)
        if (previous is not None and not previous.expired
                and (previous.error or previous.owner != owner) and not force):
            raise ValueError("hold belongs to another owner or is unreadable; use --force --reason")
        if release:
            path.unlink(missing_ok=True)
            return None
        immutable_files.write_json_atomic(path, dict(
            schema=HOST_HOLD_SCHEMA, owner=owner, reason=reason,
            created_unix=now, expires_unix=expires_unix))
        return HostHold(owner, reason, now, expires_unix)


def list_host_holds(queue_root: Path, *, now: float | None = None) -> dict[str, HostHold]:
    directory = Path(queue_root) / "holds"
    try:
        paths = sorted(directory.iterdir())
    except FileNotFoundError:
        return {}
    result = {}
    for path in paths:
        if path.suffix == ".json":
            hold = read_host_hold(queue_root, path.stem, now=now)
            if hold is not None:
                result[path.stem] = hold
    return result
