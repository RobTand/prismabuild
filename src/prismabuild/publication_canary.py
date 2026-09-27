"""Publisher-owned GPU boundary slots (#1213).

Authority is the existing trusted publication/queue store, not an environment
label or a readable bearer token. Only the publisher calls ``mint`` while it
holds its publication lock. Every grant binds one *sealed action*, one runtime
generation and one host forever. Queue publication consumes it before READY;
finish, withdrawal and process death do not replenish that budget.
"""
from __future__ import annotations

import hashlib
import math
from pathlib import Path
import re
from collections.abc import Callable, Mapping

from . import core as pb, posix_lock
from .materialize import _write_json_atomic

CAPABILITY = "publication-canary-slot-v1"
SCHEMA = "prismabuild.publication_canary_slot.v1"
# Verified same-image leg2 action 4ae5bc61d25fa5c5de7fcfc40b5e40430f5c137b6dce3eb8430ac944f3c0aa33:
# elapsed execution 3.93170428276062s, excluding queue wait. Eightfold margin.
EXECUTION_TIMEOUT_S = math.ceil(8 * 3.93170428276062)


def identity(value: object) -> dict[str, str]:
    if not isinstance(value, Mapping) or set(value) != {"generation", "host"}:
        raise ValueError("publication canary identity requires generation and host")
    generation, host = value["generation"], value["host"]
    if (not isinstance(generation, str)
            or not re.fullmatch(r"[0-9a-f]{12}-[0-9]+-[0-9a-f]{12}", generation)
            or not isinstance(host, str)
            or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", host)):
        raise ValueError("publication canary runtime generation or host malformed")
    return {"generation": generation, "host": host}


def intent(action: Mapping[str, object] | None) -> dict[str, str] | None:
    params = action.get("params", {}) if action is not None else {}
    if not isinstance(params, Mapping) or "publication_canary" not in params:
        return None
    value = identity(params["publication_canary"])
    if (type(params.get("execution_timeout_s")) not in (int, float)
            or params["execution_timeout_s"] != EXECUTION_TIMEOUT_S):
        raise ValueError("publication canary execution timeout must be 32 seconds")
    if params.get("gpu_exclusive") is not True:
        raise ValueError("publication canary requires sealed exclusive GPU admission")
    return value


def slot_path(root: Path, value: Mapping[str, str]) -> Path:
    checked = identity(value)
    digest = hashlib.sha256(
        f"{checked['generation']}\0{checked['host']}".encode()).hexdigest()
    return Path(root) / "publication-canaries" / "v1" / f"{digest}.json"


def load(root: Path, value: Mapping[str, str], key: str) -> dict[str, object]:
    path = slot_path(root, value)
    try:
        raw = pb._read_regular_file_nofollow(path, where="publication canary grant")
        grant = pb._decode_strict_json(raw, where="publication canary grant")
    except (OSError, ValueError) as exc:
        raise ValueError(f"publication canary grant unavailable: {exc}") from exc
    if (not isinstance(grant, dict) or grant.get("schema") != SCHEMA
            or any(grant.get(k) != v for k, v in value.items())
            or grant.get("execution_timeout_s") != EXECUTION_TIMEOUT_S
            or not isinstance(grant.get("run_id"), str) or not grant["run_id"]
            or "published_unix" not in grant):
        raise ValueError("publication canary grant authority malformed")
    if grant.get("action_key") != key:
        raise ValueError("publication canary grant bound to another action key")
    return grant


def mint(root: Path, action: Mapping[str, object], *, run_id: str) -> Path:
    """Store mutation for the trusted publisher's already-authorized request.

    The sole production caller is publish_runtime's authorizer: that parent
    proves publication-lock ownership before dispatching this exact request
    to its bounded helper. This storage primitive does not authenticate a
    caller or defend against malicious mutation of the trusted queue store.
    Exclusive creation is never an overwrite, even after the previous action
    has terminated. No shared secret or caller-supplied label is authority.
    """
    checked = pb.validate_action(action)
    value = intent(checked)
    if value is None or not run_id:
        raise ValueError("publication canary mint requires sealed intent and run id")
    path = slot_path(root, value)
    path.parent.mkdir(parents=True, exist_ok=True)
    pb._atomic_write_new_json(path, {
        "schema": SCHEMA, **value, "action_key": checked["action_key"],
        "execution_timeout_s": EXECUTION_TIMEOUT_S, "run_id": run_id,
        "published_unix": None,
    })
    return path


def check_envelope(value: Mapping[str, str], *, tags, needs_gpu, resources,
                   priority, retry_safe, max_attempts) -> None:
    required = {CAPABILITY, value["host"], f"runtime-generation:{value['generation']}"}
    if (not required.issubset(tags) or needs_gpu is not True
            # pbrun --exclusive preserves its existing 16-GiB minimum.
            or resources != {"cpu": 1, "gpu": 1, "mem_gb": 16}
            or priority != -10 or retry_safe is not False or max_attempts != 1):
        raise ValueError("publication canary envelope lacks fenced single-attempt GPU demand")


def bind(root: Path, value: Mapping[str, str], key: str, published_unix: float) -> None:
    """Consume before READY, under the action transition lock as well.

    A crash after this write but before READY spends the slot. Recovery never
    mints a replacement or silently resets an ambiguous budget.
    """
    path = slot_path(root, value)
    with posix_lock.held(path.with_suffix(".lock"), blocking=True) as acquired:
        if not acquired:
            raise ValueError("publication canary grant lock unavailable")
        grant = load(root, value, key)
        if grant["published_unix"] is not None:
            raise ValueError("publication canary slot already spent/published")
        _write_json_atomic(path, {**grant, "published_unix": published_unix})


def verified(root: Path, item: Mapping[str, object]) -> bool:
    """Only a validated, exact queue generation earns boundary priority."""
    try:
        value = identity(item.get("publication_canary"))
        check_envelope(value, tags=item.get("tags", []),
                       needs_gpu=item.get("needs_gpu"), resources=item.get("resources"),
                       priority=item.get("priority"), retry_safe=item.get("retry_safe"),
                       max_attempts=item.get("max_attempts"))
        grant = load(root, value, str(item.get("action_key", "")))
        stamp = grant["published_unix"]
        return (type(stamp) in (int, float) and math.isfinite(stamp)
                and stamp == item.get("published_unix"))
    except (ValueError, OSError, TypeError):
        return False


def promote(root: Path, ready: list[dict[str, object]], *,
            eligible: Callable[[Mapping[str, object]], bool] | None = None,
            ) -> list[dict[str, object]]:
    # Normal candidates do no registry I/O. Stable partition preserves every
    # existing band/aging/GPU-first decision within the ordinary population.
    # A foreign/unknown placement earns no extra priority in a bounded prefix.
    privileged, ordinary = [], []
    for item in ready:
        boost = False
        if "publication_canary" in item:
            try:
                boost = ((eligible is None or eligible(item))
                         and verified(root, item))
            except (ValueError, TypeError):
                pass  # Unknown placement is not authority to cross bands.
        (privileged if boost else ordinary).append(item)
    return privileged + ordinary
