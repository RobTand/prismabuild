"""Deterministic worker join/resign on the existing fleet lifecycle.

A worker is a host fleet member: all loops on one box. This module drives
the two authorities that already exist rather than adding a scheduler:

- desired membership: ``tools/fleet/fleet_boxes.json`` presence via
  ``fleet_roster`` (``active`` vs ``retired``/``offline``), plus the broker
  durable maintenance state (root-owned persistent JSON; restart replays it,
  and the boot hold keeps the fence closed until client verification).
- execution fence: the broker-owned volatile gate mirror
  (``worker_loop.MAINTENANCE_GATE``) with ``changed_unix`` naming the drain
  epoch, and ``PARKED_ROOT`` exact-loop markers.
- running work: the existing withdrawal ladder (``PoolQueue.withdraw``) plus
  holder ``finish``/reaper terminals. Resign never releases tokens on
  heartbeat loss and never edits ``pool.py`` state directly.

Resign order (root-reviewed): ``maintenance_begin`` under the broker mutex
(the linearization point: new scope ``create`` is refused from there) →
census owned claims → ``withdraw`` while loops are still busy → wait for per-
key terminal records (``done/``/``failed/``) AND broker ``active_scopes``
empty → collect exact-loop park acks for this ``changed_unix`` → report
``resigned`` with evidence. ``resigned`` is never returned after only a
withdraw request. Retry-safe rows need the approved requeue handoff hunk
before they can complete; until then they are reported ``pending_requeue``
and the host stays ``resigning``.

Instance identity is the live supervisor (``supervise.CLAIM`` pid +
``/proc`` starttime). Every mutating call carries ``expected_changed_unix``
read immediately before: stale processes holding an old expectation refuse
before touching the broker. The CLI operates only on its local worker.
"""

from __future__ import annotations

import argparse
import json
import os
import socket
import sys
import time
from pathlib import Path
from typing import Any, Callable

TOOL_DIR = Path(__file__).resolve().parent
REPO_ROOT = TOOL_DIR.parents[1]
#: Layout root holding RUNTIME_VERSION.json: the published generation in
#: production (``<root>/RUNTIME_VERSION.json`` beside ``<root>/tools``), the
#: source checkout in development. Tests may point this at a private
#: directory the way worker_loop's publication-lock constant allows.
RUNTIME_ROOT = REPO_ROOT
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))
if str(TOOL_DIR) not in sys.path:
    sys.path.insert(0, str(TOOL_DIR))

import fleet_roster  # noqa: E402
from prismabuild import pool as pool_module  # noqa: E402
import worker_loop  # noqa: E402

SCHEMA = "prismaquant.prismabuild.fleet_membership.v1"

DEFAULT_QUEUE_ROOT = Path("/mnt/shared/prismabuild-fleet/pb-queue")
DEFAULT_SOCKET = Path("/run/prismabuild/resources.sock")
OFFER_TIMEOUT_S = 120.0

STATES = ("eligible", "qualifying", "draining", "unavailable", "unknown")


def _utc() -> float:
    return time.time()


def gate_paths(gate: Path | str | None = None) -> tuple[Path, Path]:
    """(gate file, parked root) for an explicit or default gate location."""
    if gate is None:
        return worker_loop.MAINTENANCE_GATE, worker_loop.PARKED_ROOT
    gate_path = Path(gate)
    return gate_path, gate_path.parent / "rollout" / "parked"


def read_gate(gate_path: Path) -> dict[str, Any] | None:
    """Open gate (None) or the drain in force; unreadable parses as draining."""
    try:
        value = json.loads(gate_path.read_text())
    except FileNotFoundError:
        return {"draining": True, "reason": "maintenance gate not initialized"}
    except (OSError, ValueError):
        return {"draining": True}
    if not isinstance(value, dict):
        return {"draining": True}
    return None if value.get("draining") is False else value


def gate_epoch(gate: dict[str, Any] | None) -> Any:
    """The ``changed_unix`` naming this drain epoch (None when open/unnamed)."""
    if gate is None:
        return None
    return gate.get("changed_unix")


def gate_key(gate: dict[str, Any] | None) -> str:
    """Filename-safe drain identity shared with the loop's park markers."""
    if gate is None:
        return "open"
    return worker_loop._gate_key(gate)


def local_host() -> str:
    return socket.gethostname()


def check_host(host: str | None) -> tuple[str, str | None]:
    """The CLI operates only on its local worker; anything else refuses."""
    host = host or local_host()
    if host != local_host():
        return host, (
            f"refusing to operate on {host} from {local_host()}: "
            "join/resign drive the local broker gate and local loops only"
        )
    return host, None


def supervisor_incarnation() -> tuple[str | None, str | None]:
    """Stable owner for this box's live supervisor, or (None, reason).

    The owner names the supervision that holds the box, not the CLI process
    that happens to call: ``{host}:supervisor-{pid}:{starttime}``. A dead,
    replaced, or unreadable supervisor is unknown, never a fresh nonce —
    minting a random owner per call is what let any join end any resign.
    """
    host = local_host()
    try:
        import supervise

        claim_path = supervise.CLAIM
    except ImportError as exc:
        return None, f"cannot read supervisor claim authority: {exc}"
    try:
        pid_text = claim_path.read_text().strip().split()[0]
        pid = int(pid_text)
    except (OSError, ValueError, IndexError) as exc:
        return None, f"supervisor claim unreadable: {exc}"
    if pid <= 0:
        return None, "supervisor claim holds no live pid"
    try:
        line = Path(f"/proc/{pid}/stat").read_text()
    except OSError as exc:
        return None, f"supervisor pid {pid} not observable: {exc}"
    _, _, rest = line.rpartition(")")
    fields = rest.split()
    starttime = fields[19] if len(fields) > 19 else "unknown"
    return f"{host}:supervisor-{pid}:{starttime}", None


def roster_entry(host: str, roster_path: Path | None = None) -> dict[str, Any] | None:
    """Raw roster entry for ``host`` (alias-aware), or None when unreadable."""
    candidates: list[Path] = []
    if roster_path is not None:
        candidates = [Path(roster_path)]
    else:
        candidates = [
            REPO_ROOT / "tools" / "fleet" / "fleet_boxes.json",
            TOOL_DIR / "fleet_boxes.json",
        ]
    for path in candidates:
        try:
            boxes = json.loads(path.read_text())["boxes"]
        except (OSError, ValueError, KeyError):
            continue
        if not isinstance(boxes, dict):
            continue
        if host in boxes and isinstance(boxes[host], dict):
            return dict(boxes[host])
        aliases = [
            (name, shape)
            for name, shape in boxes.items()
            if isinstance(shape, dict) and shape.get("_alias") == host
        ]
        if len(aliases) > 1:
            raise ValueError(f"ambiguous fleet hostname alias {host}: {path}")
        if aliases:
            return dict(aliases[0][1])
    return None


def check_roster_active(host: str, roster_path: Path | None = None) -> tuple[bool, str]:
    """Whether the declaration names ``host`` active (desired membership)."""
    try:
        entry = roster_entry(host, roster_path)
    except ValueError as exc:
        return False, str(exc)
    if entry is None:
        return False, f"no fleet_boxes.json entry for {host}"
    try:
        status, detail = fleet_roster.box_status(host, entry)
    except fleet_roster.RosterPresenceError as exc:
        return False, f"cannot establish roster presence: {exc}"
    if status in fleet_roster.ABSENT:
        return (
            False,
            f"roster declares {host} {status} "
            f"({detail.get('reason')}) by {detail.get('by')}; "
            "un-declare the absence and publish to resume",
        )
    return True, "active"


def broker_status(
    socket_path: Path | None = None,
    request: Callable[[dict], dict] | None = None,
) -> dict[str, Any]:
    """Real broker ``maintenance_status`` shape, or ``ok: False`` (fail closed).

    The reply carries ``draining``, ``active_scopes``/``active_scope_ids``,
    ``health``, ``errors`` and, while draining, ``maintenance_owner``. The
    drain epoch (``changed_unix``) lives in the gate file, never here.
    """
    if request is None:
        from prismabuild.resource_scope import broker_request

        def request(payload: dict) -> dict:
            return broker_request(payload, socket_path=socket_path or DEFAULT_SOCKET)

    try:
        reply = request({"op": "maintenance_status"})
    except (OSError, ValueError) as exc:
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
    if not isinstance(reply, dict):
        return {"ok": False, "error": "broker returned a non-object status"}
    return dict(reply)


def claimed_census(
    queue: pool_module.PoolQueue, host: str
) -> tuple[list[str], list[str]]:
    """(owned keys, unknown paths). Malformed records are unknown, never empty."""
    keys: list[str] = []
    unknown: list[str] = []
    claimed_dir = queue.dir(pool_module.CLAIMED)
    if not claimed_dir.is_dir():
        return [], [f"claimed directory unreadable: {claimed_dir}"]
    try:
        paths = sorted(claimed_dir.glob("*.json"))
    except OSError as exc:
        return [], [f"claimed directory not listable: {exc}"]
    for path in paths:
        try:
            record = json.loads(path.read_text())
        except (OSError, ValueError) as exc:
            unknown.append(f"{path.name}: unreadable ({exc})")
            continue
        if not isinstance(record, dict):
            unknown.append(f"{path.name}: non-object record")
            continue
        action_key = record.get("action_key")
        if not isinstance(action_key, str) or len(action_key) != 64:
            unknown.append(f"{path.name}: bad action key")
            continue
        owner: str | None = None
        if record.get("claimed_host") == host:
            owner = host
        else:
            try:
                lease = json.loads(queue.lease_path(action_key).read_text())
            except (OSError, ValueError) as exc:
                unknown.append(f"{path.name}: lease unreadable ({exc})")
                continue
            if not isinstance(lease, dict):
                unknown.append(f"{path.name}: non-object lease")
                continue
            if lease.get("host") == host:
                owner = host
            else:
                continue
        keys.append(action_key)
    return sorted(set(keys)), unknown


def live_loop_census(
    live: list[tuple[int, str]] | None = None,
) -> tuple[list[tuple[int, str]], str | None]:
    """((pid, starttime) loops, unknown reason). Failure is unknown, not []."""
    if live is not None:
        return list(live), None
    try:
        import supervise

        pids = list(supervise._live_loops())
    except (ImportError, OSError) as exc:
        return [], f"loop census unavailable: {exc}"
    census: list[tuple[int, str]] = []
    for pid in pids:
        try:
            line = Path(f"/proc/{pid}/stat").read_text()
        except OSError as exc:
            return [], f"loop pid {pid} not observable: {exc}"
        _, _, rest = line.rpartition(")")
        fields = rest.split()
        starttime = fields[19] if len(fields) > 19 else "unknown"
        census.append((pid, starttime))
    return census, None


def parked_markers(parked_root: Path, gate: dict[str, Any] | None) -> list[Path]:
    """Markers naming exactly this drain epoch (old epochs never count)."""
    if gate is None:
        return []
    key = gate_key(gate)
    if not parked_root.is_dir():
        return []
    try:
        return sorted(p for p in parked_root.glob(f"*-{key}") if p.is_file())
    except OSError:
        return []


def expected_markers(
    census: list[tuple[int, str]], gate: dict[str, Any] | None
) -> set[str]:
    """Full marker names each live loop must post (pid + starttime + epoch)."""
    if gate is None:
        return set()
    key = gate_key(gate)
    return {f"{pid}-{starttime}-{key}" for pid, starttime in census}


def terminal_of(queue: pool_module.PoolQueue, action_key: str) -> dict[str, Any] | None:
    """The terminal record for this attempt, or None while it is still owned.

    ``done/``/``failed/`` are terminals. A withdrawn attempt concludes
    through the holder's own ``finish``, which entombs the claim and leaves
    the filed ``withdrawn/`` decision as the terminal — so claimed-gone plus
    a ``withdrawn/`` record is terminal too. A ``withdrawn/`` record beside
    a still-present claim means the holder has not concluded yet.
    """
    for state in (pool_module.DONE, pool_module.FAILED):
        try:
            record = json.loads(queue.item_path(state, action_key).read_text())
        except (OSError, ValueError):
            continue
        if isinstance(record, dict):
            return record
    try:
        claimed_gone = not queue.item_path(pool_module.CLAIMED, action_key).exists()
    except OSError:
        return None
    if not claimed_gone:
        return None
    try:
        withdrawn = json.loads(
            queue.item_path(pool_module.WITHDRAWN, action_key).read_text())
    except (OSError, ValueError):
        return None
    return withdrawn if isinstance(withdrawn, dict) else None


def _pending_requeue(record: dict[str, Any]) -> bool:
    """Whether this withdrawn row still owes a budget-preserving requeue.

    The authoritative eligibility check lives in the pool (the approved
    requeue handoff hunk generalizes it); this is the report-side reading:
    retry-safe rows with remaining budget are owed a successor, everything
    else concludes by terminal alone.
    """
    attempts = record.get("attempts")
    limit = record.get("max_attempts", pool_module.DEFAULT_MAX_ATTEMPTS)
    return (
        record.get("retry_safe") is True
        and type(attempts) is int
        and attempts >= 0
        and type(limit) is int
        and attempts + 1 < limit
    )


def join(
    host: str | None = None,
    *,
    reason: str = "",
    queue_root: Path | None = None,
    roster_path: Path | None = None,
    gate: Path | None = None,
    socket_path: Path | None = None,
    broker_call: Callable[[dict], dict] | None = None,
    expected_changed_unix: Any = None,
    runtime_root: Path | None = None,
) -> dict[str, Any]:
    """Validate, then open this host's gate. The gate is untouched on refusal."""
    host, host_error = check_host(host)
    if host_error is not None:
        return {"status": "refused", "phase": "host", "reason": host_error}
    owner, owner_error = supervisor_incarnation()
    if owner_error is not None or owner is None:
        return {"status": "refused", "phase": "incarnation", "host": host,
                "reason": owner_error or "no live supervisor incarnation"}
    active, detail = check_roster_active(host, roster_path)
    if not active:
        return {"status": "refused", "phase": "roster", "host": host,
                "owner": owner, "reason": detail}
    checks = qualify_host(host, queue_root=queue_root, roster_path=roster_path,
                          socket_path=socket_path, broker_call=broker_call,
                          runtime_root=runtime_root)
    if not checks["ok"]:
        return {"status": "refused", "phase": "qualification", "host": host,
                "owner": owner, "reason": checks["reason"], "checks": checks}
    gate_path, _ = gate_paths(gate)
    gate_now = read_gate(gate_path)
    if gate_now is None:
        return {"status": "already_joined", "host": host, "owner": owner,
                "checks": checks}
    if expected_changed_unix is None:
        expected_changed_unix = gate_epoch(gate_now)
    if gate_epoch(gate_now) != expected_changed_unix:
        return {"status": "refused", "phase": "epoch", "host": host,
                "owner": owner,
                "reason": "gate epoch moved under this call; re-read and retry"}
    call = broker_call
    if call is None:
        from prismabuild.resource_scope import broker_request

        sock = socket_path or DEFAULT_SOCKET

        def call(payload: dict) -> dict:
            return broker_request(payload, socket_path=sock)

    try:
        ended = call({"op": "maintenance_end", "owner": owner})
    except (OSError, ValueError, PermissionError, RuntimeError) as exc:
        return {"status": "refused", "phase": "maintenance_end", "host": host,
                "owner": owner, "reason": f"{type(exc).__name__}: {exc}"}
    if read_gate(gate_path) is not None:
        return {"status": "refused", "phase": "maintenance_end", "host": host,
                "owner": owner, "reason": "gate still draining after end",
                "broker": ended}
    return {"status": "joined", "host": host, "owner": owner,
            "reason": reason, "broker": ended, "checks": checks,
            "checked_unix": _utc(),
            "note": "eligibility follows when loops publish a fresh qualified "
                    "offer; frozen child scopes untouched"}


def qualify_host(
    host: str,
    *,
    queue_root: Path | None = None,
    roster_path: Path | None = None,
    socket_path: Path | None = None,
    broker_call: Callable[[dict], dict] | None = None,
    runtime_root: Path | None = None,
) -> dict[str, Any]:
    """Structured join qualification; every check fail-closed with a reason."""
    checks: dict[str, Any] = {}
    if queue_root is None:
        return {"ok": False, "reason": "no queue root to prove shared access",
                "checks": checks}
    root = Path(queue_root)
    probe = root / f".membership-probe-{os.getpid()}"
    try:
        root.mkdir(parents=True, exist_ok=True)
        probe.write_text(json.dumps({"host": host, "unix": _utc()}))
        probe.read_text()
        probe.unlink()
        checks["shared_namespace_rw"] = True
    except OSError as exc:
        checks["shared_namespace_rw"] = False
        return {"ok": False, "reason": f"shared queue not writable: {exc}",
                "checks": checks}
    receipt = (Path(runtime_root) if runtime_root is not None
               else RUNTIME_ROOT) / "RUNTIME_VERSION.json"
    try:
        commit = json.loads(receipt.read_text()).get("commit") or ""
    except (OSError, ValueError):
        commit = ""
    checks["runtime_commit"] = commit
    if not commit:
        return {"ok": False, "reason": "published runtime receipt unreadable",
                "checks": checks}
    status = broker_status(socket_path, broker_call)
    checks["broker"] = status
    if not (isinstance(status, dict) and status.get("health") is True):
        return {"ok": False, "reason": f"broker not healthy: {status}",
                "checks": checks}
    try:
        from prismabuild import box_capacity

        declared = _declared_demand(host, roster_path)
        observed = box_capacity.observe(declared, held={})
        checks["offer"] = {"declared": declared,
                           "capacity": dict(observed.capacity),
                           "foreign": dict(observed.foreign),
                           "detail": dict(observed.detail)}
        if declared.get("gpu", 0) > 0 and not observed.detail.get(
                "gpu_capacity_trusted"):
            return {"ok": False,
                    "reason": "GPU declared but broker snapshot untrusted: "
                              f"{observed.detail.get('gpu_capacity_error')}",
                    "checks": checks}
        if observed.foreign:
            return {"ok": False,
                    "reason": f"foreign load holds this box: {observed.foreign}",
                    "checks": checks}
    except (ImportError, OSError, ValueError) as exc:
        return {"ok": False, "reason": f"capacity observation failed: {exc}",
                "checks": checks}
    return {"ok": True, "checks": checks}


def _declared_demand(host: str, roster_path: Path | None) -> dict[str, int]:
    """Declared capacity parsed from the roster loop args (never invented)."""
    try:
        entry = roster_entry(host, roster_path)
    except ValueError as exc:
        raise ValueError(str(exc))
    if entry is None:
        raise ValueError(f"no fleet_boxes.json entry for {host}")
    args = [str(a) for a in (entry.get("args") or [])]
    demand: dict[str, int] = {"cpu": 1}
    if "--gpu" in args:
        demand["gpu"] = 1
    if "--mem-gb" in args:
        try:
            demand["mem_gb"] = int(args[args.index("--mem-gb") + 1])
        except (ValueError, IndexError) as exc:
            raise ValueError(f"roster mem-gb unparsable for {host}: {exc}")
    return demand


def resign(
    host: str | None = None,
    *,
    reason: str,
    queue_root: Path | None = None,
    gate: Path | None = None,
    socket_path: Path | None = None,
    broker_call: Callable[[dict], dict] | None = None,
    live: list[tuple[int, str]] | None = None,
    wait_s: float = 120.0,
    expected_changed_unix: Any = None,
    withdraw_owned: bool = True,
) -> dict[str, Any]:
    """Close the gate, withdraw owned claims while busy, prove completion."""
    if not reason.strip():
        return {"status": "refused", "phase": "args",
                "reason": "resign needs --reason"}
    host, host_error = check_host(host)
    if host_error is not None:
        return {"status": "refused", "phase": "host", "reason": host_error}
    owner, owner_error = supervisor_incarnation()
    if owner_error is not None or owner is None:
        return {"status": "refused", "phase": "incarnation", "host": host,
                "reason": owner_error or "no live supervisor incarnation"}
    gate_path, parked_root = gate_paths(gate)
    gate_before = read_gate(gate_path)
    if expected_changed_unix is None and gate_before is not None:
        expected_changed_unix = gate_epoch(gate_before)
    if (gate_before is not None
            and gate_epoch(gate_before) != expected_changed_unix):
        return {"status": "refused", "phase": "epoch", "host": host,
                "owner": owner,
                "reason": "gate epoch moved under this call; re-read and retry"}
    call = broker_call
    if call is None:
        from prismabuild.resource_scope import broker_request

        sock = socket_path or DEFAULT_SOCKET

        def call(payload: dict) -> dict:
            return broker_request(payload, socket_path=sock)

    try:
        began = call({"op": "maintenance_begin", "reason": reason, "owner": owner})
    except (OSError, ValueError, PermissionError, RuntimeError) as exc:
        return {"status": "refused", "phase": "maintenance_begin", "host": host,
                "owner": owner, "reason": f"{type(exc).__name__}: {exc}"}
    # The epoch is the gate file the broker just synced, never a reply field:
    # the status reply carries no changed_unix by contract.
    gate_now = read_gate(gate_path)
    if gate_now is None:
        return {"status": "refused", "phase": "maintenance_begin", "host": host,
                "owner": owner, "reason": "gate open after begin",
                "broker": began}
    key = gate_key(gate_now)
    if queue_root is None:
        return {"status": "resigning", "host": host, "owner": owner,
                "gate_key": key,
                "reason": "no queue root: owned claims unobservable, fence retained",
                "broker": began}
    queue = pool_module.PoolQueue(Path(queue_root))
    # Post-begin census (never one pre-census): claims that won the
    # poll-check race after the mutex closed must be identified here.
    owned, census_unknown = claimed_census(queue, host)
    if census_unknown:
        return {"status": "resigning", "host": host, "owner": owner,
                "gate_key": key,
                "reason": "claim census unknown; fence and resources retained",
                "unknown": census_unknown, "broker": began}
    withdrawn: list[dict[str, Any]] = []
    if withdraw_owned:
        for action_key in owned:
            try:
                snapshot = json.loads(
                    queue.item_path(pool_module.CLAIMED, action_key).read_text())
            except (OSError, ValueError) as exc:
                return {"status": "resigning", "host": host, "owner": owner,
                        "gate_key": key,
                        "reason": f"owned claim unreadable for withdraw: {exc}",
                        "withdrawn": withdrawn, "broker": began}
            try:
                result = queue.withdraw(
                    action_key, reason=f"resign {owner}: {reason}", by=owner)
            except (OSError, ValueError,
                    pool_module.PoolContractError) as exc:
                return {"status": "resigning", "host": host, "owner": owner,
                        "gate_key": key,
                        "reason": f"withdrawal failed for {action_key[:12]}: {exc}",
                        "withdrawn": withdrawn, "broker": began}
            withdrawn.append({"action_key": action_key, **result,
                              "retry_owed": _pending_requeue(snapshot)})
    deadline = time.monotonic() + max(0.0, float(wait_s))
    census, census_error = live_loop_census(live)
    pending_note: str | None = None
    while True:
        terminals = {k: terminal_of(queue, k) is not None for k in owned}
        broker_now = broker_status(socket_path, call)
        scopes = (broker_now.get("active_scopes")
                  if isinstance(broker_now, dict) else None)
        parked = parked_markers(parked_root, gate_now)
        parked_names = {p.name for p in parked}
        acks = expected_markers(census, gate_now) <= parked_names
        missing_terminals = sorted(k[:12] for k, done in terminals.items() if not done)
        owed = sorted(w["action_key"][:12] for w in withdrawn if w["retry_owed"])
        if census_error is not None:
            pending_note = f"loop census unknown: {census_error}"
        elif missing_terminals:
            pending_note = ("attempts without terminal: "
                            + ", ".join(missing_terminals))
        elif not isinstance(scopes, int):
            pending_note = f"broker scope census unknown: {broker_now}"
        elif scopes != 0:
            pending_note = f"broker still holds {scopes} active scopes"
        elif owed:
            pending_note = ("retry-safe rows owed a budget-preserving requeue "
                            "(approved handoff hunk pending; operator "
                            "pool_reset meanwhile): " + ", ".join(owed))
        elif not acks:
            want = sorted(expected_markers(census, gate_now))
            pending_note = ("parked acks incomplete "
                            f"({len(parked_names & set(want))}/{len(want)} "
                            f"for epoch {key})")
        else:
            pending_note = None
        if pending_note is None:
            break
        if time.monotonic() >= deadline:
            break
        time.sleep(1.0)
    if pending_note is not None:
        return {"status": "resigning", "host": host, "owner": owner,
                "gate_key": key, "reason": pending_note,
                "withdrawn": withdrawn, "broker": began,
                "checked_unix": _utc()}
    return {"status": "resigned", "host": host, "owner": owner,
            "gate_key": key, "parked": len(parked_names),
            "withdrawn": withdrawn, "broker": began,
            "checked_unix": _utc(),
            "note": "terminals filed, scopes empty, exact-loop acks complete; "
                    "offers expire, nothing released on heartbeat loss"}


def status(
    host: str | None = None,
    *,
    roster_path: Path | None = None,
    gate: Path | None = None,
    queue_root: Path | None = None,
    socket_path: Path | None = None,
    broker_call: Callable[[dict], dict] | None = None,
) -> dict[str, Any]:
    """Gate + roster + fresh-offer state: one of the STALE-free wire states."""
    host, host_error = check_host(host or local_host())
    gate_path, parked_root = gate_paths(gate)
    gate_now = read_gate(gate_path)
    active, roster_detail = (check_roster_active(host, roster_path)
                            if host_error is None else (False, host_error))
    broker = broker_status(socket_path, broker_call)
    offer_fresh: bool | None = None
    if queue_root is not None:
        try:
            live = pool_module.PoolQueue(Path(queue_root)).offers(
                max_age_s=OFFER_TIMEOUT_S)
            offer_fresh = any(str(o.get("host")) == host for o in live)
        except (OSError, ValueError):
            offer_fresh = None
    if host_error is not None or (not active and gate_now is None
                                  and offer_fresh is None):
        state = "unknown"
    elif gate_now is not None:
        state = "draining" if active else "unavailable"
    elif not active:
        state = "unavailable"
    elif offer_fresh is True:
        state = "eligible"
    elif offer_fresh is False:
        state = "qualifying"
    else:
        state = "unknown"
    census, census_error = live_loop_census()
    return {
        "schema": SCHEMA,
        "host": host,
        "fleet_state": state,
        "roster_active": active,
        "roster_detail": roster_detail,
        "gate": gate_now,
        "gate_key": gate_key(gate_now),
        "parked_markers": len(parked_markers(parked_root, gate_now)),
        "live_loops": len(census),
        "loop_census": ("unknown" if census_error is not None
                        else sorted(f"{pid}" for pid, _ in census)),
        "offer_fresh": offer_fresh,
        "broker": broker,
        "checked_unix": _utc(),
    }


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    pj = sub.add_parser("join", help="qualify then open this host's gate")
    pj.add_argument("--reason", default="", help="why this host joins")
    pj.add_argument("--host", default=None)
    pj.add_argument("--queue-root", type=Path, default=None)
    pj.add_argument("--roster", type=Path, default=None)
    pj.add_argument("--gate", type=Path, default=None)
    pj.add_argument("--socket", type=Path, default=None)
    pj.add_argument("--expected-epoch", default=None,
                    help="compare-and-set against this gate epoch")
    pr = sub.add_parser("resign", help="fence, withdraw owned, prove completion")
    pr.add_argument("--reason", required=True, help="why this host resigns")
    pr.add_argument("--host", default=None)
    pr.add_argument("--queue-root", type=Path, default=None)
    pr.add_argument("--gate", type=Path, default=None)
    pr.add_argument("--socket", type=Path, default=None)
    pr.add_argument("--wait-s", type=float, default=120.0)
    pr.add_argument("--expected-epoch", default=None)
    pr.add_argument("--no-withdraw-owned", action="store_true")
    ps = sub.add_parser("status", help="gate, roster, offer, broker state")
    ps.add_argument("--host", default=None)
    ps.add_argument("--roster", type=Path, default=None)
    ps.add_argument("--gate", type=Path, default=None)
    ps.add_argument("--queue-root", type=Path, default=None)
    ps.add_argument("--socket", type=Path, default=None)
    args = ap.parse_args(argv)
    if args.cmd == "join":
        result = join(
            args.host, reason=args.reason, queue_root=args.queue_root,
            roster_path=args.roster, gate=args.gate, socket_path=args.socket,
            expected_changed_unix=args.expected_epoch,
        )
    elif args.cmd == "resign":
        result = resign(
            args.host, reason=args.reason, queue_root=args.queue_root,
            gate=args.gate, socket_path=args.socket, wait_s=args.wait_s,
            expected_changed_unix=args.expected_epoch,
            withdraw_owned=not args.no_withdraw_owned,
        )
    else:
        result = status(
            args.host, roster_path=args.roster, gate=args.gate,
            queue_root=args.queue_root, socket_path=args.socket,
        )
    print(json.dumps(result, indent=1, sort_keys=True, default=str))
    if result.get("status") in ("refused", "resigning"):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
