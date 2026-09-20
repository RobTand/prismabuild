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
- running work: the existing withdrawal ladder (``PoolQueue.withdraw``),
  the budget-preserving ``PoolQueue.plan_requeue`` + ``publish`` handoff, and
  holder ``finish``/reaper terminals. Resign never releases tokens on
  heartbeat loss and never edits queue records by hand.

Resign order: ``maintenance_begin`` under the broker mutex (the
linearization point: new scope ``create`` is refused from there, and new
claims are refused at the rename by the ``admission_open`` fence) → census
owned claims → withdraw/requeue while loops are still busy → repeated
rounds of census + withdraw + terminal wait until the owned set is
provably empty, broker ``active_scopes`` is empty, and exact-loop park acks
for this epoch are complete → ``resigned`` with evidence. ``resigned`` is
never returned after only a withdraw request.

Epoch compare-and-set is enforced inside the broker mutex
(``expected_changed_unix``): the CLI reads the gate only to send its
expectation, never to decide. A supervisor restart takes over an old
durable drain by presenting the explicit old epoch plus a live-supervisor
owner the broker verifies itself — never ``force_end``.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform as _platform
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


def roster_loop_args(host: str, roster_path: Path | None = None) -> list[str]:
    """This host's declared loop argv (shape authority stays the roster file)."""
    try:
        entry = roster_entry(host, roster_path)
    except ValueError as exc:
        raise ValueError(str(exc))
    if entry is None:
        raise ValueError(f"no fleet_boxes.json entry for {host}")
    return [str(a) for a in (entry.get("args") or [])]


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


def broker_call_func(
    payload: dict,
    socket_path: Path | None,
    broker_call: Callable[[dict], dict] | None,
) -> dict[str, Any]:
    if broker_call is None:
        from prismabuild.resource_scope import broker_request

        def broker_call(body: dict) -> dict:
            return broker_request(body, socket_path=socket_path or DEFAULT_SOCKET)

    return broker_call(dict(payload))

#: Reader-lease containment identifiers (documented in the reader-lifetime
#: API brief; imported from ``prismabuild.reader_lease`` when that lane has
#: merged, else these documented fallback values with the source recorded).
_READER_ATTESTATION_SCHEMA_FALLBACK = (
    "prismaquant.prismabuild.reader_scope_attestation.v1")
_READER_ATTESTATIONS_SUBDIR_FALLBACK = "broker-attestations"
_READER_LEASES_SUBDIR_FALLBACK = "leases"
_READER_RESIDENCY_SUBDIR_FALLBACK = "residency"


def _reader_lease():
    """The reader-lease module when its lane has merged, else None.

    No vendoring, no parallel reimplementation: enumeration, validation,
    and release run through the owning module whenever it is importable.
    """
    try:
        from prismabuild import reader_lease as module  # noqa: E402
    except ImportError:
        return None
    return module


def _leases_roots(queue: pool_module.PoolQueue,
                  residency_roots=None) -> list[Path]:
    module = _reader_lease()
    roots = [Path(queue.root) / _READER_RESIDENCY_SUBDIR_FALLBACK
             / _READER_LEASES_SUBDIR_FALLBACK]
    for extra in residency_roots or []:
        sub = _READER_LEASES_SUBDIR_FALLBACK
        if module is not None:
            try:
                return_roots = [Path(module.leases_root(queue, extra))]
            except (OSError, ValueError, AttributeError):
                return_roots = [Path(extra) / sub]
            roots.extend(return_roots)
        else:
            roots.append(Path(extra) / sub)
    return roots


def reader_refs_census(queue: pool_module.PoolQueue, host: str,
                       residency_roots=None) -> tuple[list[dict[str, Any]], str]:
    """This host's reader refs and how the answer was established.

    Returns (refs, state) where state is ``drained-absent`` (no leases
    namespace at all — provable without the module), ``enumerated`` (real
    census through the owning module), or ``unknown-needs-reader-lease``
    (pins may exist but cannot be enumerated here). Directory presence is
    never read as drained: consumer pins may sit under it.
    """
    module = _reader_lease()
    if module is not None:
        try:
            refs = module.refs_for_holder(queue, host)
        except (OSError, ValueError) as exc:
            return [], f"unknown-refs-unreadable: {exc}"
        return ([dict(r) for r in refs], "enumerated")
    for root in _leases_roots(queue, residency_roots):
        try:
            if not root.exists():
                continue
            consumers = [e for e in os.scandir(root) if e.is_dir()]
        except OSError as exc:
            return [], f"unknown-leases-unreadable: {root}: {exc}"
        if consumers:
            return [], ("unknown-needs-reader-lease: pins may exist under "
                        f"{root}")
    return [], "drained-absent"


def reader_refs_gate(queue: pool_module.PoolQueue, host: str,
                     residency_roots=None) -> tuple[bool, str, dict[str, Any]]:
    """Whether this host holds reader refs that must drain before RESIGNED.

    Consumer side only: attestation writing and ref reclaim are the lease
    worker's automatic production path (PR #730); this lane only consumes
    the proof. Drained when the leases namespace is provably absent, or
    when an enumerated census names no refs for this host. Anything else —
    refs remaining (their automatic path drains them; resign waits) or an
    unprovable namespace — retains the fence with the reason. Never
    reported drained on heartbeat loss, missing reads, or another host's
    refs.
    """
    refs, state = reader_refs_census(queue, host, residency_roots)
    if state == "drained-absent":
        return True, state, {"refs": []}
    if state == "enumerated":
        if not refs:
            return True, "refs-drained", {"refs": []}
        return False, "refs-retained", {
            "refs": [{"ref_id": r.get("ref_id"),
                      "consumer_action_key": r.get("consumer_action_key"),
                      "pin_id": r.get("pin_id")} for r in refs]}
    return False, state, {"refs": []}


def claimed_census(
    queue: pool_module.PoolQueue, host: str
) -> tuple[list[dict[str, Any]], list[str]]:
    """(owned claim snapshots, unknown paths). Malformed is unknown, never empty."""
    owned: list[dict[str, Any]] = []
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
        mine = record.get("claimed_host") == host
        if not mine:
            try:
                lease = json.loads(queue.lease_path(action_key).read_text())
            except (OSError, ValueError) as exc:
                unknown.append(f"{path.name}: lease unreadable ({exc})")
                continue
            if not isinstance(lease, dict):
                unknown.append(f"{path.name}: non-object lease")
                continue
            mine = lease.get("host") == host
        if mine:
            owned.append({"action_key": action_key,
                          "published_unix": record.get("published_unix"),
                          "attempts": record.get("attempts"),
                          "claimed_by": record.get("claimed_by"),
                          "claimed_unix": record.get("claimed_unix"),
                          "max_attempts": record.get("max_attempts"),
                          "retry_safe": record.get("retry_safe"),
                          "record": dict(record)})
    owned.sort(key=lambda snap: str(snap["action_key"]))
    return owned, unknown


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


def terminal_of(
    queue: pool_module.PoolQueue, snapshot: dict[str, Any]
) -> tuple[str, dict[str, Any]] | None:
    """The terminal for this exact attempt: (kind, record) or None.

    Kind is ``done``, ``failed``, or ``withdrawn`` by path — never by the
    status string, which a covered finish can also spell ``withdrawn``.
    ``done/``/``failed/`` terminals must carry this attempt's generation and
    identity (``published_unix``, ``claimed_by``, ``claimed_unix``, and an
    ``attempts`` of exactly one more than the snapshot): any ``done/`` of
    the same key is somebody else's verdict. A withdrawn attempt concludes
    through the holder's own ``finish``, which entombs the claim and leaves
    the filed ``withdrawn/`` decision of this generation as the terminal.
    """
    key = str(snapshot["action_key"])
    for state in (pool_module.DONE, pool_module.FAILED):
        try:
            record = json.loads(queue.item_path(state, key).read_text())
        except (OSError, ValueError):
            continue
        if isinstance(record, dict) and _terminal_matches(record, snapshot):
            return ("done" if state == pool_module.DONE else "failed", record)
    try:
        claimed_gone = not queue.item_path(pool_module.CLAIMED, key).exists()
    except OSError:
        return None
    if not claimed_gone:
        return None
    try:
        withdrawn = json.loads(
            queue.item_path(pool_module.WITHDRAWN, key).read_text())
    except (OSError, ValueError):
        return None
    if (isinstance(withdrawn, dict)
            and withdrawn.get("published_unix") == snapshot.get("published_unix")):
        return ("withdrawn", withdrawn)
    return None


def _terminal_matches(record: dict[str, Any], snapshot: dict[str, Any]) -> bool:
    if record.get("published_unix") != snapshot.get("published_unix"):
        return False
    if (record.get("claimed_by") != snapshot.get("claimed_by")
            or record.get("claimed_unix") != snapshot.get("claimed_unix")):
        return False
    attempts = record.get("attempts")
    prior = snapshot.get("attempts")
    if isinstance(attempts, int) and isinstance(prior, int):
        return attempts in (prior, prior + 1)
    return True


def _pending_requeue(snapshot: dict[str, Any]) -> bool:
    """Whether this withdrawn row still owes a budget-preserving successor."""
    attempts = snapshot.get("attempts")
    limit = snapshot.get("max_attempts", pool_module.DEFAULT_MAX_ATTEMPTS)
    return (
        snapshot.get("retry_safe") is True
        and type(attempts) is int
        and attempts >= 0
        and type(limit) is int
        and attempts + 1 < limit
    )


def _mount_identity(path: Path) -> dict[str, Any]:
    """This path's mount (source, fstype, mountpoint) from /proc/mounts."""
    resolved = str(path.resolve())
    best: dict[str, Any] | None = None
    try:
        mounts = Path("/proc/mounts").read_text().splitlines()
    except OSError as exc:
        return {"error": f"/proc/mounts unreadable: {exc}"}
    for line in mounts:
        parts = line.split()
        if len(parts) < 3:
            continue
        mountpoint = parts[1].encode().decode("unicode_escape", "replace") \
            if "\\" in parts[1] else parts[1]
        if resolved == mountpoint or resolved.startswith(mountpoint.rstrip("/") + "/"):
            if best is None or len(mountpoint) > len(str(best["mountpoint"])):
                best = {"source": parts[0], "fstype": parts[2],
                        "mountpoint": mountpoint}
    return best or {"error": f"no mount owns {resolved}"}


def check_shared_mount(
    host: str, queue_root: Path, roster_path: Path | None = None
) -> tuple[bool, dict[str, Any]]:
    """Prove the queue root is the shared fleet namespace, not a local dir.

    A locally writable directory does not prove shared access. The queue
    root must sit on the fleet's shared export (NFS), except on the box
    whose roster entry declares the storage/tiers roles, where the local
    ZFS dataset that export serves is the same namespace. Queue identity
    markers (ready/claimed/workers/cas) must all be present.
    """
    root = Path(queue_root)
    mount = _mount_identity(root)
    if "error" in mount:
        return False, {"mount": mount}
    try:
        entry = roster_entry(host, roster_path)
    except ValueError as exc:
        return False, {"mount": mount, "reason": str(exc)}
    roles = entry.get("roles") if isinstance(entry, dict) else None
    serves_files = isinstance(roles, dict) and bool(
        set(roles) & {"storage", "tiers"})
    fstype = str(mount.get("fstype"))
    if fstype in ("nfs", "nfs4"):
        shared = True
        how = f"shared {fstype} export {mount.get('source')}"
    elif fstype == "zfs" and serves_files:
        shared = True
        how = (f"file-server local dataset {mount.get('source')} "
               f"at {mount.get('mountpoint')}")
    else:
        return False, {"mount": mount, "reason": (
            f"{root} is on fstype {fstype} ({mount.get('source')}), not the "
            "shared fleet export, and this host declares no storage/tiers role")}
    missing = [name for name in ("ready", "claimed", "workers")
               if not (root / name).is_dir()]
    if missing:
        try:
            root.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            return False, {"mount": mount, "how": how,
                           "reason": f"queue root not creatable: {exc}"}
        missing = [name for name in ("ready", "claimed", "workers")
                   if not (root / name).is_dir()]
        if missing:
            return False, {"mount": mount, "how": how,
                           "reason": f"queue identity markers absent: {missing}"}
    return True, {"mount": mount, "how": how}


def worker_evidence(host: str, roster_path: Path | None = None) -> dict[str, Any]:
    """The existing worker qualification evidence for this box, enforced.

    Reuses the exact preflight derivation (platform key from live
    OS/machine/accelerator evidence) and the toolchain executable identity
    the nonportable contract verifies: the roster class must agree with the
    attested machine (gb10 is aarch64 CUDA, x86 is x86_64 with no CUDA
    suffix), and the loop interpreter must be a readable regular file whose
    digest is recorded. Unknown backends refuse; tags never stand in for
    evidence.
    """
    try:
        from prismabuild import core as _core
    except ImportError as exc:
        return {"ok": False, "reason": f"worker contract unreadable: {exc}"}
    try:
        args = roster_loop_args(host, roster_path)
    except ValueError as exc:
        return {"ok": False, "reason": str(exc)}
    klass = "gb10"
    if "--class" in args:
        try:
            klass = args[args.index("--class") + 1]
        except IndexError:
            return {"ok": False, "reason": "roster --class has no value"}
    python = "/usr/bin/python3"
    if "--python" in args:
        try:
            python = args[args.index("--python") + 1]
        except IndexError:
            return {"ok": False, "reason": "roster --python has no value"}
    try:
        evidence = _core._collect_worker_evidence()
        platform_key = _core._platform_key_from_evidence(evidence)
    except Exception as exc:  # noqa: BLE001 — preflight refuses; so do we
        return {"ok": False, "reason": f"worker evidence refused: {exc}"}
    machine = str(evidence.get("machine", ""))
    if klass == "gb10":
        if machine != "aarch64" or "-sm" not in platform_key:
            return {"ok": False, "reason": (
                f"roster class gb10 disagrees with attested {platform_key}")}
    elif klass == "x86":
        if machine != "x86_64" or "-sm" in platform_key:
            return {"ok": False, "reason": (
                f"roster class x86 disagrees with attested {platform_key}")}
    else:
        return {"ok": False, "reason": f"unknown roster class {klass!r}"}
    try:
        exe = Path(python)
        if not exe.is_file() or exe.is_symlink() and not exe.exists():
            return {"ok": False, "reason": f"loop python not a file: {python}"}
        digest = hashlib.sha256(exe.read_bytes()).hexdigest()
        size = exe.stat().st_size
    except OSError as exc:
        return {"ok": False, "reason": f"loop python unreadable: {exc}"}
    try:
        from prismabuild import cpu_topology

        tiers = cpu_topology.inherited_tiers()
    except (ImportError, OSError) as exc:
        tiers = {"error": str(exc)}
    try:
        from prismabuild import container_images

        inventory: Any = container_images.InventoryCache().get()
        inventory = list(inventory) if inventory is not None else None
    except (ImportError, OSError, ValueError) as exc:
        inventory = {"error": str(exc)}
    return {"ok": True, "platform_key": platform_key,
            "machine": machine, "class": klass,
            "python": {"path": python, "sha256": digest, "bytes": size},
            "cpu_tiers_present": isinstance(tiers, dict) and "error" not in tiers,
            "container_inventory": inventory}


def _declared_demand(host: str, roster_path: Path | None) -> dict[str, int]:
    """Declared capacity parsed from the roster loop args (never invented)."""
    args = roster_loop_args(host, roster_path)
    demand: dict[str, int] = {"cpu": 1}
    if "--gpu" in args:
        demand["gpu"] = 1
    if "--mem-gb" in args:
        try:
            demand["mem_gb"] = int(args[args.index("--mem-gb") + 1])
        except (ValueError, IndexError) as exc:
            raise ValueError(f"roster mem-gb unparsable for {host}: {exc}")
    return demand


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
    shared, mount_info = check_shared_mount(host, Path(queue_root), roster_path)
    checks["shared_mount"] = mount_info
    if not shared:
        return {"ok": False, "reason": mount_info.get("reason", "not shared"),
                "checks": checks}
    root = Path(queue_root)
    probe = root / f".membership-probe-{os.getpid()}"
    try:
        probe.write_text(json.dumps({"host": host, "unix": _utc()}))
        assert json.loads(probe.read_text())["host"] == host
        probe.unlink()
        checks["shared_namespace_rw"] = True
    except (OSError, ValueError, AssertionError) as exc:
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
    evidence = worker_evidence(host, roster_path)
    checks["worker_evidence"] = evidence
    if not evidence.get("ok"):
        return {"ok": False, "reason": evidence.get("reason", "no worker evidence"),
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


def _broker_mutate(
    op: str,
    owner: str,
    reason: str,
    expected_changed_unix: Any,
    socket_path: Path | None,
    broker_call: Callable[[dict], dict] | None,
) -> dict[str, Any]:
    """Send a mutating broker op with the epoch; the broker enforces the CAS."""
    payload: dict[str, Any] = {"op": op, "owner": owner, "reason": reason}
    if expected_changed_unix is not None:
        payload["expected_changed_unix"] = expected_changed_unix
    return broker_call_func(payload, socket_path, broker_call)


def join(
    host: str | None = None,
    *,
    reason: str = "",
    queue_root: Path | None = None,
    roster_path: Path | None = None,
    gate: Path | None = None,
    socket_path: Path | None = None,
    broker_call: Callable[[dict], dict] | None = None,
    runtime_root: Path | None = None,
) -> dict[str, Any]:
    """Validate, then open this host's gate. The gate is untouched on refusal.

    The current gate epoch is read only to send as the broker-enforced
    expectation (a supervisor restart takes over an old durable drain this
    way, presenting the explicit old epoch — never force_end). The broker
    verdict rules; a moved gate refuses there, not here.
    """
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
    try:
        ended = _broker_mutate("maintenance_end", owner, reason or "join",
                               gate_epoch(gate_now), socket_path, broker_call)
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
    withdraw_owned: bool = True,
    residency_roots: list[str | Path] | None = None,
) -> dict[str, Any]:
    """Close the gate, withdraw owned claims while busy, prove completion.

    The per-key claim fence narrows the poll-check→rename race but cannot
    lock the broker's gate: a claim can still win the rename after ``begin``.
    Such a late claim cannot execute — broker scope ``create`` refuses under
    the mutex and the loop's cleanup path releases it — so completion is
    proven, not assumed: repeated rounds of census + handoff + terminal wait,
    with park acks bracketing the final census (acks before and after, same
    live set, same gate epoch, same supervisor incarnation) plus empty
    broker scopes. Anything unprovable stays ``resigning`` with the reason.
    """
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
    try:
        began = _broker_mutate("maintenance_begin", owner, reason,
                               gate_epoch(gate_before), socket_path, broker_call)
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
    epoch = gate_epoch(gate_now)
    key = gate_key(gate_now)
    if queue_root is None:
        return {"status": "resigning", "host": host, "owner": owner,
                "gate_key": key,
                "reason": "no queue root: owned claims unobservable, fence retained",
                "broker": began}
    queue = pool_module.PoolQueue(Path(queue_root))
    deadline = time.monotonic() + max(0.0, float(wait_s))
    handled: dict[str, dict[str, Any]] = {}
    pending_note: str | None = None
    parked_names: set[str] = set()
    armed = False
    armed_census: frozenset[tuple[int, str]] = frozenset()
    while True:
        # The gate must still be our epoch and the supervision unchanged:
        # anyone else's drain, or a supervisor replacement, ends this
        # resignation's authority to declare completeness.
        gate_check = read_gate(gate_path)
        if gate_check is None or gate_epoch(gate_check) != epoch:
            pending_note = "gate epoch moved under this resignation; stopping"
            break
        incarnation, incarnation_error = supervisor_incarnation()
        if incarnation_error is not None or incarnation != owner:
            pending_note = ("supervisor incarnation changed under this "
                            f"resignation ({incarnation_error or incarnation}); "
                            "stopping")
            break
        owned, census_unknown = claimed_census(queue, host)
        # Evaluated fresh every round: a note from the previous round must
        # never suppress this round's checks.
        pending_note = None
        if census_unknown:
            armed = False
            pending_note = ("claim census unknown; fence and resources retained: "
                            + "; ".join(census_unknown))
        else:
            fresh = [snap for snap in owned
                     if snap["action_key"] not in handled]
            handoff_error: str | None = None
            if withdraw_owned:
                for snap in fresh:
                    action_key = str(snap["action_key"])
                    try:
                        plan = None
                        if _pending_requeue(snap):
                            # Built BEFORE the withdraw: a claim that cannot
                            # be re-published must be left running, and the
                            # successor may only be published after the
                            # original attempt's exact terminal (publishing
                            # while the holder is live races its finish,
                            # whose requeue disposition would overwrite it).
                            plan = queue.plan_requeue(snap["record"])
                        result = queue.withdraw(
                            action_key,
                            reason=f"resign {owner}: {reason}", by=owner)
                        handled[action_key] = {"handoff": "withdrawn",
                                               "snapshot": _snap_id(snap),
                                               "withdraw": result.get("status")}
                        if plan is not None:
                            handled[action_key]["plan"] = plan
                    except (OSError, ValueError,
                            pool_module.PoolContractError) as exc:
                        # A holder that concluded between the census and the
                        # withdraw wins honestly: adopt its terminal below
                        # instead of failing the resignation.
                        try:
                            concluded = not queue.item_path(
                                pool_module.CLAIMED, action_key).exists()
                        except OSError:
                            concluded = False
                        if concluded:
                            handled[action_key] = {
                                "handoff": "concluded_by_holder",
                                "snapshot": _snap_id(snap)}
                            continue
                        handoff_error = (
                            f"handoff failed for {action_key[:12]}: {exc}")
                        break
            else:
                for snap in fresh:
                    handled[str(snap["action_key"])] = {
                        "handoff": "observed", "snapshot": _snap_id(snap)}
            if handoff_error is not None:
                armed = False
                pending_note = handoff_error
            else:
                broker_now = broker_status(socket_path,
                                           _call_as_request(socket_path,
                                                            broker_call))
                scopes = (broker_now.get("active_scopes")
                          if isinstance(broker_now, dict) else None)
                census, census_error = live_loop_census(live)
                parked = parked_markers(parked_root, gate_now)
                parked_names = {p.name for p in parked}
                acks = expected_markers(census, gate_now) <= parked_names
                missing = sorted(
                    snap["action_key"][:12] for snap in owned
                    if terminal_of(queue, snap) is None)
                # Settle planned successors: only after the original
                # attempt's exact terminal, and only when no ready occupant
                # (a holder that self-requeued is adopted by linkage check).
                for action_key, entry in handled.items():
                    plan = entry.get("plan")
                    if plan is None or entry.get("published"):
                        continue
                    snap = next((s for s in owned
                                 if s["action_key"] == action_key), None)
                    probe = snap if snap is not None else {
                        "action_key": action_key, **entry.get("snapshot", {})}
                    terminal = terminal_of(queue, probe)
                    if terminal is None or terminal[0] != "withdrawn":
                        continue
                    try:
                        ready = json.loads(queue.item_path(
                            pool_module.READY, action_key).read_text())
                    except (OSError, ValueError):
                        ready = None
                    if isinstance(ready, dict):
                        prior = entry["snapshot"].get("attempts")
                        if (ready.get("action_key") == action_key
                                and ready.get("attempts") == (
                                    prior + 1 if isinstance(prior, int) else None)):
                            entry["published"] = "adopted-holder-successor"
                            continue
                        armed = False
                        pending_note = (f"unexpected ready occupant for "
                                        f"{action_key[:12]}; stopping")
                        break
                    try:
                        queue.publish(**plan["arguments"],
                                      preempted_claim=plan["snapshot"],
                                      handoff_by=owner)
                    except (OSError, ValueError,
                            pool_module.PoolContractError) as exc:
                        armed = False
                        pending_note = (f"successor publish failed for "
                                        f"{action_key[:12]}: {exc}")
                        break
                    entry["published"] = "resign-successor"
                if pending_note is None:
                    if census_error is not None:
                        armed = False
                        pending_note = f"loop census unknown: {census_error}"
                    elif missing:
                        armed = False
                        pending_note = ("attempts without exact terminal: "
                                        + ", ".join(missing))
                    elif not isinstance(scopes, int):
                        armed = False
                        pending_note = f"broker scope census unknown: {broker_now}"
                    elif scopes != 0:
                        armed = False
                        pending_note = f"broker still holds {scopes} active scopes"
                    else:
                        refs_drained, refs_state, refs_detail = reader_refs_gate(
                            queue, host, residency_roots)
                        if not refs_drained:
                            armed = False
                            pending_note = (
                                f"reader refs not drained: {refs_state}; "
                                f"{refs_detail}")
                        elif not acks:
                            armed = False
                            want = sorted(expected_markers(census, gate_now))
                            pending_note = ("parked acks incomplete "
                                            f"({len(parked_names & set(want))}/{len(want)} "
                                            f"for epoch {key})")
                        else:
                            # Bracketed proof: acks complete in this round AND the
                            # previous one, with an identical live set and epoch
                            # between them — a loop that parked cannot claim, and a
                            # claim that won the fence race would appear in the
                            # re-census (fenced at the rename, unfenceable at broker
                            # create, released by loop cleanup) before SUCCESS.
                            current = frozenset(census)
                            if armed and current == armed_census:
                                pending_note = None
                                break
                            armed, armed_census = True, current
                            pending_note = ("proof arming: complete in this round, "
                                            "verifying one more round")
        if time.monotonic() >= deadline:
            if pending_note is None or pending_note.startswith("proof arming"):
                pending_note = "deadline before bracketed proof completed"
            break
        time.sleep(1.0)
    if pending_note is not None:
        return {"status": "resigning", "host": host, "owner": owner,
                "gate_key": key, "reason": pending_note,
                "handled": handled,
                "broker": began, "checked_unix": _utc()}
    return {"status": "resigned", "host": host, "owner": owner,
            "gate_key": key, "parked": len(parked_names),
            "handled": handled, "broker": began, "checked_unix": _utc(),
            "note": "owned set provably empty: exact terminals filed, scopes "
                    "empty, bracketed exact-loop acks around a stable census, "
                    "epoch and incarnation; offers expire, nothing "
                    "released on heartbeat loss"}


def _snap_id(snap: dict[str, Any]) -> dict[str, Any]:
    ids = {k: snap.get(k) for k in
           ("action_key", "published_unix", "attempts", "claimed_by",
            "claimed_unix", "max_attempts", "retry_safe")}
    return ids


def _call_as_request(
    socket_path: Path | None,
    broker_call: Callable[[dict], dict] | None,
) -> Callable[[dict], dict]:
    if broker_call is not None:
        return broker_call

    def call(payload: dict) -> dict:
        return broker_call_func(payload, socket_path, None)

    return call


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
    pr = sub.add_parser("resign", help="fence, withdraw owned, prove completion")
    pr.add_argument("--reason", required=True, help="why this host resigns")
    pr.add_argument("--host", default=None)
    pr.add_argument("--queue-root", type=Path, default=None)
    pr.add_argument("--gate", type=Path, default=None)
    pr.add_argument("--socket", type=Path, default=None)
    pr.add_argument("--wait-s", type=float, default=120.0)
    pr.add_argument("--no-withdraw-owned", action="store_true")
    pr.add_argument("--residency-root", dest="residency_roots", action="append",
                    default=None, help="extra residency root holding reader "
                    "leases (repeatable); the queue-anchored default is "
                    "always checked")
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
        )
    elif args.cmd == "resign":
        result = resign(
            args.host, reason=args.reason, queue_root=args.queue_root,
            gate=args.gate, socket_path=args.socket, wait_s=args.wait_s,
            withdraw_owned=not args.no_withdraw_owned,
            residency_roots=args.residency_roots,
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
