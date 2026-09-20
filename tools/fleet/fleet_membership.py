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
owned claims → withdraw only retry-owed rows (uninterruptible work is
retained and drained to its natural terminal; resign never cancels) →
repeated rounds of census + handoff + exact-terminal wait until the owned
set plus handled-but-unclaimed attempts are provably concluded, broker
``active_scopes`` is empty, reader refs are drained, and exact-loop park
acks bracket a stable census → ``resigned`` with evidence. ``resigned`` is
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
import resource_broker as broker_mod  # noqa: E402
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
        # An unreadable census is unknown, never an empty set of claims.
        # Path.glob suppresses directory errors; materialize the names
        # with scandir so resignation retains its gate on an I/O failure.
        with os.scandir(claimed_dir) as entries:
            names = sorted(entry.name for entry in entries
                           if entry.name.endswith(".json"))
    except OSError as exc:
        return [], [f"claimed directory not listable: {exc}"]
    paths = [claimed_dir / name for name in names]
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
            and isinstance(withdrawn.get("published_unix"), (int, float))
            and not isinstance(withdrawn.get("published_unix"), bool)
            and withdrawn.get("published_unix") == snapshot.get("published_unix")
            and isinstance(snapshot.get("claimed_by"), str)
            and snapshot.get("claimed_by")
            and withdrawn.get("claimed_by") == snapshot.get("claimed_by")
            and isinstance(snapshot.get("claimed_unix"), (int, float))
            and not isinstance(snapshot.get("claimed_unix"), bool)
            and withdrawn.get("claimed_unix") == snapshot.get("claimed_unix")):
        return ("withdrawn", withdrawn)
    return None


def _terminal_matches(record: dict[str, Any], snapshot: dict[str, Any]) -> bool:
    """Whether this terminal record concludes exactly the snapshotted attempt.

    Fail-closed exact proof: every identity field must be fully typed on
    both sides (bools are never valid numbers here), and the terminal must
    carry the actual transition counter — exactly one more than the
    snapshot's. A terminal with a missing, malformed, or non-advanced
    counter (stale generation, current-attempt number, or a counter-less
    archive record) never matches, however many other fields agree.
    """
    published = snapshot.get("published_unix")
    if (not isinstance(published, (int, float))
            or isinstance(published, bool)):
        return False
    if record.get("published_unix") != published:
        return False
    claimed_by = snapshot.get("claimed_by")
    if (not isinstance(claimed_by, str) or not claimed_by
            or record.get("claimed_by") != claimed_by):
        return False
    claimed_unix = snapshot.get("claimed_unix")
    if (not isinstance(claimed_unix, (int, float))
            or isinstance(claimed_unix, bool)
            or record.get("claimed_unix") != claimed_unix):
        return False
    prior = snapshot.get("attempts")
    attempts = record.get("attempts")
    if type(prior) is not int or type(attempts) is not int:
        return False
    return attempts == prior + 1


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
    held: Any = None,
    gpu_sample: Any = None,
    mem_gb: Any = None,
    load1: Any = None,
    now: float | None = None,
) -> dict[str, Any]:
    """Structured join qualification; every check fail-closed with a reason.

    JOIN validates runtime, mounts, identity, observation, and enforcement.
    It never decides per-action capacity: foreign or held load reduces the
    observed offer (recorded here) and per-action admission decides what
    fits — a worker serving external load (e.g. vLLM) may still accept
    compatible CPU work. The two hard refusals stay: GPU declared without
    trusted broker evidence, and unknown backends/capabilities. ``held``,
    ``gpu_sample``, ``mem_gb``, ``load1``, and ``now`` default to live
    readings; explicit values are the test seam for a busy box (same
    pattern as the sealed-identity stub in preemption tests).
    """
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
        observe_kwargs: dict[str, Any] = {}
        if gpu_sample is not None:
            observe_kwargs["gpu_sample"] = gpu_sample
        if mem_gb is not None:
            observe_kwargs["mem_gb"] = mem_gb
        if load1 is not None:
            observe_kwargs["load1"] = load1
        if now is not None:
            observe_kwargs["now"] = now
        observed = box_capacity.observe(
            declared, held if held is not None else {}, **observe_kwargs)
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
        # Foreign or held load never refuses the join: it is recorded in the
        # observed offer and per-action admission decides what fits. A GPU
        # reduced to zero by foreign holders is an explicit deferral, never
        # a silent admission — loops re-observe every poll and admit GPU
        # work only on fresh unattributed capacity.
        if declared.get("gpu", 0) > 0 and observed.capacity.get("gpu", 0) <= 0:
            checks["offer"]["gpu_admission"] = (
                "deferred-to-per-action-admission: no GPU capacity in the "
                "observed offer")
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
    """Send a mutating broker op with the epoch; the broker enforces the CAS.

    Each op carries exactly the fields ``Authority.admin`` allows: ``reason``
    rides only ``maintenance_begin``/``maintenance_takeover`` — sending it
    on ``maintenance_end``/``maintenance_force_end`` is refused as invalid
    maintenance fields, so it is never sent there.
    """
    payload: dict[str, Any] = {"op": op, "owner": owner}
    if op in {"maintenance_begin", "maintenance_takeover"}:
        payload["reason"] = reason
    if expected_changed_unix is not None:
        payload["expected_changed_unix"] = expected_changed_unix
    return broker_call_func(payload, socket_path, broker_call)


def _unsettled_owed_keys(queue: pool_module.PoolQueue,
                         owed: list[dict[str, Any]]) -> list[str]:
    """Owed rows that still block a gate open: everything but discharged exact.

    An exact successor discharges only with its complete typed lineage
    (``lineage_status``) AND the original attempt's exact withdrawn
    terminal. Foreign/unknown occupants, waiting holders (no terminal
    yet), and unpublishable rows all stay unsettled — JOIN keeps the
    fence, resign keeps waiting. Never raises: unreadable lineage reads
    as unsettled (fail-closed).
    """
    unsettled: list[str] = []
    for row in owed:
        try:
            action_key = str(row.get("action_key") or "")
            snapshot = {"action_key": action_key, **row.get("snapshot", {})}
            revive_by = str(row.get("revive_by") or "")
            status = lineage_status(queue, snapshot, revive_by)
        except Exception:
            try:
                unsettled.append(str(row.get("action_key", ""))[:12])
            except (ValueError, TypeError):
                pass
            continue
        terminal = status.get("terminal")
        if status.get("successor_exact") and terminal is not None and (
                terminal[0] == "withdrawn"):
            continue  # discharged: exact lineage + withdrawn terminal
        try:
            unsettled.append(str(row["action_key"])[:12])
        except (KeyError, ValueError, TypeError):
            continue
    return sorted(unsettled)


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
    held: Any = None,
    gpu_sample: Any = None,
    mem_gb: Any = None,
    load1: Any = None,
    now: float | None = None,
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
                          runtime_root=runtime_root, held=held,
                          gpu_sample=gpu_sample, mem_gb=mem_gb, load1=load1,
                          now=now)
    if not checks["ok"]:
        return {"status": "refused", "phase": "qualification", "host": host,
                "owner": owner, "reason": checks["reason"], "checks": checks}
    gate_path, _ = gate_paths(gate)
    gate_now = read_gate(gate_path)
    if gate_now is None:
        unsettled: list[str] = []
        unknown: list[str] = []
        if queue_root is not None:
            try:
                queue_here = pool_module.PoolQueue(Path(queue_root))
                owed, skipped = resume_owed(queue_here, host, owner)
                unsettled = _unsettled_owed_keys(queue_here, owed)
                unknown = sorted(skipped)
            except (OSError, ValueError):
                unsettled = []
                unknown = ["queue census unreadable"]
        return {"status": "already_joined", "host": host, "owner": owner,
                "checks": checks,
                "unsettled_membership_rows": unsettled,
                "unsettled_unknown_rows": unknown}
    if queue_root is not None:
        queue_here = pool_module.PoolQueue(Path(queue_root))
        owed_here, skipped_here = resume_owed(queue_here, host, owner)
        if skipped_here:
            # Unknown is not settled: an unreadable directory, a corrupt
            # decision, an unknown prior owner, or an unplannable
            # membership row keeps the fence. Other hosts' and operator
            # rows never reach `skipped` (lane-irrelevant, ignored).
            return {"status": "refused", "phase": "unsettled-unknown",
                    "host": host, "owner": owner,
                    "reason": "membership rows unreadable or unrevivable: "
                              + "; ".join(sorted(skipped_here)),
                    "checks": checks}
        unsettled_here = _unsettled_owed_keys(queue_here, owed_here)
        if unsettled_here:
            # Ending this drain would clear unsettled handoff obligations:
            # resign (adopt and settle) first, then join. Exact successors
            # with withdrawn terminals discharge; foreign/unknown and
            # waiting holders stay unsettled.
            return {"status": "refused", "phase": "unsettled-handoff",
                    "host": host, "owner": owner,
                    "reason": "membership drain holds unsettled handoffs: "
                              + ", ".join(unsettled_here),
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
        # A previous resign's drain may still be in force under a dead
        # incarnation (this process is its restart, or another CLI died
        # here). Take it over through the broker mutex — but never by
        # parsing refusal prose: the transport (`broker_request` over the
        # Unix socket) converts every broker refusal into `OSError`, so no
        # Python exception type survives it. Instead read the current gate
        # and, only when it names a closed foreign membership drain whose
        # supervision is provably gone, present this live owner with the
        # exact epoch just read. The broker re-verifies dead/live/epoch
        # under its mutex and stays authoritative: operator/upgrade holds,
        # live old supervisors, and stale epochs refuse there.
        gate_now = read_gate(gate_path)
        if gate_now is None:
            return {"status": "refused", "phase": "maintenance_begin",
                    "host": host, "owner": owner,
                    "reason": f"{type(exc).__name__}: {exc}; "
                              "gate opened under a refused begin; re-read and retry"}
        held = gate_now.get("owner") if isinstance(gate_now, dict) else None
        gone, _ = (_owner_gone(host, held) if isinstance(held, str)
                   else (False, "no owner"))
        if held == owner or not isinstance(held, str) or not gone:
            return {"status": "refused", "phase": "maintenance_begin",
                    "host": host, "owner": owner,
                    "reason": f"{type(exc).__name__}: {exc}"}
        try:
            took = _broker_mutate("maintenance_takeover", owner, reason,
                                  gate_epoch(gate_now), socket_path, broker_call)
        except (OSError, ValueError, PermissionError, RuntimeError) as exc2:
            return {"status": "refused", "phase": "maintenance_takeover",
                    "host": host, "owner": owner,
                    "reason": f"{type(exc2).__name__}: {exc2}"}
        began = took
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
            # Crash-resume: withdrawn membership rows owed a successor are
            # authoritative queue state, not process memory. A resigner that
            # died after withdraw (before terminal or publish) leaves rows
            # no CLAIMED scan can see; they rejoin here every round until
            # settled, including under a new supervisor incarnation.
            resumed, resume_skipped = ([], [])
            if withdraw_owned:
                resumed, resume_skipped = resume_owed(queue, host, owner)
            if resume_skipped:
                armed = False
                pending_note = ("unrevivable membership rows; fence retained: "
                                + "; ".join(resume_skipped))
            for row in resumed:
                if (row["action_key"] not in handled
                        and not any(s["action_key"] == row["action_key"]
                                    for s in owned)):
                    handled[row["action_key"]] = {
                        "handoff": "resumed-intent",
                        "snapshot": _snap_id({**row, **row.get("snapshot", {})}),
                        "plan": row["plan"], "revive_by": row["revive_by"]}
            fresh = [snap for snap in owned
                     if snap["action_key"] not in handled]
            handoff_error: str | None = None
            if withdraw_owned:
                for snap in fresh:
                    action_key = str(snap["action_key"])
                    if not _pending_requeue(snap):
                        # Graceful resign never cancels unrepeatable work:
                        # retry-unsafe or budget-exhausted rows are retained
                        # and drained until their natural exact terminal.
                        # Explicit destructive cancellation is a separate
                        # operator input (withdraw), never implicit resign.
                        # Budgets are never reset to enable departure.
                        handled[action_key] = {
                            "handoff": "retained-uninterruptible",
                            "snapshot": _snap_id(snap)}
                        continue
                    try:
                        # Built BEFORE the withdraw: a claim that cannot
                        # be re-published must be left running, and the
                        # successor may only be published after the
                        # original attempt's exact terminal (publishing
                        # while the holder is live races its finish,
                        # whose requeue disposition would overwrite it).
                        # The withdraw carries the plan's snapshot so the
                        # queue can prove the handoff itself -- live claim
                        # still that attempt, restart permission with
                        # remaining budget and lineage -- and persist its
                        # identity in the immutable decision; a shape-only
                        # owner string proves nothing and files ordinary.
                        plan = queue.plan_requeue(snap["record"])
                        result = queue.withdraw(
                            action_key,
                            reason=f"resign {owner}: {reason}", by=owner,
                            membership_handoff=plan["snapshot"])
                        handled[action_key] = {"handoff": "withdrawn",
                                               "snapshot": _snap_id(snap),
                                               "withdraw": result.get("status")}
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
                # Proof census: owned claims UNION handled-but-no-longer-claimed
                # attempts. A handled attempt whose claim left (finished,
                # entombed, reaped) still owes its exact terminal proof;
                # deriving missing from `owned` alone would declare such
                # work proven the moment its claim file moved. Rows whose
                # successor was published (or adopted) already proved their
                # terminal before the publish and are done.
                proof = list(owned)
                owned_keys = {str(s["action_key"]) for s in owned}
                for action_key, entry in handled.items():
                    if action_key not in owned_keys and not entry.get("published"):
                        probe = {"action_key": action_key,
                                 **entry.get("snapshot", {})}
                        proof.append(probe)
                missing = sorted(
                    snap["action_key"][:12] for snap in proof
                    if isinstance(snap.get("action_key"), str)
                    and terminal_of(queue, snap) is None)
                # Settle through the shared reconciler (the same path the
                # worker loops drive every poll): publish matured successors,
                # adopt exact ones, preserve foreign rows. Results merge into
                # this run's handled map; failures retain with reasons.
                reconciliation = reconcile_membership(queue, host, owner)
                for published_key in reconciliation["published"]:
                    for action_key, entry in handled.items():
                        if action_key[:12] == published_key:
                            entry["published"] = "resign-successor"
                for adopted_key in reconciliation["adopted"]:
                    for action_key, entry in handled.items():
                        if action_key[:12] == adopted_key:
                            entry["published"] = "adopted-exact-successor"
                for retained in reconciliation["retained"]:
                    if isinstance(retained, dict):
                        armed = False
                        pending_note = (
                            f"successor unsettled for "
                            f"{retained.get('action_key', '?')}: "
                            f"{retained.get('reason')}")
                        break
                if pending_note is None:
                    if census_error is not None:
                        armed = False
                        pending_note = f"loop census unknown: {census_error}"
                    elif missing:
                        armed = False
                        retained = sorted(
                            snap["action_key"][:12] for snap in proof
                            if isinstance(snap.get("action_key"), str)
                            and terminal_of(queue, snap) is None
                            and handled.get(
                                str(snap["action_key"]), {}).get("handoff")
                            == "retained-uninterruptible")
                        if retained and len(retained) == len(missing):
                            pending_note = ("draining uninterruptible work "
                                            "(retry-unsafe or budget-exhausted; "
                                            "cancel it explicitly with withdraw, "
                                            "resign never cancels): "
                                            + ", ".join(retained))
                        else:
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
            "claimed_unix", "max_attempts", "retry_safe",
            "withdrawn_unix", "withdrawn_by", "withdrawn_host")}
    return ids


def lineage_status(queue: pool_module.PoolQueue, snapshot: dict[str, Any],
                     revive_by: str) -> dict[str, Any]:
    """One typed exact reading of a handoff generation.

    ``snapshot`` carries the attempt identity (action_key, published_unix,
    attempts, claimed_by, claimed_unix); ``revive_by`` is the exact owner
    name allowed to revive it (the decision's ``withdrawn_by``). Returns:

    - ``terminal``: (kind, record) for the exact attempt, else None.
    - ``decision``: the covering withdrawal decision (live file preferred,
      else the retired immutable one), else None.
    - ``successor``: the ready occupant row, if any.
    - ``successor_unknown``: the ready slot could not be read (unreadable
      directory entry or corrupt bytes — distinguished from proven
      absence). Unknown is never clobbered: callers retain and block.
    - ``successor_exact``: the occupant is OUR successor — same key,
      parent generation (``supersedes_withdrawal.published_unix`` equals
      the decision's), same decision timestamp, same owner linkage
      (``resigned_by`` and link ``withdrawn_by`` equal ``revive_by``),
      counter exactly one more, same budget (``max_attempts``,
      ``retry_safe``), missing-prefix consistency, AND the queue's own
      ``_preemption_prefix_valid`` chain check passes. Same key and
      counter alone never suffice: a different generation or a foreign
      publication with coincident counters is preserved, never adopted;
      same owner/timestamp with altered attempts, budget, or parent
      refuses via the field checks and the pool validator.
    - ``foreign``: a ready occupant that is not ours. Never claimed to
      discharge this handoff; the row is preserved and diagnosed.

    Malformed records, missing proof, and unreadable paths all read as
    unknown/absent — never as a match.
    """
    key = str(snapshot.get("action_key") or "")
    out: dict[str, Any] = {"terminal": terminal_of(queue, snapshot)
                           if isinstance(key, str) and len(key) == 64 else None,
                           "decision": None, "decision_live": False,
                           "successor": None, "successor_unknown": False,
                           "successor_exact": False,
                           "foreign": False}
    if not key:
        return out
    try:
        live = json.loads(queue.item_path(
            pool_module.WITHDRAWN, key).read_text())
    except (OSError, ValueError):
        live = None
    decision = None
    if (isinstance(live, dict)
            and live.get("published_unix") == snapshot.get("published_unix")):
        decision, out["decision_live"] = live, True
    else:
        try:
            candidates = queue.withdrawal_decisions(key)
        except (OSError, ValueError, pool_module.PoolContractError):
            candidates = []
        for _, candidate in candidates:
            if (isinstance(candidate, dict)
                    and candidate.get("published_unix") == snapshot.get(
                        "published_unix")):
                decision = candidate
                break
    out["decision"] = decision
    try:
        ready_raw: str | None = queue.item_path(
            pool_module.READY, key).read_text()
    except FileNotFoundError:
        ready_raw = None  # proven absence: the slot is free
    except OSError as exc:
        # Unreadable is not absent: planning a publication over this
        # would clobber an occupant no reader can see. Retain and block.
        out["successor_unknown"] = True
        out["successor_unknown_error"] = f"{type(exc).__name__}: {exc}"
        return out
    if ready_raw is None:
        return out
    try:
        ready = json.loads(ready_raw)
    except ValueError as exc:
        out["successor_unknown"] = True
        out["successor_unknown_error"] = f"corrupt ready record: {exc}"
        return out
    if not isinstance(ready, dict):
        out["successor_unknown"] = True
        out["successor_unknown_error"] = "ready record is not an object"
        return out
    out["successor"] = ready
    link = ready.get("supersedes_withdrawal")
    # Exact-successor discharge reuses the queue's own lineage owner:
    # field equality here is only the fast prefilter; the authoritative
    # chain check is ``_preemption_prefix_valid`` below, which verifies
    # every parent generation against its immutable withdrawal decision
    # (attempts, budget, retry permission, missing-prefix linkage, and
    # the membership-shaped withdrawn_by). Same owner and same decision
    # timestamp alone never suffice: altered attempts, max_attempts, or
    # parent generation must refuse, and only the pool's validator knows
    # the full parent chain (including chained resign requeues).
    exact = False
    if isinstance(decision, dict) and isinstance(link, dict):
        ready_key = ready.get("action_key")
        dec_pub = decision.get("published_unix")
        link_pub = link.get("published_unix")
        dec_wd = decision.get("withdrawn_unix")
        link_wd = link.get("withdrawn_unix")
        dec_by = decision.get("withdrawn_by")
        link_by = link.get("withdrawn_by")
        ready_resigned = ready.get("resigned_by")
        dec_attempts = decision.get("attempts")
        ready_attempts = ready.get("attempts")
        dec_limit = decision.get("max_attempts")
        ready_limit = ready.get("max_attempts")
        dec_retry = decision.get("retry_safe")
        ready_retry = ready.get("retry_safe")
        ready_missing = ready.get("attempt_history_missing_before")
        if (isinstance(ready_key, str) and ready_key == key
                and type(dec_pub) in (int, float)
                and not isinstance(dec_pub, bool)
                and link_pub == dec_pub
                and type(dec_wd) in (int, float)
                and not isinstance(dec_wd, bool)
                and link_wd == dec_wd
                and isinstance(dec_by, str) and dec_by == revive_by
                and link_by == revive_by
                and isinstance(ready_resigned, str)
                and ready_resigned == revive_by
                and type(dec_attempts) is int and dec_attempts >= 0
                and type(ready_attempts) is int
                and ready_attempts == dec_attempts + 1
                and type(dec_limit) is int and type(ready_limit) is int
                and ready_limit == dec_limit
                and ready_attempts < ready_limit
                and dec_retry is True and ready_retry is True
                and not ready.get("attempt_history")
                and type(ready_missing) is int
                and ready_missing == ready_attempts):
            try:
                exact = bool(queue._preemption_prefix_valid(
                    ready, ready_attempts, ready_limit))
            except (OSError, ValueError,
                    pool_module.PoolContractError, AttributeError,
                    TypeError, KeyError):
                exact = False
    if exact:
        out["successor_exact"] = True
    else:
        out["foreign"] = True
    return out


def _membership_owner_parts(owner: str) -> tuple[str, int, str] | None:
    """Parse a membership supervisor owner, else None (never a takeover key)."""
    try:
        text = str(owner)
        host, _, rest = text.partition(":")
        kind, _, starttime = rest.partition(":")
        label, _, pid_text = kind.partition("-")
        if not host or label != "supervisor" or not starttime:
            return None
        pid = int(pid_text)
        if pid <= 0:
            return None
    except (ValueError, AttributeError):
        return None
    return host, pid, starttime


def _owner_gone(host: str, owner: str) -> tuple[bool, str]:
    """Whether a past supervisor owner is provably gone (dead or replaced).

    Returns (gone, reason). Only exact local-host membership owners can
    come back True; permission/read errors are unknown (False), never dead.
    """
    parts = _membership_owner_parts(owner)
    if parts is None:
        return False, "not a membership owner"
    old_host, pid, starttime = parts
    if old_host != host:
        return False, "another host's owner"
    if not broker_mod._proven_starttime(starttime):
        # Minted without proof (``unknown``) or malformed: neither proves
        # gone, and a named non-decimal value always differs from a real
        # field-22 read, which must not read as PID reuse.
        return False, "owner start time not proven"
    try:
        line = Path(f"/proc/{pid}/stat").read_text()
    except FileNotFoundError:
        return True, "pid absent"
    except OSError as exc:
        # Permission or read errors prove nothing; unknown stays refused.
        return False, f"pid unreadable: {exc}"
    _, _, rest = line.rpartition(")")
    fields = rest.split()
    current = fields[19] if len(fields) > 19 else None
    if current is None:
        # Malformed stat: missing parsed proof is unknown, never reuse.
        return False, "start time unreadable"
    if current != starttime:
        return True, "pid reused"
    return False, "supervisor still live"


def resume_owed(
    queue: pool_module.PoolQueue, host: str, owner: str
) -> tuple[list[dict[str, Any]], list[str]]:
    """Withdrawn membership rows still owed a successor (crash-resume set).

    Scans the authoritative ``withdrawn/`` decisions (top level only —
    retired superseded markers live beneath): rows this lane withdrew
    (membership-shaped ``withdrawn_by``, ``withdrawn_host`` == this host)
    that carry retry budget and have no same-generation done/failed
    terminal. A row withdrawn by the current owner is this run's own; a
    row withdrawn by a past owner is adopted only when that supervision
    is provably gone — same rule the broker enforces for gate takeover.

    Successor handling (exact lineage via ``lineage_status``, which reuses
    the queue's own ``_preemption_prefix_valid`` chain check):

    - exact successor (full typed lineage, budget, counter, parent
      generation, and publication linkage all match): discharged —
      returned with ``successor_exact=True`` and no plan so callers can
      adopt/report it without republishing. JOIN treats it as settled.
    - foreign/unknown occupant (any READY row that is not exact):
      unsettled — returned with ``plan=None`` so JOIN/resign keep the
      fence and the reconciler retains (never overwrites) it.
    - no occupant: returned with a ``plan_requeue`` plan for the
      reconciler to publish once the exact withdrawn terminal lands.

    Anything else (operator withdrawals, unparsable rows, unbuildable
    plans) is reported, never revived.
    """
    owed: list[dict[str, Any]] = []
    skipped: list[str] = []
    directory = queue.dir(pool_module.WITHDRAWN)
    if not directory.is_dir():
        return [], [f"withdrawn directory unreadable: {directory}"]
    try:
        # Materialize with os.scandir, not Path.glob: the stdlib glob
        # selector suppresses directory OSError (`pathlib._WildcardSelector.
        # _select_from` catches OSError around scandir and yields nothing),
        # so a permission-denied census would read as "nothing owed" and
        # JOIN would open the gate over unknown rows. scandir preserves
        # the error and the caller below fails closed on it.
        with os.scandir(directory) as entries:
            names = sorted(entry.name for entry in entries
                           if entry.name.endswith(".json"))
    except OSError as exc:
        return [], [f"withdrawn directory not listable: {exc}"]
    paths = [directory / name for name in names]
    for path in paths:
        try:
            record = json.loads(path.read_text())
        except (OSError, ValueError) as exc:
            skipped.append(f"{path.name}: unreadable ({exc})")
            continue
        if not isinstance(record, dict):
            skipped.append(f"{path.name}: non-object record")
            continue
        action_key = record.get("action_key")
        if not isinstance(action_key, str) or len(action_key) != 64:
            skipped.append(f"{path.name}: bad action key")
            continue
        withdrawn_by = record.get("withdrawn_by")
        if not isinstance(withdrawn_by, str) or not _membership_owner_parts(
                withdrawn_by):
            continue  # not this lane's: operator/admission/legacy rows
        if record.get("withdrawn_host") != host:
            continue  # another host's row, never ours to revive
        if withdrawn_by != owner:
            gone, why = _owner_gone(host, withdrawn_by)
            if not gone:
                skipped.append(f"{action_key[:12]}: prior owner not gone ({why})")
                continue
        snapshot = _snap_id(record)
        attempts = snapshot.get("attempts")
        limit = snapshot.get("max_attempts", pool_module.DEFAULT_MAX_ATTEMPTS)
        if not (snapshot.get("retry_safe") is True and type(attempts) is int
                and attempts >= 0 and type(limit) is int
                and attempts + 1 < limit):
            continue  # no retry budget: terminal stands, nothing owed
        status = lineage_status(queue, snapshot, withdrawn_by)
        if status["terminal"] is not None and status["terminal"][0] in (
                "done", "failed"):
            continue  # this generation concluded; nothing owed
        if status.get("successor_unknown"):
            # The ready slot could not be read: unknown, never absent.
            # Returned (no plan) so JOIN/resign keep the fence and the
            # reconciler retains instead of publishing over it.
            owed.append({"action_key": action_key,
                         "snapshot": snapshot,
                         "plan": None,
                         "revive_by": withdrawn_by,
                         "successor_exact": False})
            continue
        if status["successor_exact"]:
            # Settled: the exact successor discharges this handoff. Return
            # it (no plan) so the reconciler can adopt/report it; JOIN
            # treats exact as settled, not unsettled.
            owed.append({"action_key": action_key,
                         "snapshot": snapshot,
                         "plan": None,
                         "revive_by": withdrawn_by,
                         "successor_exact": True})
            continue
        if status["successor"] is not None:
            # Foreign/unknown occupant: unsettled, never overwritten.
            # Returned (no plan) so JOIN/resign keep the fence and the
            # reconciler retains with a reason.
            owed.append({"action_key": action_key,
                         "snapshot": snapshot,
                         "plan": None,
                         "revive_by": withdrawn_by,
                         "successor_exact": False})
            continue
        # Crash-resume plans from the withdrawn record: ``withdraw`` moves
        # the missing-prefix count aside (``attempt_history_missing_before``
        # -> ``..._withdrawal``) so readers never mistake it for this
        # record's own ending. ``plan_requeue`` (the existing queue handoff)
        # reads the live count, so restore it into the planning copy —
        # chained resign requeues (B resigning what A requeued) carry
        # attempts>0 and would otherwise read as budget-exhausted. The
        # immutable decisions still verify the full chain at publish.
        for_plan = dict(record)
        if ("attempt_history_missing_before" not in for_plan
                and "attempt_history_missing_before_withdrawal" in for_plan):
            for_plan["attempt_history_missing_before"] = for_plan[
                "attempt_history_missing_before_withdrawal"]
        try:
            plan = queue.plan_requeue(for_plan)
        except (OSError, ValueError,
                pool_module.PoolContractError) as exc:
            skipped.append(f"{action_key[:12]}: unplannable ({exc})")
            continue
        owed.append({"action_key": action_key,
                     "snapshot": snapshot,
                     "plan": plan,
                     "revive_by": withdrawn_by,
                     "successor_exact": False})
    owed.sort(key=lambda snap: str(snap["action_key"]))
    return owed, skipped


def _call_as_request(
    socket_path: Path | None,
    broker_call: Callable[[dict], dict] | None,
) -> Callable[[dict], dict]:
    if broker_call is not None:
        return broker_call

    def call(payload: dict) -> dict:
        return broker_call_func(payload, socket_path, None)

    return call


def reconcile_membership(queue: pool_module.PoolQueue, host: str,
                           owner: str | None = None) -> dict[str, Any]:
    """Settle membership handoff intents through the existing retry path.

    The deterministic settlement both the resign CLI and the worker loops
    drive (loops call this every poll, including under a closed drain —
    see worker_loop's drain branch): for every owed withdrawn row with an
    exact withdrawn-type terminal and no ready occupant, publish the
    budget-preserving successor with the exact revival linkage; adopt
    exact successors; preserve and diagnose everything else. Convergent
    and idempotent across concurrent callers: the publish guard admits
    only the exact decision revival, a second publisher finds the exact
    successor and adopts it, and foreign rows are never touched. Never
    raises — every row reports published, adopted, or retained with its
    reason.
    """
    report: dict[str, Any] = {"published": [], "adopted": [],
                              "retained": [], "skipped": []}
    if owner is None:
        owner, _ = supervisor_incarnation()
        owner = owner or ""
    try:
        owed, skipped = resume_owed(queue, host, owner)
    except (OSError, ValueError) as exc:
        report["retained"].append({"error": f"census failed: {exc}"})
        return report
    report["skipped"] = skipped
    for row in owed:
        action_key = str(row["action_key"])
        snapshot = {"action_key": action_key, **row.get("snapshot", {})}
        try:
            status = lineage_status(queue, snapshot, row["revive_by"])
        except Exception as exc:                                 # noqa: BLE001
            report["retained"].append(
                {"action_key": action_key[:12],
                 "reason": f"lineage unreadable: {exc}"})
            continue
        terminal = status["terminal"]
        if terminal is None:
            # Not yet: the holder has not concluded. Waiting is the caller's
            # job (resign rounds, loop polls); this is not a failure.
            continue
        if terminal[0] != "withdrawn":
            # Concluded by completion, not interruption: nothing to revive.
            continue
        if status.get("successor_unknown"):
            report["retained"].append(
                {"action_key": action_key[:12],
                 "reason": "ready occupant unreadable: "
                           f"{status.get('successor_unknown_error')}"})
            continue
        if status["successor_exact"]:
            report["adopted"].append(action_key[:12])
            continue
        if status["successor"] is not None:
            report["retained"].append(
                {"action_key": action_key[:12],
                 "reason": "foreign ready occupant preserved"})
            continue
        plan = row.get("plan")
        if plan is None:
            report["retained"].append(
                {"action_key": action_key[:12],
                 "reason": "no requeue plan for unoccupied row"})
            continue
        try:
            queue.publish(**plan["arguments"],
                          preempted_claim=plan["snapshot"],
                          handoff_by=row["revive_by"])
        except (OSError, ValueError,
                pool_module.PoolContractError) as exc:
            # A concurrent publisher may have won: re-read lineage once
            # so an exact successor reports adopted, not retained.
            try:
                raced = lineage_status(queue, snapshot, row["revive_by"])
            except (OSError, ValueError):
                raced = None
            if raced is not None and raced.get("successor_exact"):
                report["adopted"].append(action_key[:12])
            else:
                report["retained"].append(
                    {"action_key": action_key[:12],
                     "reason": f"successor publish failed: {exc}"})
            continue
        report["published"].append(action_key[:12])
    return report


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
        held = None
        if args.queue_root is not None:
            try:
                held = pool_module.PoolQueue(
                    Path(args.queue_root)).ledger().held
            except (OSError, ValueError):
                held = None
        result = join(
            args.host, reason=args.reason, queue_root=args.queue_root,
            roster_path=args.roster, gate=args.gate, socket_path=args.socket,
            held=held,
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
