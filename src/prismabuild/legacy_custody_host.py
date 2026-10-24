"""Host facts for the stopped-writer legacy custody operation, never a release."""
from __future__ import annotations

import os
from pathlib import Path
import socket
import stat
import subprocess
import time

from . import materialize, park_proof, resource_scope, role_session

LEGACY_REQUIRED_ROOT_KINDS = frozenset({"checkout", "quarantine", "occupancy"})


def legacy_stopped_host():
    """Independently verify the broker gate, supervisor and every writer role."""
    gate = park_proof.read_gate(role_session.MAINTENANCE_GATE)
    identity = park_proof.drain_identity(gate)
    if isinstance(identity, park_proof.ProofUnknown):
        raise ValueError(identity.reason)
    status = resource_scope.broker_request({"op": "maintenance_query"})
    if (status.get("draining") is not True or status.get("health") is not True
            or status.get("active_scopes") != 0 or status.get("active_scopes_truncated")
            or status.get("maintenance_owner") != gate.get("owner")):
        raise ValueError("broker maintenance gate or zero-scope proof unavailable")
    service = subprocess.run([
        "systemctl", "--user", "show", "prismabuild-supervisor.service",
        "--property=ActiveState,MainPID,ExecStart"],
        check=True, capture_output=True, text=True, timeout=20).stdout
    properties = dict(line.split("=", 1) for line in service.splitlines() if "=" in line)
    if (properties.get("ActiveState") != "inactive" or properties.get("MainPID") != "0"
            or "--systemd" not in properties.get("ExecStart", "")):
        raise ValueError("supervisor not stopped or cron startup is not fenced")
    writers = park_proof.select_processes(
        park_proof.proc_census(), park_proof.HOST_QUIESCENT | {"supervise.py"})
    if writers:
        raise ValueError("queue writer processes remain on the owner host")
    if park_proof.read_gate(role_session.MAINTENANCE_GATE) != gate:
        raise ValueError("maintenance gate changed during writer verification")
    return {"host": socket.gethostname(), "observed_unix": time.time(),
            "gate": gate, "broker_draining": True, "active_scopes": 0,
            "supervisor": "inactive", "writer_processes": 0, "cron_fenced": True}


def legacy_namespace_scan(key, roots):
    """Full readdir on held roots, with no symlink traversal or globbing.

    Only code-defined lazy children may be absent under their held owner root.
    """
    scans = []
    matches = []
    for spec in roots:
        path = Path(spec["path"])
        if not path.is_absolute() or ".." in path.parts or spec.get("kind") not in {
                "checkout", "cleanup-failures", "quarantine", "occupancy"}:
            raise ValueError("invalid legacy root coverage")
        fd = os.open("/", os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
        try:
            absent = False
            for index, part in enumerate(path.parts[1:]):
                try:
                    os.stat(part, dir_fd=fd, follow_symlinks=False)
                except FileNotFoundError:
                    if (index != len(path.parts) - 2 or spec["kind"] in LEGACY_REQUIRED_ROOT_KINDS
                            or spec["kind"] != "cleanup-failures" or part not in materialize.LEGACY_LAZY_CHECKOUT_ROOTS
                            or not any(r.get("kind") == "checkout" and Path(r["path"]) == path.parent for r in roots)):
                        raise
                    parent = os.fstat(fd)
                    if parent.st_uid != os.geteuid():
                        raise ValueError("lazy checkout root parent has a different owner")
                    names = os.listdir(fd)
                    if part in names:
                        raise ValueError("lazy checkout root appeared during scan")
                    scans.append({"path": str(path), "kind": spec["kind"], "absent": True,
                                  "parent_device": parent.st_dev, "parent_inode": parent.st_ino,
                                  "parent_uid": parent.st_uid, "entries_examined": len(names),
                                  "matched": [], "scanned_unix": time.time()})
                    absent = True
                    break
                child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=fd)
                os.close(fd)
                fd = child
            if absent:
                continue
            scan = {"path": str(path), "kind": spec["kind"], "entries_examined": 0,
                    "matched": [], "scanned_unix": time.time()}
            identity = os.fstat(fd)
            scan.update(device=identity.st_dev, inode=identity.st_ino)
            names = os.listdir(fd)  # A full scan, never a pattern lookup.
            scan["entries_examined"] = len(names)
            for name in names:
                if (spec["kind"] in {"occupancy", "quarantine"} or key[:12] in name
                        or name.startswith(key + ".")):
                    info = os.stat(name, dir_fd=fd, follow_symlinks=False)
                    entry = {"name": name, "device": info.st_dev, "inode": info.st_ino,
                             "directory": stat.S_ISDIR(info.st_mode), "regular": stat.S_ISREG(info.st_mode)}
                    scan["matched"].append(entry)
                    matches.append({"path": str(path / name), "kind": spec["kind"], **entry})
            # A second full readdir detects namespace movement during scan.
            if sorted(names) != sorted(os.listdir(fd)):
                raise ValueError("legacy root changed during the full scan")
            scans.append(scan)
        finally:
            os.close(fd)
    return {"host": socket.gethostname(), "scanned_unix": time.time(),
            "roots": scans, "matches": matches}


def observe_legacy_custody_host(request):
    before = legacy_stopped_host()
    result = {key: legacy_namespace_scan(key, roots) for key, roots in request.items()}
    after = legacy_stopped_host()
    if before["gate"] != after["gate"]:
        raise ValueError("maintenance gate changed across legacy namespace scans")
    return {"host": after["host"], "writers_stopped": after, "scans": result}
