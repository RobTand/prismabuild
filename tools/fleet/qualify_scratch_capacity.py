#!/usr/bin/env python3
"""Inspect dedicated O6 backing inside an admitted action, without mutation."""
from __future__ import annotations

import argparse
import fcntl
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from prismabuild import local_scratch, scratch_capacity  # noqa: E402


def inspect_host(*, root: str, dataset: str, max_gib: int, min_free_gib: int,
                 allocation_gib: int = 384) -> dict:
    checks, errors = {}, []
    try:
        found = scratch_capacity.inspect_root(root)
        checks["root"] = found
        if found["free_bytes"] < (max_gib + min_free_gib) * local_scratch.GIB:
            errors.append("root lacks the declared ceiling plus free-space floor")
        namespace = Path(root) / "prismabuild-ephemeral"
        info = namespace.stat(follow_symlinks=False)
        checks["namespace"] = {"uid": info.st_uid, "mode": stat.S_IMODE(info.st_mode)}
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o700:
            errors.append("PB namespace is not a private directory owned by the worker")
    except (OSError, ValueError) as exc:
        errors.append(f"root prerequisite: {exc}")
    properties = ("mountpoint", "quota", "reservation", "sync", "exec", "setuid", "devices")
    try:
        observed = subprocess.run(
            ["zfs", "get", "-Hp", "-o", "property,value", ",".join(properties), dataset],
            check=True, capture_output=True, text=True, timeout=15)
        values = dict(line.split("\t", 1) for line in observed.stdout.splitlines())
        checks["dataset"] = values
        expected = {"mountpoint": root, "sync": "standard", "exec": "off",
                    "setuid": "off", "devices": "off"}
        if any(values.get(k) != v for k, v in expected.items()):
            errors.append("dataset mount or protection properties differ from the approved proposal")
        if (allocation_gib < max_gib + min_free_gib
                or any(int(values.get(k, "0")) != allocation_gib * local_scratch.GIB
                       for k in ("quota", "reservation"))):
            errors.append("dataset quota and reservation differ from the declared allocation")
    except (OSError, ValueError, subprocess.SubprocessError) as exc:
        errors.append(f"dataset prerequisite: {type(exc).__name__}")
    try:
        fd = os.open("/dev/kvm", os.O_RDWR | os.O_CLOEXEC)
        try:
            version = fcntl.ioctl(fd, 0xAE00, 0)  # KVM_GET_API_VERSION
        finally:
            os.close(fd)
        checks["kvm_api_version"] = version
        if version != 12:
            errors.append("contained KVM API version is unsupported")
    except OSError as exc:
        errors.append(f"contained KVM prerequisite: {exc}")
    return {"schema": "prismabuild.scratch_capacity_host_probe.v1", "root": root,
            "dataset": dataset, "max_gib": max_gib, "min_free_gib": min_free_gib,
            "allocation_gib": allocation_gib,
            "checks": checks, "errors": errors,
            "status": "blocked" if errors else "host-prerequisites-ready",
            "native_admission_qualified": False,
            "limits": ["read-only host prerequisites only", "no VM boot or disk writes",
                       "no dedicated-root admission or stopped-attempt deletion proof"]}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True, help="Exact local mount proposed for dedicated scratch.")
    parser.add_argument("--dataset", required=True, help="ZFS dataset backing that mount.")
    parser.add_argument("--max-gib", type=int, default=320, help="Planned PB root capacity ceiling in GiB.")
    parser.add_argument("--min-free-gib", type=int, default=32, help="Required free-space floor in GiB.")
    parser.add_argument("--allocation-gib", type=int, default=384,
                        help="Exact approved quota and reservation in GiB; both must match.")
    args = parser.parse_args(argv)
    if not re.fullmatch(r"[0-9a-f]{64}", os.environ.get("PRISMABUILD_ACTION_KEY", "")):
        parser.error("submit this probe through the published PrismaBuild client")
    if (args.max_gib <= 0 or args.min_free_gib <= 0
            or args.allocation_gib < args.max_gib + args.min_free_gib):
        parser.error("capacity and free-space floor must be positive")
    report = inspect_host(root=args.root, dataset=args.dataset,
                          max_gib=args.max_gib, min_free_gib=args.min_free_gib,
                          allocation_gib=args.allocation_gib)
    print(json.dumps(report))
    return 2 if report["errors"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
