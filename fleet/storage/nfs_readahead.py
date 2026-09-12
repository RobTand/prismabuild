#!/usr/bin/python3
"""Set only the current /mnt/shared NFS backing device's readahead window.

Installed locally by an operator; never run as a worker or a runtime hook.
Mount IDs and BDI numbers are rediscovered on every invocation.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
import sys

MOUNTINFO = Path("/proc/self/mountinfo")
BDI_ROOT = Path("/sys/class/bdi")

#: The window issue #523 proposes for the /mnt/shared NFS client BDI, in KiB.
#: A reading below it is something to report, never something to refuse: this
#: is a host setting an operator installs, and no admission or scheduling
#: decision in this fleet may depend on it.
RECOMMENDED_KIB = 16384


def shared_mount(mountinfo: Path) -> tuple[str, str, str]:
    candidates = []
    for line in mountinfo.read_text().splitlines():
        before, separator, after = line.partition(" - ")
        fields = before.split()
        if len(fields) < 5 or fields[4] != "/mnt/shared":
            continue
        fs = after.split()
        if not separator or len(fs) < 2:
            raise ValueError("malformed /mnt/shared mount record")
        if fs[0] == "autofs":
            continue
        if (fs[0] not in {"nfs", "nfs4"}
                or not fs[1].endswith(":/storage_pool/shared")
                or fields[3] != "/"
                or re.fullmatch(r"[0-9]+:[0-9]+", fields[2]) is None):
            raise ValueError("/mnt/shared is not the expected whole NFS export")
        candidates.append((fields[0], fields[2], fs[1]))
    if len(candidates) != 1:
        raise ValueError("expected exactly one mounted /mnt/shared NFS export")
    return candidates[0]


def observe(*, mountinfo: Path = MOUNTINFO,
            bdi_root: Path = BDI_ROOT) -> dict:
    """Read the current /mnt/shared NFS readahead window without changing it.

    ``configure`` already inspects when ``apply`` is false, but it asks a
    different question: it takes the window an operator wants and refuses any
    value outside ``{1024, 16384}``. A readiness check asks what the window is
    now, and has to be able to report a third value rather than refuse it, so
    it gets its own entry point instead of a flag on ``configure``. Mount
    discovery is ``shared_mount``'s, unchanged.

    Two failure classes, kept apart because a caller's next move differs:
    ``ValueError`` means /mnt/shared is not the single NFS client export this
    helper is about -- absent, an autofs stub, a local filesystem, ambiguous
    or malformed -- and ``OSError`` means it is, and the reading could not be
    taken. A sysfs attribute holding something that is not a number is the
    second case, so it is raised as ``OSError`` rather than leaking a
    ``ValueError`` that would read as the first.
    """

    mount_id, bdi, source = shared_mount(mountinfo)
    raw = (bdi_root / bdi / "read_ahead_kb").read_text()
    try:
        kib = int(raw.strip())
    except ValueError as exc:
        raise OSError(
            f"bdi {bdi} read_ahead_kb is not a number: {raw!r}") from exc
    return {"mount": "/mnt/shared", "mount_id": mount_id, "source": source,
            "bdi": bdi, "read_ahead_kib": kib,
            "recommended_kib": RECOMMENDED_KIB}


def configure(kib: int, *, apply: bool = False,
              mountinfo: Path = MOUNTINFO, bdi_root: Path = BDI_ROOT) -> dict:
    if kib not in {1024, 16384}:
        raise ValueError("supported windows are 1024 and 16384 KiB")
    mount_id, bdi, source = shared_mount(mountinfo)
    target = bdi_root / bdi / "read_ahead_kb"
    before = int(target.read_text())
    if shared_mount(mountinfo) != (mount_id, bdi, source):
        raise ValueError("/mnt/shared changed during readahead discovery")
    if apply:
        # Do not create a missing sysfs attribute, even in an unexpected layout.
        with target.open("r+") as handle:
            handle.write(f"{kib}\n")
            handle.flush()
    actual = int(target.read_text())
    if shared_mount(mountinfo) != (mount_id, bdi, source):
        raise ValueError("/mnt/shared changed during readahead readback")
    if apply and actual != kib:
        raise ValueError(f"readahead readback {actual} != requested {kib}")
    return {"mount": "/mnt/shared", "mount_id": mount_id, "source": source,
            "bdi": bdi, "before_kib": before, "read_ahead_kib": actual,
            "requested_kib": kib, "applied": apply}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kib", type=int, choices=(1024, 16384), default=16384)
    parser.add_argument("--apply", action="store_true", help="write the window (requires root)")
    args = parser.parse_args(argv)
    try:
        print(json.dumps(configure(args.kib, apply=args.apply), sort_keys=True))
    except (OSError, ValueError) as exc:
        print(f"NFS readahead refused: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
