#!/usr/bin/env python3
"""Accept a >1 GiB RAM promotion under the cap its own row now reserves.

The unit tests pin the arithmetic and the sealed row; the only evidence that
separates "the demand is large enough" from "the demand is a number somebody
liked" is the kernel's own cgroup accounting on a box that owns a live tier.
This driver runs inside an ordinary admitted action -- like
``tests/tier2_acceptance.py``, it is deliberately not collected by a plain
``pytest tests/`` -- and drives the real copier from ``/stage/prewarm`` into
``/ram/prewarm`` with a 1.25 GiB range, the shape every live promotion above
1 GiB failed on 2026-09-19/20 (``memory_limit_oom`` at exactly the cap).

It prints one JSON document and exits nonzero when any check fails.  It
writes only under its own fixed scratch namespace (``.pb-ram-charge-716``),
clears that namespace before and after, and never touches campaign data.  A
run killed by containment cannot clean up after itself, so the namespace is
fixed and the next run is its own janitor.

Run it with an honest reservation at least ``runtime + range``:

    python3 tests/ram_promotion_reservation_acceptance.py
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import socket
import sys
import time

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[0] / "src"))
sys.path.insert(0, str(HERE.parents[0] / "tools" / "fleet"))
from prismabuild import core as pb  # noqa: E402
from prismabuild import storage_tiers  # noqa: E402
import ram_promote  # noqa: E402

STAGE_ROOT = Path("/stage/prewarm")
RAM_ROOT = Path("/ram/prewarm")
GIB = storage_tiers.GIB
#: Above the 1 GiB cap every live promotion died at, and small enough that a
#: bounded action is cheap: 1.25 GiB of destination plus runtime.
RANGE_BYTES = 1280 * 1024 * 1024
SCOPE = ".pb-ram-charge-716"
CONSUMER = "c" * 64
ACTION = "a" * 64


def cgroup_memory_peak_bytes() -> int | None:
    """This process's cgroup memory peak, from the kernel, or ``None``."""

    try:
        relative = Path("/proc/self/cgroup").read_text().strip().split(":")[-1]
        peak = Path("/sys/fs/cgroup") / relative.lstrip("/") / "memory.peak"
        return int(peak.read_text().strip())
    except (OSError, ValueError):
        return None


def manifest_bytes(relative: str) -> bytes:
    manifest = {
        "schema": pb.DATA_MANIFEST_SCHEMA_V1,
        "produced_by": {},
        "annotations": {},
        "mount_prefix": "/mnt/shared",
        "entries": [{"path": f"/mnt/shared/{relative}", "offset": 0,
                     "bytes": RANGE_BYTES, "sha256": None}],
        "entry_count": 1,
        "total_bytes": RANGE_BYTES,
    }
    return json.dumps(manifest).encode("utf-8")


def run(scratch: Path) -> dict[str, object]:
    relative = f"{SCOPE}/part.bin"
    source = STAGE_ROOT / relative
    manifest_path = scratch / "manifest.json"
    manifest_path.write_bytes(manifest_bytes(relative))
    pool_root = scratch / "pool"
    (pool_root / "movers").mkdir(parents=True)
    args = ram_promote.build_parser().parse_args([
        "--pool-root", str(pool_root),
        "--manifest", str(manifest_path),
        "--consumer-action-key", CONSUMER,
        "--tier-id", "ram:acceptance",
        "--ram-root", str(RAM_ROOT),
        "--source-stage-root", str(STAGE_ROOT),
        "--manifest-sha256", "0" * 64,
        "--range-start-bytes", "0",
        "--range-end-bytes", str(RANGE_BYTES),
        "--action-key", ACTION,
        "--residency-root", str(scratch / "residency"),
        "--readers", "4",
    ])

    # A previous run that containment killed left this namespace behind; the
    # next run is its own janitor before it is a promotion.
    shutil.rmtree(STAGE_ROOT / SCOPE, ignore_errors=True)
    shutil.rmtree(RAM_ROOT / SCOPE, ignore_errors=True)
    source.parent.mkdir(parents=True, exist_ok=True)
    before = cgroup_memory_peak_bytes()
    try:
        with open(source, "wb") as sink:
            sink.write(b"\x00" * RANGE_BYTES)
        started = time.time()
        receipt = ram_promote.promote(args)
        elapsed = max(1e-9, time.time() - started)
        landed = RAM_ROOT / relative
        after = cgroup_memory_peak_bytes()
        epoch = storage_tiers.read_ram_epoch(RAM_ROOT)
        checks = {
            "receipt_complete": receipt.get("complete") is True,
            "bytes_staged_is_the_range": int(receipt.get("bytes_staged", 0))
            == RANGE_BYTES,
            "landed_bytes_are_the_range": (
                landed.is_file() and landed.stat().st_size == RANGE_BYTES),
            "epoch_matches_the_mount": (
                isinstance(epoch, dict)
                and receipt.get("epoch") == epoch.get("epoch")),
            "cgroup_peak_charged_the_destination": (
                isinstance(after, int) and after >= RANGE_BYTES),
            "no_receipt_errors": not receipt.get("errors"),
        }
        return {
            "range_bytes": RANGE_BYTES,
            "elapsed_s": round(elapsed, 3),
            "mb_per_s_file_side": receipt.get("mb_per_s_file_side"),
            "epoch": receipt.get("epoch"),
            "cgroup_peak_before_bytes": before,
            "cgroup_peak_after_bytes": after,
            "checks": checks,
            "ok": all(checks.values()),
        }
    finally:
        shutil.rmtree(STAGE_ROOT / SCOPE, ignore_errors=True)
        shutil.rmtree(RAM_ROOT / SCOPE, ignore_errors=True)


def main() -> int:
    result: dict[str, object] = {
        "schema": "prismaquant.prismabuild.ram_promotion_acceptance.v1",
        "host": socket.gethostname(),
        "stage_root": str(STAGE_ROOT),
        "ram_root": str(RAM_ROOT),
    }
    missing = [str(path) for path in (STAGE_ROOT, RAM_ROOT) if not path.is_dir()]
    if missing:
        result["refusal"] = f"missing live tier roots: {', '.join(missing)}"
        print(json.dumps(result, indent=1, sort_keys=True, default=str))
        return 1
    if cgroup_memory_peak_bytes() is None:
        result["refusal"] = "no cgroup memory accounting to measure the reservation"
        print(json.dumps(result, indent=1, sort_keys=True, default=str))
        return 1
    scratch = Path(os.environ.get("PB_RAM_CHARGE_SCRATCH", "/tmp")) / SCOPE
    shutil.rmtree(scratch, ignore_errors=True)
    scratch.mkdir(parents=True)
    try:
        result.update(run(scratch))
    finally:
        shutil.rmtree(scratch, ignore_errors=True)
    print(json.dumps(result, indent=1, sort_keys=True, default=str))
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
