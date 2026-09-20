"""A >1 GiB promotion lands inside the action's own memory reservation.

The sealed row's ``mem_gb`` has to cover the tmpfs pages the promotion will
write, because shmem pages stay charged to the writing cgroup until they are
freed.  The unit tests pin the arithmetic; this test runs the real copier
against the real mounts and asks the kernel's own cgroup accounting whether
the destination was charged -- the only evidence that separates "the demand
is large enough" from "the demand is a number somebody liked".

It is guarded, not skipped silently: without a tmpfs root carrying an epoch
and a writable stage root there is no promotion to run, and the test says so
by skipping with the missing path in the reason.  It writes only under a
per-run scratch namespace and removes that namespace in a ``finally``.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import sys
import uuid

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
from prismabuild import core as pb  # noqa: E402
from prismabuild import storage_tiers  # noqa: E402
import ram_promote  # noqa: E402

STAGE_ROOT = Path("/stage/prewarm")
RAM_ROOT = Path("/ram/prewarm")
GIB = storage_tiers.GIB
#: Above the 1 GiB cap every live promotion died at, and small enough that a
#: bounded action is cheap: 1.25 GiB of destination plus runtime.
RANGE_BYTES = 1280 * 1024 * 1024
CONSUMER = "c" * 64


def _cgroup_memory_peak_bytes() -> int | None:
    """This process's cgroup memory peak, from the kernel, or ``None``."""

    try:
        relative = Path("/proc/self/cgroup").read_text().strip().split(":")[-1]
        peak = Path("/sys/fs/cgroup") / relative.lstrip("/") / "memory.peak"
        return int(peak.read_text().strip())
    except (OSError, ValueError):
        return None


def _manifest_bytes(relative: str) -> bytes:
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


#: A submission whose whole point is the live tier sets this; a placement
#: that cannot see the mounts must fail rather than skip into a green run.
REQUIRE_MOUNTS = "PB_RAM_CHARGE_REQUIRE_MOUNTS"


def test_a_promotion_larger_than_a_gib_lands_under_its_declared_cap(
        tmp_path: Path) -> None:
    required = os.environ.get(REQUIRE_MOUNTS) == "1"
    missing = [str(path) for path in (STAGE_ROOT, RAM_ROOT) if not path.is_dir()]
    if missing:
        message = f"no live tier roots: {', '.join(missing)}"
        pytest.fail(message) if required else pytest.skip(message)
    if storage_tiers.read_ram_epoch(RAM_ROOT) is None:
        pytest.skip(f"{RAM_ROOT} carries no epoch marker")
    if _cgroup_memory_peak_bytes() is None:
        pytest.skip("no cgroup memory accounting to measure the reservation")

    scope = f".pb-ram-charge-{uuid.uuid4().hex[:12]}"
    relative = f"{scope}/part.bin"
    source = STAGE_ROOT / relative
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_bytes(_manifest_bytes(relative))
    pool_root = tmp_path / "pool"
    (pool_root / "movers").mkdir(parents=True)
    args = ram_promote.build_parser().parse_args([
        "--pool-root", str(pool_root),
        "--manifest", str(manifest_path),
        "--consumer-action-key", CONSUMER,
        "--tier-id", "ram:test",
        "--ram-root", str(RAM_ROOT),
        "--source-stage-root", str(STAGE_ROOT),
        "--manifest-sha256", "0" * 64,
        "--range-start-bytes", "0",
        "--range-end-bytes", str(RANGE_BYTES),
        "--action-key", "a" * 64,
        "--residency-root", str(tmp_path / "residency"),
        "--readers", "4",
    ])

    source.parent.mkdir(parents=True, exist_ok=True)
    try:
        with open(source, "wb") as sink:
            sink.write(b"\x00" * RANGE_BYTES)
        receipt = ram_promote.promote(args)
        assert receipt["complete"] is True, receipt
        assert int(receipt["bytes_staged"]) == RANGE_BYTES
        landed = RAM_ROOT / relative
        assert landed.stat().st_size == RANGE_BYTES
        peak = _cgroup_memory_peak_bytes()
        assert peak is not None and peak >= RANGE_BYTES, (
            "the destination pages were not charged to this cgroup; the "
            f"reservation premise is unmeasured (peak={peak})")
    finally:
        shutil.rmtree(STAGE_ROOT / scope, ignore_errors=True)
        shutil.rmtree(RAM_ROOT / scope, ignore_errors=True)
