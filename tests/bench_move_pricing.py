"""Before and after #1044: what one submission's pricing read costs.

Not collected by default (no ``test_`` prefix); run it by path through
pbtest::

    pbtest.py ... --mem-gb 4 tests/bench_move_pricing.py

For a synthetic receipt history of 8,000 and 50,000 receipts in ``tmp_path``
it times three reads and counts the files each opens:

* ``before``: the read #1044 replaced, every receipt opened
  (:func:`_full_read`, verbatim from ``PoolQueue.move_records`` at
  ``c1fbf87db723``).
* ``after-first``: ``move_records`` with no pricing log, which reads every
  receipt once and logs it.
* ``after``: ``move_records`` once the log exists and 160 more receipts were
  filed through ``record_move`` since (a day of receipts at the 2026-09-23
  rate is about 3,800; 160 is one hour of them).

Each read runs twice: once with the receipts in the page cache, and once
with every receipt and the log evicted first (``posix_fadvise`` after a
``syncfs``), which is closer to a read on the loaded pool.  ``tmp_path`` is
local NVMe, not the NFS mount on the HDD pool, so these numbers bound the
file count exactly and the time only relatively.  Each read is profiled with
cProfile; the top frames by cumulative time are printed.

The results print as one JSON object per case, prefixed ``bench-move-pricing:``.
"""
from __future__ import annotations

import cProfile
import ctypes
import io
import json
import os
from pathlib import Path
import pstats
import sys
import time

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import pool, storage_tiers  # noqa: E402

TIER = "prismabuild-stage:dl380g10"
IDENTITY = {"pool": "prismabuild-stage", "guid": "1", "state": "ONLINE"}
FILED_SINCE = 160


def _full_read(queue: pool.PoolQueue, *, schemas=(pool.POOL_MOVE_SCHEMA_V1,),
               ) -> list[dict[str, object]]:
    """``PoolQueue.move_records`` as it was at ``c1fbf87db723``."""

    directory = queue.root / pool.MOVERS
    out: list[dict[str, object]] = []
    try:
        paths = sorted(directory.glob("*.json"))
    except OSError:
        return out
    for path in paths:
        try:
            record = pool._read_json(path, tolerate_stale=True)
        except pool.PoolContractError:
            continue
        if not isinstance(record, dict):
            continue
        if record.get("schema") not in schemas:
            continue
        out.append(record)
    out.sort(key=lambda r: float(r.get("unix", 0.0) or 0.0))
    return out


def _receipt(index: int) -> dict[str, object]:
    """A receipt the size of a real stage mover's (progress, landing, plan)."""

    key = f"{index:064x}"
    return {
        "schema": pool.POOL_MOVE_SCHEMA_V1, "action_key": key, "tier_id": TIER,
        "pool_identity": IDENTITY,
        "consumer_action_key": f"{index // 8:064x}",
        "manifest_sha256": f"{index % 40:064x}", "complete": True,
        "stage_root": "/stage/prismabuild", "range_start_bytes": 0,
        "range_end_bytes": 4 << 30, "bytes_staged": 4 << 30,
        "bytes_copied": 4 << 30, "entries_declared": 12, "entries_staged": 12,
        "seconds": 30.0 + index % 17, "cpu_seconds": 11.0 + index % 5,
        "peak_rss_bytes": (1 + index % 3) << 30,
        "mb_per_s_file_side": 300.0 + index % 50,
        storage_tiers.MOVER_CONCURRENCY_FIELD: 1 + index % 4,
        "disk_pacing": {storage_tiers.POOL_FILL_FIELD: 150.0 + index % 30,
                        "pool_read_bytes": 4 << 30, "held_s": 0.4,
                        "samples": [[i, 120.0 + i] for i in range(24)]},
        "progress_report": {"phases": [{"name": "copy", "at": 1000.0 + i,
                                        "bytes": i << 20} for i in range(12)]},
        "landing_report": {"landings": [{"entry": f"shard-{i:05d}.safetensors",
                                         "bytes": 1 << 28, "seconds": 0.9}
                                        for i in range(12)]},
        "reader_plan": {"readers": 4, "depth": 2, "basis": "declared"},
        "host": "dl380g10", "unix": 1_790_000_000.0 + index,
    }


def _file_history(queue: pool.PoolQueue, count: int) -> None:
    directory = queue.root / pool.MOVERS
    directory.mkdir(parents=True, exist_ok=True)
    for index in range(count):
        (directory / f"{index:064x}.json").write_bytes(
            json.dumps(_receipt(index), sort_keys=True,
                       separators=(",", ":")).encode())


def _evict(queue: pool.PoolQueue) -> bool:
    """Drop the receipts and the log from the page cache; ``False`` if not."""

    paths = list((queue.root / pool.MOVERS).glob("*.json"))
    log = queue.move_pricing_log_path()
    if log.exists():
        paths.append(log)
    try:
        libc = ctypes.CDLL(None, use_errno=True)
        descriptor = os.open(queue.root, os.O_RDONLY)
        try:
            if libc.syncfs(descriptor) != 0:
                return False
        finally:
            os.close(descriptor)
        for path in paths:
            descriptor = os.open(path, os.O_RDONLY)
            try:
                os.posix_fadvise(descriptor, 0, 0, os.POSIX_FADV_DONTNEED)
            finally:
                os.close(descriptor)
    except (OSError, AttributeError):
        return False
    return True


def _measure(queue: pool.PoolQueue, read, monkeypatch) -> dict[str, object]:
    directory = queue.root / pool.MOVERS
    log = queue.move_pricing_log_path()
    opened = {"receipts": 0, "log": 0}
    real = Path.read_bytes

    def counting(path: Path) -> bytes:
        if path.parent == directory:
            opened["receipts"] += 1
        elif path == log:
            opened["log"] += 1
        return real(path)

    with monkeypatch.context() as patch:
        patch.setattr(Path, "read_bytes", counting)
        profile = cProfile.Profile()
        started = time.perf_counter()
        profile.enable()
        records = read()
        profile.disable()
        seconds = time.perf_counter() - started
    text = io.StringIO()
    stats = pstats.Stats(profile, stream=text)
    stats.sort_stats("cumulative").print_stats(8)
    frames = [line.rstrip() for line in text.getvalue().splitlines()
              if line.strip() and ("{" in line or ".py:" in line)]
    return {"seconds": round(seconds, 4), "receipts_opened": opened["receipts"],
            "log_reads": opened["log"], "records": len(records),
            "top_frames": frames[:8], "_records": records}


def _prices(records) -> dict[str, object]:
    return {
        "demand": storage_tiers.mover_demand_from_receipts(
            records, tier_id=TIER, readers=4, fallback_mem_gb=8,
            pool_identity=IDENTITY),
        "fill": storage_tiers.mover_fill_price(
            records, tier_id=TIER, pool_identity=IDENTITY,
            manifest_sha256=f"{7:064x}"),
    }


@pytest.mark.parametrize("history", [8_000, 50_000])
def test_bench_move_pricing(history: int, tmp_path: Path, monkeypatch, capsys) -> None:
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    _file_history(queue, history)
    body = (queue.root / pool.MOVERS / f"{0:064x}.json").stat().st_size
    results: dict[str, object] = {"history": history, "receipt_bytes": body}
    evicted = {}
    for cache in ("warm", "cold"):
        log = queue.move_pricing_log_path()
        if log.exists():
            log.unlink()
        if cache == "cold":
            evicted["before"] = _evict(queue)
        before = _measure(queue, lambda: _full_read(queue), monkeypatch)
        if cache == "cold":
            evicted["after-first"] = _evict(queue)
        first = _measure(queue, lambda: queue.move_records(), monkeypatch)
        for index in range(history, history + FILED_SINCE):
            receipt = _receipt(index)
            queue.record_move(str(receipt["action_key"]), receipt)
        if cache == "cold":
            evicted["after"] = _evict(queue)
        after = _measure(queue, lambda: queue.move_records(), monkeypatch)
        # Same inputs, same prices: the projection prices as the receipts.
        assert _prices(first["_records"]) == _prices(before["_records"])
        assert _prices(after["_records"]) == _prices(_full_read(queue))
        assert after["receipts_opened"] == 0
        for name, value in (("before", before), ("after-first", first),
                            ("after", after)):
            value.pop("_records")
            results[f"{cache}:{name}"] = value
        # Undo this pass's filings so the cold pass reads the same history.
        for index in range(history, history + FILED_SINCE):
            queue.move_path(f"{index:064x}").unlink()
    results["evicted"] = evicted
    results["log_bytes"] = queue.move_pricing_log_path().stat().st_size
    with capsys.disabled():
        print("bench-move-pricing: " + json.dumps(results, sort_keys=True))
