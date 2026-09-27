"""A tiny window is never held by the disk pacer (#1235).

The measured tail: a 6-10 MiB mover's own ``phase_timings`` put 85.0 s of an
85.08 s child in a single ``pace_wait`` while its ``copy_read`` was 0.05 s --
``DiskPacer.wait`` held the copy for the whole duration of recurring client
streams, protecting the pool from reads that were a rounding error against
the share the mover's claim had already declared.  A copy whose whole window
fits inside that declared one-second share is never held, still measured, as
#1091's consumer-blocked copy is; a window with no declared share, or bigger
than the share, is held exactly as before.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "tools" / "fleet")]
import prewarm_loop, stage_move  # noqa: E402
from prismabuild import core as pb, pool  # noqa: E402

MIB = 1 << 20


def _window(tmp_path: Path, entries: int, entry_bytes: int):
    """Real one-byte-pattern files under a private mount; (manifest, digest, total)."""

    mount = tmp_path / "sources"
    mount.mkdir(exist_ok=True)
    listed = []
    for index in range(entries):
        payload = bytes([index % 251]) * entry_bytes
        source = mount / f"entry-{index}.bin"
        source.write_bytes(payload)
        listed.append({"path": str(source), "offset": 0, "bytes": entry_bytes,
                       "sha256": hashlib.sha256(payload).hexdigest()})
    total = entries * entry_bytes
    manifest = {"schema": pb.DATA_MANIFEST_SCHEMA_V1, "produced_by": {},
                "annotations": {"phases": [{"name": "layer-0", "bytes": total,
                                            "cumulative_bytes": total}]},
                "mount_prefix": str(mount), "entries": listed,
                "entry_count": entries, "total_bytes": total}
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest))
    return path, hashlib.sha256(path.read_bytes()).hexdigest(), total


def _run(tmp_path: Path, monkeypatch, *, fill_mb_s: int, entries: int,
         entry_bytes: int) -> tuple[dict, dict]:
    """Run one mover in-process against a pacer that always holds on wait.

    Returns (receipt payload, pacer call counts).  The holding pacer is the
    production ``DiskPacer`` subclass shape the #1010 suite uses: ``wait``
    enters a real hold and sleeps, so the receipt's hold accounting is the
    real one; ``sample`` only counts.
    """

    manifest, digest, total = _window(tmp_path, entries, entry_bytes)
    (tmp_path / "queue").mkdir(exist_ok=True)
    (tmp_path / "stage").mkdir(exist_ok=True)
    (tmp_path / "cas").mkdir(exist_ok=True)
    calls = {"wait": 0, "sample": 0}

    class Holding(prewarm_loop.DiskPacer):
        def wait(self, stop=None, abort=None):
            calls["wait"] += 1
            self._enter_hold()
            try:
                time.sleep(2.0)
            finally:
                self._leave_hold()

        def sample(self):
            calls["sample"] += 1

    monkeypatch.setattr(prewarm_loop, "pacer_from_args", lambda args: Holding(
        ["sdz"], max_util_pct=100.0, max_read_await_ms=1e9, max_backlog_ms=1e9,
        readers=1, max_readers=1))
    receipt_path = tmp_path / "receipt.json"
    argv = ["--pool-root", str(tmp_path / "queue"),
            "--action-key", "a" * 64,
            "--cas-root", str(tmp_path / "cas"),
            "--consumer-action-key", "c" * 64,
            "--tier-id", "tier-x",
            "--stage-root", str(tmp_path / "stage"),
            "--manifest-sha256", digest,
            "--manifest", str(manifest),
            "--range-start-bytes", "0", "--range-end-bytes", str(total),
            "--readers", "1", "--max-readers", "1", "--block", str(MIB),
            "--warm-after-copy", "never", "--unpaced",
            "--fill-mb-s-pool-side", str(fill_mb_s),
            "--receipt", str(receipt_path)]
    code = stage_move.main(argv)
    assert code == 0, code
    return json.loads(receipt_path.read_bytes()), calls


def test_a_tiny_window_is_never_held(tmp_path, monkeypatch):
    """#1235 RED: 64 KiB inside a 60 MB/s share must not wait for the pool.

    On main the copy pays the full 2 s hold for a read that takes
    milliseconds; with the rule it samples the pool and copies at once.
    """

    receipt, calls = _run(tmp_path, monkeypatch, fill_mb_s=60,
                          entries=1, entry_bytes=64 * 1024)
    pace = receipt["phase_timings"]["thread_seconds"]["pace_wait"]
    assert pace["seconds"] < 0.5, (
        f"the tiny window waited {pace['seconds']}s for a pool its share bounds")
    assert calls["wait"] == 0, calls
    assert calls["sample"] >= 1, calls
    assert receipt["complete"] is True, receipt.get("errors")


def test_a_window_bigger_than_its_share_is_still_held(tmp_path, monkeypatch):
    """No regression: 2 MiB inside a 1 MB/s share keeps the ordinary hold."""

    receipt, calls = _run(tmp_path, monkeypatch, fill_mb_s=1,
                          entries=2, entry_bytes=MIB)
    pace = receipt["phase_timings"]["thread_seconds"]["pace_wait"]
    assert calls["wait"] >= 1, calls
    assert pace["seconds"] >= 1.5, pace
    assert receipt["complete"] is True, receipt.get("errors")


def test_a_window_with_no_declared_share_is_still_held(tmp_path, monkeypatch):
    """Conservative default: no declared share means the hold stands as ever."""

    receipt, calls = _run(tmp_path, monkeypatch, fill_mb_s=0,
                          entries=1, entry_bytes=64 * 1024)
    pace = receipt["phase_timings"]["thread_seconds"]["pace_wait"]
    assert calls["wait"] >= 1, calls
    assert pace["seconds"] >= 1.5, pace
