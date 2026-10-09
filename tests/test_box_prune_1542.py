"""Maintenance-only prune removes idle digests and keeps live ones (#1542).

The dry run lists only stale entries and changes nothing. Apply removes
only the stale ones, honours the per-pass bound, keeps an old entry whose
digest belongs to a served queue root, keeps a held lock, and refuses
without the maintenance hold. Deletion while workers run stays refused.
"""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import adaptive_cpu


def _entry(root: Path, base: Path, name: str, *, age_s: float) -> str:
    directory, digest = adaptive_cpu.box_state(base)
    assert directory == root
    (root / (digest + ".lock")).touch()
    state = root / (digest + ".adaptive-cpu-v1")
    state.mkdir(mode=0o700, exist_ok=True)
    (state / "cpu-sample.json").write_text("{}")
    stamp = time.time() - age_s
    for path in (root / (digest + ".lock"), root / (digest + ".origin.json"),
                 state, state / "cpu-sample.json"):
        os.utime(path, (stamp, stamp))
    return digest

def _fixture(monkeypatch, tmp_path: Path):
    root = tmp_path / "box-state"
    monkeypatch.setattr(adaptive_cpu, "BOX_STATE_ROOT", root)
    live_base = tmp_path / "live" / "reservations" / "h"
    live_base.mkdir(parents=True)
    _, live_digest = adaptive_cpu.box_state(live_base)
    (root / (live_digest + ".lock")).touch()
    stale = _entry(root, tmp_path / "old" / "reservations" / "h", "stale", age_s=8 * 24 * 3600)
    fresh = _entry(root, tmp_path / "new" / "reservations" / "h", "fresh", age_s=60)
    return root, live_base, live_digest, stale, fresh


def test_dry_run_lists_only_stale_and_changes_nothing(tmp_path, monkeypatch):
    root, live_base, live_digest, stale, fresh = _fixture(monkeypatch, tmp_path)
    before = sorted(path.name for path in root.iterdir())
    report = adaptive_cpu.prune_box_state(queue_roots=[live_base])
    assert report["dry_run"] is True
    assert report["candidates"] == [stale]
    assert report["kept"][live_digest] == "served queue root"
    assert report["kept"][fresh] == "fresh activity"
    assert sorted(path.name for path in root.iterdir()) == before


def test_apply_removes_only_stale_and_honours_the_pass_bound(tmp_path, monkeypatch):
    root, live_base, live_digest, stale, fresh = _fixture(monkeypatch, tmp_path)
    second = _entry(root, tmp_path / "older" / "reservations" / "h", "second",
                    age_s=9 * 24 * 3600)
    report = adaptive_cpu.prune_box_state(queue_roots=[live_base], max_entries_per_pass=1,
                                          apply=True, maintenance_held=True)
    assert report["dry_run"] is False
    assert report["removed"] == [second]
    assert adaptive_cpu._box_state_digest_of(second + ".lock") == second
    assert not (root / (second + ".lock")).exists()
    assert (root / (stale + ".lock")).exists()
    assert (root / (live_digest + ".lock")).exists()
    assert (root / (fresh + ".lock")).exists()


def test_apply_keeps_a_served_root_and_a_held_lock(tmp_path, monkeypatch):
    root, live_base, live_digest, stale, fresh = _fixture(monkeypatch, tmp_path)
    old = time.time() - 8 * 24 * 3600
    for path in (root / (live_digest + ".lock"), root / (live_digest + ".origin.json")):
        os.utime(path, (old, old))
    directory, held_digest = adaptive_cpu.box_state(tmp_path / "held" / "reservations" / "h")
    held_lock = directory / (held_digest + ".lock")
    held_lock.touch()
    import fcntl
    descriptor = os.open(held_lock, os.O_RDWR)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        state = root / (held_digest + ".adaptive-cpu-v1")
        state.mkdir(mode=0o700, exist_ok=True)
        (state / "cpu-sample.json").write_text("{}")
        for path in (held_lock, state / "cpu-sample.json"):
            os.utime(path, (old, old))
        report = adaptive_cpu.prune_box_state(queue_roots=[live_base], apply=True,
                                              maintenance_held=True)
    finally:
        os.close(descriptor)
    assert stale in report["removed"]
    assert live_digest not in report["removed"]
    assert held_digest not in report["removed"]
    assert report["kept"][held_digest] == "held admission lock"


def test_apply_without_the_hold_refuses(tmp_path, monkeypatch):
    root, live_base, _live, stale, _fresh = _fixture(monkeypatch, tmp_path)
    with pytest.raises(adaptive_cpu.PruneRefused):
        adaptive_cpu.prune_box_state(queue_roots=[live_base], apply=True,
                                     maintenance_held=False)
    assert (root / (stale + ".lock")).exists()
