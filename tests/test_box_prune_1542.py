"""Maintenance-only prune removes idle digests and keeps live ones (#1542).

The dry run lists only stale entries and changes nothing. Apply
removes only the stale ones, honours the per-pass bound, keeps an
entry whose digest belongs to a served queue, keeps a held lock, a
partial census publication, a live coordination marker, and any entry
whose evidence is unreadable. Apply refuses without the maintenance
acknowledgement, and refuses when the box is not provably quiet
(a held lock, a live loop, an unresolved claim, a live scope, or
unreadable evidence). Deletion while workers run stays refused.
"""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import adaptive_cpu


def _quiet_proc(tmp_path: Path) -> Path:
    proc = tmp_path / "proc-quiet"
    proc.mkdir(exist_ok=True)
    return proc


def _entry(root: Path, base: Path, name: str, *, age_s: float) -> str:
    directory, digest = adaptive_cpu.box_state(base)
    assert directory == root
    (root / (digest + ".lock")).touch()
    state = root / (digest + ".adaptive-cpu-v1")
    state.mkdir(mode=0o700, exist_ok=True)
    (state / "cpu-sample.json").write_text("{}")
    stamp = time.time() - age_s
    candidates = [root / (digest + ".lock"), state, state / "cpu-sample.json"]
    origin = root / (digest + ".origin.json")
    if origin.exists():
        candidates.append(origin)
    for path in candidates:
        os.utime(path, (stamp, stamp))
    return digest


def _fixture(monkeypatch, tmp_path: Path):
    root = tmp_path / "box-state"
    monkeypatch.setattr(adaptive_cpu, "BOX_STATE_ROOT", root)
    queue = tmp_path / "live-queue"
    (queue / "reservations" / "live-host").mkdir(parents=True)
    (queue / "claimed").mkdir(parents=True)
    live_base = queue / "reservations" / "live-host"
    _, live_digest = adaptive_cpu.box_state(live_base)
    (root / (live_digest + ".lock")).touch()
    stale = _entry(root, tmp_path / "old" / "reservations" / "h", "stale", age_s=8 * 24 * 3600)
    fresh = _entry(root, tmp_path / "new" / "reservations" / "h", "fresh", age_s=60)
    return root, queue, live_base, live_digest, stale, fresh


def test_dry_run_lists_only_stale_and_changes_nothing(tmp_path, monkeypatch):
    root, queue, live_base, live_digest, stale, fresh = _fixture(monkeypatch, tmp_path)
    before = sorted(path.name for path in root.iterdir())
    report = adaptive_cpu.prune_box_state(queue_roots=[queue])
    assert report["dry_run"] is True
    assert report["candidates"] == [stale]
    assert report["kept"][live_digest] == "served queue root"
    assert report["kept"][fresh] == "fresh activity"
    assert sorted(path.name for path in root.iterdir()) == before


def test_apply_removes_only_stale_and_honours_the_pass_bound(tmp_path, monkeypatch):
    root, queue, live_base, live_digest, stale, fresh = _fixture(monkeypatch, tmp_path)
    second = _entry(root, tmp_path / "older" / "reservations" / "h", "second",
                    age_s=9 * 24 * 3600)
    report = adaptive_cpu.prune_box_state(queue_roots=[queue], max_entries_per_pass=1,
                                          apply=True, maintenance_held=True,
                                          proc_root=_quiet_proc(tmp_path))
    assert report["dry_run"] is False
    assert report["removed"] == [second]
    assert adaptive_cpu._box_state_digest_of(second + ".lock") == second
    assert not (root / (second + ".lock")).exists()
    assert (root / (stale + ".lock")).exists()
    assert (root / (live_digest + ".lock")).exists()
    assert (root / (fresh + ".lock")).exists()


def test_a_held_lock_refuses_apply_and_keeps_everything(tmp_path, monkeypatch):
    root, queue, live_base, live_digest, stale, fresh = _fixture(monkeypatch, tmp_path)
    old = time.time() - 8 * 24 * 3600
    for path in (root / (live_digest + ".lock"), root / (live_digest + ".origin.json")):
        if path.exists():
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
        survey = adaptive_cpu.survey_box_state(root, queue_roots=[queue])
        assert survey["kept"][held_digest] == "held admission lock"
        with pytest.raises(adaptive_cpu.PruneRefused):
            adaptive_cpu.prune_box_state(queue_roots=[queue], apply=True,
                                         maintenance_held=True,
                                         proc_root=_quiet_proc(tmp_path))
        assert (root / (stale + ".lock")).exists()
        assert (root / (held_digest + ".lock")).exists()
    finally:
        os.close(descriptor)


def test_apply_keeps_partial_reader_and_coordination_markers(tmp_path, monkeypatch):
    root, queue, _live_base, _live, _stale, _fresh = _fixture(monkeypatch, tmp_path)
    old = time.time() - 8 * 24 * 3600
    _, partial = adaptive_cpu.box_state(tmp_path / "partial" / "reservations" / "h")
    state = root / (partial + ".adaptive-cpu-v1")
    state.mkdir(mode=0o700, exist_ok=True)
    (state / "cpu-sample.json").write_text("{}")
    (root / (partial + ".measurement-reader-v1.writing")).write_text("partial")
    _, marked = adaptive_cpu.box_state(tmp_path / "marked" / "reservations" / "h")
    marked_state = root / (marked + ".adaptive-cpu-v1")
    marked_state.mkdir(mode=0o700, exist_ok=True)
    (marked_state / "cpu-sample.json").write_text("{}")
    (root / (marked + ".sweep")).touch()
    for path in list(root.iterdir()) + [state, state / "cpu-sample.json",
                                        marked_state, marked_state / "cpu-sample.json"]:
        try:
            os.utime(path, (old, old))
        except OSError:
            pass
    survey = adaptive_cpu.survey_box_state(root, queue_roots=[queue])
    assert survey["kept"][partial] == "unresolved census reader"
    assert survey["kept"][marked] == "live coordination marker"
    assert partial not in survey["candidates"]
    assert marked not in survey["candidates"]


def test_apply_protects_a_bare_queue_root_entry(tmp_path, monkeypatch):
    root = tmp_path / "box-state"
    monkeypatch.setattr(adaptive_cpu, "BOX_STATE_ROOT", root)
    queue = tmp_path / "pb-queue"
    (queue / "reservations" / "HOSTX").mkdir(parents=True)
    (queue / "claimed").mkdir(parents=True)
    ledger_base = queue / "reservations" / "HOSTX"
    _, digest = adaptive_cpu.box_state(ledger_base)
    old = time.time() - 8 * 24 * 3600
    state = root / (digest + ".adaptive-cpu-v1")
    state.mkdir(mode=0o700, exist_ok=True)
    (state / "cpu-sample.json").write_text("{}")
    for path in list(root.iterdir()) + [state, state / "cpu-sample.json"]:
        path.touch(exist_ok=True)
        os.utime(path, (old, old))
    survey = adaptive_cpu.survey_box_state(root, queue_roots=[queue])
    assert survey["kept"][digest] == "served queue root"
    assert digest not in survey["candidates"]


def test_apply_without_the_hold_refuses(tmp_path, monkeypatch):
    root, queue, live_base, _live, stale, _fresh = _fixture(monkeypatch, tmp_path)
    with pytest.raises(adaptive_cpu.PruneRefused):
        adaptive_cpu.prune_box_state(queue_roots=[queue], apply=True,
                                     maintenance_held=False,
                                     proc_root=_quiet_proc(tmp_path))
    assert (root / (stale + ".lock")).exists()


def test_apply_refuses_when_a_lock_is_held(tmp_path, monkeypatch):
    root, queue, _live_base, _live, stale, _fresh = _fixture(monkeypatch, tmp_path)
    import fcntl
    descriptor = os.open(root / (stale + ".lock"), os.O_RDWR)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(adaptive_cpu.PruneRefused):
            adaptive_cpu.prune_box_state(queue_roots=[queue], apply=True,
                                         maintenance_held=True,
                                         proc_root=_quiet_proc(tmp_path))
        assert (root / (stale + ".lock")).exists()
    finally:
        os.close(descriptor)


def test_apply_refuses_with_an_unresolved_claim(tmp_path, monkeypatch):
    root, queue, _live_base, _live, stale, _fresh = _fixture(monkeypatch, tmp_path)
    (queue / "claimed" / ("ab12" * 16 + ".json")).write_text("{}")
    with pytest.raises(adaptive_cpu.PruneRefused):
        adaptive_cpu.prune_box_state(queue_roots=[queue], apply=True,
                                     maintenance_held=True,
                                     proc_root=_quiet_proc(tmp_path))
    assert (root / (stale + ".lock")).exists()


def test_apply_refuses_with_a_live_loop_in_proc(tmp_path, monkeypatch):
    root, queue, _live_base, _live, stale, _fresh = _fixture(monkeypatch, tmp_path)
    proc = tmp_path / "proc-busy"
    pid_dir = proc / "4242"
    pid_dir.mkdir(parents=True)
    (pid_dir / "cmdline").write_bytes(b"python3\x00worker_loop.py\x00--once\x00")
    (pid_dir / "cgroup").write_text("0::/user.slice\n")
    with pytest.raises(adaptive_cpu.PruneRefused):
        adaptive_cpu.prune_box_state(queue_roots=[queue], apply=True,
                                     maintenance_held=True, proc_root=proc)
    assert (root / (stale + ".lock")).exists()


def test_apply_refuses_when_evidence_is_unreadable(tmp_path, monkeypatch):
    root, queue, _live_base, _live, stale, _fresh = _fixture(monkeypatch, tmp_path)
    with pytest.raises(adaptive_cpu.PruneRefused):
        adaptive_cpu.prune_box_state(queue_roots=[queue], apply=True,
                                     maintenance_held=True,
                                     proc_root=tmp_path / "no-such-proc")
    assert (root / (stale + ".lock")).exists()
