"""Prune never splits admission exclusion: it refuses unless quiet (#1542).

The module's own rule stays: unlinking under a live loop can leave a
holder of the old inode beside a new opener, which lapses the mutual
exclusion the file exists for. So removal runs only inside a proven
quiet window, after an acknowledged maintenance hold, and no test
asserts that a rename preserves exclusion. Concurrent openers of one
name still end up in one file with one holder.
"""
from __future__ import annotations

import fcntl
import multiprocessing as mp
import os
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import adaptive_cpu


def _try_hold(path: str, queue) -> None:
    descriptor = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            queue.put(("busy", os.fstat(descriptor).st_ino))
            time.sleep(5)
            return
        queue.put(("held", os.fstat(descriptor).st_ino))
        time.sleep(5)
    finally:
        os.close(descriptor)


def _quiet_proc(tmp_path: Path) -> Path:
    proc = tmp_path / "proc-quiet"
    proc.mkdir(exist_ok=True)
    return proc


def _aged_entry(root: Path, base: Path, *, age_s: float) -> str:
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


def test_a_holder_before_removal_refuses_the_prune(tmp_path, monkeypatch):
    root = tmp_path / "box-state"
    monkeypatch.setattr(adaptive_cpu, "BOX_STATE_ROOT", root)
    base = tmp_path / "queue" / "reservations" / "h"
    base.mkdir(parents=True)
    queue = tmp_path / "served"
    (queue / "reservations").mkdir(parents=True)
    (queue / "ready").mkdir(parents=True)
    (queue / "claimed").mkdir(parents=True)

    stale = _aged_entry(root, base, age_s=8 * 24 * 3600)
    name = stale + ".lock"
    live_inode = os.stat(root / name).st_ino
    held = os.open(root / name, os.O_RDWR)
    try:
        fcntl.flock(held, fcntl.LOCK_EX | fcntl.LOCK_NB)
        proof = adaptive_cpu.prove_box_quiescent(root, queue_roots=[queue],
                                                 proc_root=_quiet_proc(tmp_path))
        assert proof == {'quiet': False, 'reason': 'lock held: %s' % name}
        try:
            adaptive_cpu.prune_box_state(queue_roots=[queue], apply=True,
                                         maintenance_held=True, directory=root,
                                         proc_root=_quiet_proc(tmp_path))
        except adaptive_cpu.PruneRefused:
            pass
        else:
            raise AssertionError("prune removed under a held lock")
        assert os.stat(root / name).st_ino == live_inode
        assert os.fstat(held).st_ino == live_inode
    finally:
        os.close(held)


def test_an_opener_between_probe_and_removal_breaks_quiescence(tmp_path, monkeypatch):
    root = tmp_path / "box-state"
    root.mkdir()
    monkeypatch.setattr(adaptive_cpu, "BOX_STATE_ROOT", root)
    base = tmp_path / "queue" / "reservations" / "h"
    base.mkdir(parents=True)
    queue = tmp_path / "served"
    (queue / "reservations").mkdir(parents=True)
    (queue / "ready").mkdir(parents=True)
    (queue / "claimed").mkdir(parents=True)

    stale = _aged_entry(root, base, age_s=8 * 24 * 3600)
    real_scandir = os.scandir
    taken = []

    def scandir_then_hold(path, *args, **kwargs):
        entries = real_scandir(path, *args, **kwargs)
        if str(path) == str(root) and not taken:
            holder = os.open(root / (stale + ".lock"), os.O_RDWR)
            fcntl.flock(holder, fcntl.LOCK_EX | fcntl.LOCK_NB)
            taken.append(holder)
        return entries

    monkeypatch.setattr(os, "scandir", scandir_then_hold)
    try:
        try:
            adaptive_cpu.prune_box_state(queue_roots=[queue], apply=True,
                                         maintenance_held=True, directory=root,
                                         proc_root=_quiet_proc(tmp_path))
        except adaptive_cpu.PruneRefused:
            pass
        else:
            raise AssertionError("prune removed after a new holder arrived")
        assert (root / (stale + ".lock")).exists()
    finally:
        for holder in taken:
            os.close(holder)


def test_concurrent_openers_share_one_lock_inode(tmp_path, monkeypatch):
    root = tmp_path / "box-state"
    root.mkdir()
    monkeypatch.setattr(adaptive_cpu, "BOX_STATE_ROOT", root)
    base = tmp_path / "queue" / "reservations" / "h"
    base.mkdir(parents=True)
    directory, digest = adaptive_cpu.box_state(base)
    context = mp.get_context("spawn")
    queue = context.Queue()
    first = context.Process(target=_try_hold, args=(str(directory / (digest + ".lock")), queue))
    second = context.Process(target=_try_hold, args=(str(directory / (digest + ".lock")), queue))
    first.start()
    second.start()
    try:
        first.join(timeout=30)
        second.join(timeout=30)
        outcomes = sorted(queue.get(timeout=30) for _ in range(2))
        assert [outcome for outcome, _inode in outcomes] == ["busy", "held"]
        assert outcomes[0][1] == outcomes[1][1]
    finally:
        for child in (first, second):
            if child.is_alive():
                child.terminate()
                child.join(timeout=10)


def _hold_until_released(path, ready, release):
    descriptor = os.open(path, os.O_RDWR)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        ready.put(os.fstat(descriptor).st_ino)
        if not release.wait(30):
            raise RuntimeError("the parent did not release the holder")
    finally:
        os.close(descriptor)


@pytest.mark.parametrize("suffix", [".lock", ".sweep", ".preemption", ".guard"])
def test_nonadaptive_holder_in_another_process_refuses_all_removal(tmp_path, suffix):
    root = tmp_path / "box-state"
    root.mkdir()
    queue = tmp_path / "served"
    for name in ("reservations", "ready", "claimed"):
        (queue / name).mkdir(parents=True)
    digest = adaptive_cpu.box_identity(tmp_path / "obsolete")
    held_path = root / (digest + suffix)
    idle_path = root / ("01" * 32 + ".lock")
    old = time.time() - 8 * 24 * 3600
    for path in (held_path, idle_path):
        path.write_text("keep")
        os.utime(path, (old, old))
    before = {path.name: (path.stat().st_ino, path.read_bytes()) for path in root.iterdir()}
    context = mp.get_context("spawn")
    ready, release = context.Queue(), context.Event()
    holder = context.Process(target=_hold_until_released, args=(str(held_path), ready, release))
    holder.start()
    try:
        assert ready.get(timeout=30) == before[held_path.name][0]
        with pytest.raises(adaptive_cpu.PruneRefused, match="lock held"):
            adaptive_cpu.prune_box_state(
                directory=root, queue_roots=[queue], apply=True, maintenance_held=True,
                proc_root=_quiet_proc(tmp_path))
        assert {path.name: (path.stat().st_ino, path.read_bytes())
                for path in root.iterdir()} == before
    finally:
        release.set()
        holder.join(timeout=30)
        if holder.is_alive():
            holder.terminate()
            holder.join(timeout=10)
    assert holder.exitcode == 0
