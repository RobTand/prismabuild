"""Prune renames cannot strand an admission lock holder (#1542).

A holder present before removal keeps mutual exclusion: a newcomer that
opens the name after the rename re-opens the live name instead of locking
an inode nobody else can reach. Concurrent openers of one name end up in
one file with one holder.
"""
from __future__ import annotations

import contextlib
import fcntl
import multiprocessing as mp
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import adaptive_cpu


def _hold(path: str, ready, release) -> None:
    descriptor = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        ready.put(os.fstat(descriptor).st_ino)
        assert release.get(timeout=30), "the parent never released the holder"
    finally:
        os.close(descriptor)


def _try_hold(path: str, queue) -> None:
    descriptor = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            queue.put(("busy", os.fstat(descriptor).st_ino))
            return
        queue.put(("held", os.fstat(descriptor).st_ino))
        time.sleep(5)
    finally:
        os.close(descriptor)


def test_a_holder_before_removal_keeps_exclusion(tmp_path, monkeypatch):
    root = tmp_path / "box-state"
    monkeypatch.setattr(adaptive_cpu, "BOX_STATE_ROOT", root)
    base = tmp_path / "queue" / "reservations" / "h"
    base.mkdir(parents=True)

    class Ledger:
        def __init__(self, value: Path):
            self.base = value

    directory, digest = adaptive_cpu.box_state(base)
    name = digest + ".lock"
    (directory / name).touch()
    live_inode = os.stat(directory / name).st_ino
    held = os.open(directory / name, os.O_RDWR)
    try:
        fcntl.flock(held, fcntl.LOCK_EX | fcntl.LOCK_NB)
        # The prune-side rename while a holder is present: the holder keeps
        # the old inode, and the name is gone until the next opener arrives.
        staged = root / ".pruned-test"
        staged.mkdir()
        os.rename(root / name, staged / name)
        gate = adaptive_cpu.AdmissionGate(Ledger(base))
        with gate.locked():
            assert os.stat(directory / name).st_ino != live_inode
            assert os.fstat(held).st_ino == live_inode
    finally:
        with contextlib.suppress(OSError):
            os.close(held)


def test_a_rename_between_open_and_lock_is_noticed(tmp_path, monkeypatch):
    root = tmp_path / "box-state"
    root.mkdir()
    monkeypatch.setattr(adaptive_cpu, "BOX_STATE_ROOT", root)
    base = tmp_path / "queue" / "reservations" / "h"
    base.mkdir(parents=True)

    class Ledger:
        def __init__(self, value: Path):
            self.base = value

    directory, digest = adaptive_cpu.box_state(base)
    name = digest + ".lock"
    real_open = os.open
    renamed = []

    def open_then_rename(path, *args, **kwargs):
        descriptor = real_open(path, *args, **kwargs)
        if not renamed and str(path).endswith(name):
            staged = root / ".pruned-race"
            staged.mkdir(exist_ok=True)
            real_open_fn = real_open
            os.rename(root / name, staged / name)
            fresh = real_open_fn(str(root / name), os.O_CREAT | os.O_RDWR, 0o600)
            os.close(fresh)
            renamed.append(True)
        return descriptor

    monkeypatch.setattr(os, "open", open_then_rename)
    gate = adaptive_cpu.AdmissionGate(Ledger(base))
    with gate.locked():
        assert renamed == [True]
        assert os.stat(directory / name).st_nlink == 1


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
