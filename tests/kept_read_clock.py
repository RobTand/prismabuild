"""Logical coarse-clock fences over real filesystem observations."""
from __future__ import annotations

import os
import time


def after_paths(monkeypatch, *paths) -> dict[str, int]:
    observations = [os.stat(path) for path in paths]
    state = {"now": max(max(info.st_mtime_ns, info.st_ctime_ns)
                        for info in observations) + 1}
    real = time.clock_gettime_ns

    def clock(clock_id):
        return state["now"] if clock_id == 5 else real(clock_id)

    monkeypatch.setattr(time, "clock_gettime_ns", clock)
    return state


def directory_before(monkeypatch, directory, fence: int) -> None:
    real = os.lstat

    class Observation:
        def __init__(self, info):
            self.info = info
            self.st_mtime_ns = fence - 1
            self.st_ctime_ns = fence - 1

        def __getattr__(self, name):
            return getattr(self.info, name)

    def lstat(path, *args, **kwargs):
        info = real(path, *args, **kwargs)
        return Observation(info) if os.fspath(path) == str(directory) else info

    monkeypatch.setattr(os, "lstat", lstat)
