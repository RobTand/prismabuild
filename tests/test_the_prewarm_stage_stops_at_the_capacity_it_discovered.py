"""A stage that runs out stops staging; the warm does not even slow down.

Losing the second destination must never cost the first.  The ARC destination
is the one this loop has always had and the one the campaign actually reads,
so a stage pool that fills, faults or refuses a write is recorded as ``full``
and stepped over -- the window is read to its end either way.

Capacity is the pool's own ``free`` less the floor it keeps, read every cycle.
Nothing here is configured: add a device to the stage pool and the next cycle
offers more.
"""
from __future__ import annotations

import errno
import os
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from prewarm_fixture import Fleet, StagePool  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import prewarm_loop  # noqa: E402


class OutOfSpace:
    """The real ``os``, except that every write is ENOSPC.

    Installed as ``prewarm_loop.os`` rather than on the module itself, so the
    failure reaches the stage writer and nothing else in the process.
    """

    def __init__(self, real) -> None:
        self._real = real

    def __getattr__(self, name):
        return getattr(self._real, name)

    def write(self, handle, data):
        raise OSError(errno.ENOSPC, "No space left on device")


def test_the_budget_is_the_pools_free_space_and_it_binds(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    fleet = Fleet(tmp_path)
    key = fleet.action("row", [fleet.file("first.pt", 4096),
                               fleet.file("second.pt", 4096)])
    stage = StagePool(tmp_path, size=8192, free=4096)
    stage.install(monkeypatch)

    event = fleet.cycle(fleet.args(stage=True, stage_free_floor_bytes=0,
                                   readers=1, max_readers=1))

    assert event["stage"]["budget_bytes"] == 4096
    assert event["stage"]["staged_bytes"] == 4096
    assert event["stage"]["staged_entries"] == 1
    assert event["stage"]["state"] == "full"
    assert "budget" in event["stage"]["reason"]
    assert len(stage.objects()) == 1
    # The warm is untouched: both files were read, and the receipt says so.
    record = fleet.queue.prewarm(key)
    assert record["bytes_warmed"] == 8192
    assert record["status"] == "complete"


def test_a_write_that_runs_out_of_space_stops_the_stage_not_the_warm(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    fleet = Fleet(tmp_path)
    key = fleet.action("row", [fleet.file("row.pt", 8192)])
    stage = StagePool(tmp_path)
    stage.install(monkeypatch)
    monkeypatch.setattr(prewarm_loop, "os", OutOfSpace(os))

    event = fleet.cycle(fleet.args(stage=True, stage_free_floor_bytes=0))

    assert event["stage"]["state"] == "full"
    assert "out of space" in event["stage"]["reason"]
    assert event["stage"]["staged_bytes"] == 0
    assert event["stage"]["errors"]
    record = fleet.queue.prewarm(key)
    assert record["bytes_warmed"] == 8192
    assert record["status"] == "complete"


def test_a_partial_copy_never_earns_a_name(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Only a whole entry is renamed into place.

    Nothing under the stage root is ever a short file wearing a full name,
    and a failed copy leaves no temporary behind either: a consumer that
    arrives later must not find a truncated object that looks complete.
    """

    fleet = Fleet(tmp_path)
    fleet.action("row", [fleet.file("row.pt", 8192)])
    stage = StagePool(tmp_path)
    stage.install(monkeypatch)
    monkeypatch.setattr(prewarm_loop, "os", OutOfSpace(os))

    fleet.cycle(fleet.args(stage=True, stage_free_floor_bytes=0))

    assert list(stage.mount.rglob("*")) == []
