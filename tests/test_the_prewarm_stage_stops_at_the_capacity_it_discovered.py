"""A stage that runs out stops staging; the warm does not even slow down.

Losing the second destination must never cost the first.  The ARC destination
is the one this loop has always had and the one the campaign actually reads,
so a stage pool that fills, faults or refuses a write is recorded as ``full``
and stepped over -- the window is read to its end either way.

Capacity is the dataset's own ``available`` less whatever floor is kept, read
every cycle.  Nothing here is configured: add a device to the stage pool and
the next cycle offers more.
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


def test_the_budget_is_the_discovered_capacity_and_it_binds(
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


def test_capacity_is_the_datasets_available_not_the_pools_free(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The budget describes the thing being written to.

    ``zpool list`` reports pool-level ``free``, which has withheld nothing:
    not the pool's slop space, not the dataset's quota or reservation, not
    the metadata the write itself costs.  A budget taken from it stops later
    than the kernel does, so the tier's own hard stop never binds and every
    fill ends at ENOSPC instead.
    """

    fleet = Fleet(tmp_path)
    fleet.action("row", [fleet.file("first.pt", 4096),
                         fleet.file("second.pt", 4096)])
    stage = StagePool(tmp_path, size=1 << 20, free=1 << 20, available=4096)
    stage.install(monkeypatch)

    event = fleet.cycle(fleet.args(stage=True, readers=1, max_readers=1))

    assert event["stage"]["capacity_bytes"] == 4096
    assert event["stage"]["capacity_source"] == "dataset available"
    assert event["stage"]["free_bytes"] == 1 << 20
    # Nothing of this module's invention is kept back: ZFS already did that.
    assert event["stage"]["free_floor_bytes"] == 0
    assert event["stage"]["budget_bytes"] == 4096
    assert event["stage"]["staged_bytes"] == 4096
    assert event["stage"]["state"] == "full"
    assert len(stage.objects()) == 1


def test_a_dataset_that_cannot_answer_falls_back_and_says_so(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The fallback is a real answer to a different question.

    Pool ``free`` has not withheld the slop ZFS keeps, so the floor becomes
    the reserve ZFS itself would have withheld -- 1/32 of the pool, never
    below 128 MiB -- and the record names the number it used.
    """

    fleet = Fleet(tmp_path)
    fleet.action("row", [fleet.file("row.pt", 8192)])
    size = 64 << 30
    stage = StagePool(tmp_path, size=size, free=size,
                      answers_available=False)
    stage.install(monkeypatch)

    event = fleet.cycle(fleet.args(stage=True))

    assert event["stage"]["capacity_source"] == "pool free"
    assert event["stage"]["capacity_bytes"] == size
    assert event["stage"]["free_floor_bytes"] == size >> 5
    assert event["stage"]["budget_bytes"] == size - (size >> 5)
    assert event["stage"]["state"] == "present"


def test_a_stated_floor_outranks_the_derived_one(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    fleet = Fleet(tmp_path)
    fleet.action("row", [fleet.file("row.pt", 8192)])
    StagePool(tmp_path, size=1 << 30, free=1 << 30,
              available=1 << 30).install(monkeypatch)

    event = fleet.cycle(fleet.args(stage=True,
                                   stage_free_floor_bytes=(1 << 30) - 4096))

    assert event["stage"]["free_floor_bytes"] == (1 << 30) - 4096
    assert event["stage"]["budget_bytes"] == 4096


class Faulted:
    """The real ``os``, except that every stage write is EIO.

    A stage pool set ``failmode=continue`` answers a lost device this way
    rather than blocking, so this is what a fault looks like from inside the
    loop: not exhaustion, and not an exception the loop can call full.
    """

    def __init__(self, real) -> None:
        self._real = real

    def __getattr__(self, name):
        return getattr(self._real, name)

    def write(self, handle, data):  # noqa: ARG002
        raise OSError(errno.EIO, "Input/output error")


def test_a_fault_is_a_state_and_it_stops_the_retry(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A tier that is there and not working must not read as healthy.

    Exhaustion was the only failure with a state.  Anything else -- EIO under
    ``failmode=continue``, ESTALE, EROFS -- was noted into a list that stops
    at twenty entries while every receipt in the cycle went on saying
    ``present``, ``stage pool discovered``, ``staged_bytes: 0``: a healthy
    tier that staged nothing.  And the loop kept opening one object per
    manifest entry for the rest of the cycle, each one failing the same way.
    """

    fleet = Fleet(tmp_path)
    key = fleet.action("row", [fleet.file("a.pt", 4096),
                               fleet.file("b.pt", 4096)])
    stage = StagePool(tmp_path)
    stage.install(monkeypatch)
    monkeypatch.setattr(prewarm_loop, "os", Faulted(os))

    event = fleet.cycle(fleet.args(stage=True, readers=1, max_readers=1))

    assert event["stage"]["state"] == "faulted"
    assert "fault" in event["stage"]["reason"]
    assert event["stage"]["staged_bytes"] == 0
    # The first entry failed; the second was never opened, because a faulted
    # tier is not usable for the rest of the cycle.
    assert len(event["stage"]["errors"]) == 1
    assert list(stage.mount.rglob("*")) == []
    # And the warm is untouched, as it is for every other stage failure.
    assert fleet.queue.prewarm(key)["status"] == "complete"
