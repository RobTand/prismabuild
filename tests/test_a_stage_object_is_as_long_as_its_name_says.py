"""A stage object's name declares a byte range, and its content honours it.

``write(2)`` may transfer fewer bytes than it was handed.  A copy loop that
trusted the request length would count bytes the kernel never took and then
rename the temporary onto a name declaring the whole range -- a short object
wearing a complete name, which is the one thing a residency map cannot
detect.  So the write is drained, and the rename is refused unless the object
is as long as its own name says.
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


class ShortWrites:
    """The real ``os``, except that the first write of each handle is short.

    Installed as ``prewarm_loop.os`` so the behaviour reaches the stage writer
    and nothing else in the process.  ``fail_after`` makes the *retry* fail,
    which is the pool-ran-out-partway shape: some bytes accepted, the rest
    refused.
    """

    def __init__(self, real, *, fail_after: bool = False) -> None:
        self._real = real
        self._fail_after = fail_after
        self.short_handles: set[int] = set()
        self.writes: list[int] = []

    def __getattr__(self, name):
        return getattr(self._real, name)

    def write(self, handle, data):
        self.writes.append(len(data))
        if handle not in self.short_handles:
            self.short_handles.add(handle)
            half = len(data) // 2
            return self._real.write(handle, data[:half])
        if self._fail_after:
            raise OSError(errno.ENOSPC, "No space left on device")
        return self._real.write(handle, data)


class NoProgress:
    """A ``write`` that accepts nothing and raises nothing."""

    def __init__(self, real) -> None:
        self._real = real

    def __getattr__(self, name):
        return getattr(self._real, name)

    def write(self, handle, data):  # noqa: ARG002
        return 0


def test_a_short_write_is_drained_and_the_object_is_whole(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    fleet = Fleet(tmp_path)
    source = tmp_path / "shared" / "row.pt"
    fleet.action("row", [fleet.file("row.pt", 8192)])
    source.write_bytes(bytes(range(256)) * 32)
    stage = StagePool(tmp_path)
    stage.install(monkeypatch)
    wrapper = ShortWrites(os)
    monkeypatch.setattr(prewarm_loop, "os", wrapper)

    event = fleet.cycle(fleet.args(stage=True, stage_free_floor_bytes=0,
                                   readers=1, max_readers=1))

    assert stage.objects() == ["row.pt.pbstage@0+8192"]
    staged = stage.mount / "row.pt.pbstage@0+8192"
    assert staged.stat().st_size == 8192
    assert staged.read_bytes() == source.read_bytes()
    assert event["stage"]["staged_bytes"] == 8192
    assert event["stage"]["aborted_entries"] == 0
    # The short write was real: the drain loop went round twice.
    assert wrapper.writes == [8192, 4096]


def test_a_short_write_that_cannot_be_drained_earns_no_name(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    fleet = Fleet(tmp_path)
    key = fleet.action("row", [fleet.file("row.pt", 8192)])
    stage = StagePool(tmp_path)
    stage.install(monkeypatch)
    monkeypatch.setattr(prewarm_loop, "os", ShortWrites(os, fail_after=True))

    event = fleet.cycle(fleet.args(stage=True, stage_free_floor_bytes=0,
                                   readers=1, max_readers=1))

    assert list(stage.mount.rglob("*")) == []
    assert event["stage"]["staged_bytes"] == 0
    assert event["stage"]["staged_entries"] == 0
    # The bytes the kernel did take are counted, and counted apart.
    assert event["stage"]["aborted_entries"] == 1
    assert event["stage"]["aborted_bytes"] == 4096
    assert event["stage"]["state"] == "full"
    # And the warm itself is untouched.
    assert fleet.queue.prewarm(key)["status"] == "complete"


def test_a_write_that_accepts_nothing_is_a_failure_not_a_spin(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    fleet = Fleet(tmp_path)
    fleet.action("row", [fleet.file("row.pt", 8192)])
    stage = StagePool(tmp_path)
    stage.install(monkeypatch)
    monkeypatch.setattr(prewarm_loop, "os", NoProgress(os))

    event = fleet.cycle(fleet.args(stage=True, stage_free_floor_bytes=0,
                                   readers=1, max_readers=1))

    assert list(stage.mount.rglob("*")) == []
    assert event["stage"]["staged_bytes"] == 0
    assert event["stage"]["errors"]


def test_commit_refuses_an_object_shorter_than_its_name(tmp_path: Path) -> None:
    """The guard is on the object, not on the reader that fed it.

    ``commit`` is reached from the reader's own ``got == want_total`` check
    today; that check tests the *read*.  The rename must test the *write*, or
    a byte the kernel refused is published as a byte the stage holds.
    """

    mount = tmp_path / "stage"
    mount.mkdir()
    tier = prewarm_loop.StageTier(
        state="present", reason="test", mountpoint=str(mount),
        free_bytes=1 << 20)
    sink = tier.open_object("row.pt.pbstage@0+8192", 8192)
    assert sink is not None
    assert sink.write(memoryview(b"\0" * 4096)) is True
    sink.commit()

    assert list(mount.rglob("*")) == []
    assert tier.staged_bytes == 0
    assert tier.staged_entries == 0
    assert tier.aborted_bytes == 4096
    assert tier.aborted_entries == 1
    assert tier.reserved_bytes == 0
    assert any("declares" in message for message in tier.errors)
