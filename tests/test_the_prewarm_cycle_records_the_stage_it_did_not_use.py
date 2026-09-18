"""Every cycle says which of the four states the stage was in, staging or not.

pb#585 is the shape this avoids: the prewarm pacer's *correct* decision not to
hold was written nowhere, so hours of diagnosis went into establishing that
nothing had gone wrong.  A tier that is absent, unreadable or full is the same
kind of fact.  All four states are therefore recorded on every cycle, including
a cycle with nothing in the queue to stage, and each carries the reason in
words rather than leaving a reader to infer it from a missing field.

``absent`` is the state on every box today and on the file server until the
stage pool exists, so it is the one the tests exercise hardest.
"""
from __future__ import annotations

from pathlib import Path
import subprocess
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from prewarm_fixture import Fleet, StagePool  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import prewarm_loop  # noqa: E402


def stage_args(fleet: Fleet, **overrides):
    return fleet.args(stage=True, stage_free_floor_bytes=0, **overrides)


def test_an_empty_queue_still_records_the_tier(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Nothing ready, nothing claimed, nothing staged -- and still a record.

    This is the criterion stated directly: the cycle that stages nothing is
    the cycle whose silence cost the diagnosis last time.
    """

    fleet = Fleet(tmp_path)
    StagePool(tmp_path).install(monkeypatch)

    event = fleet.cycle(stage_args(fleet))

    assert event["ready"] == 0
    assert event["warmed"] == []
    assert event["stage"]["state"] == "present"
    assert event["stage"]["staged_bytes"] == 0
    assert event["stage"]["reason"]


def test_no_stage_pool_is_absent_and_says_so(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The state on every box but the file server, and on it until Rob's
    ``zpool create``.  A loop that reported a missing tier as a failure would
    make the ordinary case look like a defect."""

    fleet = Fleet(tmp_path)
    key = fleet.action("row", [fleet.file("row.pt", 8192)])
    monkeypatch.setattr(prewarm_loop, "run_tool", lambda argv: "")

    event = fleet.cycle(stage_args(fleet))

    assert event["stage"]["state"] == "absent"
    assert "prismabuild-stage" in event["stage"]["reason"]
    assert event["stage"]["pool"] == ""
    # The warm is untouched: a missing second destination never costs the
    # first one a byte.
    assert [w["action_key"] for w in event["warmed"]] == [key]
    assert fleet.queue.prewarm(key)["status"] == "complete"
    assert fleet.queue.prewarm(key)["stage"]["state"] == "absent"


def test_a_pool_of_another_name_is_not_taken_for_a_stage(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The prefix is the declaration.  ``nvme0n1p1`` on the file server still
    carries a stale ``zfs_member`` signature, and a rule that took idle
    devices would have seized it before anybody approved it."""

    fleet = Fleet(tmp_path)
    monkeypatch.setattr(
        prewarm_loop, "run_tool",
        lambda argv: "storage_pool\t100\t50\t50\tONLINE\nrpool\t10\t1\t9\tONLINE\n")

    event = fleet.cycle(stage_args(fleet))
    assert event["stage"]["state"] == "absent"


@pytest.mark.parametrize("failure", [
    OSError("no zpool on this host"),
    subprocess.CalledProcessError(1, ["zpool"]),
])
def test_a_host_that_cannot_be_asked_is_unreadable(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure) -> None:
    fleet = Fleet(tmp_path)

    def raise_it(argv):
        raise failure

    monkeypatch.setattr(prewarm_loop, "run_tool", raise_it)
    event = fleet.cycle(stage_args(fleet))

    assert event["stage"]["state"] == "unreadable"
    assert "zpool" in event["stage"]["reason"]


def test_a_pool_with_no_mounted_directory_is_unreadable(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """``mountpoint=legacy`` or ``none`` leaves nothing to write into, which
    is a tier that exists and cannot be used -- a different fact from one that
    is not there."""

    stage = StagePool(tmp_path, mounted=False)
    stage.mountpoint = "legacy"
    stage.install(monkeypatch)
    fleet = Fleet(tmp_path)

    event = fleet.cycle(stage_args(fleet))
    assert event["stage"]["state"] == "unreadable"


def test_a_faulted_pool_is_unreadable(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    StagePool(tmp_path, health="FAULTED").install(monkeypatch)
    fleet = Fleet(tmp_path)

    event = fleet.cycle(stage_args(fleet))
    assert event["stage"]["state"] == "unreadable"
    assert "FAULTED" in event["stage"]["reason"]


def test_a_pool_with_nothing_above_the_floor_is_full(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Capacity is the pool's own ``free`` less the floor it keeps, read every
    cycle rather than configured: add a device and the next cycle offers more,
    fill it and the next cycle says ``full``."""

    StagePool(tmp_path, size=1 << 30, free=1 << 20).install(monkeypatch)
    fleet = Fleet(tmp_path)
    key = fleet.action("row", [fleet.file("row.pt", 8192)])

    event = fleet.cycle(fleet.args(stage=True,
                                   stage_free_floor_bytes=1 << 30))
    assert event["stage"]["state"] == "full"
    assert event["stage"]["budget_bytes"] == 0
    assert event["stage"]["staged_bytes"] == 0
    assert fleet.queue.prewarm(key)["status"] == "complete"


def test_the_pool_root_is_used_when_the_dataset_is_absent(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The pool ships with a ``prewarm`` dataset; a pool without one is still
    a usable tier, written at its own mountpoint."""

    StagePool(tmp_path, has_dataset=False).install(monkeypatch)
    fleet = Fleet(tmp_path)

    event = fleet.cycle(stage_args(fleet))
    assert event["stage"]["state"] == "present"
    assert event["stage"]["dataset"] == "prismabuild-stage"


def test_the_dataset_is_preferred_when_the_pool_carries_one(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    StagePool(tmp_path).install(monkeypatch)
    fleet = Fleet(tmp_path)

    event = fleet.cycle(stage_args(fleet))
    assert event["stage"]["dataset"] == "prismabuild-stage/prewarm"
