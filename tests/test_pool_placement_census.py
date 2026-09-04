"""How many boxes can run each waiting item -- the number the queue never had.

A queue that reports only ``ready`` cannot tell "queued behind one busy box"
from "waiting its turn among three".  Through 2026-09-03/04 that difference
was the whole problem: an action's ``checkout_root`` is a box-local worktree
(``/home/rob/tmp/ts101``), which pins it to the submitting box, and the pin is
a silent consequence of a path.  Measured on the live queue on 2026-09-04, 129
of 394 items carried a hostname tag -- 114 of them ``sparky`` -- while
``sparklina`` held zero tokens with everything free.

These tests pin the metric and the one matcher it shares with placement.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path
import socket
import sys
from unittest import mock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from prismabuild import pool  # noqa: E402

WORKER_LOOP = Path(__file__).resolve().parents[1] / "tools" / "fleet" / "worker_loop.py"

GB10 = {"gpu": 2, "mem_gb": 48, "cpu": 10}
X86 = {"gpu": 0, "mem_gb": 60, "cpu": 80}


def _fleet(tmp_path: Path) -> pool.PoolQueue:
    """The live fleet's shape, from ``tools/fleet/fleet_boxes.json``."""

    queue = pool.PoolQueue(tmp_path / "q")
    queue.announce(host="sparky", tags=["gb10", "sparky"], has_gpu=True,
                   capacity=GB10)
    queue.announce(host="gx10-6b77", tags=["gb10", "gx10-6b77", "sparklina"],
                   has_gpu=True, capacity={"gpu": 1, "mem_gb": 40, "cpu": 10})
    queue.announce(host="dl380g10", tags=["cpu", "dl380g10", "x86"],
                   has_gpu=False, capacity=X86)
    return queue


def _publish(queue: pool.PoolQueue, seed: str, **kw) -> None:
    queue.publish(
        action_key=seed * 64, cas_root=queue.root / "cas",
        checkout_root="/home/rob/tmp/ts101",
        worker_script="/mnt/shared/prismabuild-fleet/repo/tools/prismabuild_worker.py",
        **kw,
    )


def test_placeable_hosts_names_every_box_that_fits(tmp_path: Path) -> None:
    queue = _fleet(tmp_path)
    cpu_work = {"tags": [], "needs_gpu": False, "resources": {"cpu": 1, "mem_gb": 4}}

    assert queue.placeable_hosts(cpu_work) == ["dl380g10", "gx10-6b77", "sparky"]
    assert queue.placeable_hosts({**cpu_work, "tags": ["sparky"]}) == ["sparky"]
    assert queue.placeable_hosts({**cpu_work, "needs_gpu": True}) == [
        "gx10-6b77", "sparky"]
    assert queue.placeable_hosts(
        {**cpu_work, "resources": {"gpu": 2, "mem_gb": 16}}) == ["sparky"]


def test_the_width_and_the_verdict_come_from_one_matcher(tmp_path: Path) -> None:
    """``placeable`` must never disagree with ``placeable_hosts``.

    Two copies of the matching rule is how "can this run" and "where can this
    run" end up answering different questions about one item.
    """

    queue = _fleet(tmp_path)
    for tags in ([], ["gb10"], ["sparky"], ["x86"], ["nosuchbox"]):
        for demand in ({"cpu": 1}, {"gpu": 1}, {"gpu": 9}, {"mem_gb": 4096}):
            item = {"tags": tags, "needs_gpu": False, "resources": demand}
            assert queue.placeable(item) == bool(queue.placeable_hosts(item)), (
                tags, demand)


def test_width_is_unknown_before_any_worker_announces(tmp_path: Path) -> None:
    """Unknown stays unknown -- the rule ``placeable`` already follows."""

    queue = pool.PoolQueue(tmp_path / "q")
    item = {"tags": [], "needs_gpu": False, "resources": {"cpu": 1}}

    assert queue.placeable_hosts(item) is None
    assert queue.placeable(item) is None
    census = queue.placement_census()
    assert census["known"] is False
    assert "unknown" in pool.describe_placement_census(census)


def test_the_census_counts_items_placeable_on_exactly_one_box(
    tmp_path: Path,
) -> None:
    """The live shape on 2026-09-04: everything waiting, all of it on one box."""

    queue = _fleet(tmp_path)
    _publish(queue, "a", tags=["sparky"], resources={"cpu": 1, "mem_gb": 4})
    _publish(queue, "b", tags=["sparky"], resources={"cpu": 2, "mem_gb": 8})
    _publish(queue, "c", tags=["gx10-6b77"], resources={"cpu": 1, "mem_gb": 4})
    _publish(queue, "d", tags=["gb10"], resources={"cpu": 1, "mem_gb": 4})
    _publish(queue, "e", tags=["dl380g10"], resources={"mem_gb": 4096})

    census = queue.placement_census()

    assert census["ready"] == 5
    assert census["one_box"] == 3
    assert census["pinned_to"] == {"gx10-6b77": 1, "sparky": 2}
    assert census["wide"] == 1               # the gb10 item: two boxes offer it
    assert census["unplaceable"] == 1        # 4 TB of memory, on no box
    line = pool.describe_placement_census(census)
    assert "3 on exactly one box (gx10-6b77 1, sparky 2)" in line
    assert "1 on more than one" in line and "1 on none" in line


def test_a_nameless_offer_is_reported_rather_than_dropped(tmp_path: Path) -> None:
    """Dropping it would make the width disagree with the verdict."""

    queue = pool.PoolQueue(tmp_path / "q")
    queue.announce(host="", tags=[], has_gpu=False, capacity={"cpu": 4})
    item = {"tags": [], "needs_gpu": False, "resources": {"cpu": 1}}

    assert queue.placeable(item) is True
    assert queue.placeable_hosts(item) == ["?"]


def test_an_idle_worker_says_how_much_of_the_queue_is_one_box_wide(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """The metric has to reach the log of the box that is paying for it.

    A loop that finds nothing to do is the fleet's width being spent, and
    ``ready 10`` alone does not say whether the other boxes could have helped.
    """

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.announce(host="otherbox", tags=["otherbox"], has_gpu=False,
                   capacity={"cpu": 8, "mem_gb": 16})
    _publish(queue, "a", tags=["otherbox"], resources={"cpu": 1, "mem_gb": 4})

    spec = importlib.util.spec_from_file_location("wl_census", WORKER_LOOP)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    with mock.patch.object(module, "SH", tmp_path), \
         mock.patch.object(module.cpu_topology, "pin_to_preferred", return_value=None), \
         mock.patch.object(module, "published_commit", return_value="deadbeef"), \
         mock.patch.object(sys, "argv", ["worker_loop.py", "--once", "--gpu-slots",
                                         "0", "--class", "x86", "--all-cores"]):
        assert module.main() == 0

    out = capsys.readouterr().out
    host = socket.gethostname()
    assert f"[{host}] idle; ready 1, 1 on exactly one box (otherbox 1)" in out, out
    assert "nothing admissible" in out and "on exactly one box (otherbox 1)" in out
