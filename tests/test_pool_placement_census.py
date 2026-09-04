"""How many boxes can run each waiting item -- the number the queue never had.

A queue that reports only ``ready`` cannot tell "queued behind one busy box"
from "waiting its turn among three".  Through 2026-09-03/04 that difference
was the whole problem: an action's ``checkout_root`` is a box-local worktree
(``/home/rob/tmp/ts101``), which pins it to the submitting box, and the pin is
a silent consequence of a path.  Measured on the live queue on 2026-09-04,
131 of 391 items carried a hostname tag, 129 of them by that path -- 114
pinning ``sparky`` -- while ``sparklina`` held zero tokens with everything
free.

These tests pin the metric and the one matcher it shares with placement.
"""

from __future__ import annotations

import importlib.util
import json
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
    """The live shape on 2026-09-04: everything waiting, all of it on one box.

    Every item here carries a ``/home/rob/tmp/ts101`` checkout, which is the
    shape the issue is about, so the ``gb10`` item counts as one box wide
    even though two boxes offer that tag: two may claim it, one can run it.
    """

    queue = _fleet(tmp_path)
    with mock.patch.object(pool.socket, "gethostname", return_value="sparky"):
        _publish(queue, "a", tags=["sparky"], resources={"cpu": 1, "mem_gb": 4})
        _publish(queue, "b", tags=["sparky"], resources={"cpu": 2, "mem_gb": 8})
        _publish(queue, "c", tags=["gx10-6b77"], resources={"cpu": 1, "mem_gb": 4})
        _publish(queue, "d", tags=["gb10"], resources={"cpu": 1, "mem_gb": 4})
        _publish(queue, "e", tags=["dl380g10"], resources={"mem_gb": 4096})

    census = queue.placement_census()

    assert census["ready"] == 5
    assert census["one_box"] == 4
    assert census["one_box_by_path"] == 4
    # ``d`` is attributed to sparky by ``published_by``: the tags allow two
    # boxes, and the tree is on the one that submitted it.
    assert census["pinned_to"] == {"gx10-6b77": 1, "sparky": 3}
    assert census["wide"] == 0
    assert census["unplaceable"] == 1        # 4 TB of memory, on no box
    assert census["unreadable"] == 0
    line = pool.describe_placement_census(census)
    assert "4 on exactly one box (gx10-6b77 1, sparky 3)" in line
    assert "4 by a box-local checkout" in line
    assert "0 on more than one" in line and "1 on none" in line
    assert "unreadable" not in line          # a zero clause teaches skipping


def test_a_capacity_kind_the_offer_omits_is_not_a_refusal(tmp_path: Path) -> None:
    """A publish makes two generations of offer coexist, and one omits a kind.

    ``capacity`` gained ``cpu`` on 2026-09-04.  The offer file is one
    last-writer-wins record per host, and loops of both generations write it,
    so sparky's live offer alternated between ``{"gpu": 2, "mem_gb": 48}`` and
    ``{"cpu": 10, "gpu": 2, "mem_gb": 48}`` -- 32 and 28 of 60 samples taken a
    second apart.  Read as zero, the older record made every action carrying
    the new ``cpu=1`` default unplaceable on a box that plainly runs it: 17 of
    60 identical queries answered "no live worker can run this action", which
    ``pbrun`` turns into a refused submission.
    """

    queue = pool.PoolQueue(tmp_path / "q")
    queue.announce(host="sparky", tags=["gb10", "sparky"], has_gpu=True,
                   capacity={"gpu": 2, "mem_gb": 48})     # the older generation

    item = {"tags": ["sparky"], "needs_gpu": False,
            "resources": {"cpu": 1, "mem_gb": 4}}

    assert queue.placeable_hosts(item) == ["sparky"]
    assert queue.placeable(item) is True


def test_a_capacity_kind_the_offer_states_too_small_still_refuses(
    tmp_path: Path,
) -> None:
    """Silence is unknown; a stated number is a fact, and it still binds."""

    queue = pool.PoolQueue(tmp_path / "q")
    queue.announce(host="sparky", tags=["sparky"], has_gpu=True,
                   capacity={"gpu": 2, "mem_gb": 48, "cpu": 10})

    assert queue.placeable_hosts(
        {"tags": [], "needs_gpu": False, "resources": {"cpu": 24}}) == []
    assert queue.placeable_hosts(
        {"tags": [], "needs_gpu": False, "resources": {"cpu": 10}}) == ["sparky"]


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
         mock.patch.object(module, "loaded_runtime_commit", return_value="deadbeef"), \
         mock.patch.object(module, "published_commit", return_value="deadbeef"), \
         mock.patch.object(sys, "argv", ["worker_loop.py", "--once", "--gpu-slots",
                                         "0", "--class", "x86", "--all-cores"]):
        assert module.main() == 0

    out = capsys.readouterr().out
    host = socket.gethostname()
    assert f"[{host}] idle; ready 1, 1 on exactly one box (otherbox 1)" in out, out
    assert "nothing admissible" in out and "on exactly one box (otherbox 1)" in out


def test_a_box_local_checkout_caps_the_width_at_one_box(tmp_path: Path) -> None:
    """The census asked which boxes match the TAGS, never which can see the TREE.

    An action tagged ``--tag gb10`` over a ``/home/rob/tmp/ts101`` worktree
    matches two boxes and can run on one, so it counted as ``wide`` -- the
    metric under-reporting the very pin it exists to report.  A
    ``checkout_root`` outside ``/mnt/shared`` exists on exactly one box; that
    is a fact about the path, and it caps the width at one however many boxes
    the tags match.
    """

    queue = _fleet(tmp_path)
    with mock.patch.object(pool.socket, "gethostname", return_value="sparky"):
        _publish(queue, "d", tags=["gb10"], resources={"cpu": 1, "mem_gb": 4})

    census = queue.placement_census()

    assert census["one_box"] == 1 and census["wide"] == 0
    assert census["pinned_to"] == {"sparky": 1}
    assert census["one_box_by_path"] == 1
    assert "1 by a box-local checkout" in pool.describe_placement_census(census)


def test_a_shared_checkout_is_still_counted_as_wide(tmp_path: Path) -> None:
    """The cap is the path, not the tags: the same tags on a shared tree are wide."""

    queue = _fleet(tmp_path)
    queue.publish(
        action_key="d" * 64, cas_root=queue.root / "cas",
        checkout_root="/mnt/shared/prismabuild-fleet/checkout",
        worker_script="/mnt/shared/prismabuild-fleet/repo/tools/prismabuild_worker.py",
        tags=["gb10"], resources={"cpu": 1, "mem_gb": 4},
    )

    census = queue.placement_census()

    assert census["wide"] == 1 and census["one_box"] == 0
    assert census["one_box_by_path"] == 0
def test_one_malformed_ready_item_does_not_take_the_worker_with_it(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """``3711b29``'s contract, re-opened by a diagnostic outside its try/except.

    ``claim`` skips an item tagged for another box at ``_placement_matches``,
    before ``demand_of`` is ever reached, so a ready record with a non-Mapping
    ``resources`` was harmless.  The census reads EVERY ready item, and its
    call sat outside the handler that wraps ``serve_once`` -- so one corrupted
    or out-of-band write became a raw traceback and an immediate exit on a box
    that was otherwise fine, once every supervisor cycle, against an item that
    is still there.
    """

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.announce(host="otherbox", tags=["otherbox"], has_gpu=False,
                   capacity={"cpu": 8, "mem_gb": 16})
    queue.ensure_layout()
    (queue.dir("ready") / f"{'f' * 64}.json").write_text(json.dumps({
        "schema": pool.POOL_ITEM_SCHEMA_V1, "action_key": "f" * 64,
        "cas_root": str(queue.root / "cas"), "checkout_root": "/home/rob/tmp/ts101",
        "worker_script": "/mnt/shared/prismabuild-fleet/repo/tools/x.py",
        "tags": ["otherbox"], "needs_gpu": False, "priority": 0,
        "resources": "all of it",              # not a Mapping: out-of-band write
        "attempts": 0, "max_attempts": 3, "published_unix": 0.0,
        "published_by": "otherbox",
    }))

    census = queue.placement_census()
    assert census["unreadable"] == 1
    assert "1 unreadable" in pool.describe_placement_census(census)

    spec = importlib.util.spec_from_file_location("wl_bad_item", WORKER_LOOP)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    with mock.patch.object(module, "SH", tmp_path), \
         mock.patch.object(module.cpu_topology, "pin_to_preferred", return_value=None), \
         mock.patch.object(module, "loaded_runtime_commit", return_value="deadbeef"), \
         mock.patch.object(module, "published_commit", return_value="deadbeef"), \
         mock.patch.object(sys, "argv", ["worker_loop.py", "--once", "--gpu-slots",
                                         "0", "--class", "x86", "--all-cores"]):
        assert module.main() == 0              # the item is bad; the box is not
