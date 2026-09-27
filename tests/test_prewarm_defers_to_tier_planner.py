"""#1247: the ARC loop defers manifest rows to the tier planner.

On a box whose ram tier is announced, the tier role's planner owns manifest
rows, and the ARC loop passes them over by name.  With no ram tier, a full
ARC (``headroom_effective`` zero) is skipped too: a warm there evicts a byte
for every byte it reads -- 309.9 s of pacer holds warmed 21 MB of a 63.66 GB
manifest on 2026-09-27.
"""
from __future__ import annotations

from pathlib import Path
import sys

import prismabuild.pool as pool

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import prewarm_loop  # noqa: E402


def _queue_at(tmp_path: Path) -> pool.PoolQueue:
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    return queue


def _announce_ram(queue: pool.PoolQueue, *, retired: bool = False) -> None:
    directory = queue.root / "tiers"
    directory.mkdir(parents=True, exist_ok=True)
    body = {"schema": "prismabuild.storage_tier.v1", "tier": "ram",
            "tier_id": "ram:dl380g10", "host": "dl380g10",
            "mountpoint": "/ram/prewarm"}
    if retired:
        body["retired"] = True
    (directory / "ram:dl380g10.json").write_text(__import__("json")
                                                 .dumps(body))


def _room(headroom_effective: int) -> dict:
    return {"arc_size": 1, "arc_c": 1, "arc_c_max": 2,
            "headroom_nominal": 1,
            "headroom_effective": headroom_effective,
            "capacity_budget": 1}


def test_a_live_ram_tier_takes_manifest_rows_off_the_arc_loop(tmp_path):
    queue = _queue_at(tmp_path)
    _announce_ram(queue)

    assert prewarm_loop.ram_tier_announced(queue) is True
    assert prewarm_loop.manifest_row_skip_reason(_room(10**9), True) == \
        "ram-tier-planner-owns"


def test_a_retired_ram_tier_announces_nothing(tmp_path):
    queue = _queue_at(tmp_path)
    _announce_ram(queue, retired=True)

    assert prewarm_loop.ram_tier_announced(queue) is False
    assert prewarm_loop.manifest_row_skip_reason(_room(0), False) == \
        "headroom_effective zero"


def test_a_full_arc_is_skipped_even_without_a_ram_tier():
    assert prewarm_loop.manifest_row_skip_reason(_room(0), False) == \
        "headroom_effective zero"


def test_room_left_means_warm():
    assert prewarm_loop.manifest_row_skip_reason(_room(10**9), False) is None


def test_an_unreadable_tiers_directory_answers_no_ram_tier(tmp_path):
    queue = _queue_at(tmp_path)
    import shutil
    shutil.rmtree(queue.root / "tiers")
    (queue.root / "tiers").symlink_to(str(tmp_path / "nowhere"))

    assert prewarm_loop.ram_tier_announced(queue) is False
