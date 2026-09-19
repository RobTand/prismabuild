"""Tier demand without a residency block is refused at publish (#595).

"Derived, never typed" held only for blocks that declare a range: an item
naming ``stage_gib@...`` with no block and no manifest was accepted, and the
number in the claim record traced back to nothing.  Every tier demand the
fleet's own submitters seal travels beside the block whose manifest range
(mover) or leads (consumer) it accounts for, so the pool refuses demand
with no block rather than reserving capacity nothing can attribute.

Nothing here touches the live queue, a real pool or a real device.
"""

from __future__ import annotations

from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from prismabuild import pool  # noqa: E402

KEY_A = "a" * 64
TIER = "prismabuild-stage:dl380g10"
STAGE = f"stage_gib@{TIER}"
FILL = f"fill_mb_s_pool_side@{TIER}"


@pytest.fixture()
def queue(tmp_path: Path) -> pool.PoolQueue:
    q = pool.PoolQueue(tmp_path / "pb-queue")
    q.ensure_layout()
    return q


def _submit(q: pool.PoolQueue, key: str, resources: dict[str, int], **kw: object) -> None:
    q.publish(
        action_key=key,
        cas_root=q.root / "cas",
        checkout_root=q.root / "co",
        worker_script=q.root / "worker.py",
        resources=resources,
        **kw,
    )


def _range_block(*, range_bytes: int = 1) -> dict[str, object]:
    return {
        "schema": pool.RESIDENCY_SCHEMA_V1,
        "manifest_sha256": "0" * 64,
        "manifest_bytes": range_bytes,
        "tier_id": TIER,
        "range_start_bytes": 0,
        "range_end_bytes": range_bytes,
    }


def _leads_block(*, leads: list[str]) -> dict[str, object]:
    return {
        "schema": pool.RESIDENCY_SCHEMA_V1,
        "manifest_sha256": "0" * 64,
        "manifest_bytes": 4096,
        "leads": leads,
    }


def test_publish_refuses_tier_demand_with_no_residency_block(
    queue: pool.PoolQueue,
) -> None:
    """The issue's example: a five-figure ask with no block and no manifest."""

    with pytest.raises(pool.PoolContractError, match="residency block"):
        _submit(queue, KEY_A, {"cpu": 1, f"arc_gib@arc:dl380g10": 10000})
    with pytest.raises(pool.PoolContractError, match="residency block"):
        _submit(queue, KEY_A, {"cpu": 1, STAGE: 2, FILL: 5})
    assert not queue.item_path(pool.READY, KEY_A).exists()


def test_a_range_block_with_its_floor_still_admits(queue: pool.PoolQueue) -> None:
    """The refusal is about the missing block, not about tier demand itself."""

    queue.mint_tier_capacity(TIER, {"stage_gib": 2})
    _submit(queue, KEY_A, {"cpu": 1, STAGE: 2}, residency=_range_block(range_bytes=2))
    claimed = queue.claim(owner="mover", capacity={"cpu": 1})
    assert claimed is not None and claimed["action_key"] == KEY_A
    assert claimed["tier_reservations"] == {TIER: {"stage_gib": 2}}


def test_a_leads_block_carries_tier_demand_past_publish(
    queue: pool.PoolQueue,
) -> None:
    """A consumer's block names leads, not a range: publish accepts it.

    The claim then waits on the lead like any consumer; the point here is
    only that the publish gate is the missing block, not tier demand itself.
    """

    _submit(queue, KEY_A, {"cpu": 1, STAGE: 1},
            residency=_leads_block(leads=[KEY_A]))
    assert queue.item_path(pool.READY, KEY_A).exists()


def test_a_probe_shaped_mover_with_its_range_block_still_admits(
    queue: pool.PoolQueue,
) -> None:
    """The #659 contract: the probe rule reads demand, never the block.

    ``test_the_probe_is_sized_for_the_oldest_ready_mover`` publishes
    fill-plus-stage movers; production movers always seal the range they make
    resident, so probe-scenario movers seal one too and the publish gate
    admits them.  This is the refused shape above with the block attached.
    """

    _submit(queue, KEY_A, {"cpu": 1, STAGE: 2, FILL: 5},
            residency=_range_block(range_bytes=2))
    assert queue.item_path(pool.READY, KEY_A).exists()


def test_host_demand_without_a_block_is_unchanged(queue: pool.PoolQueue) -> None:
    """Ordinary rows never carried tier demand and still need no block."""

    _submit(queue, KEY_A, {"cpu": 1, "mem_gb": 1})
    claimed = queue.claim(owner="worker", capacity={"cpu": 1, "mem_gb": 1})
    assert claimed is not None and claimed["action_key"] == KEY_A
