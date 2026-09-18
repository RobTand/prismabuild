"""A tier reservation prices two different things, and only one outlives the copy.

``stage_gib`` prices occupancy: those bytes are on the device, so the mover
keeps that token from ``finish`` until an egress deletes them.
``fill_mb_s_pool_side`` prices the pool-side bandwidth the copy *draws*, and
nothing draws it once the copy stops.  Keeping both was measured on
``prismabuild-stage:dl380g10`` on 2026-09-18: 506 of 635 fill units held by
seven terminal or never-published keys, 129 free against a fresh mover's demand
of 188, so no mover could be admitted and every stage-fed consumer waited on a
lead that could not land (#636).
"""
from __future__ import annotations

from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
from prismabuild import pool, storage_tiers  # noqa: E402
import tier_loop  # noqa: E402

MOVER = "1" * 64
CONSUMER = "2" * 64
OTHER = "4" * 64
TIER = "prismabuild-stage:dl380g10"
STAGE_KIND = f"stage_gib@{TIER}"
FILL_KIND = f"{storage_tiers.FILL_KIND}@{TIER}"
MANIFEST = "9" * 64


@pytest.fixture()
def queue(tmp_path: Path) -> pool.PoolQueue:
    q = pool.PoolQueue(tmp_path / "pb-queue")
    q.ensure_layout()
    q.mint_tier_capacity(TIER, {"stage_gib": 8, storage_tiers.FILL_KIND: 200})
    return q


def _publish(q: pool.PoolQueue, key: str, resources: dict[str, int], **kw) -> None:
    q.publish(action_key=key, cas_root=q.root / "cas", checkout_root=q.root / "co",
              worker_script=q.root / "worker.py", resources=resources, **kw)


def _residency() -> dict[str, object]:
    return {
        "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
        "manifest_sha256": MANIFEST, "manifest_bytes": 1 << 40,
        "range_start_bytes": 0, "range_end_bytes": 2 * storage_tiers.GIB,
    }


def _claim_mover(q: pool.PoolQueue, key: str = MOVER) -> dict[str, object]:
    _publish(q, key, {"cpu": 1, "mem_gb": 1, STAGE_KIND: 2, FILL_KIND: 188},
             residency=_residency(), max_attempts=1, retry_safe=False)
    claimed = q.claim(capacity={"cpu": 4, "mem_gb": 8}, tags=["dl380g10"])
    assert claimed is not None and claimed["action_key"] == key
    return claimed


def test_a_mover_that_kept_its_pin_still_returns_its_bandwidth(
    queue: pool.PoolQueue,
) -> None:
    claimed = _claim_mover(queue)
    ledger = queue.tier_ledger(TIER)
    assert ledger.holder_tokens(MOVER) == {"stage_gib": 2,
                                           storage_tiers.FILL_KIND: 188}

    queue._release_reservation(MOVER, host="dl380g10", keep_tier=True)

    # The bytes are on the device, so the occupancy stays charged to this key.
    # The copy has stopped, so the rate does not.
    assert ledger.holder_tokens(MOVER) == {"stage_gib": 2}
    assert ledger.held() == {"stage_gib": 2}
    assert claimed["action_key"] == MOVER

    # And the next mover, which asks for the same 188, can now be admitted.
    _publish(queue, OTHER, {"cpu": 1, "mem_gb": 1, STAGE_KIND: 2, FILL_KIND: 188},
             residency=_residency(), max_attempts=1, retry_safe=False)
    assert queue.claim(capacity={"cpu": 4, "mem_gb": 8},
                       tags=["dl380g10"]) is not None


def test_releasing_everything_still_releases_everything(
    queue: pool.PoolQueue,
) -> None:
    """``keep_tier=False`` is unchanged: a copy that left nothing keeps nothing."""

    _claim_mover(queue)
    queue._release_reservation(MOVER, host="dl380g10", keep_tier=False)
    assert queue.tier_ledger(TIER).holder_tokens(MOVER) == {}


def test_the_cycle_reclaims_a_rate_no_live_copy_is_drawing(
    queue: pool.PoolQueue,
) -> None:
    """The janitor for an adoption, a killed mover, and history (#636).

    An adopted key holds a whole reservation although it copied no bytes, and
    it is never published, so nothing else will ever conclude it.
    """

    _claim_mover(queue)
    ledger = queue.tier_ledger(TIER)
    queue.transfer_tier_reservation(TIER, MOVER, OTHER)
    assert ledger.holder_tokens(OTHER) == {"stage_gib": 2,
                                           storage_tiers.FILL_KIND: 188}

    events = tier_loop.reclaim_idle_rates(queue)

    assert [e["holder"] for e in events] == [OTHER]
    assert events[0]["released"] == 188
    assert events[0]["rates"] == {storage_tiers.FILL_KIND: 188}
    assert events[0]["tier_id"] == TIER
    assert ledger.holder_tokens(OTHER) == {"stage_gib": 2}
    # Idempotent: a second cycle has nothing left to say.
    assert tier_loop.reclaim_idle_rates(queue) == []


def test_the_cycle_leaves_a_running_copys_rate_alone(
    queue: pool.PoolQueue,
) -> None:
    """A claimed mover is the one reader whose reservation is real."""

    _claim_mover(queue)
    assert tier_loop.reclaim_idle_rates(queue) == []
    assert queue.tier_ledger(TIER).holder_tokens(MOVER) == {
        "stage_gib": 2, storage_tiers.FILL_KIND: 188}
