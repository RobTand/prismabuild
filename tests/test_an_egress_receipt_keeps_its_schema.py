"""An egress receipt filed through ``record_move`` stays an egress receipt (#1158).

``stage_release`` builds its receipts with ``pool.POOL_EGRESS_SCHEMA_V1`` and
files them with ``PoolQueue.record_move``.  The filing stamped
``POOL_MOVE_SCHEMA_V1`` over every receipt, so the egress price (#1021), which
reads only egress receipts, found none and sealed every egress unmeasured.

Everything runs on ``tmp_path`` queues (#628).
"""
from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from prismabuild import movement_actions as ma  # noqa: E402
from prismabuild import pool  # noqa: E402
from prismabuild import storage_tiers  # noqa: E402

MIB = 1024 * 1024
EGRESS_KEY = "e" * 64
MOVER_KEY = "9" * 64
TIER = "stage:test"


def _egress_receipt(stage_root: Path) -> dict[str, object]:
    """One egress of three entries, as ``stage_release._evict_locked`` and
    ``_hold_record`` build it."""

    return {"schema": pool.POOL_EGRESS_SCHEMA_V1, "action_key": MOVER_KEY,
            "consumer_action_key": "c" * 64, "stage_root": str(stage_root),
            "reason": "egress", "complete": True, "errors": [],
            "entries_judged": 3, "entries_deleted": 3,
            "entries_already_gone": 0, "entries_shared": 0,
            "entries_deferred": 0, "bytes_deleted": 3 * MIB,
            "tokens_released": 3,
            "census_s": 4.0, "census_validate_s": 0.5, "lock_wait_s": 0.0,
            "lock_held_s": 1.0, "unlink_s": 0.03, "prune_s": 0.0,
            "unix": 100.0}


def test_an_egress_receipt_filed_through_record_move_is_priced(
        tmp_path: Path) -> None:
    queue = pool.PoolQueue(tmp_path / "queue")
    stage = tmp_path / "stage"
    path = queue.record_move(EGRESS_KEY, _egress_receipt(stage))

    on_disk = json.loads(path.read_text())
    assert on_disk["schema"] == pool.POOL_EGRESS_SCHEMA_V1
    assert on_disk["action_key"] == EGRESS_KEY
    # The by-key read ``produced_output`` finds a finished egress through.
    read = queue.move_record(EGRESS_KEY)
    assert read is not None and read["schema"] == pool.POOL_EGRESS_SCHEMA_V1

    both = queue.move_records(schemas=(pool.POOL_MOVE_SCHEMA_V1,
                                       pool.POOL_EGRESS_SCHEMA_V1))
    assert [r["schema"] for r in both] == [pool.POOL_EGRESS_SCHEMA_V1]
    price = ma.egress_price(both, stage_root=str(stage))
    assert price["basis"] == "egress" and price["receipts"] == 1
    _policy, derivation = ma.egress_progress_policy([MIB] * 3, price=price)
    assert derivation["basis"] == "egress"

    # Not a move receipt: the default read, the one every mover price takes,
    # does not return it.
    assert queue.move_records() == []


def test_a_receipt_with_no_schema_files_as_a_move(tmp_path: Path) -> None:
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.record_move("a" * 64, {"tier_id": TIER, "seconds": 1.0})
    queue.record_move("b" * 64, {"schema": pool.POOL_MOVE_SCHEMA_V1,
                                 "tier_id": TIER, "seconds": 1.0})
    assert [r["schema"] for r in queue.move_records()] == [
        pool.POOL_MOVE_SCHEMA_V1, pool.POOL_MOVE_SCHEMA_V1]


def test_a_receipt_of_another_schema_is_refused(tmp_path: Path) -> None:
    queue = pool.PoolQueue(tmp_path / "queue")
    with pytest.raises(pool.PoolContractError, match="something.else.v1"):
        queue.record_move("a" * 64, {"schema": "something.else.v1"})
    assert not queue.move_path("a" * 64).exists()


def test_a_misfiled_egress_receipt_prices_neither_an_egress_nor_a_move(
        tmp_path: Path) -> None:
    """A receipt filed before #1158 reads ``pool_move.v1`` with an egress's
    shape.  Egress pricing starts from the fix forward, so it prices no
    egress, and it has no ``seconds`` and no ``disk_pacing``, so it prices no
    mover either."""

    queue = pool.PoolQueue(tmp_path / "queue")
    stage = tmp_path / "stage"
    legacy = {**_egress_receipt(stage), "schema": pool.POOL_MOVE_SCHEMA_V1,
              "action_key": EGRESS_KEY, "tier_id": TIER}
    path = queue.move_path(EGRESS_KEY)
    path.parent.mkdir(parents=True, exist_ok=True)
    pool._write_json_atomic(path, legacy)

    records = queue.move_records(schemas=(pool.POOL_MOVE_SCHEMA_V1,
                                          pool.POOL_EGRESS_SCHEMA_V1))
    assert len(records) == 1
    assert ma.egress_price(records, stage_root=str(stage))["basis"] == "none"

    movers = queue.move_records()
    demand = storage_tiers.mover_demand_from_receipts(
        movers, tier_id=TIER, readers=4, fallback_mem_gb=2)
    assert demand["demand_source"]["receipts_read"] == 0
    assert storage_tiers.mover_fill_price(movers, tier_id=TIER)["mb_s"] is None
