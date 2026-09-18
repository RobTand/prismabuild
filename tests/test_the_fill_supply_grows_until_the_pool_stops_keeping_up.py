"""How many movers a tier admits is measured, and it stops where the disks do (#607).

`max` over receipts cannot grow past the concurrency that produced them if the
number it maximises is one reader's own rate: a supply equal to the best single
delivery admits exactly the reader that produced it, and the first live window
ran one mover at a time for that shape of reason.  Two measurements fix it:

* `disk_pacing.mean_pool_read_mb_s` is what the *pool* delivered while a reader
  ran, whoever else was reading, so it rises the first time two movers overlap.
* a mover's receipt carries the `fill_mb_s_pool_side` its claim reserved, so a
  later cycle can ask whether the pool delivered what the ledger promised.  A
  reader that fell short while the pool delivered what it delivered is the
  measured ceiling: the supply was priced above what the disks give that many
  readers at once.

While no receipt has fallen short, the tier offers the best delivery observed
plus one ready mover's own demand and lets the next receipt decide.  So the
supply grows by measurement and stops by measurement, with no constant, and the
fold is pure -- the same history mints the same number however often it is read.
"""
from __future__ import annotations

from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
from prismabuild import pool, storage_tiers  # noqa: E402
import tier_loop  # noqa: E402

TIER = "prismabuild-stage:dl380g10"
GIB = storage_tiers.GIB
FILL = storage_tiers.FILL_KIND
KIND = f"{FILL}@{TIER}"


def _receipt(*, unix: float, delivered: float | None, sealed: int = 0,
             achieved: float | None = None, seconds: float = 100.0,
             held: float = 0.0, key: str = "0") -> dict[str, object]:
    """One mover receipt: what it reserved, what it got, what the pool gave."""

    pacing: dict[str, object] = {"held_seconds": held}
    if delivered is not None:
        pacing[storage_tiers.POOL_FILL_FIELD] = delivered
    record: dict[str, object] = {
        "action_key": key * 64, "tier_id": TIER, "complete": True,
        "seconds": seconds, "unix": unix, "disk_pacing": pacing,
        storage_tiers.MOVER_FILL_DEMAND_FIELD: sealed,
    }
    if achieved is not None:
        record["bytes_staged"] = int(achieved * 1e6 * (seconds - held))
        record["mb_per_s_file_side"] = achieved
    return record


def test_with_no_receipt_there_is_no_supply_and_no_ceiling():
    supply = storage_tiers.fill_supply_from_records([])
    assert supply["best_mb_s"] is None and supply["ceiling_mb_s"] is None
    assert supply["may_grow"] is True


def test_overlapping_movers_raise_the_best_delivery_the_fold_reports():
    """Three concurrent movers at 166 each: the pool delivered 498 in all three."""

    records = [_receipt(unix=100.0 + index, delivered=498.0, sealed=166,
                        achieved=166.0, key=str(index + 1))
               for index in range(3)]
    supply = storage_tiers.fill_supply_from_records(records)
    assert supply["best_mb_s"] == 498.0
    assert supply["ceiling_mb_s"] is None
    assert supply["may_grow"] is True


def test_a_mover_that_did_not_get_what_it_reserved_sets_the_ceiling():
    """A fourth mover, priced at 166, achieving 130 while the pool gave 522."""

    records = [_receipt(unix=100.0, delivered=498.0, sealed=166, achieved=166.0,
                        key="1"),
               _receipt(unix=200.0, delivered=522.0, sealed=166, achieved=130.0,
                        key="2")]
    supply = storage_tiers.fill_supply_from_records(records)
    assert supply["ceiling_mb_s"] == 522.0
    assert supply["ceiling_receipt"] == "2" * 64
    assert supply["may_grow"] is False


def test_a_later_window_that_beat_the_ceiling_refutes_it():
    """The pool did more than the ceiling claimed, so the ceiling was not one."""

    records = [_receipt(unix=200.0, delivered=522.0, sealed=166, achieved=130.0,
                        key="2"),
               _receipt(unix=300.0, delivered=610.0, sealed=166, achieved=166.0,
                        key="3")]
    supply = storage_tiers.fill_supply_from_records(records)
    assert supply["ceiling_mb_s"] is None
    assert supply["best_mb_s"] == 610.0


def test_the_fold_is_pure_and_order_free():
    records = [_receipt(unix=300.0, delivered=610.0, sealed=166, achieved=166.0,
                        key="3"),
               _receipt(unix=100.0, delivered=498.0, sealed=166, achieved=166.0,
                        key="1"),
               _receipt(unix=200.0, delivered=522.0, sealed=166, achieved=130.0,
                        key="2")]
    first = storage_tiers.fill_supply_from_records(records)
    second = storage_tiers.fill_supply_from_records(list(reversed(records)))
    assert first == second
    # 300 beats the 200 ceiling, so the ceiling is refuted and best is 610.
    assert first["ceiling_mb_s"] is None and first["best_mb_s"] == 610.0


def test_a_held_reader_is_not_read_as_a_pool_deficit():
    """The pacer stopped it on purpose; that shortfall is not the pool's."""

    # 100 s elapsed, 60 s held: 8300 MB in the 40 s it was reading is 207 MB/s,
    # above the 166 it reserved, so no ceiling.  Charged over the full 100 s it
    # would read as 83 MB/s and set a false one.
    record = _receipt(unix=100.0, delivered=522.0, sealed=166, seconds=100.0,
                      held=60.0, achieved=207.5, key="1")
    supply = storage_tiers.fill_supply_from_records([record])
    assert supply["ceiling_mb_s"] is None


def test_a_receipt_that_reserved_no_fill_can_never_set_a_ceiling():
    """Every receipt that shipped reserved none; none of them is evidence."""

    record = _receipt(unix=100.0, delivered=166.0, sealed=0, achieved=1.0,
                      key="1")
    supply = storage_tiers.fill_supply_from_records([record])
    assert supply["ceiling_mb_s"] is None and supply["best_mb_s"] == 166.0


def _cycle(queue, fill_records_seen=None):
    def discover(**kwargs):
        record = {"schema": storage_tiers.TIER_RECORD_SCHEMA_V1,
                  "tier_id": TIER, "host": "dl380g10", "tier": "stage",
                  "mountpoint": "/stage/prewarm", "capacity_bytes": 600 * GIB}
        record[storage_tiers.FILL_RECORD_FIELD] = storage_tiers.fill_rate_from_records(
            kwargs.get("fill_records") or ())
        return {TIER: record}

    return tier_loop.cycle(queue, host="dl380g10", source_pool="storage_pool",
                           receipts=tier_loop.ReceiptCache(),
                           discover=discover)[0]


def _ready_mover(queue, key: str, fill: int) -> None:
    queue.publish(action_key=key * 64, cas_root="/cas", worker_script="w.py",
                  checkout_root="/co", tags=["dl380g10"],
                  resources={"cpu": 4, "mem_gb": 2, KIND: fill,
                             f"stage_gib@{TIER}": 4})


def test_the_tier_offers_one_more_mover_than_the_pool_has_carried(tmp_path):
    """Through the real cycle: measured delivery plus one queued mover's demand."""

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    queue.record_move("1" * 64, _receipt(unix=100.0, delivered=166.0, sealed=166,
                                         achieved=166.0, key="1"))
    _ready_mover(queue, "a", 166)
    record = _cycle(queue)
    assert record["fill_source"] == "measured-growing"
    assert record["tokens"][FILL] == 166 + 166
    assert record["fill_probe_mb_s"] == 166


def test_a_measured_ceiling_stops_the_growth_at_what_the_disks_gave(tmp_path):
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    queue.record_move("1" * 64, _receipt(unix=100.0, delivered=498.0, sealed=166,
                                         achieved=166.0, key="1"))
    queue.record_move("2" * 64, _receipt(unix=200.0, delivered=522.0, sealed=166,
                                         achieved=130.0, key="2"))
    _ready_mover(queue, "a", 166)
    record = _cycle(queue)
    assert record["fill_source"] == "measured-ceiling"
    assert record["tokens"][FILL] == 522
    assert record["fill_ceiling_receipt"] == "2" * 64


def test_with_nothing_measured_the_probe_rule_is_unchanged(tmp_path):
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    _ready_mover(queue, "a", 210)
    record = _cycle(queue)
    assert record["fill_source"] == "probe"
    assert record["tokens"][FILL] == 210


def test_with_nothing_measured_and_nothing_asking_the_tier_offers_no_fill(tmp_path):
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    record = _cycle(queue)
    assert record["fill_source"] == "none"
    assert FILL not in record["tokens"]


@pytest.mark.parametrize("file_side,delivered,sharers,expected", [
    (229.4, 166.0, 1, 166),     # a solo cold copy: the pool bounds it
    (1477.9, 166.0, 1, 166),    # ARC-warm: the disks never produced 1478
    (120.0, 500.0, 1, 120),     # a slow copy on a fast pool: itself bounds it
    (300.0, 498.0, 3, 166),     # three at once: the pool's number is all three
    (300.0, 522.0, 4, 130),     # four at once, and the share falls with them
])
def test_a_movers_own_fill_demand_is_one_receipts_own_share(
        file_side, delivered, sharers, expected):
    """Each receipt bounds one mover twice over; the smaller bound is its answer.

    The division by ``movers_claimed_on_tier`` is the point.
    ``mean_pool_read_mb_s`` is the *pool's* delivery, so a window shared by
    three copies reports three copies' worth; reading it as one mover's rate
    would reserve the whole pool for each of them, which re-serializes movers
    through the fill token -- the same failure the cpu demand fixes, arriving
    by another resource kind.
    """

    record = {"action_key": "1" * 64, "tier_id": TIER, "seconds": 44.5,
              "mb_per_s_file_side": file_side,
              storage_tiers.MOVER_CONCURRENCY_FIELD: sharers,
              "disk_pacing": {storage_tiers.POOL_FILL_FIELD: delivered}}
    assert storage_tiers.mover_fill_demand_from_receipts(
        [record], tier_id=TIER) == expected


def test_without_all_three_measurements_a_mover_reserves_no_fill_at_all():
    """A guessed bandwidth is the habit this replaces, so there is no default."""

    only_file = {"action_key": "1" * 64, "tier_id": TIER, "seconds": 44.5,
                 "mb_per_s_file_side": 244.7,
                 storage_tiers.MOVER_CONCURRENCY_FIELD: 1, "disk_pacing": {}}
    assert storage_tiers.mover_fill_demand_from_receipts(
        [only_file], tier_id=TIER) is None
    no_count = {"action_key": "1" * 64, "tier_id": TIER, "seconds": 44.5,
                "mb_per_s_file_side": 244.7,
                "disk_pacing": {storage_tiers.POOL_FILL_FIELD: 498.0}}
    assert storage_tiers.mover_fill_demand_from_receipts(
        [no_count], tier_id=TIER) is None
    assert storage_tiers.mover_fill_demand_from_receipts(
        [], tier_id=TIER) is None
