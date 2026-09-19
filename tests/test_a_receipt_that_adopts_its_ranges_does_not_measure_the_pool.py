"""A receipt that adopts its ranges does not measure the pool (#654).

Adoption is the fleet's own optimization: a resubmitted campaign's phases are
adopted from the stage instead of re-read from the pool, so the mover barely
touches the pool.  Its pacing's ``mean_pool_read_mb_s`` measures the few
device reads it made -- live on 2026-09-19 on ``prismabuild-stage:dl380g10``,
1.5-6.5 MB/s -- while its ``bytes_staged`` shows GiB served from the stage;
the honest receipt beside it, ``b14ebfcf``, read 1.81x its staged bytes at
311.7 MB/s.  Read as a pool measurement, an adoption window that fell short
of its sealed fill mints a ceiling no later receipt of the same shape can
refute (its delivered is tiny too) and no honest mover can fit under
(``never_fits_tier_capacity``), so no honest receipt can ever land either:
deadlock by construction, and campaign 1e44a4d1d367 sat in it.

The fold's answer is that such a receipt is not a pool measurement at all.
A receipt whose pool reads are less than ``POOL_MEASUREMENT_MIN_SHARE`` of the
bytes it staged is skipped whole -- it sets no ceiling, refutes nothing,
raises no best -- and a receipt missing either counter keeps the bare
treatment #607 gave it, so honest receipts fold exactly as they did before.
"""
from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import storage_tiers  # noqa: E402

TIER = "prismabuild-stage:dl380g10"

#: The two live shapes of 2026-09-19, as they lie in ``pb-queue/movers/``.
#: ``staged_mb_s`` is bytes_staged over reading seconds, the number
#: ``_fell_short`` compares against the sealed fill.
ADOPTION_SHORT = {  # 2cdd1396: 10.9 GB staged, 0.36 GB read off the pool
    "unix": 1789775816.534334, "seconds": 60.644,
    "bytes_staged": 10_895_323_277, "pool_read_bytes": 390_627_328,
    "delivered": 6.5, "sealed": 292, "staged_mb_s": 225.3,
}
ADOPTION_KEPT_UP = {  # 4bbaa6aa: 3.3 GB staged, 0.017 GB read off the pool
    "unix": 1789775838.22257, "seconds": 7.249,
    "bytes_staged": 3_297_835_634, "pool_read_bytes": 17_379_328,
    "delivered": 2.5, "sealed": 292, "staged_mb_s": 471.2,
}
HONEST_SHORT = {  # b14ebfcf: 87.8 GB staged, 159.1 GB read off the pool
    "unix": 1789770767.459642, "seconds": 521.009,
    "bytes_staged": 87_768_029_391, "pool_read_bytes": 159_090_130_944,
    "delivered": 311.7, "sealed": 188, "staged_mb_s": 168.5,
}


def _receipt(shape: dict[str, object], *, key: str,
             delivered: float | None = None,
             pool_read_bytes: int | None = None,
             with_pool_rate: bool = True,
             with_pool_read_bytes: bool = True) -> dict[str, object]:
    """One mover receipt in a live shape; either counter may be struck out."""

    pacing: dict[str, object] = {"held_seconds": 0.252}
    if with_pool_rate:
        pacing[storage_tiers.POOL_FILL_FIELD] = (
            shape["delivered"] if delivered is None else delivered)
    if with_pool_read_bytes:
        pacing["pool_read_bytes"] = (
            shape["pool_read_bytes"] if pool_read_bytes is None
            else pool_read_bytes)
    return {
        "action_key": key * 64, "tier_id": TIER, "complete": True,
        "unix": shape["unix"], "seconds": shape["seconds"],
        "bytes_staged": shape["bytes_staged"],
        "mb_per_s_file_side": shape["staged_mb_s"],
        storage_tiers.MOVER_FILL_DEMAND_FIELD: shape["sealed"],
        "disk_pacing": pacing,
    }


def test_a_history_of_adoption_receipts_mints_no_ceiling_and_no_best():
    """One that fell short and one that kept up: neither measured the pool."""

    records = [_receipt(ADOPTION_SHORT, key="a"),
               _receipt(ADOPTION_KEPT_UP, key="b")]
    supply = storage_tiers.fill_supply_from_records(records)
    assert supply == {"ceiling_mb_s": None, "ceiling_receipt": None,
                      "best_mb_s": None, "may_grow": True}


def test_an_adoption_receipt_after_an_honest_shortfall_leaves_the_honest_ceiling():
    """b14ebfcf then 2cdd1396, tonight's own order: the 311.7 ceiling stands.

    Before the fix the adoption receipt re-sealed the ceiling at 6.5, the tier
    minted 6 MB/s, and every mover was refused ``never_fits_tier_capacity``.
    """

    records = [_receipt(HONEST_SHORT, key="b"),
               _receipt(ADOPTION_SHORT, key="a")]
    supply = storage_tiers.fill_supply_from_records(records)
    assert supply == storage_tiers.fill_supply_from_records(records[:1])
    assert supply["ceiling_mb_s"] == 311.7
    assert supply["ceiling_receipt"] == "b" * 64
    assert supply["best_mb_s"] is None
    assert supply["may_grow"] is False


def test_an_adoption_receipt_whose_pool_rate_tops_the_ceiling_still_refutes_nothing():
    """A hot pool under other readers reads 610 in an adoption window; the
    window still staged its bytes from the stage, so it says nothing about
    what the pool gives a mover that actually reads it."""

    records = [_receipt(HONEST_SHORT, key="b"),
               _receipt(ADOPTION_SHORT, key="a", delivered=610.0)]
    supply = storage_tiers.fill_supply_from_records(records)
    assert supply["ceiling_mb_s"] == 311.7
    assert supply["ceiling_receipt"] == "b" * 64
    assert supply["may_grow"] is False


def test_an_honest_receipt_still_sets_the_ceiling_exactly_as_it_did():
    """The b14ebfcf shape: pool reads 1.81x staged, 168.5 drawn under 188."""

    supply = storage_tiers.fill_supply_from_records(
        [_receipt(HONEST_SHORT, key="b")])
    assert supply == {"ceiling_mb_s": 311.7, "ceiling_receipt": "b" * 64,
                      "best_mb_s": None, "may_grow": False}


def test_a_later_honest_delivery_still_refutes_the_ceiling():
    """An honest 610 over an honest 522 clears it, exactly as in #607."""

    honest = dict(HONEST_SHORT, delivered=522.0, sealed=166,
                  staged_mb_s=130.0, unix=200.0)
    later = dict(HONEST_SHORT, delivered=610.0, sealed=166,
                 staged_mb_s=166.0, unix=300.0)
    supply = storage_tiers.fill_supply_from_records(
        [_receipt(honest, key="2"), _receipt(later, key="3")])
    assert supply["ceiling_mb_s"] is None
    assert supply["ceiling_receipt"] is None
    assert supply["best_mb_s"] == 610.0
    assert supply["may_grow"] is True


def test_a_receipt_without_a_pool_rate_still_says_nothing_at_all():
    """No ``mean_pool_read_mb_s``: skipped by the ``_delivered`` path, and it
    disturbs neither an honest ceiling nor an empty fold."""

    records = [_receipt(HONEST_SHORT, key="b"),
               _receipt(ADOPTION_SHORT, key="a", with_pool_rate=False)]
    supply = storage_tiers.fill_supply_from_records(records)
    assert supply["ceiling_mb_s"] == 311.7
    assert supply == storage_tiers.fill_supply_from_records([records[0]])
    bare = storage_tiers.fill_supply_from_records(
        [_receipt(ADOPTION_SHORT, key="a", with_pool_rate=False)])
    assert bare == {"ceiling_mb_s": None, "ceiling_receipt": None,
                    "best_mb_s": None, "may_grow": True}


def test_a_receipt_without_pool_read_bytes_keeps_its_bare_treatment():
    """Every #607 receipt carries no ``pool_read_bytes``; they still fold."""

    # The ADOPTION_SHORT shape without the counter fell short of 292 while
    # delivering 6.5, and before this change it sealed a 6.5 ceiling.
    record = _receipt(ADOPTION_SHORT, key="a", with_pool_read_bytes=False)
    supply = storage_tiers.fill_supply_from_records([record])
    assert supply["ceiling_mb_s"] == 6.5
    assert supply["ceiling_receipt"] == "a" * 64
    assert supply["may_grow"] is False
