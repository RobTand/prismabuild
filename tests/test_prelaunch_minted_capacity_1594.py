"""The prelaunch capacity checks compare against the minted supply (#1594).

A stage tier mints ``writable room + landed copies``.  The tier loop announces
the writable room as ``capacity_bytes`` and the minted supply as ``tokens``.
The first live submissions of the prelaunch feature (2026-10-07) were refused
against the writable room: 9 GiB, while the tier had minted 178 GiB and the
prefixes needed 88 and 102 GiB.  The earlier tests restated the comparison
instead of calling it, and used an empty tier where the two numbers are equal.

These tests call the production functions with the live record's shape.
"""
from __future__ import annotations

from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools" / "fleet"))
from prismabuild import storage_tiers  # noqa: E402
import pbgang  # noqa: E402
import pbrun  # noqa: E402
from test_prelaunch_plan_1594 import GIB, TIER, _member  # noqa: E402

#: The announced record of prismabuild-stage:dl380g10 at 2026-10-07 18:36Z,
#: reduced to the fields that matter here.
LIVE = {
    "tier": "stage", "tier_id": TIER,
    "capacity_bytes": 9798529024,
    "capacity_source": "zfs available",
    "capacity_basis": "zfs available + landed",
    "writable_gib": 9, "held_gib": 171, "landed_gib": 169, "in_flight_gib": 2,
    "tokens": {"fill_mb_s_pool_side": 255, "stage_gib": 178},
    "ledger": {"capacity": {"fill_mb_s_pool_side": 255, "stage_gib": 178}},
}
EMPTY_TIER = {"tier": "stage", "tier_id": TIER, "capacity_bytes": 210 * GIB}


def _bound(peak: int) -> dict[str, int]:
    return {"peak_gib": peak, "retained_gib": peak, "suffix_gib": 0}


class _Queue:
    def __init__(self, records):
        self._records = records

    def tiers(self):
        return list(self._records)


# -- the helper ----------------------------------------------------------------


def test_the_minted_supply_is_the_announced_tokens_not_the_writable_room():
    assert storage_tiers.minted_tokens(LIVE)["stage_gib"] == 178
    # tier_tokens is the discovery number the tier loop mints FROM.
    assert storage_tiers.tier_tokens(LIVE)["stage_gib"] == 9


@pytest.mark.parametrize("tokens", [
    None, {}, {"fill_mb_s_pool_side": 255},
    {"stage_gib": 0}, {"stage_gib": True}, {"stage_gib": "178"},
    {"stage_gib": -5}, "stage_gib",
])
def test_a_record_without_a_usable_supply_falls_back_to_the_discovery_number(
        tokens):
    record = dict(EMPTY_TIER)
    if tokens is not None:
        record["tokens"] = tokens
    assert storage_tiers.minted_tokens(record)["stage_gib"] == 210


def test_a_record_with_no_capacity_at_all_mints_nothing():
    assert "stage_gib" not in storage_tiers.minted_tokens(
        {"tier": "stage", "tier_id": TIER})


# -- pbrun: one submission ---------------------------------------------------------


@pytest.mark.parametrize("peak", [88, 102, 178])
def test_a_prefix_that_fits_the_minted_supply_is_not_refused(peak):
    assert pbrun.prelaunch_capacity_refusal(
        LIVE, TIER, _bound(peak), ["source"], "e5342393c5fc" + "0" * 52) is None


def test_a_prefix_above_the_minted_supply_is_still_refused_naming_it():
    message = pbrun.prelaunch_capacity_refusal(
        LIVE, TIER, _bound(179), ["source"], "e5342393c5fc" + "0" * 52)
    assert message is not None
    assert "needs peak 179 GiB" in message
    assert "minted capacity of 178 GiB" in message
    assert "Nothing was sealed or published." in message


def test_an_empty_tier_behaves_as_before():
    digest = "e5342393c5fc" + "0" * 52
    assert pbrun.prelaunch_capacity_refusal(
        EMPTY_TIER, TIER, _bound(210), ["source"], digest) is None
    assert "210 GiB" in pbrun.prelaunch_capacity_refusal(
        EMPTY_TIER, TIER, _bound(211), ["source"], digest)


def test_unknown_capacity_never_refuses():
    assert pbrun.prelaunch_capacity_refusal(
        {"tier": "stage", "tier_id": TIER}, TIER, _bound(10_000),
        ["source"], "e5342393c5fc" + "0" * 52) is None


# -- pbgang: the joint sum -----------------------------------------------------------


def _gang_refusal(monkeypatch, record, sizes):
    plan = _member(sizes, 1, "a" * 64, "gang-member")
    monkeypatch.setattr(pbgang.residency_plan, "read",
                        lambda queue, key: plan)
    return pbgang.gang_prelaunch_refusal(_Queue([record]), ["member-0"])


def test_a_gang_prefix_that_fits_the_minted_supply_is_not_refused(monkeypatch):
    assert _gang_refusal(monkeypatch, dict(LIVE), [88, 10, 10]) is None


def test_a_gang_prefix_above_the_minted_supply_is_refused_naming_it(
        monkeypatch):
    message = _gang_refusal(monkeypatch, dict(LIVE), [200, 10, 10])
    assert message is not None
    assert "minted capacity of 178 GiB" in message


def test_a_gang_on_an_empty_tier_behaves_as_before(monkeypatch):
    assert _gang_refusal(monkeypatch, dict(EMPTY_TIER), [88, 10, 10]) is None
    message = _gang_refusal(monkeypatch, dict(EMPTY_TIER), [240, 10, 10])
    assert message is not None and "210 GiB" in message
