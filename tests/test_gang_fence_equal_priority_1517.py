"""When an elected gang drains its hosts for new equal-priority work (#1517, 2026-10-06).

``gang_blocking`` is a pure function of the census, the row, the host and the
time, so the rule is read directly.  The election in the census carries the
gang's rank (whose second field is its first member's publication time) and the
leads its members wait on.
"""
import pytest

from prismabuild import _measurement_reservation as reservation

GANG_FIRST_PUBLISHED = 1000.0
LEAD = "e" * 64


def _census(priority=-10):
    return {"gang_elections": {"g" * 63 + "1": {
        "group": "g" * 32, "index": 0, "action_key": "g" * 63 + "1", "host": "sparky",
        "priority": priority, "rank": [-priority, GANG_FIRST_PUBLISHED, "g" * 32],
        "leads": [LEAD]}}}


def _row(**over):
    row = {"action_key": "a" * 64, "priority": -10, "published_unix": GANG_FIRST_PUBLISHED + 5,
           "needs_gpu": True, "resources": {"cpu": 4, "gpu": 1, "mem_gb": 24}}
    row.update(over)
    return row


YOUNG = GANG_FIRST_PUBLISHED + reservation.GANG_DRAIN_AFTER_S - 1
OLD = GANG_FIRST_PUBLISHED + reservation.GANG_DRAIN_AFTER_S + 1


def _blocked(row, *, now=OLD, **kw):
    return reservation.gang_blocking(_census(**kw), row, host="sparky", group=None, now=now)


def test_a_young_gang_does_not_drain_equal_priority_work():
    assert _blocked(_row(), now=YOUNG) is None


def test_a_gang_that_has_waited_ten_minutes_drains_new_equal_priority_work():
    blocked = _blocked(_row())
    assert blocked is not None and blocked["drain"] is True


@pytest.mark.parametrize("resources,needs_gpu", [
    ({"cpu": 4, "gpu": 1, "mem_gb": 24}, True),   # a GPU single
    ({"cpu": 1, "mem_gb": 1}, False),             # CPU-only, or egress-shaped: no per-row exemption
    ({"cpu": 1, "mem_gb": 1, "stage_gib@t:h": 4}, False)])
def test_no_per_row_exemption_for_unrelated_work(resources, needs_gpu):
    assert _blocked(_row(resources=resources, needs_gpu=needs_gpu)) is not None


def test_published_before_or_after_the_gang_makes_no_difference_once_it_drains():
    assert _blocked(_row(published_unix=GANG_FIRST_PUBLISHED - 100)) is not None


def test_strictly_lower_priority_is_fenced_from_the_start_and_is_not_marked_as_a_drain():
    blocked = _blocked(_row(priority=-20), now=GANG_FIRST_PUBLISHED + 1)
    assert blocked is not None and "drain" not in blocked


def test_higher_priority_is_never_drained():
    assert _blocked(_row(priority=0)) is None


def test_the_leads_the_gang_waits_on_are_exempt():
    """A member waiting for a lead cannot start if the drain forbids the lead."""
    assert _blocked(_row(action_key=LEAD)) is None


def test_a_publication_canary_slot_is_exempt():
    row = _row(resources={"cpu": 1, "gpu": 1, "mem_gb": 16},
               publication_canary={"host": "sparky", "generation": "g", "run_id": "r"})
    assert _blocked(row) is None


def test_a_gang_member_is_never_drained_by_another_gangs_election():
    row = _row(gang={"group": "h" * 32, "size": 2, "index": 1})
    assert reservation.gang_blocking(_census(), row, host="sparky", group="h" * 32, now=OLD) is None


def test_another_host_and_a_missing_rank_are_not_drained():
    assert reservation.gang_blocking(_census(), _row(), host="sparklina", group=None, now=OLD) is None
    census = _census()
    next(iter(census["gang_elections"].values()))["rank"] = None
    assert reservation.gang_blocking(census, _row(), host="sparky", group=None, now=OLD) is None
