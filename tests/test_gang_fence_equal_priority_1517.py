"""Which equal-priority rows a waiting gang fences (#1517, 2026-10-06).

The census election carries the gang's rank, whose second field is its first
member's publication time.  ``gang_blocking`` is a pure function of the census,
the row and the host, so the rule is read directly.
"""
from prismabuild import _measurement_reservation as reservation

GANG_FIRST_PUBLISHED = 1000.0


def _census(priority=-10):
    return {"gang_elections": {"g" * 63 + "1": {
        "group": "g" * 32, "index": 0, "action_key": "g" * 63 + "1", "host": "sparky",
        "priority": priority, "rank": [-priority, GANG_FIRST_PUBLISHED, "g" * 32]}}}


def _row(**over):
    row = {"action_key": "a" * 64, "priority": -10, "published_unix": GANG_FIRST_PUBLISHED + 5}
    row.update(over)
    return row


def _blocked(row, **kw):
    return reservation.gang_blocking(_census(**kw), row, host="sparky", group=None) is not None


def test_a_later_single_of_the_gangs_priority_is_fenced():
    assert _blocked(_row())


def test_a_single_that_arrived_first_is_not_fenced():
    assert not _blocked(_row(published_unix=GANG_FIRST_PUBLISHED - 1))
    assert not _blocked(_row(published_unix=GANG_FIRST_PUBLISHED))


def test_strictly_lower_priority_is_fenced_whenever_it_was_published():
    assert _blocked(_row(priority=-20, published_unix=GANG_FIRST_PUBLISHED - 100))


def test_higher_priority_is_not_fenced():
    assert not _blocked(_row(priority=0))


def test_a_gang_member_is_not_fenced_by_another_gangs_election_at_equal_priority():
    row = _row(gang={"group": "h" * 32, "size": 2, "index": 1})
    assert reservation.gang_blocking(_census(), row, host="sparky", group="h" * 32) is None


def test_a_residency_mover_is_not_fenced_because_the_gang_waits_on_it():
    assert not _blocked(_row(residency={"range_start_bytes": 0, "range_end_bytes": 1 << 30}))


def test_a_producers_dependent_is_not_fenced():
    assert not _blocked(_row(dependent_of="p" * 64))


def test_a_row_without_a_publication_time_keeps_the_old_rule():
    row = _row()
    del row["published_unix"]
    assert not _blocked(row)


def test_another_host_is_not_fenced():
    assert reservation.gang_blocking(_census(), _row(), host="sparklina", group=None) is None
