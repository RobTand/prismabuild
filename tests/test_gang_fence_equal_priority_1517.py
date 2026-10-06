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


EGRESS = "c" * 64
DEEP_LEAD = "d" * 64
EXPORT = "b" * 64


def _census(priority=-10):
    return {"gang_elections": {"g" * 63 + "1": {
        "group": "g" * 32, "index": 0, "action_key": "g" * 63 + "1", "host": "sparky",
        "priority": priority, "rank": [-priority, GANG_FIRST_PUBLISHED, "g" * 32],
        "leads": [LEAD, DEEP_LEAD]}},
        "capacity_returners": [EGRESS]}


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
    ({"cpu": 1, "mem_gb": 1}, False),             # CPU-only, even egress-shaped: the census names egresses
    ({"cpu": 1, "mem_gb": 1, "stage_gib@t:h": 4}, False)])    # a row that takes tier tokens
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


def test_work_that_returns_capacity_is_never_drained():
    """Principle 1: a stage or RAM egress, named by the census, and an export."""
    egress = _row(action_key=EGRESS, needs_gpu=False, resources={"cpu": 1, "mem_gb": 1})
    assert _blocked(egress) is None
    assert reservation.returns_capacity(egress, _census())
    export = _row(action_key=EXPORT, needs_gpu=False, resources={"cpu": 1, "mem_gb": 1},
                  dependent_of="f" * 64)
    assert _blocked(export) is None
    assert not reservation.returns_capacity(_row(), _census())


def test_the_whole_prerequisite_closure_is_never_drained():
    """Principle 2: a lead's own prerequisite is exempt, not only the direct lead."""
    assert _blocked(_row(action_key=DEEP_LEAD)) is None
    election = next(iter(_census()["gang_elections"].values()))
    assert reservation.in_gang_closure(_row(action_key=DEEP_LEAD), election)
    assert not reservation.in_gang_closure(_row(), election)


#: Every row class the drain can meet on a member host: (name, row, what the
#: gang waits on, held).  "Waited on" rows are the edges of the wait-for graph:
#: the running incumbents (admitted before the drain, so never held), what returns
#: capacity, and the prerequisite closure.  The gang starts within the drain bound
#: plus the longest running job exactly when no waited-on row is ever held, and
#: it stays starvable by new work exactly when every other competitor is held.
ROW_CLASSES = [
    ("gpu single", dict(), False, True),
    ("cpu-only single", dict(needs_gpu=False, resources={"cpu": 8, "mem_gb": 64}), False, True),
    ("tier-token taker", dict(needs_gpu=False, resources={"cpu": 1, "mem_gb": 1, "stage_gib@t:h": 4}),
     False, True),
    ("stage or ram egress", dict(action_key=EGRESS, needs_gpu=False,
                                 resources={"cpu": 1, "mem_gb": 1}), True, False),
    ("produced export", dict(action_key=EXPORT, needs_gpu=False, dependent_of="f" * 64,
                             resources={"cpu": 1, "mem_gb": 1}), True, False),
    ("direct lead", dict(action_key=LEAD), True, False),
    ("lead of a lead", dict(action_key=DEEP_LEAD, needs_gpu=False,
                            resources={"cpu": 1, "mem_gb": 1, "stage_gib@t:h": 2}), True, False),
    ("publication canary", dict(publication_canary={"host": "sparky", "generation": "g", "run_id": "r"}),
     False, False),
    ("another gang's member", dict(gang={"group": "h" * 32, "size": 2, "index": 1}), False, False),
    ("higher priority", dict(priority=0), False, False),
]


@pytest.mark.parametrize("name,over,waited_on,held", ROW_CLASSES, ids=[c[0] for c in ROW_CLASSES])
def test_every_row_class_against_the_drain(name, over, waited_on, held):
    blocked = _blocked(_row(**over))
    assert (blocked is not None) is held, name
    assert not (waited_on and blocked is not None), f"{name}: a cycle, the gang waits on it"


def test_the_gang_starts_within_the_drain_bound_plus_the_longest_running_job():
    """No edge of the wait-for graph is held, so the longest chain is bounded.

    A gang's start time is when its drain begins (``GANG_DRAIN_AFTER_S``) plus
    the longest run of anything it waits on.  Each waited-on class is admitted
    during the drain (the parametrised test above), and each runs for at most
    ``MAX_RUN_S``; classes that are held cannot extend the wait because they are
    not admitted.  The closure's depth adds one run per level at worst.
    """
    max_run_s = 1800.0
    depth = 2  # a lead, then the lead's own prerequisite
    admitted_during_drain = [c for c in ROW_CLASSES
                             if _blocked(_row(**c[1])) is None and c[2]]
    assert {c[0] for c in admitted_during_drain} == {
        "stage or ram egress", "produced export", "direct lead", "lead of a lead"}
    # Every waited-on class is admitted, so the longest chain the gang can wait
    # through is the drain bound plus one run per level of the closure.
    assert reservation.GANG_DRAIN_AFTER_S + (1 + depth) * max_run_s == 6000.0
