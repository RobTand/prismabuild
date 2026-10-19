"""Literal reservation and gang positions, independent of queue effects."""
import pytest

from prismabuild import _gang, _measurement_reservation as reservation


@pytest.mark.parametrize("authority, expected", [(True, 10), (False, None), (1, None), ("yes", None), (None, None)])
@pytest.mark.parametrize("candidate, suspended", [(9, True), (10, True), (11, False)])
def test_only_mature_authority_suspends_a_measurement_at_or_below_the_gang(
        authority, expected, candidate, suspended):
    census = {"gang_elections": {"member": {
        "host": "local", "group": "group", "priority": 10,
        "rank": [-10, 1000.0, "group"], "member_state": "ready"}}}
    position = reservation.candidate_priority(
        census, {"priority": candidate}, host="local", now=1601,
        authority=authority, group=None)
    assert position.priority == expected
    assert position.measurement_suspended is (suspended if expected is not None else False)


@pytest.mark.parametrize("now, group, host", [(1600, None, "local"), (1601, "group", "local"), (1601, None, "other")])
def test_a_nonreserving_position_does_not_coerce_candidate_priority(now, group, host):
    census = {"gang_elections": {"member": {
        "host": "local", "group": "group", "priority": 10,
        "rank": [-10, 1000.0, "group"], "member_state": "ready"}}}
    position = reservation.candidate_priority(
        census, {"priority": object()}, host=host, now=now, authority=True, group=group)
    assert position == reservation.ReservationPriority(None, False)


@pytest.mark.parametrize("reserved, suspended, expected", [(None, False, True), (7, True, True), (10, True, False), (11, True, False)])
def test_measurement_priority_suppression_keeps_the_strictly_higher_selection(
        reserved, suspended, expected):
    chosen = {"action_key": "chosen", "priority": 10, "host": "local"}
    census = {"elections": {"chosen": chosen}}
    position = reservation.ReservationPriority(reserved, suspended)
    blocked = reservation.measurement_blocker(
        census, {"action_key": "candidate", "priority": 0}, position, host="local")
    assert blocked is (chosen if expected else None)


@pytest.mark.parametrize("owner, held, elections, serves, deferred", [
    (None, None, {}, False, True),
    (7, None, {}, False, True),
    ("producer", [], {}, False, True),
    ("producer", ["producer"], {}, True, False),
    ("producer", ["producer"], {"producer": {}}, False, True),
    ("chosen", [], {}, False, False),
])
def test_only_a_held_unelected_incumbent_clears_the_blocker(owner, held, elections, serves, deferred):
    chosen = {"action_key": "chosen"}
    position = reservation.candidate_blocker(
        chosen, dependent_owner=owner, held_keys=held, census={"elections": elections})
    assert position.serves_incumbent is serves
    assert position.must_defer is deferred
    assert position.blocker is (None if serves else chosen)


def test_incumbent_election_access_remains_conditional():
    class NotObserved(dict):
        def __getitem__(self, key):
            raise AssertionError(key)

    for owner, held in [(None, None), (object(), None), ("producer", [])]:
        position = reservation.candidate_blocker(
            None, dependent_owner=owner, held_keys=held, census=NotObserved())
        assert not position.serves_incumbent and not position.must_defer


@pytest.mark.parametrize("why, elsewhere, expected", [
    ("measurement_drain_expired", True, {"withhold": False, "drain_resolves": False,
        "why": "measurement_reserved_on_other_host"}),
    ("measurement_drain_expired", False, {"withhold": True, "drain_resolves": True,
        "why": "measurement_reservation_waiting"}),
    ("draining_for_measurement", False, {"withhold": False, "drain_resolves": True,
        "why": "draining_for_measurement"}),
])
def test_canonical_selection_amends_attention_without_changing_its_input(why, elsewhere, expected):
    nested = {}
    verdict = {"why": why, "withhold": False, "drain_resolves": True, "detail": nested}
    chosen = {"host": "reserved"}
    result = reservation.amend_verdict(verdict, chosen, reserved_elsewhere=elsewhere)
    assert result == dict(expected, detail=nested, selection=chosen)
    assert result["selection"] is chosen and result["detail"] is nested
    assert verdict == {"why": why, "withhold": False, "drain_resolves": True, "detail": {}}
    assert reservation.amend_verdict(verdict, None, reserved_elsewhere=False) is verdict


def test_census_failure_keeps_original_evidence_and_replaces_only_attention():
    nested = {}
    verdict = {"why": "old", "withhold": False, "detail": nested}
    result = reservation.unavailable_verdict(verdict, "reader unavailable")
    assert result == {"why": "measurement_census_unavailable", "withhold": True,
                      "detail": nested, "census_error": "reader unavailable"}
    assert result["detail"] is nested
    assert verdict == {"why": "old", "withhold": False, "detail": {}}


@pytest.mark.parametrize("mine_host, sibling_host, needs", [
    (None, None, True), ("local", None, False), ("remote", None, False),
    (None, "local", False), (None, "remote", True), ("remote", "local", False),
])
def test_member_position_keeps_standing_and_sibling_fences(mine_host, sibling_host, needs):
    record = {"group": "ours", "priority": 10,
              "members": [{"published_unix": 1002}, {"published_unix": 1000}]}
    elections = {}
    mine = {"group": "ours", "index": 0, "host": mine_host}
    if mine_host is not None:
        elections["mine"] = mine
    if sibling_host is not None:
        elections["sibling"] = {"group": "ours", "index": 1, "host": sibling_host}
    position = _gang.election_position(elections, record, {"group": "ours", "index": 0}, host="local")
    assert isinstance(position, _gang.ElectionPosition)
    assert position.mine is (mine if mine_host is not None else None)
    assert position.sibling_here is (sibling_host == "local")
    assert position.needs_election is needs
    assert _gang.elected_elsewhere(position.mine, host="local") is (mine_host != "local")


@pytest.mark.parametrize("other_host, rank, wins", [
    ("local", [-11, 1100, "other"], True),
    ("local", [-10, 999, "other"], True),
    ("local", [-10, 1000, "a"], True),
    ("local", [-10, 1000, "z"], False),
    ("local", [-9, 900, "other"], False),
    ("unrelated", [-11, 900, "other"], False),
    ("peer", [-11, 900, "other"], True),
])
def test_only_a_better_gang_on_a_shared_host_ranks_ahead(other_host, rank, wins):
    record = {"group": "ours", "priority": 10,
              "members": [{"published_unix": 1002}, {"published_unix": 1000}]}
    other = {"group": "other", "index": 0, "host": other_host, "rank": rank}
    elections = {"other": other, "peer": {"group": "ours", "index": 1, "host": "peer"}}
    position = _gang.election_position(elections, record, {"group": "ours", "index": 0}, host="local")
    if wins:
        assert isinstance(position, _gang.RankedBehind)
        assert position.election is other
    else:
        assert position == _gang.ElectionPosition(None, False, True)


def test_the_best_rank_wins_even_when_it_arrives_after_a_worse_rank():
    record = {"group": "ours", "priority": 10, "members": [{"published_unix": 1000}]}
    worse = {"group": "worse", "index": 0, "host": "local", "rank": [-11, 1000, "worse"]}
    best = {"group": "best", "index": 0, "host": "local", "rank": [-12, 1200, "best"]}
    position = _gang.election_position(
        {"worse": worse, "best": best}, record, {"group": "ours", "index": 0}, host="local")
    assert position == _gang.RankedBehind(best)
    assert position.election is best


def test_equal_rank_ties_keep_the_original_election_iteration_order():
    record = {"group": "ours", "priority": 10, "members": [{"published_unix": 1000}]}
    first = {"group": "first", "index": 0, "host": "local", "rank": [-11, 1000, "tie"]}
    second = {"group": "second", "index": 0, "host": "local", "rank": [-11, 1000, "tie"]}
    position = _gang.election_position(
        {"first": first, "second": second}, record, {"group": "ours", "index": 0}, host="local")
    assert position.election is first
