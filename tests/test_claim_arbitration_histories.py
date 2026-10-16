"""Arbitration preserves real queue state and conditional observation boundaries."""
import sys

import pytest

from prismabuild import _gang, _measurement_reservation as reservation, pool
from prismabuild import runtime_publication
from test_gang_reservation_1517 import gang_fleet  # noqa: F401
from test_measurement_drains_gpu_backfill import fleet  # noqa: F401


def _row(queue, key):
    return pool._read_json(queue.item_path(pool.READY, key))


def _assert_ready_unheld(queue, key):
    assert queue.item_path(pool.READY, key).is_file()
    assert not queue.item_path(pool.CLAIMED, key).exists()
    assert queue.ledger("sparklina").holder_tokens(key) == {}


def _elect(queue, group, key, host, now):
    record = _gang.read_group(queue, group)
    row = _row(queue, key)
    entry = _gang.member(record, row, row["gang"])
    return _gang.elect_gang_member(queue, record, entry, host, now)


def _census_election(queue, election):
    group = _gang.read_group(queue, election["group"])
    return {
        **{key: election[key] for key in ("group", "index", "action_key", "host", "priority")},
        "rank": [-group["priority"], min(row["published_unix"] for row in group["members"]), group["group"]],
        "demand": _row(queue, election["action_key"])["resources"],
        "member_state": "ready",
    }


def _unexpected(*args, **kwargs):
    raise AssertionError("an excluded arbitration observation ran")


def test_an_ordinary_claim_skips_gang_and_untrusted_movement_reads(gang_fleet, monkeypatch):
    queue, clock, publish, finish, claim, denial, members = gang_fleet
    key = publish("ordinary-arbitration", tags=["sparklina"])
    monkeypatch.setattr(runtime_publication, "live_authority", lambda: False)
    monkeypatch.setattr(reservation, "movement_drained", _unexpected)
    monkeypatch.setattr(_gang, "sealed", _unexpected)
    monkeypatch.setattr(_gang, "resident_hold", _unexpected)
    monkeypatch.setattr(queue, "_gang_prelaunch_blocker", _unexpected)
    assert claim("sparklina", ready=[_row(queue, key)]) == key
    record = pool._read_json(queue.item_path(pool.CLAIMED, key))
    assert record["claimed_host"] == "sparklina"
    assert "gang_backfill" not in record
    assert queue.ledger("sparklina").held_keys() == [key]


@pytest.mark.parametrize("failure", ["missing_group", "member_error"])
def test_gang_diagnostic_failure_keeps_the_active_handler(gang_fleet, monkeypatch, failure):
    queue, clock, publish, finish, claim, denial, members = gang_fleet
    group, (key, peer) = members("active-gang-handler", file_group=failure != "missing_group")
    cause = ValueError("original gang diagnostic")
    write_error = OSError("diagnostic unavailable")
    observed = []

    def refused(item, reason, evidence=None):
        observed.append((reason, sys.exception()))
        if reason == "gang_group_incomplete":
            raise cause
        assert sys.exception() is cause
        raise write_error

    def invalid_member(*args):
        raise cause

    if failure == "member_error":
        monkeypatch.setattr(_gang, "member", invalid_member)
    monkeypatch.setattr(queue, "record_denial", refused)
    with pytest.raises(OSError) as caught:
        claim("sparklina", ready=[_row(queue, key)])
    assert caught.value is write_error
    assert observed[-1] == ("gang_contract_invalid", cause)
    _assert_ready_unheld(queue, key)
    assert queue.item_path(pool.READY, peer).is_file()
    assert _gang.elections(queue, group, 2) == {}


def test_a_failed_census_is_cached_after_real_gang_preparation(gang_fleet, monkeypatch):
    queue, clock, publish, finish, claim, denial, members = gang_fleet
    group, (first, peer) = members("cached-census-gang")
    second = publish("cached-census-ordinary", tags=["sparklina"])
    reads = []
    reclaimed = []
    real_reclaim = queue._preempt_gang_backfill

    def unavailable(self):
        reads.append(self)
        raise reservation.CensusUnavailable("native characterization refusal")

    def reclaim(*args, **kwargs):
        reclaimed.append(args[1]["group"])
        return real_reclaim(*args, **kwargs)

    monkeypatch.setattr(reservation.CensusReader, "capture", unavailable)
    monkeypatch.setattr(queue, "_preempt_gang_backfill", reclaim)
    assert claim("sparklina", ready=[_row(queue, first), _row(queue, second)]) is None
    assert len(reads) == 1
    assert reclaimed == [group]
    for key in (first, second):
        refusal = denial(key, "sparklina")
        assert refusal["reason"] == "measurement_census_unavailable"
        assert refusal["evidence"]["unavailable"] == "native characterization refusal"
        _assert_ready_unheld(queue, key)
    assert queue.item_path(pool.READY, peer).is_file()


def test_a_gang_reservation_denial_is_published_while_H_is_held(gang_fleet, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    from prismabuild import adaptive_cpu

    queue, clock, publish, finish, claim, denial, members = gang_fleet
    group, (member, peer) = members("under-H-denial", priority=10)
    election = _elect(queue, group, member, "sparklina", clock[0])
    key = publish("fenced-under-H", priority=0, tags=["sparklina"])
    real_denial = queue.record_denial
    seen = []

    def available_H():
        gate = adaptive_cpu.AdmissionGate(queue.ledger("sparklina"))
        try:
            with gate.locked():
                return True
        except adaptive_cpu.AdmissionBusy:
            return False

    def record(item, reason, evidence=None):
        if reason == "deferred_for_gang_reservation":
            with ThreadPoolExecutor(max_workers=1) as executor:
                seen.append(executor.submit(available_H).result(timeout=5))
        return real_denial(item, reason, evidence)

    monkeypatch.setattr(queue, "record_denial", record)
    assert claim("sparklina", ready=[_row(queue, key)]) is None
    assert seen == [False]
    assert available_H()
    assert denial(key)["evidence"]["gang_election"] == _census_election(queue, election)
    _assert_ready_unheld(queue, key)


@pytest.mark.parametrize("existing", ["mine", "foreign", "sibling"])
def test_an_existing_election_skips_resident_and_prelaunch_observation(
        gang_fleet, monkeypatch, existing):
    queue, clock, publish, finish, claim, denial, members = gang_fleet
    group, (key, peer) = members("standing-election")
    target = peer if existing == "sibling" else key
    election = _elect(queue, group, target,
                      "sparky" if existing == "foreign" else "sparklina", clock[0])
    row = _row(queue, key)
    row["resident_set"] = "a" * 64
    pool._write_json_atomic(queue.item_path(pool.READY, key), row)
    monkeypatch.setattr(_gang, "resident_hold", _unexpected)
    monkeypatch.setattr(queue, "_gang_prelaunch_blocker", _unexpected)
    assert claim("sparklina", ready=[row]) is None
    refusal = denial(key)
    if existing == "mine":
        assert refusal["reason"] == "gang_waiting_for_peers"
    else:
        assert refusal["reason"] == "gang_member_elected_elsewhere"
        assert refusal["evidence"]["election"] == (
            None if existing == "sibling" else _census_election(queue, election))
        assert refusal["evidence"]["sibling_on_this_host"] is (existing == "sibling")
    assert _gang.elections(queue, group, 2)[row["gang"]["index"] if existing != "sibling" else 1] == election
    _assert_ready_unheld(queue, key)


@pytest.mark.parametrize("resident", [None, "", 7, "a" * 64])
@pytest.mark.parametrize("outcome", ["hold", "prelaunch", "error_fallback"])
def test_resident_preference_precedes_prelaunch_and_keeps_its_fallback(
        gang_fleet, monkeypatch, resident, outcome):
    queue, clock, publish, finish, claim, denial, members = gang_fleet
    group, (key, peer) = members("resident-arbitration")
    row = _row(queue, key)
    row["resident_set"] = resident
    pool._write_json_atomic(queue.item_path(pool.READY, key), row)
    events = []
    hold = {"set_id": "a" * 64, "preferred_host": "sparky"}
    blocker = {"group": group, "pending": [peer]}

    def resident_hold(*args, **kwargs):
        events.append("resident")
        assert args[5] == clock[0]
        if outcome == "error_fallback":
            raise AttributeError("resident observation unreadable")
        return hold if outcome == "hold" else None

    def prelaunch(*args):
        events.append("prelaunch")
        return blocker

    monkeypatch.setattr(_gang, "resident_hold", resident_hold)
    monkeypatch.setattr(queue, "_gang_prelaunch_blocker", prelaunch)
    monkeypatch.setattr(_gang, "elect_gang_member", _unexpected)
    assert claim("sparklina", ready=[row], advance=None) is None
    asked_resident = isinstance(resident, str) and bool(resident)
    held = asked_resident and outcome == "hold"
    assert events == (["resident"] if held else
                      ["resident", "prelaunch"] if asked_resident else ["prelaunch"])
    refusal = denial(key)
    assert refusal["reason"] == ("deferred_for_resident_copy" if held else "deferred_for_gang_prelaunch")
    assert refusal["evidence"] == (hold if held else blocker)
    assert _gang.elections(queue, group, 2) == {}
    _assert_ready_unheld(queue, key)


def test_a_ranked_gang_skips_all_later_election_observers(gang_fleet, monkeypatch):
    queue, clock, publish, finish, claim, denial, members = gang_fleet
    ahead, (first, _) = members("higher-ranked", priority=10)
    election = _elect(queue, ahead, first, "sparklina", clock[0])
    group, (key, peer) = members("ranked-behind", priority=10)
    monkeypatch.setattr(_gang, "resident_hold", _unexpected)
    monkeypatch.setattr(queue, "_gang_prelaunch_blocker", _unexpected)
    assert claim("sparklina", ready=[_row(queue, key)]) is None
    assert denial(key)["evidence"] == {
        "withheld_for": first, "gang_election": _census_election(queue, election), "ranked_behind": True}
    assert _gang.elections(queue, group, 2) == {}
    _assert_ready_unheld(queue, key)


def test_an_aged_shortfall_never_asks_for_a_backfill_proof(gang_fleet, monkeypatch):
    queue, clock, publish, finish, claim, denial, members = gang_fleet
    group, (member, peer) = members("reserved-shortfall", priority=-10)
    _elect(queue, group, member, "sparklina", clock[0])
    clock[0] += reservation.GANG_RESERVE_AFTER_S + 1
    key = publish("shortfall-is-no-loan", priority=-10, retry_safe=True, tags=["sparklina"])
    monkeypatch.setattr(_gang, "backfill_enabled", _unexpected)
    monkeypatch.setattr(queue, "_preemption_eligible", _unexpected)
    monkeypatch.setattr(queue, "_preemption_proof_binding", _unexpected)
    assert claim("sparklina", ready=[_row(queue, key)]) is None
    refused = denial(key)
    assert refused["reason"] == "deferred_for_gang_reservation"
    assert refused["evidence"]["gang_election"]["reservation"] == {"gpu": 1}
    _assert_ready_unheld(queue, key)


def test_backfill_proofs_stop_at_the_first_refusal(gang_fleet, monkeypatch):
    queue, clock, publish, finish, claim, denial, members = gang_fleet
    elections = []
    for index in range(3):
        group, (member, peer) = members(f"ordered-backfill-{index}", priority=10)
        elections.append(_elect(queue, group, member, "sparklina", clock[0]))
        record = _gang.read_group(queue, group)
        row = _row(queue, member)
        _gang.mark_ready(queue, record, _gang.member(record, row, row["gang"]),
                         "sparklina", clock[0])
    elections.sort(key=lambda entry: entry["action_key"])
    elections = [_census_election(queue, entry) for entry in elections]
    key = publish("partly-loanable", priority=-10, retry_safe=True, tags=["sparklina"])
    proofs = []
    original = _gang.backfill_allowed

    def allowed(queue, election, now):
        proofs.append(election)
        assert election == elections[len(proofs) - 1]
        assert len(proofs) <= 2
        assert original(queue, election, now)
        return len(proofs) == 1

    monkeypatch.setattr(_gang, "backfill_allowed", allowed)
    assert claim("sparklina", ready=[_row(queue, key)]) is None
    assert proofs == elections[:2]
    assert denial(key)["evidence"]["gang_election"] == elections[1]
    _assert_ready_unheld(queue, key)
