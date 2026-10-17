"""Coexisting generations and unknown observations never collapse into one state."""
from pathlib import Path

import pytest

from prismabuild import endings, lifecycle, pool, record_store


def _claim(generation=1.0, owner="original"):
    return dict(action_key="e" * 64, published_unix=generation,
                claimed_by=owner, claimed_unix=generation + .5, attempts=0)


def _terminals(**records):
    return record_store.TerminalCapture(tuple(record_store.RecordObservation(
        record_store.QueueAddress(Path("/capture") / state, state, "e" * 64),
        record_store.Present(record)) for state, record in records.items()))


def test_ready_successor_and_previous_terminal_remain_separate_facts():
    previous, ready = _claim(), _claim(2.0, "successor")
    facts = lifecycle.classify_action(lifecycle.ActionCapture(
        publication=record_store.Present(ready), claim=record_store.Absent(),
        terminals=_terminals(done={**previous, "status": "executed"})))
    assert [fact.kind for fact in facts.for_generation(1.0)] == ["done"]
    assert [fact.kind for fact in facts.for_generation(2.0)] == ["publication"]
    assert facts.claim is lifecycle.ClaimRelation.ABSENT
    assert facts.terminal["generation"] == 1.0


def test_exact_claim_and_reservation_are_independent_of_generation_equality():
    original, successor = _claim(), _claim(owner="successor")
    held = lifecycle.ReservationFact("host", lifecycle.ReservationPhase.COMMITTED, successor)
    facts = lifecycle.classify_action(lifecycle.ActionCapture(
        claim=record_store.Present(successor), expected_claim=original, reservations=(held,)))
    assert facts.claim is lifecycle.ClaimRelation.DIFFERENT
    assert facts.reservations == (held,)
    assert facts.for_generation(1.0)[0].record == successor


def test_withdrawal_is_a_decision_beside_a_live_exact_claim():
    claim = _claim()
    decision = {**claim, "status": "withdrawn", "by": "operator"}
    facts = lifecycle.classify_action(lifecycle.ActionCapture(
        claim=record_store.Present(claim), expected_claim=claim,
        artifacts=(lifecycle.RecordFact("withdrawal_decision", decision),),
        terminals=_terminals(withdrawn=decision)))
    assert facts.claim is lifecycle.ClaimRelation.EXACT
    assert {fact.kind for fact in facts.for_generation(1.0)} == {
        "claim", "withdrawal_decision", "withdrawn"}
    assert facts.reservations is lifecycle.NotCaptured.INPUT


def test_before_and_after_claim_persistence_keep_distinct_reservation_authority():
    acquiring = lifecycle.ReservationFact("host", lifecycle.ReservationPhase.ACQUIRING)
    committed = lifecycle.ReservationFact("host", lifecycle.ReservationPhase.COMMITTED, _claim())
    absent = lifecycle.classify_action(lifecycle.ActionCapture(
        claim=record_store.Absent(), reservations=(acquiring, committed)))
    assert absent.claim is lifecycle.ClaimRelation.ABSENT
    assert absent.reservations == (acquiring, committed)
    assert acquiring.phase is not committed.phase


def test_uncaptured_unknown_and_absent_are_different_answers():
    uncaptured = lifecycle.classify_action(lifecycle.ActionCapture())
    unknown = lifecycle.classify_action(lifecycle.ActionCapture(
        claim=record_store.Unknown("unavailable")))
    absent = lifecycle.classify_action(lifecycle.ActionCapture(claim=record_store.Absent()))
    assert uncaptured.claim is lifecycle.ClaimRelation.UNOBSERVED
    assert unknown.claim is lifecycle.ClaimRelation.UNKNOWN
    assert absent.claim is lifecycle.ClaimRelation.ABSENT
    assert "claim" in uncaptured.uncaptured and "claim" not in unknown.uncaptured
    assert unknown.unknown == ("claim",) and absent.unknown == ()


def test_classification_uses_the_single_terminal_interpreter_without_io(monkeypatch):
    capture = _terminals(done={**_claim(), "status": "executed"})
    expected = endings.resolve_current(capture)
    observed = []
    resolve = endings.resolve_current

    def interpreted(value):
        observed.append(value)
        return resolve(value)

    with monkeypatch.context() as patch:
        patch.setattr(endings, "resolve_current", interpreted)
        patch.setattr(Path, "open", lambda *a, **kw: pytest.fail("classification performed I/O"))
        facts = lifecycle.classify_action(lifecycle.ActionCapture(terminals=capture))
    assert observed == [capture]
    assert facts.terminal == expected


@pytest.mark.parametrize("transition,destination", [
    (lifecycle.Transition.CANCEL, "ready"),
    (lifecycle.Transition.RELEASE_UNSTARTED, "failed"),
    (lifecycle.Transition.LATE, "done"),
])
def test_legal_transitions_reject_unowned_destinations(transition, destination):
    with pytest.raises(pool.PoolContractError, match="illegal"):
        lifecycle._legal_destination(transition, destination)


def test_unknown_cancelled_attempt_retains_claim_lease_and_reservation(tmp_path, monkeypatch):
    queue = pool.PoolQueue(tmp_path / "queue")
    key = "e" * 64
    queue.publish(action_key=key, cas_root=tmp_path / "cas", checkout_root=tmp_path,
                  worker_script="worker.py", resources={"cpu": 1})
    claim = queue.claim(owner="worker", capacity={"cpu": 1})
    queue.withdraw(key, signal_child=False)
    paths = (queue.records.item_path("claimed", key), queue.records.lease_path(key))
    saved = [path.read_bytes() for path in paths]
    monkeypatch.setattr(queue.records, "attempt_presence", lambda *args:
                        record_store.Unknown("unavailable", error=OSError("cannot read attempt")))
    assert queue.reap_stale(timeout_s=-1) == []
    assert [path.read_bytes() for path in paths] == saved
    assert queue.ledger().held() == {"cpu": 1}
    assert not queue.records.attempt_path(claim, 1).exists()
