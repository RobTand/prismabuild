"""Dual-signal gang loans in private queues, driven by a fake clock."""
from __future__ import annotations

import json

import pytest

from prismabuild import _gang, fleet_roster, pool
from test_gang_reservation_1517 import gang_fleet  # noqa: F401
from test_measurement_drains_gpu_backfill import fleet  # noqa: F401
from test_gang_fence_backfill import waiting


@pytest.fixture
def absent_partner(gang_fleet, monkeypatch, request):
    queue, clock, publish, finish, claim, denial, members = gang_fleet
    incumbent = publish("partner-measurement", measurement=True, tags=["sparky"])
    assert claim("sparky") == incumbent
    group, keys = members("window", priority=getattr(request, "param", 10), mem_gb=104)
    assert claim("sparky") is None
    assert claim("sparklina") is None
    queue.records.worker_directory().mkdir(exist_ok=True)
    document = {"boxes": {
        "sparklina": {},
        "sparky": {"status": "offline", "status_reason": "test operator declaration",
                   "status_by": "fixture", "status_unix": clock.now}}}
    monkeypatch.setattr(fleet_roster.Roster, "at", classmethod(
        lambda cls, *args, **kwargs: fleet_roster.Roster.capture(json.dumps(document).encode())))
    return group, keys, incumbent, document


def observe(gang_fleet, seconds):
    queue, clock, publish, finish, claim, denial, members = gang_fleet
    for _ in range(seconds // 20):
        clock.now += 20
        ready = [row for row in queue.ready_items() if row.get("gang")]
        assert claim("sparklina", ready=ready) is None


def test_sustained_dual_absence_admits_one_declared_bounded_loan(
        gang_fleet, absent_partner):
    queue, clock, publish, finish, claim, denial, members = gang_fleet
    group, keys, incumbent, document = absent_partner
    observe(gang_fleet, 620)
    key = publish("bounded-dead-partner-loan", priority=-10, mem_gb=24,
                  tags=["sparklina"], timeout_s=1800, retry_safe=False, max_attempts=1)
    assert claim("sparklina") == key, "sustained dual absence refused the bounded loan"
    claimed = pool.record_store.value_or_raise(queue.records.item(pool.record_store.CLAIMED, key))
    assert claimed["gang_backfill"][0]["group"] == group
    assert queue.ledger("sparklina").held_keys() == [key]
    assert _gang.teardown(queue, group) is None


def bounded(publish, name="bounded", **options):
    return publish(name, priority=-10, mem_gb=1, cpu=1, gpu=0,
                   tags=["sparklina"], retry_safe=False, max_attempts=1, **options)


@pytest.mark.parametrize("timeout_s", [None, 1801])
def test_absence_never_lends_to_unbounded_or_overlong_work(gang_fleet, absent_partner, timeout_s):
    queue, clock, publish, finish, claim, denial, members = gang_fleet
    observe(gang_fleet, 620)
    key = bounded(publish, timeout_s=timeout_s)
    assert claim("sparklina") is None
    assert denial(key)["reason"] == "deferred_for_gang_reservation"
    assert queue.ledger("sparklina").held_keys() == []


def test_one_loan_slot_survives_completion_and_observer_restart(gang_fleet, absent_partner):
    queue, clock, publish, finish, claim, denial, members = gang_fleet
    observe(gang_fleet, 620)
    first = bounded(publish, "first", timeout_s=1800)
    second = bounded(publish, "second", timeout_s=60)
    assert claim("sparklina") == first
    assert claim("sparklina") is None, "admitted a second concurrent loan with free tokens"
    finish(first, "sparklina")
    from prismabuild.gang_loans import Observer
    queue._gang_loans = Observer()
    observe(gang_fleet, 620)
    assert claim("sparklina") is None, "completion or observer restart reused the bounded loan slot"
    assert queue.records.item_path(pool.record_store.READY, second).exists()
    assert queue.ledger("sparklina").held_keys() == []


@pytest.mark.parametrize("reset", ["gap", "unknown_offer", "runtime_present", "partner_offer", "restart"])
def test_unknown_presence_and_gaps_reset_without_a_bounded_candidate(
        gang_fleet, absent_partner, reset):
    queue, clock, publish, finish, claim, denial, members = gang_fleet
    group, keys, incumbent, document = absent_partner
    observe(gang_fleet, 620)
    if reset == "gap":
        clock.now += 31
    elif reset == "unknown_offer":
        path = queue.records.worker_offer_path("sparky")
        path.write_text("{unreadable")
    elif reset == "runtime_present":
        document["boxes"]["sparky"]["status"] = "active"
    elif reset == "partner_offer":
        queue.announce(host="sparky", capacity={"cpu": 20, "gpu": 1, "mem_gb": 120},
                       has_gpu=True, tags=["unrelated-tag"])
    else:
        from prismabuild.gang_loans import Observer
        queue._gang_loans = Observer()
    if reset == "unknown_offer":
        clock.now += 20
        queue._gang_loans.observe(queue, queue.ready_items(), "sparklina", clock.now)
    else:
        observe(gang_fleet, 20)
    document["boxes"]["sparky"]["status"] = "offline"
    queue.records.worker_offer_path("sparky").unlink(missing_ok=True)
    observe(gang_fleet, 280)
    key = bounded(publish, timeout_s=1800)
    assert claim("sparklina") is None, "an interrupted absence interval remained aged"
    observe(gang_fleet, 340)
    assert claim("sparklina") == key, "fresh continuous dual absence never restored eligibility"


def test_transient_offer_loss_with_runtime_present_lends_nothing(gang_fleet, absent_partner):
    queue, clock, publish, finish, claim, denial, members = gang_fleet
    absent_partner[3]["boxes"]["sparky"]["status"] = "active"
    observe(gang_fleet, 620)
    key = bounded(publish, timeout_s=1800)
    assert claim("sparklina") is None
    assert _gang.teardown(queue, absent_partner[0]) is None


def test_partner_returns_during_loan_without_claiming_until_actual_release(gang_fleet, absent_partner):
    queue, clock, publish, finish, claim, denial, members = gang_fleet
    group, keys, incumbent, document = absent_partner
    observe(gang_fleet, 620)
    loan = publish("active-return-loan", priority=-10, mem_gb=24,
                   tags=["sparklina"], timeout_s=1800, retry_safe=False, max_attempts=1)
    assert claim("sparklina") == loan
    document["boxes"]["sparky"]["status"] = "active"
    finish(incumbent, "sparky")
    assert claim("sparky") is None, "partner committed while a nonpreemptible loan held capacity"
    assert claim("sparklina") is None
    assert not queue.records.withdrawal_decisions(loan)
    assert queue.ledger("sparklina").held_keys() == [loan]
    finish(loan, "sparklina")
    assert claim("sparky") == keys[1]
    assert claim("sparklina") == keys[0]


@pytest.mark.parametrize("absent_partner", [-10], indirect=True)
def test_absence_loan_preserves_equal_priority_demand_reservation(gang_fleet, absent_partner):
    queue, clock, publish, finish, claim, denial, members = gang_fleet
    from prismabuild import _measurement_reservation as reservation
    observe(gang_fleet, int(reservation.GANG_RESERVE_AFTER_S) + 640)
    key = publish("reserved-gpu", priority=-10, mem_gb=24, tags=["sparklina"],
                  timeout_s=1800, retry_safe=False, max_attempts=1)
    assert claim("sparklina") is None, "bounded loan bypassed the gang's demand reservation"
    assert denial(key)["evidence"]["gang_election"]["reservation"]["gpu"] == 1
    assert queue.ledger("sparklina").held_keys() == []


def test_loan_permission_does_not_clear_a_second_fence(gang_fleet, absent_partner):
    queue, clock, publish, finish, claim, denial, members = gang_fleet
    observe(gang_fleet, 620)
    other, other_keys = members("second-fence", priority=10)
    record = _gang.read_group(queue, other)
    _gang.elect_gang_member(queue, record, record["members"][0], "sparklina", clock.now)
    _gang.mark_ready(queue, record, record["members"][0], "sparklina", clock.now)
    key = bounded(publish, timeout_s=1800)
    assert claim("sparklina") is None, "permission for one gang cleared a later fence"
    assert denial(key)["evidence"]["gang_election"]["group"] == other
    assert queue.ledger("sparklina").held_keys() == []


def test_finished_bounded_loan_preserves_default_preemptible_loans(gang_fleet, absent_partner):
    queue, clock, publish, finish, claim, denial, members = gang_fleet
    observe(gang_fleet, 620)
    first = bounded(publish, timeout_s=1800)
    assert claim("sparklina") == first
    finish(first, "sparklina")
    key = publish("default-preemptible", priority=-10, mem_gb=24,
                  tags=["sparklina"], timeout_s=None, retry_safe=True, max_attempts=3)
    assert claim("sparklina") == key, "bounded slot disabled the existing preemptible policy"


def test_alias_presence_with_wrong_tags_resets_absence(gang_fleet, absent_partner):
    queue, clock, publish, finish, claim, denial, members = gang_fleet
    document = absent_partner[3]
    document["boxes"]["canonical-partner"] = {
        **document["boxes"].pop("sparky"), "_alias": "sparky"}
    observe(gang_fleet, 620)
    queue.announce(host="sparky", capacity={"cpu": 20, "gpu": 1, "mem_gb": 120},
                   has_gpu=True, tags=["unrelated-tag"])
    observe(gang_fleet, 20)
    queue.records.worker_offer_path("sparky").unlink()
    observe(gang_fleet, 280)
    key = bounded(publish, timeout_s=1800)
    assert claim("sparklina") is None, "a partner's live alias did not reset absence"


def test_contended_member_prevents_a_bounded_loan_without_spending_tokens(
        gang_fleet, absent_partner):
    import threading
    queue, clock, publish, finish, claim, denial, members = gang_fleet
    observe(gang_fleet, 620)
    key = bounded(publish, timeout_s=1800)
    acquired, release = threading.Event(), threading.Event()
    def hold_member():
        with queue.action_transition_lock(absent_partner[1][1]):
            acquired.set()
            release.wait(60)
    worker = threading.Thread(target=hold_member)
    worker.start()
    try:
        assert acquired.wait(60)
        assert claim("sparklina") is None
        assert queue.ledger("sparklina").held_keys() == []
        assert queue.records.item_path(pool.record_store.READY, key).exists()
    finally:
        release.set()
        worker.join(60)
    assert not worker.is_alive()
    assert claim("sparklina") == key
