"""PB #1429: the lifetime contract's seal, clock, predicate, audit and admission.

The sealed ``execution_timeout_s`` is a payload budget; it never proves a
release bound. The opt-in ``params.lifetime`` contract is separate. These
tests pin what a finite verdict needs: the clock, every phase bounded, and
the strict comparison with the original opportunity. Admission cases run
through the real queue, ledgers and controllers with only clocks and sampler
observations controlled.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "tools" / "fleet")]
sys.path[:0] = [str(ROOT / "tests")]
from prismabuild import _measurement_reservation as reservation  # noqa: E402
from prismabuild import adaptive_gpu, core as pb, lifetime_fence, local_scratch, pool  # noqa: E402
from lifetime_fixtures_1429 import (  # noqa: E402
    FENCE_S,
    claim as claim_fenced,
    claim_on_host,
    fenced_queue,
    good_log,
    publish_fenced,
    seal,
)
from test_measurement_drains_gpu_backfill import fleet as fleet_fixture  # noqa: E402
from test_measurement_reservation_backfill_1419 import (  # noqa: E402
    _assert_holder_unchanged,
    _bounded_measurement_wait,
    _observe_real_sharing_permission,
)

fleet = fleet_fixture


def _param(fence_s):
    return {"lifetime": {"schema": lifetime_fence.LIFETIME_SCHEMA_V1, "fence_s": fence_s}}


def test_lifetime_seal_is_versioned_and_bounded(tmp_path):
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    (checkout / "task.py").write_text("print('fence')\n")
    fenced = seal(checkout, _param(300))
    assert pb.action_lifetime(fenced) == {
        "schema": lifetime_fence.LIFETIME_SCHEMA_V1, "fence_s": 300.0}
    assert lifetime_fence.required_tags(pb.action_lifetime(fenced)) == [
        lifetime_fence.LIFETIME_TAG]
    assert lifetime_fence.fence_seconds(pb.action_lifetime(fenced)) == 300.0
    assert lifetime_fence.fence_seconds(None) is None
    # An unfenced twin seals byte-identical params to before the flag existed.
    plain = seal(checkout, {})
    assert "lifetime" not in plain["params"]
    assert pb.action_lifetime(plain) is None
    assert lifetime_fence.required_tags(None) == []
    # The shortest fence is the reserve plus a minute to run in.
    assert lifetime_fence.MIN_FENCE_S == lifetime_fence.RELEASE_RESERVE_S + 60.0
    seal(checkout, _param(lifetime_fence.MIN_FENCE_S))
    seal(checkout, _param(lifetime_fence.MAX_FENCE_S))
    for bad in (5.0, lifetime_fence.MIN_FENCE_S - 1.0, lifetime_fence.MAX_FENCE_S + 1.0,
                10 ** 9, 0.0, -3.0, "300", float("nan"), float("inf"), True):
        with pytest.raises(pb.ActionContractError):
            seal(checkout, _param(bad))
    with pytest.raises(pb.ActionContractError):
        seal(checkout, {"lifetime": {"schema": "other.v1", "fence_s": 300}})
    with pytest.raises(pb.ActionContractError):
        seal(checkout, {"lifetime": {"fence_s": 300}})


def test_the_clock_keeps_a_reserve_between_the_stop_instant_and_the_deadline():
    fence_clock = lifetime_fence.clock(published_unix=1000.0, fence_s=300.0)
    assert fence_clock.deadline_unix == 1300.0
    assert fence_clock.stop_unix == 1300.0 - lifetime_fence.RELEASE_RESERVE_S
    assert not fence_clock.launch_expired(fence_clock.stop_unix - 0.001)
    assert fence_clock.launch_expired(fence_clock.stop_unix)
    assert fence_clock.launch_expired(None)
    assert fence_clock.launch_expired(float("nan"))
    # A phase before the launch must end by the stop instant; the rest by the deadline.
    for component in lifetime_fence.COMPONENTS:
        expected = (fence_clock.stop_unix if component.bound == lifetime_fence.STOP
                    else fence_clock.deadline_unix)
        assert fence_clock.bound(component.phase) == expected
    assert [c.phase for c in lifetime_fence.COMPONENTS if c.bound == lifetime_fence.STOP] == [
        "admission", "checkout", "readiness", "prelaunch"]
    # No room before the stop instant, or an unreadable end: UNKNOWN.
    for published, fence in ((1000.0, lifetime_fence.RELEASE_RESERVE_S), (None, 300.0),
                             (1000.0, None), (float("nan"), 300.0), (1000.0, float("inf")),
                             (True, 300.0), (1000.0, True), (1000.0, "300")):
        assert lifetime_fence.clock(published_unix=published, fence_s=fence) is None
        assert lifetime_fence.fence_deadline(published_unix=published, fence_s=fence) is None
    assert lifetime_fence.fence_deadline(published_unix=1000.0, fence_s=300.0) == 1300.0


def test_the_phase_table_names_every_applicable_phase_once():
    assert lifetime_fence.PHASES == (
        "admission", "checkout", "readiness", "prelaunch", "payload",
        "credited_waits", "termination", "cleanup", "scope_settlement",
        "resource_release")
    assert lifetime_fence.EXECUTION_PHASES + lifetime_fence.SETTLEMENT_PHASES == (
        lifetime_fence.PHASES)
    assert len(set(lifetime_fence.PHASES)) == len(lifetime_fence.PHASES)
    for component in lifetime_fence.COMPONENTS:
        assert component.bound in (lifetime_fence.STOP, lifetime_fence.RELEASE)
        assert component.mechanism


def _support(**overrides):
    support = lifetime_fence.components_support()
    support.update(overrides)
    return support


def test_a_finite_prospective_bound_needs_every_phase_bounded():
    kwargs = dict(published_unix=1000.0, fence_s=300.0, now_unix=1100.0)
    stop = lifetime_fence.clock(published_unix=1000.0, fence_s=300.0).stop_unix
    assert lifetime_fence.components_support() == {p: None for p in lifetime_fence.PHASES}
    assert lifetime_fence.prospective_bound(components=_support(), **kwargs) == 1300.0
    # The stop instant still ahead is part of the verdict; at it, UNKNOWN.
    assert lifetime_fence.prospective_bound(
        components=_support(), **{**kwargs, "now_unix": stop - 0.001}) == 1300.0
    for now in (stop, stop + 1.0, 1300.0, 1400.0, None, float("nan")):
        assert lifetime_fence.prospective_bound(
            components=_support(), **{**kwargs, "now_unix": now}) is None
    # One unfenced component, in any phase, makes the whole verdict UNKNOWN.
    for phase in lifetime_fence.PHASES:
        assert lifetime_fence.prospective_bound(
            components=_support(**{phase: "this phase has no enforcement"}),
            **kwargs) is None
        absent = _support()
        del absent[phase]
        assert lifetime_fence.prospective_bound(components=absent, **kwargs) is None
    for components in (None, {}, [], "bounded"):
        assert lifetime_fence.prospective_bound(components=components, **kwargs) is None
    for broken in ({"published_unix": None}, {"fence_s": None}, {"fence_s": 100.0}):
        assert lifetime_fence.prospective_bound(
            components=_support(), **{**kwargs, **broken}) is None
    # Shapes the contract does not cover name the phase they leave unfenced.
    assert lifetime_fence.components_support(gang=True)["admission"]
    assert lifetime_fence.components_support(scratch=True)["cleanup"]
    for shape in ({"gang": True}, {"scratch": True}):
        assert lifetime_fence.prospective_bound(
            components=lifetime_fence.components_support(**shape), **kwargs) is None


def _audit(record):
    return lifetime_fence.release_bound(
        published_unix=1000.0, fence_s=300.0, evidence=record)


def test_the_audit_accepts_a_complete_record_and_nothing_less():
    log = good_log()
    assert _audit(log.as_record()) == 1300.0
    assert lifetime_fence.AttemptLog.from_record(log.as_record()).as_record() == log.as_record()
    for evidence in (None, "record", [], {}):
        assert _audit(evidence) is None
    for field, value in (("schema", "other.v1"), ("published_unix", 999.0),
                         ("fence_s", 301.0), ("deadline_unix", 1301.0),
                         ("stop_unix", 1181.0), ("reserve_s", 60.0)):
        assert _audit({**log.as_record(), field: value}) is None
    assert lifetime_fence.release_bound(
        published_unix=None, fence_s=300.0, evidence=log.as_record()) is None
    assert lifetime_fence.release_bound(
        published_unix=1000.0, fence_s=None, evidence=log.as_record()) is None
    # Another publication's record never answers for this one.
    assert lifetime_fence.release_bound(
        published_unix=1001.0, fence_s=300.0, evidence=log.as_record()) is None


def _break(record, phase, how):
    phases = {name: dict(entry) for name, entry in record["phases"].items()}
    bound = phases[phase]["bound_unix"]
    if how == "missing":
        del phases[phase]
    elif how == "unenforced":
        phases[phase]["enforced"] = False
    elif how == "foreign-mechanism":
        phases[phase]["mechanism"] = "something-else"
    elif how == "no-mechanism":
        phases[phase]["mechanism"] = None
    elif how == "no-evidence":
        phases[phase]["evidence"] = ""
    elif how == "untyped-evidence":
        phases[phase]["evidence"] = None
    elif how == "ended-at-the-bound":
        # A record that still says enforced: the audit re-checks the end,
        # and an end exactly at the bound did not end before it.
        phases[phase]["ended_unix"] = bound
    elif how == "ended-late":
        phases[phase]["ended_unix"] = bound + 0.001
    elif how == "never-ended":
        phases[phase]["ended_unix"] = None
    elif how == "bound-moved":
        phases[phase]["bound_unix"] = bound + 60.0
        phases[phase]["ended_unix"] = bound + 30.0
    return {**record, "phases": phases}


@pytest.mark.parametrize("phase", lifetime_fence.PHASES)
@pytest.mark.parametrize("how", [
    "missing", "unenforced", "foreign-mechanism", "no-mechanism", "no-evidence",
    "untyped-evidence", "ended-at-the-bound", "ended-late", "never-ended",
    "bound-moved"])
def test_one_unproven_phase_makes_the_audit_unknown(phase, how):
    assert _audit(_break(good_log().as_record(), phase, how)) is None


@pytest.mark.parametrize("phase", lifetime_fence.PHASES)
def test_a_phase_is_enforced_only_under_its_own_mechanism_within_its_bound(phase):
    component = {c.phase: c for c in lifetime_fence.COMPONENTS}[phase]
    fence_clock = lifetime_fence.clock(published_unix=1000.0, fence_s=300.0)
    bound = fence_clock.bound(phase)
    log = lifetime_fence.AttemptLog(fence_clock)
    assert log.end(phase, mechanism=component.mechanism, evidence="e",
                   ended_unix=bound - 0.001)
    assert not log.end(phase, mechanism=component.mechanism, evidence="e", ended_unix=bound)
    assert not log.end(phase, mechanism=component.mechanism, evidence="e",
                       ended_unix=bound + 0.001)
    assert not log.end(phase, mechanism=None, evidence="e", ended_unix=bound - 1.0)
    assert not log.end(phase, mechanism="other", evidence="e", ended_unix=bound - 1.0)
    assert not log.end(phase, mechanism=component.mechanism, evidence="e", ended_unix=None)
    assert not log.end(phase, mechanism=component.mechanism, evidence="e",
                       ended_unix=float("nan"))
    assert not log.end(phase, mechanism=component.mechanism, evidence="e", ended_unix=True)


def test_a_refused_launch_names_its_phase_and_never_reads_as_enforced():
    fence_clock = lifetime_fence.clock(published_unix=1000.0, fence_s=300.0)
    log = lifetime_fence.AttemptLog(fence_clock, {"action_key": "a" * 64})
    log.refuse("readiness", now_unix=fence_clock.stop_unix, evidence="late")
    record = log.as_record()
    assert record["expired_phase"] == "readiness"
    assert record["phases"]["readiness"]["enforced"] is False
    assert _audit(record) is None


def test_timed_backfill_gate_refuses_equality_and_later_bounds():
    assert lifetime_fence.timed_backfill_allowed(
        candidate_bound=100.0, original_opportunity=200.0) is True
    assert lifetime_fence.timed_backfill_allowed(
        candidate_bound=199.999, original_opportunity=200.0) is True
    assert lifetime_fence.timed_backfill_allowed(
        candidate_bound=200.0, original_opportunity=200.0) is False
    assert lifetime_fence.timed_backfill_allowed(
        candidate_bound=300.0, original_opportunity=200.0) is False
    assert lifetime_fence.timed_backfill_allowed(
        candidate_bound="UNKNOWN", original_opportunity=200.0) is False
    assert lifetime_fence.timed_backfill_allowed(
        candidate_bound=100.0, original_opportunity="UNKNOWN") is False
    assert lifetime_fence.timed_backfill_allowed(
        candidate_bound=float("nan"), original_opportunity=200.0) is False
    assert lifetime_fence.timed_backfill_allowed(
        candidate_bound=True, original_opportunity=200.0) is False


def test_payload_only_timeout_never_proves_a_release_bound(fleet):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    incumbent, measurement, original_end, snapshot = _bounded_measurement_wait(fleet)
    # A five-second payload budget is still opportunity metadata only.
    candidate = publish("payload-only-five-seconds", priority=-10, timeout_s=5)
    _observe_real_sharing_permission(fleet, candidate)
    assert claim() is None
    refusal = denial(candidate)
    assert refusal["reason"] in (
        "deferred_for_measurement_reservation", "deferred_behind_withheld_row")
    if refusal["reason"] == "deferred_for_measurement_reservation":
        assert refusal["evidence"]["candidate_release_bound"] == "UNKNOWN"
    else:
        assert refusal["evidence"]["withheld_for"] == measurement


def test_unfenced_candidate_holds_the_reserved_host(fleet):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    incumbent, measurement, original_end, snapshot = _bounded_measurement_wait(fleet)
    candidate = publish("unfenced-timed-candidate", priority=-10, timeout_s=60)
    _observe_real_sharing_permission(fleet, candidate)
    assert claim() is None, "an unfenced candidate took the reserved host"
    assert queue.item_path(pool.READY, candidate).exists()
    assert queue.ledger().held_keys() == [incumbent]
    _assert_holder_unchanged(queue, incumbent, snapshot)
    assert queue.item_path(pool.READY, measurement).exists()
    # Past the payload-only point the host still holds: expiry never
    # returns capacity, only proved settlement does.
    tick(original_end - clock[0] + 0.1)
    _observe_real_sharing_permission(fleet, candidate)
    assert claim() is None
    _assert_holder_unchanged(queue, incumbent, snapshot)
    queue.finish(incumbent, status="executed")
    tick(adaptive_gpu.PSI_AVG10_S + 1)
    assert claim() == measurement, denial(measurement)
    assert queue.item_path(pool.READY, candidate).exists()


def test_candidate_release_bound_reads_unknown_without_fence(fleet):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    incumbent, measurement, original_end, snapshot = _bounded_measurement_wait(fleet)
    candidate = publish("bound-unknown-candidate", priority=-10, timeout_s=60)
    item = pool._read_json(queue.item_path(pool.READY, candidate))
    assert isinstance(item, dict)
    assert reservation.candidate_release_bound(queue, item) == "UNKNOWN"
    allowed, bound = reservation.timed_backfill_permitted(
        queue, item, {"opportunity_unix": original_end, "action_key": measurement})
    assert allowed is False
    assert bound == "UNKNOWN"


def test_ready_row_carries_the_sealed_clock_and_reads_a_finite_bound(fleet):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    incumbent, measurement, original_end, snapshot = _bounded_measurement_wait(fleet)
    key = publish_fenced(fleet, "fenced-ready-candidate")
    row = pool._read_json(queue.item_path(pool.READY, key))
    assert isinstance(row, dict)
    # Publish projects the clock and requires the capability tag.
    assert row.get("lifetime_fence_s") == FENCE_S
    assert row.get("lifetime_deadline_unix") == row["published_unix"] + FENCE_S
    assert lifetime_fence.LIFETIME_TAG in (row.get("tags") or [])
    assert "claimed_unix" not in row
    bound, support = reservation.candidate_release_verdict(queue, row)
    assert bound == row["lifetime_deadline_unix"]
    assert support == {phase: None for phase in lifetime_fence.PHASES}
    assert reservation.candidate_release_bound(queue, row) == bound


def test_a_candidate_whose_stop_instant_has_passed_reads_unknown_and_never_claims(fleet):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    incumbent, measurement, original_end, snapshot = _bounded_measurement_wait(fleet)
    key = publish_fenced(fleet, "fenced-expired-candidate")
    row = pool._read_json(queue.item_path(pool.READY, key))
    tick(row["lifetime_deadline_unix"] - lifetime_fence.RELEASE_RESERVE_S - clock[0] + 0.5)
    stale = pool._read_json(queue.item_path(pool.READY, key))
    assert reservation.candidate_release_bound(queue, stale) == "UNKNOWN"
    _observe_real_sharing_permission(fleet, key)
    assert claim_on_host(fleet) is None
    assert queue.item_path(pool.READY, key).exists()
    _assert_holder_unchanged(queue, incumbent, snapshot)


def test_a_box_without_the_capability_cannot_claim_a_fenced_row(fleet):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    key = publish_fenced(fleet, "needs-the-capability")
    result = queue.claim(
        capacity={"cpu": 20, "gpu": 1, "mem_gb": 120}, has_gpu=True,
        cpu_tiers={"preferred": list(range(20)), "fallback": []}, adaptive_cpu=True,
        tags=["gb10", "sparklina"])
    assert result is None
    assert queue.item_path(pool.READY, key).exists()
    assert queue.ledger().held_keys() == []


def test_a_scratch_declaring_candidate_reads_unknown_in_its_cleanup_phase(fleet):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    incumbent, measurement, original_end, snapshot = _bounded_measurement_wait(fleet)
    declaration = json.dumps([{"root_env": "PB_SCRATCH_ROOT", "name": "work"}])
    key = publish_fenced(fleet, "declares-scratch",
                         extra_variables={local_scratch.DECLARATIONS_ENV: declaration})
    row = pool._read_json(queue.item_path(pool.READY, key))
    bound, support = reservation.candidate_release_verdict(queue, row)
    assert bound is None
    assert support["cleanup"] and all(
        reason is None for phase, reason in support.items() if phase != "cleanup")
    _observe_real_sharing_permission(fleet, key)
    assert claim_on_host(fleet) is None
    assert queue.item_path(pool.READY, key).exists()


def _opportunity_case(fleet, delta):
    """A fenced candidate whose deadline is ``delta`` seconds from the opportunity."""

    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    incumbent, measurement, original_end, snapshot = _bounded_measurement_wait(fleet)
    fence_s = (original_end + delta) - clock[0]
    key = publish_fenced(fleet, f"fenced-{delta}", fence_s=fence_s)
    row = pool._read_json(queue.item_path(pool.READY, key))
    assert row["lifetime_deadline_unix"] == original_end + delta
    _observe_real_sharing_permission(fleet, key)
    return key, incumbent, measurement, original_end, snapshot


@pytest.mark.parametrize("delta", [-60.0, -0.001])
def test_a_bound_strictly_before_the_opportunity_backfills(fleet, delta):
    queue = fleet[0]
    key, incumbent, measurement, original_end, snapshot = _opportunity_case(fleet, delta)
    bound = reservation.candidate_release_bound(
        queue, pool._read_json(queue.item_path(pool.READY, key)))
    assert bound == original_end + delta
    assert claim_on_host(fleet) == key, fleet[7](key)
    # Capacity and isolation are unchanged: the incumbent keeps its tokens, the
    # measurement keeps waiting, and the candidate holds its own reservation.
    assert set(queue.ledger().held_keys()) == {incumbent, key}
    _assert_holder_unchanged(queue, incumbent, snapshot)
    assert queue.item_path(pool.READY, measurement).exists()
    claimed = pool._read_json(queue.item_path(pool.CLAIMED, key))
    assert claimed["lifetime_deadline_unix"] == original_end + delta


@pytest.mark.parametrize("delta", [0.0, 0.001, 60.0])
def test_equality_and_later_bounds_stay_ready_through_real_admission(fleet, delta):
    queue = fleet[0]
    key, incumbent, measurement, original_end, snapshot = _opportunity_case(fleet, delta)
    bound = reservation.candidate_release_bound(
        queue, pool._read_json(queue.item_path(pool.READY, key)))
    # The bound is finite: it is the comparison, not the evidence, that refuses.
    assert bound == original_end + delta
    assert claim_on_host(fleet) is None
    refusal = fleet[7](key)
    assert refusal["reason"] in (
        "deferred_for_measurement_reservation", "deferred_behind_withheld_row")
    if refusal["reason"] == "deferred_for_measurement_reservation":
        assert refusal["evidence"]["candidate_release_bound"] == bound
    assert queue.item_path(pool.READY, key).exists()
    assert queue.ledger().held_keys() == [incumbent]
    _assert_holder_unchanged(queue, incumbent, snapshot)
    assert queue.item_path(pool.READY, measurement).exists()


def test_once_the_opportunity_has_passed_no_later_bound_admits_backfill(fleet):
    # An overrunning holder cannot turn into a stream of admitted backfill:
    # every bound ahead of the clock is later than the opportunity behind it.
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    incumbent, measurement, original_end, snapshot = _bounded_measurement_wait(fleet)
    tick(original_end - clock[0] + 1.0)
    key = publish_fenced(fleet, "fenced-after-the-opportunity")
    row = pool._read_json(queue.item_path(pool.READY, key))
    assert row["lifetime_deadline_unix"] > original_end
    _observe_real_sharing_permission(fleet, key)
    assert claim_on_host(fleet) is None
    assert queue.item_path(pool.READY, key).exists()
    _assert_holder_unchanged(queue, incumbent, snapshot)


def test_publish_stamps_the_publication_anchored_deadline(tmp_path):
    # Real publish path: the deadline is stamped where the publication
    # stamps its clock, before any claim or resource.
    queue, action = fenced_queue(tmp_path)
    row = pool._read_json(queue.item_path(pool.READY, action["action_key"]))
    assert row.get("lifetime_fence_s") == FENCE_S
    assert row.get("lifetime_deadline_unix") == row["published_unix"] + FENCE_S
    item = claim_fenced(queue)
    assert item.get("lifetime_fence_s") == FENCE_S
    assert item.get("lifetime_deadline_unix") == item["published_unix"] + FENCE_S
    # An old worker offering no fence tag cannot claim fenced work.
    (tmp_path / "second").mkdir()
    queue2, _ = fenced_queue(tmp_path / "second")
    assert queue2.queue.claim(capacity={"cpu": 8, "mem_gb": 16}, tags=[]) is None
