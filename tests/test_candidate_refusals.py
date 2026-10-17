"""Refusals distinguish own aging, diagnostic deferral and later scan joins."""
from collections.abc import Mapping
import json

import pytest

from prismabuild import candidate_policy as policy
from test_measurement_drains_gpu_backfill import fleet


GPU = frozenset({"gpu"})


@pytest.mark.parametrize("source,owner,result,extra", [
    ("deferred_behind_withholding", None, policy.Silent, None),
    ("deferred_behind_withholding", "producer", policy.Uncounted,
     {"withheld_for": "ahead", "withheld_kinds": ["gpu"]}),
    ("deferred_for_ready_gpu_row", None, policy.Uncounted, {"gpu_row": "ahead"}),
])
def test_placement_waits_do_not_earn_the_candidates_own_pass(source, owner, result, extra):
    order = policy.ScanOrder().withhold("ahead", GPU)
    prefix = policy.prepare_adaptive_refusal(
        source, None, None, order=order, dependent_owner=owner,
        room_taken={"gpu_row": "ahead"})
    assert isinstance(prefix, result)
    if extra is not None:
        assert prefix.denial.reason == source
        assert prefix.denial.evidence == {"decision": {}, **extra}
        assert prefix.denial.order_before is order
        assert prefix.denial.withhold_after is None


@pytest.mark.parametrize("why,suffix", [
    ("drains_soon", "_withholding"),
    ("unknown_past_ceiling", "_past_ceiling"),
    ("refilled_past_ceiling", "_past_ceiling"),
    ("holder_does_not_drain_soon", "_starved"),
])
def test_gpu_refusal_keeps_its_room_even_after_its_withhold_expires(why, suffix):
    verdict = {"eligible": True, "withhold": why == "drains_soon", "mode": "gpu",
               "why": why, "holders": [{"action_key": "incumbent"}]}
    order = policy.ScanOrder()
    prefix = policy.prepare_adaptive_refusal(
        "adaptive_gpu_refused", {"reason": "exclusive_holder"}, verdict,
        order=order, dependent_owner=None, room_taken=None)
    denial = policy.counted_adaptive_refusal(
        prefix, key="waiting", order=order, demand={"gpu": 1},
        reservation_demand={"cpu": 2, "gpu": 1}, mode="gpu", foreign=False,
        free_at_refusal={"cpu": 2, "gpu": 0})
    assert denial.reason == "adaptive_gpu_refused" + suffix
    assert denial.evidence["drain_resolves"] is True
    assert prefix.evidence == {"decision": {"reason": "exclusive_holder"}}
    assert denial.evidence["gpu_room_kept"] == {"cpu": 2, "gpu": 1}
    assert denial.order_before.rooms[0].action_key == "waiting"
    assert denial.order_before.withheld_for is None
    if why == "drains_soon":
        later = denial.withhold_after.apply(
            denial.order_before, identity=None, measurement_suspended=False)
        assert later.withheld_kinds == GPU
        assert later.withheld_for == "waiting"
    else:
        assert denial.withhold_after is None
        assert denial.evidence["starved"]["holders"] == verdict["holders"]


def test_a_measurements_own_dependent_is_starved_instead_of_stopping_its_producer():
    verdict = {"eligible": True, "withhold": True, "mode": "exclusive", "why": "drains_soon"}
    prefix = policy.prepare_adaptive_refusal(
        "adaptive_cpu_refused", {"reason": "measurement_holder", "isolated_by": "producer"},
        verdict, order=policy.ScanOrder(), dependent_owner="producer", room_taken=None)
    denial = policy.counted_adaptive_refusal(
        prefix, key="export", order=policy.ScanOrder(), demand={"cpu": 1},
        reservation_demand={"cpu": 1}, mode="exclusive", foreign=False, free_at_refusal={})
    assert denial.reason == "adaptive_cpu_refused_starved"
    assert denial.evidence["starved"]["why"] == "measurement_admits_only_its_dependents"
    assert denial.withhold_after is None
    assert verdict["withhold"] is True


def test_a_falsey_decision_still_contributes_its_original_post_count_reason():
    class FalseyDecision(dict):
        def __bool__(self):
            return False

    decision = FalseyDecision(reason="max_actions")
    prefix = policy.prepare_adaptive_refusal(
        "adaptive_gpu_refused", decision, None,
        order=policy.ScanOrder(), dependent_owner=None, room_taken=None)
    denial = policy.counted_adaptive_refusal(
        prefix, key="waiting", order=policy.ScanOrder(), demand={"gpu": 1},
        reservation_demand={"gpu": 1}, mode="gpu", foreign=False, free_at_refusal=None)
    assert denial.evidence == {"decision": {}, "drain_resolves": True}
    assert prefix.decision is decision


@pytest.mark.parametrize("eligible,withhold,reason", [
    (False, False, "reservation_unavailable"),
    (True, False, "reservation_unavailable_starved"),
    (True, True, "reservation_unavailable_withholding"),
])
def test_token_refusal_retains_unknown_evidence_and_only_joins_after_publication(
    eligible, withhold, reason,
):
    order = policy.ScanOrder().withhold("gpu-ahead", GPU)
    evidence = {"withhold": {"withhold": withhold, "why": "holders_cannot_cover"},
                "token_shortage": None, "withhold_age_s": None, "cpu_decision": None}
    denial = policy.counted_token_refusal(
        evidence, key="waiting", order=order, eligible=eligible,
        asked={"gpu": 1}, free_at_refusal={"gpu": 1})
    assert denial.reason == reason
    assert denial.evidence["token_shortage"] is None
    assert denial.evidence["withhold_age_s"] is None
    assert denial.evidence["cpu_decision"] is None
    assert ("starved" in denial.evidence) is (eligible and not withhold)
    assert denial.evidence["gpu_room_fit_at_refusal"] is True
    assert "gpu_room_fit_at_refusal" not in evidence
    assert "starved" not in evidence
    assert denial.order_before is order
    if withhold:
        joined = denial.withhold_after.apply(order, identity=None, measurement_suspended=False)
        assert joined.whole_box_held
        assert joined.withheld_for == "waiting"
    else:
        assert denial.withhold_after is None


def test_unknown_availability_does_not_read_reservation_counts_or_ineligible_verdict():
    class Unreadable(Mapping):
        def __getitem__(self, key):
            raise AssertionError("no observation owns this count")

        def __iter__(self):
            raise AssertionError("no observation owns this count")

        def __len__(self):
            raise AssertionError("no observation owns this count")

    denial = policy.counted_token_refusal(
        {"withhold": Unreadable()}, key="waiting", order=policy.ScanOrder(),
        eligible=False, asked=Unreadable(), free_at_refusal=None)
    assert denial.reason == "reservation_unavailable"
    assert denial.order_before.rooms == ()


@pytest.mark.parametrize("suspended,measurement,joined", [
    (False, True, True), (True, True, False), (True, False, True),
])
def test_suspension_only_suppresses_a_measurements_later_join(suspended, measurement, joined):
    order = policy.ScanOrder()
    after = policy.WithholdAfterDenial("waiting", None)
    actual = after.apply(order, identity=("shape", measurement), measurement_suspended=suspended)
    assert (actual.withheld_for == "waiting") is joined
    assert actual.measurement_withholds == (frozenset({"waiting"}) if joined and measurement else frozenset())


def test_falsey_identity_is_not_indexed_and_retains_its_truth_conversion():
    class FalseyIdentity(dict):
        def __bool__(self):
            return False

        def __getitem__(self, key):
            raise AssertionError("falsey identity must not be indexed")

    identity = FalseyIdentity()
    actual = policy.WithholdAfterDenial("waiting", None).apply(
        policy.ScanOrder(), identity=identity, measurement_suspended=False)
    assert actual.withheld_for == "waiting"
    assert actual.measurement_withholds == frozenset()


@pytest.mark.parametrize("fail_publication", [False, True])
def test_real_denial_counts_and_publishes_before_join_without_consuming_holder(
    fleet, monkeypatch, fail_publication,
):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    holder = publish("before-denial", timeout_s=1200)
    assert claim() == holder
    waiting = publish("waiting-denial", measurement=True, pinned=True)
    trace = []
    error = KeyError("denial publication failed")
    record = queue.record_denial
    apply = policy.WithholdAfterDenial.apply

    def record_then_join(item, reason, evidence=None):
        if item["action_key"] == waiting:
            assert queue.passes(waiting) == 1
            assert queue.ledger().held_keys() == [holder]
            trace.append("denial")
            if fail_publication:
                raise error
        return record(item, reason, evidence)

    def after_publication(self, order, **kwargs):
        ring = json.loads(queue.records.denial_transitions_path(waiting).read_text())
        assert ring["transitions"][-1]["reason"].endswith("_withholding")
        trace.append("join")
        return apply(self, order, **kwargs)

    monkeypatch.setattr(queue, "record_denial", record_then_join)
    monkeypatch.setattr(policy.WithholdAfterDenial, "apply", after_publication)
    if fail_publication:
        with pytest.raises(KeyError) as caught:
            claim()
        assert caught.value is error
        assert trace == ["denial"]
    else:
        assert claim() is None
        assert trace == ["denial", "join"]
        assert denial(waiting)["reason"].endswith("_withholding")
    assert queue.ledger().held_keys() == [holder]
    assert queue.records.item_path("ready", waiting).exists()
