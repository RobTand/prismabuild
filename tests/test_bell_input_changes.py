"""Meaningful input changes and real evidence expiry do not become a poll tick."""
from prismabuild.queue_bell_input_changes import (
    InputChanges, InputObservation, claim_denial_meaning, pass_meaning,
    peer_evidence_deadlines,
    adaptive_meaning,
    denial_transition_meaning,
)


KEY = "a" * 64


def test_timestamp_only_peer_refusals_and_own_passes_do_not_feed_back():
    changes = InputChanges()
    record = {"records": {KEY + ":100.0": {
        "action_key": KEY, "published_unix": 100., "host": "peer",
        "reason": "reservation_unavailable", "denied_unix": 100.,
    }}}
    observe = lambda: InputObservation(claim_denial_meaning(record), "confirmed")
    assert changes.observe("claim-denials", "peer", observe())
    for stamp in range(101, 111):
        record["records"][KEY + ":100.0"]["denied_unix"] = stamp
        assert not changes.observe("claim-denials", "peer", observe())
    passes = {"action_key": KEY, "passes": 3, "first_unix": 100., "updated_unix": 110.}
    assert not changes.observe("passes", KEY,
        InputObservation(pass_meaning(passes), "confirmed"), own_write=True)
    passes["updated_unix"] = 111.
    assert not changes.observe("passes", KEY,
        InputObservation(pass_meaning(passes), "confirmed"))
    passes["passes"] = 4
    assert changes.observe("passes", KEY,
        InputObservation(pass_meaning(passes), "confirmed"))
    assert changes.changed == 2
    assert changes.coalesced == 12


def test_equal_capacity_does_not_hide_freshness_recovery_or_mature_proof():
    changes = InputChanges()
    assert changes.observe("adaptive", "peer", InputObservation({"cpu": 4}, "unknown"))
    assert changes.observe("adaptive", "peer", InputObservation({"cpu": 4}, "fresh", (105.,)))
    assert not changes.observe("adaptive", "peer", InputObservation({"cpu": 4}, "fresh", (106.,)))
    assert changes.next_deadline == 106.
    assert changes.due(105.) == set()
    assert changes.due(106.) == {("adaptive", "peer")}
    assert changes.due(1000.) == set()
    assert changes.observe("adaptive", "peer", InputObservation({"cpu": 4}, "stale"))
    assert changes.observe("adaptive", "peer",
        InputObservation({"cpu": 4, "baseline_mature": True}, "fresh", (1010.,)))
    assert changes.observe("adaptive", "peer", InputObservation(None, "absent"))
    assert changes.next_deadline is None


def test_offer_expiry_is_evidence_and_does_not_create_a_1262_policy_timeout():
    changes = InputChanges()
    assert changes.observe("workers", "peer", InputObservation({"cpu": 4}, "fresh", (220.,)))
    assert changes.next_deadline == 220.
    assert changes.due(219.999) == set()
    assert changes.due(220.) == {("workers", "peer")}
    assert changes.next_deadline is None
    assert changes.due(100000.) == set()


def test_mutable_observation_cannot_rewrite_the_baseline():
    changes = InputChanges()
    body = {"low_samples": 1, "rounded_capacity": 4}
    assert changes.observe("adaptive", "peer", InputObservation(body, "fresh"))
    body["low_samples"] = 2
    assert changes.observe("adaptive", "peer", InputObservation(body, "fresh"))


def test_deadlines_use_actual_offer_cpu_and_gpu_horizons():
    import math
    assert peer_evidence_deadlines(now=100., announced_unix=100., cpu_sampled_unix=100.,
        gpu_sampled_unix=100.) == (
            math.nextafter(220., math.inf), math.nextafter(105., math.inf),
            math.nextafter(105., math.inf))
    assert peer_evidence_deadlines(now=221., announced_unix=100.,
                                   cpu_sampled_unix=100., gpu_sampled_unix=100.) == ()


def test_same_value_mirror_timestamps_do_not_hide_proof_or_freshness_changes():
    changes = InputChanges()
    documents = {"gpu-state.json": {"sampled_unix": 100., "sample_id": "100",
                                   "low_samples": 1, "rounded_capacity": 4}}
    observe = lambda: InputObservation(adaptive_meaning(documents), "fresh")
    assert changes.observe("adaptive", "peer", observe())
    for stamp in range(101, 111):
        documents["gpu-state.json"].update(sampled_unix=stamp, sample_id=str(stamp),
            _snapshot={"copied_unix": stamp})
        assert not changes.observe("adaptive", "peer", observe())
    documents["gpu-state.json"]["low_samples"] = 2
    assert changes.observe("adaptive", "peer", observe())
    assert changes.changed == 2
    assert changes.coalesced == 10


def test_sample_history_times_remain_part_of_baseline_maturity():
    documents = {"cpu-sample.json": {"sampled_unix": 100.,
        "observation": {"sampled_unix": 100., "cpu": 4},
        "history": [{"sampled_unix": 99., "cpu": 4}]}}
    changes = InputChanges()
    observe = lambda: InputObservation(adaptive_meaning(documents), "fresh")
    assert changes.observe("adaptive", "peer", observe())
    documents["cpu-sample.json"]["history"][0]["sampled_unix"] = 98.
    assert changes.observe("adaptive", "peer", observe())


def test_repeated_denial_stamps_coalesce_but_a_flap_to_an_old_reason_changes():
    changes = InputChanges()
    document = {"action_key": KEY, "transitions": [
        {"host": "peer", "published_unix": 100., "unix": 100., "last_unix": 100.,
         "reason": "transition_busy", "count": 1},
        {"host": "peer", "published_unix": 100., "unix": 101., "last_unix": 101.,
         "reason": "reservation_unavailable", "count": 1},
    ]}
    observe = lambda: InputObservation(denial_transition_meaning(document), "confirmed")
    assert changes.observe("denial-transitions", KEY, observe())
    document["transitions"][1].update(last_unix=102., count=2)
    assert not changes.observe("denial-transitions", KEY, observe())
    document["transitions"][0].update(last_unix=103., count=2)
    assert changes.observe("denial-transitions", KEY, observe())
