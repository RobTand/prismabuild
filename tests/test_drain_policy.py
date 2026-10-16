"""Fairness episodes reduce staged observations without owning those reads."""
from dataclasses import replace
import json

import pytest

from prismabuild import adaptive_cpu, drain_policy as drain, pool
from test_measurement_drains_gpu_backfill import fleet


GEN = "a" * 64
KEY = "b" * 64
NOW = 2000.0


def prepared(*, prior=None, now=NOW, host="host", **kwargs):
    return drain.prepare_drain(drain.DrainQuestion(KEY, {"cpu": 2}, "tokens", **kwargs),
                               {"passes": 3} if prior is None else prior, now=now, host=host)


def assessment(result):
    assert isinstance(result, drain.DrainAssessment), result
    return result


@pytest.mark.parametrize("age,kind,withhold,why", [
    (899, "unknown", True, "drains_soon"),
    (901, "unknown", False, "unknown_past_ceiling"),
    (901, "transient", True, "drains_soon"),
    (1, "long", False, "holder_does_not_drain_soon"),
])
def test_declared_opportunity_and_own_age_have_distinct_limits(age, kind, withhold, why):
    start = drain.begin_drain(prepared(prior={"passes": 3, "first_unix": NOW-age}), {}, None)
    added = drain.add_holder(start, holder="holder", bound={"bound": kind}, counts={"cpu": 2})
    result = assessment(drain.finish_drain(added))
    assert (result.verdict["withhold"], result.verdict["why"]) == (withhold, why)
    assert result.notes.changes["blocked_unix"] == NOW
    assert start.bounds == []


def test_partial_coverage_cannot_hold_the_box_and_clears_an_old_episode():
    start = drain.begin_drain(prepared(), {}, None)
    added = drain.add_holder(start, holder="holder", bound={"bound": "transient"}, counts={"cpu": 1})
    result = assessment(drain.finish_drain(added))
    assert result.verdict["why"] == "holders_cannot_cover"
    assert result.notes.changes == {"blocked_unix": NOW, "epoch_unix": None, "expired_unix": None}


def test_refill_expires_then_a_real_departure_opens_a_new_episode():
    prior = {"passes": 3, "epoch_unix": 100.0}
    start = drain.begin_drain(prepared(prior=prior), {}, None)
    full = drain.add_holder(start, holder="refill", bound={
        "bound": "transient", "claimed_unix": 1500., "action_key": "refill"}, counts={"cpu": 2})
    expired = assessment(drain.finish_drain(full))
    assert expired.verdict["why"] == "refilled_past_ceiling"
    carried = {**prior, **drain.ordinary_note_fields(prior, expired.notes)}
    next_start = drain.begin_drain(prepared(prior=carried, now=2001.), {}, None)
    replacement = drain.add_holder(next_start, holder="later", bound={
        "bound": "transient", "claimed_unix": 2001., "action_key": "later"}, counts={"cpu": 2})
    resumed = assessment(drain.finish_drain(replacement))
    assert resumed.verdict["why"] == "drains_soon"
    assert resumed.notes.changes["epoch_unix"] == 2001.
    assert resumed.notes.changes["expired_unix"] is None


def test_measurement_snapshots_declared_end_and_does_not_renew_for_later_arrivals():
    p = prepared(measurement=True, measurement_generation=GEN)
    started = drain.begin_drain(p, {}, None)
    first = drain.add_holder(started, holder="first", bound={
        "bound": "long", "claimed_unix": 1900., "requested_timeout_s": 2000.}, counts={"cpu": 2})
    result = assessment(drain.finish_drain(first))
    assert result.verdict["drain_until_unix"] == 3900.
    prior = {"passes": 3, **drain.measurement_note_fields({}, result.notes)}
    later = drain.begin_drain(prepared(prior=prior, now=3800., measurement=True,
                                       measurement_generation=GEN), {}, None)
    later = drain.add_holder(later, holder="new", bound={
        "bound": "long", "claimed_unix": 3800., "requested_timeout_s": 9000.}, counts={"cpu": 2})
    assert assessment(drain.finish_drain(later)).verdict["drain_until_unix"] == 3900.
    other = drain.begin_drain(prepared(prior=prior, now=4000., host="peer", measurement=True,
                                       measurement_generation=GEN), {}, None)
    assert assessment(drain.finish_drain(other)).verdict["drain_until_unix"] == 4900.


@pytest.mark.parametrize("error_type", [OSError, drain.PrismaBuildError, ValueError, TypeError, KeyError, OverflowError])
def test_failed_read_keeps_partial_notes_without_publishing_partial_holder_census(error_type):
    p = prepared(measurement=True, measurement_generation=GEN,
                 measurement_unmeasured=True, measurement_foreign_clear=True)
    start = drain.begin_drain(p, {}, None)
    partial = drain.add_holder(start, holder="first", bound={"bound": "transient"}, counts={"cpu": 2})
    error = error_type("later holder read")
    result = drain.failed_drain(partial, error)
    assert result.notes.changes == {"unmeasured_until_unix": 2180., "foreign_unix": None}
    if error_type in (KeyError, OverflowError):
        assert isinstance(result, drain.DrainFailure)
        assert result.error is error
    else:
        result = assessment(result)
        assert result.verdict["why"] == "verdict_unreadable"
        assert "holders" not in result.verdict


def test_uncaught_fold_failure_returns_before_the_next_observation():
    start = drain.begin_drain(prepared(measurement=True, measurement_generation=GEN,
                                      measurement_unmeasured=True), {}, None)
    result = drain.add_holder(start, holder="bad", bound={}, counts={"cpu": 2})
    assert isinstance(result, drain.DrainFailure)
    assert isinstance(result.error, KeyError)
    assert result.notes.changes == {"unmeasured_until_unix": 2180.}
    assert start.bounds == []


def test_finalizer_failure_replaces_original_exception_and_publishes_no_notes():
    start = drain.begin_drain(prepared(measurement=True, measurement_generation=GEN,
                                      measurement_unmeasured=True), {}, None)
    start.verdict["drain_until_unix"] = 10 ** 1000
    result = drain.failed_drain(start, KeyError("earlier"))
    assert isinstance(result, drain.DrainFailure)
    assert isinstance(result.error, OverflowError)
    assert result.notes is None


@pytest.mark.parametrize("cutoff,expected", [(None, 0.), (True, 0.), (float("inf"), 0.),
                                             (10 ** 1000, 0.), (2050., 2050.)])
def test_unreadable_unmeasured_cutoff_is_preserved_as_expired(cutoff, expected):
    prior = {"measurement_drains": {"host": {"generation": GEN,
                                              "unmeasured_until_unix": cutoff}}}
    result = drain.measurement_note_fields(drain.measurement_episodes(prior), drain.MeasurementNotes("host", GEN, {}))
    assert result["measurement_drains"]["host"]["unmeasured_until_unix"] == expected
    p = prepared(prior=prior, measurement=True, measurement_generation=GEN, measurement_unmeasured=True)
    result = drain.begin_drain(p, {}, None)
    if expected == 0.:
        result = assessment(result)
        assert result.verdict["why"] == "verdict_unreadable"
        assert result.verdict["drain_resolves"] is False
    else:
        assert isinstance(result, drain.PendingDrain)
        assert result.unmeasured_until == expected


def test_note_values_detach_input_and_keep_carry_clear_update_distinct():
    changes = {"blocked_unix": 3., "foreign_unix": None}
    notes = drain.OrdinaryNotes(changes)
    changes["blocked_unix"] = 99.
    assert drain.ordinary_note_fields({"foreign_unix": 1., "epoch_unix": 2.}, notes) == {
        "blocked_unix": 3., "epoch_unix": 2.}
    with pytest.raises(TypeError):
        notes.changes["blocked_unix"] = 4.


def test_real_election_write_is_preserved_by_the_following_drain_count(fleet, monkeypatch):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    holder = publish("incumbent-before-count", timeout_s=1200)
    assert claim() == holder
    waiting = publish("election-before-count", measurement=True, priority=0, cpu=8)
    count = queue.record_pass
    selected = []

    def after_election(key, **kwargs):
        assert key == waiting
        record = json.loads(queue.records.passes_path(key).read_text())
        selected.append(record["measurement_reservation"])
        assert record.get("passes", 0) == 0
        return count(key, **kwargs)

    monkeypatch.setattr(queue, "record_pass", after_election)
    assert claim() is None
    assert len(selected) == 1
    record = json.loads(queue.records.passes_path(waiting).read_text())
    assert record["passes"] == 1
    assert record["measurement_reservation"] == selected[0]
    assert record["measurement_drains"]["sparklina"]["measurement_until_unix"] == (
        denial(waiting)["evidence"]["withhold"]["drain_until_unix"])
    assert queue.ledger().held_keys() == [holder]


@pytest.mark.parametrize("phase,ordinary_left,measurement_left", [
    ("count", True, True), ("ordinary", False, True),
    ("measurement", False, False), ("write", False, False),
])
def test_failure_consumes_only_notes_past_the_existing_write_preparation_point(
        tmp_path, monkeypatch, phase, ordinary_left, measurement_left):
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.records.ensure_layout()
    prior = {}
    if phase == "count":
        prior["first_unix"] = 10 ** 1000
    elif phase == "ordinary":
        prior["blocked_unix"] = 10 ** 1000
    elif phase == "measurement":
        prior["measurement_drains"] = {"host": {"generation": GEN, "blocked_unix": 10 ** 1000}}
    pool._write_json_atomic(queue.records.passes_path(KEY), prior)
    queue._drain_accounting.stage(KEY, drain.OrdinaryNotes({"foreign_unix": 10.}))
    queue._drain_accounting.stage(KEY, drain.MeasurementNotes("host", GEN, {"epoch_unix": 20.}))
    write = pool._write_json_atomic
    if phase == "write":
        def fail_write(*args):
            raise OSError("sidecar unavailable")
        monkeypatch.setattr(pool, "_write_json_atomic", fail_write)
    with pytest.raises((OverflowError, OSError)):
        queue.record_pass(KEY, drain_notes=queue._drain_accounting.for_key(KEY))
    monkeypatch.setattr(pool, "_write_json_atomic", write)
    write(queue.records.passes_path(KEY), {})
    queue.record_pass(KEY, drain_notes=queue._drain_accounting.for_key(KEY))
    record = json.loads(queue.records.passes_path(KEY).read_text())
    assert (record.get("foreign_unix") == 10.) is ordinary_left
    assert ("measurement_drains" in record) is measurement_left


def test_a_holder_released_between_bounds_is_not_counted_from_the_census_snapshot(tmp_path):
    queue = pool.PoolQueue(tmp_path / "queue")
    ledger = queue.ledger()
    ledger.ensure_capacity({"cpu": 4})
    first, departing = "1" * 64, "2" * 64
    assert ledger.acquire(first, {"cpu": 1})
    assert ledger.acquire(departing, {"cpu": 1})
    for _ in range(3):
        queue.record_pass(KEY)
    bound = queue.holder_bound
    observed = []

    def release_after_first(holder, **kwargs):
        observed.append(holder)
        result = bound(holder, **kwargs)
        if holder == first:
            ledger.release(departing)
        return result

    queue.holder_bound = release_after_first
    result = assessment(queue._observe_drain(KEY, ledger=ledger, need={"cpu": 4}, mode="tokens"))
    assert observed == [first]
    assert ledger.available() == {"cpu": 3}
    assert result.verdict["why"] == "holders_cannot_cover"
    result = assessment(queue._observe_drain(KEY, ledger=ledger, need={"cpu": 4}, mode="tokens"))
    assert result.verdict["why"] == "drains_soon"


@pytest.mark.parametrize("sample,gpu_first,foreign", [
    (None, True, False), ({}, False, False),
    ({"sampled_unix": NOW, "foreign_processes": []}, True, False),
    ({"sampled_unix": 1., "foreign_processes": [42]}, False, True),
])
def test_no_gpu_observation_differs_from_invalid_or_foreign_observations(sample, gpu_first, foreign):
    p = prepared(prior={})
    p = replace(p, question=replace(p.question, need={"cpu": 2, "gpu": 1}))
    result = drain.begin_drain(p, {"gpu": 1}, sample)
    assert result.verdict["gpu_first"] is gpu_first
    assert result.verdict["eligible"] is gpu_first
    if foreign:
        assert result.notes.changes["foreign_unix"] == NOW


@pytest.mark.parametrize("declared", [False, True])
def test_retiring_a_measurement_foreign_veto_requires_declared_gpu_attribution(declared):
    prior = {"measurement_drains": {"host": {"generation": GEN, "foreign_unix": NOW-5}}}
    p = prepared(prior=prior, measurement=True, measurement_generation=GEN,
                 measurement_foreign_clear=True)
    p = replace(p, question=replace(p.question, need={"cpu": 2, "gpu": 1}))
    sample = {"sampled_unix": NOW, "foreign_processes": []}
    if declared:
        sample.update(schema="prismabuild.gpu_capacity.v1", complete=True, attributed=True)
    result = drain.begin_drain(p, {"gpu": 1}, sample)
    if declared:
        assert isinstance(result, drain.PendingDrain)
        assert result.notes["foreign_unix"] is None
    else:
        assert assessment(result).verdict["why"] == "foreign_load"


def test_real_election_admission_race_retains_observed_notes_until_later_count(fleet, monkeypatch):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    holder = publish("incumbent-before-election", timeout_s=1200)
    assert claim() == holder
    waiting = publish("election-contender", measurement=True, priority=0, cpu=8)
    canonical = queue._canonical_measurement_verdict
    seen = []

    def contend(item, verdict, **kwargs):
        seen.append(dict(verdict))
        # A second real descriptor holds H after observation, just as a sibling
        # can between the first decision and the election's reacquisition.
        with adaptive_cpu.AdmissionGate(kwargs["ledger"]).locked():
            return canonical(item, verdict, **kwargs)

    monkeypatch.setattr(queue, "_canonical_measurement_verdict", contend)
    assert claim() is None
    assert len(seen) == 1 and seen[0]["why"] == "draining_for_measurement"
    assert queue.passes(waiting) == 0
    assert queue.ledger().held_keys() == [holder]
    assert queue.records.item_path(pool.record_store.READY, waiting).exists()
    queue._drain_accounting.stage(waiting, None)
    queue.record_pass(waiting, drain_notes=queue._drain_accounting.for_key(waiting))
    record = json.loads(queue.records.passes_path(waiting).read_text())
    episode = record["measurement_drains"]["sparklina"]
    assert episode["measurement_until_unix"] == seen[0]["drain_until_unix"]
    assert episode["generation"] == seen[0]["drain_generation"]


def test_initial_history_coercion_failure_precedes_available_and_keeps_pending_notes(tmp_path, monkeypatch):
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.records.ensure_layout()
    ledger = queue.ledger()
    ledger.ensure_capacity({"cpu": 4})
    queue._drain_accounting.stage(KEY, drain.OrdinaryNotes({"foreign_unix": 10.}))
    pool._write_json_atomic(queue.records.passes_path(KEY), {"first_unix": 10 ** 1000})
    reads = []
    available = ledger.available

    def observe_available():
        reads.append(True)
        return available()

    monkeypatch.setattr(ledger, "available", observe_available)
    with pytest.raises(OverflowError):
        queue._observe_drain(KEY, ledger=ledger, need={"cpu": 2}, mode="tokens")
    assert not reads
    pool._write_json_atomic(queue.records.passes_path(KEY), {})
    queue.record_pass(KEY, drain_notes=queue._drain_accounting.for_key(KEY))
    assert json.loads(queue.records.passes_path(KEY).read_text())["foreign_unix"] == 10.
