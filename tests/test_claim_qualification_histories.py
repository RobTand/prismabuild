"""Qualification failures retain their real row, diagnostic and holder state."""
import sys

import pytest

from prismabuild import local_scratch, pool
from test_local_scratch_measured_placement import FAST, SLOW, fleet as scratch_fleet
from test_measurement_drains_gpu_backfill import fleet


def _rewrite(queue, key, **changes):
    path = queue.item_path(pool.READY, key)
    row = pool.read_queue_record(path)
    row.update(changes)
    pool._write_json_atomic(path, row)
    return path.read_bytes()


def _assert_unclaimed(queue, key, ready_bytes):
    assert queue.item_path(pool.READY, key).read_bytes() == ready_bytes
    assert not queue.item_path(pool.CLAIMED, key).exists()
    assert not queue.lease_path(key).exists()
    assert queue.ledger().holder_tokens(key) == {}
    assert queue.tier_holdings(key) == {}
    assert queue.passes(key) == 0


@pytest.mark.parametrize("fault,reason,cause_type", [
    ("demand", "malformed_demand", ValueError),
    ("tier", "malformed_tier_demand", ValueError),
    ("residency", "residency_lead_record_unreadable", pool.PoolContractError),
    ("scratch", "local_scratch_io_intent_invalid", pool.PoolContractError),
], ids=["demand", "tier", "residency", "scratch"])
def test_persisted_handler_denial_keeps_its_active_exception_and_holder(
    fleet, monkeypatch, fault, reason, cause_type,
):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    holder = publish("context-holder", gpu=0)
    assert claim() == holder
    holder_bytes = queue.item_path(pool.CLAIMED, holder).read_bytes()
    lease_bytes = queue.lease_path(holder).read_bytes()
    available = queue.ledger().available()
    candidate = publish("context-" + fault, gpu=0, priority=0)
    if fault == "demand":
        ready_bytes = _rewrite(queue, candidate, resources={"cpu": "unreadable-count"})
    elif fault == "tier":
        ready_bytes = _rewrite(queue, candidate, resources={"cpu": 2, "stage_gib@bad/slash": 1})
    elif fault == "residency":
        lead = "a" * 64
        queue.item_path(pool.DONE, lead).write_bytes(b'{"status": "executed"')
        ready_bytes = _rewrite(queue, candidate, residency={
            "schema": pool.RESIDENCY_SCHEMA_V1, "manifest_sha256": "b" * 64,
            "manifest_bytes": 1, "leads": [lead]})
    else:
        ready_bytes = _rewrite(queue, candidate, **{local_scratch.IO_ENV: {"forged": True}})
    original = queue.record_denial
    publication_error = OSError("diagnostic transport failed after persistence")

    def fail_after_record(item, actual_reason, evidence=None):
        if item["action_key"] == candidate and actual_reason == reason:
            assert isinstance(sys.exception(), cause_type)
            original(item, actual_reason, evidence)
            raise publication_error
        return original(item, actual_reason, evidence)

    with monkeypatch.context() as faulted:
        faulted.setattr(queue, "record_denial", fail_after_record)
        with pytest.raises(OSError) as caught:
            claim()
    assert caught.value is publication_error
    assert isinstance(caught.value.__context__, cause_type)
    persisted = denial(candidate)
    assert persisted["reason"] == reason
    assert persisted["evidence"]["error"] == str(caught.value.__context__)
    _assert_unclaimed(queue, candidate, ready_bytes)
    assert queue.item_path(pool.CLAIMED, holder).read_bytes() == holder_bytes
    assert queue.lease_path(holder).read_bytes() == lease_bytes

    assert queue.ledger().available() == available
    assert set(queue.ledger().held_keys()) == {holder}
    assert queue.ledger().holder_tokens(holder) == {"cpu": 2, "mem_gb": 8}


def test_interpreter_refusal_skips_later_invalid_demand_and_claims_the_next_real_row(fleet):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    candidate = publish("missing-interpreter-before-demand", gpu=0, priority=0)
    ready_bytes = _rewrite(queue, candidate,
                           interpreter="/prismabuild-fixture-missing/interpreter",
                           resources={"cpu": "unreadable-count"})
    good = publish("good-after-interpreter-refusal", gpu=0)
    assert claim() == good
    assert denial(candidate)["reason"] == "interpreter_not_present"
    _assert_unclaimed(queue, candidate, ready_bytes)
    claimed = pool.read_queue_record(queue.item_path(pool.CLAIMED, good))
    lease = pool.read_queue_record(queue.lease_path(good))
    assert claimed["action_key"] == good
    assert lease["action_key"] == good
    assert set(queue.ledger().held_keys()) == {good}
    assert queue.ledger().holder_tokens(good) == {"cpu": 2, "mem_gb": 8}


def test_unreadable_residency_carries_a_real_measurement_drain_across_two_polls(fleet):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    holder = publish("residency-carry-holder")
    assert claim() == holder
    holder_bytes = queue.item_path(pool.CLAIMED, holder).read_bytes()
    lease_bytes = queue.lease_path(holder).read_bytes()
    waiting = publish("residency-carry-measurement", measurement=True, pinned=True)
    assert claim() is None
    first = denial(waiting)
    assert first["reason"] == "adaptive_cpu_refused_withholding"
    until = first["evidence"]["withhold"]["drain_until_unix"]
    lead = "c" * 64
    queue.item_path(pool.DONE, lead).write_bytes(b'{"status": "executed"')
    waiting_bytes = _rewrite(queue, waiting, residency={
        "schema": pool.RESIDENCY_SCHEMA_V1, "manifest_sha256": "d" * 64,
        "manifest_bytes": 1, "leads": [lead]})
    later = publish("cpu-after-unreadable-measurement", gpu=0)
    later_bytes = queue.item_path(pool.READY, later).read_bytes()
    for _ in range(2):
        assert claim() is None
        current = denial(waiting)
        assert current["reason"] == "residency_lead_record_unreadable"
        assert current["evidence"]["withhold_carried"]["reason"] == first["reason"]
        assert current["evidence"]["withhold_carried"]["drain_until_unix"] == until
        assert denial(later)["reason"] == "deferred_behind_withheld_row"
        assert denial(later)["evidence"]["withheld_for"] == waiting
        assert queue.item_path(pool.READY, waiting).read_bytes() == waiting_bytes
        assert queue.item_path(pool.READY, later).read_bytes() == later_bytes
    assert queue.passes(waiting) == 1
    _assert_unclaimed(queue, later, later_bytes)
    assert not queue.item_path(pool.CLAIMED, waiting).exists()
    assert not queue.lease_path(waiting).exists()
    assert queue.ledger().holder_tokens(waiting) == {}
    assert set(queue.ledger().held_keys()) == {holder}
    assert queue.item_path(pool.CLAIMED, holder).read_bytes() == holder_bytes
    assert queue.lease_path(holder).read_bytes() == lease_bytes


def test_scratch_yield_publication_failure_is_caught_before_handler_publication(
    scratch_fleet, monkeypatch,
):
    scratch_fleet.publish()
    queue, key = scratch_fleet.queue, scratch_fleet.key
    ready_bytes = queue.item_path(pool.READY, key).read_bytes()
    original = queue.record_denial
    yield_error = OSError("scratch yield diagnostic failed after persistence")
    handler_error = OSError("scratch handler diagnostic failed after persistence")

    def fail_after_record(item, reason, evidence=None):
        if reason == "deferred_for_local_scratch_io":
            assert sys.exception() is None
            assert evidence["yield_to"] == FAST
            original(item, reason, evidence)
            raise yield_error
        if reason == "local_scratch_io_profile_invalid":
            assert sys.exception() is yield_error
            original(item, reason, evidence)
            raise handler_error
        return original(item, reason, evidence)

    with monkeypatch.context() as faulted:
        faulted.setattr(queue, "record_denial", fail_after_record)
        with pytest.raises(OSError) as caught:
            scratch_fleet.claim(SLOW)
    assert caught.value is handler_error
    assert caught.value.__context__ is yield_error
    persisted = scratch_fleet.denial(SLOW)
    assert persisted["reason"] == "local_scratch_io_profile_invalid"
    assert persisted["evidence"] == {"error": str(yield_error)}
    assert queue.item_path(pool.READY, key).read_bytes() == ready_bytes
    assert queue.tier_holdings(key) == {}
    scratch_fleet.ready_unspent()


def test_scratch_yield_publication_preserves_an_uncaught_error_and_diagnostic(
    scratch_fleet, monkeypatch,
):
    scratch_fleet.publish()
    queue, key = scratch_fleet.queue, scratch_fleet.key
    ready_bytes = queue.item_path(pool.READY, key).read_bytes()
    original = queue.record_denial
    publication_error = RuntimeError("scratch diagnostic transport failed after persistence")

    def fail_after_record(item, reason, evidence=None):
        original(item, reason, evidence)
        if reason == "deferred_for_local_scratch_io":
            raise publication_error

    with monkeypatch.context() as faulted:
        faulted.setattr(queue, "record_denial", fail_after_record)
        with pytest.raises(RuntimeError) as caught:
            scratch_fleet.claim(SLOW)
    assert caught.value is publication_error
    assert caught.value.__context__ is None
    persisted = scratch_fleet.denial(SLOW)
    assert persisted["reason"] == "deferred_for_local_scratch_io"
    assert persisted["evidence"]["yield_to"] == FAST
    assert queue.item_path(pool.READY, key).read_bytes() == ready_bytes
    assert queue.tier_holdings(key) == {}
    scratch_fleet.ready_unspent()


def test_scratch_root_replacement_after_placement_keeps_boundary_exception_context(
    scratch_fleet, monkeypatch,
):
    scratch_fleet.publish()
    queue, key = scratch_fleet.queue, scratch_fleet.key
    ready_bytes = queue.item_path(pool.READY, key).read_bytes()
    placement, record = queue._scratch_placement, queue.record_denial
    publication_error = OSError("scratch boundary diagnostic failed after persistence")

    def replace_root_after_placement(*args, **kwargs):
        evidence, yielding = placement(*args, **kwargs)
        assert evidence["selected_host"] == FAST
        assert not yielding
        scratch_fleet.devices[FAST]["root_inode"] += 1
        return evidence, yielding

    def fail_after_record(item, reason, evidence=None):
        if reason == "local_scratch_io_profile_invalid":
            assert isinstance(sys.exception(), local_scratch.LocalScratchError)
            assert str(sys.exception()) == "scratch profile/root changed before admission"
            record(item, reason, evidence)
            raise publication_error
        return record(item, reason, evidence)

    with monkeypatch.context() as faulted:
        faulted.setattr(queue, "_scratch_placement", replace_root_after_placement)
        faulted.setattr(queue, "record_denial", fail_after_record)
        with pytest.raises(OSError) as caught:
            scratch_fleet.claim(FAST)
    assert caught.value is publication_error
    assert isinstance(caught.value.__context__, local_scratch.LocalScratchError)
    assert str(caught.value.__context__) == "scratch profile/root changed before admission"
    persisted = scratch_fleet.denial(FAST)
    assert persisted["reason"] == "local_scratch_io_profile_invalid"
    assert persisted["evidence"] == {"error": "scratch profile/root changed before admission"}
    assert queue.item_path(pool.READY, key).read_bytes() == ready_bytes
    assert queue.tier_holdings(key) == {}
    scratch_fleet.ready_unspent()
