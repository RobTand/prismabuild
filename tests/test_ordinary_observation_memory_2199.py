"""An ordinary proof's observations survive a sidecar disappearing mid-pass."""
from contextlib import ExitStack, contextmanager
from concurrent.futures import ThreadPoolExecutor

import pytest

from prismabuild import _measurement_reservation as reservation, adaptive_cpu, pool
from test_measurement_drains_gpu_backfill import fleet  # noqa: F401
from test_ordinary_census_admission_2199 import _fixture_census_fence, _fixture_selection
from test_shared_census_safety_2199 import shared_age_clock  # noqa: F401


@pytest.mark.parametrize("higher_first", [False, True])
def test_a_pass_keeps_an_election_observed_by_an_ordinary_proof(
        fleet, monkeypatch, higher_first):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    selected = publish("ordinary-observed-election", measurement=True, priority=10, gpu=0)
    _fixture_selection(queue, selected)
    candidates = [publish(f"ordinary-observed-follower-{i}", priority=-10, gpu=0)
                  for i in range(2)]
    ordered = list(candidates)
    holders = []
    if higher_first:
        holder = publish("ordinary-observed-incumbent", gpu=0)
        queue.ledger().ensure_capacity({"cpu": 20, "mem_gb": 120})
        assert queue.ledger().acquire(holder, {"mem_gb": 30})
        holders.append(holder)
        ordered.insert(0, publish("ordinary-observed-higher", priority=20, gpu=0, mem_gb=100))
    rows = [pool.record_store.value_or_raise(queue.records.item(pool.READY, key))
            for key in ordered]
    monkeypatch.setattr(reservation.CensusReader, "shared_census", lambda self: None)
    monkeypatch.setattr(reservation, "FENCE_WAIT_S", 0)
    original = reservation.candidate_admission_census
    seen = []

    @contextmanager
    def remove_sidecar_between_candidates(*args, **kwargs):
        with original(*args, **kwargs) as current:
            seen.append(args[4]["action_key"])
            yield current
        if len(seen) == 1:
            queue.records.passes_path(selected).unlink()

    monkeypatch.setattr(reservation, "candidate_admission_census", remove_sidecar_between_candidates)
    with _fixture_census_fence(queue):
        result = queue.claim(capacity={"cpu": 20, "mem_gb": 120}, ready=rows)
    assert result is None, "an ordinary proof forgot a live election between candidates"
    assert seen == ordered
    assert all(queue.records.item_path(pool.READY, key).is_file()
               for key in [selected, *ordered])
    assert queue.ledger().held_keys() == holders


@pytest.mark.parametrize("state", ["missing", "retired", "busy", "holder", "sibling"])
def test_observed_elections_require_retirement_authority(
        fleet, monkeypatch, state, shared_age_clock):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    selected = publish("ordinary-control-election", measurement=True, priority=10, gpu=0)
    _fixture_selection(queue, selected)
    candidates = [publish(f"ordinary-control-follower-{i}", gpu=0) for i in range(2)]
    rows = [pool.record_store.value_or_raise(queue.records.item(pool.READY, key))
            for key in candidates]
    ledger = queue.ledger()
    if state in {"busy", "holder"}:
        queue.withdraw(selected)
    if state == "holder":
        ledger.ensure_capacity({"cpu": 20, "mem_gb": 120})
        assert ledger.acquire(selected, {"cpu": 1})
    if state != "sibling":
        monkeypatch.setattr(reservation.CensusReader, "shared_census", lambda self: None)
    monkeypatch.setattr(reservation, "FENCE_WAIT_S", 0)
    original = reservation.candidate_admission_census
    seen = []

    with ExitStack() as fence:
        fence.enter_context(_fixture_census_fence(queue))

        @contextmanager
        def remove_sidecar(*args, **kwargs):
            with original(*args, **kwargs) as current:
                seen.append(args[4]["action_key"])
                yield current
            if len(seen) == 1:
                queue.records.passes_path(selected).unlink()
                if state == "missing":
                    queue.records.item_path(pool.READY, selected).unlink()
                elif state == "retired":
                    queue.withdraw(selected)
                elif state == "sibling":
                    fence.close()
                    with reservation.locked_census(
                            queue, ledger, adaptive_cpu.AdmissionGate(ledger)) as sibling:
                        assert sibling["elections"] == {}
                    fence.enter_context(_fixture_census_fence(queue))

        monkeypatch.setattr(reservation, "candidate_admission_census", remove_sidecar)
        with ExitStack() as held:
            if state == "busy":
                assert held.enter_context(queue.action_transition_lock(selected, blocking=False))
                with ThreadPoolExecutor(max_workers=1) as executor:
                    result = executor.submit(
                        queue.claim, capacity={"cpu": 20, "mem_gb": 120}, ready=rows).result(timeout=30)
            else:
                result = queue.claim(capacity={"cpu": 20, "mem_gb": 120}, ready=rows)

    assert seen == candidates
    if state == "retired":
        assert result is not None and result["action_key"] == candidates[1]
        assert ledger.held_keys() == [candidates[1]]
    else:
        assert result is None
        assert ledger.held_keys() == ([selected] if state == "holder" else [])
        assert all(queue.records.item_path(pool.READY, key).is_file() for key in candidates)


def test_complete_census_fallback_preserves_an_ordinary_observation(fleet, monkeypatch):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    selected = publish("ordinary-full-election", measurement=True, priority=10, gpu=0)
    chosen = _fixture_selection(queue, selected)
    candidate = publish("ordinary-full-follower", gpu=0)
    row = pool.record_store.value_or_raise(queue.records.item(pool.READY, candidate))
    ledger = queue.ledger()
    gate = adaptive_cpu.AdmissionGate(ledger)
    saved = reservation.PassCensus(queue, ledger, gate)
    monkeypatch.setattr(reservation.CensusReader, "shared_census", lambda self: None)
    monkeypatch.setattr(reservation, "FENCE_WAIT_S", 0)
    with _fixture_census_fence(queue):
        with reservation.candidate_admission_census(
                saved, queue, ledger, gate, row, dependent_owner=None) as first:
            assert "unavailable" in first
    queue.records.passes_path(selected).unlink()
    with reservation.candidate_admission_census(
            saved, queue, ledger, gate, row, dependent_owner=None) as current:
        assert reservation.blocking_selection(
            current, row, host=ledger.base.name, funded_by=None) == chosen
    assert saved.census["selections"][selected] == chosen
    assert ledger.held_keys() == []


def test_a_foreign_predecessor_cannot_hide_its_local_successor(
        fleet, monkeypatch, shared_age_clock):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    selected = publish("ordinary-successor-election", measurement=True, priority=10, gpu=0)
    _fixture_selection(queue, selected, host="sparky")
    candidate = publish("ordinary-successor-follower", gpu=0)
    row = pool.record_store.value_or_raise(queue.records.item(pool.READY, candidate))
    ledger = queue.ledger()
    gate = adaptive_cpu.AdmissionGate(ledger)
    saved = reservation.PassCensus(queue, ledger, gate)
    monkeypatch.setattr(reservation, "FENCE_WAIT_S", 0)
    with reservation.candidate_admission_census(
            saved, queue, ledger, gate, row, dependent_owner=None) as foreign:
        assert foreign["elections"] == {}
    queue.withdraw(selected)
    assert publish("ordinary-successor-election", measurement=True, priority=10, gpu=0) == selected
    successor = _fixture_selection(queue, selected)
    with _fixture_census_fence(queue):
        with reservation.candidate_admission_census(
                saved, queue, ledger, gate, row, dependent_owner=None) as blocked:
            assert "unavailable" in blocked
    queue.records.passes_path(selected).unlink()
    with reservation.locked_census(queue, ledger, gate) as sibling:
        assert sibling["elections"] == {}
    with _fixture_census_fence(queue):
        with reservation.candidate_admission_census(
                saved, queue, ledger, gate, row, dependent_owner=None) as current:
            assert reservation.blocking_selection(
                current, row, host=ledger.base.name, funded_by=None) == successor
    assert ledger.held_keys() == []

