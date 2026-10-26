"""Optional shared verification must not suppress an independent ordinary proof."""
import pytest

from prismabuild import _measurement_reservation as reservation, adaptive_cpu, pool
from test_measurement_drains_gpu_backfill import fleet  # noqa: F401
from test_ordinary_census_admission_2199 import _fixture_census_fence, _fixture_selection
from test_shared_census_safety_2199 import shared_age_clock  # noqa: F401


@pytest.mark.parametrize("election", [False, True])
def test_a_busy_optional_reader_falls_back_to_the_ordinary_proof(
        fleet, monkeypatch, shared_age_clock, election):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    candidate = publish("optional-source-follower", priority=-10, gpu=0)
    ledger = queue.ledger()
    with reservation.locked_census(queue, ledger, adaptive_cpu.AdmissionGate(ledger)):
        pass
    observer = reservation.CensusReader(queue, ledger)
    assert observer.shared_census() is not None
    if election:
        selected = publish("optional-source-election", measurement=True, priority=10, gpu=0)
        _fixture_selection(queue, selected)
    row = pool.record_store.value_or_raise(queue.records.item(pool.READY, candidate))
    monkeypatch.setattr(reservation, "FENCE_WAIT_S", 0)
    with reservation._SourceGenerationReader(observer).held(), _fixture_census_fence(queue):
        result = queue.claim(capacity={"cpu": 20, "mem_gb": 120}, ready=[row])
    if election:
        assert result is None
        assert ledger.held_keys() == []
    else:
        assert result is not None and result["action_key"] == candidate
        assert ledger.held_keys() == [candidate]


def test_unsafe_local_rendezvous_still_refuses_ordinary_admission(
        fleet, monkeypatch, shared_age_clock):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    candidate = publish("unsafe-optional-source-follower", gpu=0)
    ledger = queue.ledger()
    with reservation.locked_census(queue, ledger, adaptive_cpu.AdmissionGate(ledger)):
        pass
    observer = reservation.CensusReader(queue, ledger)
    row = pool.record_store.value_or_raise(queue.records.item(pool.READY, candidate))
    monkeypatch.setattr(reservation, "FENCE_WAIT_S", 0)
    with _fixture_census_fence(queue):
        def untrusted_storage(descriptor):
            raise reservation.local_scratch.LocalScratchError("fixture local storage is untrusted")

        monkeypatch.setattr(reservation.local_scratch, "_descriptor_state_identity", untrusted_storage)
        result = queue.claim(capacity={"cpu": 20, "mem_gb": 120}, ready=[row])
    assert result is None
    assert ledger.held_keys() == []
