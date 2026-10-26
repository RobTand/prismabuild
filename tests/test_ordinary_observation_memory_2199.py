"""An ordinary proof's observations survive a sidecar disappearing mid-pass."""
from contextlib import contextmanager

import pytest

from prismabuild import _measurement_reservation as reservation, pool
from test_measurement_drains_gpu_backfill import fleet  # noqa: F401
from test_ordinary_census_admission_2199 import _fixture_census_fence, _fixture_selection


@pytest.mark.parametrize("higher_between", [False, True])
def test_a_pass_keeps_an_election_observed_by_an_ordinary_proof(
        fleet, monkeypatch, higher_between):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    selected = publish("ordinary-observed-election", measurement=True, priority=10, gpu=0)
    _fixture_selection(queue, selected)
    candidates = [publish(f"ordinary-observed-follower-{i}", priority=-10, gpu=0)
                  for i in range(2)]
    ordered = list(candidates)
    if higher_between:
        ordered.insert(1, publish("ordinary-observed-higher", priority=20, gpu=0, cpu=21))
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
    assert queue.ledger().held_keys() == []

