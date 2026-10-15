"""Untrusted IPC and recovery cannot turn supersession into execution or follow."""
from __future__ import annotations

import json

import pytest

from prismabuild import core as pb, pool
import pbcanary
import pbrun
import pbwait

from test_pool_requeue_preserves_newer_generation import (
    DEMAND, KEY, _end_claim, clock, queue,
)
from test_pool_publication_endings import _collision


def _ended(queue, clock):
    original, successor = _collision(queue, clock)
    _end_claim(queue, original, "finish_retry", clock)
    ending = queue.publication_ending(KEY, generation=original["published_unix"])
    assert ending is not None
    return original, successor, ending


@pytest.mark.parametrize("consumer", ["pbrun", "pbwait"])
@pytest.mark.parametrize("damage", [
    "missing_proof", "wrong_original", "changed_generation", "no_generation", "mixed_execution",
])
def test_malformed_ipc_refuses_without_following_or_changing_queue_state(
    queue, clock, monkeypatch, consumer, damage,
):
    original, successor, ending = _ended(queue, clock)
    generation = original["published_unix"]
    proof = json.loads(ending.path.read_bytes())
    wire = {"generation": generation, "landed": None,
            "superseded": {"path": str(ending.path), "record": proof}}
    if damage == "missing_proof":
        del proof["successor"]
    elif damage == "wrong_original":
        proof["published_unix"] = successor["published_unix"]
    elif damage == "changed_generation":
        wire["generation"] = successor["published_unix"]
    elif damage == "no_generation":
        wire["generation"] = None
    else:
        claim = queue.claim(owner="successor", capacity=DEMAND)
        assert claim is not None
        path = queue.finish(KEY, status="executed", claim_snapshot=claim)
        wire["landed"] = {"path": str(path), "record": json.loads(path.read_bytes())}
    decision_bytes = ending.path.read_bytes()
    held = queue.ledger().held()
    payload = wire if consumer == "pbrun" else {
        "found": {"transport": "pool", "generation": successor["published_unix"],
                  "submission": successor},
        "outcome": wire, "observed_generation": generation,
        "action": None, "receipt_published": False,
    }
    monkeypatch.setattr(pbrun, "_bounded_pool_read", lambda *args, **kwargs: payload)

    with pytest.raises(pbrun.OutcomeReadUnavailable):
        if consumer == "pbrun":
            pbrun.bounded_outcome_observation(queue, KEY, generation, budget_s=5)
        else:
            pbwait.bounded_observation(
                queue, KEY, pb.PrismaBuildCAS(queue.root / "cas"), generation,
                lane_root=queue.root / "slurm")

    assert ending.path.read_bytes() == decision_bytes
    assert queue.ledger().held() == held


def test_last_snapshot_can_report_supersession_without_attempt_rendering(
    queue, clock, monkeypatch,
):
    original, _, _ = _ended(queue, clock)
    read = pbrun._bounded_pool_read
    observations = 0

    def unavailable_once(section, *args, **kwargs):
        nonlocal observations
        if section == "pool outcome verification":
            pytest.fail("a publication ending entered execution verification")
        observations += 1
        if observations == 1:
            raise pbrun.OutcomeReadUnavailable("initial reader unavailable")
        return read(section, *args, **kwargs)

    monkeypatch.setattr(pbrun, "_bounded_pool_read", unavailable_once)

    assert pbrun.await_outcome(queue, KEY, wait_s=0,
                              generation=original["published_unix"]) == 76
    assert observations == 2


def test_staging_canary_uses_no_execution_evidence_from_publication_ending(
    queue, clock, monkeypatch,
):
    original, _, _ = _ended(queue, clock)

    def unexpected_render(*args, **kwargs):
        pytest.fail("canary tried to render a superseded publication as an attempt")

    monkeypatch.setattr(pbrun, "bounded_outcome_render", unexpected_render)

    assert pbcanary.staging_wait_evidence(
        {"queue_root": str(queue.root)}, KEY, original["published_unix"],
    ) is None
