"""Execution slots and IPC branches cannot impersonate a publication ending."""
from __future__ import annotations

import json
import time

import pytest

from prismabuild import core as pb, pool
import pbrun
import pbwait

from test_pool_requeue_preserves_newer_generation import (
    DEMAND, KEY, _end_claim, _publish, clock, queue,
)


def _legacy_slot(queue, clock, damage):
    _publish(queue)
    executed = queue.claim(owner="executed", capacity=DEMAND)
    assert executed is not None
    queue.finish(KEY, status="executed", detail={"returncode": 0}, claim_snapshot=executed)
    attempt = queue.records.attempt_path(executed, 1)
    attempt_bytes = attempt.read_bytes()
    slot = queue.records.item_path(pool.record_store.DONE, KEY)
    retained = json.loads(slot.read_bytes())
    legacy = {name: retained[name] for name in (
        "schema", "action_key", "status", "finished_unix", "finished_host",
        "attempts", "detail",
    )}
    if damage in {"status", "both"}:
        legacy["status"] = "superseded"
    if damage in {"schema", "both"}:
        legacy["schema"] = "prismabuild.publication_superseded.v1"
    slot.write_text(json.dumps(legacy))
    clock[0] += 1
    _publish(queue)
    watched = json.loads(queue.records.item_path(pool.record_store.READY, KEY).read_bytes())["published_unix"]
    return executed, watched, attempt, attempt_bytes


def _observe(queue, generation, consumer):
    if consumer == "pbrun":
        return pbrun.await_outcome(queue, KEY, wait_s=0, generation=generation), None
    row = pbwait.wait_one(
        queue, KEY, cas=pb.PrismaBuildCAS(queue.root / "cas"),
        deadline=time.monotonic(), generation=generation,
        lane_root=queue.root / "slurm")
    return pbwait.verdict([row]), row


@pytest.mark.parametrize("consumer", ["pbrun", "pbwait"])
@pytest.mark.parametrize("damage", ["status", "schema", "both"])
def test_untyped_terminal_is_unavailable_instead_of_execution_or_supersession(
    queue, clock, consumer, damage,
):
    executed, watched, attempt, attempt_bytes = _legacy_slot(queue, clock, damage)
    archived = queue.archived_generation_outcomes(KEY, generation=executed["published_unix"])
    assert len(archived) == 1
    assert archived[0][1]["status"] == "executed"

    code, row = _observe(queue, watched, consumer)

    assert code == 74
    if row is not None:
        assert row["status"] == "record_error"
        assert row["succeeded"] is False
    assert attempt.read_bytes() == attempt_bytes
    assert not list((queue.root / "publication-endings").glob(f"{KEY}/*.json"))


@pytest.mark.parametrize("consumer", ["pbrun", "pbwait"])
def test_exact_immutable_execution_keeps_precedence_over_the_bad_legacy_slot(
    queue, clock, consumer,
):
    executed, _, attempt, attempt_bytes = _legacy_slot(queue, clock, "both")

    code, row = _observe(queue, executed["published_unix"], consumer)

    assert code == 0
    if row is not None:
        assert row["status"] == "executed"
    assert attempt.read_bytes() == attempt_bytes


@pytest.mark.parametrize("consumer", ["pbrun", "pbwait"])
def test_valid_legacy_execution_fallback_remains_readable(queue, clock, consumer):
    _, watched, attempt, attempt_bytes = _legacy_slot(queue, clock, None)

    code, row = _observe(queue, watched, consumer)

    assert code == 0
    if row is not None:
        assert row["status"] == "executed"
    assert attempt.read_bytes() == attempt_bytes


@pytest.mark.parametrize("consumer", ["pbrun", "pbwait"])
def test_publication_proof_in_the_execution_ipc_branch_is_unavailable(
    queue, clock, monkeypatch, consumer,
):
    _publish(queue)
    original = queue.claim(owner="original", capacity=DEMAND)
    assert original is not None
    clock[0] += 1
    _publish(queue)
    successor = json.loads(queue.records.item_path(pool.record_store.READY, KEY).read_bytes())
    _end_claim(queue, original, "finish_retry", clock)
    decision = queue.publication_ending(KEY, generation=original["published_unix"])
    assert decision is not None
    raw = decision.path.read_bytes()
    outcome = {"generation": original["published_unix"],
               "landed": {"path": str(decision.path), "record": json.loads(raw)}}
    payload = outcome if consumer == "pbrun" else {
        "found": {"transport": "pool", "generation": successor["published_unix"],
                  "submission": successor},
        "outcome": outcome, "observed_generation": original["published_unix"],
        "action": None, "receipt_published": False,
    }
    monkeypatch.setattr(pbrun, "_bounded_pool_read", lambda *args, **kwargs: payload)

    with pytest.raises(pbrun.OutcomeReadUnavailable):
        if consumer == "pbrun":
            pbrun.bounded_outcome_observation(
                queue, KEY, original["published_unix"], budget_s=5)
        else:
            pbwait.bounded_observation(queue, KEY, pb.PrismaBuildCAS(queue.root / "cas"),
                                       original["published_unix"], lane_root=queue.root / "slurm")

    assert decision.path.read_bytes() == raw


@pytest.mark.parametrize("renderer", ["summary", "bounded", "pbwait"])
def test_direct_execution_renderers_reject_a_publication_marker(queue, clock, renderer):
    _, _, attempt, attempt_bytes = _legacy_slot(queue, clock, "both")
    slot = queue.records.item_path(pool.record_store.DONE, KEY)
    record = json.loads(slot.read_bytes())

    with pytest.raises(pbrun.OutcomeReadUnavailable):
        if renderer == "summary":
            pbrun.outcome_summary(queue, slot, record)
        elif renderer == "bounded":
            pbrun.bounded_outcome_render(queue, slot, record, budget_s=5)
        else:
            pbwait._from_ending(queue, (slot, record))

    assert attempt.read_bytes() == attempt_bytes


def test_verification_ipc_cannot_report_a_publication_status_as_execution(
    queue, clock, monkeypatch,
):
    executed, _, _, _ = _legacy_slot(queue, clock, None)
    path, record = queue.archived_generation_outcomes(KEY, generation=executed["published_unix"])[0]
    summary = pbrun.outcome_summary(queue, path, record)
    payload = {"summary": {**summary, "status": "superseded"}, "attempts": []}
    monkeypatch.setattr(pbrun, "_bounded_pool_read", lambda *args, **kwargs: payload)

    with pytest.raises(pbrun.OutcomeReadUnavailable):
        pbrun.bounded_outcome_render(queue, path, record, budget_s=5)
