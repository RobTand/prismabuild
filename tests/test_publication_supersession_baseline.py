"""Freeze exact waiter behavior before B16 adds a publication ending."""
from __future__ import annotations

import json
import time

import pytest

from prismabuild import core as pb, pool
import pbcampaign
import pbrun
import pbwait

from test_pool_requeue_preserves_newer_generation import (
    DEMAND, ENDINGS, KEY, _end_claim, _publish, clock, queue,
)


def _ready(queue: pool.PoolQueue) -> dict:
    return json.loads(queue.item_path(pool.READY, KEY).read_bytes())


def _assert_exact_wait_has_no_ending(queue: pool.PoolQueue, generation: float) -> None:
    assert pbrun.landed_outcome(queue, KEY, wait_s=0, generation=generation) is None
    assert pbrun.bounded_outcome_observation(
        queue, KEY, generation, budget_s=5,
    ) == (None, generation)
    assert pbrun.await_outcome(
        queue, KEY, wait_s=0, generation=generation,
    ) == 75
    row = pbwait.wait_one(
        queue, KEY, cas=pb.PrismaBuildCAS(queue.root / "cas"),
        deadline=time.monotonic(), generation=generation,
        lane_root=queue.root / "slurm",
    )
    assert row["status"] == "waiting"
    assert row["succeeded"] is False
    assert row["returncode"] is None


@pytest.mark.parametrize("ending", ENDINGS)
def test_old_wait_stays_pending_before_and_after_exact_successor_execution(
    queue: pool.PoolQueue, clock: list[float], ending: str,
) -> None:
    _publish(queue)
    original = queue.claim(owner="original", capacity=DEMAND)
    assert original is not None
    original_generation = original["published_unix"]
    clock[0] += 1
    _publish(queue)
    successor_generation = _ready(queue)["published_unix"]
    assert successor_generation == original_generation + 1

    _end_claim(queue, original, ending, clock)

    assert pbcampaign._pool_slot_occupied(queue, KEY) is True
    _assert_exact_wait_has_no_ending(queue, original_generation)

    successor = queue.claim(owner="successor", capacity=DEMAND)
    assert successor is not None
    assert successor["published_unix"] == successor_generation
    queue.finish(KEY, status="executed", detail={"stdout": "successor-only\n"},
                 claim_snapshot=successor)

    assert pbcampaign._pool_slot_occupied(queue, KEY) is False
    _assert_exact_wait_has_no_ending(queue, original_generation)
    landed, watched = pbrun.outcome_poll(queue, KEY, successor_generation)
    assert landed is not None
    assert watched == successor_generation
    assert landed[1]["published_unix"] == successor_generation
    assert landed[1]["status"] == "executed"
    assert pbrun.await_outcome(queue, KEY, wait_s=0,
                              generation=successor_generation) == 0


@pytest.mark.parametrize("ending", ENDINGS)
def test_same_generation_retry_remains_the_original_wait(
    queue: pool.PoolQueue, clock: list[float], ending: str,
) -> None:
    _publish(queue)
    original = queue.claim(owner="original", capacity=DEMAND)
    assert original is not None
    generation = original["published_unix"]

    _end_claim(queue, original, ending, clock)

    assert _ready(queue)["published_unix"] == generation
    assert pbrun.outcome_poll(queue, KEY, generation) == (None, generation)
    retry = queue.claim(owner="retry", capacity=DEMAND)
    assert retry is not None
    queue.finish(KEY, status="executed", claim_snapshot=retry)
    landed, watched = pbrun.outcome_poll(queue, KEY, generation)
    assert landed is not None
    assert watched == generation
    assert landed[1]["published_unix"] == generation
    assert landed[1]["status"] == "executed"


def test_an_original_execution_ending_remains_its_own_exact_answer(
    queue: pool.PoolQueue, clock: list[float],
) -> None:
    _publish(queue)
    original = queue.claim(owner="original", capacity=DEMAND)
    assert original is not None
    queue.finish(KEY, status="executed", detail={"stdout": "original-only\n"},
                 claim_snapshot=original)
    clock[0] += 1
    _publish(queue)
    successor = queue.claim(owner="successor", capacity=DEMAND)
    assert successor is not None
    queue.finish(KEY, status="executed", detail={"stdout": "successor-only\n"},
                 claim_snapshot=successor)

    landed, watched = pbrun.outcome_poll(queue, KEY, original["published_unix"])

    assert landed is not None
    assert watched == original["published_unix"]
    assert landed[1]["published_unix"] == original["published_unix"]
    summary = pbrun.outcome_summary(queue, *landed)
    assert summary["detail"]["stdout"] == "original-only\n"


@pytest.mark.parametrize("damage", [
    "not_json", "empty_object", "wrong_key", "wrong_schema", "same_generation",
    "older_generation", "missing_generation", "boolean_generation",
    "nonfinite_generation", "bad_resources",
])
def test_uncertain_successor_does_not_make_an_exact_wait_follow_another_row(
    queue: pool.PoolQueue, clock: list[float], damage: str,
) -> None:
    _publish(queue)
    original = queue.claim(owner="original", capacity=DEMAND)
    assert original is not None
    generation = original["published_unix"]
    clock[0] += 1
    _publish(queue)
    candidate = _ready(queue)
    if damage == "not_json":
        raw = b"{unfinished"
    elif damage == "empty_object":
        raw = b"{}"
    else:
        if damage == "wrong_key":
            candidate["action_key"] = "0" * 64
        elif damage == "wrong_schema":
            candidate["schema"] = pool.POOL_OUTCOME_SCHEMA_V1
        elif damage == "same_generation":
            candidate["published_unix"] = generation
        elif damage == "older_generation":
            candidate["published_unix"] = generation - 1
        elif damage == "missing_generation":
            del candidate["published_unix"]
        elif damage == "boolean_generation":
            candidate["published_unix"] = True
        elif damage == "nonfinite_generation":
            candidate["published_unix"] = float("inf")
        elif damage == "bad_resources":
            candidate["resources"] = "unreadable demand"
        raw = json.dumps(candidate).encode()
    queue.item_path(pool.READY, KEY).write_bytes(raw)

    _end_claim(queue, original, "finish_retry", clock)

    assert pbrun.outcome_poll(queue, KEY, generation) == (None, generation)


def test_launcher_exit_seventy_six_is_currently_an_action_status(
    queue: pool.PoolQueue, clock: list[float], capsys: pytest.CaptureFixture[str],
) -> None:
    _publish(queue)
    original = queue.claim(owner="original", capacity=DEMAND)
    assert original is not None
    queue.finish(KEY, status="failed", detail={"returncode": 76},
                 claim_snapshot=original)
    retry = queue.claim(owner="retry", capacity=DEMAND)
    assert retry is not None
    queue.finish(KEY, status="failed", detail={"returncode": 76},
                 claim_snapshot=retry)

    assert pbrun.await_outcome(queue, KEY, wait_s=0,
                              generation=original["published_unix"]) == 76
    assert "which is one of pbrun's own exit codes" not in capsys.readouterr().err
