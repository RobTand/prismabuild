"""A superseded exact publication ends without adopting successor execution."""
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
    return json.loads(queue.records.item_path(pool.record_store.READY, KEY).read_bytes())


def _assert_superseded(
    queue: pool.PoolQueue, generation: float, successor: float,
) -> None:
    assert pbrun.await_outcome(
        queue, KEY, wait_s=0, generation=generation,
    ) == 76
    row = pbwait.wait_one(
        queue, KEY, cas=pb.PrismaBuildCAS(queue.root / "cas"),
        deadline=time.monotonic(), generation=generation,
        lane_root=queue.root / "slurm",
    )
    assert row["status"] == "superseded"
    assert row["published_unix"] == generation
    assert row["superseded_by_published_unix"] == successor
    assert row["succeeded"] is False
    assert row["returncode"] is None
    assert row["action_returncode"] is None
    assert row["action_signal"] is None
    assert row["receipt_published"] is None
    assert pbwait.verdict([row]) == 76
    landed, watched = pbrun.bounded_outcome_observation(
        queue, KEY, generation, budget_s=5,
    )
    assert landed is not None
    assert watched == generation
    assert pbrun.landed_outcome(queue, KEY, wait_s=0,
                                generation=generation) is not None


def _decision_path(queue: pool.PoolQueue, generation: float):
    identity = {"action_key": KEY, "published_unix": generation}
    return (queue.root / "publication-endings" / KEY
            / f"{queue.records.attempt_generation(identity)}.json")


@pytest.mark.parametrize("ending", ENDINGS)
def test_old_wait_ends_before_and_after_exact_successor_execution(
    queue: pool.PoolQueue, clock: list[float], ending: str,
    capsys: pytest.CaptureFixture[str],
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
    _assert_superseded(queue, original_generation, successor_generation)

    successor = queue.claim(owner="successor", capacity=DEMAND)
    assert successor is not None
    assert successor["published_unix"] == successor_generation
    queue.finish(KEY, status="executed", detail={"stdout": "successor-only\n"},
                 claim_snapshot=successor)

    assert pbcampaign._pool_slot_occupied(queue, KEY) is False
    _assert_superseded(queue, original_generation, successor_generation)
    assert "successor-only" not in capsys.readouterr().out
    landed, watched = pbrun.outcome_poll(queue, KEY, successor_generation)
    assert landed is not None
    assert watched == successor_generation
    assert landed[1]["published_unix"] == successor_generation
    assert landed[1]["status"] == "executed"
    unpinned, _ = pbrun.outcome_poll(queue, KEY, None)
    assert unpinned is not None
    assert unpinned[1]["published_unix"] == successor_generation
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

    assert not _decision_path(queue, generation).exists()
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
    assert not _decision_path(queue, original["published_unix"]).exists()


def test_exact_withdrawal_still_ends_the_original_wait_after_republication(
    queue: pool.PoolQueue, clock: list[float],
) -> None:
    _publish(queue)
    original = queue.claim(owner="original", capacity=DEMAND)
    assert original is not None
    queue.withdraw(KEY, reason="operator decision", signal_child=False)
    clock[0] += 1
    _publish(queue)
    _end_claim(queue, original, "finish_retry", clock)

    assert pbrun.await_outcome(queue, KEY, wait_s=0,
                              generation=original["published_unix"]) == 143
    assert not _decision_path(queue, original["published_unix"]).exists()
    assert pbcampaign._pool_slot_occupied(queue, KEY) is True


def test_exact_publication_ending_outranks_an_unstamped_legacy_terminal(
    queue: pool.PoolQueue, clock: list[float],
) -> None:
    _publish(queue)
    legacy = queue.claim(owner="legacy", capacity=DEMAND)
    assert legacy is not None
    queue.finish(KEY, status="executed", claim_snapshot=legacy)
    attempt = queue.records.attempt_path(legacy, 1)
    attempt_bytes = attempt.read_bytes()
    path = queue.records.item_path(pool.record_store.DONE, KEY)
    retained = json.loads(path.read_bytes())
    legacy_record = {name: retained[name] for name in (
        "schema", "action_key", "status", "finished_unix", "finished_host",
        "attempts", "detail",
    )}
    path.write_text(json.dumps(legacy_record))
    assert pbrun.await_outcome(queue, KEY, wait_s=0,
                              generation=legacy["published_unix"] + 100) == 0
    clock[0] += 1
    _publish(queue)
    original = queue.claim(owner="original", capacity=DEMAND)
    assert original is not None
    clock[0] += 1
    _publish(queue)
    successor = _ready(queue)["published_unix"]
    _end_claim(queue, original, "finish_retry", clock)

    assert attempt.read_bytes() == attempt_bytes
    archived = queue.archived_generation_outcomes(KEY, generation=legacy["published_unix"])
    assert len(archived) == 1
    assert archived[0][1]["published_unix"] == legacy["published_unix"]
    _assert_superseded(queue, original["published_unix"], successor)


@pytest.mark.parametrize("damage", [
    "not_json", "empty_object", "wrong_key", "wrong_schema", "same_generation",
    "older_generation", "missing_generation", "boolean_generation",
    "nonfinite_generation", "bad_resources", "symlink",
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
    ready = queue.records.item_path(pool.record_store.READY, KEY)
    if damage == "symlink":
        target = queue.root / "successor-target.json"
        target.write_bytes(raw)
        ready.unlink()
        ready.symlink_to(target)
    else:
        ready.write_bytes(raw)

    try:
        _end_claim(queue, original, "finish_retry", clock)
    except (OSError, pool.PoolContractError, pb.PrismaBuildError):
        pass

    assert pbrun.outcome_poll(queue, KEY, generation) == (None, generation)
    assert not _decision_path(queue, generation).exists()
    assert queue.records.item_path(pool.record_store.READY, KEY).read_bytes() == raw
    if damage != "same_generation":
        retained = [*queue.records.dir(pool.record_store.CLAIMED).glob(f"{KEY}.json"),
                    *queue.records.dir(pool.record_store.CLAIMED).glob(f"{KEY}.*{pool.record_store.TOMBSTONE_SUFFIX}")]
        assert len(retained) == 1
        assert json.loads(retained[0].read_bytes())["published_unix"] == generation


def test_launcher_exit_seventy_six_cannot_impersonate_publication_supersession(
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
                              generation=original["published_unix"]) == 1
    assert "which is one of pbrun's own exit codes" in capsys.readouterr().err
