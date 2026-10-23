"""The worker's withdrawal and terminal guards read their own key by name.

``withdrawn``, ``done`` and ``failed`` grow with the queue's whole history:
4,092, 87,440 and 24,978 names on 2026-10-20.  The guards below asked
whether one key was in them by listing them: a running action's withdrawal
poll every 2 s (about 55 listings/s with 110 actions running), every claim
scan and claim commit, every 30 s reap, and each submission's live-withdrawal
check.  Each now looks up ``<key>.json`` in each directory after one open of
it, which is what revalidates this NFS client's cached misses (#808).

The listing tests count ``os.listdir``/``os.scandir`` on those directories
with the audit hook from ``test_a_waiting_client_reads_its_ending_by_name``.
The cache test is that module's negative-lookup model: a withdrawal another
box files after this one looked is still seen on the next look.  Loudness on
an unreadable directory is pinned by the ``test_an_unreadable_*`` modules.
"""
from __future__ import annotations

from admitted_queue_fixture import simulated_capacity

from contextlib import contextmanager
import json
import os
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "tools" / "fleet")]
from prismabuild import pool  # noqa: E402
from prismabuild.record_store import DONE, FAILED, WITHDRAWN  # noqa: E402

import test_a_waiting_client_reads_its_ending_by_name as waiting  # noqa: E402

KEY = "a7" * 32
OTHERS = 200


@pytest.fixture()
def queue(tmp_path: Path) -> pool.PoolQueue:
    """One published action, beside other actions' withdrawals and endings."""

    made = pool.PoolQueue(tmp_path / "pb-queue")
    made.publish(
        action_key=KEY, cas_root=made.root / "cas", checkout_root=made.root / "co",
        worker_script=made.root / "worker.py", resources={"cpu": 1},
        max_attempts=2, retry_safe=True,
    )
    for index in range(OTHERS):
        for state in waiting.TERMINAL:
            waiting._file(made, state, waiting._outcome(f"{index:064x}", 1.0))
    return made


@contextmanager
def listings(queue: pool.PoolQueue, tmp_path: Path):
    """Every listing of withdrawn/, done/ or failed/ inside the block."""

    log = tmp_path / "listings.log"
    watched = set()
    for state in waiting.TERMINAL:
        directory = queue.records.dir(state)
        watched.add(os.path.normpath(os.path.abspath(directory)))
        watched.add(os.path.realpath(directory))
    seen: list[str] = []
    waiting._WATCHED, waiting._LOG = frozenset(watched), str(log)
    try:
        yield seen
    finally:
        waiting._WATCHED, waiting._LOG = frozenset(), None
        if log.exists():
            seen.extend(log.read_text(encoding="utf-8").splitlines())


def _marker(queue: pool.PoolQueue, generation: float) -> Path:
    """A visible withdrawal marker with no immutable decision behind it."""

    path = queue.records.item_path(WITHDRAWN, KEY)
    path.write_text(json.dumps({
        "schema": pool.POOL_OUTCOME_SCHEMA_V1, "action_key": KEY,
        "status": "withdrawn", "published_unix": generation,
        "withdrawn_by": "rob", "reason": "operator cancelled it",
    }), encoding="utf-8")
    return path


def _generation(queue: pool.PoolQueue) -> float:
    ready = queue.records.item_path(pool.record_store.READY, KEY)
    return float(json.loads(ready.read_text(encoding="utf-8"))["published_unix"])


def test_the_running_action_and_submission_withdrawal_checks_list_nothing(
    queue: pool.PoolQueue, tmp_path: Path,
) -> None:
    """``execute``'s cross-box poll, ``live_withdrawal`` and ``publish``'s refusal."""

    generation = _generation(queue)
    record = {"action_key": KEY, "published_unix": generation}
    with listings(queue, tmp_path) as seen:
        assert queue.withdrawal_covers(None, action_key=KEY) is None
        assert queue.live_withdrawal(KEY) is None
        _marker(queue, generation)
        assert queue.withdrawal_covers(None, action_key=KEY)["reason"] == "operator cancelled it"
        assert queue.withdrawal_covers(record, action_key=KEY) is not None
        assert queue.live_withdrawal(KEY)["reason"] == "operator cancelled it"
    assert seen == []


def test_a_claim_lists_no_terminal_or_withdrawn_directory(
    queue: pool.PoolQueue, tmp_path: Path,
) -> None:
    """The scan's withdrawal guard and the commit's terminal and withdrawal guards."""

    with listings(queue, tmp_path) as seen:
        claimed = queue.claim(capacity=simulated_capacity({"cpu": 1}))
    assert claimed is not None and claimed["action_key"] == KEY
    assert seen == []


def test_a_withdrawn_ready_copy_is_still_dropped_without_a_listing(
    queue: pool.PoolQueue, tmp_path: Path,
) -> None:
    _marker(queue, _generation(queue))
    with listings(queue, tmp_path) as seen:
        assert queue.claim(capacity=simulated_capacity({"cpu": 1})) is None
    assert not queue.records.item_path(pool.record_store.READY, KEY).exists()
    assert seen == []


def test_a_reap_lists_no_terminal_directory(
    queue: pool.PoolQueue, tmp_path: Path,
) -> None:
    """A stranded claim whose generation already ended is cleaned, not requeued."""

    assert queue.claim(capacity=simulated_capacity({"cpu": 1})) is not None
    claimed_path = queue.records.item_path(pool.record_store.CLAIMED, KEY)
    record = json.loads(claimed_path.read_text())
    record["claimed_unix"] = pool._now() - (pool.HEARTBEAT_S * 10)
    pool.immutable_files.write_json_atomic(claimed_path, record)
    queue.records.lease_path(KEY).unlink(missing_ok=True)
    pool.immutable_files.write_json_atomic(
        queue.records.item_path(DONE, KEY),
        {"schema": pool.POOL_OUTCOME_SCHEMA_V1, "action_key": KEY,
         "status": "executed", "published_unix": record["published_unix"],
         "finished_unix": pool._now(), "finished_host": "some-other-box"},
    )

    with listings(queue, tmp_path) as seen:
        queue.reap_stale()
    assert not queue.records.item_path(pool.record_store.READY, KEY).exists()
    assert seen == []


@pytest.mark.parametrize("state", [DONE, FAILED])
def test_the_terminal_guard_reads_both_endings_by_name(
    queue: pool.PoolQueue, tmp_path: Path, state: str,
) -> None:
    generation = _generation(queue)
    record = {"action_key": KEY, "published_unix": generation}
    with listings(queue, tmp_path) as seen:
        assert queue.terminal_outcome_covers(record, action_key=KEY) is None
        waiting._file(queue, state, waiting._outcome(KEY, generation))
        covered = queue.terminal_outcome_covers(record, action_key=KEY)
    assert covered is not None and covered[0] == state
    assert seen == []


def test_a_withdrawal_filed_after_a_cached_miss_is_seen(
    queue: pool.PoolQueue, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The listings were the revalidation; the directory open has to do its job."""

    waiting._NegativeLookupCache(monkeypatch, queue)
    assert queue.withdrawal_covers(None, action_key=KEY) is None
    _marker(queue, _generation(queue))

    assert queue.withdrawal_covers(None, action_key=KEY) is not None
