"""A waiter pinned to an ordinary generation recovers it after overwrite (#817).

``done/<key>.json`` is one slot per action key: ``finish`` writes it
unconditionally, so a later generation of the same key replaces the earlier
generation's row.  ``pbrun.terminal_record`` selects on exact generation
equality, and the only archived fallback is ``archived_preemption_outcomes``,
which skips attempts without ``preemption_context``.  An ordinary generation
therefore has no exact reader once a later generation overwrites its row, and
a waiter polls until ``--wait-s``.

The evidence already exists: ``finish`` publishes the immutable per-generation
attempt under ``attempts/<key>/<generation>/`` before moving the mutable
pointer, and nothing deletes it.  These tests pin a waiter to G1, overwrite
its mutable row with a real G2 of the same key, and require the waiter to
resolve G1's immutable terminal attempt -- never G2's row, never timestamp
ordering.  Retried generations must bind their full history exactly.
"""
from __future__ import annotations

import json
import time
import uuid
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools" / "fleet"))

from prismabuild import pool  # noqa: E402
import pbrun  # noqa: E402


@pytest.fixture(autouse=True)
def known_generation_action(monkeypatch):
    monkeypatch.setattr(
        pool.cpu_admission, "action_identity", lambda item: ("shape", False))


@pytest.fixture()
def queue(tmp_path: Path) -> pool.PoolQueue:
    q = pool.PoolQueue(tmp_path / "pb-queue")
    q.ensure_layout()
    return q


def _key() -> str:
    return uuid.uuid4().hex + uuid.uuid4().hex


def _publish(q: pool.PoolQueue, key: str, **kw: object) -> dict:
    q.publish(
        action_key=key,
        cas_root=kw.pop("cas_root", "/cas"),
        checkout_root=kw.pop("checkout_root", "/co"),
        worker_script=kw.pop("worker_script", "/w.py"),
        resources=kw.pop("resources", {"cpu": 1}),
        **kw,
    )
    claimed = q.claim(capacity={"cpu": 4})
    assert claimed is not None and claimed["action_key"] == key
    return claimed


def _terminal(q: pool.PoolQueue, key: str) -> tuple[str, dict]:
    for state in (pool.DONE, pool.FAILED):
        path = q.item_path(state, key)
        try:
            record = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        return state, record
    raise AssertionError(f"no terminal row for {key[:12]}")


def test_ordinary_done_survives_later_generation_overwrite(
    queue: pool.PoolQueue,
) -> None:
    """G1 done overwritten by G2 done still resolves G1 for a G1 waiter."""
    key = _key()
    holder = _publish(queue, key, max_attempts=1)
    g1 = float(holder["published_unix"])
    queue.finish(key, status="executed",
                 detail={"returncode": 0, "stdout": "g1 causal", "stderr": ""})
    state, record = _terminal(queue, key)
    assert state == pool.DONE and float(record["published_unix"]) == g1

    time.sleep(0.01)
    holder2 = _publish(queue, key, max_attempts=1)
    g2 = float(holder2["published_unix"])
    assert g2 != g1
    queue.finish(key, status="executed",
                 detail={"returncode": 0, "stdout": "g2 unrelated", "stderr": ""})
    state, record = _terminal(queue, key)
    assert state == pool.DONE and float(record["published_unix"]) == g2

    landed, generation = pbrun.outcome_poll(queue, key, g1)
    assert landed is not None, "G1 waiter lost its overwritten done row"
    assert float(landed[1]["published_unix"]) == g1
    assert landed[1]["status"] == "executed"
    assert landed[1]["detail"]["stdout"] == "g1 causal"
    assert generation == g1
    summary = pbrun.outcome_summary(queue, *landed)
    assert summary["status"] == "executed"
    assert summary["returncode"] == 0
    assert summary["adopted"] is not None
    # Recovery is read-only: G2's row still stands.
    _, record = _terminal(queue, key)
    assert float(record["published_unix"]) == g2


def test_ordinary_failed_survives_later_failed_overwrite(
    queue: pool.PoolQueue,
) -> None:
    """G1 failed overwritten by G2 failed still reports G1's failure."""
    key = _key()
    holder = _publish(queue, key, max_attempts=1)
    g1 = float(holder["published_unix"])
    queue.finish(key, status="failed",
                 detail={"returncode": 3, "stdout": "g1 failed", "stderr": ""})
    state, record = _terminal(queue, key)
    assert state == pool.FAILED and float(record["published_unix"]) == g1

    time.sleep(0.01)
    holder2 = _publish(queue, key, max_attempts=1)
    g2 = float(holder2["published_unix"])
    assert g2 != g1
    queue.finish(key, status="failed",
                 detail={"returncode": 9, "stdout": "g2 unrelated", "stderr": ""})
    state, record = _terminal(queue, key)
    assert state == pool.FAILED and float(record["published_unix"]) == g2

    landed, generation = pbrun.outcome_poll(queue, key, g1)
    assert landed is not None, "G1 waiter lost its overwritten failed row"
    assert float(landed[1]["published_unix"]) == g1
    assert landed[1]["detail"]["stdout"] == "g1 failed"
    assert generation == g1
    summary = pbrun.outcome_summary(queue, *landed)
    assert summary["returncode"] == 3
    assert summary["detail"]["stdout"] == "g1 failed"
    _, record = _terminal(queue, key)
    assert float(record["published_unix"]) == g2


def test_retried_success_binds_both_attempts_after_overwrite(
    queue: pool.PoolQueue,
) -> None:
    """G1 fail-then-done (two attempts) recovers its full history, not G2."""
    key = _key()
    holder = _publish(queue, key, max_attempts=3, retry_safe=True)
    g1 = float(holder["published_unix"])
    queue.finish(key, status="failed",
                 detail={"returncode": 1, "stdout": "g1 first", "stderr": ""})
    retry = queue.claim(capacity={"cpu": 4})
    assert retry is not None and float(retry["published_unix"]) == g1
    assert retry["attempts"] == 1
    queue.finish(key, status="executed",
                 detail={"returncode": 0, "stdout": "g1 second", "stderr": ""})
    state, record = _terminal(queue, key)
    assert state == pool.DONE and record["attempts"] == 2

    time.sleep(0.01)
    holder2 = _publish(queue, key, max_attempts=1)
    g2 = float(holder2["published_unix"])
    assert g2 != g1
    queue.finish(key, status="executed",
                 detail={"returncode": 0, "stdout": "g2 unrelated", "stderr": ""})

    landed, _ = pbrun.outcome_poll(queue, key, g1)
    assert landed is not None
    assert float(landed[1]["published_unix"]) == g1
    assert landed[1]["attempts"] == 2
    assert len(landed[1]["attempt_history"]) == 2
    summary = pbrun.outcome_summary(queue, *landed)
    assert summary["status"] == "executed"
    assert summary["detail"]["stdout"] == "g1 second"
    attempts = queue.attempt_outcomes(landed[1])
    assert [a["attempt"] for a in attempts] == [1, 2]
    assert attempts[0]["status"] == "failed"
    assert attempts[1]["status"] == "executed"


def test_retried_failure_binds_both_attempts_after_overwrite(
    queue: pool.PoolQueue,
) -> None:
    """G1 fail-then-fail (terminal, two attempts) keeps its own verdict."""
    key = _key()
    holder = _publish(queue, key, max_attempts=2, retry_safe=True)
    g1 = float(holder["published_unix"])
    queue.finish(key, status="failed",
                 detail={"returncode": 1, "stdout": "g1 first", "stderr": ""})
    retry = queue.claim(capacity={"cpu": 4})
    assert retry is not None and float(retry["published_unix"]) == g1
    queue.finish(key, status="failed",
                 detail={"returncode": 4, "stdout": "g1 second", "stderr": ""})
    state, record = _terminal(queue, key)
    assert state == pool.FAILED and record["attempts"] == 2

    time.sleep(0.01)
    holder2 = _publish(queue, key, max_attempts=1)
    g2 = float(holder2["published_unix"])
    assert g2 != g1
    queue.finish(key, status="failed",
                 detail={"returncode": 9, "stdout": "g2 unrelated", "stderr": ""})

    landed, _ = pbrun.outcome_poll(queue, key, g1)
    assert landed is not None
    assert float(landed[1]["published_unix"]) == g1
    summary = pbrun.outcome_summary(queue, *landed)
    assert summary["returncode"] == 4
    assert summary["detail"]["stdout"] == "g1 second"
    attempts = queue.attempt_outcomes(landed[1])
    assert [a["attempt"] for a in attempts] == [1, 2]


def test_overwritten_recovery_refuses_a_tampered_log(
    queue: pool.PoolQueue,
) -> None:
    """Recovery revalidates immutable evidence: a replaced log must raise."""
    key = _key()
    holder = _publish(queue, key, max_attempts=1)
    g1 = float(holder["published_unix"])
    queue.finish(key, status="executed",
                 detail={"returncode": 0, "stdout": "verified original",
                         "stderr": ""})
    time.sleep(0.01)
    holder2 = _publish(queue, key, max_attempts=1)
    assert float(holder2["published_unix"]) != g1
    queue.finish(key, status="executed",
                 detail={"returncode": 0, "stdout": "unrelated", "stderr": ""})
    generation_name = queue.attempt_generation(
        {"action_key": key, "published_unix": g1})
    log_path = next(iter(
        (queue.root / pool.ATTEMPTS / key / generation_name).glob(
            "00000001.stdout.*.log")))
    log_path.chmod(0o644)
    log_path.write_bytes(b"changed after publication")
    log_path.chmod(0o444)
    with pytest.raises(pool.PoolContractError):
        pbrun.outcome_poll(queue, key, g1)
