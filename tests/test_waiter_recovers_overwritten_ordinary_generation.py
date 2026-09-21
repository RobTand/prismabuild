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


def test_missing_earliest_attempt_is_refused_not_adopted(
    queue: pool.PoolQueue,
) -> None:
    """A deleted earliest attempt must refuse recovery, never adopt the rest.

    Directory absence proves nothing about a permitted legacy prefix: attempt
    1 deleted out of a two-attempt G1 must not read as one unrecorded attempt
    before attempt 2. The waiter must raise rather than report G1 done on
    attempt 2's evidence alone.
    """
    key = _key()
    holder = _publish(queue, key, max_attempts=2, retry_safe=True)
    g1 = float(holder["published_unix"])
    queue.finish(key, status="failed",
                 detail={"returncode": 1, "stdout": "g1 first", "stderr": ""})
    retry = queue.claim(capacity={"cpu": 4})
    assert retry is not None and float(retry["published_unix"]) == g1
    queue.finish(key, status="executed",
                 detail={"returncode": 0, "stdout": "g1 second", "stderr": ""})
    state, record = _terminal(queue, key)
    assert state == pool.DONE and record["attempts"] == 2
    time.sleep(0.01)
    holder2 = _publish(queue, key, max_attempts=1)
    assert float(holder2["published_unix"]) != g1
    queue.finish(key, status="executed",
                 detail={"returncode": 0, "stdout": "unrelated", "stderr": ""})
    generation_name = queue.attempt_generation(
        {"action_key": key, "published_unix": g1})
    first = queue.root / pool.ATTEMPTS / key / generation_name / "00000001.json"
    assert first.exists()
    first.chmod(0o644)
    first.unlink()
    with pytest.raises(pool.PoolContractError):
        pbrun.outcome_poll(queue, key, g1)


@pytest.mark.parametrize("damage", ["invalid-json", "empty"])
def test_malformed_attempt_is_refused_not_skipped(
    queue: pool.PoolQueue, damage: str,
) -> None:
    """A corrupt attempt file must raise, even when the directory exists.

    An unreadable-but-present file is damaged evidence, not an absent one:
    neither invalid JSON nor an empty file may read as just another reason to
    keep polling or to adopt what remains.
    """
    key = _key()
    holder = _publish(queue, key, max_attempts=1)
    g1 = float(holder["published_unix"])
    queue.finish(key, status="executed",
                 detail={"returncode": 0, "stdout": "g1 causal", "stderr": ""})
    time.sleep(0.01)
    holder2 = _publish(queue, key, max_attempts=1)
    assert float(holder2["published_unix"]) != g1
    queue.finish(key, status="executed",
                 detail={"returncode": 0, "stdout": "unrelated", "stderr": ""})
    generation_name = queue.attempt_generation(
        {"action_key": key, "published_unix": g1})
    target = (queue.root / pool.ATTEMPTS / key / generation_name
              / "00000001.json")
    target.chmod(0o644)
    target.write_bytes(b"" if damage == "empty" else b"{not json")
    target.chmod(0o444)
    with pytest.raises(pool.PoolContractError):
        pbrun.outcome_poll(queue, key, g1)


def test_wrong_generation_file_is_refused(
    queue: pool.PoolQueue,
) -> None:
    """A file carrying another generation's identity must raise.

    Same key, same filename, G2's body planted in G1's directory: the reader
    must refuse the wrong-identity record rather than adopt it as G1's ending
    or fall through to G2's row.
    """
    key = _key()
    holder = _publish(queue, key, max_attempts=1)
    g1 = float(holder["published_unix"])
    queue.finish(key, status="executed",
                 detail={"returncode": 0, "stdout": "g1 causal", "stderr": ""})
    time.sleep(0.01)
    holder2 = _publish(queue, key, max_attempts=1)
    g2 = float(holder2["published_unix"])
    assert g2 != g1
    queue.finish(key, status="executed",
                 detail={"returncode": 0, "stdout": "unrelated", "stderr": ""})
    g1_name = queue.attempt_generation(
        {"action_key": key, "published_unix": g1})
    g2_name = queue.attempt_generation(
        {"action_key": key, "published_unix": g2})
    planted = (queue.root / pool.ATTEMPTS / key / g2_name / "00000001.json"
               ).read_bytes()
    target = queue.root / pool.ATTEMPTS / key / g1_name / "00000001.json"
    target.chmod(0o644)
    target.write_bytes(planted)
    target.chmod(0o444)
    with pytest.raises(pool.PoolContractError):
        pbrun.outcome_poll(queue, key, g1)


def test_failed_g1_is_not_replaced_by_later_success(
    queue: pool.PoolQueue,
) -> None:
    """G1 failed beside a later G2 success still reports G1's failure.

    Cross-status generations occupy different terminal slots, so the live
    FAILED row already answers exactly; the archived fallback must not divert
    the waiter to the newer success.
    """
    key = _key()
    holder = _publish(queue, key, max_attempts=1)
    g1 = float(holder["published_unix"])
    queue.finish(key, status="failed",
                 detail={"returncode": 3, "stdout": "g1 failed", "stderr": ""})
    time.sleep(0.01)
    holder2 = _publish(queue, key, max_attempts=1)
    g2 = float(holder2["published_unix"])
    assert g2 != g1
    queue.finish(key, status="executed",
                 detail={"returncode": 0, "stdout": "g2 success", "stderr": ""})
    assert queue.item_path(pool.FAILED, key).exists()
    assert queue.item_path(pool.DONE, key).exists()
    landed, _ = pbrun.outcome_poll(queue, key, g1)
    assert landed is not None
    assert float(landed[1]["published_unix"]) == g1
    summary = pbrun.outcome_summary(queue, *landed)
    assert summary["returncode"] == 3
    assert summary["detail"]["stdout"] == "g1 failed"
