"""A waiter pinned to a generation reads its ending from immutable attempts.

``done/<key>.json`` and ``failed/<key>.json`` are one slot per action key, so a
later generation of the same content-addressed key legitimately replaces an
earlier generation's terminal row (#817).  ``finish`` publishes every attempt
under ``attempts/<key>/<sha(key, published_unix)>/<n>.json`` before the mutable
row moves and nothing deletes it, so the ending is still there; before the
exact-generation archive reader the waiter had no way to read it and polled
until ``--wait-s``.

These are the ordinary-generation counterparts of
``test_preemption_is_visible_to_readers``: no preemption, no handoff context --
just a finished run whose row a later run replaced.  The reader stays exact to
the generation (a newer ending is never reported as this run's verdict) and
refuses tampered, malformed or incomplete attempt evidence rather than reading
around it.
"""

from __future__ import annotations

import json
from pathlib import Path
import sys
import time

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import core as pb  # noqa: E402
from prismabuild import pool  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import pbrun  # noqa: E402
import pbwait  # noqa: E402

KEY = "f" * 64


@pytest.fixture()
def queue(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> pool.PoolQueue:
    monkeypatch.setattr(pbrun, "POLL_S", 0.001)
    made = pool.PoolQueue(tmp_path / "pb-queue")
    made.ensure_layout()
    return made


def _publish(q: pool.PoolQueue, *, max_attempts: int = 1,
             retry_safe: bool | None = None) -> float:
    q.publish(action_key=KEY, cas_root="/cas", checkout_root="/co",
              worker_script="/w.py", max_attempts=max_attempts,
              retry_safe=retry_safe)
    ready = json.loads(q.item_path(pool.READY, KEY).read_text(encoding="utf-8"))
    return float(ready["published_unix"])


def _run(q: pool.PoolQueue, *, status: str, returncode: int,
         stdout: str) -> dict:
    claim = q.claim()
    assert claim is not None and claim["action_key"] == KEY
    q.finish(KEY, status=status,
             detail={"returncode": returncode, "stdout": stdout},
             claim_snapshot=claim)
    return claim


def test_a_later_done_row_does_not_hide_the_waited_generation(queue) -> None:
    """The immutable attempt answers a generation whose done row was replaced."""

    first = _publish(queue)
    _run(queue, status="executed", returncode=0, stdout="first run\n")
    second = _publish(queue)
    assert second != first
    _run(queue, status="executed", returncode=0, stdout="later run\n")
    terminal = json.loads(
        queue.item_path(pool.DONE, KEY).read_text(encoding="utf-8"))
    assert float(terminal["published_unix"]) == second

    path, ending = pbrun.landed_outcome(
        queue, KEY, wait_s=0, generation=first)
    assert ending is not None
    assert float(ending["published_unix"]) == first
    assert ending["status"] == "executed"
    assert ending["attempts"] == 1
    summary = pbrun.outcome_summary(queue, path, ending)
    assert summary["status"] == "executed"
    assert summary["detail"]["stdout"] == "first run\n"
    assert path == queue.attempt_path(ending, ending["attempts"])
    assert path.exists()
    # Recovery is a read: the later generation's row is left where it is.
    assert json.loads(
        queue.item_path(pool.DONE, KEY).read_text(encoding="utf-8")) == terminal


def test_the_cli_wait_reports_the_recovered_generation(queue, monkeypatch) -> None:
    """The CLI's own bounded wait selects the archive for the pinned run."""

    monkeypatch.setattr(pbrun, "OUTCOME_READ_TIMEOUT_S", 30.0)
    first = _publish(queue)
    _run(queue, status="executed", returncode=0, stdout="first run\n")
    second = _publish(queue)
    _run(queue, status="executed", returncode=0, stdout="later run\n")
    assert second != first

    assert pbrun.await_outcome(
        queue, KEY, wait_s=30.0, generation=first) == 0


def test_a_later_failed_row_does_not_hide_the_waited_generations_failure(
        queue) -> None:
    """A G1 failure survives a G2 failure in the same one-slot directory."""

    first = _publish(queue)
    _run(queue, status="failed", returncode=7, stdout="first failed\n")
    second = _publish(queue)
    _run(queue, status="failed", returncode=3, stdout="later failed\n")
    assert second != first
    terminal = json.loads(
        queue.item_path(pool.FAILED, KEY).read_text(encoding="utf-8"))
    assert float(terminal["published_unix"]) == second

    path, ending = pbrun.landed_outcome(
        queue, KEY, wait_s=0, generation=first)
    assert ending is not None
    assert float(ending["published_unix"]) == first
    summary = pbrun.outcome_summary(queue, path, ending)
    assert summary["status"] == "failed"
    assert summary["returncode"] == 7
    assert summary["detail"]["stdout"] == "first failed\n"
    assert json.loads(
        queue.item_path(pool.FAILED, KEY).read_text(encoding="utf-8")) == terminal


def test_a_retried_generation_recovers_its_terminal_attempt_and_history(
        queue) -> None:
    """G1's two attempts are linked and verified, not just its last status."""

    first = _publish(queue, max_attempts=2, retry_safe=True)
    _run(queue, status="failed", returncode=7, stdout="first attempt\n")
    requeued = json.loads(
        queue.item_path(pool.READY, KEY).read_text(encoding="utf-8"))
    assert float(requeued["published_unix"]) == first
    assert requeued["attempts"] == 1
    _run(queue, status="failed", returncode=9, stdout="causal retry\n")
    second = _publish(queue)
    _run(queue, status="failed", returncode=3, stdout="later run\n")
    assert second != first
    terminal = json.loads(
        queue.item_path(pool.FAILED, KEY).read_text(encoding="utf-8"))
    assert float(terminal["published_unix"]) == second

    path, ending = pbrun.landed_outcome(
        queue, KEY, wait_s=0, generation=first)
    assert ending is not None
    assert float(ending["published_unix"]) == first
    assert ending["attempts"] == 2
    attempts = queue.attempt_outcomes(ending)
    assert [row["attempt"] for row in attempts] == [1, 2]
    assert attempts[0]["stdout"] == "first attempt\n"
    assert attempts[-1]["stdout"] == "causal retry\n"
    assert path == queue.attempt_path(ending, 2)
    summary = pbrun.outcome_summary(queue, path, ending)
    assert summary["status"] == "failed"
    assert summary["returncode"] == 9
    assert summary["detail"]["stdout"] == "causal retry\n"


def test_a_newer_generations_failure_is_never_the_waited_verdict(queue) -> None:
    """No exact ending means no ending; a newer row is not a fallback."""

    first = _publish(queue)
    second = _publish(queue)
    assert second != first
    _run(queue, status="failed", returncode=3, stdout="later run\n")

    landed, generation = pbrun.outcome_poll(queue, KEY, first)
    assert landed is None
    assert generation == first
    assert pbrun.landed_outcome(
        queue, KEY, wait_s=0, generation=first) is None


def test_pbwait_reports_the_recovered_generation(queue, tmp_path, monkeypatch) -> None:
    """The bounded waiter shares the selection step, so it recovers too."""

    monkeypatch.setattr(pbwait, "PBWAIT_READ_TIMEOUT_S", 30.0)
    first = _publish(queue)
    _run(queue, status="executed", returncode=0, stdout="first run\n")
    second = _publish(queue)
    _run(queue, status="executed", returncode=0, stdout="later run\n")
    assert second != first

    row = pbwait.wait_one(
        queue, KEY, cas=pb.PrismaBuildCAS(tmp_path / "cas"),
        deadline=time.monotonic(), generation=first,
    )
    assert row["status"] == "executed"
    assert row["returncode"] == 0
    assert row["succeeded"] is True


def _published_attempt(queue: pool.PoolQueue, generation: float) -> Path:
    return queue.attempt_path(
        {"action_key": KEY, "published_unix": generation}, 1)


def _rewrite(path: Path, mutate) -> None:
    path.chmod(0o644)
    value = json.loads(path.read_text(encoding="utf-8"))
    mutate(value)
    path.write_text(json.dumps(value), encoding="utf-8")
    path.chmod(0o444)


@pytest.mark.parametrize("damage", ["action_key", "generation", "path",
                                    "malformed", "log"])
def test_tampered_attempt_evidence_is_refused(queue, damage) -> None:
    """The reader verifies what it reads instead of reading around damage."""

    generation = _publish(queue)
    _run(queue, status="executed", returncode=0, stdout="real run\n")
    later = _publish(queue)
    _run(queue, status="executed", returncode=0, stdout="later run\n")
    assert later != generation
    attempt = _published_attempt(queue, generation)
    assert attempt.exists()

    if damage == "action_key":
        _rewrite(attempt, lambda value: value.__setitem__("action_key", "0" * 64))
    elif damage == "generation":
        _rewrite(attempt, lambda value: value.__setitem__(
            "published_unix", float(generation) + 1.0))
    elif damage == "path":
        moved = attempt.with_name("00000002.json")
        attempt.chmod(0o644)
        moved.write_bytes(attempt.read_bytes())
        moved.chmod(0o444)
        attempt.unlink()
    elif damage == "malformed":
        attempt.chmod(0o644)
        attempt.write_bytes(b"not JSON")
        attempt.chmod(0o444)
    else:
        value = json.loads(attempt.read_text(encoding="utf-8"))
        log = queue.root / value["logs"]["stdout"]["path"]
        log.chmod(0o644)
        log.write_bytes(b"changed after publication")
        log.chmod(0o444)

    with pytest.raises(pool.PoolContractError):
        queue.archived_generation_outcomes(KEY, generation=generation)
    with pytest.raises(pool.PoolContractError):
        pbrun.landed_outcome(queue, KEY, wait_s=0, generation=generation)


@pytest.mark.parametrize("absent", ["first", "middle"])
def test_an_incomplete_attempt_run_is_refused(queue, absent) -> None:
    """A gap, or a first attempt with no handoff context, is not history."""

    generation = _publish(queue, max_attempts=3, retry_safe=True)
    _run(queue, status="failed", returncode=7, stdout="attempt one\n")
    _run(queue, status="failed", returncode=8, stdout="attempt two\n")
    _run(queue, status="failed", returncode=9, stdout="attempt three\n")
    number = 1 if absent == "first" else 2
    removed = queue.attempt_path(
        {"action_key": KEY, "published_unix": generation}, number)
    assert removed.exists()
    removed.unlink()

    with pytest.raises(pool.PoolContractError):
        queue.archived_generation_outcomes(KEY, generation=generation)
