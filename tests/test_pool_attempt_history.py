"""Each pool attempt keeps the evidence that explains its own outcome."""

from __future__ import annotations

import json
from pathlib import Path
import stat
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

from prismabuild import pool  # noqa: E402
import pbrun  # noqa: E402


KEY = "e" * 64


@pytest.fixture()
def queue(tmp_path: Path) -> pool.PoolQueue:
    value = pool.PoolQueue(tmp_path / "queue")
    value.ensure_layout()
    value.publish(
        action_key=KEY,
        cas_root="/cas",
        checkout_root="/checkout",
        worker_script="/worker.py",
        max_attempts=2,
    )
    return value


def test_first_cause_and_later_stale_output_refusal_both_survive(
    queue: pool.PoolQueue, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    """The live #23 sequence, without the model-sized payload.

    Attempt one does the useful work and then fails its reference gate.  Its
    retry sees the external output and refuses it immediately.  Before the
    fix only attempt two survived in both the mutable queue record and the one
    truncate-on-open pbrun result; the causal failure was gone.

    main: the terminal record has no ``attempt_history``.
    branch: two immutable outcomes and their two logs remain and are reported.
    """

    assert queue.claim() is not None
    queue.finish(
        KEY,
        status="failed",
        detail={
            "returncode": 23,
            "stdout": "CAUSAL: corpus gives 4088 positions, dump has 4096\n",
            "stderr": "first gate failed\n",
            "elapsed_s": 600.0,
        },
    )
    assert queue.item_path(pool.READY, KEY).exists(), "retry is explicitly enabled"

    assert queue.claim() is not None
    terminal_path = queue.finish(
        KEY,
        status="failed",
        detail={
            "returncode": 24,
            "stdout": "REFUSED: stale output exists: teacher_bf16.json.npz\n",
            "stderr": "second gate failed\n",
            "elapsed_s": 0.148,
        },
    )
    terminal = json.loads(terminal_path.read_text(encoding="utf-8"))
    assert terminal_path == queue.item_path(pool.FAILED, KEY)
    assert terminal["attempts"] == 2
    assert "CAUSAL:" not in terminal["detail"]["stdout"], (
        "the mutable terminal still demonstrates why immutable history is needed"
    )

    links = terminal["attempt_history"]
    assert len(links) == 2 and links[0] != links[1]
    attempts = queue.attempt_outcomes(terminal)
    assert [one["attempt"] for one in attempts] == [1, 2]
    assert [one["status"] for one in attempts] == ["failed", "failed"]
    assert "CAUSAL: corpus gives 4088" in attempts[0]["stdout"]
    assert "REFUSED: stale output exists" in attempts[1]["stdout"]
    assert "first gate failed" in attempts[0]["stderr"]
    assert "second gate failed" in attempts[1]["stderr"]

    for link, attempt in zip(links, attempts, strict=True):
        outcome_path = queue.root / str(link["outcome"])
        stdout_path = queue.root / attempt["logs"]["stdout"]["path"]
        stderr_path = queue.root / attempt["logs"]["stderr"]["path"]
        assert stat.S_IMODE(outcome_path.stat().st_mode) == 0o444
        assert stat.S_IMODE(stdout_path.stat().st_mode) == 0o444
        assert stat.S_IMODE(stderr_path.stat().st_mode) == 0o444

    monkeypatch.setattr(pbrun, "POLL_S", 0.001)
    assert pbrun.await_outcome(queue, KEY, wait_s=1.0) == 24
    shown = capsys.readouterr()
    assert "CAUSAL: corpus gives 4088" in shown.out
    assert "REFUSED: stale output exists" in shown.out
    assert "attempt 1/2" in shown.err and "attempt 2/2" in shown.err


def test_lease_loss_is_an_attempt_with_immutable_empty_logs(
    queue: pool.PoolQueue,
) -> None:
    """A dead claimant consumed an attempt even though it returned no pipes."""

    assert queue.claim(owner="dead-box:1") is not None
    lease = json.loads(queue.lease_path(KEY).read_text(encoding="utf-8"))
    lease["heartbeat_unix"] = 0.0
    queue.lease_path(KEY).write_text(json.dumps(lease), encoding="utf-8")
    assert queue.reap_stale(timeout_s=-1.0) == [KEY]

    requeued = json.loads(
        queue.item_path(pool.READY, KEY).read_text(encoding="utf-8")
    )
    assert requeued["attempts"] == 1
    first = queue.attempt_outcomes(requeued)
    assert len(first) == 1
    assert first[0]["status"] == "lease_lost"
    assert first[0]["disposition"] == "requeued"
    assert first[0]["stdout"] == first[0]["stderr"] == ""

    assert queue.claim(owner="dead-box:2") is not None
    lease = json.loads(queue.lease_path(KEY).read_text(encoding="utf-8"))
    lease["heartbeat_unix"] = 0.0
    queue.lease_path(KEY).write_text(json.dumps(lease), encoding="utf-8")
    assert queue.reap_stale(timeout_s=-1.0) == [KEY]
    terminal = json.loads(
        queue.item_path(pool.FAILED, KEY).read_text(encoding="utf-8")
    )
    attempts = queue.attempt_outcomes(terminal)
    assert [one["attempt"] for one in attempts] == [1, 2]
    assert attempts[1]["status"] == "lease_lost_max_attempts"
    assert attempts[1]["disposition"] == pool.FAILED


def test_first_writer_is_relinked_after_a_crash_between_archive_and_summary(
    queue: pool.PoolQueue,
) -> None:
    """A later reaper adopts, rather than conflicts with, the causal outcome."""

    assert queue.claim() is not None
    queue.finish(
        KEY,
        status="failed",
        detail={
            "returncode": 23,
            "stdout": "CAUSAL: first writer\n",
            "stderr": "gate failed\n",
        },
    )
    requeued = json.loads(
        queue.item_path(pool.READY, KEY).read_text(encoding="utf-8")
    )
    expected_history = requeued.pop("attempt_history")

    # Model a crash after immutable publication but before the mutable record
    # kept its link.  A reaper reaches the same numbered attempt with a
    # different observation; first-writer evidence is authoritative.
    recovered = queue.archive_attempt(
        requeued,
        attempt=1,
        status="lease_lost",
        disposition="requeued",
        detail={"reason": "lease expired while finish was publishing"},
    )
    assert recovered == expected_history
    relinked = {**requeued, "attempt_history": recovered}
    outcome = queue.attempt_outcomes(relinked)[0]
    assert outcome["status"] == "failed"
    assert outcome["stdout"] == "CAUSAL: first writer\n"


def test_finish_adopts_reaper_attempt_that_won_immutable_race(
    queue: pool.PoolQueue, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A late finisher cannot file ``done`` over a requeued lease loss."""

    assert queue.claim(owner="worker:1") is not None
    real_archive = queue.archive_attempt
    injected = False

    def reaper_wins(record, **finisher):
        nonlocal injected
        if not injected:
            injected = True
            real_archive(
                record,
                attempt=finisher["attempt"],
                status="lease_lost",
                disposition="requeued",
                detail={"reason": "lease expired while the worker finished"},
            )
        return real_archive(record, **finisher)

    monkeypatch.setattr(queue, "archive_attempt", reaper_wins)
    landed = queue.finish(
        KEY,
        status="executed",
        detail={"returncode": 0, "stdout": "late success\n"},
    )

    assert landed == queue.item_path(pool.READY, KEY)
    assert not queue.item_path(pool.DONE, KEY).exists()
    ready = json.loads(landed.read_text(encoding="utf-8"))
    adopted = queue.attempt_outcomes(ready)[0]
    summary = queue.adopted_attempt_summary(ready)
    assert ready["status"] == adopted["status"] == "lease_lost"
    assert ready["detail"] == summary["detail"]
    assert ready["detail"]["stdout"] == ready["detail"]["stderr"] == ""
    assert adopted["disposition"] == "requeued"


def test_reaper_adopts_finisher_attempt_and_await_returncode(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture,
) -> None:
    """The immutable finisher decides terminal state, detail, and caller rc."""

    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    queue.publish(
        action_key=KEY,
        cas_root="/cas",
        checkout_root="/checkout",
        worker_script="/worker.py",
        max_attempts=1,
    )
    assert queue.claim(owner="worker:1") is not None
    lease = json.loads(queue.lease_path(KEY).read_text(encoding="utf-8"))
    lease["heartbeat_unix"] = 0.0
    queue.lease_path(KEY).write_text(json.dumps(lease), encoding="utf-8")
    real_archive = queue.archive_attempt
    injected = False

    def finisher_wins(record, **reaper):
        nonlocal injected
        if not injected:
            injected = True
            real_archive(
                record,
                attempt=reaper["attempt"],
                status="failed",
                disposition=pool.FAILED,
                detail={
                    "returncode": 37,
                    "stdout": "finisher's causal error\n",
                    "elapsed_s": 2.0,
                },
            )
        return real_archive(record, **reaper)

    monkeypatch.setattr(queue, "archive_attempt", finisher_wins)
    assert queue.reap_stale(timeout_s=-1.0) == [KEY]

    terminal_path = queue.item_path(pool.FAILED, KEY)
    terminal = json.loads(terminal_path.read_text(encoding="utf-8"))
    adopted = queue.attempt_outcomes(terminal)[0]
    summary = queue.adopted_attempt_summary(terminal)
    assert terminal["status"] == adopted["status"] == "failed"
    assert terminal["detail"]["returncode"] == 37
    assert terminal["detail"] == summary["detail"]

    monkeypatch.setattr(pbrun, "POLL_S", 0.001)
    assert pbrun.await_outcome(queue, KEY, wait_s=1.0) == 37
    shown = capsys.readouterr()
    assert "finisher's causal error" in shown.out
    assert "lease expired" not in shown.err

    terminal["detail"]["returncode"] = 99
    terminal_path.write_text(json.dumps(terminal), encoding="utf-8")
    with pytest.raises(pool.PoolContractError, match="adopted immutable"):
        pbrun.await_outcome(queue, KEY, wait_s=1.0)


def test_adopted_attempt_requires_a_finite_finish_time(
    queue: pool.PoolQueue, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """NaN/Infinity cannot become terminal provenance or ordering evidence."""

    claimed = queue.claim(owner="worker:1")
    assert claimed is not None
    claimed.update({"finished_unix": 1.0, "finished_host": "worker"})
    history = queue.archive_attempt(
        claimed,
        attempt=1,
        status="failed",
        disposition="requeued",
        detail={"returncode": 1},
    )
    linked = {**claimed, "attempts": 1, "attempt_history": history}
    real_outcomes = queue.attempt_outcomes

    def nonfinite(record):
        outcomes = real_outcomes(record)
        outcomes[-1]["finished_unix"] = float("inf")
        return outcomes

    monkeypatch.setattr(queue, "attempt_outcomes", nonfinite)
    with pytest.raises(pool.PoolContractError, match="finished_unix must be finite"):
        queue.adopted_attempt_summary(linked)


def test_rollout_archives_the_next_attempt_without_inventing_old_history(
    queue: pool.PoolQueue, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    """A requeued pre-deploy record remains executable across the rollout."""

    assert queue.claim() is not None
    claimed_path = queue.item_path(pool.CLAIMED, KEY)
    claimed = json.loads(claimed_path.read_text(encoding="utf-8"))
    claimed["attempts"] = 1
    claimed_path.write_text(json.dumps(claimed), encoding="utf-8")

    terminal_path = queue.finish(
        KEY,
        status="failed",
        detail={"returncode": 24, "stdout": "second attempt\n"},
    )
    terminal = json.loads(terminal_path.read_text(encoding="utf-8"))
    assert terminal["attempt_history_missing_before"] == 1
    assert [one["attempt"] for one in queue.attempt_outcomes(terminal)] == [2]

    monkeypatch.setattr(pbrun, "POLL_S", 0.001)
    assert pbrun.await_outcome(queue, KEY, wait_s=1.0) == 24
    shown = capsys.readouterr()
    assert "1 earlier attempt predates immutable history" in shown.err
    assert "attempt 2/2" in shown.err
    assert "second attempt" in shown.out


def test_terminal_attempt_count_cannot_outlive_its_history_links(
    queue: pool.PoolQueue, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Removing a causal link must refuse instead of showing only the tail."""

    for returncode in (23, 24):
        assert queue.claim() is not None
        terminal_path = queue.finish(
            KEY,
            status="failed",
            detail={"returncode": returncode, "stdout": f"rc {returncode}\n"},
        )
    terminal = json.loads(terminal_path.read_text(encoding="utf-8"))
    assert terminal["attempts"] == 2
    terminal["attempt_history"] = []
    terminal_path.write_text(json.dumps(terminal), encoding="utf-8")

    monkeypatch.setattr(pbrun, "POLL_S", 0.001)
    with pytest.raises(pool.PoolContractError, match="attempt count"):
        pbrun.await_outcome(queue, KEY, wait_s=1.0)
