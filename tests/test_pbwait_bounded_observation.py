"""``pbwait`` keeps read-only queue and CAS observation abandonable.

The FIFO is private to each test and stands in for a hard-NFS read: it makes
the actual record open block after the caller has identified the exact path.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import threading
import time

import pytest


ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "tools" / "fleet")]
from prismabuild import core as pb, pool  # noqa: E402
import pbrun  # noqa: E402
import pbwait  # noqa: E402


KEY = "a" * 64


def _queue(tmp_path: Path):
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    return queue


def _outcome(*, generation: float = 1.0) -> dict:
    return {
        "schema": pool.POOL_OUTCOME_SCHEMA_V1,
        "action_key": KEY,
        "status": "executed",
        "published_unix": generation,
        "finished_unix": 2.0,
        "finished_host": "worker",
        "attempts": 1,
        "detail": {"returncode": 0, "elapsed_s": 1.0,
                   "stdout": "", "stderr": ""},
    }


def test_fifo_submission_observation_is_bounded_with_a_causal_marker(
    tmp_path: Path,
) -> None:
    """The unchanged source blocks; the bounded reader returns per-key 74.

    The child-owned FIFO has no writer. The marker is made at the exact
    ``outstanding_submission`` read, before its open blocks, so the outer
    watchdog distinguishes this regression from a test-runner delay. The wait
    has zero patience, so this pins the single-read contract: a patient wait
    retries a reaped timeout (#558, ``test_pbwait_observation_retry.py``).
    """

    queue = _queue(tmp_path)
    fifo = queue.item_path(pool.READY, KEY)
    marker = tmp_path / "entered-outstanding-submission-read"
    os.mkfifo(fifo)
    code = f'''\
import sys, time
from pathlib import Path
sys.path[:0] = [{str(ROOT / "src")!r}, {str(ROOT / "tools" / "fleet")!r}]
from prismabuild import core as pb, pool
import pbwait
queue = pool.PoolQueue(Path({str(queue.root)!r}))
fifo = queue.item_path(pool.READY, {KEY!r})
marker = Path({str(marker)!r})
original = Path.read_text
def blocked(path, *args, **kwargs):
    if path == fifo:
        marker.write_text("entered", encoding="utf-8")
    return original(path, *args, **kwargs)
Path.read_text = blocked
pbwait.slurm_lane.recorded_submission = lambda *_args, **_kwargs: None
pbwait.PBWAIT_READ_TIMEOUT_S = 0.05
row = pbwait.wait_one(queue, {KEY!r}, cas=pb.PrismaBuildCAS(Path({str(tmp_path / "cas")!r})), deadline=time.monotonic())
print(row["status"])
print(row["note"])
'''
    try:
        completed = subprocess.run(
            [sys.executable, "-c", code], text=True, capture_output=True,
            timeout=2.0, check=False,
        )
    except subprocess.TimeoutExpired as exc:
        pytest.fail(
            "pbwait exceeded its bounded observation budget; causal-marker="
            + str(marker.exists()) + "; stdout="
            + (exc.stdout.decode() if isinstance(exc.stdout, bytes) else (exc.stdout or ""))
        )
    assert completed.returncode == 0, completed.stderr
    assert marker.read_text(encoding="utf-8") == "entered"
    assert completed.stdout.splitlines()[0] == "record_error"
    assert "pbwait observation timed out" in completed.stdout


def test_immutable_summary_verification_is_bounded_per_key(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue = _queue(tmp_path)
    queue.item_path(pool.DONE, KEY).write_text(json.dumps(_outcome()), encoding="utf-8")
    real_read = pbrun._bounded_pool_read

    def short_verification(section, read, *, budget_s, **kwargs):
        # Only the verification is meant to time out. The observation before
        # it must answer, so it keeps its real budget (#1170).
        if section == "pool outcome verification":
            budget_s = 0.05
        return real_read(section, read, budget_s=budget_s, **kwargs)

    def blocked(*_args, **_kwargs):
        time.sleep(30)

    monkeypatch.setattr(pbrun, "_bounded_pool_read", short_verification)
    monkeypatch.setattr(pbrun, "outcome_summary", blocked)
    # Zero patience pins one bounded verification; a patient wait retries it.
    row = pbwait.wait_one(
        queue, KEY, cas=pb.PrismaBuildCAS(tmp_path / "cas"),
        deadline=time.monotonic(),
    )
    assert row["status"] == "record_error"
    assert "pool outcome verification timed out" in str(row["note"])


def test_terminal_record_outranks_and_skips_cas_lookup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue = _queue(tmp_path)
    queue.item_path(pool.DONE, KEY).write_text(json.dumps(_outcome()), encoding="utf-8")
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    monkeypatch.setattr(pbwait, "recorded_action", lambda *_args: pytest.fail(
        "a landed ending must return before the sealed request is read"))
    monkeypatch.setattr(cas, "lookup", lambda *_args: pytest.fail(
        "a landed ending must return before the CAS is read"))
    row = pbwait.wait_one(
        queue, KEY, cas=cas, deadline=time.monotonic() + 1,
        lane_root=tmp_path / "lane",
    )
    assert row["status"] == "executed"


def test_unreadable_terminal_record_outranks_a_blocked_cas_lookup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue = _queue(tmp_path)
    path = queue.item_path(pool.FAILED, KEY)
    path.write_text("{", encoding="utf-8")
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    monkeypatch.setattr(pbwait, "recorded_action", lambda *_args: time.sleep(30))
    monkeypatch.setattr(cas, "lookup", lambda *_args: time.sleep(30))
    row = pbwait.wait_one(queue, KEY, cas=cas, deadline=time.monotonic() + 1)
    assert row["status"] == "unreadable"
    assert "not valid JSON" in str(row["note"])


def test_preemption_successor_is_observed_after_the_original_deadline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue = _queue(tmp_path)
    path = queue.item_path(pool.DONE, KEY)
    outcome = _outcome(generation=2.0)
    path.write_text(json.dumps(outcome), encoding="utf-8")
    observations = [
        (("pool", 1.0, {"published_unix": 1.0}), None, 2.0, None, False, None, 1.0),
        (("pool", 2.0, {"published_unix": 2.0}), (path, outcome), 2.0, None, False, None, 2.0),
    ]
    monkeypatch.setattr(pbwait, "bounded_observation", lambda *_args, **_kwargs:
                        observations.pop(0))
    row = pbwait.wait_one(
        queue, KEY, cas=pb.PrismaBuildCAS(tmp_path / "cas"),
        deadline=time.monotonic() - 1, generation=1.0,
    )
    assert row["status"] == "executed"
    assert not observations


def test_bare_key_follows_an_immediate_preemption_successor_after_deadline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue = _queue(tmp_path)
    path = queue.item_path(pool.DONE, KEY)
    outcome = _outcome(generation=2.0)
    path.write_text(json.dumps(outcome), encoding="utf-8")
    observations = [
        (("pool", 1.0, {"published_unix": 1.0}), None, 2.0, None, False, None, 1.0),
        (("pool", 2.0, {"published_unix": 2.0}), (path, outcome), 2.0, None, False, None, 2.0),
    ]
    monkeypatch.setattr(pbwait, "bounded_observation", lambda *_args, **_kwargs:
                        observations.pop(0))
    row = pbwait.wait_one(
        queue, KEY, cas=pb.PrismaBuildCAS(tmp_path / "cas"),
        deadline=time.monotonic() - 1,
    )
    assert row["status"] == "executed"
    assert not observations


def test_preemption_handoff_skips_cas_until_the_successor_is_observed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue = _queue(tmp_path)
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    monkeypatch.setattr(pbwait, "outstanding", lambda *_args, **_kwargs:
                        ("pool", 1.0, {"published_unix": 1.0}))
    monkeypatch.setattr(pbrun, "outcome_poll", lambda *_args, **_kwargs:
                        (None, 2.0))
    monkeypatch.setattr(pbwait, "recorded_action", lambda *_args: pytest.fail(
        "the stopped generation must not reach the request/CAS read"))
    observed = pbwait._bounded_observation_value(
        queue, KEY, cas, 1.0, lane_root=tmp_path / "lane")
    assert observed["outcome"]["generation"] == 2.0
    assert observed["action"] is None
    assert observed["receipt_published"] is False


class _BlockedRead(Exception):
    """Stands in for a shared-filesystem read that never answers."""


def test_one_blocked_key_does_not_hide_a_healthy_key_in_a_multi_wait(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A key whose read hangs neither delays nor overwrites a healthy key's row.

    The reads are injected, so no bound here depends on how fast the box
    answers (#1170). The blocked key's submission read holds until the healthy
    key's row has been returned, then times out the way a real bounded reader
    does. A multi-wait that served the keys in turn would hold that read until
    the safety bound instead, and one that let the timed-out row stand for the
    other key would report two errors. The real FIFO-backed reader bound is
    covered by ``test_fifo_submission_observation_is_bounded_with_a_causal_marker``.
    """

    queue = _queue(tmp_path)
    blocked = "a" * 64
    healthy = "b" * 64
    good = {**_outcome(), "action_key": healthy}
    queue.item_path(pool.DONE, healthy).write_text(json.dumps(good), encoding="utf-8")
    healthy_returned = threading.Event()
    released_by_healthy_row = []

    def outstanding(q, key, **kwargs):
        if key == blocked:
            # Only a failing multi-wait reaches the safety bound; a passing
            # one releases this read as soon as the healthy row exists.
            released_by_healthy_row.append(healthy_returned.wait(timeout=60.0))
            raise _BlockedRead
        return pbrun.outstanding_submission(q, key, **kwargs)

    def inline_reader(section, read, *, budget_s, **_kwargs):
        try:
            return read()
        except _BlockedRead:
            raise pbrun.OutcomeObservationTimedOut(
                f"{section} timed out after {budget_s}s") from None

    real_wait_one = pbwait.wait_one

    def observed_wait_one(q, key, **kwargs):
        row = real_wait_one(q, key, **kwargs)
        if key == healthy:
            healthy_returned.set()
        return row

    monkeypatch.setattr(pbwait, "outstanding", outstanding)
    monkeypatch.setattr(pbrun, "_bounded_pool_read", inline_reader)
    monkeypatch.setattr(pbwait, "wait_one", observed_wait_one)
    # Zero patience: the blocked key's single timed-out read is its row. A
    # patient wait would retry it until the deadline (#558).
    rows = pbwait.wait_for_keys(
        queue, [blocked, healthy], cas=pb.PrismaBuildCAS(tmp_path / "cas"),
        wait_s=0,
    )
    assert released_by_healthy_row == [True], (
        "the healthy key's row waited for the blocked key's read")
    assert [row["status"] for row in rows] == ["record_error", "executed"]
    assert "pbwait observation timed out" in str(rows[0]["note"])


def test_reader_setup_failure_never_retries_reads_in_the_parent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue = _queue(tmp_path)
    cas = pb.PrismaBuildCAS(tmp_path / "cas")

    def setup_failed(*_args, **_kwargs):
        raise OSError(24, "Too many open files")

    def forbidden(*_args, **_kwargs):
        pytest.fail("reader setup failure must not retry shared reads")

    monkeypatch.setattr(pbrun, "_bounded_pool_read", setup_failed)
    monkeypatch.setattr(pbwait, "outstanding", forbidden)
    monkeypatch.setattr(pbwait, "recorded_action", forbidden)
    row = pbwait.wait_one(queue, KEY, cas=cas, deadline=time.monotonic() + 30)
    assert pbwait.verdict([row]) == 74
    assert "Too many open files" in row["note"]


def test_retained_observation_reader_stops_before_render_or_another_poll(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue = _queue(tmp_path)
    queue.item_path(pool.DONE, KEY).write_text(json.dumps(_outcome()), encoding="utf-8")
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    retained = []

    def retain(pid, section, _started, abandoned):
        record = {"pid": pid, "section": section,
                  "starttime_ticks": pbrun.pbstatus._starttime_ticks(pid)}
        retained.append(record)
        abandoned.append(record)

    def forbidden(*_args, **_kwargs):
        pytest.fail("an unreaped observation must not reach terminal verification")

    monkeypatch.setattr(pbrun.pbstatus, "_reap_within", lambda *_args: False)
    monkeypatch.setattr(pbrun.pbstatus, "_stop_reader", retain)
    monkeypatch.setattr(pbrun, "bounded_outcome_render", forbidden)
    try:
        row = pbwait.wait_one(queue, KEY, cas=cas, deadline=time.monotonic() + 30)
        assert pbwait.verdict([row]) == 74
        assert len(retained) == 1
        assert retained[0]["starttime_ticks"] is not None
        assert '"pid": ' + str(retained[0]["pid"]) in row["note"]
        assert "could not be reaped" in row["note"]
    finally:
        # Exact children created by this fixture only; never a process-name scan.
        for record in retained:
            try:
                os.kill(record["pid"], signal.SIGKILL)
                os.waitpid(record["pid"], 0)
            except (ChildProcessError, ProcessLookupError):
                pass
