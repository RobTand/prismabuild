"""Review regressions for submission diagnostics and publication safety (#1936)."""

from admitted_queue_fixture import simulated_admission

from admitted_queue_fixture import simulated_capacity
import errno
import json
import os
from pathlib import Path
import threading

import pbrun
import pbwait
import pytest
from prismabuild import core as pb, filesystem_floor, immutable_files, owned_tree
from prismabuild import posix_lock, submission, submission_timing
from test_pbrun_detach import _one_json_line
from test_pbrun_sealed_publication import sealed_fleet, _consumer_argv
from test_pool_requeue_preserves_newer_generation import (
    DEMAND, ENDINGS, KEY, _end_claim, _publish, clock, queue,
)


def _review_active_phases():
    timings = pb._SUBMISSION_TIMINGS.get()
    return tuple(name for name, started in timings.stack) if timings else ()


def test_floor_mode_read_is_timed_even_when_off(sealed_fleet, monkeypatch, capsys):
    work, _, _ = sealed_fleet
    observed = []
    original = filesystem_floor.mode
    def observe_mode(*args, **kwargs):
        observed.append(_review_active_phases())
        return original(*args, **kwargs)
    monkeypatch.setattr(filesystem_floor, "mode", observe_mode)
    assert pbrun.main(_consumer_argv(work, "--seal-only")) == 0
    record = _one_json_line(capsys.readouterr())
    assert observed and observed[0] == ("filesystem_floor",)
    assert "filesystem_floor" in record["phases"]


def test_acknowledgement_verification_is_timed_and_in_response(sealed_fleet, monkeypatch, capsys):
    work, _, _ = sealed_fleet
    assert pbrun.main(_consumer_argv(work, "--seal-only")) == 0
    key = _one_json_line(capsys.readouterr())["action_key"]
    observed, offset = [], [0.0]
    real_clock = submission_timing.time.monotonic
    monkeypatch.setattr(submission_timing.time, "monotonic", lambda: real_clock() + offset[0])
    original = pb.PrismaBuildCAS.read_action_request
    def observe_request(*args, **kwargs):
        observed.append(_review_active_phases())
        result = original(*args, **kwargs)
        offset[0] += 0.25
        return result
    monkeypatch.setattr(pb.PrismaBuildCAS, "read_action_request", observe_request)
    assert pbrun.main(["--publish-sealed", key, "--detach"]) == 0
    record = _one_json_line(capsys.readouterr())
    assert observed and observed[-1] == ("acknowledgement_verify",)
    assert record["phases"]["acknowledgement_verify"] >= 0.25


def test_slow_diagnostic_failure_does_not_leak_the_mutex(tmp_path, monkeypatch):
    now, real_mutex = [100.0], threading.RLock()
    monkeypatch.setattr(submission_timing.time, "monotonic", lambda: now[0])
    class SlowReviewMutex:
        def acquire(self, *, blocking):
            acquired = real_mutex.acquire(blocking=blocking)
            now[0] += 2.0
            return acquired
        def release(self):
            real_mutex.release()
    path = posix_lock.canonical(tmp_path / "transition-locks" / "review.lock")
    monkeypatch.setitem(posix_lock._threads, str(path), SlowReviewMutex())
    def full_diagnostic(_):
        raise OSError(errno.ENOSPC, "stderr target full")
    failure = None
    try:
        with submission_timing.SubmissionTimings(on_slow=full_diagnostic).activate():
            with posix_lock.held(path) as acquired:
                assert acquired
    except OSError as exc:
        failure = exc
    observed = []
    def other_thread():
        acquired = real_mutex.acquire(blocking=False)
        observed.append(acquired)
        if acquired:
            real_mutex.release()
    other = threading.Thread(target=other_thread)
    other.start()
    other.join()
    try:
        assert observed == [True]
        assert failure is None
    finally:
        if observed == [False]:
            real_mutex.release()


@pytest.mark.parametrize("state", ["started", "finished"])
def test_phase_callback_failure_does_not_interrupt_work(state):
    def failed_channel(event):
        if event.state == state:
            raise OSError(errno.EPIPE, "diagnostic channel closed")
    timings = submission_timing.SubmissionTimings(on_phase=failed_channel)
    with timings.activate(), submission_timing.submission_phase("cas_read"):
        pass
    assert timings.stack == []
    assert "cas_read" in timings.snapshot()


@pytest.mark.parametrize("ending", ENDINGS)
def test_timed_retry_supersession_keeps_closed_fields_and_original_timing(queue, clock, ending):
    with submission_timing.SubmissionTimings().activate():
        with submission_timing.submission_phase("queue_publication"):
            _publish(queue)
    original = queue.claim(owner="original", **simulated_admission(queue, simulated_capacity(DEMAND)))
    assert original is not None and original["phases"]
    clock[0] += 1
    _publish(queue)
    _end_claim(queue, original, ending, clock)
    decision = queue.publication_ending(KEY, generation=original["published_unix"])
    assert decision is not None
    assert set(decision.record) == {"schema", "status", "action_key", "published_unix", "successor"}
    row = pbwait._from_ending(queue, decision)
    assert row["phases"] == original["phases"]
    assert row["status"] == "superseded"


def test_timing_adds_no_ready_fsync(sealed_fleet, monkeypatch, capsys):
    work, queue, _ = sealed_fleet
    writes, original = [], immutable_files.os.fsync
    def count_ready(fd):
        path = Path(os.readlink(f"/proc/self/fd/{fd}"))
        if path.parent == queue.records.dir("ready"):
            writes.append(str(path))
        return original(fd)
    monkeypatch.setattr(immutable_files.os, "fsync", count_ready)
    assert pbrun.main(_consumer_argv(work, "--detach")) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "submitted"
    assert len(writes) == 1


def test_owner_file_io_is_not_counted_as_lock_contention(tmp_path, monkeypatch):
    now = [100.0]
    monkeypatch.setattr(submission_timing.time, "monotonic", lambda: now[0])
    monkeypatch.setattr(submission.tempfile, "gettempdir", lambda: str(tmp_path))
    original = owned_tree.os.pwrite
    def slow_owner_header(*args, **kwargs):
        now[0] += 2.0
        return original(*args, **kwargs)
    monkeypatch.setattr(owned_tree.os, "pwrite", slow_owner_header)
    timings = submission_timing.SubmissionTimings()
    with timings.activate(), submission_timing.submission_phase("bundle_pack"):
        with submission.owned_snapshot_directory():
            pass
    assert timings.snapshot()["lock_wait"] == 0
    assert timings.snapshot()["bundle_pack"] >= 2


def test_span_finalization_cannot_leak_an_acquired_mutex(tmp_path, monkeypatch):
    from contextlib import contextmanager
    path = posix_lock.canonical(tmp_path / "transition-locks" / "finalization.lock")
    mutex = threading.RLock()
    monkeypatch.setitem(posix_lock._threads, str(path), mutex)
    @contextmanager
    def failed_finalization(_):
        yield
        raise RuntimeError("controlled span finalization failure")
    monkeypatch.setattr(posix_lock, "submission_phase", failed_finalization)
    with pytest.raises(RuntimeError, match="controlled span finalization"):
        with submission_timing.SubmissionTimings().activate(), posix_lock.held(path):
            pass
    observed = []
    def try_lock():
        acquired = mutex.acquire(blocking=False)
        observed.append(acquired)
        if acquired:
            mutex.release()
    other = threading.Thread(target=try_lock)
    other.start()
    other.join()
    try:
        assert observed == [True]
    finally:
        if observed == [False]:
            mutex.release()


@pytest.mark.parametrize("refused", [False, True])
def test_floor_cleanup_is_timed_and_receives_body_exceptions(sealed_fleet, monkeypatch, refused):
    from contextlib import contextmanager
    from prismabuild.submission_types import SubmissionRequest
    _, _, store = sealed_fleet
    observed = []
    @contextmanager
    def floor_window(*args, **kwargs):
        observed.append(("enter", _review_active_phases()))
        try:
            yield []
        except ValueError:
            observed.append(("exception", _review_active_phases()))
            raise
        finally:
            observed.append(("exit", _review_active_phases()))
    monkeypatch.setattr(filesystem_floor, "operation", floor_window)
    with submission_timing.SubmissionTimings().activate():
        if refused:
            with pytest.raises(ValueError, match="controlled floor body"):
                with submission.write_window(SubmissionRequest(), store=store):
                    raise ValueError("controlled floor body")
        else:
            with submission.write_window(SubmissionRequest(), store=store):
                pass
    assert observed == [(kind, ("filesystem_floor",)) for kind in
                        (["enter", "exception", "exit"] if refused else ["enter", "exit"])]


def test_supersession_timing_survives_retirement_archive_removal(queue, clock):
    with submission_timing.SubmissionTimings().activate(), submission_timing.submission_phase("queue_publication"):
        _publish(queue)
    original = queue.claim(owner="original", **simulated_admission(queue, simulated_capacity(DEMAND)))
    clock[0] += 1
    _publish(queue)
    _end_claim(queue, original, "finish_retry", clock)
    decision = queue.publication_ending(KEY, generation=original["published_unix"])
    for path, record in queue.records.superseded_records(KEY, fresh=True):
        if record["published_unix"] == original["published_unix"]:
            path.unlink()
    assert pbwait._from_ending(queue, decision)["phases"] == original["phases"]
