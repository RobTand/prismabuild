"""Submission diagnostics follow actual work and survive publication (#1936)."""
from __future__ import annotations

import json
import os

import pytest
import pbrun
import pbwait
from prismabuild import submission, submission_timing, client_channel, core as pb, posix_lock
from test_pbrun_sealed_publication import sealed_fleet, _consumer_argv
from test_pbrun_detach import _one_json_line
from test_pbrun_reseal import _generation
from submission_support import patch_store

__all__ = ["sealed_fleet"]


@pytest.mark.parametrize("verb", ["plain", "seal-only", "publish-sealed", "as-sealed-by"])
def test_submission_reports_the_phases_it_actually_runs(sealed_fleet, monkeypatch, capsys, verb):
    work, queue, store = sealed_fleet
    if verb == "as-sealed-by":
        patch_store(monkeypatch, wrapper_dir=_generation(store.root, "timing-original"))
    key = None
    if verb in {"publish-sealed", "as-sealed-by"}:
        assert pbrun.main(_consumer_argv(work, "--seal-only")) == 0
        key = _one_json_line(capsys.readouterr())["action_key"]
    argv = (["--publish-sealed", key, "--detach"] if verb == "publish-sealed" else
            _consumer_argv(work, *(["--seal-only"] if verb == "seal-only" else
                ["--detach", *(["--as-sealed-by", key] if key else [])])))
    assert pbrun.main(argv) == 0
    record = _one_json_line(capsys.readouterr())
    phases = record["phases"]
    expected = ({"sealed_request_read", "cas_input_verify"} if verb == "publish-sealed" else
                {"checkout_identity", "bundle_pack", "cas_stage", "cas_write"})
    if verb != "seal-only":
        expected |= {"offer_read", "d38_gate", "queue_publication", "transition_lock_wait"}
        ready = json.loads(submission.Path(record["submission"]).read_text())
        assert expected <= ready["phases"].keys()
    assert expected <= phases.keys()
    assert all(type(value) is float and value >= 0 for value in phases.values())
    assert ("checkout_identity" in phases) == (verb != "publish-sealed")


def test_phase_events_survive_a_refused_publication(sealed_fleet, monkeypatch, capsys):
    work, queue, store = sealed_fleet
    assert pbrun.main(_consumer_argv(work, "--seal-only")) == 0
    key = _one_json_line(capsys.readouterr())["action_key"]
    def phase_gate_refusal(*args, **kwargs):
        raise submission.SubmissionRefused("controlled gate refusal")
    monkeypatch.setattr(submission.d38_gate, "judge_publication", phase_gate_refusal)
    read_fd, write_fd = os.pipe()
    try:
        assert pbrun.main(["--publish-sealed", key, "--detach",
                           "--client-events-fd", str(write_fd)]) == 1
        events = [json.loads(line) for line in os.read(read_fd, 65536).splitlines()]
    finally:
        os.close(read_fd)
    phases = [event for event in events if event["event"] == "phase"]
    gate = [event for event in phases if event["phase"] == "d38_gate"]
    assert [event["state"] for event in gate] == ["started", "finished"]
    assert gate[-1]["seconds"] >= 0
    assert phases[-1]["phase"] == "filesystem_floor"
    assert phases[-1]["state"] == "finished"
    assert events[-1]["event"] == "ending"
    assert queue.ready_items() == []


def test_nested_lock_duration_is_separate_from_io(monkeypatch, tmp_path):
    clock = [100.0]
    monkeypatch.setattr(submission_timing.time, "monotonic", lambda: clock[0])
    original = posix_lock._lockf
    def delayed_kernel_lock(descriptor, blocking):
        clock[0] += 17.0
        original(descriptor, blocking)
    monkeypatch.setattr(posix_lock, "_lockf", delayed_kernel_lock)
    events, slow = [], []
    timings = submission_timing.SubmissionTimings(on_phase=events.append, on_slow=slow.append)
    with timings.activate():
        with submission_timing.submission_phase("queue_publication"):
            clock[0] += 2.0
            with posix_lock.held(tmp_path / "transition-locks" / "test.lock"):
                clock[0] += 3.0
    assert timings.snapshot() == {"queue_publication": 5.0, "transition_lock_wait": 17.0}
    assert len(slow) == 2
    assert all("test.lock" not in line for line in slow)
    assert events[-1] == submission_timing.SubmissionPhaseEvent("queue_publication", "finished", 5.0)


@pytest.mark.parametrize("name", sorted(submission_timing.SUBMISSION_PHASE_NAMES))
def test_exception_closes_each_phase_without_recording_its_reason(name):
    events = []
    timings = submission_timing.SubmissionTimings(on_phase=events.append)
    with timings.activate(), pytest.raises(ValueError, match="private reason"):
        with submission_timing.submission_phase(name):
            raise ValueError("private reason")
    assert events[0] == submission_timing.SubmissionPhaseEvent(name, "started")
    assert events[1].phase == name and events[1].state == "finished"
    assert events[1].seconds >= 0
    assert set(timings.snapshot()) == {name}
    assert "private" not in repr(events)


def test_killed_client_leaves_the_phase_entered_before_blocking(sealed_fleet, monkeypatch, capsys):
    import signal
    work, queue, store = sealed_fleet
    assert pbrun.main(_consumer_argv(work, "--seal-only")) == 0
    key = _one_json_line(capsys.readouterr())["action_key"]
    entered_read, entered_write = os.pipe()
    block_read, block_write = os.pipe()
    events_read, events_write = os.pipe()
    def blocked_phase_gate(*args, **kwargs):
        os.write(entered_write, b"1")
        os.read(block_read, 1)
    monkeypatch.setattr(submission.d38_gate, "judge_publication", blocked_phase_gate)
    child = os.fork()
    if child == 0:
        try:
            os.close(events_read)
            pbrun.main(["--publish-sealed", key, "--detach", "--client-events-fd", str(events_write)])
        finally:
            os._exit(1)
    os.close(events_write)
    try:
        assert os.read(entered_read, 1) == b"1"
        os.kill(child, signal.SIGKILL)
        assert os.waitpid(child, 0)[1] == signal.SIGKILL
        child = None
        decoder = client_channel.ClientEventDecoder(queue_root=store.queue_root, cas_root=store.cas_root)
        decoder.feed(os.read(events_read, 65536))
        assert decoder.last_phase == "d38_gate"
        assert "d38_gate" not in decoder.phases
        assert decoder.finish(-signal.SIGKILL).problem == "missing client ending"
        assert queue.ready_items() == []
    finally:
        if child is not None:
            os.kill(child, signal.SIGKILL)
            os.waitpid(child, 0)
        for fd in (entered_read, entered_write, block_read, block_write, events_read):
            os.close(fd)


def test_publication_phases_reach_claim_terminal_attempt_and_both_wait_formats(sealed_fleet, monkeypatch, capsys):
    work, queue, store = sealed_fleet
    assert pbrun.main(_consumer_argv(work, "--detach")) == 0
    submitted = _one_json_line(capsys.readouterr())
    key = submitted["action_key"]
    ready = json.loads(submission.Path(submitted["submission"]).read_text())
    phases = ready["phases"]
    assert phases["queue_publication"] <= submitted["phases"]["queue_publication"]
    # A pending row exposes the same publisher's diagnostic snapshot.
    assert pbwait.main([key, "--endings", "--wait-s", "0"]) == 75
    assert _one_json_line(capsys.readouterr())["endings"][0]["phases"] == phases
    claim = queue.claim(tags=["sparky", "gb10"], capacity={"cpu": 4, "mem_gb": 16, "gpu": 1})
    assert claim is not None and claim["phases"] == phases
    queue.finish(key, status="failed", claim_snapshot=claim,
        detail={"returncode": 7, "action_returncode": 7, "stdout": "failed fixture\n", "stderr": ""})
    terminal = json.loads(queue.records.item_path("failed", key).read_text())
    assert terminal["phases"] == phases
    attempt = json.loads(queue.records.attempt_path(terminal, 1).read_text())
    assert attempt["phases"] == phases
    assert pbwait.main([key, "--endings", "--wait-s", "0"]) == 1
    row = _one_json_line(capsys.readouterr())["endings"][0]
    assert row["phases"] == phases
    assert pbwait.main([key, "--json", "--wait-s", "0"]) == 1
    assert json.loads(capsys.readouterr().out)[0]["phases"] == phases


def test_phase_channel_keeps_identity_and_ending_contracts(sealed_fleet, capsys):
    work, queue, store = sealed_fleet
    read_fd, write_fd = os.pipe()
    try:
        assert pbrun.main(_consumer_argv(work, "--detach", "--client-events-fd", str(write_fd))) == 0
        raw = os.read(read_fd, 65536)
    finally:
        os.close(read_fd)
    decoder = client_channel.ClientEventDecoder(queue_root=store.queue_root, cas_root=store.cas_root)
    # Streaming callers need not align reads with JSON lines.
    for byte in raw:
        decoder.feed(bytes([byte]))
    result = decoder.finish(0)
    assert result.problem is None
    assert result.identity is not None and result.ending is not None
    assert decoder.events == 2
    assert decoder.phase_events > 2
    assert {"checkout_identity", "queue_publication"} <= decoder.phases.keys()


@pytest.mark.parametrize("damage", ["name", "state", "negative", "nonfinite", "missing", "extra"])
def test_invalid_phase_events_never_authorize_recovery(sealed_fleet, damage):
    _, _, store = sealed_fleet
    record = dict(schema=client_channel.CLIENT_EVENT_SCHEMA, event="phase",
                  phase="cas_stage", state="finished", seconds=1.0)
    if damage == "name":
        record["phase"] = "private input"
    elif damage == "state":
        record["state"] = []
    elif damage == "negative":
        record["seconds"] = -1
    elif damage == "nonfinite":
        record["seconds"] = float("inf")
    elif damage == "missing":
        del record["seconds"]
    else:
        record["argv"] = ["not allowed"]
    decoder = client_channel.ClientEventDecoder(queue_root=store.queue_root, cas_root=store.cas_root)
    decoder.feed((json.dumps(record) + "\n").encode())
    assert decoder.finish(0).problem is not None


def test_diagnostic_event_budget_preserves_the_control_channel(sealed_fleet, monkeypatch):
    from prismabuild import action_client
    _, _, store = sealed_fleet
    read_fd, write_fd = os.pipe()
    writer = client_channel.ClientEventWriter(write_fd)
    monkeypatch.setattr(client_channel, "MAX_SUBMISSION_PHASE_EVENTS", 2)
    event = submission_timing.SubmissionPhaseEvent("cas_stage", "started")
    writer.send(event)
    writer.send(submission_timing.SubmissionPhaseEvent("cas_stage", "finished", 1.0))
    writer.send(event)
    writer.send(action_client.ClientEnding(action_client.ClientCommand(0)))
    writer.close()
    try:
        decoder = client_channel.ClientEventDecoder(queue_root=store.queue_root, cas_root=store.cas_root)
        decoder.feed(os.read(read_fd, 65536))
        assert decoder.finish(0).problem is None
    finally:
        os.close(read_fd)


def test_timing_sample_uses_only_the_ordinary_ready_write(sealed_fleet, monkeypatch, capsys):
    work, queue, store = sealed_fleet
    original = submission.pool.PoolQueue(store.queue_root).records.__class__.replace_record_at
    writes = []
    def forbid_timing_refresh(self, path, record):
        if path.parent.name == "ready":
            writes.append(record)
            assert len(writes) == 1, "timing must not add another READY write"
        return original(self, path, record)
    monkeypatch.setattr(queue.records.__class__, "replace_record_at", forbid_timing_refresh)
    assert pbrun.main(_consumer_argv(work, "--detach")) == 0
    published = _one_json_line(capsys.readouterr())
    assert published["status"] == "submitted"
    ready = json.loads(submission.Path(published["submission"]).read_text())
    assert "queue_publication" in ready["phases"]


def test_worker_locks_bypass_client_timing_when_no_observer_is_active(tmp_path, monkeypatch):
    def unwanted_client_timing(*args, **kwargs):
        pytest.fail("worker lock entered client timing without a submitter")
    monkeypatch.setattr(posix_lock, "submission_phase", unwanted_client_timing)
    with posix_lock.held(tmp_path / "transition-locks" / "worker.lock") as acquired:
        assert acquired


def test_cleanup_phases_precede_the_one_client_ending(sealed_fleet, monkeypatch, capsys):
    from contextlib import contextmanager
    work, queue, store = sealed_fleet
    original = submission.write_window
    @contextmanager
    def window_with_lock_cleanup(*args, **kwargs):
        with original(*args, **kwargs):
            try:
                yield
            finally:
                with submission_timing.submission_phase("lock_wait"):
                    pass
    monkeypatch.setattr(submission, "write_window", window_with_lock_cleanup)
    read_fd, write_fd = os.pipe()
    try:
        assert pbrun.main(_consumer_argv(work, "--detach", "--client-events-fd", str(write_fd))) == 0
        raw = os.read(read_fd, 65536)
    finally:
        os.close(read_fd)
    decoder = client_channel.ClientEventDecoder(queue_root=store.queue_root, cas_root=store.cas_root)
    decoder.feed(raw)
    assert decoder.finish(0).problem is None
    assert json.loads(raw.splitlines()[-1])["event"] == "ending"
