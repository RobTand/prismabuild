"""Shared W4 lifetime checks; no PB submission or nested pytest execution.

The small lease below spells out the pending shared_sources fd interface. These
checks exercise kernel descriptor lifetime, not a serialized proof or a mocked
flock. The core launch hook and real lease API remain main-owned integration.
"""
from contextlib import contextmanager
import ctypes
import fcntl
import os
from pathlib import Path
import select
import signal
import subprocess
import sys
import time

import pytest

from prismabuild import _pbtest_layout as layout
from prismabuild import core, materialize, pool


@pytest.fixture
def attempt_inputs(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    local = tmp_path / "local"
    action = {
        "params": {"execution_layout": "pbtest-source-readonly-v1"},
        "task": {"result_path": "results/shard.json"},
    }
    return action, source, local


def _attempt(inputs):
    action, source, local = inputs
    return layout._attempt_environment(
        action, checkout_root=source, local_checkout_root=local)


@pytest.mark.parametrize("outcome", [
    None,
    {"status": "failed", "returncode": 1},
    {"status": "timeout", "returncode": 124},
    {"status": "withdrawn", "returncode": 125},
    {"status": "failed", "returncode": -signal.SIGKILL},
    {"status": "executed", "returncode": 0, "action_returncode": 1},
    {"status": "executed", "returncode": 0, "termination_reason": "memory_limit"},
    {"status": "executed", "returncode": 0, "action_survived_kill": True},
])
def test_unsuccessful_or_unacknowledged_attempt_retains_scratch(attempt_inputs, outcome):
    with _attempt(attempt_inputs) as environment:
        scratch = Path(environment["TMPDIR"])
        evidence = scratch / "partial-result"
        evidence.write_bytes(b"keep diagnostics")
        if outcome is not None:
            environment.record_outcome(outcome)
    assert evidence.read_bytes() == b"keep diagnostics"
    if outcome is not None:
        assert outcome["attempt_scratch"] == {
            "path": str(scratch), "retained": True, "reason": "attempt_unsuccessful"}


@pytest.mark.parametrize("error", [RuntimeError, KeyboardInterrupt, SystemExit])
def test_exception_after_success_acknowledgement_still_retains(attempt_inputs, error):
    outcome = {"status": "executed", "returncode": 0}
    with pytest.raises(error):
        with _attempt(attempt_inputs) as environment:
            scratch = Path(environment["TMPDIR"])
            environment.record_outcome(outcome)
            raise error()
    assert scratch.is_dir()
    assert outcome["attempt_scratch"]["retained"] is True


def test_success_cleanup_waits_for_context_exit_and_leaves_other_attempts(attempt_inputs):
    success = {"status": "executed", "returncode": 0}
    failed = {"status": "failed", "returncode": 1}
    with _attempt(attempt_inputs) as unsuccessful:
        old = Path(unsuccessful["TMPDIR"])
        unsuccessful.record_outcome(failed)
    with _attempt(attempt_inputs) as environment:
        scratch = Path(environment["TMPDIR"])
        assert scratch != old
        environment.record_outcome(success)
        assert scratch.is_dir()  # result publication/drain context still open
        assert old.is_dir()
    assert not scratch.exists()
    assert old.is_dir()
    assert success["attempt_scratch"] == {
        "path": str(scratch), "retained": False, "reason": "success_drained"}


@pytest.mark.parametrize("defect", [
    "missing-lock", "symlink-lock", "replaced-lock", "renamed-root", "replaced-root", "remove-tree"])
def test_cleanup_uncertainty_retains_without_reclassifying_success(
        attempt_inputs, monkeypatch, defect):
    outcome = {"status": "executed", "returncode": 0}
    with _attempt(attempt_inputs) as environment:
        scratch = Path(environment["TMPDIR"])
        lock = scratch / layout._ATTEMPT_READERS
        if defect == "missing-lock":
            lock.unlink()
        elif defect == "symlink-lock":
            lock.unlink()
            lock.symlink_to(attempt_inputs[1])
        elif defect == "replaced-lock":
            lock.unlink()
            lock.touch(mode=0o600)
        elif defect in ("renamed-root", "replaced-root"):
            moved = scratch.with_name(scratch.name + "-moved")
            scratch.rename(moved)
            if defect == "renamed-root":
                scratch.symlink_to(moved, target_is_directory=True)
            else:
                scratch.mkdir(mode=0o700)
                (moved / layout._ATTEMPT_READERS).rename(lock)
        else:
            def fail(*args, **kwargs):
                raise PermissionError("cleanup refused")
            from prismabuild import owned_tree
            monkeypatch.setattr(owned_tree, "remove_tree", fail)
        environment.record_outcome(outcome)
    assert scratch.exists()
    assert outcome["status"] == "executed"
    assert outcome["returncode"] == 0
    assert outcome["attempt_scratch"]["reason"] == "cleanup_unavailable"


@pytest.mark.parametrize("verdict", ["success", "worker-failure", "source-failure", "resource-failure"])
def test_pool_records_final_verdict_only_after_worker_drain_and_source_check(
        attempt_inputs, tmp_path, monkeypatch, verdict):
    action, source, local = attempt_inputs
    monkeypatch.setattr(pool, "LOCAL_CHECKOUT_ROOT", local)
    monkeypatch.setattr(pool, "_shared_execution_action", lambda item: action)
    queue = pool.PoolQueue(tmp_path / "queue")
    monkeypatch.setattr(queue, "_resource_profile", lambda *args: {})
    monkeypatch.setattr(queue, "_record_execution_layout", lambda *args, **kwargs: None)
    events = []

    class Lease:
        checkout_root = source

        def preflight(self, candidate):
            events.append("source-check")
            if verdict == "source-failure" and len(events) > 1:
                raise core.ActionContractError("source changed after execution")

    @contextmanager
    def shared(*args, **kwargs):
        yield Lease()
        events.append("source-released")

    monkeypatch.setattr(materialize, "_shared_source_checkout", shared)
    scratch = None

    def execute(*args, layout_environment, **kwargs):
        nonlocal scratch
        scratch = Path(layout_environment["TMPDIR"])
        (scratch / "diagnostic").write_text("worker state")
        events.append("worker-drained")
        return {"status": "failed" if verdict == "worker-failure" else "executed",
                "returncode": 1 if verdict == "worker-failure" else 0}

    monkeypatch.setattr(queue, "_execute_in_checkout", execute)
    item = {"action_key": "a" * 64, "cas_root": str(tmp_path / "cas")}
    if verdict == "resource-failure":
        item["resource_scope"] = "test-scope"
        monkeypatch.setattr(queue, "scope_for_record", lambda item: object())
        monkeypatch.setattr(queue, "_sample_resource_scope", lambda scope: {})
        monkeypatch.setattr(queue, "_resource_failure", lambda telemetry: "memory_limit")
    outcome = queue.execute(item)
    assert events == ["source-check", "worker-drained", "source-check", "source-released"]
    assert scratch.is_dir() == (verdict != "success")
    assert outcome["attempt_scratch"]["retained"] == (verdict != "success")
    assert outcome["status"] == ("executed" if verdict == "success" else "failed")
    if verdict == "resource-failure":
        assert outcome["termination_reason"] == "memory_limit"


class _ReaderLease:
    """Test-side implementation of _SourceLease.duplicate_reader_fd() -> int."""

    def __init__(self, reader):
        self.reader = reader
        self.duplicates = 0

    def duplicate_reader_fd(self):
        self.duplicates += 1
        return os.dup(self.reader)


@contextmanager
def _worker_pins(environment, source_lock):
    source_reader = os.open(source_lock, os.O_RDWR | os.O_CREAT | os.O_CLOEXEC, 0o600)
    try:
        fcntl.flock(source_reader, fcntl.LOCK_SH)
        lease = _ReaderLease(source_reader)
        with layout._attempt_lock(Path(environment["TMPDIR"]), fcntl.LOCK_SH) as reader:
            worker = layout._WorkerLayout(
                action={}, lease=lease, result_root=Path(environment["TMPDIR"]),
                environment=dict(environment), worker_launcher_identity=None,
                attempt_reader_descriptor=reader)
            try:
                yield worker, lease
            finally:
                worker._close()
    finally:
        os.close(source_reader)  # close-only, just like shared_sources reader


def test_missing_source_fd_api_is_not_permission_to_launch_unpinned(attempt_inputs, tmp_path):
    with _attempt(attempt_inputs) as environment:
        with _worker_pins(environment, tmp_path / "readers") as (worker, lease):
            lease.duplicate_reader_fd = None
            with pytest.raises(core.ActionContractError, match="descriptor handoff"):
                _ = worker.action_pass_fds


@pytest.mark.parametrize("request_pins", [False, True])
def test_worker_layout_holds_attempt_reader_through_source_verification(
        attempt_inputs, tmp_path, monkeypatch, request_pins):
    from prismabuild import action_result

    action, source, _ = attempt_inputs
    action["action_key"] = "a" * 64
    action["params"]["cwd"] = "."
    action["environment"] = {"variables": {}}
    action["task"].update(working_directory=".", definition_id="fleet/pbrun")
    monkeypatch.setattr(core, "validate_action", lambda candidate: candidate)
    monkeypatch.setattr(action_result, "bind_standard_capture_command", lambda candidate: None)
    source_lock = tmp_path / "readers"
    checks = []

    @contextmanager
    def shared(*args, **kwargs):
        reader = os.open(source_lock, os.O_RDWR | os.O_CREAT | os.O_CLOEXEC, 0o600)
        fcntl.flock(reader, fcntl.LOCK_SH)
        lease = _ReaderLease(reader)
        lease.checkout_root = source

        def preflight(candidate, *, worker_launcher_identity):
            checks.append(worker_launcher_identity)
            with pytest.raises(BlockingIOError):
                with layout._attempt_lock(scratch, fcntl.LOCK_EX | fcntl.LOCK_NB):
                    pass
            return {}

        lease.preflight = preflight
        try:
            yield lease
        finally:
            os.close(reader)

    monkeypatch.setattr(materialize, "_shared_source_checkout", shared)
    descriptors = ()
    with _attempt(attempt_inputs) as environment:
        scratch = Path(environment["TMPDIR"])
        for key, value in environment.items():
            monkeypatch.setenv(key, value)
        with layout._worker_layout(
                action, cas_root=tmp_path / "cas", checkout_root=source,
                worker_launcher_identity="test-launcher") as worker:
            if request_pins:
                descriptors = worker.action_pass_fds
            assert _source_is_busy(source_lock)
        assert checks == ["test-launcher", "test-launcher"]
        assert not _source_is_busy(source_lock)
        for descriptor in descriptors:
            with pytest.raises(OSError):
                os.fstat(descriptor)
        environment.record_outcome({"status": "executed", "returncode": 0})
    assert not scratch.exists()


def _source_is_busy(path):
    descriptor = os.open(path, os.O_RDWR | os.O_CLOEXEC)
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return True
        return False
    finally:
        os.close(descriptor)


def test_action_inherited_fds_pin_source_and_scratch_after_worker_closes(
        attempt_inputs, tmp_path):
    source_lock = tmp_path / "source-readers"
    success = {"status": "executed", "returncode": 0}
    child = None
    try:
        with _attempt(attempt_inputs) as environment:
            scratch = Path(environment["TMPDIR"])
            with _worker_pins(environment, source_lock) as (worker, lease):
                descriptors = worker.action_pass_fds
                assert worker.action_pass_fds == descriptors
                assert lease.duplicates == 1
                assert all(not os.get_inheritable(fd) for fd in descriptors)
                child = subprocess.Popen(
                    [sys.executable, "-c", "import os; os.read(0, 1)"],
                    stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                    pass_fds=descriptors)
            # All launcher copies are gone, but the action child still owns
            # both locks. Neither close may have issued LOCK_UN.
            assert _source_is_busy(source_lock)
            with pytest.raises(core.ActionContractError, match="no longer live"):
                _ = worker.action_pass_fds
            environment.record_outcome(success)
        assert scratch.is_dir()
        assert success["attempt_scratch"]["reason"] == "readers_present"
        child.communicate(b"x", timeout=5)
        assert child.returncode == 0
        assert not _source_is_busy(source_lock)
        # No new sweep: an attempt retained at completion remains available.
        assert scratch.is_dir()
    finally:
        if child is not None and child.poll() is None:
            child.kill()
            child.wait(timeout=5)


@contextmanager
def _subreaper():
    if sys.platform != "linux":
        pytest.skip("SIGKILL descendant check needs Linux child subreaper support")
    libc = ctypes.CDLL(None, use_errno=True)
    previous = ctypes.c_int()
    if libc.prctl(37, ctypes.byref(previous), 0, 0, 0) != 0:
        pytest.skip("cannot query child subreaper setting")
    if libc.prctl(36, 1, 0, 0, 0) != 0:
        pytest.skip("cannot adopt the killed worker's child")
    try:
        yield
    finally:
        libc.prctl(36, previous.value, 0, 0, 0)


def _reap(pid):
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        try:
            waited, _ = os.waitpid(pid, os.WNOHANG)
        except ChildProcessError:
            return
        if waited == pid:
            return
        time.sleep(0.01)
    pytest.fail("descendant did not exit")


def test_sigkill_launcher_cannot_release_descendants_source_pin(attempt_inputs, tmp_path):
    source_lock = tmp_path / "source-readers"
    launcher, descendant = None, None
    program = """import os, subprocess, sys, time
fds = tuple(map(int, sys.argv[1:]))
child = subprocess.Popen([sys.executable, '-c', 'import os,sys; os.read(int(sys.argv[1]), 1)', str(fds[-1])],
    pass_fds=fds, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
print(child.pid, flush=True)
time.sleep(60)
"""
    with _subreaper():
        release_read, release_write = os.pipe()
        try:
            with _attempt(attempt_inputs) as environment:
                scratch = Path(environment["TMPDIR"])
                with _worker_pins(environment, source_lock) as (worker, _):
                    pins = worker.action_pass_fds
                    launcher = subprocess.Popen(
                        [sys.executable, "-c", program, *map(str, (*pins, release_read))],
                        pass_fds=(*pins, release_read), stdout=subprocess.PIPE,
                        stderr=subprocess.DEVNULL, text=True)
                    assert select.select([launcher.stdout], [], [], 5)[0], "launcher did not start"
                    descendant = int(launcher.stdout.readline())
                launcher.kill()
                assert launcher.wait(timeout=5) == -signal.SIGKILL
                assert _source_is_busy(source_lock)
                environment.record_outcome({"status": "failed", "returncode": -signal.SIGKILL})
            assert scratch.is_dir()
            assert _source_is_busy(source_lock)
            os.write(release_write, b"x")
            _reap(descendant)
            descendant = None
            assert not _source_is_busy(source_lock)
            with layout._attempt_lock(scratch, fcntl.LOCK_EX | fcntl.LOCK_NB):
                pass
        finally:
            if launcher is not None and launcher.poll() is None:
                launcher.kill()
                launcher.wait(timeout=5)
            if descendant is not None:
                try:
                    os.kill(descendant, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                _reap(descendant)
            if launcher is not None and launcher.stdout is not None:
                launcher.stdout.close()
            os.close(release_read)
            os.close(release_write)
