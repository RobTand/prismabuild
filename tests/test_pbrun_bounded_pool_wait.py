"""The synchronous pool wait must leave a blocked mount read behind safely.

The FIFO is a private stand-in for a hard-NFS terminal-record read: directory
listing succeeds, then opening the named record blocks.  These tests exercise
the actual forked reader boundary; they never point at a fleet queue.
"""
from __future__ import annotations

import fcntl
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import pbrun  # noqa: E402


KEY = "b" * 64


class Queue:
    def __init__(self, root: Path) -> None:
        self.root = root
        for state in ("done", "failed", "withdrawn"):
            (root / state).mkdir(parents=True)

    def item_path(self, state: str, key: str) -> Path:
        return self.root / state / f"{key}.json"

    def withdrawal_decisions(self, key: str, **_kwargs) -> list:
        return []


def _ending(status: str, returncode: int) -> dict:
    return {
        "action_key": KEY,
        "status": status,
        "finished_host": "worker",
        "attempts": 1,
        "detail": {"returncode": returncode, "stdout": "ok\\n", "stderr": ""},
    }


@pytest.mark.parametrize(
    ("state", "status", "returncode", "expected"),
    [
        ("done", "executed", 0, 0),
        ("failed", "failed", 9, 9),
        ("withdrawn", "withdrawn", 143, pbrun.WITHDRAWN_EXIT),
    ],
)
def test_bounded_wait_keeps_healthy_terminal_verdicts(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], state: str,
    status: str, returncode: int, expected: int,
) -> None:
    queue = Queue(tmp_path / "queue")
    queue.item_path(state, KEY).write_text(
        json.dumps(_ending(status, returncode)), encoding="utf-8")

    assert pbrun.await_outcome(queue, KEY, wait_s=0) == expected
    assert "unavailable pool outcome" not in capsys.readouterr().err


def _blocked_fifo_queue(tmp_path: Path) -> Queue:
    queue = Queue(tmp_path / "queue")
    os.mkfifo(queue.item_path("done", KEY))
    return queue


def test_fifo_terminal_read_is_bounded_and_is_not_a_wait_timeout(
    tmp_path: Path,
) -> None:
    """A baseline times out this isolated caller; fixed source returns 74.

    Keeping the watchdog inside the regression turns the unchanged baseline
    result into a pytest assertion with proof that it entered ``await_outcome``
    rather than letting the enclosing PrismaBuild action time out.
    """

    root = tmp_path / "queue"
    source = Path(__file__).resolve().parents[1]
    code = f'''\
import os, sys
from pathlib import Path
sys.path.insert(0, {str(source / "tools" / "fleet")!r})
import pbrun
pbrun.OUTCOME_READ_TIMEOUT_S = 0.05
root = Path({str(root)!r})
for state in ("done", "failed", "withdrawn"):
    (root / state).mkdir(parents=True)
class Queue:
    def item_path(self, state, key):
        return root / state / f"{{key}}.json"
    def withdrawal_decisions(self, key, **kwargs):
        return []
key = {KEY!r}
os.mkfifo(Queue().item_path("done", key))
print("entered await_outcome", flush=True)
print(pbrun.await_outcome(Queue(), key, wait_s=0), flush=True)
'''
    try:
        completed = subprocess.run(
            [sys.executable, "-c", code], text=True, capture_output=True,
            timeout=1.0, check=False)
    except subprocess.TimeoutExpired as exc:
        stdout = (exc.stdout or "").decode() if isinstance(exc.stdout, bytes) else (exc.stdout or "")
        pytest.fail(
            "await_outcome exceeded its bounded caller budget; stdout=" + stdout)
    assert completed.returncode == 0
    assert completed.stdout == f"entered await_outcome\n{pbrun.RECORD_WRITE_FAILED_EXIT}\n"
    assert "pool outcome observation timed out" in completed.stderr
    assert "gave up waiting" not in completed.stderr


def test_immutable_verification_timeout_is_loud_non_success(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    queue = Queue(tmp_path / "queue")
    outcome = _ending("executed", 0)
    queue.item_path("done", KEY).write_text(json.dumps(outcome), encoding="utf-8")
    monkeypatch.setattr(pbrun, "OUTCOME_READ_TIMEOUT_S", 0.05)

    def blocked_summary(*_args, **_kwargs):
        time.sleep(30)

    monkeypatch.setattr(pbrun, "outcome_summary", blocked_summary)
    assert pbrun.await_outcome(queue, KEY, wait_s=0) == pbrun.RECORD_WRITE_FAILED_EXIT
    assert "pool outcome verification timed out" in capsys.readouterr().err


def test_parent_carries_preemption_successor_between_bounded_observations(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue = Queue(tmp_path / "queue")
    outcome = _ending("executed", 0)
    path = queue.item_path("done", KEY)
    path.write_text(json.dumps(outcome), encoding="utf-8")
    seen: list[float | None] = []

    def observation(_queue, _key, generation, *, budget_s):
        assert budget_s > 0
        seen.append(generation)
        if len(seen) == 1:
            return None, 200.0
        return (path, outcome), generation

    monkeypatch.setattr(pbrun, "bounded_outcome_observation", observation)
    assert pbrun.await_outcome(queue, KEY, wait_s=1, generation=100.0) == 0
    assert seen == [100.0, 200.0]


def test_eof_payload_with_retained_reader_refuses_before_verification(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue = Queue(tmp_path / "queue")
    queue.item_path("done", KEY).write_text(
        json.dumps(_ending("executed", 0)), encoding="utf-8")
    retained: list[int] = []

    def retain(pid, _section, _started, abandoned):
        retained.append(pid)
        abandoned.append({"pid": pid, "section": "pool outcome observation",
                          "starttime_ticks": pbrun.pbstatus._starttime_ticks(pid)})

    def never_verify(*_args, **_kwargs):
        raise AssertionError("a retained observation reader must stop the wait")

    monkeypatch.setattr(pbrun.pbstatus, "_reap_within", lambda *_args: False)
    monkeypatch.setattr(pbrun.pbstatus, "_stop_reader", retain)
    monkeypatch.setattr(pbrun, "bounded_outcome_render", never_verify)
    try:
        assert pbrun.await_outcome(queue, KEY, wait_s=0) == pbrun.RECORD_WRITE_FAILED_EXIT
        assert len(retained) == 1
    finally:
        for pid in retained:
            try:
                os.waitpid(pid, 0)
            except ChildProcessError:
                pass


def test_retained_outcome_reader_names_exact_identity_and_releases_parent_fds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    queue = _blocked_fifo_queue(tmp_path)
    retained: list[int] = []

    def retain(pid, _section, _started, abandoned):
        retained.append(pid)
        abandoned.append({"pid": pid, "section": "pool outcome observation",
                          "starttime_ticks": pbrun.pbstatus._starttime_ticks(pid)})

    monkeypatch.setattr(pbrun, "OUTCOME_READ_TIMEOUT_S", 0.05)
    monkeypatch.setattr(pbrun.pbstatus, "_stop_reader", retain)
    held = (tmp_path / "parent.lock").open("w")
    fcntl.flock(held, fcntl.LOCK_EX)
    try:
        assert pbrun.await_outcome(queue, KEY, wait_s=0) == pbrun.RECORD_WRITE_FAILED_EXIT
        assert len(retained) == 1
        shown = capsys.readouterr().err
        assert '"pid": ' + str(retained[0]) in shown
        assert '"starttime_ticks": null' not in shown
        held.close()
        with (tmp_path / "parent.lock").open("w") as contender:
            fcntl.flock(contender, fcntl.LOCK_EX | fcntl.LOCK_NB)
    finally:
        held.close()
        for pid in retained:
            try:
                done, _ = os.waitpid(pid, os.WNOHANG)
                if not done:
                    os.kill(pid, signal.SIGKILL)
                    os.waitpid(pid, 0)
            except ChildProcessError:
                pass


def test_interrupt_reaps_the_owned_outcome_reader(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue = _blocked_fifo_queue(tmp_path)
    children: list[int] = []
    fork = pbrun.pbstatus.os.fork

    def tracked_fork():
        pid = fork()
        if pid:
            children.append(pid)
        return pid

    def interrupt(*_args):
        raise KeyboardInterrupt

    monkeypatch.setattr(pbrun.pbstatus.os, "fork", tracked_fork)
    monkeypatch.setattr(pbrun.pbstatus.select, "select", interrupt)
    with pytest.raises(KeyboardInterrupt):
        pbrun.await_outcome(queue, KEY, wait_s=0)
    assert len(children) == 1
    with pytest.raises(ChildProcessError):
        os.waitpid(children[0], os.WNOHANG)
