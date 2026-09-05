"""Leader exit is not proof the action group is gone.

Issue #71: `_terminate_process_group` polled and waited on the group leader
only, so a descendant that ignores SIGTERM while the leader accepts it kept
running after PrismaBuild reported the timeout, holding the compute and its
inherited copy of the output lock.  The old early return after the leader's
`wait()` skipped SIGKILL entirely.

Every process these tests start is in a group the test itself signals down and
reaps in `finally`; nothing outside the test's own children is touched.
"""

from __future__ import annotations

import ctypes
import functools
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from prismabuild import core as pb  # noqa: E402

TEST_GRACE_S = 0.3
CONDITION_BUDGET_S = 30.0

# A leader that forks a SIGTERM-ignoring child.  The child records its pid and
# its process group so the test can prove group membership and then reap it.
LEADER_SOURCE = """
import os, pathlib, signal, sys, time
child = os.fork()
if child == 0:
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    pathlib.Path(sys.argv[1]).write_text(f"{os.getpid()} {os.getpgrp()}")
    time.sleep(3600)
else:
    if sys.argv[2] == "exit":
        # Let the child publish its marker, then leave the group behind.
        while not pathlib.Path(sys.argv[1]).exists():
            time.sleep(0.005)
        sys.exit(0)
    time.sleep(3600)
"""


@pytest.fixture
def subreaper():
    """Adopt the test's orphaned grandchildren so pytest can reap them.

    Without this the killed descendant is reparented to init and the test
    cannot observe whether it died.  The flag is cleared afterwards so the
    rest of the pytest process is unchanged.
    """

    libc = ctypes.CDLL(None, use_errno=True)
    assert libc.prctl(36, 1, 0, 0, 0) == 0        # PR_SET_CHILD_SUBREAPER
    try:
        yield
    finally:
        libc.prctl(36, 0, 0, 0, 0)


def _await(condition) -> None:
    deadline = time.monotonic() + CONDITION_BUDGET_S
    while not condition():
        assert time.monotonic() < deadline, "condition never became true"
        time.sleep(0.005)


def _await_marker(marker: Path) -> tuple[int, int]:
    """The child's pid and process group, once the marker actually holds them.

    ``write_text`` creates the file and then writes it, so a reader waiting on
    ``exists()`` can win the race and parse an empty file. That is a test
    failing on its own timing rather than on the behaviour it covers, and it
    is what made this file fail about one run in six on the x86 box while
    passing on the Sparks. Waiting for two integers waits for the write.
    """

    fields: list[str] = []

    def written() -> bool:
        nonlocal fields
        try:
            fields = marker.read_text().split()
        except OSError:
            return False
        return len(fields) == 2 and all(field.isdigit() for field in fields)

    _await(written)
    return int(fields[0]), int(fields[1])


def _start_leader(marker: Path, leader_mode: str) -> subprocess.Popen[bytes]:
    return subprocess.Popen(
        [sys.executable, "-c", LEADER_SOURCE, str(marker), leader_mode],
        stdin=subprocess.DEVNULL,
        start_new_session=True,
    )


def _reap(pgid: int | None, pids: list[int]) -> None:
    if pgid is not None:
        try:
            os.killpg(pgid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            pass
    for pid in pids:
        try:
            os.waitpid(pid, 0)
        except (ChildProcessError, OSError):
            pass


def _alive(pid: int) -> bool:
    """Whether ``pid`` names a process that has not exited.

    A reaped-by-nobody zombie still answers ``kill(pid, 0)``, so read its
    state out of ``/proc`` instead.
    """

    try:
        with open(f"/proc/{pid}/stat", "rb") as handle:
            raw = handle.read()
    except OSError:
        return False
    _, _, tail = raw.rpartition(b")")
    fields = tail.split()
    return bool(fields) and fields[0] not in {b"Z", b"X", b"x"}


@pytest.mark.parametrize("leader_mode", ["stay", "exit"])
def test_termination_kills_a_term_ignoring_descendant(
    tmp_path: Path, subreaper, leader_mode: str,
) -> None:
    """The TERM, grace, KILL protocol runs against the group, not the leader.

    ``stay`` is the reported case: the leader accepts SIGTERM and dies inside
    the grace, so the old code returned before SIGKILL.  ``exit`` covers the
    leader-already-exited branch: the helper is handed a group whose leader is
    gone before it is called at all.
    """

    marker = tmp_path / "child.txt"
    process = _start_leader(marker, leader_mode)
    child_pid: int | None = None
    pgid: int | None = None
    try:
        child_pid, pgid = _await_marker(marker)
        assert pgid == process.pid
        assert os.getpgid(child_pid) == pgid
        if leader_mode == "exit":
            process.wait(timeout=CONDITION_BUDGET_S)

        pb._terminate_process_group(process, grace_s=TEST_GRACE_S)

        assert process.poll() is not None
        assert not _alive(child_pid), (
            "a same-group descendant outlived the termination protocol"
        )
        assert not pb._process_group_has_live_members(pgid)
    finally:
        # ``process.pid`` rather than ``pgid``: ``start_new_session`` makes the
        # leader its own group leader, so the group is known the moment
        # ``Popen`` returns. ``pgid`` is still ``None`` whenever the body
        # raises before ``_await_marker`` returns, and reaping ``None`` left
        # the SIGTERM-ignoring child running for its full hour.
        _reap(process.pid, [child_pid] if child_pid is not None else [])


def test_termination_returns_promptly_when_the_group_is_compliant(
    tmp_path: Path, subreaper,
) -> None:
    """A group that honours SIGTERM must not wait out the grace budget."""

    checkout = tmp_path / "checkout"
    checkout.mkdir()
    source = "import time; time.sleep(3600)"
    process = subprocess.Popen(
        [sys.executable, "-c", source],
        stdin=subprocess.DEVNULL,
        start_new_session=True,
    )
    try:
        started = time.monotonic()
        pb._terminate_process_group(process, grace_s=CONDITION_BUDGET_S)
        elapsed = time.monotonic() - started
    finally:
        _reap(process.pid, [])

    assert process.poll() is not None
    assert elapsed < CONDITION_BUDGET_S / 2


def test_action_timeout_kills_a_term_ignoring_descendant(
    tmp_path: Path, subreaper, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The end-to-end contract: a timed-out action leaves nothing running."""

    checkout = tmp_path / "checkout"
    checkout.mkdir()
    (checkout / "code.txt").write_text("closure\n")
    marker = tmp_path / "action-child.txt"
    action = pb.seal_action(
        {
            "schema": pb.ACTION_SCHEMA_V2,
            "task": {
                "definition_id": "test/timeout-group",
                "definition_version": "v1",
                "task_class": "generation",
                "determinism": "deterministic",
                "artifact_family": "generic",
                "artifact_kind": "generic",
                "argv": [
                    sys.executable, "-c", LEADER_SOURCE, str(marker), "stay",
                ],
                "working_directory": ".",
                "result_path": "result.bin",
            },
            "inputs": [],
            "code_closure": pb.build_code_closure(checkout, ["code.txt"]),
            "params": {},
            "environment": {"variables": {}, "toolchain": {}},
            "execution_scope": {
                "portability": "portable",
                "platform_key": None,
                "host_class": None,
            },
        }
    )
    monkeypatch.setattr(
        pb,
        "_terminate_process_group",
        functools.partial(pb._terminate_process_group, grace_s=TEST_GRACE_S),
    )

    child_pid: int | None = None
    pgid: int | None = None
    try:
        with pytest.raises(pb.LocalActionError, match="timed out"):
            pb.run_local_action(
                action,
                cas_root=tmp_path / "cas",
                checkout_root=checkout,
                timeout_seconds=0.5,
            )
        child_pid, pgid = _await_marker(marker)
        assert not _alive(child_pid), (
            "the action's descendant outlived the reported timeout"
        )
    finally:
        _reap(pgid, [child_pid] if child_pid is not None else [])
