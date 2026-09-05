"""A real worker loop, a real action, and an operator who changes their mind.

The unit tests take the pieces apart; this one puts them back together, because
the failure being fixed was never in a piece.  Every step of the hand-edit had a
correct-looking primitive underneath it -- ``finish`` filed the outcome,
``reap_stale`` recovered the lease, the kill reached the group -- and the
cancellation still lost, because nothing joined them up.  So what is asserted
here is the join: a worker loop that survives the withdrawal and goes back to
polling, an action that is actually dead, capacity that is actually back, and a
queue in which the cancelled work exists in exactly one place and it is not
``failed``.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import socket
import signal
import sys
import subprocess
import time
import uuid

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from prismabuild import pool  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

import pbrun  # noqa: E402

WORKER_LOOP = Path(__file__).resolve().parents[1] / "tools" / "fleet" / "worker_loop.py"
# Unique per process; see the note in ``test_pool_withdraw``.
KEY = uuid.uuid4().hex + uuid.uuid4().hex


def _await(predicate, *, timeout_s: float = 30.0) -> bool:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return predicate()
def _await_pid(path: Path, *, timeout_s: float = 30.0) -> int:
    """The pid a helper wrote, once the file actually holds one.

    ``write_text`` creates the file before it writes it, so waiting on
    ``exists()`` and then calling ``int()`` can read an empty file and raise
    ``invalid literal for int() with base 10: ''``. Waiting for a parsable pid
    waits for the write instead.
    """

    pid = 0

    def written() -> bool:
        nonlocal pid
        try:
            text = path.read_text().strip()
        except OSError:
            return False
        if not text.isdigit():
            return False
        pid = int(text)
        return True

    assert _await(written, timeout_s=timeout_s), f"nothing wrote a pid to {path}"
    return pid


@pytest.fixture()
def pidfile(tmp_path: Path):
    """Where the action records its pid -- and the sweep for it.

    A failure partway through this test would otherwise leave a ``sleep 600``
    running on a box other agents are using, and ``find_launcher_pids`` scans
    every process on the box: a leak from one run is something a later run can
    find and signal.
    """

    path = tmp_path / "action.pid"
    yield path
    try:
        pid = int(path.read_text())
    except (OSError, ValueError):
        return
    for shot in (lambda: os.killpg(pid, signal.SIGKILL), lambda: os.kill(pid, signal.SIGKILL)):
        try:
            shot()
        except OSError:
            pass


def test_an_operator_stops_a_running_action_and_the_worker_carries_on(
    tmp_path: Path, pidfile: Path
) -> None:
    stub = tmp_path / "stub_worker.py"
    # Shaped like the real worker: ``core.run_local_action`` puts the action in
    # its own session, which is why the launcher is the wrong thing to signal.
    stub.write_text(
        "import pathlib, subprocess, sys\n"
        "child = subprocess.Popen(['sleep', '600'], start_new_session=True)\n"
        f"pathlib.Path({str(pidfile)!r}).write_text(str(child.pid))\n"
        "sys.exit(child.wait())\n"
    )

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    queue.publish(
        action_key=KEY, cas_root=str(tmp_path / "cas"),
        checkout_root=str(tmp_path), worker_script=str(stub),
        resources={"cpu": 1, "mem_gb": 1},
    )

    host = socket.gethostname()
    # main owns process signals. Run it in a process, as the supervisor does,
    # so withdrawal exercises the real SIGTERM handler as well as the queue.
    bootstrap = tmp_path / "run_worker.py"
    bootstrap.write_text(f"""
import sys
from pathlib import Path
sys.path.insert(0, {str(WORKER_LOOP.parents[2] / 'src')!r})
sys.path.insert(0, {str(WORKER_LOOP.parent)!r})
import worker_loop as wl
wl.SH = Path({str(tmp_path)!r})
wl.loaded_runtime_commit = lambda: 'deadbeef'
wl.published_commit = lambda: 'deadbeef'
raise SystemExit(wl.main())
""")
    worker = subprocess.Popen([
        sys.executable, str(bootstrap), "--once", "--all-cores", "--class", "x86",
        "--gpu-slots", "0", "--mem-gb", "4", "--cpu-slots", "2",
        "--poll-s", "0.05", "--python", sys.executable,
    ], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    try:
        action_pid = _await_pid(pidfile)
        assert _await(lambda: (queue.lease_path(KEY).exists() and json.loads(
            queue.lease_path(KEY).read_text()).get("child_pid") is not None))
        assert queue.ledger(host).available().get("cpu", 0) == 1, (
            "one of two cpu tokens is held while the action runs")

        assert pbrun.withdraw_main(
            queue, [KEY[:12]], reason="ten merges stale", by=f"rob@{host}") == 0

        output, _ = worker.communicate(timeout=60.0)
        assert worker.returncode == 0, output

        assert _await(lambda: not pool._process_alive(action_pid)), (
            "the action outlived the operator's decision")

        # One place, and it is not the failure record.
        filed = json.loads(queue.item_path(pool.WITHDRAWN, KEY).read_text())
        assert filed["status"] == "withdrawn"
        assert filed["reason"] == "ten merges stale"
        for state in (pool.READY, pool.CLAIMED, pool.DONE, pool.FAILED):
            assert list(queue.dir(state).glob("*.json")) == [], state
        assert not queue.lease_path(KEY).exists()

        # And the box is whole again: every token back, nothing widowed.
        ledger = queue.ledger(host)
        assert ledger.held_keys() == []
        assert ledger.available() == ledger.capacity()

        # The submitter is told, rather than waiting out ``--wait-s``.
        rc = pbrun.await_outcome(queue, KEY, wait_s=1.0)
        assert rc == pbrun.WITHDRAWN_EXIT
    finally:
        if worker.poll() is None:
            # This child belongs to the test. Do not leak it on an assertion;
            # the pidfile fixture separately tears down the action session.
            worker.kill()
        worker.communicate()
