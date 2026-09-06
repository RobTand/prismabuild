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

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from prismabuild import core as pb, pool, resource_scope  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

import pbrun  # noqa: E402

WORKER_LOOP = Path(__file__).resolve().parents[1] / "tools" / "fleet" / "worker_loop.py"


def _await(predicate, *, timeout_s: float = 30.0) -> bool:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return predicate()
def _await_pid(path: Path, *, worker: subprocess.Popen | None = None, timeout_s: float = 30.0) -> int:
    """The pid a helper wrote, once the file actually holds one.

    ``write_text`` creates the file before it writes it, so waiting on
    ``exists()`` and then calling ``int()`` can read an empty file and raise
    ``invalid literal for int() with base 10: ''``. Waiting for a parsable pid
    waits for the write instead.
    """

    pid = 0

    def written() -> bool:
        nonlocal pid
        if worker is not None and worker.poll() is not None:
            output, _ = worker.communicate(timeout=5)
            pytest.fail(f"worker exited before the action started:\n{output}")
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
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    # Exercise the real containment contract: an immutable request, canonical
    # worker and sealed resource demand. A stub with a fabricated CAS key is
    # refused before launch by the live worker's required scope setup.
    task = checkout / "task.py"
    task.write_text(
        "import os, pathlib, time\n"
        f"pathlib.Path({str(pidfile)!r}).write_text(str(os.getpid()))\n"
        "time.sleep(600)\n"
    )
    demand = {"cpu": 1, "mem_gb": 1}
    action = pb.seal_action({
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {
            "definition_id": "tests/withdraw-running-action",
            "definition_version": "v1", "task_class": "generation",
            "determinism": "deterministic", "artifact_family": "generic",
            "artifact_kind": "generic", "argv": [sys.executable, "task.py"],
            "working_directory": ".", "result_path": "result.bin",
        },
        "inputs": [], "code_closure": pb.build_code_closure(checkout, ["task.py"]),
        "params": {"demand": demand},
        "environment": {"variables": {}, "toolchain": {}},
        "execution_scope": {"portability": "portable", "platform_key": None,
                            "host_class": None},
    })
    key = action["action_key"]
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    cas.publish_action_request(action)
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    queue.publish(
        action_key=key, cas_root=cas.root, checkout_root=checkout,
        worker_script=WORKER_LOOP.parents[1] / "prismabuild_worker.py",
        resources=demand, max_attempts=1,
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
        sys.executable, str(bootstrap), "--once", "--all-cores", "--assume-idle", "--class", "x86",
        "--gpu-slots", "0", "--mem-gb", "4", "--cpu-slots", "2",
        "--poll-s", "0.05", "--python", sys.executable,
    ], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    try:
        action_pid = _await_pid(pidfile, worker=worker)
        assert _await(lambda: (queue.lease_path(key).exists() and json.loads(
            queue.lease_path(key).read_text()).get("child_pid") is not None))
        assert queue.ledger(host).available().get("cpu", 0) == 1, (
            "one of two cpu tokens is held while the action runs")
        claim = json.loads(queue.item_path(pool.CLAIMED, key).read_text())
        control = claim["resource_scope"]
        assert control["memory_max_bytes"] == 1024 ** 3
        membership = Path(f"/proc/{action_pid}/cgroup").read_text()
        assert control["scope_id"] in membership

        assert pbrun.withdraw_main(
            queue, [key[:12]], reason="ten merges stale", by=f"rob@{host}") == 0

        output, _ = worker.communicate(timeout=60.0)
        assert worker.returncode == 0, output

        assert _await(lambda: not pool._process_alive(action_pid)), (
            "the action outlived the operator's decision")

        # One place, and it is not the failure record.
        filed = json.loads(queue.item_path(pool.WITHDRAWN, key).read_text())
        assert filed["status"] == "withdrawn"
        assert filed["reason"] == "ten merges stale"
        for state in (pool.READY, pool.CLAIMED, pool.DONE, pool.FAILED):
            assert list(queue.dir(state).glob("*.json")) == [], state
        assert not queue.lease_path(key).exists()

        # And the box is whole again: every token back, nothing widowed.
        ledger = queue.ledger(host)
        assert ledger.held_keys() == []
        assert ledger.available() == ledger.capacity()
        stopped = resource_scope.broker_request({
            "op": "status", "action_key": key,
            "nonce": control["nonce"], "token": control["token"],
        })
        assert stopped.get("released") is True
        assert _await(lambda: not Path(control["cgroup_path"]).exists())

        # The submitter is told, rather than waiting out ``--wait-s``.
        rc = pbrun.await_outcome(queue, key, wait_s=1.0)
        assert rc == pbrun.WITHDRAWN_EXIT
    finally:
        if worker.poll() is None:
            # This child belongs to the test. Do not leak it on an assertion;
            # the pidfile fixture separately tears down the action session.
            worker.kill()
        worker.communicate()
