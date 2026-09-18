"""The payload the resource daemon forked is nobody's descendant here.

``resource_exec.py`` hands the broker its stdio and waits on the resources
socket; the broker forks the payload.  So the pid the pool holds is a proxy
with no children, and its liveness is not the payload's.  Action
``766d7ae5e0382b755a1189d4c1c3a42407d90fdb3cea56898b02b26855908489`` is what
that cost: ``launcher_alive: true`` and ``stdout_bytes: 218`` for the whole
3600 s ceiling, while a pytest child burned 15% of a core in a futex wait.  A
shard that hung 31 s in was indistinguishable from one that was working (#600).

The cgroup the broker put the payload in is what this worker and that payload
have in common, so these cases build one -- a directory with the files
``resource_scope`` reads and a real process's pid in ``cgroup.procs`` -- and
ask the observation what it says about it.  The pid is a live process of this
test's own, not a fixture number, because "alive" has to mean the kernel
answered, not that a file said so.

The other half is refusal.  An unreadable cgroup is not an empty one, and a
worker that cannot see the payload must say ``unobserved`` rather than report
a liveness it invented.
"""

from __future__ import annotations

import subprocess
import sys
import uuid
from pathlib import Path

import pytest

from prismabuild import pool


class _Scope:
    """As much of a resource scope as the observation reads: its cgroup."""

    def __init__(self, cgroup_path: Path | None) -> None:
        self.cgroup_path = cgroup_path


def _cgroup(root: Path, pids, *, cpu_usec: int = 1_500_000,
            user_usec: int = 900_000, system_usec: int = 600_000) -> Path:
    """A group carrying what ``resource_scope`` reads out of a real one."""

    root.mkdir(parents=True, exist_ok=True)
    (root / "cgroup.procs").write_text(
        "".join(f"{pid}\n" for pid in pids), encoding="utf-8")
    (root / "cpu.stat").write_text(
        f"usage_usec {cpu_usec}\nuser_usec {user_usec}\n"
        f"system_usec {system_usec}\n", encoding="utf-8")
    (root / "memory.current").write_text("125829120\n", encoding="utf-8")
    (root / "memory.peak").write_text("125829120\n", encoding="utf-8")
    (root / "memory.events").write_text("oom_kill 0\n", encoding="utf-8")
    (root / "memory.events.local").write_text("oom 0\n", encoding="utf-8")
    return root


class _Launcher:
    """A proxy that is alive and has written nothing since it started.

    Standing in for ``resource_exec.py``: ``poll()`` is ``None`` and its pipes
    are quiet, which is exactly the state the hung action's record showed.
    """

    returncode = None

    def poll(self):
        return None


@pytest.fixture
def silent_child(tmp_path: Path):
    """A real process that holds a cgroup and prints nothing, then is reaped."""

    proc = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(30)"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        yield proc
    finally:
        proc.kill()
        proc.wait(timeout=10)


def test_the_observation_carries_the_daemon_spawned_child_and_its_silence(
        tmp_path: Path, silent_child: subprocess.Popen) -> None:
    """main: the record names the payload's pid, liveness, CPU and silence.

    branch: a claim whose child has gone quiet while its CPU advances is
    distinguishable from one that is working, which the launcher's own
    liveness could never say.
    """

    scope = _Scope(_cgroup(tmp_path / "scope", [silent_child.pid]))
    spoke = pool._now() - 120.0
    observed = pool._observe_execution(
        _Launcher(), {"stdout_bytes": 218, "stderr_bytes": 0,
                      "last_output_unix": spoke},
        stdout=b"x" * 218, scope=scope)

    child = observed["child"]
    assert child["source"] == "resource-scope-cgroup"
    assert silent_child.pid in child["pids"], (
        "the record must name the pid the daemon forked; the pool's own pid "
        "is the proxy, and #600 is what believing it costs")
    assert child["pid_count"] == 1
    assert child["alive"] is True
    assert child["cpu_seconds"] == pytest.approx(1.5)
    assert child["cpu_user_seconds"] == pytest.approx(0.9)
    assert child["cpu_system_seconds"] == pytest.approx(0.6)
    assert child["silent_s"] == pytest.approx(120.0, abs=5.0), (
        "silence is measured against the last output observed, which is the "
        "half of the signal the CPU counter cannot give")
    # The launcher half is unchanged: this adds a view, it does not replace one.
    assert observed["source"] == "launcher-pipes"
    assert observed["launcher_alive"] is True


def test_a_child_that_has_exited_is_not_reported_alive(
        tmp_path: Path) -> None:
    """A pid in a stale ``cgroup.procs`` is not a running process.

    branch: liveness is read from the kernel per pid, so a group listing a
    reaped pid reports the child gone rather than inheriting the file's word
    for it.
    """

    proc = subprocess.Popen([sys.executable, "-c", "pass"])
    proc.wait(timeout=30)
    scope = _Scope(_cgroup(tmp_path / "scope", [proc.pid]))

    child = pool._observe_execution(_Launcher(), scope=scope)["child"]

    assert child["source"] == "resource-scope-cgroup"
    assert child["alive"] is False


def test_an_empty_scope_is_observed_as_empty_not_as_unobserved(
        tmp_path: Path) -> None:
    """A readable group with nothing in it is a fact, and says so.

    branch: this is the one case that may report ``alive: False`` without a
    pid, because the worker did read the group.
    """

    scope = _Scope(_cgroup(tmp_path / "scope", []))

    child = pool._observe_execution(_Launcher(), scope=scope)["child"]

    assert child["source"] == "resource-scope-cgroup"
    assert child["pids"] == []
    assert child["alive"] is False


@pytest.mark.parametrize("scope", [None, _Scope(None)])
def test_a_scope_that_cannot_be_read_reports_unobserved_not_a_liveness(
        scope) -> None:
    """main: no cgroup means no claim about the child, ever.

    branch: the record carries ``source: unobserved`` and no ``alive`` field,
    so a reader cannot mistake "we could not look" for "nothing is running"
    -- which is the failure mode that let ``766d7ae5...`` look healthy.
    """

    child = pool._observe_execution(_Launcher(), scope=scope)["child"]

    assert child["source"] == "unobserved"
    assert "alive" not in child


def test_an_unreadable_cgroup_is_not_an_empty_one(tmp_path: Path) -> None:
    """A group this worker cannot open says unobserved, not empty.

    branch: ``scope_pids`` records the refusal, and an empty result beside a
    refusal is indistinguishable from a departed payload, so neither is
    asserted.
    """

    child = pool._observe_execution(
        _Launcher(), scope=_Scope(tmp_path / "absent"))["child"]

    assert child["source"] == "unobserved"
    assert "alive" not in child
    assert child.get("errors"), (
        "an unobserved child must say what stopped the observation")


def test_execute_hands_its_scope_to_the_observation(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The field is worthless if the execution loop never passes the scope.

    branch: every sample ``execute`` takes is taken with the attempt's scope,
    so an action that has one is observed through it rather than through the
    launcher alone.
    """

    script = tmp_path / "worker.py"
    script.write_text("print('done', flush=True)\n", encoding="utf-8")
    queue = pool.PoolQueue(tmp_path / "queue")
    key = uuid.uuid4().hex * 2
    queue.publish(action_key=key, cas_root="/cas", checkout_root=tmp_path,
                  worker_script=script)
    item = queue.claim()

    seen: list[bool] = []
    original = pool._observe_execution

    def observe(process, previous=None, **kwargs):
        seen.append("scope" in kwargs)
        return original(process, previous, **kwargs)

    monkeypatch.setattr(pool, "_observe_execution", observe)
    result = queue.execute(item, heartbeat_s=0.05)

    assert result["status"] == "executed"
    assert seen and all(seen), (
        "every execution sample must be taken with the attempt's scope; one "
        "that is not reports an unobserved child for an action that has one")


def test_pbstatus_says_a_child_is_silent_and_says_when_it_cannot_tell(
        tmp_path: Path) -> None:
    """The reader that decides whether to surface a claim reads the note.

    main: a fresh observation's note carries the child's liveness, CPU and
    silence beside the launcher's.
    branch: an observation with no child record, such as one written before
    this field existed, reads as unobserved rather than as a healthy claim.
    """

    from test_pbstatus import pbstatus

    now = 1_000_000.0
    key = "a" * 64
    claim = {"action_key": key, "claimed_by": "w", "claimed_host": "dl380g10",
             "claimed_unix": now - 3700, "published_unix": now - 3800}

    def lease(child):
        # The identity fields a lease must carry before pbstatus reads either
        # observation on it; a mismatch is reported as invalid, never as fresh.
        return {"action_key": key, "owner": "w", "host": "dl380g10",
                "claimed_unix": now - 3700, "published_unix": now - 3800,
                "heartbeat_unix": now - 5,
                "execution_observation": {
                    "source": "launcher-pipes", "sampled_unix": now - 5,
                    "launcher_alive": True, "stdout_bytes": 218,
                    "stderr_bytes": 0, "last_output_unix": now - 3600,
                    **({} if child is None else {"child": child})}}

    noisy = pbstatus._execution_observation(
        claim, lease({"source": "resource-scope-cgroup", "pids": [1286583],
                      "pid_count": 1, "alive": True, "silent_s": 3595.0,
                      "cpu_seconds": 300.0}), now=now)
    assert noisy["state"] == "fresh"
    assert "child running (1 pid)" in noisy["note"]
    assert "silent 3595s" in noisy["note"]
    assert "cpu 300s" in noisy["note"]

    blind = pbstatus._execution_observation(claim, lease(None), now=now)
    assert "child unobserved" in blind["note"]
