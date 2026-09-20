"""Two supervisors must not each serve the fleet's single roles (#709).

On 2026-09-19 a duplicate supervisor ran beside the primary on the storage
box.  Each was free to spawn its own ``prewarm_loop`` and ``tier_loop``
against one queue: two readers of the same ready list, double-published
movers, contradictory fill measurements -- the compounding half of the
fill-capacity wedge that night.  The supervisor's box claim should have
refused the second process, and the role guard exists because "should have"
is not something a role can rely on: a role must not depend on its launcher
being single.

The contract tested here, against real subprocesses and real locks:

1. a service role takes a host-local, per-role ``flock`` at startup -- under a
   private per-uid directory, named by the role script rather than by a
   generation, never unlinked -- and refuses to start when another instance
   already holds it.  A ``--once`` operator cycle is a bounded diagnostic,
   not a second service, and keeps the pool's own mint-lock serialization.
2. the supervisor probes the same lock before spawning, so an instance the
   ownership census cannot prove is reported instead of raced, and nothing
   unproven is signalled.
3. the lock path does not move when the generation is republished, one
   role's lock never excludes another role, and the lock is released when
   its holder exits.

Nothing here inspects or signals a live fleet process: the contender is a
real child of this test, and the only ``SIGSTOP`` in the suite is a test
child stopping itself.
"""

from __future__ import annotations

import fcntl
import json
import os
from pathlib import Path
import subprocess
import sys
import time

import pytest

ROOT = Path(__file__).resolve().parents[1]
TOOLS = ROOT / "tools" / "fleet"
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(TOOLS))

import supervise  # noqa: E402
import worker_loop  # noqa: E402

HOST = "dl380g10"
STORAGE = "prewarm_loop.py"
TIERS = "tier_loop.py"

#: The lock-file name the contract pins, independent of the generation a
#: script was loaded from.
LOCK_NAME = "prewarm_loop.lock"


def _lock_path(root: Path, script: str = STORAGE) -> Path:
    return root / f"{Path(script).stem}.lock"


def _wait_for(predicate, timeout: float = 10.0) -> bool:
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() >= deadline:
            return False
        time.sleep(0.02)
    return True


#: Hold a real flock on a role lock with no knowledge of the new API, so the
#: defect is reproduced even before the guard exists.  The lock root is the
#: test's own; nothing live is touched.
_HOLD_LOCK = """
import fcntl, os, pathlib, sys, time
lock = pathlib.Path(sys.argv[1])
lock.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
fd = os.open(lock, os.O_RDWR | os.O_CREAT, 0o600)
fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
pathlib.Path(sys.argv[2]).touch()
time.sleep(float(sys.argv[3]))
"""

#: Run the real service entry point with the test's lock root.  The stubs
#: below only keep the pre-fix process from doing real queue or disk work
#: while the test proves it should never have started at all.
_ROLE_RUNNER = """
import json, sys
from pathlib import Path
sys.path.insert(0, {tools!r})
import worker_loop
worker_loop.ROLE_LOCK_ROOT = Path({lock_root!r})
receipt = Path({tmp!r}) / "generation" / "RUNTIME_VERSION.json"
receipt.parent.mkdir(parents=True, exist_ok=True)
receipt.write_text(json.dumps({{"commit": "c" * 40, "generation": "gen-test"}}))
worker_loop.GENERATION_VERSION = receipt
worker_loop.RUNTIME_VERSION = receipt
worker_loop.read_maintenance_gate = lambda: None
import prewarm_loop
prewarm_loop.cycle = lambda *args, **kwargs: {{}}
prewarm_loop.require_storage_pacing = lambda *args, **kwargs: None
raise SystemExit(prewarm_loop.main({argv!r}))
"""


def test_a_second_service_role_refuses_the_host_singleton_lock(
    tmp_path: Path,
) -> None:
    """The incident's shape: one lock held, a second real role started.

    Before the guard the second process entered its service loop and stayed:
    both readers served the same queue.  The holder here is a real child
    holding a real flock; the contender is the role's own entry point.
    """

    lock_root = tmp_path / "role-locks"
    ready = tmp_path / "holder-ready"
    holder = subprocess.Popen(
        [sys.executable, "-c", _HOLD_LOCK,
         str(_lock_path(lock_root)), str(ready), "30"])
    second = None
    try:
        assert _wait_for(ready.exists), "the holder never took the lock"
        local = tmp_path / "storage_pool" / "shared"
        local.mkdir(parents=True)
        runner = _ROLE_RUNNER.format(
            tools=str(TOOLS), lock_root=str(lock_root), tmp=str(tmp_path),
            argv=["--mount-map", f"/mnt/shared={local}",
                  "--pool-root", str(tmp_path / "pb-queue"),
                  "--poll-s", "0.05"])
        second = subprocess.Popen(
            [sys.executable, "-c", runner],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        try:
            out, _ = second.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            # Retrying communicate after a timeout loses no output.
            second.terminate()
            try:
                out, _ = second.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                second.kill()
                out, _ = second.communicate(timeout=5)
            pytest.fail(
                "a second storage role served beside the lock holder:\n" + out)
        assert second.returncode == worker_loop.ROLE_SINGLETON_HELD_EXIT, out
        assert "refusing" in out.lower(), out
        assert str(_lock_path(lock_root)) in out, out
    finally:
        if second is not None and second.poll() is None:
            second.kill()
            second.wait(timeout=5)
        holder.terminate()
        holder.wait(timeout=5)


def test_the_singleton_is_released_when_its_holder_exits(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A crashed or rotated role must not wedge the box's next role."""

    lock_root = tmp_path / "role-locks"
    monkeypatch.setattr(worker_loop, "ROLE_LOCK_ROOT", lock_root)
    script = tmp_path / "gen" / "tools" / STORAGE
    ready = tmp_path / "holder-ready"
    holder = subprocess.Popen(
        [sys.executable, "-c", _HOLD_LOCK,
         str(_lock_path(lock_root)), str(ready), "30"])
    try:
        assert _wait_for(ready.exists)
        assert worker_loop.role_singleton_holder(script) == (True, holder.pid)
    finally:
        holder.terminate()
        holder.wait(timeout=5)
    assert _wait_for(
        lambda: worker_loop.role_singleton_holder(script) == (False, None)), (
        "the singleton outlived its holder")


def test_one_roles_lock_never_excludes_another_role(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Per-role isolation: storage and tiers are separate singletons."""

    monkeypatch.setattr(worker_loop, "ROLE_LOCK_ROOT", tmp_path / "role-locks")
    generation = tmp_path / "gen" / "tools"
    storage = generation / STORAGE
    tiers = generation / TIERS
    storage_lock = worker_loop.take_role_singleton(storage)
    try:
        with pytest.raises(worker_loop.RoleLockHeld) as held:
            worker_loop.take_role_singleton(storage)
        assert held.value.role == "prewarm_loop"
        assert held.value.holder == os.getpid()
        tiers_lock = worker_loop.take_role_singleton(tiers)
        os.close(tiers_lock)
    finally:
        os.close(storage_lock)
    again = worker_loop.take_role_singleton(storage)
    os.close(again)


def test_the_lock_path_is_stable_across_published_generations(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The generation moves; the role is one per host, so the inode must not."""

    monkeypatch.setattr(worker_loop, "ROLE_LOCK_ROOT", tmp_path / "role-locks")
    store = tmp_path / "runtime-generations"
    old = store / "aaaaaaaa-old" / "tools" / STORAGE
    new = store / "bbbbbbbb-new" / "tools" / "fleet" / STORAGE

    assert worker_loop.role_lock_path(old) == worker_loop.role_lock_path(new)
    assert worker_loop.role_lock_path(old).name == LOCK_NAME
    assert not worker_loop.role_lock_path(old).is_relative_to(store), (
        "a generation-local lock would let a stale role and a fresh one "
        "each serve")


def test_the_supervisor_does_not_spawn_over_a_held_role_lock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys,
) -> None:
    """The duplicate supervisor's child, a hand-started loop, or a stale
    generation's: none of them appears in an ownership census built on this
    supervisor's mark and roots, and the lock is what must stop the race."""

    mirror = tmp_path / "fleet"
    generation = mirror / "runtime-generations" / "gen-live"
    (generation / "tools").mkdir(parents=True)
    for script in (STORAGE, TIERS, "worker_loop.py"):
        (generation / "tools" / script).write_text("# a loop\n")
    (generation / "RUNTIME_VERSION.json").write_text(
        json.dumps({"commit": "a" * 40, "generation": "gen-live"}))
    (mirror / "repo").symlink_to(generation)
    monkeypatch.setattr(supervise, "MIRROR", mirror)
    monkeypatch.setattr(supervise, "LOG_DIR", tmp_path / "logs")
    monkeypatch.setattr(supervise.socket, "gethostname", lambda: HOST)
    monkeypatch.setattr(supervise, "declared_roles",
                        lambda host: [("storage", ["--readers", "4"])])
    monkeypatch.setattr(supervise, "_live_role_loops", lambda *a, **k: [])
    monkeypatch.setattr(supervise, "_published_receipt", lambda: {})

    lock_root = tmp_path / "role-locks"
    monkeypatch.setattr(worker_loop, "ROLE_LOCK_ROOT", lock_root)
    lock = _lock_path(lock_root)
    lock.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
    held = os.open(lock, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(held, fcntl.LOCK_EX | fcntl.LOCK_NB)
    spawned: list[list[str]] = []

    class _Spawned:
        pid = 4242

    monkeypatch.setattr(supervise.subprocess, "Popen",
                        lambda argv, **kwargs: spawned.append(argv) or _Spawned())
    try:
        assert supervise.ensure_roles(HOST) == []
        assert spawned == [], (
            "the supervisor started a role another instance already holds")
        out = capsys.readouterr().out
        assert "role storage not started" in out, out
        assert str(lock) in out, out
    finally:
        os.close(held)


def test_a_once_operator_cycle_takes_no_service_lock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The one-cycle form stays the operator's diagnostic (see #593).

    It is a bounded, single mint serialized by the pool's own per-tier lock,
    not a second service; refusing it while the supervised role is up would
    break the advertised operator mode.
    """

    import prewarm_loop

    lock_root = tmp_path / "role-locks"
    monkeypatch.setattr(worker_loop, "ROLE_LOCK_ROOT", lock_root)
    receipt = tmp_path / "generation" / "RUNTIME_VERSION.json"
    receipt.parent.mkdir(parents=True, exist_ok=True)
    receipt.write_text(json.dumps({"commit": "c" * 40}))
    monkeypatch.setattr(worker_loop, "GENERATION_VERSION", receipt)
    monkeypatch.setattr(worker_loop, "RUNTIME_VERSION", receipt)
    monkeypatch.setattr(worker_loop, "read_maintenance_gate", lambda: None)
    monkeypatch.setattr(prewarm_loop, "cycle", lambda *a, **k: {})
    local = tmp_path / "storage_pool" / "shared"
    local.mkdir(parents=True)

    lock = _lock_path(lock_root)
    lock.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
    held = os.open(lock, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(held, fcntl.LOCK_EX | fcntl.LOCK_NB)
    try:
        assert prewarm_loop.main([
            "--mount-map", f"/mnt/shared={local}",
            "--pool-root", str(tmp_path / "pb-queue"),
            "--once", "--dry-run"]) == 0
    finally:
        os.close(held)
