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

1. every valid role invocation takes a host-local, per-role ``flock`` at
   startup -- under a private per-uid directory, named by the role script
   rather than by a generation, never unlinked -- and refuses when another
   instance already holds it.  A ``--once`` invocation is not exempt: it can
   mint, announce and warm against the real queue, so the one-cycle form
   takes the same lock and the same refusal.
2. the supervisor probes the same lock before spawning, so an instance the
   ownership census cannot prove is reported instead of raced, and nothing
   unproven is signalled.
3. the lock path does not move when the generation is republished, one
   role's lock never excludes another role, and the lock is released when
   its holder exits.  Refusal never depends on naming the holder: the pid is
   a /proc/locks diagnostic, and contention is safe without it.

Nothing here inspects or signals a live fleet process: the contender is a
real child of this test, and the only ``SIGSTOP`` in the suite is a test
child stopping itself.
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


def _role_box(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """A fake mount with two generations, a fake ``/proc``, a pgrep stand-in.

    ``offered`` maps a script name to the pids the fake ``pgrep`` reports for
    it, so a test can make a role appear and exit by moving one list.
    """

    mirror = tmp_path / "fleet"
    store = mirror / "runtime-generations"
    for name in ("gen-old", "gen-live"):
        generation = store / name
        (generation / "tools").mkdir(parents=True)
        for script in (STORAGE, TIERS, "worker_loop.py"):
            (generation / "tools" / script).write_text("# a loop\n")
        (generation / "RUNTIME_VERSION.json").write_text(
            json.dumps({"commit": name, "generation": name}))
    (mirror / "repo").symlink_to(store / "gen-live")
    proc = tmp_path / "proc"
    proc.mkdir()
    monkeypatch.setattr(supervise, "MIRROR", mirror)
    monkeypatch.setattr(supervise, "PROC", proc)
    monkeypatch.setattr(supervise, "LOG_DIR", tmp_path / "logs")
    monkeypatch.setattr(supervise.socket, "gethostname", lambda: HOST)
    offered: dict[str, list[int]] = {}

    def fake_run(argv, *_args, **_kwargs):
        assert argv[0] == "pgrep", argv
        return subprocess.CompletedProcess(
            argv, 0, "\n".join(str(pid) for pid in offered.get(argv[-1], [])),
            "")

    monkeypatch.setattr(supervise.subprocess, "run", fake_run)
    return mirror, proc, offered


def _role_process(proc: Path, pid: int, argv: list[str], *,
                  mark: str | None = HOST) -> int:
    """Write the ``/proc`` bytes one candidate role process would have."""

    directory = proc / str(pid)
    directory.mkdir()
    (directory / "cmdline").write_bytes(
        b"".join(part.encode() + b"\0" for part in argv))
    entries = ["HOME=/home/rob"]
    if mark is not None:
        entries.append(f"{supervise.OWNERSHIP_ENV}={mark}")
    (directory / "environ").write_bytes(
        b"".join(entry.encode() + b"\0" for entry in entries))
    (directory / "stat").write_bytes(
        f"{pid} (prewarm_loop.py) S 1 1 1".encode())
    return pid


def _spawn_recorder(monkeypatch: pytest.MonkeyPatch, pid: int = 4242):
    """Capture ``Popen`` argv, the way a spawn would have run it."""

    spawned: list[list[str]] = []

    class _Spawned:
        pass

    _Spawned.pid = pid
    monkeypatch.setattr(supervise.subprocess, "Popen",
                        lambda argv, **kwargs: spawned.append(argv) or _Spawned())
    return spawned


def test_the_supervisor_does_not_spawn_over_a_held_role_lock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys,
) -> None:
    """The duplicate supervisor's child, a hand-started loop, or a stale
    generation's: none of them appears in an ownership census built on this
    supervisor's mark and roots, and the lock is what must stop the race."""

    _mirror, _proc, offered = _role_box(tmp_path, monkeypatch)
    offered[STORAGE] = []
    monkeypatch.setattr(supervise, "declared_roles",
                        lambda host: [("storage", ["--readers", "4"])])
    monkeypatch.setattr(supervise, "_published_receipt", lambda: {})

    lock_root = tmp_path / "role-locks"
    monkeypatch.setattr(worker_loop, "ROLE_LOCK_ROOT", lock_root)
    lock = _lock_path(lock_root)
    lock.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
    held = os.open(lock, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(held, fcntl.LOCK_EX | fcntl.LOCK_NB)
    spawned = _spawn_recorder(monkeypatch)
    try:
        assert supervise.ensure_roles(HOST) == []
        assert spawned == [], (
            "the supervisor started a role another instance already holds")
        out = capsys.readouterr().out
        assert "role storage not started" in out, out
        assert str(lock) in out, out
    finally:
        os.close(held)


def test_a_signalled_predecessor_is_not_called_gone(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An old role holds no singleton lock, so its exit is the only proof.

    A pre-#709 generation cannot hold the new lock; SIGTERM to its idle loop
    is a request, and starting the replacement while it still reads the queue
    would overlap two readers.  The survivor keeps the role counted until the
    census says it is gone, and the replacement starts on the next tick.
    """

    mirror, proc, offered = _role_box(tmp_path, monkeypatch)
    store = mirror / "runtime-generations"
    role = _role_process(proc, 4242, [
        "/usr/bin/python3", str(store / "gen-old" / "tools" / STORAGE),
        "--readers", "1"])
    offered[STORAGE] = [role]
    monkeypatch.setattr(supervise, "declared_roles",
                        lambda host: [("storage", ["--readers", "4"])])
    monkeypatch.setattr(supervise, "_published_receipt", lambda: {})
    monkeypatch.setattr(supervise, "_claim_holders", lambda: frozenset())
    monkeypatch.setattr(supervise, "_is_idle", lambda pid, *_a: True)
    killed: list[tuple[int, int]] = []
    monkeypatch.setattr(
        supervise.os, "kill",
        lambda pid, sig: killed.append((pid, int(sig))))
    spawned = _spawn_recorder(monkeypatch, pid=9001)

    assert supervise.ensure_roles(HOST) == []
    assert killed == [(role, int(signal.SIGTERM))], killed
    assert spawned == [], "a replacement served beside its dying predecessor"

    offered[STORAGE] = []              # the predecessor exits
    assert supervise.ensure_roles(HOST) == [("storage", 9001)]
    assert spawned and spawned[0][1] == str(
        (mirror / "repo" / "tools" / STORAGE).resolve()), spawned


def test_contention_refuses_even_when_no_holder_can_be_named(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Safety is the flock, not a pid file: an unreadable holder still refuses.

    The holder here writes nothing anywhere.  The refusal reads /proc/locks
    only to decorate its message, and a failure to do so must not change the
    answer: "held, holder unknown" is never free.
    """

    monkeypatch.setattr(worker_loop, "ROLE_LOCK_ROOT", tmp_path / "role-locks")
    script = tmp_path / "gen" / "tools" / STORAGE
    lock = _lock_path(tmp_path / "role-locks")
    lock.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
    held = os.open(lock, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(held, fcntl.LOCK_EX | fcntl.LOCK_NB)
    # The admission lock's holder rule is the one parse; make it unable to
    # answer, as an unreadable /proc/locks would.
    monkeypatch.setattr(worker_loop.adaptive_cpu, "_holder_of",
                        lambda descriptor: None)
    try:
        assert worker_loop.role_singleton_holder(script) == (True, None)
        with pytest.raises(worker_loop.RoleLockHeld) as refusal:
            worker_loop.take_role_singleton(script)
        assert refusal.value.holder is None
    finally:
        os.close(held)


def test_a_once_operator_cycle_also_refuses_a_held_lock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys,
) -> None:
    """A one-shot invocation mutates too, so it takes the same lock (#709).

    ``prewarm_loop --once`` warms bytes and publishes prewarm records just as
    the service does; exempting the one-cycle form would be exactly the
    bypass the guard exists to close.  The regression pins that the cycle
    never runs behind a held lock.
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
    monkeypatch.setattr(prewarm_loop, "require_storage_pacing",
                        lambda *a, **k: None)
    warmed: list[bool] = []
    monkeypatch.setattr(prewarm_loop, "cycle",
                        lambda *a, **k: warmed.append(True) or {})
    local = tmp_path / "storage_pool" / "shared"
    local.mkdir(parents=True)

    lock = _lock_path(lock_root)
    lock.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
    held = os.open(lock, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(held, fcntl.LOCK_EX | fcntl.LOCK_NB)
    try:
        code = prewarm_loop.main([
            "--mount-map", f"/mnt/shared={local}",
            "--pool-root", str(tmp_path / "pb-queue"), "--once"])
        assert code == worker_loop.ROLE_SINGLETON_HELD_EXIT, code
        assert warmed == [], "a second reader warmed behind the singleton"
        assert "refusing" in capsys.readouterr().err.lower()
    finally:
        os.close(held)


def test_a_once_tier_cycle_also_refuses_a_held_lock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The tier minter's one-shot form can mint and announce; same lock."""

    import tier_loop

    lock_root = tmp_path / "role-locks"
    monkeypatch.setattr(worker_loop, "ROLE_LOCK_ROOT", lock_root)
    receipt = tmp_path / "generation" / "RUNTIME_VERSION.json"
    receipt.parent.mkdir(parents=True, exist_ok=True)
    receipt.write_text(json.dumps({"commit": "c" * 40}))
    monkeypatch.setattr(tier_loop.runtime_gate, "GENERATION_VERSION", receipt)
    monkeypatch.setattr(tier_loop.runtime_gate, "RUNTIME_VERSION", receipt)
    minted: list[bool] = []
    monkeypatch.setattr(tier_loop, "cycle",
                        lambda *a, **k: minted.append(True) or [])

    lock = _lock_path(lock_root, TIERS)
    lock.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
    held = os.open(lock, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(held, fcntl.LOCK_EX | fcntl.LOCK_NB)
    try:
        assert tier_loop.main([
            "--pool-root", str(tmp_path / "pb-queue"), "--once",
        ]) == worker_loop.ROLE_SINGLETON_HELD_EXIT
        assert minted == [], "a second minter minted behind the singleton"
    finally:
        os.close(held)
