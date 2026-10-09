"""Real publication and supervisor adoption preserve matching loops (#1664).

The fixture changes paths, not process or reconciliation functions. Private
workers read their own receipt to isolate supervisor adoption from their
independent idle upgrade. A removal control must replace the same idle loops.
"""
from __future__ import annotations

import fcntl
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import signal
import socket
import subprocess
import sys
import time

import pytest

ROOT = Path(__file__).resolve().parents[1]
_SPEC = importlib.util.spec_from_file_location(
    "publish_runtime_adoption_1664", ROOT / "tools/fleet/publish_runtime.py")
publish_runtime = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(publish_runtime)

HOST = socket.gethostname()
SHAPE = ["--class", "x86", "--all-cores", "--cpu-slots", "1",
         "--mem-gb", "1", "--poll-s", "0.1", "--max-idle", "10000",
         "--gang-admission"]


def _git(checkout: Path, *args: str) -> None:
    subprocess.run(["git", "-C", str(checkout), *args], check=True,
                   capture_output=True, text=True)


def _private_checkout(tmp_path: Path) -> Path:
    checkout = tmp_path / "checkout"
    for directory in ("src", "tools/fleet"):
        shutil.copytree(ROOT / directory, checkout / directory,
                        ignore=shutil.ignore_patterns("__pycache__"))
    fleet = tmp_path / "fleet"
    # Each exec loads these private paths from its own published script.
    replacements = {
        "supervise.py": {
            'MIRROR = Path("/mnt/shared/prismabuild-fleet")':
                f"MIRROR = Path({str(fleet)!r})",
            'CLAIM = Path("/home/rob/tmp/prismabuild-supervisor.claim")':
                f"CLAIM = Path({str(tmp_path / 'supervisor.claim')!r})",
            'LOG_DIR = Path("/home/rob/tmp")':
                f"LOG_DIR = Path({str(tmp_path / 'logs')!r})",
        },
        "worker_loop.py": {
            'SH = Path("/mnt/shared/prismabuild-fleet")':
                f"SH = Path({str(fleet)!r})",
            'RUNTIME_VERSION = SH / "repo" / "RUNTIME_VERSION.json"':
                "RUNTIME_VERSION = GENERATION_VERSION",
            'PUBLICATION_LOCK_ROOT = Path(f"/tmp/prismabuild-offer-publish-{os.getuid()}")':
                f"PUBLICATION_LOCK_ROOT = Path({str(tmp_path / 'offer-publish')!r})",
        },
    }
    for name, changes in replacements.items():
        path = checkout / "tools/fleet" / name
        source = path.read_text()
        for original, replacement in changes.items():
            assert source.count(original) == 1
            source = source.replace(original, replacement)
        path.write_text(source)
    (tmp_path / "maintenance.json").write_text('{"draining": false}\n')
    _git(checkout, "init", "--quiet")
    _write_roster(checkout, SHAPE)
    return checkout


def _write_roster(checkout: Path, shape: list[str]) -> None:
    (checkout / "tools/fleet/fleet_boxes.json").write_text(json.dumps({
        "boxes": {HOST: {"loops": 2, "args": shape}},
    }))
    _git(checkout, "add", "src", "tools")
    _git(checkout, "-c", "user.name=PB private test", "-c",
         "user.email=pb-private-test@example.invalid", "commit", "--quiet",
         "--allow-empty", "-m", "Declare the private CPU worker shape")


def _publish(checkout: Path, fleet: Path, monkeypatch, *, remove: bool = False) -> Path:
    monkeypatch.setattr(publish_runtime, "CHECKOUT", checkout)
    monkeypatch.setattr(publish_runtime, "MIRROR", fleet / "repo")
    argv = ["publish_runtime.py", "--rollout", "rolling", "--rollout-reason",
            "private CPU adoption test", "--shape-gate-waiver",
            "private CPU adoption test", "--no-canary"]
    if remove:
        argv += ["--drop-roster-option-by", "PB private test",
                 "--drop-roster-option-reason", "exercise the removal control"]
    monkeypatch.setattr(sys, "argv", argv)
    assert publish_runtime.main() == 0
    return (fleet / "repo").resolve(strict=True)


def _identity(pid: int) -> tuple[str, ...]:
    # Start time prevents a reused PID from satisfying the preservation check.
    fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
    return fields[1], fields[2], fields[3], fields[19]


def _workers(fleet: Path) -> dict[int, tuple[str, ...]]:
    result = subprocess.run(["pgrep", "-f", "worker_loop.py"],
                            capture_output=True, text=True, check=False)
    found = {}
    store = fleet / "runtime-generations"
    for token in result.stdout.split():
        pid = int(token)
        try:
            argv = Path(f"/proc/{pid}/cmdline").read_bytes().split(b"\0")
            if len(argv) < 2:
                continue
            script = Path(os.fsdecode(argv[1])).resolve()
            if script.name != "worker_loop.py" or not script.is_relative_to(store):
                continue
            found[pid] = _identity(pid)
        except (OSError, IndexError):
            continue
    return found


def _wait_for(predicate, supervisor: subprocess.Popen, log: Path) -> object:
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        result = predicate()
        if result:
            return result
        assert supervisor.poll() is None, log.read_text()
        time.sleep(0.02)
    workers = "\n".join(f"{path.name}:\n{path.read_text()}" for path in
                        sorted((log.parent / "logs").glob("pb-worker-*.log")))
    pytest.fail(f"private supervisor did not reach the expected state:\n"
                f"{log.read_text()}\n{workers}")


def _adopted(supervisor: subprocess.Popen, generation: Path, log: Path) -> bool:
    try:
        argv = Path(f"/proc/{supervisor.pid}/cmdline").read_bytes().split(b"\0")
        # The main cycle sleeps only after its census and reconciliation pass.
        return (os.fsencode(generation / "tools/supervise.py") in argv
                and f"-> {generation.name}; preserving pid {supervisor.pid}" in log.read_text()
                and Path(f"/proc/{supervisor.pid}/wchan").read_text().strip()
                == "hrtimer_nanosleep")
    except OSError:
        return False


def test_a_matching_adoption_restarts_no_loop(tmp_path, monkeypatch, capsys) -> None:
    checkout = _private_checkout(tmp_path)
    fleet = tmp_path / "fleet"
    first = _publish(checkout, fleet, monkeypatch)
    environment = {**os.environ,
                   "PRISMABUILD_BOX_STATE_ROOT": str(tmp_path / "box-state"),
                   "PRISMABUILD_POOL_ROOT": str(fleet / "pb-queue"),
                   "PRISMABUILD_MAINTENANCE_GATE": str(tmp_path / "maintenance.json"),
                   "PRISMABUILD_LOCAL_CHECKOUT_ROOT": str(tmp_path / "checkouts"),
                   "PYTHONDONTWRITEBYTECODE": "1"}
    environment.pop("PYTHONPATH", None)
    log = tmp_path / "supervisor.log"
    with log.open("w") as output:
        supervisor = subprocess.Popen(
            [sys.executable, str(first / "tools/supervise.py"), "--systemd",
             "--loops", "2", "--interval-s", "0.1"], cwd=fleet, env=environment,
            stdout=output, stderr=subprocess.STDOUT)
    original_supervisor = _identity(supervisor.pid)
    known = {}
    try:
        def idle_workers():
            workers = _workers(fleet)
            known.update(workers)
            logs = list((tmp_path / "logs").glob("pb-worker-*.log"))
            return workers if (len(workers) == 2 and len(logs) == 2
                               and all("] idle;" in path.read_text() for path in logs)) else None

        original = _wait_for(idle_workers, supervisor, log)
        assert all(identity[0] == str(supervisor.pid) for identity in original.values())
        assert all(identity[1:3] == (str(pid), str(pid))
                   for pid, identity in original.items())
        assert all(Path(f"/proc/{pid}/cmdline").read_bytes().split(b"\0")[2:-1]
                   == [os.fsencode(arg) for arg in SHAPE] for pid in original)
        claim = tmp_path / "supervisor.claim"
        with claim.open("r+") as contender:
            with pytest.raises(BlockingIOError):
                fcntl.flock(contender, fcntl.LOCK_EX | fcntl.LOCK_NB)
        reduced = [arg for arg in SHAPE if arg != "--gang-admission"]
        before = {path.relative_to(first): path.read_bytes()
                  for path in first.rglob("*") if path.is_file()}
        store_before = set((fleet / "runtime-generations").iterdir())
        _write_roster(checkout, reduced)
        with pytest.raises(SystemExit, match="--gang-admission"):
            _publish(checkout, fleet, monkeypatch)
        assert (fleet / "repo").resolve() == first
        assert {path.relative_to(first): path.read_bytes()
                for path in first.rglob("*") if path.is_file()} == before
        assert set((fleet / "runtime-generations").iterdir()) == store_before
        assert _workers(fleet) == original
        _write_roster(checkout, SHAPE)

        second = _publish(checkout, fleet, monkeypatch)
        assert second != first
        assert (first / "tools/fleet_boxes.json").read_bytes() == (
            second / "tools/fleet_boxes.json").read_bytes()
        _wait_for(lambda: _adopted(supervisor, second, log), supervisor, log)
        assert _identity(supervisor.pid) == original_supervisor
        assert _workers(fleet) == original
        assert "stopped the" not in log.read_text()
        assert len(re.findall(r"spawned loop \d+ pid \d+", log.read_text())) == 2
        with claim.open("r+") as contender:
            with pytest.raises(BlockingIOError):
                fcntl.flock(contender, fcntl.LOCK_EX | fcntl.LOCK_NB)

        # The same driver must replace idle loops when the new shape differs.
        _write_roster(checkout, reduced)
        third = _publish(checkout, fleet, monkeypatch, remove=True)
        _wait_for(lambda: _adopted(supervisor, third, log), supervisor, log)

        def replacements():
            workers = _workers(fleet)
            known.update(workers)
            return workers if (len(workers) == 2 and not set(workers) & set(original)
                               and all(not Path(f"/proc/{pid}").exists() for pid in original)) else None

        replacement = _wait_for(replacements, supervisor, log)
        assert _identity(supervisor.pid) == original_supervisor
        assert all(identity[0] == str(supervisor.pid) for identity in replacement.values())
        assert all(Path(f"/proc/{pid}/cmdline").read_bytes().split(b"\0")[2:-1]
                   == [os.fsencode(arg) for arg in reduced] for pid in replacement)
        stopped = {
            int(pid) for group in re.findall(
                r"stopped the \d+ idle one\(s\) \[([^\]]+)\] onto:", log.read_text())
            for pid in group.split(",")
        }
        assert stopped == set(original)
        with capsys.disabled():
            print("private-adoption: " + json.dumps({
                "supervisor_pid": supervisor.pid,
                "first": first.name, "matching": second.name, "control": third.name,
                "preserved_workers": sorted(original),
                "replacement_workers": sorted(replacement),
                "refusal_preserved_generation": True,
                "claim_preserved": True,
            }), flush=True)
    finally:
        known.update(_workers(fleet))
        supervisor.terminate()
        try:
            supervisor.wait(timeout=10)
        except subprocess.TimeoutExpired:
            # Signal exact, start-time-verified fixture processes only.
            for pid, identity in known.items():
                try:
                    if _identity(pid) == identity:
                        os.kill(pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                except FileNotFoundError:
                    pass
            supervisor.wait(timeout=10)
    assert supervisor.returncode == 0, log.read_text()
    assert "shutdown complete; owned workers and roles exited" in log.read_text()
    assert _workers(fleet) == {}
