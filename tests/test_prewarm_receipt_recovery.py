"""Actual storage reads resume after receipt repair and finish through receipt loss."""
import json
import os
from pathlib import Path
import selectors
import signal
import subprocess
import sys

import pytest

from prewarm_fixture import Fleet
from test_prewarm_requires_known_receipts import KNOWN, REPOSITORY


def _replace(path, value):
    replacement = path.with_suffix(".next")
    replacement.write_text(json.dumps(value))
    os.replace(replacement, path)


def _start(root, loaded, published, *, mode="stop", interval=0.001,
           once=False, closed=False, stop_signal=signal.SIGTERM):
    fleet = Fleet(root / "fleet")
    key = fleet.action("receipt-recovery", [fleet.file("row.bin", 4096)])
    for name, value in (("loaded.json", loaded), ("published.json", published)):
        if value is not None:
            _replace(root / name, value)
    _replace(root / "maintenance.json", {"draining": closed, "changed_unix": 101.5})
    (root / "parked").mkdir()
    disk = root / "sys-block" / "private0"
    disk.mkdir(parents=True)
    (disk / "stat").write_text("0 " * 11)
    (root / "nfsd-io").write_text("io 0 0\n")
    arcstats = fleet.arcstats(size=0, c=1 << 20, c_max=1 << 20)
    script = root / "exercise.py"
    script.write_text(f'''
import json, os, signal, sys
from functools import partial
from pathlib import Path
repository = Path({str(REPOSITORY)!r})
root = Path({str(root)!r})
sys.path.insert(0, str(repository / "src"))
sys.path.insert(0, str(repository / "tools" / "fleet"))
import prewarm_loop as role
from prismabuild import generation_receipt, pool, role_session, storage_tiers
assert Path(role.__file__).resolve() == repository / "tools" / "fleet" / "prewarm_loop.py"
for name, module in (("pool", pool), ("role_session", role_session),
                     ("generation_receipt", generation_receipt), ("storage_tiers", storage_tiers)):
    assert Path(module.__file__).resolve() == repository / "src" / "prismabuild" / (name + ".py")
role_session.GENERATION_VERSION = root / "loaded.json"
role_session.RUNTIME_VERSION = root / "published.json"
role_session.MAINTENANCE_GATE = root / "maintenance.json"
role_session.PARKED_ROOT = root / "parked"
role_session.ROLE_LOCK_ROOT = root / "role-locks"
role.DiskPacer = partial(role.DiskPacer,
    stat_source=partial(storage_tiers.read_disk_stat, root=str(root / "sys-block")))
affinity = os.sched_getaffinity(0)
setups = 0
def observed_return(frame, event, value):
    global setups
    if event != "return":
        return
    if frame.f_code == pool.PoolQueue.__init__.__code__:
        (root / "queue-opened").touch()
    if frame.f_code == role.pacer_from_args.__code__:
        setups += 1
        if {mode!r} == "setup" and setups == 2:
            (root / "published.json").unlink()
    if frame.f_code == role.cycle.__code__:
        assert isinstance(value, dict)
        assert os.sched_getaffinity(0) == affinity
        assert list((root / "parked").iterdir()) == []
        sys.setprofile(None)
        if {mode!r} in ("park", "lose-and-stop"):
            (root / "published.json").unlink()
        if {mode!r} in ("park", "move"):
            replacement = root / "replacement.next"
            target = root / ("maintenance.json" if {mode!r} == "park" else "published.json")
            replacement.write_text(json.dumps(
                {{"draining": True, "changed_unix": 101.5}} if {mode!r} == "park"
                else {{"commit": "a" * 40, "generation": "second"}}))
            os.replace(replacement, target)
        else:
            os.kill(os.getpid(), {int(stop_signal)!r})
arguments = [
    "--pool-root", {str(fleet.queue.root)!r}, "--cas-root", {str(fleet.cas_root)!r},
    "--mount-map", {str(fleet.mount) + "=" + str(fleet.mount)!r},
    "--arcstats", {arcstats!r}, "--arc-reserve-fraction", "1",
    "--min-manifest-bytes", "0", "--readers", "1", "--max-readers", "1",
    "--disks", "private0", "--pace-sample-s", "0", "--export-stats", "",
    "--nfsd-io", str(root / "nfsd-io"), "--log", str(root / "cycles.jsonl"),
    "--poll-s", {str(interval)!r}]
if {once!r}:
    arguments.append("--once")
sys.setprofile(observed_return)
try:
    code = role.main(arguments)
finally:
    sys.setprofile(None)
assert os.sched_getaffinity(0) == affinity
raise SystemExit(code)
''')
    environment = {
        **os.environ, "PYTHONDONTWRITEBYTECODE": "1",
        "PRISMABUILD_POOL_ROOT": str(fleet.queue.root),
        "PRISMABUILD_BOX_STATE_ROOT": str(root / "box-state"),
        "PRISMABUILD_LOCAL_CHECKOUT_ROOT": str(root / "checkouts"),
        "PRISMABUILD_PRODUCED_SPOOL_ROOT": str(root / "spool"),
        "PRISMABUILD_SPOOL_OFFER_ROOT": str(root / "spool-offer"),
        "PRISMABUILD_MAINTENANCE_GATE": str(root / "maintenance.json"),
        "PRISMABUILD_FILESYSTEM_FLOOR": "off",
        "OMP_NUM_THREADS": "1", "MKL_NUM_THREADS": "1", "OPENBLAS_NUM_THREADS": "1",
    }
    child = subprocess.Popen([sys.executable, "-B", str(script)], env=environment,
                             stdout=subprocess.PIPE, stderr=subprocess.STDOUT, bufsize=0)
    return child, fleet, key


def _finish(child):
    if child.poll() is None:
        child.kill()
    child.communicate(timeout=5)


def _boundary(child, needle=b'"event": "prewarm-runtime-unknown"'):
    output = bytearray()
    with selectors.DefaultSelector() as selector:
        selector.register(child.stdout, selectors.EVENT_READ)
        while needle not in output:
            assert selector.select(5), bytes(output).decode(errors="replace")
            chunk = os.read(child.stdout.fileno(), 65536)
            assert chunk, bytes(output).decode(errors="replace")
            output.extend(chunk)
    return bytes(output)


def _assert_state(root, fleet, key, *, cycles, queue_opened, parked):
    log = root / "cycles.jsonl"
    assert (len(log.read_text().splitlines()) if log.exists() else 0) == cycles
    assert (root / "queue-opened").exists() is queue_opened
    record = fleet.queue.prewarm(key)
    if cycles:
        assert record["status"] == "complete"
        assert record["bytes_warmed"] == 4096
        assert record["disk_pacing"]["active"] is True
        assert record["disk_pacing"]["telemetry_state"] == "complete"
    else:
        assert record is None
    for state in ("claimed", "done", "failed", "generation-drift"):
        assert list((fleet.queue.root / state).glob("*.json")) == []
    assert (fleet.queue.root / "ready" / (key + ".json")).exists()
    markers = list((root / "parked").iterdir())
    assert len(markers) == parked
    assert all(marker.name.endswith("-101.5") for marker in markers)


@pytest.mark.parametrize("unknown_side", ["loaded", "published"])
def test_atomic_repair_resumes_with_the_first_known_loaded_identity(tmp_path, unknown_side):
    child, fleet, key = _start(tmp_path, None if unknown_side == "loaded" else KNOWN,
                              None if unknown_side == "published" else KNOWN)
    try:
        prefix = _boundary(child)
        _assert_state(tmp_path, fleet, key, cycles=0, queue_opened=False, parked=0)
        if unknown_side == "published":
            _replace(tmp_path / "loaded.json", {**KNOWN, "generation": "replacement"})
        _replace(tmp_path / f"{unknown_side}.json", KNOWN)
        output = prefix + child.communicate(timeout=10)[0]
        assert child.returncode == 0, output.decode(errors="replace")
        assert b"runtime moved" not in output
        _assert_state(tmp_path, fleet, key, cycles=1, queue_opened=True, parked=0)
    finally:
        _finish(child)


@pytest.mark.parametrize("unknown_side", ["loaded", "published"])
@pytest.mark.parametrize("stop_signal", [signal.SIGTERM, signal.SIGINT])
def test_unknown_wait_is_interruptible(tmp_path, unknown_side, stop_signal):
    child, fleet, key = _start(tmp_path, None if unknown_side == "loaded" else KNOWN,
                              None if unknown_side == "published" else KNOWN, interval=3600)
    try:
        prefix = _boundary(child)
        child.send_signal(stop_signal)
        output = prefix + child.communicate(timeout=3)[0]
        assert child.returncode == 0, output.decode(errors="replace")
        assert b"runtime moved" not in output
        _assert_state(tmp_path, fleet, key, cycles=0, queue_opened=False, parked=0)
    finally:
        _finish(child)


@pytest.mark.parametrize("after_cycle", [False, True])
def test_unknown_publication_allows_only_the_actual_idle_park(tmp_path, after_cycle):
    child, fleet, key = _start(tmp_path, KNOWN, KNOWN if after_cycle else None,
                              mode="park", once=not after_cycle, closed=not after_cycle)
    try:
        prefix = b""
        if after_cycle:
            prefix = _boundary(child, b"maintenance gate closed; storage cycle deferred")
            _assert_state(tmp_path, fleet, key, cycles=1, queue_opened=True, parked=1)
            child.send_signal(signal.SIGTERM)
        output = prefix + child.communicate(timeout=10)[0]
        assert child.returncode == (0 if after_cycle else 75), output.decode(errors="replace")
        assert b"runtime moved" not in output
        _assert_state(tmp_path, fleet, key, cycles=int(after_cycle), queue_opened=after_cycle, parked=1)
    finally:
        _finish(child)


@pytest.mark.parametrize("stop_signal", [signal.SIGTERM, signal.SIGINT])
def test_receipt_loss_does_not_undo_completed_read_or_accounting(tmp_path, stop_signal):
    child, fleet, key = _start(tmp_path, KNOWN, KNOWN, mode="lose-and-stop", stop_signal=stop_signal)
    try:
        output = child.communicate(timeout=10)[0]
        assert child.returncode == 0, output.decode(errors="replace")
        assert b"runtime moved" not in output
        _assert_state(tmp_path, fleet, key, cycles=1, queue_opened=True, parked=0)
    finally:
        _finish(child)


@pytest.mark.parametrize("once", [True, False])
def test_topology_return_cannot_authorize_work_after_publication_loss(tmp_path, once):
    child, fleet, key = _start(tmp_path, KNOWN, KNOWN, mode="setup", once=once)
    try:
        prefix = _boundary(child)
        _assert_state(tmp_path, fleet, key, cycles=0, queue_opened=True, parked=0)
        if not once:
            _replace(tmp_path / "published.json", KNOWN)
        output = prefix + child.communicate(timeout=10)[0]
        assert child.returncode == (75 if once else 0), output.decode(errors="replace")
        assert b"runtime moved" not in output
        _assert_state(tmp_path, fleet, key, cycles=int(not once), queue_opened=True, parked=0)
    finally:
        _finish(child)


def test_known_drift_after_a_real_read_retires_before_a_second_cycle(tmp_path):
    child, fleet, key = _start(tmp_path, KNOWN, KNOWN, mode="move")
    try:
        output = child.communicate(timeout=10)[0]
        assert child.returncode == 0, output.decode(errors="replace")
        assert b"runtime moved" in output
        _assert_state(tmp_path, fleet, key, cycles=1, queue_opened=True, parked=0)
    finally:
        _finish(child)
