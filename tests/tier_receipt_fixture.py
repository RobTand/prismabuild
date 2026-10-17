"""Private actual tier roles with causal controls after real operation returns."""
import json
import os
from pathlib import Path
import selectors
import signal
import socket
import subprocess
import sys
from types import SimpleNamespace

from prismabuild import pool
from test_prewarm_requires_known_receipts import REPOSITORY


def replace(path, value):
    replacement = path.with_suffix(".next")
    replacement.write_text(json.dumps(value))
    os.replace(replacement, path)


def start(root, loaded, published, *, mode="stop", once=False, interval=0.001,
          closed=False, epoch=101.5, stop_signal=signal.SIGTERM):
    for name, value in (("loaded.json", loaded), ("published.json", published)):
        if value is not None:
            replace(root / name, value)
    replace(root / "maintenance.json", {"draining": closed, "changed_unix": epoch})
    if mode == "marker-failure":
        (root / "parked").write_text("not a directory")
    else:
        (root / "parked").mkdir()
    queue = pool.PoolQueue(root / "queue")
    receipt = queue.root / pool.record_store.PREWARM / "boundary-input.json"
    receipt.parent.mkdir(parents=True)
    receipt.write_text("{" if mode == "fail-stop" else json.dumps({"receipt_probe": "actual bytes"}))
    (root / "arcstats").write_text(f"size 4 0\nc 4 {1 << 30}\nc_max 4 {1 << 30}\n")
    (root / "ram-policy.json").write_text("null\n")
    (root / "pressure").write_text("some avg10=0.00 avg60=0.00 avg300=0.00 total=0\n")
    (root / "zpool.txt").write_text("")
    control, peer = socket.socketpair()
    control.settimeout(5)
    script = root / "exercise.py"
    script.write_text(f'''
import json, os, signal, socket, sys
from pathlib import Path
repository = Path({str(REPOSITORY)!r})
root = Path({str(root)!r})
sys.path.insert(0, str(repository / "src"))
sys.path.insert(0, str(repository / "tools" / "fleet"))
import tier_loop as role
from prismabuild import filesystem_floor, generation_receipt, pool, role_session, storage_tiers
assert Path(role.__file__).resolve() == repository / "tools" / "fleet" / "tier_loop.py"
for name, module in (("pool", pool), ("role_session", role_session),
                     ("generation_receipt", generation_receipt), ("storage_tiers", storage_tiers)):
    assert Path(module.__file__).resolve() == repository / "src" / "prismabuild" / (name + ".py")
role_session.GENERATION_VERSION = root / "loaded.json"
role_session.RUNTIME_VERSION = root / "published.json"
role_session.MAINTENANCE_GATE = root / "maintenance.json"
role_session.PARKED_ROOT = root / "parked"
role_session.ROLE_LOCK_ROOT = root / "role-locks"
role.RECEIPT_READERS = 1
read_arc, read_policy = storage_tiers.read_arcstats, storage_tiers.read_ram_policy
read_pressure = storage_tiers.read_io_pressure_some
storage_tiers.read_arcstats = lambda _path: read_arc(str(root / "arcstats"))
storage_tiers.read_ram_policy = lambda _path: read_policy(root / "ram-policy.json")
storage_tiers.read_io_pressure_some = lambda _path: read_pressure(str(root / "pressure"))
def kernel_command(argv):
    assert Path(argv[0]).name == "zpool", argv
    return (root / "zpool.txt").read_text()
storage_tiers._run = kernel_command
control = socket.socket(fileno={peer.fileno()!r})
affinity = os.sched_getaffinity(0)
handled = set()
def replace(path, value):
    replacement = path.with_suffix(".next")
    replacement.write_text(json.dumps(value))
    os.replace(replacement, path)
def close_gate():
    replace(root / "maintenance.json", {{"draining": True, "changed_unix": 101.5}})
def observed_return(frame, event, value):
    if event != "return":
        return
    assert os.sched_getaffinity(0) == affinity
    code = frame.f_code
    fences = {{"fence-construct": pool.PoolQueue.__init__.__code__,
               "fence-layout": pool.record_store.RecordStore.ensure_layout.__code__, "fence-adopt": role._start.__code__}}
    if code == fences.get({mode!r}) and "fence" not in handled:
        handled.add("fence")
        (root / "fence-reached").touch()
        (root / "published.json").unlink()
    if (code == Path.read_bytes.__code__
            and frame.f_locals.get("self") == root / "queue" / "prewarm" / "boundary-input.json"):
        assert isinstance(value, bytes)
        (root / "receipt-read").write_bytes(value)
        if {mode!r} == "held-read":
            owner = frame
            while owner.f_code != role._serve_cycles.__code__:
                owner = owner.f_back
            stop = owner.f_locals["stop"]
            control.sendall(b"held")
            while True:
                command = control.recv(1)
                if command == b"r":
                    break
                assert command == b"?"
                control.sendall(b"1" if stop.is_set() else b"0")
        if {mode!r} == "fail-stop":
            (root / "published.json").unlink()
            close_gate()
            os.kill(os.getpid(), {int(stop_signal)!r})
    if code == role.cycle.__code__ and value is not None and "cycle" not in handled:
        handled.add("cycle")
        assert role.LAST_CYCLE["completed"] is True
        assert list((root / "parked").iterdir()) == []
        if {mode!r} in ("park", "rotate-epoch", "lose-stop", "lose-only"):
            (root / "published.json").unlink()
        if {mode!r} in ("park", "rotate-epoch"):
            close_gate()
        elif {mode!r} == "move":
            replace(root / "published.json", {{"commit": "a" * 40, "generation": "second"}})
        elif {mode!r} != "lose-only":
            os.kill(os.getpid(), {int(stop_signal)!r})
    if code == filesystem_floor.loop_tick.__code__:
        (root / "floor-finished").touch()
    if code == role_session.post_park_marker.__code__:
        if {mode!r} == "rotate-epoch":
            replace(root / "maintenance.json", {{"draining": True, "changed_unix": 202.5}})
        os.kill(os.getpid(), {int(stop_signal)!r})
sys.setprofile(observed_return)
arguments = ["--pool-root", str(root / "queue"), "--interval-s", {str(interval)!r}]
if {once!r}:
    arguments.append("--once")
try:
    code = role.main(arguments)
finally:
    sys.setprofile(None)
assert os.sched_getaffinity(0) == affinity
raise SystemExit(code)
''')
    environment = {
        **os.environ, "PYTHONDONTWRITEBYTECODE": "1",
        "PRISMABUILD_POOL_ROOT": str(queue.root),
        "PRISMABUILD_BOX_STATE_ROOT": str(root / "box-state"),
        "PRISMABUILD_LOCAL_CHECKOUT_ROOT": str(root / "checkouts"),
        "PRISMABUILD_PRODUCED_SPOOL_ROOT": str(root / "spool"),
        "PRISMABUILD_SPOOL_OFFER_ROOT": str(root / "spool-offer"),
        "PRISMABUILD_MAINTENANCE_GATE": str(root / "maintenance.json"),
        "PRISMABUILD_FILESYSTEM_FLOOR": "off",
        "OMP_NUM_THREADS": "1", "MKL_NUM_THREADS": "1", "OPENBLAS_NUM_THREADS": "1",
    }
    try:
        child = subprocess.Popen([sys.executable, "-B", str(script)], env=environment,
                                 pass_fds=(peer.fileno(),), stdout=subprocess.PIPE,
                                 stderr=subprocess.STDOUT, bufsize=0)
    finally:
        peer.close()
    return SimpleNamespace(root=root, child=child, control=control, queue=queue)


def finish(role):
    if role.child.poll() is None:
        role.child.kill()
    role.child.communicate(timeout=5)
    role.control.close()


def unknown_boundary(role):
    output = bytearray()
    with selectors.DefaultSelector() as selector:
        selector.register(role.child.stdout, selectors.EVENT_READ)
        while b'"event": "tier-runtime-unknown"' not in output:
            assert selector.select(5), bytes(output).decode(errors="replace")
            chunk = os.read(role.child.stdout.fileno(), 65536)
            assert chunk, bytes(output).decode(errors="replace")
            output.extend(chunk)
    return bytes(output)


def state(role, *, cycles, layout, parked=0, failed=False):
    assert (role.queue.root / pool.record_store.READY).exists() is layout
    assert (role.root / "receipt-read").exists() is bool(cycles or failed)
    assert (role.root / "floor-finished").exists() is bool(cycles or failed)
    tiers = role.queue.tiers()
    assert len(tiers) == int(bool(cycles))
    if tiers:
        assert tiers[0]["tokens"] == {"arc_gib": 1}
        assert role.queue.tier_ledger(tiers[0]["tier_id"]).available() == {"arc_gib": 1}
    markers = list((role.root / "parked").iterdir()) if (role.root / "parked").is_dir() else []
    assert len(markers) == parked
    for name in ("claimed", "done", "failed", "generation-drift"):
        assert list((role.queue.root / name).glob("*.json")) == []
    return markers


def complete(role, output=b"", *, code=0, cycles=1, failed=0, adoption=None, timeout=10):
    output += role.child.communicate(timeout=timeout)[0]
    assert role.child.returncode == code, output.decode(errors="replace")
    records = []
    for line in output.splitlines():
        try:
            record = json.loads(line)
        except ValueError:
            continue
        if isinstance(record, dict):
            records.append(record)
    assert sum(row.get("event") == "tier-cycle" for row in records) == cycles
    failures = [row for row in records if row.get("event") == "tier-cycle-failed"]
    assert len(failures) == failed
    assert all(row["completed"] is False for row in failures)
    assert all(row["completed"] is True for row in records if row.get("event") == "tier-cycle")
    if adoption is not None:
        assert sum(row.get("event") == "tier-records-adopted" for row in records) == adoption
    return output
