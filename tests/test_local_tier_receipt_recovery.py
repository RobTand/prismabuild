"""Real local-tier receipt repair, idle park and cooperative stop boundaries."""
import json
import os
from pathlib import Path
import selectors
import signal
import subprocess
import sys

import pytest

from prismabuild import resident_sets
from test_local_tier_requires_known_receipts import KNOWN, REPOSITORY


def _replace(path, value):
    replacement = path.with_suffix(".next")
    replacement.write_text(json.dumps(value))
    os.replace(replacement, path)


def _start(root, loaded, published, *, mode="stop", interval=0.001,
           once=False, closed=False, stop_signal=signal.SIGTERM):
    for name, value in (("loaded.json", loaded), ("published.json", published)):
        if value is not None:
            _replace(root / name, value)
    (root / "parked").mkdir()
    _replace(root / "maintenance.json", {"draining": closed, "changed_unix": 101.5})
    _replace(root / "policy.json", {"schema": resident_sets.POLICY_SCHEMA, "hosts": {}})
    script = root / "exercise.py"
    script.write_text(f'''
import json, os, signal, sys
from pathlib import Path
repository = Path({str(REPOSITORY)!r})
root = Path({str(root)!r})
sys.path.insert(0, str(repository / "src"))
sys.path.insert(0, str(repository / "tools" / "fleet"))
import local_tier_loop as role
from prismabuild import generation_receipt, pool, role_session
assert Path(role.__file__).resolve() == repository / "tools" / "fleet" / "local_tier_loop.py"
for name, module in (("pool", pool), ("role_session", role_session),
                     ("generation_receipt", generation_receipt)):
    assert Path(module.__file__).resolve() == repository / "src" / "prismabuild" / (name + ".py")
role_session.GENERATION_VERSION = root / "loaded.json"
role_session.RUNTIME_VERSION = root / "published.json"
role_session.MAINTENANCE_GATE = root / "maintenance.json"
role_session.PARKED_ROOT = root / "parked"
role_session.ROLE_LOCK_ROOT = root / "role-locks"
affinity = os.sched_getaffinity(0)
real_cycle = role.local_tier_cycle
def cycle(queue, host, policy):
    assert queue.root == root / "queue"
    assert os.sched_getaffinity(0) == affinity
    result = real_cycle(queue, host, policy)
    assert list((root / "parked").iterdir()) == []
    with (root / "passes.jsonl").open("a") as stream:
        stream.write(json.dumps(result) + "\\n")
    if {mode!r} in ("park", "lose-and-stop"):
        (root / "published.json").unlink()
    if {mode!r} == "park":
        replacement = root / "gate.next"
        replacement.write_text(json.dumps({{"draining": True, "changed_unix": 101.5}}))
        os.replace(replacement, root / "maintenance.json")
    else:
        os.kill(os.getpid(), {int(stop_signal)!r})
    return result
role.local_tier_cycle = cycle
arguments = ["--pool-root", str(root / "queue"), "--policy", str(root / "policy.json"),
             "--interval-s", {str(interval)!r}]
if {once!r}:
    arguments.append("--once")
code = role.main(arguments)
assert os.sched_getaffinity(0) == affinity
raise SystemExit(code)
''')
    environment = {
        **os.environ, "PYTHONDONTWRITEBYTECODE": "1",
        "PRISMABUILD_POOL_ROOT": str(root / "queue"),
        "PRISMABUILD_BOX_STATE_ROOT": str(root / "box-state"),
        "PRISMABUILD_LOCAL_CHECKOUT_ROOT": str(root / "checkouts"),
        "PRISMABUILD_PRODUCED_SPOOL_ROOT": str(root / "spool"),
        "PRISMABUILD_SPOOL_OFFER_ROOT": str(root / "spool-offer"),
        "PRISMABUILD_MAINTENANCE_GATE": str(root / "maintenance.json"),
        "PRISMABUILD_FILESYSTEM_FLOOR": "off",
        "OMP_NUM_THREADS": "1", "MKL_NUM_THREADS": "1", "OPENBLAS_NUM_THREADS": "1",
    }
    return subprocess.Popen([sys.executable, "-B", str(script)], env=environment,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, bufsize=0)


def _finish(child):
    if child.poll() is None:
        child.kill()
    child.communicate(timeout=5)


def _unknown_boundary(child):
    output = bytearray()
    with selectors.DefaultSelector() as selector:
        selector.register(child.stdout, selectors.EVENT_READ)
        while b'"event": "localtier-runtime-unknown"' not in output:
            assert selector.select(5), bytes(output).decode(errors="replace")
            chunk = os.read(child.stdout.fileno(), 65536)
            assert chunk, bytes(output).decode(errors="replace")
            output.extend(chunk)
    return bytes(output)


def _assert_state(root, *, passes, queue_created, parked):
    path = root / "passes.jsonl"
    assert (len(path.read_text().splitlines()) if path.exists() else 0) == passes
    assert (root / "queue").exists() is queue_created
    assert list((root / "queue" / "generation-drift").glob("*.json")) == []
    markers = list((root / "parked").iterdir())
    assert len(markers) == parked
    assert all(marker.name.endswith("-101.5") for marker in markers)


@pytest.mark.parametrize("unknown_side", ["loaded", "published"])
def test_atomic_repair_resumes_one_pass_with_the_first_known_loaded_identity(tmp_path, unknown_side):
    child = _start(tmp_path, None if unknown_side == "loaded" else KNOWN,
                   None if unknown_side == "published" else KNOWN)
    try:
        prefix = _unknown_boundary(child)
        _assert_state(tmp_path, passes=0, queue_created=False, parked=0)
        if unknown_side == "published":
            _replace(tmp_path / "loaded.json", {**KNOWN, "generation": "replacement"})
        _replace(tmp_path / f"{unknown_side}.json", KNOWN)
        output = prefix + child.communicate(timeout=10)[0]
        assert child.returncode == 0, output.decode(errors="replace")
        _assert_state(tmp_path, passes=1, queue_created=True, parked=0)
    finally:
        _finish(child)


@pytest.mark.parametrize("unknown_side", ["loaded", "published"])
@pytest.mark.parametrize("stop_signal", [signal.SIGTERM, signal.SIGINT])
def test_unknown_retry_wait_is_interruptible(tmp_path, unknown_side, stop_signal):
    child = _start(tmp_path, None if unknown_side == "loaded" else KNOWN,
                   None if unknown_side == "published" else KNOWN, interval=3600)
    try:
        prefix = _unknown_boundary(child)
        _assert_state(tmp_path, passes=0, queue_created=False, parked=0)
        child.send_signal(stop_signal)
        output = prefix + child.communicate(timeout=3)[0]
        assert child.returncode == 0, output.decode(errors="replace")
        _assert_state(tmp_path, passes=0, queue_created=False, parked=0)
    finally:
        _finish(child)


@pytest.mark.parametrize("after_pass", [False, True])
def test_unknown_publication_still_allows_an_idle_park_proof(tmp_path, after_pass):
    child = _start(tmp_path, KNOWN, KNOWN if after_pass else None, mode="park",
                   once=not after_pass, closed=not after_pass)
    try:
        prefix = b""
        if after_pass:
            prefix = _unknown_boundary(child)
            _assert_state(tmp_path, passes=1, queue_created=True, parked=1)
            child.send_signal(signal.SIGTERM)
        output = prefix + child.communicate(timeout=10)[0]
        assert child.returncode == (0 if after_pass else 75), output.decode(errors="replace")
        _assert_state(tmp_path, passes=int(after_pass), queue_created=after_pass, parked=1)
    finally:
        _finish(child)


@pytest.mark.parametrize("stop_signal", [signal.SIGTERM, signal.SIGINT])
def test_completed_pass_stays_complete_when_publication_is_lost_before_stop(tmp_path, stop_signal):
    child = _start(tmp_path, KNOWN, KNOWN, mode="lose-and-stop", stop_signal=stop_signal)
    try:
        output = child.communicate(timeout=10)[0]
        assert child.returncode == 0, output.decode(errors="replace")
        _assert_state(tmp_path, passes=1, queue_created=True, parked=0)
    finally:
        _finish(child)
