"""Actual workers pause on unknown receipts and retain their loaded identity."""
import json
import os
from pathlib import Path
import selectors
import signal
import subprocess
import sys

import pytest

from prismabuild import pool


REPOSITORY = Path(__file__).resolve().parents[1]
KNOWN = {"commit": "abc123def456abc", "generation": "gen-first"}
MOVED = {"commit": "successor", "generation": "gen-second"}
KEYS = ["a" * 64, "b" * 64]


def _replace(path, value):
    replacement = path.with_suffix(".next")
    replacement.write_text(json.dumps(value))
    os.replace(replacement, path)


def _start(tmp_path, loaded, published, *, continuous=False, poll_s=0, unpublish_on_offer=False):
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    for key in KEYS:
        queue.publish(action_key=key, cas_root=str(tmp_path / "cas"),
                      checkout_root=str(tmp_path), worker_script="worker.py",
                      resources={"cpu": 1, "mem_gb": 1}, max_attempts=1)
    for name, value in (("loaded.json", loaded), ("published.json", published)):
        if value is not None:
            _replace(tmp_path / name, value)
    gate = tmp_path / "maintenance.json"
    gate.write_text(json.dumps({"draining": False}))
    script = tmp_path / "exercise.py"
    script.write_text(f'''
from functools import partial
import json, os, signal, socket, sys
from pathlib import Path
repository = Path({str(REPOSITORY)!r})
root = Path({str(tmp_path)!r})
sys.path.insert(0, str(repository / "src"))
sys.path.insert(0, str(repository / "tools" / "fleet"))
import worker_loop as wl
from prismabuild import generation_receipt, pool, role_session
assert wl.RUNTIME_ROOT == repository
for name, module in (("pool", pool), ("role_session", role_session),
                     ("generation_receipt", generation_receipt)):
    assert Path(module.__file__).resolve() == repository / "src" / "prismabuild" / (name + ".py")
wl.SH = root
wl.PUBLICATION_LOCK_ROOT = root / "publication-lock"
wl.container_images.InventoryCache = partial(
    wl.container_images.InventoryCache, root=root / "image-inventory")
role_session.GENERATION_VERSION = root / "loaded.json"
role_session.RUNTIME_VERSION = root / "published.json"
role_session.MAINTENANCE_GATE = root / "maintenance.json"
role_session.PARKED_ROOT = root / "parked"
role_session.ROLE_LOCK_ROOT = root / "role-locks"
affinity = os.sched_getaffinity(0)
def record(name, value):
    with (root / name).open("a") as stream:
        stream.write(json.dumps(value) + "\\n")
def execute(self, item, **kwargs):
    key = item["action_key"]
    assert json.loads(self.records.item_path(pool.record_store.CLAIMED, key).read_text())["action_key"] == key
    assert self.records.lease_path(key).is_file()
    assert kwargs.get("containment") is True
    assert os.sched_getaffinity(0) == affinity
    record("executions.jsonl", key)
    if {continuous!r}:
        os.kill(os.getpid(), signal.SIGTERM)
    return {{"action_key": key, "status": "executed", "returncode": 0}}
pool.PoolQueue.execute = execute
if {unpublish_on_offer!r}:
    offer = root / "pb-queue" / "workers" / (socket.gethostname() + ".json")
    removed = root / "publication-removed"
    def audit(event, arguments):
        if event == "os.rename" and arguments[1] == str(offer) and not removed.exists():
            (root / "published.json").unlink()
            removed.write_text("publication removed at actual offer rename")
    sys.addaudithook(audit)
arguments = ["--assume-idle", "--all-cores", "--cpu-slots", "1", "--mem-gb", "1",
             "--gpu-slots", "0", "--class", "x86", "--poll-s", {str(poll_s)!r}, "--max-idle", "1"]
if not {continuous!r}:
    arguments.append("--once")
code = wl.main(arguments, on_outcome=lambda value: record("outcomes.jsonl", value))
assert os.sched_getaffinity(0) == affinity
raise SystemExit(code)
''')
    environment = {
        **os.environ, "PYTHONDONTWRITEBYTECODE": "1",
        "PRISMABUILD_POOL_ROOT": str(queue.root),
        "PRISMABUILD_BOX_STATE_ROOT": str(tmp_path / "box-state"),
        "PRISMABUILD_LOCAL_CHECKOUT_ROOT": str(tmp_path / "checkouts"),
        "PRISMABUILD_PRODUCED_SPOOL_ROOT": str(tmp_path / "spool"),
        "PRISMABUILD_SPOOL_OFFER_ROOT": str(tmp_path / "spool-offer"),
        "PRISMABUILD_MAINTENANCE_GATE": str(gate),
        "PRISMABUILD_FILESYSTEM_FLOOR": "off",
        "OMP_NUM_THREADS": "1", "MKL_NUM_THREADS": "1", "OPENBLAS_NUM_THREADS": "1",
    }
    child = subprocess.Popen([sys.executable, "-B", str(script)], env=environment,
                             stdout=subprocess.PIPE, stderr=subprocess.STDOUT, bufsize=0)
    return queue, child


def _finish(child):
    if child.poll() is None:
        child.kill()
    child.communicate(timeout=5)


def _unknown_boundary(child, boundary="poll top"):
    output = bytearray()
    marker = f"runtime receipt unknown at {boundary}".encode()
    with selectors.DefaultSelector() as selector:
        selector.register(child.stdout, selectors.EVENT_READ)
        while marker not in output:
            assert selector.select(5), bytes(output).decode(errors="replace")
            chunk = os.read(child.stdout.fileno(), 65536)
            assert chunk, bytes(output).decode(errors="replace")
            output.extend(chunk)
    return bytes(output)


def _lines(path):
    return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []


def _assert_paused(queue, tmp_path):
    assert _lines(tmp_path / "executions.jsonl") == []
    assert _lines(tmp_path / "outcomes.jsonl") == []
    assert queue.terminal_keys() == frozenset()
    assert list(queue.records.dir(pool.record_store.CLAIMED).iterdir()) == []
    assert sorted(path.stem for path in queue.records.dir(pool.record_store.READY).glob("*.json")) == KEYS
    assert list((queue.root / "generation-drift").glob("*.json")) == []


def _assert_one_completed(queue, tmp_path):
    assert _lines(tmp_path / "executions.jsonl") == [KEYS[0]]
    assert _lines(tmp_path / "outcomes.jsonl") == [
        {"action_key": KEYS[0], "status": "executed", "returncode": 0}]
    assert queue.terminal_keys() == frozenset([KEYS[0]])
    assert json.loads(queue.records.item_path(pool.record_store.DONE, KEYS[0]).read_text())["status"] == "executed"
    assert queue.records.item_path(pool.record_store.READY, KEYS[1]).is_file()
    assert list(queue.records.dir(pool.record_store.CLAIMED).iterdir()) == []
    assert list((queue.root / "generation-drift").glob("*.json")) == []
    assert [offer["runtime_commit"] for offer in queue.offers()] == [KNOWN["commit"]]


def test_known_match_offer_carries_the_loaded_commit(tmp_path):
    queue, child = _start(tmp_path, KNOWN, KNOWN)
    try:
        output = child.communicate(timeout=30)[0]
        assert child.returncode == 0, output.decode(errors="replace")
        _assert_one_completed(queue, tmp_path)
    finally:
        _finish(child)


def test_known_stale_worker_does_not_refresh_its_offer_or_claim(tmp_path):
    queue, child = _start(tmp_path, KNOWN, MOVED)
    try:
        output = child.communicate(timeout=10)[0]
        assert child.returncode == 0, output.decode(errors="replace")
        assert queue.offers() == []
        assert queue.terminal_keys() == frozenset()
        assert _lines(tmp_path / "executions.jsonl") == []
        assert _lines(tmp_path / "outcomes.jsonl") == []
        assert sorted(path.stem for path in queue.records.dir(pool.record_store.READY).glob("*.json")) == KEYS
        records = list((queue.root / "generation-drift").glob("*.json"))
        assert len(records) == 1
        assert json.loads(records[0].read_text())["loaded_commit"] == KNOWN["commit"]
    finally:
        _finish(child)


@pytest.mark.parametrize("unknown_side", ["loaded", "published"])
def test_atomic_receipt_repair_resumes_one_authorized_unit(tmp_path, unknown_side):
    queue, child = _start(tmp_path, None if unknown_side == "loaded" else KNOWN,
                          None if unknown_side == "published" else KNOWN, continuous=True)
    try:
        prefix = _unknown_boundary(child)
        _assert_paused(queue, tmp_path)
        if unknown_side == "published":
            _replace(tmp_path / "loaded.json", MOVED)
        _replace(tmp_path / f"{unknown_side}.json", KNOWN)
        output = prefix + child.communicate(timeout=30)[0]
        assert child.returncode == 0, output.decode(errors="replace")
        _assert_one_completed(queue, tmp_path)
    finally:
        _finish(child)


@pytest.mark.parametrize("unknown_side", ["loaded", "published"])
def test_sigterm_interrupts_unknown_retry_wait(tmp_path, unknown_side):
    queue, child = _start(tmp_path, None if unknown_side == "loaded" else KNOWN,
                          None if unknown_side == "published" else KNOWN,
                          continuous=True, poll_s=60)
    try:
        prefix = _unknown_boundary(child)
        _assert_paused(queue, tmp_path)
        child.send_signal(signal.SIGTERM)
        output = prefix + child.communicate(timeout=3)[0]
        assert child.returncode == 0, output.decode(errors="replace")
        _assert_paused(queue, tmp_path)
    finally:
        _finish(child)


@pytest.mark.parametrize("continuous", [False, True])
def test_publication_lost_after_offer_cannot_authorize_a_claim(tmp_path, continuous):
    queue, child = _start(tmp_path, KNOWN, KNOWN, continuous=continuous, unpublish_on_offer=True)
    try:
        prefix = b""
        if continuous:
            prefix = _unknown_boundary(child, "claim boundary")
            _assert_paused(queue, tmp_path)
            _replace(tmp_path / "published.json", KNOWN)
        output = prefix + child.communicate(timeout=30)[0]
        assert child.returncode == (0 if continuous else 75), output.decode(errors="replace")
        assert (tmp_path / "publication-removed").is_file()
        if continuous:
            _assert_one_completed(queue, tmp_path)
        else:
            _assert_paused(queue, tmp_path)
    finally:
        _finish(child)
