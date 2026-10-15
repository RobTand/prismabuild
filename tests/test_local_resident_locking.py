"""Lock-order regressions with a live caller and an expired evictor clock."""
from contextlib import contextmanager
import fcntl
import json
import os
from pathlib import Path
import subprocess
import sys
import threading

import pytest

from prismabuild import local_resident, local_tier, pool
from test_local_resident_mover import world

from local_resident_space import roomy_disk  # noqa: F401

pytestmark = pytest.mark.usefixtures("roomy_disk")


HOLD_AND_EVICT = """
import json, sys
from prismabuild import local_resident, posix_lock, resident_sets
queue_root, set_id, host, spec_json = sys.argv[1:5]
store = resident_sets.ResidentSets(queue_root)
spec = json.loads(spec_json)
lock = store.copy_path(set_id, host).with_suffix('.move.lock')
with posix_lock.held(lock):
    print('held', flush=True)
    if sys.stdin.readline().strip() != 'go':
        raise SystemExit('caller did not enter its mover-lock acquire')
    result = local_resident.evict_resident_copy(store, set_id, host, spec, now=201)
    print('evicted ' + result['state'], flush=True)
"""


def _live_lease_race(tmp_path, monkeypatch, operation, *, expire_while_waiting=False):
    store, record, spec = world(tmp_path)
    set_id = record["set_id"]
    # Start with occupied bytes and their publication hold. The caller sees a
    # LIVE until=150/max=200 lease at now=120; only the evictor sees now=201.
    local_resident.copy(store, set_id, "test-host", spec, now=120)
    source = tmp_path / "manual"
    if operation == "adopt":
        source.mkdir()
        (source / "weights").write_bytes(b"weights")
        source_inode = (source / "weights").stat().st_ino
    function = local_resident.copy if operation == "copy" else local_resident.adopt_resident_copy
    mover_lock = store.copy_path(set_id, "test-host").with_suffix(".move.lock").resolve()
    child = subprocess.Popen([sys.executable, "-c", HOLD_AND_EVICT,
        str(store.queue_root), set_id, "test-host", json.dumps(spec)],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    entering = threading.Event()
    outcome = {}
    caller = None
    original_lockf = local_resident.posix_lock._lockf
    clock = {"now": 120}

    def observed_lockf(descriptor, blocking):
        if (threading.current_thread() is caller and blocking
                and Path(os.readlink(f"/proc/self/fd/{descriptor}")) == mover_lock):
            # The child positively holds THIS lock. Signal immediately before
            # the caller's real blocking kernel acquire, never by a timed sleep.
            entering.set()
        return original_lockf(descriptor, blocking)

    def call():
        try:
            args = (store, set_id, "test-host", spec)
            if operation == "adopt":
                args += (source,)
            outcome["result"] = function(*args, now=None if expire_while_waiting else 120)
        except BaseException as exc:
            outcome["error"] = exc

    try:
        assert child.stdout.readline().strip() == "held"
        assert local_resident.lease_active(store, set_id, now=120)
        assert not local_resident.lease_active(store, set_id, now=201)
        with monkeypatch.context() as patch:
            patch.setattr(local_resident.posix_lock, "_lockf", observed_lockf)
            if expire_while_waiting:
                patch.setattr(local_resident.time, "time", lambda: clock["now"])
            caller = threading.Thread(target=call)
            caller.start()
            assert entering.wait(10), "caller did not enter the real blocking mover-lock acquire"
            if expire_while_waiting:
                # Only advance after the caller has entered its blocking acquire,
                # and before the eviction child is permitted to unlock.
                clock["now"] = 201
            child.stdin.write("go\n")
            child.stdin.flush()
            caller.join(30)
            assert not caller.is_alive(), "caller did not finish after eviction released the lock"
            stdout, stderr = child.communicate(timeout=10)
            assert child.returncode == 0, stderr
            assert stdout.strip() == "evicted absent"
        if expire_while_waiting:
            assert isinstance(outcome.get("error"), ValueError), "caller must refuse an expired lease after locking"
            assert str(outcome["error"]) == "resident lease expired"
            assert "result" not in outcome
            assert store.read_copy(set_id, "test-host")["state"] == "absent"
            ledger = pool.PoolQueue(store.queue_root).tier_ledger("local:test-host")
            assert ledger.holder_tokens(set_id).get("local_gib", 0) == 0
            assert not Path(spec["root"]).joinpath(set_id).exists()
            assert not Path(spec["root"]).joinpath(set_id + ".partial").exists()
            if operation == "adopt":
                assert source.exists()
                assert (source / "weights").stat().st_ino == source_inode
            return
        assert "error" not in outcome, outcome.get("error")
        assert outcome["result"]["state"] == "resident"
        assert store.read_copy(set_id, "test-host")["state"] == "resident"
        ledger = pool.PoolQueue(store.queue_root).tier_ledger("local:test-host")
        assert ledger.held().get("local_gib", 0) == 1, "resident copy must hold its tokens"
        final = Path(outcome["result"]["local_root"])
        assert (final / "weights").read_bytes() == b"weights"
        assert not final.with_name(set_id + ".evicting").exists()
        assert not final.with_name(set_id + ".partial").exists()
        if operation == "adopt":
            assert not source.exists(), "adoption must consume its source exactly once"
            assert (final / "weights").stat().st_ino == source_inode
            # A retry verifies the already moved tree without consuming it twice
            # or taking another occupancy reservation.
            replay = function(store, set_id, "test-host", spec, source, now=120)
            assert replay["state"] == "resident"
            assert ledger.held().get("local_gib", 0) == 1
            assert (final / "weights").stat().st_ino == source_inode
    finally:
        if child.poll() is None:
            if not child.stdin.closed:
                child.stdin.close()
            child.wait(timeout=10)
        if caller is not None:
            caller.join(30)


@pytest.mark.parametrize("operation", ["copy", "adopt"])
def test_live_lease_caller_blocked_on_mover_lock_lands_with_tokens(tmp_path, monkeypatch, operation):
    _live_lease_race(tmp_path, monkeypatch, operation)


@pytest.mark.parametrize("operation", ["copy", "adopt"])
def test_lease_expiring_while_waiting_is_checked_after_the_lock(tmp_path, monkeypatch, operation):
    _live_lease_race(tmp_path, monkeypatch, operation, expire_while_waiting=True)


def test_copy_refuses_an_expired_lease_before_any_byte(tmp_path):
    store, record, spec = world(tmp_path)
    store.release(record["set_id"], by="test")
    ledger = pool.PoolQueue(store.queue_root).tier_ledger("local:test-host")
    # Remove the publication hold for this absent, byte-less fixture. A
    # refused new attempt must not create a fresh occupancy reservation.
    ledger.release(record["set_id"])
    with pytest.raises(ValueError, match="resident lease expired"):
        local_resident.copy(store, record["set_id"], "test-host", spec)
    assert store.read_copy(record["set_id"], "test-host")["state"] == "absent"
    assert not Path(spec["root"]).joinpath(record["set_id"]).exists()
    assert not Path(spec["root"]).joinpath(record["set_id"] + ".partial").exists()
    assert ledger.holder_tokens(record["set_id"]).get("local_gib", 0) == 0


def test_adopt_refuses_an_expired_lease_and_keeps_the_source(tmp_path):
    store, record, spec = world(tmp_path)
    store.release(record["set_id"], by="test")
    ledger = pool.PoolQueue(store.queue_root).tier_ledger("local:test-host")
    ledger.release(record["set_id"])
    source = tmp_path / "manual"
    source.mkdir()
    (source / "weights").write_bytes(b"weights")
    with pytest.raises(ValueError, match="resident lease expired"):
        local_resident.adopt_resident_copy(store, record["set_id"], "test-host", spec, source)
    assert (source / "weights").read_bytes() == b"weights"
    assert not Path(spec["root"]).joinpath(record["set_id"]).exists()
    assert store.read_copy(record["set_id"], "test-host")["state"] == "absent"
    assert ledger.holder_tokens(record["set_id"]).get("local_gib", 0) == 0


def _host_locked(root):
    fd = os.open(Path(root) / ".resident.lock", os.O_RDWR)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return True
        return False
    finally:
        os.close(fd)


def _pin_protection(store, set_id, spec, monkeypatch):
    checked = []
    original = local_resident._pinned

    def observe(*args):
        assert _host_locked(spec["root"]), "pin check must run under the host flock"
        checked.append(True)
        return original(*args)

    with monkeypatch.context() as patch:
        patch.setattr(local_resident, "_pinned", observe)
        result = local_resident.evict_resident_copy(store, set_id, "test-host", spec, now=201)
    assert result["reason"] == "pinned"
    assert checked
    assert store.read_copy(set_id, "test-host")["state"] == "resident", "pinned copy must stay resident"
    assert not Path(spec["root"]).joinpath(set_id + ".evicting").exists()


def _pinned_world(tmp_path):
    store, record, spec = world(tmp_path)
    local_resident.copy(store, record["set_id"], "test-host", spec, now=120)
    context = {"action_key": "a" * 64, "nonce": "b" * 32, "scope_id": "s"}
    token = local_resident.pin(store, record["set_id"], "test-host", spec, context, now=120)
    return store, record["set_id"], spec, token


def test_pinned_refusal_leaves_record_resident_and_lock_held_during_check(tmp_path, monkeypatch):
    store, set_id, spec, token = _pinned_world(tmp_path)
    _pin_protection(store, set_id, spec, monkeypatch)
    local_resident.release_pin(store, set_id, "test-host", spec, token)


def test_legacy_attempt_without_served_from_still_validates(tmp_path):
    from admitted_queue_fixture import AdmittedQueueFixture
    queue = AdmittedQueueFixture(pool.PoolQueue(tmp_path / "queue"), capacity={"cpu": 1, "mem_gb": 2},
                                 default_demand={"cpu": 1, "mem_gb": 1})
    queue.ensure_layout()
    key = "c" * 64
    queue.publish(action_key=key, cas_root=str(tmp_path / "cas"), checkout_root=str(tmp_path),
                  worker_script="/worker.py", max_attempts=1)
    assert queue.claim() is not None
    terminal = json.loads(queue.finish(key, status="executed").read_text())
    assert queue.attempt_outcomes(terminal)[0]["served_from"] == "canonical"
    outcome_path = queue.root / terminal["attempt_history"][0]["outcome"]
    legacy = json.loads(outcome_path.read_text())
    legacy.pop("served_from")
    legacy.pop("resident_set", None)
    outcome_path.chmod(0o644)
    outcome_path.write_text(json.dumps(legacy))
    outcome_path.chmod(0o444)
    assert "served_from" not in queue.attempt_outcomes(terminal)[0]
