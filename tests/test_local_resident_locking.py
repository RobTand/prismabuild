import json
import subprocess
import sys
import threading
from pathlib import Path

import pytest

from prismabuild import pool, resident_sets
from test_local_resident_mover import world


def spec_json(spec):
    return json.dumps(spec)


def _child(script, *args):
    return subprocess.Popen([sys.executable, "-c", script, *map(str, args)],
                            stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)


HOLD_AND_EVICT = """
import json, sys
import fcntl
from prismabuild import local_resident, resident_sets
queue_root, set_id, host, spec = sys.argv[1:5]
spec = json.loads(spec)
lock = resident_sets.ResidentSets(queue_root).copy_path(set_id, host).with_suffix('.move.lock')
handle = open(lock, 'a')
fcntl.lockf(handle, fcntl.LOCK_EX)
print('held', flush=True)
sys.stdin.readline()
result = local_resident.evict(resident_sets.ResidentSets(queue_root), set_id, host, spec, now=201)
print('evicted ' + result['state'], flush=True)
"""


def test_copy_after_a_completed_eviction_holds_tokens_again(tmp_path):
    """B1: the reserve must happen inside the mover lock, after the lease check.

    The published hold is still present when copy() calls reserve() today, so
    it acquires nothing; the eviction then releases the ledger while this
    copy waits on the mover lock, and the copy lands resident holding zero
    tokens.
    """
    from prismabuild import local_resident
    store, record, spec = world(tmp_path)
    child = _child(HOLD_AND_EVICT, store.queue_root, record["set_id"], "test-host", spec_json(spec))
    try:
        assert child.stdout.readline().strip() == "held"
        outcome = {}

        def run():
            try:
                outcome["result"] = local_resident.copy(store, record["set_id"], "test-host", spec, now=201)
            except ValueError as exc:
                outcome["refusal"] = str(exc)

        worker = threading.Thread(target=run)
        worker.start()
        child.stdin.write("go\n")
        child.stdin.flush()
        worker.join(60)
        assert not worker.is_alive()
        assert child.stdout.readline().strip() == "evicted absent"
        if "result" in outcome:
            ledger = pool.PoolQueue(store.queue_root).tier_ledger("local:test-host")
            assert ledger.held()["local_gib"] == 1, "resident copy must hold its tokens"
        else:
            assert "lease" in outcome["refusal"]
    finally:
        child.stdin.close()
        child.wait(timeout=30)


def test_copy_refuses_an_expired_lease_before_any_byte(tmp_path):
    from prismabuild import local_resident
    store, record, spec = world(tmp_path)
    with pytest.raises(ValueError, match="lease"):
        local_resident.copy(store, record["set_id"], "test-host", spec, now=201)
    assert store.read_copy(record["set_id"], "test-host")["state"] != "resident"
    assert not Path(spec["root"]).joinpath(record["set_id"]).exists()
    assert not Path(spec["root"]).joinpath(record["set_id"] + ".partial").exists()


def test_adopt_refuses_an_expired_lease_and_keeps_the_source(tmp_path):
    from prismabuild import local_resident
    store, record, spec = world(tmp_path)
    source = tmp_path / "manual"
    source.mkdir()
    (source / "weights").write_bytes(b"weights")
    with pytest.raises(ValueError, match="lease"):
        local_resident.adopt(store, record["set_id"], "test-host", spec, source, now=201)
    assert (source / "weights").read_bytes() == b"weights"
    assert not Path(spec["root"]).joinpath(record["set_id"]).exists()
    assert store.read_copy(record["set_id"], "test-host")["state"] == "absent"


def held(root):
    import fcntl
    import os
    fd = os.open(Path(root) / ".resident.lock", os.O_RDWR)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return True
        return False
    finally:
        os.close(fd)


def test_pinned_refusal_leaves_record_resident_and_lock_held_during_check(tmp_path, monkeypatch):
    from prismabuild import local_resident
    store, record, spec = world(tmp_path)
    local_resident.copy(store, record["set_id"], "test-host", spec, now=120)
    ctx = {"action_key": "a" * 64, "nonce": "b" * 32, "scope_id": "s"}
    token = local_resident.pin(store, record["set_id"], "test-host", spec, ctx, now=120)
    seen = {}
    original = local_resident._pinned

    def checked(store_, set_id, host, final):
        seen["host_lock_held"] = held(spec["root"])
        return original(store_, set_id, host, final)

    monkeypatch.setattr(local_resident, "_pinned", checked)
    result = local_resident.evict(store, record["set_id"], "test-host", spec, now=201)
    assert result["reason"] == "pinned"
    assert seen["host_lock_held"] is True
    assert store.read_copy(record["set_id"], "test-host")["state"] == "resident"
    assert not Path(spec["root"]).joinpath(record["set_id"] + ".evicting").exists()
    local_resident.release_pin(store, record["set_id"], "test-host", spec, token)


def test_reordering_the_pin_check_fails_the_protection_test(tmp_path, monkeypatch):
    """N2(b): a mutant that checks pins after writing evicting must fail.

    The strengthened assertions above (state resident, no .evicting tree)
    are what catch it; this test executes the mutant to prove it.
    """
    from prismabuild import local_resident
    store, record, spec = world(tmp_path)
    local_resident.copy(store, record["set_id"], "test-host", spec, now=120)
    ctx = {"action_key": "a" * 64, "nonce": "b" * 32, "scope_id": "s"}
    local_resident.pin(store, record["set_id"], "test-host", spec, ctx, now=120)
    root, final, partial, evicting = local_resident._paths(record["set_id"], spec)
    import os as _os

    def mutant(store_, set_id_, host_, spec_, *, now_=None):
        with local_resident.posix_lock.held(store_.copy_path(set_id_, host_).with_suffix(".move.lock"), blocking=False) as got:
            if not got:
                return {"state": "copying", "reason": "copy_in_progress"}
            with local_tier_host_lock(spec_["root"]):
                current = store_.read_copy(set_id_, host_)
                store_.write_copy(set_id_, host_, {**current, "state": "evicting"})
                if local_resident._pinned(store_, set_id_, host_, final):
                    return {"state": "evicting", "reason": "pinned"}
        return {"state": "evicting", "reason": "unreachable"}

    from prismabuild import local_tier

    def local_tier_host_lock(path):
        return local_tier.host_lock(path)

    monkeypatch.setattr(local_resident, "evict", mutant)
    result = local_resident.evict(store, record["set_id"], "test-host", spec, now=201)
    assert result["reason"] == "pinned"
    # The mutant already wrote evicting and left the tree renamed-eligible:
    assert store.read_copy(record["set_id"], "test-host")["state"] == "resident", (
        "a pinned refusal must never leave the record evicting")
    assert not evicting.exists()


def test_legacy_attempt_without_served_from_still_validates(tmp_path):
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    key = "c" * 64
    queue.publish(action_key=key, cas_root=str(tmp_path / "cas"), checkout_root=str(tmp_path),
                  worker_script="/worker.py", max_attempts=1)
    assert queue.claim() is not None
    terminal = json.loads(queue.finish(key, status="executed").read_text())
    attempts = queue.attempt_outcomes(terminal)
    assert attempts[0]["served_from"] == "canonical"
    # A legacy attempt record, rewritten without the field, must still read.
    outcome_path = queue.root / terminal["attempt_history"][0]["outcome"]
    legacy = json.loads(outcome_path.read_text())
    legacy.pop("served_from")
    legacy.pop("resident_set", None)
    outcome_path.chmod(0o644)
    outcome_path.write_text(json.dumps(legacy))
    outcome_path.chmod(0o444)
    reread = queue.attempt_outcomes(terminal)
    assert "served_from" not in reread[0]
