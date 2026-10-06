import fcntl
import json
import os
from pathlib import Path

import pytest

from prismabuild import pool, resident_sets
from test_local_resident_mover import world


def resident(tmp_path):
    from prismabuild import local_resident
    store, record, spec = world(tmp_path)
    local_resident.copy(store, record["set_id"], "test-host", spec, now=120)
    return store, record, spec


def test_until_live_rows_and_hard_max_are_one_lease_rule(tmp_path):
    from prismabuild import local_resident
    store, record, spec = resident(tmp_path)
    assert local_resident.lease_active(store, record["set_id"], now=149)
    assert not local_resident.lease_active(store, record["set_id"], now=151)
    ready = store.queue_root / pool.READY
    ready.mkdir(exist_ok=True)
    (ready / ("a" * 64 + ".json")).write_text(json.dumps({"resident_set": record["set_id"]}))
    assert local_resident.lease_active(store, record["set_id"], now=151)
    assert not local_resident.lease_active(store, record["set_id"], now=200)
    store.release(record["set_id"], by="test", now=120)
    assert not local_resident.lease_active(store, record["set_id"], now=121)


def test_campaign_release_and_explicit_renewal_leave_body_immutable(tmp_path):
    from prismabuild import local_resident
    from test_resident_sets_records import publish
    store, record = publish(tmp_path, lease={"campaign": "campaign-one", "hard_max": 200})
    before = store.set_path(record["set_id"]).read_bytes()
    assert local_resident.lease_active(store, record["set_id"], now=190)
    assert not local_resident.lease_active(store, record["set_id"], now=200)
    store.release(record["set_id"], by="test", now=120)
    assert not local_resident.lease_active(store, record["set_id"], now=121)
    store.renew(record["set_id"], {"until": 250, "hard_max": 300}, by="test", now=201)
    assert local_resident.lease_active(store, record["set_id"], now=250 - .1)
    assert store.set_path(record["set_id"]).read_bytes() == before


def test_pin_is_the_only_protection_and_maximum_stops_new_pins(tmp_path):
    from prismabuild import local_resident
    store, record, spec = resident(tmp_path)
    ctx = {"action_key": "a" * 64, "nonce": "b" * 32, "scope_id": "test-scope"}
    pin = local_resident.pin(store, record["set_id"], "test-host", spec, ctx, now=120)
    with pytest.raises(ValueError, match="lease"):
        local_resident.pin(store, record["set_id"], "test-host", spec, ctx, now=201)
    assert local_resident.evict_resident_copy(store, record["set_id"], "test-host", spec, now=201)["reason"] == "pinned"
    local_resident.release_pin(store, record["set_id"], "test-host", spec, pin)
    assert local_resident.evict_resident_copy(store, record["set_id"], "test-host", spec, now=201)["state"] == "absent"


def held(root):
    fd = os.open(Path(root) / ".resident.lock", os.O_RDWR)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return True
        return False
    finally:
        os.close(fd)


def test_evicting_is_filed_under_lock_before_rename_delete_then_release(tmp_path, monkeypatch):
    from prismabuild import local_resident
    store, record, spec = resident(tmp_path)
    queue = pool.PoolQueue(store.queue_root)
    original_rename = local_resident.os.rename
    original_delete = local_resident.shutil.rmtree
    events = []
    def rename(source, target):
        if Path(source).parent == Path(spec["root"]):
            assert held(spec["root"])
            assert store.read_copy(record["set_id"], "test-host")["state"] == "evicting"
            events.append("rename")
        return original_rename(source, target)
    def delete(path):
        assert not held(spec["root"])
        assert queue.tier_ledger("local:test-host").held()["local_gib"] == 1
        events.append("delete")
        return original_delete(path)
    monkeypatch.setattr(local_resident.os, "rename", rename)
    monkeypatch.setattr(local_resident.shutil, "rmtree", delete)
    assert local_resident.evict_resident_copy(store, record["set_id"], "test-host", spec, now=201)["state"] == "absent"
    assert events == ["rename", "delete"]
    assert queue.tier_ledger("local:test-host").held() == {}


def test_restart_finishes_evicting_tree_before_mint(tmp_path, monkeypatch):
    from prismabuild import local_resident, local_tier
    import local_tier_loop
    store, record, spec = resident(tmp_path)
    original = local_resident.shutil.rmtree
    monkeypatch.setattr(local_resident.shutil, "rmtree", lambda _: (_ for _ in ()).throw(OSError("interrupted delete")))
    with pytest.raises(OSError, match="interrupted"):
        local_resident.evict_resident_copy(store, record["set_id"], "test-host", spec, now=201)
    assert store.read_copy(record["set_id"], "test-host")["state"] == "evicting"
    assert pool.PoolQueue(store.queue_root).tier_ledger("local:test-host").held()["local_gib"] == 1
    monkeypatch.setattr(local_resident.shutil, "rmtree", original)
    mint = local_tier.mint_local_tier_capacity
    def checked(queue, host, policy):
        assert not Path(spec["root"]).joinpath(record["set_id"] + ".evicting").exists()
        assert queue.tier_ledger("local:test-host").held() == {}
        return mint(queue, host, policy)
    monkeypatch.setattr(local_tier, "mint_local_tier_capacity", checked)
    local_tier_loop.local_tier_cycle(pool.PoolQueue(store.queue_root), "test-host", {"hosts": {"test-host": spec}})


def test_remote_egress_uses_retained_host_pinned_action(tmp_path, monkeypatch):
    from prismabuild import local_resident
    store, record, spec = resident(tmp_path)
    row = {"action_key": "e" * 64, "cas_root": str(tmp_path / "cas"),
           "worker_script": "/worker.py", "checkout_root": str(tmp_path), "tags": ["test-host"],
           "resources": {"cpu": 1, "mem_gb": 1}}
    store.update_movements(record["set_id"], "test-host", {"evict": row})
    def no_local(*args, **kwargs):
        raise AssertionError("cross-host request must not delete in caller")
    monkeypatch.setattr(local_resident, "evict_resident_copy", no_local)
    result = local_resident.request_eviction(store, record["set_id"], "test-host", spec,
                                           caller_host="elsewhere", now=201)
    assert result["state"] == "queued"
    assert pool.PoolQueue(store.queue_root).item_path(pool.READY, "e" * 64).exists()


def test_busy_mover_is_not_deleted(tmp_path):
    from prismabuild import local_resident, posix_lock
    store, record, spec = resident(tmp_path)
    lock = store.copy_path(record["set_id"], "test-host").with_suffix(".move.lock")
    # Same-thread POSIX locks are reentrant, so use a distinct child process.
    import subprocess
    import sys
    program = "import fcntl,sys; f=open(sys.argv[1],'a'); fcntl.lockf(f,fcntl.LOCK_EX); print('held',flush=True); sys.stdin.read()"
    child = subprocess.Popen([sys.executable, "-c", program, str(lock)], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
    try:
        assert child.stdout.readline().strip() == "held"
        assert local_resident.evict_resident_copy(store, record["set_id"], "test-host", spec, now=201)["reason"] == "copy_in_progress"
    finally:
        child.stdin.close()
        child.wait()
