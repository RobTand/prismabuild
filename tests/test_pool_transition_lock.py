"""A repeated action key cannot acquire a predecessor's ownership paths."""
from concurrent.futures import ThreadPoolExecutor
import pytest
from prismabuild import pool
from test_pool_resource_scope import scoped

KEY = "d" * 64
OTHER = "e" * 64


def publish(queue, key):
    queue.publish(action_key=key, cas_root="/cas", checkout_root="/checkout",
                  worker_script="/worker.py", resources={"cpu": 1})


def test_successor_waits_for_holder_cleanup(tmp_path):
    queue = pool.PoolQueue(tmp_path / "queue")
    publish(queue, KEY)
    first = queue.claim(owner="first", capacity={"cpu": 2})
    publish(queue, KEY)
    lease = queue.lease_path(KEY).read_bytes()
    assert queue.claim(owner="second", capacity={"cpu": 2}) is None
    assert pool._same_claim(pool._read_json(queue.item_path(pool.CLAIMED, KEY)), first)
    assert queue.lease_path(KEY).read_bytes() == lease
    tombstone, taken = queue._entomb_claim(KEY, expect=first)
    assert taken and tombstone is not None
    assert queue.claim(owner="second", capacity={"cpu": 2}) is None
    queue.sweep_finish_tombstones(grace_s=-1)
    queue.finish(KEY, status="executed", detail={"returncode": 0}, claim_snapshot=first)
    successor = queue.claim(owner="second", capacity={"cpu": 2})
    assert successor and successor["claimed_by"] == "second"


def test_busy_key_does_not_block_independent_claim(tmp_path):
    queue = pool.PoolQueue(tmp_path / "queue")
    publish(queue, KEY)
    publish(queue, OTHER)
    with ThreadPoolExecutor(max_workers=1) as executor:
        with queue._transition_locked(KEY):
            claimed = executor.submit(queue.claim, owner="worker", capacity={"cpu": 2}).result(timeout=5)
    assert claimed["action_key"] == OTHER
    assert queue.item_path(pool.READY, KEY).exists()


def test_scope_start_keeps_creation_with_its_claim(scoped, monkeypatch):
    """A reaper must not conclude the claim between scope intent and create."""
    from prismabuild import resource_scope
    queue, item, calls = scoped
    original = resource_scope.ResourceScope.create
    monkeypatch.setattr(queue, "cleanup_action_containers", lambda *a, **kw: {"complete": True})
    def create(scope):
        with ThreadPoolExecutor(max_workers=1) as executor:
            assert executor.submit(queue.reap_stale, timeout_s=-1).result(timeout=5) == []
        assert pool._same_claim(pool._read_json(queue.item_path(pool.CLAIMED, item["action_key"])), item)
        return original(scope)
    monkeypatch.setattr(resource_scope.ResourceScope, "create", create)
    queue._start_resource_scope(item=item)


def test_old_owner_cannot_replace_successor_lease(tmp_path):
    import pytest
    queue = pool.PoolQueue(tmp_path / "queue")
    publish(queue, KEY)
    first = queue.claim(owner="first", capacity={"cpu": 2})
    queue.finish(KEY, status="executed", detail={"returncode": 0}, claim_snapshot=first)
    publish(queue, KEY)
    queue.claim(owner="second", capacity={"cpu": 2})
    lease = queue.lease_path(KEY).read_bytes()
    with pytest.raises(pool.PoolContractError, match="claim.*changed|owner.*differs"):
        queue.write_lease(KEY, owner="first")
    assert queue.lease_path(KEY).read_bytes() == lease


def test_same_owner_old_attempt_cannot_replace_successor_lease(tmp_path):
    import pytest
    queue = pool.PoolQueue(tmp_path / "queue")
    publish(queue, KEY)
    first = queue.claim(owner="worker", capacity={"cpu": 2})
    queue.finish(KEY, status="executed", detail={"returncode": 0}, claim_snapshot=first)
    publish(queue, KEY)
    queue.claim(owner="worker", capacity={"cpu": 2})
    lease = queue.lease_path(KEY).read_bytes()
    with pytest.raises(pool.PoolContractError, match="claim changed"):
        queue.write_lease(KEY, owner="worker", claim_snapshot=first)
    assert queue.lease_path(KEY).read_bytes() == lease


def test_claim_census_does_not_trust_cached_negative_name(tmp_path, monkeypatch):
    from pathlib import Path
    queue = pool.PoolQueue(tmp_path / "queue")
    publish(queue, KEY)
    first = queue.claim(owner="first", capacity={"cpu": 2})
    publish(queue, KEY)
    claimed = queue.item_path(pool.CLAIMED, KEY)
    exists = Path.exists
    monkeypatch.setattr(Path, "exists", lambda path: False if path == claimed else exists(path))
    assert queue.claim(owner="second", capacity={"cpu": 2}) is None
    assert pool._same_claim(pool._read_json(claimed), first)


@pytest.mark.parametrize("field,value", [
    ("owner", "other-worker"), ("host", "other-host"),
    ("claimed_unix", 1.0), ("published_unix", 1.0),
])
def test_heartbeat_retains_each_conflicting_lease_identity(tmp_path, field, value):
    queue = pool.PoolQueue(tmp_path / "queue")
    publish(queue, KEY)
    first = queue.claim(owner="worker", capacity={"cpu": 1})
    lease = pool._read_json(queue.lease_path(KEY))
    lease[field] = value
    pool._write_json_atomic(queue.lease_path(KEY), lease)
    before = queue.lease_path(KEY).read_bytes()
    claim = queue.item_path(pool.CLAIMED, KEY).read_bytes()
    with pytest.raises(pool.AmbiguousClaimHolder, match="lease"):
        queue.write_lease(KEY, owner="worker", claim_snapshot=first)
    assert queue.lease_path(KEY).read_bytes() == before
    assert queue.item_path(pool.CLAIMED, KEY).read_bytes() == claim
    assert queue.ledger().held() == {"cpu": 1}


def test_heartbeat_upgrades_compatible_legacy_lease_and_refreshes_observation(tmp_path):
    queue = pool.PoolQueue(tmp_path / "queue")
    publish(queue, KEY)
    first = queue.claim(owner="worker", capacity={"cpu": 1})
    lease = pool._read_json(queue.lease_path(KEY))
    for field in ("claimed_unix", "published_unix"):
        lease.pop(field)
    pool._write_json_atomic(queue.lease_path(KEY), lease)
    for pid in (123, 456):
        observation = {"last_progress_unix": float(pid)}
        queue.write_lease(KEY, owner="worker", child_pid=pid,
                          claim_snapshot=first, execution_observation=observation)
        refreshed = pool._read_json(queue.lease_path(KEY))
        assert refreshed["child_pid"] == pid
        assert refreshed["execution_observation"] == observation
        for field in ("claimed_unix", "published_unix"):
            assert refreshed[field] == first[field]
