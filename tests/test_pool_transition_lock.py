"""A repeated action key cannot acquire a predecessor's ownership paths."""
from concurrent.futures import ThreadPoolExecutor
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
    queue._start_resource_scope(item)
