"""The drain-time saved-finish retry is the reaper's narrow subset (#1403).

``PoolQueue.retry_own_pending_finishes`` exists for the one caller that must
retry a saved outcome with the admission gate closed: ``worker_loop``'s
maintenance branch, which never reaches ``serve_once`` and therefore never
reached ``reap_stale``.  It must conclude this host's own ``finish_pending``
records through the ordinary ``finish`` lifecycle while touching nothing else:
no claim, no expired-lease requeue, no foreign row, no malformed row, and at
most one pass per heartbeat per box.
"""

from __future__ import annotations

import json
import socket
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from prismabuild import pool, resource_scope  # noqa: E402
from test_pool_resource_scope import _process, scoped  # noqa: E402


def _pending_owner(scoped_queue, monkeypatch, *, status: str = "executed"):
    """Run the real execute/finish path into a retained ``finish_pending``.

    The broker boundary refuses ``release`` once, exactly as a scope that has
    not drained yet: ``finish`` retains the claim, the lease, the reservation
    and the original status/detail, which is the state the drain must retry.
    The caller heals ``release`` when the retry should succeed.
    """

    queue, item, calls = scoped_queue
    _process(monkeypatch, queue, item, calls)
    outcome = queue.execute(item, containment=True)
    release = resource_scope.ResourceScope.release
    monkeypatch.setattr(
        resource_scope.ResourceScope, "release",
        lambda scope: (_ for _ in ()).throw(OSError("scope still populated")))
    queue.finish(item["action_key"], status=status,
                 detail=outcome if status == "executed" else {"returncode": 137},
                 claim_snapshot=item)
    return queue, item, release


def _claim_stale(queue: pool.PoolQueue, tmp_path: Path, key: str) -> Path:
    """A claimed record with an expired lease, as a dead claimant leaves one."""

    queue.publish(action_key=key, cas_root=tmp_path / "cas",
                  checkout_root=tmp_path / "checkout",
                  worker_script=str(tmp_path / "worker.py"),
                  tags=["another-box"], resources={"cpu": 1, "mem_gb": 1})
    path = queue.item_path(pool.READY, key).rename(
        queue.item_path(pool.CLAIMED, key))
    record = json.loads(path.read_text())
    record["claimed_unix"] = pool._now() - (pool.LEASE_TIMEOUT_S + pool.HEARTBEAT_S)
    record["claimed_by"] = "gone:1:deadbeef"
    record["claimed_host"] = "gone"
    path.write_text(json.dumps(record))
    pool._write_json_atomic(queue.lease_path(key), {
        "action_key": key, "owner": "gone:1:deadbeef", "host": "gone",
        "heartbeat_unix": pool._now() - (pool.LEASE_TIMEOUT_S + pool.HEARTBEAT_S)})
    return path


def test_a_saved_finish_retains_its_charge_until_cleanup_is_proved(
        scoped, monkeypatch):
    """Unproven cleanup keeps the claim and its tokens; proof concludes once.

    The retry is not a new attempt and not a lease loss: the saved status and
    detail stay authoritative, the failure count advances, and only a later
    sweep that can prove the exact scope empty concludes the original outcome.
    """

    queue, item, release = _pending_owner(scoped, monkeypatch)
    key = item["action_key"]
    assert queue.ledger().held() == {"cpu": 1, "mem_gb": 2}
    monkeypatch.setattr(queue, "_sweep_due", lambda: True)

    assert queue.retry_own_pending_finishes() == []
    pending = json.loads(queue.item_path(pool.CLAIMED, key).read_text())
    assert pending["finish_pending"]["status"] == "executed"
    assert pending["container_cleanup_attempts"] == 2
    assert queue.ledger().held() == {"cpu": 1, "mem_gb": 2}
    assert queue.lease_path(key).exists()
    assert not queue.item_path(pool.DONE, key).exists()

    monkeypatch.setattr(resource_scope.ResourceScope, "release", release)
    assert queue.retry_own_pending_finishes() == []
    result = json.loads(queue.item_path(pool.DONE, key).read_text())
    assert result["status"] == "executed"
    assert queue.ledger().held() == {}
    assert not queue.item_path(pool.CLAIMED, key).exists()
    assert not queue.lease_path(key).exists()


def test_an_ordinary_expired_lease_is_left_for_the_full_reaper(
        scoped, tmp_path, monkeypatch):
    """The narrow retry never requeues ordinary stale work.

    An expired lease is ``reap_stale``'s business and stays untouched here --
    bytes identical, still claimed -- while the same pass still concludes the
    host's saved finish.  The full reaper afterwards proves the ordinary path
    was not removed.
    """

    queue, item, release = _pending_owner(scoped, monkeypatch)
    key = item["action_key"]
    monkeypatch.setattr(resource_scope.ResourceScope, "release", release)

    stale_key = "e" * 64
    stale_path = _claim_stale(queue, tmp_path, stale_key)
    before = stale_path.read_bytes()

    monkeypatch.setattr(queue, "_sweep_due", lambda: True)
    assert queue.retry_own_pending_finishes() == []
    assert queue.item_path(pool.DONE, key).exists()
    assert stale_path.read_bytes() == before

    assert queue.reap_stale(timeout_s=-1) == [stale_key]
    assert queue.item_path(pool.READY, stale_key).exists()


def test_a_malformed_saved_outcome_is_skipped_without_stopping_the_sweep(
        scoped, tmp_path, monkeypatch):
    """This box's own bad saved record is reported and skipped, not fatal.

    ``reap_stale`` refuses a malformed ``finish_pending``; the drain-time
    retry must survive it so one corrupt record cannot pin the drain.  The
    record keeps its bytes and no exception escapes.
    """

    queue, item, release = _pending_owner(scoped, monkeypatch)
    key = item["action_key"]
    monkeypatch.setattr(resource_scope.ResourceScope, "release", release)

    broken_key = "0" * 64
    queue.publish(action_key=broken_key, cas_root=tmp_path / "cas",
                  checkout_root=tmp_path / "checkout",
                  worker_script=str(tmp_path / "worker.py"),
                  resources={"cpu": 1, "mem_gb": 1})
    broken_path = queue.item_path(pool.READY, broken_key).rename(
        queue.item_path(pool.CLAIMED, broken_key))
    broken = json.loads(broken_path.read_text())
    broken["claimed_by"] = "this-box:1:beef"
    broken["claimed_host"] = socket.gethostname()
    broken["finish_pending"] = ["not", "a", "mapping"]
    pool._write_json_atomic(broken_path, broken)
    before = broken_path.read_bytes()

    monkeypatch.setattr(queue, "_sweep_due", lambda: True)
    assert queue.retry_own_pending_finishes() == []
    assert broken_path.read_bytes() == before
    assert queue.item_path(pool.DONE, key).exists()


def test_a_foreign_boxes_saved_finish_is_left_alone(scoped, monkeypatch):
    """Only the claiming host retries; a foreign row is byte-identical after.

    The copied record carries the owner's exact scope authority, so a retry
    that missed the owner filter would have to refuse or rewrite it; the
    assertion is that nothing touched it at all.
    """

    queue, item, release = _pending_owner(scoped, monkeypatch)
    key = item["action_key"]
    monkeypatch.setattr(resource_scope.ResourceScope, "release", release)

    foreign_key = "f" * 64
    live = json.loads(queue.item_path(pool.CLAIMED, key).read_text())
    foreign_path = queue.item_path(pool.CLAIMED, foreign_key)
    pool._write_json_atomic(foreign_path, dict(
        live, action_key=foreign_key, claimed_by="foreign-box:1:feed",
        claimed_host="foreign-box"))
    before = foreign_path.read_bytes()

    monkeypatch.setattr(queue, "_sweep_due", lambda: True)
    assert queue.retry_own_pending_finishes() == []
    assert foreign_path.read_bytes() == before
    assert queue.item_path(pool.DONE, key).exists()


def test_the_narrow_retry_sweeps_once_per_heartbeat(tmp_path, monkeypatch):
    """One pass per heartbeat per box, not one per drain poll.

    A saved finish that lands after this heartbeat's pass waits for the next
    one, exactly as ``serve_once``'s reaper does; the marker is host-local and
    shared, so the drain and the open path spend the same sweep budget.
    """

    at = [1_788_700_000.0]
    monkeypatch.setattr(pool, "_now", lambda: at[0])
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()

    def save_finish(key: str) -> None:
        queue.publish(action_key=key, cas_root=tmp_path / "cas",
                      checkout_root=tmp_path / "checkout",
                      worker_script=str(tmp_path / "worker.py"),
                      resources={"cpu": 1, "mem_gb": 2}, max_attempts=1)
        item = queue.claim(capacity={"cpu": 2, "mem_gb": 4})
        assert item is not None and item["action_key"] == key
        record = pool._read_json(queue.item_path(pool.CLAIMED, key))
        record["finish_pending"] = {"status": "failed",
                                    "detail": {"returncode": 137}}
        pool._write_json_atomic(queue.item_path(pool.CLAIMED, key), record)

    first = "1" * 64
    save_finish(first)
    assert queue.retry_own_pending_finishes() == []
    assert queue.item_path(pool.FAILED, first).exists()

    second = "2" * 64
    save_finish(second)
    assert queue.retry_own_pending_finishes() == []
    assert queue.item_path(pool.CLAIMED, second).exists(), (
        "a second pass inside the same heartbeat must not re-read the queue")

    at[0] += pool.HEARTBEAT_S
    assert queue.retry_own_pending_finishes() == []
    assert queue.item_path(pool.FAILED, second).exists()
