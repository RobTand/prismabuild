"""Deterministic regressions for agreement among heartbeat freshness judges."""
import subprocess
import sys

import pytest

from prismabuild import census, pool, record_store
from test_pool_transition_lock import publish
from test_heartbeat_liveness_2230 import _busy_key

WINDOW = {"evidence_unix": None, "accepted_count": 0, "staged_wait_exempt_s": 0.0}
GRACE = {"accepted_count": 0, "staged_wait_exempt_s": 0.0, "quiet_s": 0.0, "grace_s": 720.0}


@pytest.fixture(autouse=True)
def clock(monkeypatch):
    value = [2_000_000_000.]
    monkeypatch.setattr(pool, "_now", lambda: value[0])
    monkeypatch.setattr(census.time, "time", lambda: value[0])
    return value


def _claimed(queue, key, owner):
    publish(queue, key)
    item = queue.claim(owner=owner, capacity={"cpu": 2})
    assert item and item["action_key"] == key
    queue.write_lease(key, owner=owner, claim_snapshot=item, progress_observation=GRACE)
    return item


def _rewrite_beat(queue, key, beat):
    lease = record_store.read_document(queue.records.lease_path(key))
    lease["heartbeat_unix"] = beat
    queue.records.replace_record_at(queue.records.lease_path(key), lease)
    return lease


def _dead_pid():
    child = subprocess.Popen([sys.executable, "-c", "pass"])
    child.wait(timeout=60)
    return child.pid

@pytest.mark.parametrize("beat", ["future", "malformed"])
def test_reaper_pass_survives_skewed_live_lease_and_recovers_dead_owner(tmp_path, beat, capsys, monkeypatch):
    """A writer host 2 s ahead of the reaper must not stop dead-owner recovery."""
    queue = pool.PoolQueue(tmp_path / "queue")
    skewed, dead = "0" * 63 + "1", "f" * 63 + "1"
    _claimed(queue, skewed, "w1")
    _claimed(queue, dead, "w2")
    _rewrite_beat(queue, skewed, pool._now() + 2.0 if beat == "future" else "invalid")
    lease = _rewrite_beat(queue, dead, pool._now() - pool.LEASE_TIMEOUT_S - 60)
    lease["pid"] = _dead_pid()
    queue.records.replace_record_at(queue.records.lease_path(dead), lease)
    paths = queue.records.json_slot_paths
    def pass_order(state, *args, **kwargs):
        if state == record_store.CLAIMED:
            return tuple(queue.records.item_path(state, key) for key in (skewed, dead))
        return paths(state, *args, **kwargs)
    monkeypatch.setattr(queue.records, "json_slot_paths", pass_order)
    if beat == "future":
        assert queue.lease_age(skewed) == 0.0
    reaped = queue.reap_stale()
    assert reaped == [dead]
    if beat == "malformed":
        assert skewed in capsys.readouterr().err
    assert queue.records.item_path(record_store.CLAIMED, skewed).exists()


def test_mover_and_order_judges_count_a_beat_published_after_their_clock_sample(tmp_path, clock):
    """A caller clock sample must not override a later heartbeat observation."""
    queue = pool.PoolQueue(tmp_path / "queue")
    key = "a" * 64
    item = _claimed(queue, key, "worker")
    now = pool._now()
    clock[0] += .5
    queue.write_lease(key, owner="worker", claim_snapshot=item, progress_observation=GRACE)
    mover = queue._mover_lease(key, item, now=now)
    live, evidence = queue._claim_order_ahead_live(
        key, now=now, window_s=60.0, prior=dict(WINDOW, evidence_unix=now - 3600))
    age = queue.lease_age(key)
    assert age is not None and age <= pool.LEASE_TIMEOUT_S      # reaper: live
    assert mover is not None and mover["lease_live"] is True    # mover must agree
    assert live is True                                          # order must agree


def test_contended_stamp_after_clock_sample_keeps_mover_and_reaper_in_agreement(tmp_path, clock):
    """Lease bytes > LEASE_TIMEOUT_S old; a real contended stamp lands after ``now``."""
    root = tmp_path / "queue"
    queue = pool.PoolQueue(root)
    key = "a" * 64
    item = _claimed(queue, key, "worker")
    _rewrite_beat(queue, key, pool._now() - pool.LEASE_TIMEOUT_S - 60)
    now = clock[0]
    before = queue.records.lease_path(key).read_bytes()
    with _busy_key(queue, key):
        clock[0] += .5
        queue.write_lease(key, owner="worker", claim_snapshot=item, progress_observation=GRACE)
    assert queue.records.lease_path(key).read_bytes() == before
    age = queue.lease_age(key)
    mover = queue._mover_lease(key, item, now=now)
    live, evidence = queue._claim_order_ahead_live(
        key, now=now, window_s=60.0, prior=dict(WINDOW, evidence_unix=now - 3600))
    assert age <= pool.LEASE_TIMEOUT_S
    assert mover is not None and mover["lease_live"] is True
    assert live is True


def test_status_census_keeps_a_slightly_skewed_live_lease_readable(tmp_path):
    queue = pool.PoolQueue(tmp_path / "queue")
    key = "a" * 64
    _claimed(queue, key, "worker")
    _rewrite_beat(queue, key, pool._now() + 2.0)
    result = census.action_rows(queue.root)
    row = next(row for row in result.value["jobs"] if row["action_key"] == key)
    assert row["state"] == "CLAIMED"
    assert result.value["queue"]["complete"] is True


@pytest.mark.parametrize("judge", ["mover", "order"])
def test_stamp_landing_between_lease_and_stamp_reads_is_live(tmp_path, monkeypatch, judge):
    queue = pool.PoolQueue(tmp_path / "queue")
    key = "a" * 64
    item = _claimed(queue, key, "worker")
    before = pool._now()
    lease = _rewrite_beat(queue, key, before - 360)
    read = record_store.read_document
    clock = [before]
    reads = []
    def arrival(path):
        value = read(path)
        if path == queue.records.lease_path(key):
            clock[0] = before + 0.5
            queue.records.replace_record_at(queue._heartbeat_stamp_path(lease),
                dict(lease, heartbeat_unix=clock[0]))
        reads.append(path)
        return value
    monkeypatch.setattr(record_store, "read_document", arrival)
    monkeypatch.setattr(pool, "_now", lambda: clock[0])
    if judge == "mover":
        assert queue._mover_lease(key, item, now=before)["lease_live"] is True
    else:
        live, evidence = queue._claim_order_ahead_live(key, now=before, window_s=60.,
            prior=dict(WINDOW, evidence_unix=before - 3600))
        assert live and evidence["evidence"] == "ahead-within-grace"
    assert queue._heartbeat_stamp_path(lease) in reads


def test_a_liveness_stamp_does_not_renew_stale_progress(tmp_path):
    queue = pool.PoolQueue(tmp_path / "queue")
    key = "a" * 64
    _claimed(queue, key, "worker")
    lease = _rewrite_beat(queue, key, pool._now() - 800)
    queue.records.replace_record_at(queue._heartbeat_stamp_path(lease),
        dict(lease, heartbeat_unix=pool._now()))
    assert queue.lease_age(key) < pool.LEASE_TIMEOUT_S
    live, evidence = queue._claim_order_ahead_live(key, now=pool._now(), window_s=60.,
        prior=dict(WINDOW, evidence_unix=pool._now() - 3600))
    assert live is False and evidence["evidence"] == "none"


def test_all_freshness_ages_are_sampled_after_the_stamp_read(tmp_path, monkeypatch):
    queue = pool.PoolQueue(tmp_path / "queue")
    key = "a" * 64
    _claimed(queue, key, "worker")
    lease = record_store.read_document(queue.records.lease_path(key))
    events = []
    read = record_store.read_document
    now = pool._now()
    def observe(path):
        events.append("read")
        return read(path)
    def clock():
        events.append("clock")
        return now + 1
    monkeypatch.setattr(pool, "_now", clock)
    timing = queue._effective_heartbeat(lease, now=now - 1000, read=observe)
    assert events == ["read", "clock"]
    assert timing.age_s == 1.0


def test_foreign_action_key_cannot_select_a_stamp_destination(tmp_path):
    from test_heartbeat_liveness_2230 import _busy_key
    queue = pool.PoolQueue(tmp_path / "queue")
    key = "a" * 64
    item = _claimed(queue, key, "worker")
    path = queue.records.lease_path(key)
    lease = record_store.read_document(path)
    lease["action_key"] = "f" * 64
    queue.records.replace_record_at(path, lease)
    before = path.read_bytes()
    with _busy_key(queue, key):
        with pytest.raises(pool.PoolContractError, match="changed"):
            queue.write_lease(key, owner="worker", claim_snapshot=item)
    assert path.read_bytes() == before
    assert not queue._heartbeat_stamp_path(lease).exists()
