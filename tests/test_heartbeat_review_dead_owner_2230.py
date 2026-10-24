"""Review probe 2: a genuinely dead owner (SIGKILL during a key-lock hold).

Real processes: worker W stamps behind holder H's lock and is SIGKILLed; H
releases; judge R reaps every second. The claim must be requeued only after
the last effective beat is older than LEASE_TIMEOUT_S, within one judge period
plus slack, and the successor W2 must run, stamp under contention, and finish.
"""
import threading
import uuid

from prismabuild import pool, record_store
from test_pool_transition_lock import publish
from heartbeat_review_harness_2230 import Actor, summary


def test_dead_owner_is_recovered_within_its_bound(tmp_path):
    root = tmp_path / "queue"
    queue = pool.PoolQueue(root)
    key = uuid.uuid4().hex + uuid.uuid4().hex
    publish(queue, key)
    runs = tmp_path / "runs.txt"
    w = Actor("worker", root=str(root), key=key, owner="worker", interval_s=5.0, runs=str(runs))
    actors = [w]
    try:
        w.expect("claimed", 120)
        r = Actor("judge", root=str(root), key=key, period_s=1.0)
        actors.append(r)
        r.expect("judge", 120)
        h = Actor("holder", root=str(root), key=key, hold_s=60.0)
        actors.append(h)
        h.expect("acquired", 120)
        w.expect("beat", 60, where=lambda e: e["stamped"])
        w.expect("beat", 60, where=lambda e: e["stamped"])
        killed = w.kill()
        lease = record_store.read_document(queue.records.lease_path(key))
        stamp = record_store.read_document(queue._heartbeat_stamp_path(lease))
        last_beat = max(lease["heartbeat_unix"], stamp["heartbeat_unix"])
        detected = queue._heartbeat_cleanup_due(lease)
        h.expect("released", 180)
        w2 = Actor("claimer", root=str(root), owner="worker2", period_s=1.0, runs=str(runs),
                   finish=True, interval_s=2.0, beats=8)
        actors.append(w2)
        claimed = w2.expect("claimed", last_beat + pool.LEASE_TIMEOUT_S + 120 - killed)
        # Successor beats under a fresh contended hold.
        h2 = Actor("holder", root=str(root), key=key, hold_s=7.0)
        actors.append(h2)
        h2.expect("acquired", 60)
        h2.expect("released", 60)
        finished = w2.expect("finished", 120)
        r.expect("judge", 120, where=lambda e: e["at"] > finished["at"])
    finally:
        codes = {actor.role: actor.stop() for actor in reversed(actors)}
    judged = r.of("judge")
    reaps = [s for s in judged if s.get("reaped")]
    first_reap = reaps[0]["at"] if reaps else None
    early = [s for s in judged if s["at"] < last_beat + pool.LEASE_TIMEOUT_S and s.get("reaped")]
    stamps = sorted(p.name for p in (root / "lease-heartbeats" / key).iterdir())
    summary("dead-owner", codes=codes, killed_after_last_beat_s=killed - last_beat, detected=detected,
            first_reap_after_last_beat_s=None if first_reap is None else first_reap - last_beat,
            reaps=[s["reaped"] for s in reaps], early=early,
            judge_errors=[s for s in judged if any(k.endswith("_error") for k in s)][:5],
            successor_claim_after_last_beat_s=claimed["at"] - last_beat, successor=finished,
            stamp_files=stamps, runs=runs.read_text().splitlines())
    assert detected is True
    assert first_reap is not None and not early
    assert pool.LEASE_TIMEOUT_S < first_reap - last_beat <= pool.LEASE_TIMEOUT_S + 10.0
    assert [s["reaped"] for s in reaps] == [[key]]
    assert claimed["key"] == key
    assert finished["stamped"] >= 1 and not finished["errors"]
    assert len(stamps) == 2  # predecessor stamp is retained (retention is #2232); successor has its own
    assert queue.current_ending(key)["state"] == record_store.DONE
    assert len(runs.read_text().splitlines()) == 2
