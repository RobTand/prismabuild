"""Review probe 1: a LIVE claim behind a key lock held past LEASE_TIMEOUT_S.

Real processes: worker W (claims, heartbeats with stalls at every heartbeat
read/write point), holder H (holds the key's transition lock 345 s and reaps
under its own hold from 305 s), judge R (reaper + mover/order/census judges
every second), claimer W2 (any claim of the key would be a second run).
"""
import threading
import uuid

from prismabuild import claim_identity, pool, record_store
from test_pool_transition_lock import publish
from heartbeat_review_harness_2230 import Actor, summary

HOLD_S = 345.0


def test_live_claim_survives_long_key_lock_with_stalls(tmp_path):
    root = tmp_path / "queue"
    queue = pool.PoolQueue(root)
    key = uuid.uuid4().hex + uuid.uuid4().hex
    publish(queue, key)
    runs = tmp_path / "runs.txt"
    w = Actor("worker", root=str(root), key=key, owner="worker", interval_s=10.0, stall_s=1.5, runs=str(runs))
    actors = [w]
    try:
        w.expect("claimed", 120)
        w.expect("beat", 120)
        w2 = Actor("claimer", root=str(root), owner="worker2", period_s=2.0, runs=str(runs))
        r = Actor("judge", root=str(root), key=key, period_s=1.0)
        actors += [w2, r]
        r.expect("judge", 120)
        h = Actor("holder", root=str(root), key=key, hold_s=HOLD_S, reap_from_s=305.0)
        actors.append(h)
        acquired = h.expect("acquired", 120)
        released = h.expect("released", HOLD_S + 300)
        after = w.expect("beat", 180, where=lambda e: not e["stamped"] and e["at"] > released["at"])
        r.expect("judge", 120, where=lambda e: e["at"] > after["done"])
    finally:
        codes = {actor.role: actor.stop() for actor in reversed(actors)}
    beats_in_hold = [e for e in w.of("beat") if acquired["at"] < e["at"] < released["at"]]
    held = h.of("held_reap")
    judged = r.of("judge")
    in_hold = [s for s in judged if acquired["at"] < s["at"] < released["at"]]
    post = [s for s in judged if s["at"] > released["at"]]
    disagreements = [s for s in judged if s.get("mover_live") is not True or s.get("order_live") is not True
                     or list(s.get("census") or ()) != ["CLAIMED", False]]
    summary("live-hold", codes=codes, hold_s=released["at"] - acquired["at"],
            beats_in_hold=len(beats_in_hold), stamped_in_hold=sum(e["stamped"] for e in beats_in_hold),
            beat_errors=w.of("beat_error"), first_lease_rewrite_after_release_s=after["at"] - released["at"],
            held_reaps=len(held), held_max_raw_lease_age=max(s["raw_lease_age"] for s in held),
            held_max_lease_age=max(s.get("lease_age", -1) for s in held),
            held_errors=[s for s in held if "error" in s],
            held_reaped=[s["reaped"] for s in held if s.get("reaped")],
            judge_samples=len(judged), judge_in_hold=len(in_hold), judge_post=len(post),
            judge_max_raw_lease_age=max(s["raw_lease_age"] or 0 for s in judged),
            judge_max_lease_age=max(s.get("lease_age") or 0 for s in judged),
            judge_reaped=[s["reaped"] for s in judged if s.get("reaped")],
            judge_errors=[s for s in judged if any(k.endswith("_error") for k in s)],
            disagreements=disagreements[:5], disagreement_count=len(disagreements),
            w2_claims=w2.of("claimed"), runs=runs.read_text().splitlines())
    # Never requeued or reaped, never run twice.
    assert not w.of("beat_error")
    assert len(beats_in_hold) >= 15 and all(e["stamped"] for e in beats_in_hold)
    assert held and min(s["raw_lease_age"] for s in held) > pool.LEASE_TIMEOUT_S
    assert all(s.get("reaped") == [] and s.get("lease_age", 1e9) <= pool.LEASE_TIMEOUT_S for s in held)
    assert not [s for s in judged if s.get("reaped")]
    assert not [s for s in judged if any(k.endswith("_error") for k in s)]
    assert max(s["raw_lease_age"] or 0 for s in judged) > pool.LEASE_TIMEOUT_S
    assert max(s.get("lease_age") or 0 for s in judged) <= pool.LEASE_TIMEOUT_S
    assert not w2.of("claimed") and len(runs.read_text().splitlines()) == 1
    live = record_store.read_document(queue.records.item_path(record_store.CLAIMED, key))
    assert live and live["claimed_by"] == "worker" and live["attempts"] == 0
    assert not queue.records.item_path(record_store.READY, key).exists()
    assert not disagreements, disagreements[:5]
