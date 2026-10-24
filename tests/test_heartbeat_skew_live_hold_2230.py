"""Real +2 s owner clock; stamp replacement wakes a reaper under a 345 s hold."""
import uuid

from prismabuild import pool, record_store
from test_pool_transition_lock import publish
from heartbeat_review_harness_2230 import Actor, summary


def test_live_owner_with_two_second_clock_lead_survives_long_key_lock(tmp_path):
    root = tmp_path / "queue"
    queue = pool.PoolQueue(root)
    key = uuid.uuid4().hex + uuid.uuid4().hex
    publish(queue, key)
    runs = tmp_path / "runs.txt"
    worker = Actor("worker", root=str(root), key=key, owner="worker", interval_s=10.,
                   clock_offset_s=2., runs=str(runs))
    actors = [worker]
    try:
        worker.expect("claimed", 120)
        worker.expect("beat", 120)
        successor = Actor("claimer", root=str(root), owner="worker2", period_s=1., runs=str(runs))
        actors.append(successor)
        holder = Actor("skew-holder", root=str(root), key=key, hold_s=345.)
        actors.append(holder)
        holder.expect("acquired", 120)
        released = holder.expect("released", 525)
        if released["reaps"]:
            successor.expect("claimed", 120)
        else:
            worker.expect("beat", 120, where=lambda e: not e["stamped"] and e["at"] > released["at"])
    finally:
        codes = {actor.role: actor.stop() for actor in reversed(actors)}
    samples = holder.of("skew_sample")
    within = [sample for sample in samples if sample["inside_skew"]]
    late = [sample for sample in within if sample["late"]]
    disagreements = [sample for sample in within if not all(sample[field] is True for field in
                      ("lease_live", "mover_live", "order_live", "census_live"))]
    proof = dict(clock_offset_s=2., hold_s=released["hold_s"], samples=len(samples),
                 inside_skew=len(within), late_samples=len(late), reaps=released["reaps"],
                 second_claims=len(successor.of("claimed")), disagreements=len(disagreements),
                 beat_errors=len(worker.of("beat_error")), runs=len(runs.read_text().splitlines()), codes=codes)
    summary("S1-skew-live-hold", **proof)
    assert late, "probe missed the expired-lease/future-stamp window"
    assert proof["reaps"] == proof["second_claims"] == proof["disagreements"] == 0
    assert len(late) >= 2
    assert proof["beat_errors"] == 0 and proof["runs"] == 1
    assert set(codes.values()) == {0}
    claim = record_store.read_document(queue.records.item_path(record_store.CLAIMED, key))
    assert claim and claim["claimed_by"] == "worker"
    assert not queue.records.item_path(record_store.READY, key).exists()
