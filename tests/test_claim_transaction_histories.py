"""Claim-frame uncertainty and persisted recovery use different authority."""
import multiprocessing
import os

import pytest

from prismabuild import adaptive_cpu, pool, window_credit
from test_adaptive_gpu import gpu_rig
from test_tier_funding import (
    RAM_KIND, RAM_TIER, STAGE_KIND, TIER, _hexkey, _plan, _publish_mover,
    _queue, _reserve_and_transfer,
)


@pytest.mark.parametrize("intent_written", [False, True])
def test_uncertain_error_after_spending_probe_returns_tokens_but_keeps_sample_spent(
        gpu_rig, monkeypatch, intent_written):
    queue, clock, sample, capacity, publish, tick, claim = gpu_rig
    holder = publish(1)
    first = claim()
    assert first is not None
    candidate = publish(2)
    before = queue.ledger().available()
    ready_bytes = queue.item_path(pool.READY, candidate).read_bytes()
    tick()
    original = queue._write_claim_intent
    observed = []

    def fail_intent(key, *, owner):
        assert key == candidate
        state = adaptive_cpu.read_json(
            adaptive_cpu.local_state_base(queue.ledger().base) / "gpu-state.json")
        assert state["consumed_probe_id"]
        assert state["consumed_sample_id"] == sample["sample_id"]
        observed.append(dict(state))
        if intent_written:
            original(key, owner=owner)
        raise OSError("uncertain intent publication")

    with monkeypatch.context() as fault:
        fault.setattr(queue, "_write_claim_intent", fail_intent)
        with pytest.raises(OSError, match="uncertain intent publication"):
            claim()

    assert len(observed) == 1
    assert queue.ledger().available() == before
    assert set(queue.ledger().held_keys()) == {holder}
    assert queue.item_path(pool.READY, candidate).read_bytes() == ready_bytes
    assert not queue.item_path(pool.CLAIMED, candidate).exists()
    assert pool.read_queue_record(queue.item_path(pool.CLAIMED, holder)) == first
    assert claim() is None, "an uncertain claim refunded a spent GPU sample"
    tick()
    resumed = claim()
    assert resumed is not None and resumed["action_key"] == candidate
    queue.finish(candidate, status="executed", claim_snapshot=resumed)
    queue.finish(holder, status="executed", claim_snapshot=first)


@pytest.mark.parametrize("lease_written", [False, True],
                         ids=["after-claim-record", "after-lease-before-mark"])
def test_process_death_before_consumed_mark_recovers_from_persisted_claim(
        tmp_path, monkeypatch, lease_written):
    queue = _queue(tmp_path, stage_gib=4, ram_gib=4)
    mover, consumer = _hexkey("crash-mover"), _hexkey("crash-consumer")
    plan = _plan(queue, consumer, mover, tag="crash")
    row = _publish_mover(queue, plan, mover, resources={
        "cpu": 1, "mem_gb": 1, STAGE_KIND: 2, RAM_KIND: 2})
    grant = window_credit.grant_key(consumer, TIER, "mover_row", "phase-crash")
    generation = _reserve_and_transfer(
        queue, plan, mover, row, kind="stage_gib", gib=2, grant=grant)
    stage = queue.tier_ledger(TIER)
    ram = queue.tier_ledger(RAM_TIER)
    context = multiprocessing.get_context("fork")
    parent, child = context.Pipe()

    def claimant():
        parent.close()
        original = pool.PoolQueue.write_lease

        def stop_before_mark(self, key, **kwargs):
            assert key == mover
            if lease_written:
                original(self, key, **kwargs)
            child.send("persisted")
            assert child.recv() == "exit"
            os._exit(73)

        pool.PoolQueue.write_lease = stop_before_mark
        queue.claim(tags=["dl380g10"], owner="crashing-claimant")
        os._exit(74)

    process = context.Process(target=claimant)
    process.start()
    child.close()
    try:
        assert parent.poll(10), "claimant never reached the real persistence boundary"
        assert parent.recv() == "persisted"
        persisted = pool.read_queue_record(queue.item_path(pool.CLAIMED, mover))
        assert persisted["attempts"] == 0
        assert persisted["published_unix"] == row["published_unix"]
        assert persisted["tier_funding"][TIER]["generation"] == generation
        assert queue.lease_path(mover).exists() is lease_written
        assert queue.read_funding(mover, TIER)["state"] == "transferring"
        assert stage.holder_tokens(mover) == {"stage_gib": 2}
        assert ram.holder_tokens(mover) == {"ram_gib": 2}
        assert queue.ledger().holder_tokens(mover) == {"cpu": 1, "mem_gb": 1}
        assert not queue.attempt_path(persisted, 1).exists()
        parent.send("exit")
        process.join(10)
        assert process.exitcode == 73
    finally:
        parent.close()
        if process.is_alive():
            process.terminate()
            process.join(10)

    later = float(persisted["claimed_unix"]) + pool.LEASE_TIMEOUT_S * 2
    monkeypatch.setattr(pool, "_now", lambda: later)
    assert queue.reap_stale(timeout_s=1) == [mover]
    restored = pool.read_queue_record(queue.item_path(pool.READY, mover))
    assert restored["published_unix"] == row["published_unix"]
    assert restored["attempts"] == int(lease_written)
    assert "claimed_by" not in restored and "tier_funding" not in restored
    assert not queue.item_path(pool.CLAIMED, mover).exists()
    assert not queue.lease_path(mover).exists()
    assert stage.holder_tokens(mover) == {}
    assert ram.holder_tokens(mover) == {}
    assert stage.available()["stage_gib"] == 4
    assert ram.available()["ram_gib"] == 4
    assert queue.ledger().holder_tokens(mover) == {}
    assert queue.read_funding(mover, TIER)["state"] == "transferring"
    assert queue.funded_cover(TIER, restored, "stage_gib", 2) == (0, None)
    assert queue.attempt_path(persisted, 1).exists() is lease_written
