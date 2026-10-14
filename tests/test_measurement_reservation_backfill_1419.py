"""PB #1419: preserve the first bounded measurement drain opportunity.

Reuse the real sealed requests, queue, ledgers and CPU/GPU controllers from
#1185. Only its deterministic clock and sampler observations are controlled.
No admission verdict, withholding history or reservation field is fabricated.
A sealed payload timeout is NOT a verified resource-release bound: even short
candidates remain unknown for this conservative starvation-only checkpoint.
These simulations need neither a GPU nor a running worker process.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import pytest
from test_measurement_drains_gpu_backfill import fleet as fleet_fixture

from prismabuild import adaptive_cpu, adaptive_gpu, pool

fleet = fleet_fixture
TIERS = {"preferred": list(range(20)), "fallback": []}


def _holder_snapshot(queue, key):
    ledger = queue.ledger("sparklina")
    return (queue.item_path(pool.CLAIMED, key).read_bytes(),
            ledger.holder_tokens(key),
            sorted(path.name for path in (ledger.held_dir / key).glob("*-*")))


def _assert_holder_unchanged(queue, key, snapshot):
    assert _holder_snapshot(queue, key) == snapshot
    assert queue.ledger("sparklina").holds_gpu(key)
    assert not queue.item_path(pool.READY, key).exists()


def _bounded_measurement_wait(fleet, *, with_other_host=False):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    incumbent = publish("bounded-incumbent", priority=-10, timeout_s=1200,
                        cpu=2, gpu=1, mem_gb=8)
    assert claim() == incumbent
    snapshot = _holder_snapshot(queue, incumbent)
    bound = queue.holder_bound(incumbent)
    assert bound["bound"] == "transient"
    assert bound["governed_by"] == "deadline"
    assert bound["requested_timeout_s"] == 1200
    # The existing drain policy snapshots this opportunity, not a guarantee
    # that checkout, checkpoint I/O and cleanup release resources by then.
    declared_end = bound["claimed_unix"] + bound["requested_timeout_s"]
    if with_other_host:
        other = publish("other-host-incumbent", priority=-10, timeout_s=4200)
        assert claim("sparky") == other
        assert claim("sparklina") is None

    measurement = publish("priority-zero-measurement", measurement=True,
                          priority=0, cpu=8, gpu=1, mem_gb=48)
    item = pool._read_json(queue.item_path(pool.READY, measurement))
    assert isinstance(item, dict)
    assert adaptive_cpu.action_identity(item)[1] is True
    assert item["resources"] == {"cpu": 8, "gpu": 1, "mem_gb": 48}
    assert item["priority"] == 0
    assert claim() is None
    evidence = denial(measurement)["evidence"]
    decision = evidence["decision"]
    # Current main already contains #1422: this must reach real occupancy,
    # not unknown history or the old off-affinity aggregate-idle refusal.
    assert decision["reason"] == "measurement_holder", decision
    assert decision["holder"] == incumbent
    assert decision["fresh"] is True
    assert decision["measurement_pool_drain"]["foreign_clear"] is True
    assert decision["sample"]["psi_some"] == 0
    assert sample["complete"] is True and sample["attributed"] is True
    assert not sample["foreign_processes"]
    assert sample["cpu_pressure_some"] == 0
    assert evidence["withhold"]["why"] == "draining_for_measurement"
    assert evidence["withhold"]["drain_until_unix"] == pytest.approx(declared_end)
    assert declared_end > clock[0] + pool.WITHHOLD_CEILING_S
    _assert_holder_unchanged(queue, incumbent, snapshot)
    return incumbent, measurement, declared_end, snapshot


def _observe_real_sharing_permission(fleet, candidate):
    """Settle actual GPU observations without reserving or spending a probe.

    Like fleet.claim's incumbent observation, this uses the real controller
    under host admission. Two distinct samples recover after a clock gap;
    no history or decision is replaced. The queue must still admit for itself.
    """
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    item = pool._read_json(queue.item_path(pool.READY, candidate))
    assert isinstance(item, dict)
    gpu = None
    permission = None
    for _ in range(2):
        tick(adaptive_gpu.SETTLE_S + 1)
        cpu = adaptive_cpu.Controller(queue.ledger(), TIERS)
        gpu = adaptive_gpu.Controller(queue.ledger())
        with cpu.locked():
            permission = gpu.decision(item, item["resources"])
    assert gpu is not None
    assert permission is not None and permission["probe"] is True, gpu.last_decision


def test_expiry_cannot_give_a_continuous_lower_priority_stream_the_original_drain_point(fleet):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    incumbent, measurement, original_end, snapshot = _bounded_measurement_wait(fleet)
    lower = []
    # Real arrivals, not rewritten publications: none proves release before
    # the first opportunity. Restart and a genuinely contended transition
    # lock must not move that opportunity to a later holder's lifetime.
    for index in range(3):
        tick(100)
        lower.append(publish(f"long-refill-{index}", priority=-10, timeout_s=4200))
        assert claim() is None
        assert denial(lower[-1])["evidence"]["withheld_for"] == measurement
        _assert_holder_unchanged(queue, incumbent, snapshot)
    restarted = pool.PoolQueue(queue.root)
    with (ThreadPoolExecutor(max_workers=1) as executor,
          queue._transition_locked(measurement) as acquired):
        assert acquired
        assert executor.submit(claim, q=restarted).result(timeout=5) is None
    assert denial(measurement)["reason"] == "transition_busy"
    assert claim(q=restarted) is None
    assert denial(measurement)["evidence"]["decision"]["reason"] == "measurement_holder"
    assert denial(measurement)["evidence"]["withhold"]["drain_until_unix"] == original_end

    # Warm genuine sharing proof just before the point so a RED cannot be
    # masked by a discontinuous sampler or an unauthorized GPU probe.
    tick(original_end - clock[0] - 7)
    _observe_real_sharing_permission(fleet, lower[0])
    tick(original_end - clock[0] + 0.1)
    refills = []
    remaining = incumbent
    measurement_claimed = False
    for index in range(3):
        if index:
            tick(adaptive_gpu.PSI_AVG10_S + 1)
            if remaining is not None:
                _observe_real_sharing_permission(fleet, lower[index])
        before = _holder_snapshot(queue, remaining) if remaining is not None else None
        result = claim(q=pool.PoolQueue(queue.root))
        # A claimant cannot preempt or return the current holder's tokens,
        # even when that holder has just reached its declared end.
        if remaining is not None:
            _assert_holder_unchanged(queue, remaining, before)
        if result == measurement:
            assert remaining is None, "measurement admitted over a real holder"
            measurement_claimed = True
            break
        if result is not None:
            assert result in lower
            waiting = denial(measurement)["evidence"]
            assert waiting["decision"]["reason"] == "measurement_holder", waiting
            refills.append((result, clock[0]))
            assert queue.item_path(pool.READY, measurement).exists()
            assert queue.ledger().holds_gpu(result)
        # The declared-end incumbent, then each replacement, finishes through
        # the real queue. On main each newly claimed replacement stays while
        # the previous one leaves, depriving measurement of the empty box.
        if remaining is not None:
            queue.finish(remaining, status="executed")
        remaining = result
    if not measurement_claimed:
        if remaining is not None:
            queue.finish(remaining, status="executed")
        tick(adaptive_gpu.PSI_AVG10_S + 1)
        assert claim(q=pool.PoolQueue(queue.root)) == measurement, denial(measurement)
    assert queue.ledger().held_keys() == [measurement]
    assert queue.item_path(pool.CLAIMED, measurement).exists()
    assert not queue.item_path(pool.READY, measurement).exists()
    assert not refills, (
        "lower-priority claims stole the original bounded drain point",
        {"original_end": original_end, "claims": refills,
         "lower_keys": lower, "measurement": measurement},
    )


@pytest.mark.parametrize("bound_kind", ["unknown", "short_payload", "late", "long"])
def test_backfill_without_a_finite_pre_point_finish_stays_ready(fleet, bound_kind):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    incumbent, measurement, original_end, snapshot = _bounded_measurement_wait(fleet)
    # Payload-only timeout, including five seconds, does not cover resource
    # preparation, checkpoint I/O or cleanup. Do not invent a finite proof.
    timeout = {"unknown": None, "short_payload": 5,
               "late": original_end - clock[0] + 1, "long": 4200}[bound_kind]
    candidate = publish(f"unsafe-{bound_kind}-backfill", priority=-10, timeout_s=timeout)
    item = pool._read_json(queue.item_path(pool.READY, candidate))
    assert isinstance(item, dict)
    assert pool._declared_run_bound(item) == (("stall" if timeout is None else "deadline"), timeout)
    _observe_real_sharing_permission(fleet, candidate)
    assert claim() is None, f"{bound_kind} backfill consumed the reserved host"
    assert queue.item_path(pool.READY, candidate).exists()
    assert not queue.item_path(pool.CLAIMED, candidate).exists()
    assert queue.ledger().held_keys() == [incumbent]
    _assert_holder_unchanged(queue, incumbent, snapshot)
    assert queue.item_path(pool.READY, measurement).exists()
    tick(original_end - clock[0] + 0.1)
    _observe_real_sharing_permission(fleet, candidate)
    assert claim() is None, f"expiry let {bound_kind} backfill steal the drain point"
    _assert_holder_unchanged(queue, incumbent, snapshot)
    assert queue.ledger().held_keys() == [incumbent]
    queue.finish(incumbent, status="executed")
    tick(adaptive_gpu.PSI_AVG10_S + 1)
    assert claim() == measurement, denial(measurement)
    assert queue.item_path(pool.READY, candidate).exists()


def test_a_portable_measurement_does_not_shut_two_matching_hosts_to_backfill(fleet):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    incumbent, measurement, original_end, snapshot = _bounded_measurement_wait(
        fleet, with_other_host=True)
    other_ledger = queue.ledger("sparky")
    other_holders = other_ledger.held_keys()
    assert len(other_holders) == 1
    other = other_holders[0]
    other_claim = queue.item_path(pool.CLAIMED, other).read_bytes()
    other_tokens = other_ledger.holder_tokens(other)
    item = pool._read_json(queue.item_path(pool.READY, measurement))
    assert isinstance(item, dict)
    assert item["tags"] == []
    # Both hosts match the real measurement; this is NOT the old pinned-row
    # test where placement alone makes the second box irrelevant.
    for host in ("sparklina", "sparky"):
        assert queue._placement_matches(item, tags={"gb10", host}, has_gpu=True)
    candidate = publish("other-matching-host-backfill", priority=-10, timeout_s=None)
    assert claim("sparky") == candidate, "one waiting row shut both matching GPUs"
    assert set(other_ledger.held_keys()) == {other, candidate}
    assert other_ledger.holds_gpu(candidate)
    assert other_ledger.holder_tokens(other) == other_tokens
    assert queue.item_path(pool.CLAIMED, other).read_bytes() == other_claim
    _assert_holder_unchanged(queue, incumbent, snapshot)
    assert queue.item_path(pool.READY, measurement).exists()
    tick(original_end - clock[0])
    queue.finish(incumbent, status="executed")
    tick(adaptive_gpu.PSI_AVG10_S + 1)
    assert claim("sparklina") == measurement, denial(measurement)
    assert queue.ledger("sparklina").held_keys() == [measurement]
    assert set(other_ledger.held_keys()) == {other, candidate}
