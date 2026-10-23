"""A successful exact-generation claim consumes pre-claim election authority.

Real sealed publications, admission, ledgers and durable claims are exercised;
only clocks/samplers and explicitly named interruption windows are controlled.
"""
from __future__ import annotations

import json

import pytest

from test_measurement_drains_gpu_backfill import fleet as fleet_fixture
from test_measurement_reservation_backfill_1419 import (
    _assert_holder_unchanged, _bounded_measurement_wait,
)
from prismabuild import _measurement_reservation as reservation, adaptive_gpu, pool

fleet = fleet_fixture


def _selection(queue, key):
    path = queue.records.passes_path(key)
    return reservation.selection(pool.record_store.read_document(path)) if path.exists() else None


def _assert_consumed(queue, key):
    path = queue.records.passes_path(key)
    if path.exists():
        assert reservation.FIELD not in json.loads(path.read_bytes()), (
            "durable successful claim must remove the stored pre-claim election field"
        )
    assert _selection(queue, key) is None


@pytest.mark.parametrize("host", ["sparklina", "sparky"], ids=["same-host", "other-host"])
def test_successful_claim_consumes_election_without_releasing_any_claim_tokens(fleet, host):
    queue, _, _, _, publish, tick, claim, _ = fleet
    incumbent, key, _, snapshot = _bounded_measurement_wait(fleet)
    elected = _selection(queue, key)
    if host == "sparklina":
        queue.finish(incumbent, status="executed")
        tick(adaptive_gpu.PSI_AVG10_S + 1)
    assert claim(host) == key
    row = pool.record_store.value_or_raise(queue.records.item(pool.record_store.CLAIMED, key))
    assert row["claimed_host"] == host
    assert queue.records.attempt_generation(row) == elected["generation"]
    assert queue.records.lease_path(key).exists()
    ledger = queue.ledger(host)
    tokens = ledger.holder_tokens(key)
    assert tokens and ledger.holds_gpu(key)
    claim_bytes = queue.records.item_path(pool.record_store.CLAIMED, key).read_bytes()

    if host == "sparky":
        _assert_holder_unchanged(queue, incumbent, snapshot)
        # Let the old incumbent end normally: consumption must reopen its old
        # host without touching the measurement's authority on the other host.
        queue.finish(incumbent, status="executed")
        lower = publish("lower-after-other-host-claim", priority=-10)
        tick(adaptive_gpu.PSI_AVG10_S + 1)
        assert claim("sparklina", q=pool.PoolQueue(queue.root)) == lower, (
            "old elected host still defers lower work after measurement claimed elsewhere"
        )
    _assert_consumed(queue, key)
    assert ledger.holder_tokens(key) == tokens
    assert queue.records.item_path(pool.record_store.CLAIMED, key).read_bytes() == claim_bytes


@pytest.mark.parametrize("fault", ["refused", "missing-ready", "claim-write", "lease-write"])
def test_uncommitted_claim_keeps_election_until_a_real_success(fleet, monkeypatch, fault):
    queue, _, _, _, _, tick, claim, _ = fleet
    incumbent, key, _, snapshot = _bounded_measurement_wait(fleet)
    elected = _selection(queue, key)
    path = queue.records.passes_path(key)
    if fault == "refused":
        assert claim() is None
        _assert_holder_unchanged(queue, incumbent, snapshot)
    elif fault == "missing-ready":
        ready_path = queue.records.item_path(pool.record_store.READY, key)
        ready = ready_path.read_bytes()
        ready_path.unlink()
        assert claim(q=pool.PoolQueue(queue.root)) is None
        ready_path.write_bytes(ready)
    else:
        queue.finish(incumbent, status="executed")
        tick(adaptive_gpu.PSI_AVG10_S + 1)
        with monkeypatch.context() as patch:
            if fault == "lease-write":
                def failed_lease(*args, **kwargs):
                    raise OSError("#2181 injected lease write failure")
                patch.setattr(queue, "write_lease", failed_lease)
            else:
                original = queue.records.replace_record_at

                def failed_claim(dst, record, *args, **kwargs):
                    if dst == queue.records.item_path(pool.record_store.CLAIMED, key):
                        raise OSError("#2181 injected claim write failure")
                    return original(dst, record, *args, **kwargs)
                patch.setattr(queue.records, "replace_record_at", failed_claim)
            assert claim() is None
        assert queue.records.item_path(pool.record_store.READY, key).exists()
        assert not queue.ledger("sparklina").holder_tokens(key)
    assert path.exists() and _selection(queue, key) == elected
    if fault in ("refused", "missing-ready"):
        queue.finish(incumbent, status="executed")
    tick(adaptive_gpu.PSI_AVG10_S + 1)
    assert claim(q=pool.PoolQueue(queue.root)) == key
    _assert_consumed(queue, key)


def test_restart_consumes_interrupted_claim_election_without_deleting_live_pass(fleet):
    queue, _, _, _, publish, tick, claim, _ = fleet
    incumbent, key, _, snapshot = _bounded_measurement_wait(fleet)
    path = queue.records.passes_path(key)
    sidecar = path.read_bytes()
    assert claim("sparky") == key
    _assert_holder_unchanged(queue, incumbent, snapshot)
    claim_path = queue.records.item_path(pool.record_store.CLAIMED, key)
    lease_path = queue.records.lease_path(key)
    authority = (claim_path.read_bytes(), lease_path.read_bytes(),
                 queue.ledger("sparky").holder_tokens(key))
    # Persisted state of a crash after claim commit but before retirement.
    # An unrelated pass counter update must not disappear with the field.
    legacy = json.loads(sidecar)
    legacy["passes"] = int(legacy.get("passes", 0)) + 7
    queue.records.replace_record_at(path, legacy)
    restarted = pool.PoolQueue(queue.root)
    restarted.sweep_orphan_passes(limit=1)
    assert path.exists(), "claim recovery consumes a field, not a live pass"
    _assert_consumed(restarted, key)
    remaining = json.loads(path.read_bytes())
    assert remaining == {name: value for name, value in legacy.items() if name != reservation.FIELD}
    assert (claim_path.read_bytes(), lease_path.read_bytes(),
            queue.ledger("sparky").holder_tokens(key)) == authority
    queue.finish(incumbent, status="executed")
    lower = publish("after-recovered-cross-host-claim", priority=-10)
    tick(adaptive_gpu.PSI_AVG10_S + 1)
    assert claim("sparklina", q=pool.PoolQueue(queue.root)) == lower


def test_claim_retirement_revalidates_selection_and_preserves_successor_updates(fleet, monkeypatch):
    queue, _, _, _, _, tick, claim, _ = fleet
    incumbent, key, _, _ = _bounded_measurement_wait(fleet)
    path = queue.records.passes_path(key)
    prior = json.loads(path.read_bytes())
    queue.finish(incumbent, status="executed")
    tick(adaptive_gpu.PSI_AVG10_S + 1)
    successor = dict(prior)
    next_election = dict(prior[reservation.FIELD])
    next_election["published_unix"] += 1
    next_election["generation"] = queue.records.attempt_generation(next_election)
    successor[reservation.FIELD] = next_election
    successor["passes"] = int(prior.get("passes", 0)) + 9
    original = queue.write_lease

    def publish_successor_after_commit(*args, **kwargs):
        result = original(*args, **kwargs)
        # Delayed observation of same-key replacement at the mutation seam.
        # No production locking/retirement helper is replaced by this test.
        queue.records.replace_record_at(path, successor)
        return result

    monkeypatch.setattr(queue, "write_lease", publish_successor_after_commit)
    assert claim() == key
    assert json.loads(path.read_bytes()) == successor
    # The old claim cannot consume the replacement. After restoring the exact
    # old selection (crash recovery), it must be consumed, leaving counters.
    queue.records.replace_record_at(path, prior)
    pool.PoolQueue(queue.root).sweep_orphan_passes(limit=1)
    _assert_consumed(queue, key)
