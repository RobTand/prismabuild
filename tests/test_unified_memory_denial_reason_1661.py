"""Unknown unified residency stays visible after worker token retirement (#1661)."""
from __future__ import annotations

import pytest

from test_gb10_unified_gpu_cap_admission_1661 import (
    CAPACITY, _denial, _sealed,
    adaptive_gpu, box_capacity, pool, real_gpu_contract, rig,
)
from prismabuild import census
import pbstatus


def retired_unified_offer(rig, observer):
    queue, now, sample, _args = rig
    sample["cpu_pressure_full"] = 0.0
    capacity = observer.offer(CAPACITY, queue.ledger().held,
                              gpu_sample=sample, mem_gb=120, load1=0, now=now[0])
    # The production worker does this before serve_once/claim.
    queue.ledger().retire_free_capacity(capacity)
    return capacity


def claim_observed_unified_offer(rig, capacity, observer):
    queue, _now, _sample, args = rig
    return queue.claim(**dict(args, capacity=capacity,
                              observed_external_gib=observer.last_offer_external_gib,
                              observed_unified_memory_unknown=bool(
                                  observer.last.detail.get("external_unified_gpu_unknown"))))


@pytest.mark.parametrize("warm", [False, True])
@pytest.mark.parametrize("gpu", [False, True])
def test_unknown_is_named_in_claim_denials_and_pbstatus(
        rig, tmp_path, monkeypatch, warm, gpu):
    queue, now, sample, _args = rig
    monkeypatch.setattr(adaptive_gpu, "trusted_sample", lambda: sample)
    observer = box_capacity.CapacityObserver(samples=3)
    if warm:
        initial = retired_unified_offer(rig, observer)
        queue.ledger().ensure_capacity(initial)
    sample["foreign_processes"] = [{"gpu_uuid": "GPU-1", "used_bytes": None}]
    capacity = retired_unified_offer(rig, observer)
    key, cas_root, checkout = _sealed(tmp_path, f"visible-{int(warm)}-{int(gpu)}")
    queue.publish(action_key=key, cas_root=cas_root, checkout_root=checkout,
                  worker_script="/w.py", resources={"cpu": 2, "mem_gb": 4,
                                                       **({"gpu": 1} if gpu else {})},
                  needs_gpu=gpu, priority=-10, tags=["gb10"])
    assert claim_observed_unified_offer(rig, capacity, observer) is None
    assert queue.records.item_path(pool.READY, key).is_file()
    denial = _denial(queue, key)
    assert denial["reason"] == "unified_gpu_memory_unknown"
    assert queue.ledger().held().get("mem_gb", 0) == 0
    row = pool.record_store.read_document(queue.records.item_path(pool.READY, key))
    projected = census._claim_denials_for(row, [denial], now=now[0])
    lines = pbstatus.pool_job_lines([
        {"action_key_prefix": key[:12], "state": "ready", "admission_denials": projected}
    ], {"empty": False})
    assert "unified_gpu_memory_unknown" in "\n".join(lines), (
        "unknown residency has no named pbstatus reason", denial["reason"],
        projected[0]["decision_reason"])


@pytest.mark.parametrize("unknown", [False, True])
def test_worker_passes_the_current_unknown_cause_without_memory_credit(
        tmp_path, monkeypatch, unknown):
    from test_worker_loop_offers_what_is_free import BASE, _run, sample

    seen = []

    def capture_claim(self, **kwargs):
        seen.append(kwargs)
        return None

    monkeypatch.setattr(pool.PoolQueue, "serve_once", capture_claim)
    telemetry = sample(foreign=[{"gpu_uuid": "GPU-one", "used_bytes": None}]
                       if unknown else [])
    _run(tmp_path, [*BASE, "--once"], gpu_sample=telemetry)
    assert len(seen) == 1
    assert seen[0]["observed_unified_memory_unknown"] is unknown
    assert (seen[0]["capacity"]["mem_gb"] == 0) is unknown
