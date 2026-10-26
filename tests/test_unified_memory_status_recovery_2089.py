"""Current status must not present expired or recovered unknown GPU evidence."""
from __future__ import annotations

import pytest

from prismabuild import adaptive_cpu, adaptive_gpu, box_capacity, census, pool
from test_gang_reservation_1517 import CAPACITY
from test_measurement_drains_gpu_backfill import fleet
from test_unified_memory_commitment_fence_2089 import direct_claim, unknown
import pbstatus


@pytest.fixture(autouse=True)
def publish_census_evidence_synchronously(monkeypatch):
    monkeypatch.setattr(adaptive_cpu.adaptive_snapshot, "publish",
                        adaptive_cpu.adaptive_snapshot.copy_snapshot)


def announce_sample(fleet, sample):
    queue, clock, *_ = fleet
    seen = box_capacity.observe(CAPACITY, queue.ledger().held, gpu_sample=sample,
                                mem_gb=120, load1=0, now=clock.now)
    queue.announce(host="sparklina", tags=["gb10", "sparklina"], has_gpu=True,
                   capacity=CAPACITY, observed_capacity=seen.capacity,
                   foreign=seen.foreign, observed_detail=seen.detail)
    return seen


def rendered_row(queue, key):
    value = census.action_rows(queue.root).value
    row = next(row for row in value["jobs"] if row["action_key"] == key)
    output = "\n".join(pbstatus.pool_job_lines([row], value["queue"]))
    return row, output


@pytest.mark.parametrize("gpu", [False, True], ids=["cpu", "gpu"])
@pytest.mark.parametrize("recovery", ["fresh_clear_offer", "expired_sample"])
def test_ready_status_retires_unknown_without_claiming_the_row(fleet, monkeypatch, gpu, recovery):
    queue, clock, _readings, sample, publish, tick, _claim, denial = fleet
    unknown(sample)
    key = publish("unknown-status", gpu=int(gpu), mem_gb=4, tags=["gb10"])
    assert direct_claim(fleet) is None
    original = queue.records.item_path(pool.READY, key).read_bytes()
    refused = denial(key)
    assert refused["reason"] == "unified_gpu_memory_unknown"
    seen = announce_sample(fleet, sample)
    diagnostic = adaptive_cpu.local_state_base(queue.ledger().base) / pool.CLAIM_DENIALS
    history = diagnostic.read_bytes()

    def broker_read_is_not_status_evidence(*_args, **_kwargs):
        pytest.fail("status attempted a new trusted GPU broker read")

    monkeypatch.setattr(adaptive_gpu, "trusted_sample", broker_read_is_not_status_evidence)
    row, output = rendered_row(queue, key)
    assert "unified_gpu_memory_unknown" in output
    decision = row["admission_denials"][0]["evidence"]["decision"]
    assert decision["sample_id"] == sample["sample_id"]
    assert decision["sampled_unix"] == sample["sampled_unix"]
    if recovery == "fresh_clear_offer":
        sample["foreign_processes"] = []
        tick(1.0)
        announce_sample(fleet, sample)
    else:
        clock.now += box_capacity.GPU_SAMPLE_MAX_AGE_S + 0.01
        queue.announce(host="sparklina", tags=["gb10", "sparklina"], has_gpu=True,
                       capacity=CAPACITY, observed_capacity=seen.capacity,
                       foreign=seen.foreign, observed_detail=seen.detail)

    recovered, output = rendered_row(queue, key)

    assert "unified_gpu_memory_unknown" not in output
    assert recovered["admission_denials"] == []
    assert queue.records.item_path(pool.READY, key).read_bytes() == original
    assert diagnostic.read_bytes() == history


def test_older_clear_offer_does_not_hide_a_fresh_unknown_denial(fleet):
    queue, clock, _readings, sample, publish, *_ = fleet
    unknown(sample)
    key = publish("current-unknown-status", gpu=0, mem_gb=4, tags=["gb10"])
    assert direct_claim(fleet) is None
    older = {**sample, "sample_id": "older-clear", "sampled_unix": clock.now - 1,
             "foreign_processes": []}
    announce_sample(fleet, older)

    row, output = rendered_row(queue, key)

    assert "unified_gpu_memory_unknown" in output
    assert row["admission_denials"][0]["evidence"]["decision"]["sample_id"] == sample["sample_id"]
