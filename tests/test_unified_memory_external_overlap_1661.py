"""A published sample does not prove the union of foreign CPU and GPU pages."""
from test_measurement_drains_gpu_backfill import fleet

CAPACITY = {"cpu": 20, "gpu": 1, "mem_gb": 104}


def test_unproven_foreign_overlap_cannot_authorize_a_new_commitment(fleet):
    queue, _clock, _readings, sample, publish, _tick, *_rest = fleet
    ledger = queue.ledger()
    ledger.ensure_capacity(CAPACITY)
    assert ledger.acquire("a" * 64, {"mem_gb": 40})
    sample.update(host_total_bytes=128 << 30, host_available_bytes=96 << 30,
                  cpu_pressure_full=0.0, jobs=[])
    sample["foreign_processes"] = [
        {"pid": 9101, "gpu_uuid": "GPU-1", "used_bytes": 32 << 30}]
    key = publish("unproven-external-union", gpu=0, mem_gb=40, tags=["gb10"])

    # The same public counters permit 32 GiB of visible foreign CPU pages
    # beside 32 GiB of driver-held GPU pages invisible to MemAvailable.
    # That world leaves 56 GiB for PB, including the unresident 40-GiB hold.
    claimed = queue.claim(
        capacity=CAPACITY, has_gpu=True, tags=["gb10", "sparklina"],
        gpu_sampler=lambda: sample)

    assert claimed is None, (
        "the sample cannot exclude a 64-GiB foreign union", key,
        ledger.held().get("mem_gb"))
    assert ledger.held()["mem_gb"] == 40
