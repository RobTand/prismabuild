"""Literal, fresh and idle shared-memory GPU telemetry for admission fixtures."""
from prismabuild import pool


def idle_gpu_sample():
    return {
        "schema": "prismabuild.gpu_capacity.v1", "sample_id": str(pool._now()),
        "sampled_unix": pool._now(), "complete": True, "attributed": True,
        "devices": [{"uuid": "GPU-1", "name": "NVIDIA GB10", "power_w": 15.0,
                     "power_limit_w": None, "power_reference_w": 140.0,
                     "power_reference_scope": "soc_tdp", "memory_domain": "shared_system",
                     "limited": False}],
        "host_total_bytes": 128 * (1 << 30), "host_available_bytes": 120 * (1 << 30),
        "memory_pressure_some": 0.0, "memory_pressure_full": 0.0,
        "cpu_pressure_some": 0.0, "foreign_processes": [], "jobs": [],
    }
