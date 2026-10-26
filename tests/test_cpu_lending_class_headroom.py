"""Opted claims preserve READY GPU room without writing legacy CPU history."""
import json

import pytest

from prismabuild import adaptive_cpu, adaptive_gpu, pool
from gpu_capacity_fixture import idle_gpu_sample
from test_class_scoped_cpu_admission import (
    class_claim_host,
    _class_denial,
    _class_gpu_lock_busy,
    _class_holder,
    _publish_class_action,
)


@pytest.mark.parametrize("funded", [False, True])
@pytest.mark.parametrize("busy, fits", [(0.0, True), (6.0, False)])
def test_opted_class_claim_preserves_gpu_headroom_and_legacy_history(
    class_claim_host, monkeypatch, funded, busy, fits,
):
    host = class_claim_host
    holder = _class_holder(host, slots=2 if funded else None)
    candidate = _publish_class_action(
        host, "candidate", {"cpu": 1, "mem_gb": 1}, owner=holder if funded else None)
    path = host.queue.records.item_path(pool.READY, candidate)
    row = json.loads(path.read_bytes())
    row.update(cpu_qos="opportunistic", priority=-10)
    path.write_text(json.dumps(row))
    gpu = _publish_class_action(host, "gpu", {"cpu": 4, "gpu": 1, "mem_gb": 16})
    base = adaptive_cpu.local_state_base(host.queue.ledger().base)
    (base / "jobs-opportunistic.json").write_bytes((base / "jobs.json").read_bytes())
    history = {name: (base / name).read_bytes() for name in ("jobs.json", "profiles.json")}
    host.busy = busy
    monkeypatch.setattr(adaptive_cpu.Controller, "sample", lambda self, **kwargs: {
        "sampled_unix": host.now, "cpu_count": 8, "interval_s": 1.0,
        "busy_cpus": host.busy, "psi_some": 0.0})
    monkeypatch.setattr(adaptive_gpu.Controller, "sample", lambda self: idle_gpu_sample())
    with _class_gpu_lock_busy(host, gpu):
        claimed = host.queue.claim(
            tags=["gb10", "arm64", "sparky"], has_gpu=True,
            capacity=host.capacity, cpu_tiers=host.tiers,
            adaptive_cpu=True, cpu_lending_ramp_cap=2)
    assert bool(claimed) is fits, _class_denial(host, candidate)
    if fits:
        assert claimed["action_key"] == candidate
    else:
        assert _class_denial(host, candidate)["reason"] == "deferred_for_ready_gpu_row"
        assert not host.queue.ledger().holder_tokens(candidate)
    assert {name: (base / name).read_bytes() for name in history} == history
