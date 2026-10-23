"""GB10's GPU budget is a subset of, not an addition to, host memory."""
from pathlib import Path
import sys
import time

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import adaptive_gpu, core, decomposition as dc, pool
from prismabuild.resource_scope import ResourceScope

from test_adaptive_gpu import _cpu_sample
from test_core import _body
from test_decomposition_refuses_before_it_publishes_anything import (
    _common,
    _frozen,
    _request,
)


def test_gb10_gpu_capacity_2087(tmp_path, monkeypatch):
    clock = [100.0]
    monkeypatch.setattr(time, "time", lambda: clock[0])
    gib = adaptive_gpu.GIB
    capacity = {"cpu": 8, "mem_gb": 104, "gpu": 1}

    # Every row starts with an empty 104-GiB offer. Separate VRAM must not be
    # constrained by the host-memory subset rule that applies to GB10.
    cases = [
        ("shared_system", 104, 100),
        ("shared_system", 80, 64),
        ("shared_system", 104, None),
        ("shared_system", 104, 104),
        ("discrete", 64, 104),
    ]
    for index, (domain, memory_gb, gpu_memory_gb) in enumerate(cases):
        case_dir = tmp_path / str(index)
        case_dir.mkdir()
        demand = {"cpu": 1, "mem_gb": memory_gb, "gpu": 1}
        body = _body(case_dir)
        body["params"] = {"demand": demand, "gpu_exclusive": False}
        if gpu_memory_gb is not None:
            body["params"]["gpu_memory_gb"] = gpu_memory_gb
        action = core.seal_action(body)
        cas = core.PrismaBuildCAS(case_dir / "cas")
        cas.publish_action_request(action)
        queue = pool.PoolQueue(case_dir / "queue")
        queue.publish(
            action_key=action["action_key"], cas_root=str(cas.root),
            checkout_root=str(case_dir), worker_script="worker.py",
            resources=demand, needs_gpu=True,
        )
        assert not queue.ledger().held_keys()

        device = {
            "uuid": "GPU-1",
            "name": "NVIDIA GB10" if domain == "shared_system" else "Discrete GPU",
            "power_w": 15.0, "power_limit_w": None,
            "power_reference_w": 140.0, "power_reference_scope": "soc_tdp",
            "memory_domain": domain, "limited": False,
        }
        if domain == "discrete":
            device.update(power_limit_w=200.0, memory_total_bytes=128 * gib,
                          memory_free_bytes=120 * gib,
                          memory_used_bytes=8 * gib)
        sample = {
            "schema": "prismabuild.gpu_capacity.v1", "sample_id": str(index),
            "sampled_unix": clock[0], "complete": True, "attributed": True,
            "devices": [device],
            "host_total_bytes": 128 * gib, "host_available_bytes": 120 * gib,
            "memory_pressure_some": 0.0, "memory_pressure_full": 0.0,
            "cpu_pressure_some": 0.0, "foreign_processes": [], "jobs": [],
        }
        claimed = queue.claim(
            capacity=capacity,
            cpu_tiers={"preferred": list(range(8)), "fallback": []},
            adaptive_cpu=True, has_gpu=True,
            cpu_sampler=lambda: _cpu_sample(clock),
            gpu_sampler=lambda: sample,
        )
        assert claimed is not None, (domain, memory_gb, gpu_memory_gb)
        assert claimed["action_key"] == action["action_key"]
        budget_bytes = (memory_gb if gpu_memory_gb is None else gpu_memory_gb) * gib
        admission = claimed["gpu_admission"]
        assert admission["memory_domain"] == domain
        assert admission["gpu_memory_budget_bytes"] == budget_bytes
        assert admission["exclusive"] is False
        assert admission["probe"] is False

        # Inspect real tokens, not just claim metadata: shared GPU memory is
        # already inside mem_gb, and discrete VRAM does not mint host tokens.
        ledger = queue.ledger()
        assert ledger.capacity() == capacity
        assert len(ledger.held_keys()) == 1
        assert ledger.held()["mem_gb"] == memory_gb
        assert ledger.held()["gpu"] == 1
        assert ledger.available().get("mem_gb", 0) == 104 - memory_gb
        assert ledger.available().get("gpu", 0) == 0

        # Constructor only: never enter a scope or contact a real GPU/cgroup.
        scope_kwargs = {}
        if gpu_memory_gb is not None:
            scope_kwargs["gpu_memory_max_bytes"] = gpu_memory_gb * gib
        scope = ResourceScope(
            action["action_key"], "a" * 32, memory_gb * gib,
            case_dir / "telemetry.json", **scope_kwargs,
        )
        assert scope.memory_max_bytes == memory_gb * gib
        assert scope.gpu_memory_max_bytes == budget_bytes

    # Keep this refusal last: the successful accounting checks above are part
    # of the same acceptance check. _frozen calls dc.freeze_common, so it must
    # also be inside raises if validation moves to the sealing boundary.
    invalid_demand = {"cpu": 1, "mem_gb": 104, "gpu": 1}
    request = _request(common=_common(
        demand=invalid_demand, gpu_memory_gb=105, host_class="gb10",
    ))
    with pytest.raises(dc.ActionContractError):
        frozen = _frozen(
            request, demand=invalid_demand,
            placement={"required_tags": ["gb10"]},
        )
        dc.build_plan(request, frozen)
