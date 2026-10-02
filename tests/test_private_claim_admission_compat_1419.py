"""Private callers keep the real admission boundary and shared observation."""
from pathlib import Path
import time

import pytest

from prismabuild import pool
import worker_loop
import bench_claim_pass
import bench_tier_cycle_r13 as cycle
import test_prepaid_writer_integration as funding

_isolated_synthetic_launch_context = funding._isolated_synthetic_launch_context
ROOT = Path(__file__).resolve().parents[1]
TIERS = {"preferred": [0, 1], "fallback": []}
CAPACITY = {"cpu": 2, "mem_gb": 4}


def test_private_parameters_observe_the_actual_ledger_and_keep_admission(tmp_path, monkeypatch):
    queue = pool.PoolQueue(tmp_path / "queue")
    seen = []
    monkeypatch.setattr(worker_loop.cpu_topology, "inherited_tiers", lambda: TIERS)
    monkeypatch.setattr(worker_loop, "declared_host_capacity", lambda args, cores: CAPACITY)
    class Observer:
        def __init__(self, *, samples, ledger_total):
            assert samples > 0 and ledger_total == {}
        def offer(self, declared, held, **overrides):
            assert callable(held) and held() == queue.ledger().held()
            assert declared == {"gpu": 0, **CAPACITY}
            seen.append(overrides)
            return {"cpu": 2, "mem_gb": 3}
    monkeypatch.setattr(worker_loop.box_capacity, "CapacityObserver", Observer)
    parameters = worker_loop.private_claim_parameters(queue)
    assert parameters == {"capacity": {"cpu": 2, "mem_gb": 3},
                          "cpu_tiers": TIERS, "adaptive_cpu": True}
    assert len(seen) == 1 and seen[0]["gpu_sample"] is None
    monkeypatch.setattr(pool.cpu_admission.Controller, "sample", lambda self: {
        "sampled_unix": time.time(), "busy_cpus": 0., "psi_some": 0.,
        "cpu_count": 2, "interval_s": 1.})
    key = "a" * 64
    queue.publish(action_key=key, cas_root=queue.root / "cas",
                  checkout_root=queue.root / "co", worker_script=queue.root / "worker.py",
                  resources={"cpu": 1, "mem_gb": 1})
    assert queue.claim(**parameters)["action_key"] == key
    assert queue.ledger().holder_tokens(key) == {"cpu": 1, "mem_gb": 1}
    queue.finish(key, status="executed")
    assert queue.ledger().held_keys() == []


def test_private_parameters_refuse_unknown_inherited_topology(tmp_path, monkeypatch):
    queue = pool.PoolQueue(tmp_path / "queue")
    monkeypatch.setattr(worker_loop.cpu_topology, "inherited_tiers", lambda: None)
    with pytest.raises(pool.PoolContractError, match="known inherited CPU topology"):
        worker_loop.private_claim_parameters(queue)
    assert queue.ledger().held_keys() == []


def test_private_poll_caller_supplies_observation_to_both_real_passes(tmp_path, monkeypatch, capsys):
    seen = []
    original = pool.PoolQueue.claim
    def claim(self, *args, **kwargs):
        seen.append(dict(kwargs))
        return original(self, *args, **kwargs)
    monkeypatch.setattr(pool.PoolQueue, "claim", claim)
    monkeypatch.setattr(worker_loop, "private_claim_parameters", lambda queue: {
        "capacity": CAPACITY, "cpu_tiers": TIERS, "adaptive_cpu": False})
    bench_claim_pass.build_and_poll(ROOT, tmp_path, ready=1, claimed=0, passes=0)
    assert len(seen) == 2 and all(row["capacity"] == CAPACITY for row in seen)
    assert all(row["cpu_tiers"] == TIERS for row in seen)
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    assert len(queue.ready_items()) == 1 and not queue.ledger().held_keys()
    assert '"claimed": null' in capsys.readouterr().out


def test_private_write_only_owner_claims_and_holds_real_host_tokens(tmp_path, monkeypatch):
    queue = pool.PoolQueue(tmp_path / "queue")
    monkeypatch.setattr(worker_loop, "private_claim_parameters", lambda queue: {
        "capacity": CAPACITY, "cpu_tiers": TIERS, "adaptive_cpu": False})
    template = cycle._write_only_template(tmp_path / "out")
    key = "b" * 64
    instance = cycle._bind_owner(queue, template, key)
    assert instance["owner_action_key"] == key
    assert queue.item_path(pool.CLAIMED, key).exists()
    assert queue.ledger().holder_tokens(key) == {"cpu": 1, "mem_gb": 1}
    queue.finish(key, status="executed")
    assert queue.ledger().held_keys() == []
