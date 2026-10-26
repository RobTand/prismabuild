import copy

import gc

import hashlib

import json

import pickle

import time

from types import MappingProxyType, SimpleNamespace

import warnings

import weakref

import pytest

from prismabuild import fleet_roster, host_cap, pool, pool_reconcile, resource_scope

from test_capped_partial_host import capped  # noqa: F401  (fixture)

def _evidence(label, value):
    """pbtest forwards no -s/-r; the warnings summary carries the evidence."""
    text = label + " " + json.dumps(value, sort_keys=True, default=str)
    print(text)
    warnings.warn(text, UserWarning, stacklevel=2)

def _publish(queue, key="d" * 64):
    queue.publish(action_key=key, cas_root=queue.root / "cas", checkout_root=queue.root / "co",
                  worker_script=queue.root / "worker.py", resources={"cpu": 1, "mem_gb": 1})
    return key

def _sampler():
    return {"sampled_unix": time.time(), "busy_cpus": 0.0, "psi_some": 0.0,
            "cpu_count": 8, "interval_s": 1.0}

@pytest.mark.parametrize("producer", ["private", "simulation", "ordinary"])
def test_blocking_a_stale_capability_cannot_claim_without_its_fence(capped, tmp_path, monkeypatch,
                                                                   producer):
    """BLOCKING candidate. A genuine clamp-minted value proves minting history,
    not current containment. Omitting the optional admission_open callback lets
    it claim after the worker leaves the declared slice."""
    worker, _, _, membership = capped
    monkeypatch.setattr(worker.cpu_topology, "inherited_tiers", lambda: {
        "preferred": [4, 5, 6, 7], "fallback": [12, 13, 14, 15]})
    queue = pool.PoolQueue(tmp_path / "queue")
    key = _publish(queue)
    if producer == "private":
        parameters = worker.private_claim_parameters(queue)
    elif producer == "simulation":
        parameters = {**worker.capped_simulation_parameters(queue, {"cpu": 8, "mem_gb": 32}),
                      "cpu_tiers": {"preferred": [4, 5, 6, 7], "fallback": [12, 13, 14, 15]},
                      "adaptive_cpu": True}
    else:
        args = worker.build_parser().parse_args(["--assume-idle"])
        _, capacity, _ = worker.observed_claim_capacity(
            queue, args=args, base_declared={"cpu": 8, "mem_gb": 32},
            observer=None, gpu_sample=None, known_physical_gpus=0)
        parameters = {"capacity": capacity, "admission_open": worker.claim_capacity_fence(queue, capacity),
                      "cpu_tiers": {"preferred": [4, 5, 6, 7], "fallback": [12, 13, 14, 15]},
                      "adaptive_cpu": True}
    fence = parameters.pop("admission_open")
    assert fence()
    membership.write_text("0::/user.slice/worker\n")
    assert not fence(), "the fence itself observes the change"
    with pytest.raises(TypeError, match="registered claim fence"):
        queue.claim(**parameters, cpu_sampler=_sampler)
    outcome = {"producer": producer,
               "held": queue.ledger().held_keys() == [key]}
    _evidence("FENCELESS STALE CAPABILITY", outcome)
    assert queue.ledger().held_keys() == []
