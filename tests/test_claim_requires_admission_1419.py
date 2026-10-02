"""Causal PB1419 admission-front-door controls, not metadata exemptions."""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

from prismabuild import pool

KEY = "a" * 64
ROOT = Path(__file__).resolve().parents[1]


def publish(queue, resources):
    options = {} if resources is None else {"resources": resources}
    queue.publish(action_key=KEY, cas_root=queue.root / "cas",
                  checkout_root=queue.root / "co", worker_script=queue.root / "worker.py",
                  max_attempts=1, **options)


@pytest.mark.parametrize("resources", [None, {}, {"cpu": 0, "mem_gb": 0}, {"cpu": 1}])
def test_unmanaged_claim_never_moves_an_executable_row(tmp_path, monkeypatch, resources):
    queue = pool.PoolQueue(tmp_path / "queue")
    publish(queue, resources)
    refused = []
    original = queue.record_denial
    def denial(item, reason, evidence=None):
        refused.append((reason, evidence))
        original(item, reason, evidence)
    monkeypatch.setattr(queue, "record_denial", denial)
    assert queue.claim() is None, "unmanaged executable claim bypassed admission"
    assert queue.item_path(pool.READY, KEY).exists()
    assert not queue.item_path(pool.CLAIMED, KEY).exists()
    assert not queue.item_path(pool.INTENT, KEY).exists()
    assert refused[-1][0] == "admission_capacity_required"
    assert not queue.ledger().held_keys()


@pytest.mark.parametrize("resources", [None, {}, {"cpu": 0, "mem_gb": 0}])
def test_capacity_does_not_invent_positive_declared_demand(tmp_path, monkeypatch, resources):
    queue = pool.PoolQueue(tmp_path / "queue")
    publish(queue, resources)
    refused = []
    monkeypatch.setattr(queue, "record_denial", lambda item, reason, evidence=None: refused.append(reason))
    assert queue.claim(capacity={"cpu": 2, "mem_gb": 4}) is None
    assert refused[-1] == "admission_demand_required"
    assert queue.item_path(pool.READY, KEY).exists()
    assert not queue.ledger().held_keys()


def test_missing_capacity_stops_serve_before_execute(tmp_path, monkeypatch):
    queue = pool.PoolQueue(tmp_path / "queue")
    publish(queue, {"cpu": 1})
    def execute(*args, **kwargs):
        raise AssertionError("serve_once executed an unmanaged claim")
    monkeypatch.setattr(queue, "execute", execute)
    monkeypatch.setattr(queue, "_sweep_due", lambda: False)
    assert queue.serve_once() is None
    assert queue.item_path(pool.READY, KEY).exists()


def test_explicit_controlled_admission_retains_normal_claim(tmp_path):
    queue = pool.PoolQueue(tmp_path / "queue")
    publish(queue, {"cpu": 1, "mem_gb": 1})
    claimed = queue.claim(capacity={"cpu": 2, "mem_gb": 4})
    assert claimed is not None and claimed["action_key"] == KEY
    assert queue.ledger().holder_tokens(KEY) == {"cpu": 1, "mem_gb": 1}


def test_production_one_shot_supplies_admission_not_just_containment(tmp_path, monkeypatch, capsys):
    spec = importlib.util.spec_from_file_location("admission_one_shot_test", ROOT / "tools/fleet/worker.py")
    assert spec is not None and spec.loader is not None
    worker = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(worker)
    monkeypatch.setattr(worker, "refusal", lambda: "")
    monkeypatch.setattr(worker, "SH", tmp_path)
    seen = []
    def serve(queue, **options):
        seen.append(options)
        return None
    monkeypatch.setattr(pool.PoolQueue, "serve_once", serve)
    # The shared loop's existing preparation is tested independently. Here
    # its admitted boundary is witnessed on the actual production entrypoint.
    import worker_loop
    def once(stop_requested, **options):
        queue = pool.PoolQueue(tmp_path / "pb-queue")
        outcome = queue.serve_once(capacity={"cpu": 2, "mem_gb": 4},
                                   cpu_tiers={"preferred": [0, 1], "fallback": []},
                                   adaptive_cpu=True, containment=True, has_gpu=True)
        if options.get("on_outcome") is not None:
            options["on_outcome"](outcome)
        return 0
    monkeypatch.setattr(worker_loop, "_run_loop", once)
    assert worker.main() == 0
    result = json.loads(capsys.readouterr().out)
    assert result["outcome"] is None
    assert len(seen) == 1
    assert seen[0].get("capacity") == {"cpu": 2, "mem_gb": 4}, "one-shot bypassed shared preparation"
    assert seen[0]["adaptive_cpu"] is True and seen[0]["containment"] is True


def test_tier_funding_without_host_demand_never_grants_execution(tmp_path, monkeypatch):
    queue = pool.PoolQueue(tmp_path / "queue")
    tier = "prismabuild-stage:fixture"
    queue.mint_tier_capacity(tier, {"stage_gib": 1})
    resources = {f"stage_gib@{tier}": 1}
    queue.publish(action_key=KEY, cas_root=queue.root / "cas",
                  checkout_root=queue.root / "co", worker_script=queue.root / "worker.py",
                  resources=resources, residency={"schema": pool.RESIDENCY_SCHEMA_V1,
                      "tier_id": tier, "manifest_sha256": "b" * 64,
                      "manifest_bytes": 1, "range_start_bytes": 0,
                      "range_end_bytes": 1})
    refused = []
    monkeypatch.setattr(queue, "record_denial", lambda item, reason, evidence=None: refused.append(reason))
    assert queue.claim(capacity={"cpu": 2, "mem_gb": 4}) is None
    assert refused[-1] == "admission_demand_required"
    assert queue.item_path(pool.READY, KEY).exists()
    assert not queue.item_path(pool.CLAIMED, KEY).exists()
    assert not queue.ledger().held_keys()
    assert not queue.tier_ledger(tier).held_keys()
