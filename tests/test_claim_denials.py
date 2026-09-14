"""Claim-denial diagnostics are best-effort evidence, never admission state."""
from __future__ import annotations

import json
from pathlib import Path
import sys
import time
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "tools/fleet")]
from prismabuild import adaptive_cpu, adaptive_gpu, adaptive_snapshot, pool  # noqa: E402
import pbstatus  # noqa: E402

KEY_A, KEY_B = "a" * 64, "b" * 64


def publish(queue, key, **kwargs):
    queue.publish(action_key=key, cas_root="/cas", checkout_root="/co", worker_script="/w.py", **kwargs)


def local_records(queue):
    path = adaptive_cpu.local_state_base(queue.ledger().base) / pool.CLAIM_DENIALS
    return adaptive_cpu.read_json(path).get("records", {})


def test_transition_busy_is_recorded_while_a_neighbour_claims(tmp_path, monkeypatch):
    queue = pool.PoolQueue(tmp_path / "queue")
    publish(queue, KEY_A, priority=1)
    publish(queue, KEY_B)
    original = queue._transition_locked

    def contested(key, **kwargs):
        return original(key, blocking=False) if key != KEY_A else _unheld()

    monkeypatch.setattr(queue, "_transition_locked", contested)
    assert queue.claim()["action_key"] == KEY_B
    denial = next(value for value in local_records(queue).values() if value["action_key"] == KEY_A)
    assert denial["reason"] == "transition_busy"


class _unheld:
    def __enter__(self): return False
    def __exit__(self, *args): return False


def test_denial_history_is_bounded_and_snapshot_copy_preserves_it(tmp_path, monkeypatch):
    queue = pool.PoolQueue(tmp_path / "queue")
    monkeypatch.setattr(pool.socket, "gethostname", lambda: "box")
    for number in range(pool.MAX_CLAIM_DENIALS + 1):
        item = {"action_key": f"{number:064x}", "published_unix": float(number + 1)}
        queue.record_denial(item, "fixture", {"number": number})
    records = local_records(queue)
    assert len(records) == pool.MAX_CLAIM_DENIALS
    local = adaptive_cpu.local_state_base(queue.ledger().base)
    copied = adaptive_snapshot.copy_snapshot(local, queue.ledger().base / "adaptive")
    assert pool.CLAIM_DENIALS in copied
    shared = json.loads((queue.ledger().base / "adaptive" / pool.CLAIM_DENIALS).read_text())
    assert shared["schema"] == pool.CLAIM_DENIALS_SCHEMA_V1 and len(shared["records"]) == pool.MAX_CLAIM_DENIALS


def test_diagnostic_failure_does_not_change_a_claim_or_passes(tmp_path, monkeypatch):
    queue = pool.PoolQueue(tmp_path / "queue")
    publish(queue, KEY_A, resources={"gpu": 99})
    publish(queue, KEY_B, resources={"gpu": 1})
    original_write = adaptive_cpu.write_json
    def fail_denial(path, value):
        if Path(path).name == pool.CLAIM_DENIALS:
            raise OSError("full")
        return original_write(path, value)
    monkeypatch.setattr(adaptive_cpu, "write_json", fail_denial)
    assert queue.claim(capacity={"gpu": 2})["action_key"] == KEY_B
    assert queue.passes(KEY_A) == 0


def test_cpu_and_gpu_refusals_keep_the_actual_current_decision(tmp_path, monkeypatch):
    queue = pool.PoolQueue(tmp_path / "queue")
    ledger = queue.ledger()
    ledger.ensure_capacity({"cpu": 2, "gpu": 1})
    tiers = {"preferred": [0, 1], "fallback": []}
    cpu = adaptive_cpu.Controller(ledger, tiers)
    monkeypatch.setattr(cpu, "sample", lambda: {"sampled_unix": time.time(), "busy_cpus": 1.95,
                                                   "psi_some": .2, "cpu_count": 2, "interval_s": 1.})
    assert cpu.decision({"action_key": KEY_A}, {"cpu": 1}, identity=(None, False)) is None
    assert cpu.last_decision["reason"] == "host_pressure"
    assert cpu.last_decision["sample"]["psi_some"] == .2

    gpu = adaptive_gpu.Controller(ledger)
    gpu.last_decision = {"reason": "old"}
    sample = {"sampled_unix": time.time(), "schema": "prismabuild.gpu_capacity.v1", "complete": True,
              "attributed": True, "sample_id": "now", "foreign_processes": [], "jobs": [],
              "host_total_bytes": 128 * 1024**3, "host_available_bytes": 120 * 1024**3,
              "memory_pressure_some": 0., "memory_pressure_full": 0., "cpu_pressure_some": 0.,
              "devices": [{"power_w": 1., "power_limit_w": 10., "uuid": "gpu", "limited": False,
                           "memory_domain": "discrete"}]}
    monkeypatch.setattr(gpu, "sample", lambda: sample)
    assert gpu.decision({"action_key": KEY_A}, {"gpu": 1}, contract=(None, False, False, 1)) is None
    assert gpu.last_decision["reason"] == "gpu_memory_sample_invalid"
    assert gpu.last_decision["sample"] is sample


def test_unfunded_denial_distinguishes_withholding_from_past_ceiling(tmp_path):
    queue = pool.PoolQueue(tmp_path / "queue")
    publish(queue, KEY_A, resources={"gpu": 4})
    publish(queue, KEY_B, resources={"gpu": 1})
    ledger = queue.ledger()
    ledger.ensure_capacity({"gpu": 4})
    assert ledger.acquire("0" * 64, {"gpu": 2})
    for _ in range(pool.STARVATION_FLOOR - 1):
        queue.record_pass(KEY_A)
    assert queue.claim(capacity={"gpu": 4}) is None
    denial = next(value for value in local_records(queue).values() if value["action_key"] == KEY_A)
    assert denial["reason"] == "reservation_unavailable_withholding"
    sidecar = json.loads(queue.passes_path(KEY_A).read_text())
    sidecar["first_unix"] -= pool.WITHHOLD_CEILING_S + 1
    queue.passes_path(KEY_A).write_text(json.dumps(sidecar))
    assert queue.claim(capacity={"gpu": 4})["action_key"] == KEY_B
    denial = next(value for value in local_records(queue).values() if value["action_key"] == KEY_A)
    assert denial["reason"] == "reservation_unavailable_past_ceiling"


def test_status_keeps_two_hosts_and_rejects_old_or_future_evidence(tmp_path):
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    publish(queue, KEY_A, resources={"cpu": 1})
    item = json.loads(queue.item_path(pool.READY, KEY_A).read_text())
    for host, stamp, generation in (("one", time.time() - 1, item["published_unix"]),
                                    ("two", time.time() - 2, item["published_unix"]),
                                    ("old", time.time() - 3, 0.0),
                                    ("future", time.time() + 30, item["published_unix"])):
        path = queue.ledger(host).base / "adaptive" / pool.CLAIM_DENIALS
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({"schema": pool.CLAIM_DENIALS_SCHEMA_V1, "records": {host: {
            "action_key": KEY_A, "published_unix": generation, "host": host,
            "reason": "fixture", "evidence": {}, "denied_unix": stamp}}}))
    row = next(row for row in pbstatus.read_pool(queue.root)["jobs"] if row["action_key"] == KEY_A)
    assert [record["host"] for record in row["admission_denials"]] == ["one", "two"]


def memory_blocked_queue(tmp_path):
    queue = pool.PoolQueue(tmp_path / "queue")
    capacity = {"cpu": 20, "gpu": 1, "mem_gb": 104}
    ledger = queue.ledger()
    ledger.ensure_capacity(capacity)
    assert ledger.acquire(KEY_B, {"cpu": 6, "gpu": 1, "mem_gb": 101})
    publish(queue, KEY_A, resources={"cpu": 4, "mem_gb": 8})
    return queue, ledger, capacity


def test_memory_shortage_names_observed_tokens_and_preserves_reservations(tmp_path):
    queue, ledger, capacity = memory_blocked_queue(tmp_path)
    assert queue.claim(capacity=capacity) is None
    assert ledger.available() == {"cpu": 14, "mem_gb": 3}
    denial = next(iter(local_records(queue).values()))
    assert denial["reason"] == "reservation_unavailable"
    assert denial["evidence"].get("token_shortage") == {
        "resource": "mem_gb", "requested": 8, "available": 3,
    }
    assert queue.passes(KEY_A) == 1
    ledger.release(KEY_B)
    assert queue.claim(capacity=capacity)["action_key"] == KEY_A


def test_cpu_only_shortage_does_not_quote_a_previous_gpu_decision(tmp_path):
    queue, ledger, capacity = memory_blocked_queue(tmp_path)
    gpu = SimpleNamespace(last_decision={"reason": "unrelated_gpu_refusal"})
    assert queue._claim(capacity=capacity, gpu_controller=gpu) is None
    denial = next(iter(local_records(queue).values()))
    assert denial["evidence"].get("gpu_decision") is None


@pytest.mark.parametrize("resource", ["cpu", "gpu", "mem_gb"])
@pytest.mark.parametrize("free", [0, 3])
def test_shortage_uses_acquired_tokens_and_resets_on_the_next_attempt(tmp_path, resource, free):
    ledger = pool.ResourceLedger(tmp_path / "reservations")
    ledger.ensure_capacity({resource: 4})
    assert ledger.acquire(KEY_B, {resource: 4 - free})
    assert ledger.begin_acquire(KEY_A, {resource: 4}) is None
    assert ledger.last_token_shortage == {
        "resource": resource, "requested": 4, "available": free,
    }
    assert ledger.available().get(resource, 0) == free
    ledger.release(KEY_B)
    handle = ledger.begin_acquire(KEY_A, {resource: 4})
    assert handle is not None
    assert ledger.last_token_shortage is None
    ledger.abandon_acquire(handle)
