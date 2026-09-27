"""Opt-in PB-only paired claim-loop profile for #1221 (no live queue or GPU)."""
from __future__ import annotations

import contextlib
import json
import os
from pathlib import Path
import time

import pytest

from prismabuild import adaptive_cpu, pool
from test_claim_denials import publish


@pytest.mark.skipif(os.environ.get("PB_DENIAL_PROFILE") != "1", reason="opt-in PB claim-loop profile")
def test_private_claim_loop_profile(tmp_path, monkeypatch):
    queue = pool.PoolQueue(tmp_path / "queue")
    for index in range(550):
        publish(queue, f"{index:064x}", resources={"cpu": 1})
    # Fixed refusal isolates claim bookkeeping from adaptive policy/calibration.
    # Real claim/lock/ring/pass/diagnostic code still runs. Two scans remain
    # below the existing starvation floor; no scheduler threshold is changed.
    def refuse(controller, *args, **kwargs):
        controller.last_decision = {
            "reason": "host_pressure", "sample": {
                "sampled_unix": time.time(), "busy_cpus": 1.95,
                "psi_some": .2, "cpu_count": 2, "interval_s": 1.0}}
        return None
    monkeypatch.setattr(adaptive_cpu.Controller, "decision", refuse)
    # Snapshot copying is not the defect under measurement; count requests,
    # without a second background writer perturbing this private fixture.
    counters = {"denial_writes": 0, "denial_bytes": 0, "pass_writes": 0,
                "pass_bytes": 0, "snapshot_requests": 0, "admission_holds": []}
    monkeypatch.setattr(adaptive_cpu.adaptive_snapshot, "publish",
                        lambda *a, **k: counters.__setitem__("snapshot_requests", counters["snapshot_requests"] + 1))
    original_local = adaptive_cpu.write_json
    original_shared = pool._write_json_atomic
    original_locked = adaptive_cpu.Controller.locked

    def local_write(path, value):
        result = original_local(path, value)
        if Path(path).name == pool.CLAIM_DENIALS:
            counters["denial_writes"] += 1
            counters["denial_bytes"] += Path(path).stat().st_size
        return result

    def shared_write(path, value):
        result = original_shared(path, value)
        if Path(path).parent == queue.root / pool.PASSES:
            counters["pass_writes"] += 1
            counters["pass_bytes"] += Path(path).stat().st_size
        return result

    @contextlib.contextmanager
    def locked(controller):
        with original_locked(controller):
            began = time.perf_counter()
            try:
                yield
            finally:
                counters["admission_holds"].append(time.perf_counter() - began)

    monkeypatch.setattr(adaptive_cpu, "write_json", local_write)
    monkeypatch.setattr(pool, "_write_json_atomic", shared_write)
    monkeypatch.setattr(adaptive_cpu.Controller, "locked", locked)
    for iteration in range(2):
        ready = queue.ready_items()  # snapshot work outside admission timing
        for name in counters:
            counters[name] = [] if name == "admission_holds" else 0
        began = time.perf_counter()
        result = queue.claim(capacity={"cpu": 2}, cpu_tiers={"preferred": [0, 1], "fallback": []},
                             adaptive_cpu=True, ready=ready)
        elapsed = time.perf_counter() - began
        assert result is None
        holds = counters.pop("admission_holds")
        record = {"schema": "pb.claim_denial_profile.v1", "iteration": iteration,
                  "ready_rows": len(ready), "wall_s": elapsed,
                  "admission_lock_sum_s": sum(holds),
                  "admission_lock_max_s": max(holds, default=0),
                  "admission_lock_acquisitions": len(holds), **counters}
        print("PB-DENIAL-PROFILE " + json.dumps(record, sort_keys=True), flush=True)
        counters["admission_holds"] = []
