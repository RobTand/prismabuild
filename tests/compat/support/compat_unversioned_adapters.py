"""Actual unversioned queue and ledger carriers with private admission observations."""
from __future__ import annotations

import base64
import json

C, G = "c" * 64, "7" * 32
TIERS = {"preferred": [0, 1], "fallback": []}


def operate(request, store, load):
    family, variant = request["case"].split("/", 1)
    writing = request["op"] == "write"
    pool = load("prismabuild.pool")
    queue = pool.PoolQueue(store / "queue")
    queue.ensure_layout()
    if family == "q-passes":
        path = queue.passes_path(C)
        if writing:
            ordinary = {"blocked_unix": 90., "epoch_unix": 95.} if variant == "ordinary" else {}
            measurement = {"foreign_unix": 91., "measurement_until_unix": 110.} if variant == "measurement" else {}
            for _ in range(2 if variant == "replay" else 1):
                if hasattr(pool, "_PendingDrainNotes"):
                    notes = pool._PendingDrainNotes(C,
                        {C: pool.drain_policy.OrdinaryNotes(ordinary)} if ordinary else {},
                        {C: pool.drain_policy.MeasurementNotes("compat-host", "publication-1", measurement)} if measurement else {})
                    queue.record_pass(C, drain_notes=notes)
                else:
                    queue._drain_notes = {C: ordinary}
                    queue._measurement_drain_notes = {C: ("compat-host", "publication-1", measurement)} if measurement else {}
                    queue.record_pass(C)
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(base64.b64decode(request["raw"]))
            request["_consumer_boundary"]()
            count = queue.passes(C)
            assert count == (2 if variant == "replay" else 1)
            return {"passes": count}
    else:
        cpu = load("prismabuild.adaptive_cpu")
        cpu.BOX_STATE_ROOT = store / "states"
        ledger = pool.ResourceLedger(store / "queue/reservations", "compat-host")
        if family == "host-cpu-map":
            path = ledger.base / "cpu-map.json"
            if writing:
                ledger.configure_cpu_tiers(TIERS)
                if variant == "replay":
                    assert ledger.configure_cpu_tiers(TIERS) == TIERS
            else:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(base64.b64decode(request["raw"]))
                request["_consumer_boundary"]()
                return ledger.configure_cpu_tiers(TIERS, initialize=False)
        else:
            gpu = load("prismabuild.adaptive_gpu")
            if family == "host-cpu-metadata":
                demand = 2 if variant in ("borrowed", "allowance") else 1
                ledger.ensure_capacity({"cpu": 1 if variant == "borrowed" else demand})
                facts = {"measurement": True, "declared_cpu": demand, "admitted_unix": 100.}
                if variant == "borrowed":
                    facts.update(borrowing=True, preferred_borrow=1, borrowable_cpus=[1])
                if variant == "allowance":
                    facts["dependent_allowance"] = {"cpu": 1, "mem_gb": 1, "slots": 1, "basis": "fixture"}
                handle = ledger.begin_acquire(C, {"cpu": demand}, adaptive=facts, cpu_tiers=TIERS)
                marker = cpu.METADATA
            else:
                ledger.ensure_capacity({"gpu": 1})
                facts = {"measurement": variant == "measurement", "exclusive": variant == "exclusive", "admitted_unix": 100.}
                handle = ledger.begin_acquire(C, {"gpu": 1}, adaptive_gpu=facts)
                marker = gpu.METADATA
            assert handle and ledger.commit_acquire(C, handle) > 0
            path = ledger.held_dir / C / marker
            if not writing:
                path.write_bytes(base64.b64decode(request["raw"]))
                if family == "host-cpu-metadata":
                    sample = {"sampled_unix": 100., "cpu_count": 2, "busy_cpus": 0., "psi_some": 0., "interval_s": 1.}
                    controller = cpu.Controller(ledger, TIERS)
                    controller.sample = lambda: sample
                    request["_consumer_boundary"]()
                    result = controller.decision({"action_key": "d" * 64}, {"cpu": 1}, identity=("compat-shape", False), owner=None)
                    assert result is None and controller.last_decision["reason"] == "measurement_holder", controller.last_decision
                else:
                    controller = gpu.Controller(ledger)
                    controller.sample = lambda: {}
                    request["_consumer_boundary"]()
                    result = controller.decision({}, {"gpu": 1}, contract=("compat-shape", False, False, 8))
                    assert result is None and controller.last_decision["reason"] == "exclusive_holder", controller.last_decision
                return {"refusal": controller.last_decision["reason"], "physical_admission": False}
    return {"raw": base64.b64encode(path.read_bytes()).decode(),
            "fixture_facts": ["private counter/CPU-token/GPU-token files; supplied topology/admission/telemetry facts; no physical CPU or GPU allocation"]}
