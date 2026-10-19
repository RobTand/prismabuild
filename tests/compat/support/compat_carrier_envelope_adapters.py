"""Full unversioned ledger/telemetry envelopes and externally written wave inputs."""
from __future__ import annotations

import base64
import json
from pathlib import Path
import runpy
import time

C, G = "c" * 64, "7" * 32


def operate(request, store, load):
    bl = load("prismabuild.batch_ledger")
    family, variant = request["case"].split("/", 1)
    writing = request["op"] == "write"
    if family == "controller-wave":
        pool, core = load("prismabuild.pool"), load("prismabuild.core")
        entry = {"key": C, "batch": "compat-batch"}
        if variant == "published":
            entry["published_unix"] = 100.
        value = {"waves": [] if variant == "empty" else [{"wave": 1, "closed": True, "members": [entry]}]}
        if not writing:
            value = json.loads(base64.b64decode(request["raw"]))
            request["_consumer_boundary"]()
        members, reason = pool.PoolQueue._awaited_wave_members(value)
        assert members is not None and not reason, reason
        assert members == ({} if variant == "empty" else {C: "compat-batch"})
        if not writing:
            return members
        return {"raw": base64.b64encode(core._canonical_file_bytes(value)).decode(),
                "fixture_facts": ["external controller caller input; no repository/runtime writer"]}
    if family == "out-ledger":
        helpers = runpy.run_path(str(Path(__file__).with_name("compat_batch_adapters.py")))
        if variant in ("staged", "origin", "preserved"):
            po, pool, core, queue, template, instance, _, _ = helpers["committed"](store, load, "origin-retain" if variant == "origin" else "payload")
        else:
            po, pool, core, queue, template, instance = helpers["context"](store, load, "payload")
        path = bl.BatchLedger.for_instance(queue.root, instance).path
        if writing and variant == "empty":
            bl.BatchLedger(path).file({"batches": {}, "admission": None})
        if writing and variant == "preserved":
            before = bl.BatchLedger(path).capture()
            bl.BatchLedger(path).file({"batches": before["batches"]})
            assert bl.BatchLedger(path).capture()["admission"] == before["admission"]
        if not writing:
            path.write_bytes(base64.b64decode(request["raw"]))
            request["_consumer_boundary"]()
            result = bl.BatchLedger(path).capture()
            sums = bl._class_sums(result["batches"])
            assert sums["payload"] == (8 if variant in ("staged", "origin", "preserved") else 0), sums
            assert (result["admission"] is None) == (variant == "empty")
            return {"charged_bytes": sums, "admission_present": result["admission"] is not None}
        return {"raw": base64.b64encode(path.read_bytes()).decode(),
                "fixture_facts": ["real private admission and batch commit; supplied claim and portable identities; no physical owner proof"]}
    helpers = runpy.run_path(str(Path(__file__).with_name("compat_adaptive_state_adapters.py")))
    with helpers["owned_publications"]():
        cpu, ledger, controller, item = helpers["context"](store, load)
        resource = load("prismabuild.resource_scope")
        path = store / "telemetry.json"
        if writing:
            scope = resource.ResourceScope(item["action_key"], G, 1024, path)
            scope.started, scope.cgroup_path, scope.unit = 90., store / "cgroup", "private-fixture.slice"
            scope.sample_process_io = lambda: {"source": "proc_io", "read_bytes": 8, "write_bytes": 8}
            def direct(path):
                if variant == "partial":
                    raise OSError("private unavailable cgroup fixture")
                return {"cpu_seconds": 4., "memory_current_bytes": 8, "memory_peak_bytes": 8, "oom_kill": 0, "oom_local": 0}
            resource.read_cgroup = direct
            original = time.monotonic
            time.monotonic = lambda: 100.
            try:
                scope.sample()
            finally:
                time.monotonic = original
            return {"raw": base64.b64encode(path.read_bytes()).decode(),
                    "fixture_facts": ["supplied private cgroup/proc observations; actual scope telemetry writer; no cgroup, broker, drain or performance proof"]}
        path.write_bytes(base64.b64decode(request["raw"]))
        request["_consumer_boundary"]()
        value = cpu.read_json(path)
        result = cpu.record_completion(ledger, item, value)
        assert result == (variant == "complete"), result
        return {"learned": result}
