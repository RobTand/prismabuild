"""Actual compressed tables, exact publication endings and mover observations."""
from __future__ import annotations

import base64
import json
from pathlib import Path
import runpy
import threading
import time
from types import SimpleNamespace

C = "c" * 64


def operate(request, store, load):
    family, variant = request["case"].split("/", 1)
    writing = request["op"] == "write"
    if family == "shape-table":
        gate, core = load("shape_gate"), load("prismabuild.core")
        if writing:
            fixture = runpy.run_path(str(Path(__file__).with_name("compat_core_adapters.py")))["manifest"]
            manifest = fixture(core, "read-plan", True)
            if variant == "hashed":
                manifest["entries"][0]["sha256"] = "a" * 64
            raw = gate.table_bytes(gate.table_from_manifest(manifest, manifest_sha256="b" * 64))
        else:
            request["_consumer_boundary"]()
            result = gate.parse_table(base64.b64decode(request["raw"]))
            assert result["source"]["total_bytes"] == 8 and len(result["entries"]) == 1
            return result
    else:
        pool = load("prismabuild.pool")
        queue = pool.PoolQueue(store / "queue")
        if family == "publication-ending":
            original = {"action_key": C, "published_unix": 100.0}
            path = queue.publication_ending_path(original)
            if writing:
                clock = time.time
                time.time = lambda: 101.0
                try:
                    successor_queue = pool.PoolQueue(store / "successor-queue")
                    successor = successor_queue.publish(action_key=C, cas_root=store / "cas", checkout_root="/compat/checkout",
                        worker_script="/compat/worker.py", resources={"cpu": 1, "mem_gb": 1})
                finally:
                    time.time = clock
                successor = json.loads(successor.read_bytes())
                queue._persist_publication_supersession(original, successor=successor)
                if variant == "replay":
                    queue._persist_publication_supersession(original, successor=successor)
                raw = path.read_bytes()
            else:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(base64.b64decode(request["raw"]))
                path.chmod(0o444)
                request["_consumer_boundary"]()
                result = queue.publication_ending(C, generation=100.0)
                assert result is not None and result.successor_generation == 101.0
                return {"predecessor": 100.0, "successor": result.successor_generation}
        elif writing:
            mover = load("stage_move")
            copier = SimpleNamespace(lock=threading.Lock(), bytes_staged=8 if variant == "landed" else 4,
                                     bytes_read=lambda: 8)
            report = mover._LandingReport(copier, queue, C, interval_s=1.0, range_bytes=8)
            report.write()
            if variant == "repeated":
                report.write()
            raw = queue.mover_landing_path(C).read_bytes()
        else:
            path = queue.mover_landing_path(C)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(base64.b64decode(request["raw"]))
            request["_consumer_boundary"]()
            result = queue.mover_landing(C)
            assert result and result["copied_bytes"] == 8 and result["landed_bytes"] == (8 if variant == "landed" else 4)
            return result
    return {"raw": base64.b64encode(raw).decode()}
