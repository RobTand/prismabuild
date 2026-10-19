"""Real residency-plan and shared-range publication/consumption."""
from __future__ import annotations

import base64
import json

C, M, D = "c" * 64, "a" * 64, "9" * 64
TIER, RAM = "prismabuild-stage:compat", "ram:compat"


def mover(pool, key, tier, start=0, end=8):
    kind = "ram_gib" if tier == RAM else "stage_gib"
    return {"action_key": key, "cas_root": "/compat/cas", "worker_script": "/compat/worker.py",
            "checkout_root": "/compat/checkout", "resources": {"cpu": 1, "mem_gb": 1, kind + "@" + tier: 1},
            "residency": {"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": tier,
                          "manifest_sha256": D, "manifest_bytes": 8,
                          "range_start_bytes": start, "range_end_bytes": end}}


def plan(rp, pool, variant):
    phase = {"name": "copy", "start_bytes": 0, "end_bytes": 8}
    stage = mover(pool, M, TIER)
    egress = {"action_key": "b" * 64, "cas_root": "/compat/cas", "worker_script": "/compat/worker.py",
              "checkout_root": "/compat/checkout", "resources": {"cpu": 1, "mem_gb": 1}}
    chunked = variant in ("chunks", "ram-chunks", "combined")
    if chunked:
        phase["stage_chunks"] = [{"chunk_index": 0, "start_bytes": 0, "end_bytes": 8,
                                  "stage_gib": 1, "mover_row": stage, "egress_row": egress}]
    else:
        phase.update(mover_row=stage, egress_row=egress)
    optional = {}
    if variant.startswith("ram-") or variant == "combined":
        optional["ram_tier_id"] = RAM
        ram = mover(pool, "d" * 64, RAM)
        ram_egress = {**egress, "action_key": "e" * 64}
        if chunked:
            phase["ram_chunks"] = [{"chunk_index": 0, "start_bytes": 0, "end_bytes": 8,
                                    "stage_gib": 1, "ram_mover_row": ram, "ram_egress_row": ram_egress}]
        else:
            phase.update(ram_mover_row=ram, ram_egress_row=ram_egress)
    if variant in ("reader-depth", "reader-rate", "reader-both", "combined"):
        optional["reader"] = {}
        if variant != "reader-rate":
            optional["reader"]["prefetch_depth_bytes"] = 8
        if variant != "reader-depth":
            optional["reader"]["read_mb_s"] = 8
    if variant in ("demand-source", "combined"):
        optional["demand_source"] = {"manifest_sha256": D, "quoted_bytes": 8}
    if variant in ("prelaunch", "combined"):
        phase["resident_before_launch"] = True
    return rp.build_plan(consumer_action_key=C, tier_id=TIER, stage_root="/compat/stage",
                         manifest_sha256=D, manifest_bytes=8, phases=[phase], **optional)


def shared(rp, pool, queue, variant):
    kwargs = {"manifest_sha256": D, "tier_id": TIER, "start": 0, "end": 8,
              "registered_by": "compat", "sealed_against": {"mover_python": "/compat/python", "mover_tools_root": "/compat/tools"}}
    rp.register_shared_range(queue, seal=lambda: (mover(pool, M, TIER), {"source": "compat"}), **kwargs)
    key = M
    if variant != "first":
        if variant == "withdrawn":
            queue.publish(action_key=M, cas_root="/compat/cas", worker_script="/compat/worker.py",
                          checkout_root="/compat/checkout", resources={"cpu": 1, "mem_gb": 1})
            queue.withdraw(M, reason="compat operator", by="compat", signal_child=False)
        else:
            kwargs["sealed_against"]["mover_tools_root"] = "/compat/new-tools"
        key = "f" * 64
        rp.register_shared_range(queue, seal=lambda: (mover(pool, key, TIER), {"source": "compat"}), **kwargs)
    return rp.share_namespace(D, TIER, 0, 8), key


def operate(request, store, load):
    family, variant = request["case"].split("/", 1)
    rp, pool = load("prismabuild.residency_plan"), load("prismabuild.pool")
    queue = pool.PoolQueue(store / "queue")
    if request["op"] == "write":
        if family == "plan":
            rp.freeze(queue, plan(rp, pool, variant))
            path = queue.records.residency_plan_path(C)
        else:
            namespace, key = shared(rp, pool, queue, variant)
            path = rp.shared_range_path(queue, namespace) if family == "shared-range" else rp.shared_mover_path(queue, key)
        return {"raw": base64.b64encode(path.read_bytes()).decode()}
    raw = base64.b64decode(request["raw"])
    body = json.loads(raw)
    if family == "plan":
        key, path, reader = C, queue.records.residency_plan_path(C), rp.read
    elif family == "shared-range":
        key = body["share_namespace"]
        path, reader = rp.shared_range_path(queue, key), rp.read_shared_range
    else:
        key = body["mover_action_key"]
        path, reader = rp.shared_mover_path(queue, key), rp.read_shared_mover
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(raw)
    request["_consumer_boundary"]()
    result = reader(queue, key)
    assert result, "plan/shared-range consumer refused the record"
    if family == "plan":
        assert result["manifest_bytes"] == 8
    else:
        assert result["share_namespace"] == rp.share_namespace(D, TIER, 0, 8)
    return result
