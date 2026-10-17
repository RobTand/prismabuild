"""Actual submitter, campaign and output residency-block writer paths."""
from __future__ import annotations

import base64
import json
from pathlib import Path
import runpy
from types import SimpleNamespace

C, M, TIER, RAM = "c" * 64, "a" * 64, "prismabuild-stage:compat", "ram:compat"


def fixture(name, symbol):
    return runpy.run_path(str(Path(__file__).with_name(name)))[symbol]


def operate(request, store, load):
    variant = request["case"].split("/", 1)[1]
    pool, core = load("prismabuild.pool"), load("prismabuild.core")
    queue = pool.PoolQueue(store / "queue")
    queue.ensure_layout()
    if request["op"] == "read":
        value = json.loads(base64.b64decode(request["raw"]))
        request["_consumer_boundary"]()
        result = queue.validate_residency(value, {"stage_gib@" + TIER: 1, "ram_gib@" + RAM: 1})
        assert result and result["manifest_bytes"] > 0
        assert result.get("leads") or result["range_end_bytes"] == 8
        return result
    if variant.startswith("output-"):
        po, pool, core, queue, template, instance, _, _ = fixture("compat_batch_adapters.py", "committed")(store, load, "staged")
        if variant == "output-publish-writer":
            result = po._publish_output_mover_row(queue, mover_key=M, cas_root=str(store / "cas"),
                launch={"worker_script": "/compat/worker.py", "priority": -10, "addressing": {"checkout_root": "/compat/checkout"}},
                host="compat-host", tier=TIER, kind="stage_gib", gib=1, manifest_digest="9" * 64, total=8,
                retry_policy={"max_attempts": 2, "retry_safe": True})
            assert result["published"], result
            path = queue.item_path(pool.READY, M)
        else:
            rows = po.due_mover_rows(queue, instance, template)
            assert len(rows) == 1, rows
            row = rows[0]
            path = queue.publish(action_key=M, cas_root=store / "cas", worker_script="/compat/worker.py",
                checkout_root="/compat/checkout", resources=row["resources"], residency=row["residency"])
    else:
        run = load("pbrun")
        run.SH = store
        cas = core.PrismaBuildCAS(store / "cas")
        body = fixture("compat_core_adapters.py", "action_body")(core, store)
        codec = fixture("compat_core_adapters.py", "manifest_owner")(core, load)
        manifest = fixture("compat_core_adapters.py", "manifest")(codec, "phased", False)
        entry, _ = cas.ingest_bytes(core._canonical_file_bytes(manifest), input_id="data_manifest")
        snapshot, _ = cas.ingest_bytes(b"unexecuted private snapshot declaration", input_id=core.PBRUN_CHECKOUT_SNAPSHOT_INPUT_ID)
        body["inputs"] = [snapshot, entry]
        body["params"].update(command=["true"], cwd="/compat/checkout", demand={"cpu": 1, "mem_gb": 1},
            placement={"required_tags": []}, retry_policy={"max_attempts": 1, "retry_safe": False},
            data_manifest={"input": entry}, checkout_snapshot={"schema": core.PBRUN_CHECKOUT_SNAPSHOT_SCHEMA_V1,
                "commit": "a" * 40, "subdirectory": ".", "input": snapshot})
        body["environment"]["variables"]["PATH"] = "/usr/bin"
        template = {**body, "cas": cas, "marker_root": store / "markers", "checkout_identity": {"commit": "a" * 40},
                    "log_name": "result.log", "stamp_name": "task_code.py"}
        args = SimpleNamespace(priority=-10, max_attempts=1, retry_safe=False, transport="pool",
            residency="stage", residency_tier=None, residency_mover_mem_gb=1, residency_mover_readers=1,
            residency_mover_max_attempts=2, residency_ram="auto" if variant == "pbrun-ram-writer" else "off")
        tier = {"schema": load("prismabuild.storage_tiers").TIER_RECORD_SCHEMA_V1, "tier_id": TIER, "host": "compat-host",
                "tier": "stage", "mountpoint": str(store / "stage"), "capacity_bytes": 4 << 30,
                "mover_python": "/compat/python", "mover_tools_root": "/compat/tools"}
        if variant == "pbrun-ram-writer":
            queue.announce_tier({**tier, "tier_id": RAM, "tier": "ram", "mountpoint": str(store / "ram"),
                                 "window_gib": 1, "promotion_chunk_gib": 1})
        staged = run.residency_stage_rows(template, consumer_action_key=C, tier=tier, args=args, queue=queue, cas=cas)
        rp = load("prismabuild.residency_plan")
        rp.freeze(queue, staged["plan"])
        if variant == "pbrun-reuse-writer":
            staged = run.residency_stage_rows(template, consumer_action_key=C, tier=tier, args=args, queue=queue, cas=cas)
            assert staged["reused_frozen_plan"]
        if variant in ("pbrun-mover-writer", "pbrun-ram-writer"):
            leg = rp.legs_over(staged["plan"], 0, 8, mover_role="ram_mover_row" if variant == "pbrun-ram-writer" else "mover_row")[0]
            row = leg["mover_row"]
            key = row["action_key"]
            path = queue.publish(**row)
            assert path == queue.item_path(pool.READY, key)
        elif variant == "campaign-writer":
            child = run.seal_action_from_template(template)
            plan = {**staged["plan"], "consumer_action_key": child["action_key"]}
            report = load("pbcampaign").child_record(child, args=args, queue=queue, cas=cas, staged_plan=plan)
            assert report["status"] == "submitted", report
            path = queue.item_path(pool.READY, child["action_key"])
        else:
            path = queue.publish(action_key=C, cas_root=cas.root, worker_script="/compat/worker.py", checkout_root="/compat/checkout",
                resources={"cpu": 1, "mem_gb": 1}, residency=staged["residency"])
    raw = fixture("compat_receipt_adapters.py", "nested_bytes")(path.read_bytes(), ("residency",))
    return {"raw": base64.b64encode(raw).decode(),
            "fixture_facts": ["actual private CAS sealing and queue publication; snapshot input is an unexecuted declaration, no admitted nested job"]}
