"""Production submitter and manifest builders over private, deterministic inputs."""
from __future__ import annotations

from pathlib import Path
import json
import runpy
import base64
import hashlib
import uuid


def helper(name):
    return runpy.run_path(str(Path(__file__).with_name(name)))


def action(core, store, load, variant):
    if variant == "smoke-builder":
        (store / "task_code.py").write_bytes(b"print('compat')\n")
        return load("seal_and_publish").build_action(store)
    body = helper("compat_core_adapters.py")["action_body"](core, store)
    command = ["/bin/sh", "-c", "printf compat > result.bin"]
    body["params"].update(command=command, cwd="/compat/checkout", demand={"cpu": 1, "mem_gb": 1},
                          retry_policy={"max_attempts": 1, "retry_safe": False}, placement={"required_tags": []})
    body["environment"]["variables"] = {"PATH": "/usr/bin:/bin"}
    template = {**body, "marker_root": Path("/compat/markers"), "checkout_identity": {"commit": "a" * 40},
                "log_name": "pbrun_result.log"}
    return load("prismabuild.submission").seal(template)


def manifest(core, store, load, variant):
    bl = load("prismabuild.batch_ledger")
    oc = load("prismabuild.output_contract")
    if variant == "canary-builder":
        canary = load("pbcanary_legs.leg3")
        return canary.manifest_for_run([str(store / name) for name in canary.LEG3_CHUNK_NAMES], str(store))
    factory = helper("compat_core_adapters.py")["manifest"]
    codec = helper("compat_core_adapters.py")["manifest_owner"](core, load)
    if variant == "task-builder":
        dc = load("prismabuild.decomposition")
        policy = {"schema": dc.TASK_DATA_MANIFEST_SCHEMA_V1, "payload_field": "entries", "mount_prefix": "/compat/input",
                  "residency_tier": None, "residency_ram": "off", "mover_readers": 1, "mover_mem_gb": 1}
        envelope = {"parent_key": "a" * 64, "plan_key": "b" * 64, "child_ordinal": 0,
                    "tasks": [{"id": "one", "payload": {"entries": factory(codec, "unphased", False)["entries"]}}]}
        return dc.task_data_manifest(policy, envelope)
    origin = variant in ("origin-builder", "merged-builder", "placed-builder")
    po, pool, _, queue, template, instance, descriptor, path = helper("compat_batch_adapters.py")["committed"](
        store, load, "origin" if origin else "staged")
    batch = json.loads(path.read_bytes())
    if variant == "stage-builder":
        return po.build_stage_manifest(batch, str(store / "outputs"))
    bl.portable_origin_identity = lambda info: {"ino": 2, "size": info.st_size, "mtime_ns": 3, "ctime_ns": 4}
    refs = [oc.origin_batch_ref(instance, batch_id="batch", manifest_digest=oc.output_manifest_sha256([descriptor]))]
    if variant == "origin-builder":
        return po.origin_batch_manifest(queue.root, refs)
    if variant == "merged-builder":
        batches = po.origin_batch_manifest(queue.root, refs)
        static = factory(codec, "phased", False)
        static["mount_prefix"] = str(store)
        static["entries"][0]["path"] = str(store / "static.bin")
        return load("prismabuild.action_edges").merged_manifest(static, batches)
    static = factory(codec, "read-plan", True)
    static["mount_prefix"] = str(store)
    static["entries"][0]["path"] = str(store / "static.bin")
    static["read_plan"]["phases"].append({"name": "origin", "entry_indices": [], "bytes": 0, "cumulative_bytes": 8})
    placed = po.place_origin_batches(queue.root, static, [{"phase": "origin", "refs": refs}])
    po.verify_placed_origin_batches(queue.root, placed)
    return placed


def output_funding(request, store, load):
    bl = load("prismabuild.batch_ledger")
    oc = load("prismabuild.output_contract")
    variant = request["case"].split("/", 1)[1]
    pool = load("prismabuild.pool")
    queue = pool.PoolQueue(store / "queue")
    mover, owner, nonce, tier = "a" * 64, "c" * 64, "7" * 32, "prismabuild-stage:compat"
    path = queue.records.funding_output_path(mover, tier)
    if request["op"] == "read":
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(base64.b64decode(request["raw"]))
        request["_consumer_boundary"]()
        value = queue.funding.read_output(mover, tier)
        assert value and value["state"] == ("reserved" if variant == "stage-intent" else "transferring")
        return value
    uuid.uuid4 = lambda: uuid.UUID(nonce)
    po, pool, core, queue, template, instance = helper("compat_batch_adapters.py")["context"](store, load, "staged")
    assert queue.tier(tier).take(owner, {"stage_gib": 1})
    origin = Path(template["output_prefix"]) / "payload.bin"
    assert bl.BatchLedger.reserve(queue, instance, template, batch_id="batch", tier=tier,
        class_bytes={"payload": 8, "checkpoint": 0, "temp": 0}, paths=[str(origin)])["ok"]
    origin.parent.mkdir(parents=True)
    origin.write_bytes(b"contents")
    descriptor = oc.validate_descriptor({"schema": oc.DESCRIPTOR_SCHEMA_V2, "slot": "payload", "artifact_class": "payload",
        "path": str(origin), "bytes": 8, "sha256": hashlib.sha256(b"contents").hexdigest(),
        "producer_generation": nonce, "owner_action_key": owner, "owner_attempt": dict(instance["owner_attempt"])}, template, instance)
    args = dict(tier_id=tier, owner_key=owner, mover_key=mover, instance=instance,
                template=template, batch_id="batch", descriptors=[descriptor])
    if variant == "stage-intent":
        result = queue.funding.stage_output(**args)
    else:
        ref = queue.build_produced_output_batch_ref(instance=instance, template=template,
            batch_id="batch", descriptors=[descriptor], tier_id=tier)
        staged = queue.funding.stage_output(**args)
        assert staged["ok"], staged
        queue.publish(action_key=mover, cas_root=store / "cas", checkout_root="/compat/checkout",
            worker_script="/compat/worker.py", resources={"cpu": 1, "mem_gb": 1, "stage_gib@" + tier: 1},
            residency={"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": tier, "manifest_sha256": oc.output_manifest_sha256([descriptor]),
                       "manifest_bytes": 8, "range_start_bytes": 0, "range_end_bytes": 8}, produced_output_batch=ref)
        # A legacy publisher could leave a genuine absent intent. Keep the
        # current publication guard, then construct that private recovery state.
        path.unlink()
        result = queue.funding.fund_output(**args)
    assert result["ok"], result
    return {"raw": base64.b64encode(path.read_bytes()).decode(),
            "fixture_facts": ["private supplied owner claim; actual eight-byte prewrite and tier token transfer",
                              "direct-fund case has an explicitly removed private intent after guarded publication, representing a legacy absent-intent row"]}
