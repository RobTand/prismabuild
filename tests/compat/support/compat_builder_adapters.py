"""Production submitter and manifest builders over private, deterministic inputs."""
from __future__ import annotations

from pathlib import Path
import runpy


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
    return load("pbrun").seal_action_from_template(template)


def manifest(core, store, load, variant):
    factory = helper("compat_core_adapters.py")["manifest"]
    if variant == "task-builder":
        dc = load("prismabuild.decomposition")
        policy = {"schema": dc.TASK_DATA_MANIFEST_SCHEMA_V1, "payload_field": "entries", "mount_prefix": "/compat/input",
                  "residency_tier": None, "residency_ram": "off", "mover_readers": 1, "mover_mem_gb": 1}
        envelope = {"parent_key": "a" * 64, "plan_key": "b" * 64, "child_ordinal": 0,
                    "tasks": [{"id": "one", "payload": {"entries": factory(core, "unphased", False)["entries"]}}]}
        return dc.task_data_manifest(policy, envelope)
    origin = variant in ("origin-builder", "merged-builder", "placed-builder")
    po, pool, _, queue, template, instance, descriptor, path = helper("compat_batch_adapters.py")["committed"](
        store, load, "origin" if origin else "staged")
    batch = core.json.loads(path.read_bytes())
    if variant == "stage-builder":
        return po.build_stage_manifest(batch, str(store / "outputs"))
    refs = [po.origin_batch_ref(batch)]
    if variant == "origin-builder":
        return po.origin_batch_manifest(queue.root, refs)
    if variant == "merged-builder":
        batches = po.origin_batch_manifest(queue.root, refs)
        static = factory(core, "phased", False)
        static["mount_prefix"] = str(store)
        static["entries"][0]["path"] = str(store / "static.bin")
        return load("prismabuild.action_edges").merged_manifest(static, batches)
    static = factory(core, "read-plan", True)
    static["mount_prefix"] = str(store)
    static["entries"][0]["path"] = str(store / "static.bin")
    static["read_plan"]["phases"].append({"name": "origin", "entry_indices": [], "bytes": 0, "cumulative_bytes": 8})
    placed = po.place_origin_batches(queue.root, static, [{"phase": "origin", "refs": refs}])
    po.verify_placed_origin_batches(queue.root, placed)
    return placed
