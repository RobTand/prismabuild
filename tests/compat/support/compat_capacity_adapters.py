"""Image and GPU snapshot contracts with private supplied observations only."""
from __future__ import annotations

import base64
import json
import uuid


def gpu_snapshot(store, load, variant):
    module = load("prismabuild.gpu_capacity")
    proc = store / "proc"
    (proc / "pressure").mkdir(parents=True)
    (proc / "pressure/cpu").write_text("some avg10=0.0 total=0\nfull avg10=0.0 total=0\n")
    scope = "prismabuild-job" + "a" * 32 + ".slice"
    records = {scope: {"action_key": "a" * 64, "nonce": "b" * 32,
                      "cgroup_identity": [1, 2], "memory_max_bytes": 4096}}
    job = {"scope_id": scope, "cgroup_identity": [1, 2], "budget_bytes": 4096,
           "host_bytes": 1024, "gpu_reported_bytes": 512, "gpu_lower_bound_bytes": 512,
           "lower_bound_bytes": 1024, "upper_bound_bytes": 1536, "complete": variant != "incomplete", "processes": [], "errors": []}
    sample = {"sampled_unix": 100.0, "jobs": [job], "gpu_query_complete": True, "host_total_bytes": 16384,
              "host_available_bytes": 8192, "psi_some_avg10": 0.0, "psi_full_avg10": 0.0, "foreign_processes": [], "errors": []}
    shared = variant == "unified"
    device = {"uuid": "GPU-compat", "name": "NVIDIA GB10" if shared else "NVIDIA GeForce RTX 4090",
              "sampled_unix": 100.0, "memory_domain": "shared_system" if shared else "discrete",
              "memory_total_bytes": None if shared else 16 * 1024 * 1024,
              "memory_free_bytes": None if shared else 12 * 1024 * 1024,
              "memory_used_bytes": None if shared else 4 * 1024 * 1024,
              "complete": variant != "incomplete", "limited": False, "telemetry_class": "memory_only"}
    original = module.uuid.uuid4
    module.uuid.uuid4 = lambda: uuid.UUID("7" * 32)
    try:
        return module.collect(sample, records, proc_root=proc, device_readings=([device], []))
    finally:
        module.uuid.uuid4 = original


def operate(request, store, load):
    family, variant = request["case"].split("/", 1)
    writing = request["op"] == "write"
    core = load("prismabuild.core")
    if family == "gpu-capacity":
        if writing:
            value = gpu_snapshot(store, load, variant)
        else:
            window = load("prismabuild.box_window").DiscreteFramebufferWindow("a" * 64, "b" * 32,
                "prismabuild-job" + "a" * 32 + ".slice", start_unix=99.0)
            request["_consumer_boundary"]()
            observed = window.observe(json.loads(base64.b64decode(request["raw"])), now=100.0)
            assert observed == (variant == "discrete"), "actual framebuffer consumer changed the sample disposition"
            return {"framebuffer_observed": observed}
    else:
        images = load("prismabuild.container_images")
        content = "content:sha256:" + "a" * 64
        declaration = {"schema": images.CLASS_REQUIREMENTS_SCHEMA, "classes": {
            "compat": {"store_root": "/compat/docker", "images": {} if variant == "empty" else {"fixture:v1": content}}}}
        if family == "image-requirements":
            if writing:
                value = images.ClassImagePolicy(declaration, klass="compat")._requirements
            else:
                request["_consumer_boundary"]()
                result = images.normalize_class_image_requirements(json.loads(base64.b64decode(request["raw"])))
                assert "compat" in result
                return {"classes": sorted(result), "store": result["compat"].store_root}
        elif writing:
            inventory = {"schema": images.INVENTORY_SCHEMA, "observed_unix": 100.0, "entries": [content],
                         "store_root": "/compat/docker", "image_contents": {"fixture:v1": content} if variant != "missing" else {}}
            if variant == "bare-satisfied":
                value = images.class_image_verdict(declaration, "compat", inventory, now=100.0)
            else:
                value = images.ClassImagePolicy(declaration, klass="compat").evaluate(
                    None if variant == "unknown" else inventory, now=100.0)
        else:
            request["_consumer_boundary"]()
            eligible = images.offer_class_image_eligible(json.loads(base64.b64decode(request["raw"])), now=100.0)
            assert eligible == (variant == "satisfied"), "actual image offer consumer changed container eligibility"
            return {"container_work_eligible": eligible}
    return {"raw": base64.b64encode(core._canonical_file_bytes(value)).decode(),
            "fixture_facts": ["supplied private inventory and device observations; no Docker, GPU or broker probe"]}
