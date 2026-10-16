"""Fixed caller inputs, validated by real readers; these have no runtime writer."""
from __future__ import annotations

import base64
import json


def operate(request, store, load):
    family, variant = request["case"].split("/", 1)
    writing = request["op"] == "write"
    if family == "input-test-capabilities":
        module = load("pbtest_capabilities")
        value = {"schema": module.SCHEMA, "capabilities": {} if variant == "empty" else {"compat": {"tags": ["cpu"]}}}

        def reader(raw):
            path = store / "capabilities.json"
            path.write_bytes(raw)
            return module.load_config(path)
    elif family == "input-d38-preflight":
        gate = load("d38_gate")
        value = {"schema": gate.RECEIPT_SCHEMA, "job_identity_hash": "a" * 64,
                 "preflight_action_key": "b" * 64, "namespace": "sha256:" + "c" * 64,
                 "image_digest": [], "host_class": "x86", "result": "fail" if variant == "fail" else "pass",
                 "created_at": "2026-10-01T00:00:00+00:00",
                 "expires": "2026-10-02T00:00:00+00:00" if variant == "expires" else None}
        reader = gate._parse_receipt
    elif family == "input-d38-plan":
        gate, core = load("d38_gate"), load("prismabuild.core")
        value = {"schema": gate.PLAN_SCHEMA, "target": "compat-target", "images": []}
        cas = core.PrismaBuildCAS(store / "cas")

        def reader(raw):
            entry, _ = cas.ingest_bytes(raw, input_id=gate.PLAN_INPUT_ID)
            preflight = {"params": {gate.PRODUCER_PARAM: {"producer": gate.PRODUCER_ID,
                         "plan_sha256": entry["sha256"]}, "demand": {"cpu": 1}}, "inputs": [entry]}
            return gate.load_plan(cas, preflight)
    elif family == "input-scratch-io":
        scratch = load("prismabuild.local_scratch")
        value = {"schema": scratch.IO_SCHEMA, "write_bytes": 0 if variant == "read" else 8,
                 "read_bytes": 0 if variant == "write" else 8,
                 "profile_contract": scratch.PROFILE_CONTRACT, "max_profile_age_s": 60.0}

        def reader(raw):
            result = scratch.io_intent({scratch.PAIRS_ENV: "COMPAT_ROOT:COMPAT_MAX", "COMPAT_ROOT": "/compat/scratch",
                                       "COMPAT_MAX": "16", scratch.IO_ENV: raw.decode()})
            assert result and result["root"] == "/compat/scratch"
            return result
    else:
        raise ValueError(f"unknown caller input: {family}")
    raw = json.dumps(value, sort_keys=True, separators=(",", ":")).encode() + b"\n" if writing else base64.b64decode(request["raw"])
    if not writing:
        request["_consumer_boundary"]()
    result = reader(raw)
    if writing:
        return {"raw": base64.b64encode(raw).decode(), "origin": "deterministic caller input; no runtime writer"}
    return result
