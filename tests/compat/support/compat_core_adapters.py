"""Actual core constructors, CAS input writers and request consumers."""
from __future__ import annotations

import base64
import json
from pathlib import Path
import runpy


def code_files(store, variant):
    (store / "task_code.py").write_bytes(b"print('compat')\n")
    names = ["task_code.py"]
    if variant == "multiple":
        (store / "lambda.py").write_bytes(b"")
        names.append("lambda.py")
    return names


def action_body(core, store):
    names = code_files(store, "single")
    return {"schema": core.ACTION_SCHEMA_V2,
            "task": {"definition_id": "compat/record", "definition_version": "v1",
                     "task_class": "generation", "determinism": "deterministic",
                     "artifact_family": "generic", "artifact_kind": "generic",
                     "argv": ["/bin/sh", "-c", "printf compat > result.bin"],
                     "working_directory": ".", "result_path": "result.bin"},
            "inputs": [], "code_closure": core.build_code_closure(store, names),
            "params": {"unicode": "caf\u00e9 \u03bb"},
            "environment": {"variables": {}, "toolchain": {}},
            "execution_scope": {"portability": "portable", "platform_key": None, "host_class": None}}


def manifest_owner(core, load):
    return core if hasattr(core, "validate_data_manifest") else load("prismabuild.data_manifest")


def manifest(codec, variant, v2):
    value = {"schema": codec.DATA_MANIFEST_SCHEMA_V2 if v2 else codec.DATA_MANIFEST_SCHEMA_V1,
             "produced_by": {}, "annotations": {}, "mount_prefix": "/compat/input",
             "entries": [{"path": "/compat/input/weights", "offset": 0, "bytes": 8, "sha256": None}],
             "entry_count": 1, "total_bytes": 8}
    phases = [{"name": "copy", "bytes": 8, "cumulative_bytes": 8}]
    if v2:
        phases[0]["entry_indices"] = [0]
        value["read_plan"] = {"phases": phases, "read_bytes": 8}
    elif variant == "phased":
        value["annotations"]["phases"] = phases
    return value


def operate(request, store, load):
    family, variant = request["case"].split("/", 1)
    core = load("prismabuild.core")
    cas = core.PrismaBuildCAS(store / "cas")
    write = request["op"] == "write"
    if family in ("core-progress-policy", "core-contention", "core-awaited"):
        body = action_body(core, store)
        policy = {"schema": core.PROGRESS_POLICY_SCHEMA_V1,
                  "phases": [{"name": "copy", "grace_s": 10.0}]}
        body["params"][core.PROGRESS_PARAM] = policy
        if family == "core-progress-policy":
            key = core.PROGRESS_PARAM
            if variant.startswith("cli-"):
                value = load("pbrun").parse_progress_phases(["copy=10", "publish=5"], cycle=variant == "cli-cycle")
            elif variant == "mover":
                value, _ = load("prismabuild.movement_actions").mover_progress_policy(
                    [8, 16], landing_bytes_per_s=8.0, report_latency_s=2.0)
            elif variant == "egress":
                value, _ = load("prismabuild.movement_actions").egress_progress_policy(
                    [8, 16], price={"basis": "egress", "census_s": 1.0, "unlink_s_per_entry": 0.5, "settle_s": 1.0},
                    report_latency_s=2.0)
            else:
                value = core.validate_progress_policy(policy)
            reader = core.action_progress_policy
        elif family == "core-contention":
            key = core.POOL_CONTENTION_PARAM
            value = load("prismabuild.movement_actions").pool_contention_spec(
                members=["sdb", "sda", "sda"], stage_root="/compat/stage", priced_bytes_per_s=8.0)
            if variant == "validated":
                value = core.validate_pool_contention(value)
            reader = core.action_pool_contention
        else:
            key = core.AWAITED_BATCH_PARAM
            value = load("pbrun").parse_awaited_batch("a" * 64 + ":" + "b" * 64 + ":/compat/controller")
            if variant == "validated":
                value = core.validate_awaited_batch(value)
            reader = core.action_awaited_batch
        body["params"][key] = value if write else json.loads(base64.b64decode(request["raw"]))
        if not write:
            request["_consumer_boundary"]()
        action = core.seal_action(body)
        if not write:
            return reader(action)
        path = cas.publish_action_request(action)
        enclosing = path.read_bytes()
        text = enclosing.decode()
        needle = json.dumps(key) + ":"
        assert text.count(needle) == 1
        start = text.index(needle) + len(needle)
        actual, end = json.JSONDecoder().raw_decode(text, start)
        assert actual == value
        raw = text[start:end].encode()
        return {"raw": base64.b64encode(raw).decode(),
                "enclosing_raw": base64.b64encode(enclosing).decode(), "enclosing_path": str(path)}
    if family == "core-action":
        if write:
            value = (runpy.run_path(str(Path(__file__).with_name("compat_builder_adapters.py")))["action"](
                core, store, load, variant) if variant.endswith("builder") else core.seal_action(action_body(core, store)))
            path = cas.publish_action_request(value)
        else:
            raw = base64.b64decode(request["raw"])
            value = json.loads(raw)
            key = value["action_key"]
            path = cas.root / "requests" / key[:2] / (key + ".json")
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(raw)
            request["_consumer_boundary"]()
            result = cas.read_action_request(key)
            assert result and result["task"]["definition_id"] == (
                "fleet/first-dispatch" if variant == "smoke-builder" else "compat/record"), "request consumer refused the action"
            if variant != "smoke-builder":
                assert result["params"]["unicode"] == "caf\u00e9 \u03bb"
            return result
    else:
        if family == "core-closure":
            names = code_files(store, variant)
            if variant == "stamp-builder":
                (store / "stamp.py").write_text("compat stamp\n")
                value = load("pbrun").build_stamp_closure("stamp.py", "compat stamp\n") if write else None
            else:
                value = core.build_code_closure(store, names) if write else None
            reader = lambda value: core.verify_code_closure(value, store)
        elif family in ("core-manifest-v1", "core-manifest-v2"):
            codec = manifest_owner(core, load)
            if write:
                value = (runpy.run_path(str(Path(__file__).with_name("compat_builder_adapters.py")))["manifest"](
                    core, store, load, variant) if variant.endswith("builder") else
                    codec.validate_data_manifest(manifest(codec, variant, family.endswith("v2"))))
            else:
                value = None
            reader = codec.validate_data_manifest
        else:
            raise ValueError(f"unknown core adapter: {family}")
        if not write:
            request["_consumer_boundary"]()
            return reader(json.loads(base64.b64decode(request["raw"])))
        entry, _ = cas.ingest_bytes(core._canonical_file_bytes(value), input_id="compat.record")
        path = cas.input_path(entry)
    return {"raw": base64.b64encode(path.read_bytes()).decode()}
