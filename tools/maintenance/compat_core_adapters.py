"""Actual core constructors, CAS input writers and request consumers."""
from __future__ import annotations

import base64
import json


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


def manifest(core, variant, v2):
    value = {"schema": core.DATA_MANIFEST_SCHEMA_V2 if v2 else core.DATA_MANIFEST_SCHEMA_V1,
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
    if family == "core-action":
        if write:
            value = core.seal_action(action_body(core, store))
            path = cas.publish_action_request(value)
        else:
            raw = base64.b64decode(request["raw"])
            value = json.loads(raw)
            key = value["action_key"]
            path = cas.root / "requests" / key[:2] / (key + ".json")
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(raw)
            result = cas.read_action_request(key)
            assert result and result["params"]["unicode"] == "caf\u00e9 \u03bb", "request consumer refused the action"
            return result
    else:
        if family == "core-closure":
            names = code_files(store, variant)
            value = core.build_code_closure(store, names) if write else None
            reader = lambda value: core.verify_code_closure(value, store)
        elif family in ("core-manifest-v1", "core-manifest-v2"):
            value = core.validate_data_manifest(manifest(core, variant, family.endswith("v2"))) if write else None
            reader = core.validate_data_manifest
        else:
            raise ValueError(f"unknown core adapter: {family}")
        if not write:
            return reader(json.loads(base64.b64decode(request["raw"])))
        entry, _ = cas.ingest_bytes(core._canonical_file_bytes(value), input_id="compat.record")
        path = cas.input_path(entry)
    return {"raw": base64.b64encode(path.read_bytes()).decode()}
