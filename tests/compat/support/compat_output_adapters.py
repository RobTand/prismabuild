"""Produced-output envelope publication through real immutable owners."""
from __future__ import annotations

import base64
import json
from pathlib import Path
import runpy

C, D, G = "c" * 64, "9" * 64, "7" * 32


def template(po, variant):
    write_only = variant.startswith("write-only")
    value = {"schema": po.TEMPLATE_SCHEMA_V1, "version": 1, "template_id": "compat",
             "output_prefix": "/compat/outputs", "slots": {"slot0": {"class": "payload"}},
             "durable_maxima": {"payload_max_bytes": 8, "checkpoint_max_bytes": 0, "temp_max_bytes": 0},
             "working_demands": {"prismabuild-stage:compat": {"minimum_gib": 0, "window_gib": 0 if write_only else 1}},
             "permitted_tiers": ["prismabuild-stage:compat"]}
    if write_only:
        value["write_only"] = True
    if variant.endswith("rate"):
        value["export_rate_family"] = "compat-rate"
    return value


def operate(request, store, load):
    family, variant = request["case"].split("/", 1)
    if family in ("out-consumer", "out-release", "out-release-index"):
        return runpy.run_path(str(Path(__file__).with_name("compat_consumer_adapters.py")))["operate"](request, store, load)
    if family in ("out-batch", "out-descriptor", "out-ref", "out-batch-ref", "out-origin-ref"):
        return runpy.run_path(str(Path(__file__).with_name("compat_batch_adapters.py")))["operate"](request, store, load)
    po, core = load("prismabuild.produced_output"), load("prismabuild.core")
    value = template(po, variant)
    if family == "out-template":
        reader = po.validate_template
    elif family == "out-instance":
        value = {"schema": po.INSTANCE_SCHEMA_V1, "version": 1, "template_id": "compat",
                 "template_sha256": po.template_sha256(value), "owner_action_key": C,
                 "owner_attempt": {"nonce": G, "scope_id": "compat-scope"},
                 "attempt_source": "broker-launch", "output_prefix": "/compat/outputs", "bound_unix": 100.0}
        reader = po.validate_instance
    elif family == "out-declaration":
        reader = core.validate_produced_output_declaration
    else:
        raise ValueError(f"unknown output adapter: {family}")
    if request["op"] == "read":
        request["_consumer_boundary"]()
        result = reader(json.loads(base64.b64decode(request["raw"])))
        assert result["template_id"] == "compat", "output envelope lost template binding"
        return result
    if family == "out-template":
        path = po.declare_template(store / "queue", value)
    elif family == "out-instance":
        path = po.declare_instance(store / "queue", value)
    else:
        path = po.declare_template(store / "queue", value)
        cas = core.PrismaBuildCAS(store / "cas")
        entry, _ = cas.ingest_input(path, input_id=core.PRODUCED_OUTPUT_TEMPLATE_INPUT_ID)
        value = po.build_declaration(value, entry)
        entry, _ = cas.ingest_bytes(core._canonical_file_bytes(value), input_id="compat.declaration")
        path = cas.input_path(entry)
    return {"raw": base64.b64encode(path.read_bytes()).decode()}
