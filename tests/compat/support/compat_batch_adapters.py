"""Real private batch commits with supplied claim and deterministic stat facts."""
from __future__ import annotations

import base64
import hashlib
import json
from pathlib import Path
import runpy

C, M, G = "c" * 64, "a" * 64, "7" * 32
TIER = "prismabuild-stage:compat"


def context(store, load, variant):
    bl = load("prismabuild.batch_ledger")
    oc = load("prismabuild.output_contract")
    po, pool, core = (load("prismabuild." + name) for name in ("produced_output", "pool", "core"))
    origin = variant.startswith("origin") or variant in ("retain", "consumed")
    template = {"schema": oc.TEMPLATE_SCHEMA_V1, "version": 1, "template_id": "compat-batch",
        "output_prefix": str(store / "outputs"),
        "slots": {"payload": {"class": "payload"}, "checkpoint": {"class": "checkpoint"}, "temp": {"class": "temp"}},
        "durable_maxima": {"payload_max_bytes": 8, "checkpoint_max_bytes": 8, "temp_max_bytes": 8},
        "working_demands": {TIER: {"minimum_gib": 0, "window_gib": 0 if origin else 1}}, "permitted_tiers": [TIER]}
    if origin:
        template["write_only"] = True
    if variant == "rate":
        template["export_rate_family"] = "compat-rate"
    template = oc.validate_template(template)
    queue = pool.PoolQueue(store / "queue")
    queue.records.ensure_layout()
    queue.tier(TIER).mint({"stage_gib": 2})
    path = queue.publish(action_key=C, cas_root=store / "cas", checkout_root="/compat/checkout",
        worker_script="/compat/worker.py", resources={"cpu": 1, "mem_gb": 1, **oc.owner_demand_terms(template)},
        produced_output_template=template)
    record = json.loads(path.read_bytes())
    scope = po._broker_scope_id(C, G)
    record.update(claimed_by="compat-owner", claimed_host="compat-host", claimed_unix=100.0,
                  resource_scope={"action_key": C, "nonce": G, "scope_id": scope})
    path.unlink()
    queue.records.replace_record_at(queue.records.item_path(pool.CLAIMED, C), record)
    po.declare_template(queue.root, template)
    instance = po.bind_instance(queue, template, owner_action_key=C, claim_snapshot=record,
        env={"PRISMABUILD_ACTION_KEY": C, "PRISMABUILD_ACTION_NONCE": G, "PRISMABUILD_ACTION_SCOPE": scope})
    bl.declare_instance(queue.root, instance)
    assert bl.BatchLedger.admit(queue, instance, template)["ok"]
    return po, pool, core, queue, template, instance




def committed(store, load, variant):
    bl = load("prismabuild.batch_ledger")
    oc = load("prismabuild.output_contract")
    po, pool, core, queue, template, instance = context(store, load, variant)
    cls = variant if variant in ("checkpoint", "temp") else "payload"
    path = Path(template["output_prefix"]) / "payload.bin"
    classes = {name: 8 if name == cls else 0 for name in ("payload", "checkpoint", "temp")}
    assert bl.BatchLedger.reserve(queue, instance, template, batch_id="batch", tier=TIER,
                               class_bytes=classes, paths=[str(path)])["ok"]
    path.parent.mkdir(parents=True)
    path.write_bytes(b"contents")
    descriptor = oc.validate_descriptor({"schema": oc.DESCRIPTOR_SCHEMA_V2, "slot": cls, "artifact_class": cls,
        "path": str(path), "bytes": 8, "sha256": None if variant == "null" else hashlib.sha256(b"contents").hexdigest(),
        "producer_generation": G, "owner_action_key": C, "owner_attempt": dict(instance["owner_attempt"])}, template, instance)
    physical_identity = bl._portable_identity_of
    # These are writer input facts. Restore the real function before any
    # retained consumer runs; no physical file identity is claimed here.
    bl._portable_identity_of = lambda info: {"ino": 2, "size": info.st_size, "mtime_ns": 3, "ctime_ns": 4}
    try:
        def commit():
            if template.get("write_only") or variant == "readback-origin":
                landed = {str(path): bl._portable_identity_of(path.stat())} if variant == "readback-origin" else None
                return bl.BatchLedger.commit_origin(queue, instance, template, [descriptor], batch_id="batch", landed=landed,
                    lifetime="consumed" if variant in ("origin-consumed", "consumed") else "retain")
            return po.commit_batch(queue, instance, template, [descriptor], batch_id="batch", tier=TIER, mover_key=M)
        result = commit()
        assert result.get("ok"), result
        if variant.endswith("replay"):
            assert commit().get("duplicate") is True
    finally:
        bl._portable_identity_of = physical_identity
    path = queue.root / "residency" / oc.OUTPUT_BATCHES_SUBDIR / oc.instance_namespace(instance) / "batch.json"
    return po, pool, core, queue, template, instance, descriptor, path


def extract(raw, key, *, array=False):
    text = raw.decode()
    needle = json.dumps(key) + ":"
    assert text.count(needle) == 1
    start = text.index(needle) + len(needle)
    while text[start].isspace():
        start += 1
    if array:
        assert text[start] == "["
        start += 1
    _, end = json.JSONDecoder().raw_decode(text, start)
    return text[start:end].encode()


def operate(request, store, load):
    bl = load("prismabuild.batch_ledger")
    oc = load("prismabuild.output_contract")
    family, variant = request["case"].split("/", 1)
    write = request["op"] == "write"
    if family == "out-ref":
        po, pool, core, queue, template, instance = context(store, load, variant)
        path = queue.records.item_path(pool.CLAIMED, C)
        if write:
            raw = extract(path.read_bytes(), "produced_output")
        else:
            record = json.loads(path.read_bytes())
            record["produced_output"] = json.loads(base64.b64decode(request["raw"]))
            queue.records.replace_record_at(path, record)
            request["_consumer_boundary"]()
            return po.declared_template(queue, C)
    else:
        po, pool, core, queue, template, instance, descriptor, path = committed(store, load, variant)
        if family == "out-batch":
            if write:
                raw = path.read_bytes()
            else:
                path.chmod(0o600)
                path.write_bytes(base64.b64decode(request["raw"]))
                request["_consumer_boundary"]()
                result = bl.BatchLedger.record(queue, instance, template, batch_id="batch")
                assert result["record"]["entry_count"] == 1
                return result
        elif family == "out-descriptor":
            if write:
                raw = extract(path.read_bytes(), "entries", array=True)
            else:
                request["_consumer_boundary"]()
                return oc.validate_descriptor(json.loads(base64.b64decode(request["raw"])), template, instance)
        elif family == "out-batch-ref":
            if not write:
                request["_consumer_boundary"]()
                return queue.validate_produced_output_batch(json.loads(base64.b64decode(request["raw"])), {"stage_gib@" + TIER: 1})
            value = queue.build_produced_output_batch_ref(instance=instance, template=template,
                batch_id="batch", descriptors=[descriptor], tier_id=TIER)
            factory = runpy.run_path(str(Path(__file__).with_name("compat_core_adapters.py")))["action_body"]
            body = factory(core, store)
            body["params"]["produced_output_batch"] = value
            action = core.seal_action(body)
            path = core.PrismaBuildCAS(store / "ref-cas").publish_action_request(action)
            raw = extract(path.read_bytes(), "produced_output_batch")
        else:
            if not write:
                request["_consumer_boundary"]()
                return oc._checked_origin_ref(json.loads(base64.b64decode(request["raw"])))
            value = oc.origin_batch_ref(instance, batch_id="batch", manifest_digest=oc.output_manifest_sha256([descriptor]))
            codec = runpy.run_path(str(Path(__file__).with_name("compat_core_adapters.py")))["manifest_owner"](core, load)
            body = {"schema": codec.DATA_MANIFEST_SCHEMA_V1, "produced_by": {}, "annotations": {"produced_output_batches": [value]},
                "mount_prefix": str(store / "outputs"), "entries": [{"path": descriptor["path"], "offset": 0,
                    "bytes": 8, "sha256": descriptor["sha256"]}], "entry_count": 1, "total_bytes": 8}
            cas = core.PrismaBuildCAS(store / "manifest-cas")
            entry, _ = cas.ingest_bytes(core._canonical_file_bytes(codec.validate_data_manifest(body)), input_id="data_manifest")
            raw = extract(cas.input_path(entry).read_bytes(), "produced_output_batches", array=True)
    return {"raw": base64.b64encode(raw).decode(),
            "fixture_facts": ["private supplied admitted claim", "deterministic portable identity during writer only"]}
