"""Production construction and validation for decomposition documents."""
from __future__ import annotations

import base64
import json
from pathlib import Path
import runpy
from types import SimpleNamespace


def context(dc):
    task = {"id": "task0", "payload": {"message": "caf\u00e9 \u03bb"}, "residency_key": "r",
            "estimated_seconds": 8.0, "estimate_evidence": "cas:sha256:" + "0" * 64,
            "output_id": "output0"}
    request = {"schema": dc.LOGICAL_REQUEST_SCHEMA_V1,
               "common": {"argv": ["python", "task.py", dc.TASK_BATCH_PLACEHOLDER],
                          "cwd": "/compat/checkout", "demand": {"cpu": 1, "mem_gb": 1},
                          "env": {}, "gpu_memory_gb": None, "data_manifest": None},
               "roster": {"schema": dc.LOGICAL_TASK_ROSTER_SCHEMA_V1, "tasks": [task]},
               "batch_policy": {"schema": dc.ROSTER_BATCH_POLICY_SCHEMA_V1,
                                "residencies": [{"key": "r", "setup_seconds": 0.0,
                                                  "setup_evidence": "cas:sha256:" + "0" * 64}],
                                "max_setup_fraction": 0.5, "max_estimated_wall_seconds": 100.0}}
    common = {"task": {"definition_id": "fleet/pbrun", "definition_version": "v1",
                       "task_class": "generation", "determinism": "stochastic",
                       "artifact_family": "generic", "artifact_kind": "generic", "working_directory": "."},
              "params": {"cwd": ".", "demand": {"cpu": 1, "mem_gb": 1},
                         "placement": {"required_tags": []},
                         "retry_policy": {"max_attempts": 1, "retry_safe": False},
                         "checkout_snapshot": {"input": {"id": "pbrun.checkout-snapshot",
                                                          "sha256": "a" * 64, "bytes": 4096}}},
              "inputs": [{"id": "pbrun.checkout-snapshot", "sha256": "a" * 64, "bytes": 4096}],
              "code_closure": {"schema": "prismaquant.prismabuild.code_closure.v1",
                               "files": [], "closure_sha256": "c" * 64},
              "environment": {"variables": {}, "toolchain": {}},
              "execution_scope": {"kind": "platform_keyed", "platform_key": "linux-x86_64"}}
    frozen = dc.freeze_common(request["common"], action_common=common)
    return request, frozen, dc.build_plan(request, frozen)


def operate(request, store, load):
    family, variant = request["case"].split("/", 1)
    dc = load("prismabuild.decomposition")
    declared, frozen, plan = context(dc)
    path = store / "record.json"
    if family == "dc-data-plans":
        campaign, core = load("pbcampaign"), load("prismabuild.core")
        cas = core.PrismaBuildCAS(store / "cas")
        children = [{"action_key": "c" * 64, "params": {}}]
        directory = (dc.record_dir(cas.root, plan["parent_key"]) if hasattr(dc, "record_dir")
                     else campaign.decomposition_dir(cas, plan["parent_key"]))
        path = directory / "data-plans.json"
        declared["task_data_manifest"] = {"residency_tier": None, "residency_ram": "off", "mover_readers": 1, "mover_mem_gb": 1}
        args = dict(request=declared, plan=plan, children=children, template={}, args=SimpleNamespace(), queue=None, cas=cas)
        if request["op"] == "write":
            assert campaign.frozen_child_data_plans(**args) == [None]
            if variant == "replay":
                assert campaign.frozen_child_data_plans(**args) == [None]
            return {"raw": base64.b64encode(path.read_bytes()).decode()}
        path.parent.mkdir(parents=True)
        path.write_bytes(base64.b64decode(request["raw"]))
        request["_consumer_boundary"]()
        result = campaign.frozen_child_data_plans(**args)
        assert result == [None]
        return result
    if family in ("dc-envelope", "dc-membership", "dc-group"):
        core = load("prismabuild.core")
        cas = core.PrismaBuildCAS(store / "cas")
        if family == "dc-envelope":
            declared["roster"]["tasks"][0]["payload"] = {"entries": [
                {"path": "/compat/input/weights", "offset": 0, "bytes": 8, "sha256": None}]}
            plan = dc.build_plan(declared, frozen)
            value = dc.batch_envelope(declared, plan, child_ordinal=0)
            if request["op"] == "write":
                dc.write_document(path, value)
            else:
                policy = dc.validate_task_data_policy({"schema": dc.TASK_DATA_MANIFEST_SCHEMA_V1, "payload_field": "entries",
                    "mount_prefix": "/compat/input", "residency_tier": None, "residency_ram": "off", "mover_readers": 1, "mover_mem_gb": 1})
                request["_consumer_boundary"]()
                result = dc.task_data_manifest(policy, json.loads(base64.b64decode(request["raw"])))
                assert result and result["entry_count"] == 1 and result["total_bytes"] == 8
                return result
        elif family == "dc-membership":
            value = dc.logical_batch_param(declared, plan, child_ordinal=0)
            if request["op"] == "write":
                dc.write_document(path, value)
            else:
                body = runpy.run_path(str(Path(__file__).with_name("compat_core_adapters.py")))["action_body"](core, store)
                body["params"][dc.LOGICAL_BATCH_PARAM] = json.loads(base64.b64decode(request["raw"]))
                action = core.seal_action(body)
                cas.publish_action_request(action)
                args = dict(cas_root=cas.root, parent_key=plan["parent_key"], plan_key=plan["plan_key"],
                            plan=plan, publication={"child_action_keys": [action["action_key"]]})
                request["_consumer_boundary"]()
                if hasattr(dc, "read_child_request"):
                    result, reason = dc.read_child_request(action["action_key"], **args)
                else:
                    result, reason = load("prismabuild.pool").PoolQueue(store / "queue")._queued_child_link(action["action_key"], **args)
                assert result is not None and not reason, reason
                return {"action_key": result["action_key"], "membership": result["params"][dc.LOGICAL_BATCH_PARAM]}
        else:
            manifest = {"schema": dc.CHILD_RESULT_MANIFEST_SCHEMA_V1, "parent_key": plan["parent_key"], "plan_key": plan["plan_key"],
                "child_ordinal": 0, "results": [{"task_id": "task0", "output_id": "output0", "value_sha256": "d" * 64}]}
            value = dc.verify_exact_cover(declared, plan, [manifest])
            if hasattr(dc, "_publish_group_receipt"):
                publish = dc._publish_group_receipt
                directory = dc.record_dir(cas.root, plan["parent_key"])
            else:
                campaign = load("pbcampaign")
                publish = campaign.publish_group_receipt
                directory = campaign.decomposition_dir(cas, plan["parent_key"])
            if request["op"] == "read":
                directory.mkdir(parents=True, exist_ok=True)
                (directory / "group.json").write_bytes(base64.b64decode(request["raw"]))
                request["_consumer_boundary"]()
                result = publish(value, cas=cas, parent_key=plan["parent_key"])
                assert result.read_bytes() == dc.document_bytes(value)
                return {"task_count": value["task_count"], "child_count": value["child_count"]}
            path = publish(value, cas=cas, parent_key=plan["parent_key"])
            if variant == "replay":
                assert publish(value, cas=cas, parent_key=plan["parent_key"]) == path
        return {"raw": base64.b64encode(path.read_bytes()).decode()}
    if family == "dc-roster":
        value, reader = declared["roster"], dc.validate_roster
    elif family == "dc-policy":
        value, reader = declared["batch_policy"], dc.validate_batch_policy
    elif family == "dc-request":
        value, reader = declared, dc.validate_logical_request
    elif family == "dc-frozen":
        value, reader = frozen, dc.validate_frozen_common
    elif family == "dc-plan":
        value = plan
        reader = getattr(dc, "_validate_plan", None) or dc.validate_plan
    elif family == "dc-publication":
        if request["op"] == "write":
            if hasattr(dc, "publish_children"):
                dc.publish_children(plan, cas_root=store / "cas",
                                    batch_input_digests=["b" * 64], child_action_keys=["c" * 64])
                directory = dc.record_dir(store / "cas", plan["parent_key"])
            else:
                campaign = load("pbcampaign")
                cas = SimpleNamespace(root=store / "cas")
                index = dc.publication_index(plan, batch_input_digests=["b" * 64],
                                             child_action_keys=["c" * 64])
                campaign.publish_index(index, cas=cas, parent_key=plan["parent_key"])
                directory = campaign.decomposition_dir(cas, plan["parent_key"])
            path = directory / "publication.json"
            return {"raw": base64.b64encode(path.read_bytes()).decode()}
        reader = getattr(dc, "_validate_publication_index", None) or dc.validate_publication_index
        value = None
    elif family == "dc-child-result":
        value = {"schema": dc.CHILD_RESULT_MANIFEST_SCHEMA_V1,
                 "parent_key": plan["parent_key"], "plan_key": plan["plan_key"], "child_ordinal": 0,
                 "results": [{"task_id": "task0", "output_id": "output0", "value_sha256": "d" * 64}]}
        reader = getattr(dc, "_validate_child_result_manifest", None) or dc.validate_child_result_manifest
    elif family == "dc-task-data":
        value = {"schema": dc.TASK_DATA_MANIFEST_SCHEMA_V1, "payload_field": "entries",
                 "mount_prefix": "/compat/input", "residency_tier": None, "residency_ram": "off",
                 "mover_readers": 1, "mover_mem_gb": 1}
        if variant == "reader":
            value.update(prefetch_depth_gib=1, read_mb_s=8)
        reader = dc.validate_task_data_policy
    else:
        raise ValueError(f"unknown decomposition adapter: {family}")
    if request["op"] == "write":
        value = reader(value)
        dc.write_document(path, value)
        return {"raw": base64.b64encode(path.read_bytes()).decode()}
    value = json.loads(base64.b64decode(request["raw"]))
    request["_consumer_boundary"]()
    return reader(value)
