"""Queue funding and diagnostic writer scenarios in private stores."""
from __future__ import annotations

import base64
import json
import time
import uuid
from pathlib import Path
import runpy

M, C, G, D = "a" * 64, "c" * 64, "7" * 32, "9" * 64


def operate(request, store, load):
    family, variant = request["case"].split("/", 1)
    if family == "q-output-funding" and variant in ("stage-intent", "fund-direct"):
        return runpy.run_path(str(Path(__file__).with_name("compat_builder_adapters.py")))["output_funding"](request, store, load)
    pool = load("prismabuild.pool")
    queue = pool.PoolQueue(store / "queue")
    write = request["op"] == "write"
    if family in ("q-funding", "q-output-funding"):
        output = family == "q-output-funding"
        path = queue.records.funding_output_path(M, "stage:compat") if output else queue.records.funding_path(M, "stage:compat")
        reader = queue.funding.read_output if output else queue.funding.read_window
        writer = queue.funding.write_output if output else queue.funding.write_window
        if write:
            value = {"schema": load("prismabuild.funding").TIER_FUNDING_OUTPUT_SCHEMA_V1 if output else load("prismabuild.funding").TIER_FUNDING_SCHEMA_V1,
                     "tier_id": "stage:compat", "kind": "stage_gib", "mover_action_key": M,
                     "range_start_bytes": 0, "range_end_bytes": 8, "tokens": ["stage_gib-000000"],
                     "generation": G, "state": "reserved", "unix": 100.0, "published_unix": 50.0}
            if output:
                value.update(owner_action_key=C, owner_nonce=G, owner_scope_id="scope-1",
                             owner_published_unix=50.0, template_id="compat", template_sha256=D,
                             batch_id="batch0", manifest_digest=D)
            else:
                value.update(consumer_action_key=C, plan_sha256=D)
            if not output and variant in ("prelaunch-birth", "prelaunch-binding", "reserve-fence"):
                uuid.uuid4 = lambda: uuid.UUID(G)
                if variant == "reserve-fence":
                    queue.tier("stage:compat").mint({"stage_gib": 1})
                    assert queue.funding.reserve_window("stage:compat", C, value, 1).bound
                else:
                    group = load("prismabuild.prelaunch_group")
                    arguments = dict(tier_id="stage:compat", consumer=C, digest=D, mover=M,
                                     start=0, end=8, kind="stage_gib", published=50.0, tokens=value["tokens"])
                    if variant == "prelaunch-birth":
                        assert queue.funding._chunk_birth_record(**arguments) == G
                    else:
                        writer(queue.funding._chunk_fresh_binding(**arguments))
            else:
                writer(value)
            steps = {"reserved": [], "transferring": ["transferring"],
                     "consumed": ["transferring", "consumed"], "released": ["transferring", "released"]}.get(variant, [])
            for state in steps:
                value["state"] = state
                writer(value, expect_generation=G)
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(base64.b64decode(request["raw"]))
            request["_consumer_boundary"]()
            value = reader(M, "stage:compat")
            expected = variant if variant in ("reserved", "transferring", "consumed", "released") else "reserved"
            assert value and value["state"] == expected, "funding reader refused binding"
            return value
    elif family in ("q-denials", "q-denial-transitions"):
        path = (queue.denial_transitions_path(M) if family == "q-denial-transitions" else
                pool.cpu_admission.local_state_base(queue.ledger().base) / pool.CLAIM_DENIALS)
        if write:
            tick = [100.0]
            time.time = lambda: tick[0]
            item = {"action_key": M, "published_unix": 50.0, "attempts": 0}
            queue.record_denial(item, "tokens", {"note": "caf\u00e9 \u03bb"})
            if variant in ("repeat", "damped"):
                tick[0] = 101.0 if variant == "damped" else 100.0
                queue.record_denial(item, "headroom", {})
                tick[0] = 102.0 if variant == "damped" else 100.0
                queue.record_denial(item, "tokens", {"note": "caf\u00e9 \u03bb"})
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(base64.b64decode(request["raw"]))
            request["_consumer_boundary"]()
            if family == "q-denials":
                value = queue.latest_denials({M})
                assert value.get(M) and value[M][0]["reason"] == "tokens", "latest diagnostic lost the denial"
            else:
                value = queue.denial_transitions(M, published_unix=50.0)
                assert value and value[0]["reason"] == "tokens", "transition reader lost the denial"
                if variant == "damped":
                    assert value[0]["count"] == 2 and value[0]["last_unix"] == 102.0, "repeat was not damped"
            return value
    else:
        raise ValueError(f"unknown queue adapter: {family}")
    return {"raw": base64.b64encode(path.read_bytes()).decode()}
