"""Queue funding and diagnostic writer scenarios in private stores."""
from __future__ import annotations

import base64
import json
import time

M, C, G, D = "a" * 64, "c" * 64, "7" * 32, "9" * 64


def operate(request, store, load):
    family, variant = request["case"].split("/", 1)
    pool = load("prismabuild.pool")
    queue = pool.PoolQueue(store / "queue")
    write = request["op"] == "write"
    if family in ("q-funding", "q-output-funding"):
        output = family == "q-output-funding"
        path = queue.funding_output_path(M, "stage:compat") if output else queue.funding_path(M, "stage:compat")
        reader = queue.read_output_funding if output else queue.read_funding
        writer = queue.write_output_funding if output else queue.write_funding
        if write:
            value = {"schema": pool.TIER_FUNDING_OUTPUT_SCHEMA_V1 if output else pool.TIER_FUNDING_SCHEMA_V1,
                     "tier_id": "stage:compat", "kind": "stage_gib", "mover_action_key": M,
                     "range_start_bytes": 0, "range_end_bytes": 8, "tokens": ["stage_gib-000000"],
                     "generation": G, "state": "reserved", "unix": 100.0, "published_unix": 50.0}
            if output:
                value.update(owner_action_key=C, owner_nonce=G, owner_scope_id="scope-1",
                             owner_published_unix=50.0, template_id="compat", template_sha256=D,
                             batch_id="batch0", manifest_digest=D)
            else:
                value.update(consumer_action_key=C, plan_sha256=D)
            writer(value)
            steps = {"reserved": [], "transferring": ["transferring"],
                     "consumed": ["transferring", "consumed"], "released": ["transferring", "released"]}[variant]
            for state in steps:
                value["state"] = state
                writer(value, expect_generation=G)
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(base64.b64decode(request["raw"]))
            value = reader(M, "stage:compat")
            assert value and value["state"] == variant, "funding reader refused binding"
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
