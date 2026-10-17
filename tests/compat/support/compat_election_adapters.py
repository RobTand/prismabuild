"""Election and retry-stop codecs with explicitly supplied admission facts."""
from __future__ import annotations

import base64
from contextlib import contextmanager
import json
from pathlib import Path
import runpy
from types import SimpleNamespace

C, M, G = "c" * 64, "a" * 64, "7" * 32


def operate(request, store, load):
    family, variant = request["case"].split("/", 1)
    pool = load("prismabuild.pool")
    queue = pool.PoolQueue(store / "queue")
    writing = request["op"] == "write"
    if family == "measurement-election":
        election = load("prismabuild._measurement_reservation")
        item = {"action_key": C, "published_unix": 50., "priority": 10}
        path = queue.passes_path(C)
        if writing:
            census = {"elections": {}, "gang_elections": {}, "measurements": {C: [item]},
                      "opportunities": {M: {"host": "compat-host", "claimed_unix": 99.,
                                            "requested": 8. if variant == "bounded" else None}}}
            @contextmanager
            def supplied_census(*args):
                yield census
            original = election.locked_census
            election.locked_census = supplied_census
            holder = SimpleNamespace(name=M, kind=election.HolderKind.OTHER_HOLDER) if hasattr(election, "HolderKind") else None
            ledger = SimpleNamespace(base=Path("/compat/compat-host"), held_keys=lambda: [M],
                holder_census=lambda: SimpleNamespace(non_private_holders=[holder]))
            try:
                value = election.elect(queue, ledger, None, item,
                    {"withhold": True, "why": "draining_for_measurement", "holders": [M]},
                    sampled_unix=100., gpu_sample=None)
            finally:
                election.locked_census = original
            assert value and value["opportunity_unix"] == (107. if variant == "bounded" else 100.)
        else:
            request["_consumer_boundary"]()
            result = election.selection({election.FIELD: json.loads(base64.b64decode(request["raw"]))})
            assert result and result["action_key"] == C
            return result
    else:
        fixture = runpy.run_path(str(Path(__file__).with_name("compat_queue_adapters.py")))["operate"]
        fixture({"case": "q-output-funding/consumed", "op": "write"}, store, load)
        funding = queue.funding.read_output(M, "stage:compat")
        ref = {key: funding[key] for key in queue._OUTPUT_BATCH_IDENTITY_FIELDS}
        ref["schema"] = pool.PRODUCED_OUTPUT_BATCH_REF_SCHEMA_V1
        binding = {"variant": "output", "generation": G, "kinds": {"stage_gib": 1}, "tokens": funding["tokens"]}
        item = {"action_key": M, "published_unix": 50., "cas_root": str(store / "cas"),
                "produced_output_batch": ref, "tier_funding": {"stage:compat": binding},
                "residency": {"tier_id": "stage:compat", "manifest_sha256": funding["manifest_digest"],
                              "range_start_bytes": 0, "range_end_bytes": 8}}
        if writing:
            value = queue._spent_output_retry_stop(item)
            assert value
        else:
            request["_consumer_boundary"]()
            value = json.loads(base64.b64decode(request["raw"]))
            queue._validate_output_retry_stop(value, item)
            return {"reason": value["reason"], "tokens": value["claim_funding"]["tokens"]}
    return {"raw": base64.b64encode(json.dumps(value, sort_keys=True).encode()).decode(),
            "fixture_facts": ["private supplied census/ledger or legacy request absence; no admission authority"]}
