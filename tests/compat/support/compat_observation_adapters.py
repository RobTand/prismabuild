"""Private production pacing, refusal, tick and idle-history scenarios."""
from __future__ import annotations

import base64
import json
import time
from pathlib import Path


def operate(request, store, load):
    family, variant = request["case"].split("/", 1)
    writing = request["op"] == "write"
    if family == "idle-baseline":
        cpu = load("prismabuild.adaptive_cpu")
        state = None
        if writing:
            for stamp in (90.0, 95.0):
                _, state, _ = cpu.idle_judgement_with_reference(state,
                    {"sampled_unix": stamp, "interval_s": 1., "busy_cpus": 0.25, "psi_some": 0.},
                    holders=[], identity="compat", prior_rule=lambda current: False)
            return {"raw": base64.b64encode(json.dumps(state, sort_keys=True).encode()).decode()}
        state = json.loads(base64.b64decode(request["raw"]))
        request["_consumer_boundary"]()
        verdict, _, reference = cpu.idle_judgement_with_reference(state,
            {"sampled_unix": 100., "interval_s": 1., "busy_cpus": 0.5 if variant == "excursion" else 0.25, "psi_some": 0.},
            holders=[], identity="compat", prior_rule=lambda current: False)
        assert reference and len(reference) == 2
        assert verdict["basis"] == "measured" and verdict["exceeds"] == (variant == "excursion")
        return verdict
    spool, pool = load("prismabuild.produced_spool"), load("prismabuild.pool")
    queue = pool.PoolQueue(store / "queue")
    if family == "spool-pacing":
        if writing:
            monotonic = time.monotonic
            time.monotonic = lambda: 99.
            try:
                pacer = spool.ExportPacer(1)
                time.monotonic = lambda: 100.
                if variant == "written":
                    with (store / "payload").open("wb") as handle:
                        pacer.wrote(8, handle)
                value = pacer.record("stage:compat")
            finally:
                time.monotonic = monotonic
        else:
            value = json.loads(base64.b64decode(request["raw"]))
            request["_consumer_boundary"]()
            spool._check_pacing(value)
            return {"bytes": value["bytes"], "rate": value["rate_mb_s"]}
    elif family == "spool-refusal":
        if writing:
            group = store / "group"
            group.mkdir()
            spool._write(group / "receipt.json", {"observed": "private fixture"})
            record = {"batch_id": "batch", "export_key": "e" * 64, "manifest_sha256": "a" * 64}
            evidence = {"code": "identity_changed", "entry_index": 0}
            result = spool._file_refusal(queue, group, "c" * 64, record, evidence, where="poll")
            if variant == "replay":
                assert spool._file_refusal(queue, group, "c" * 64, record, evidence, where="poll") == result
            return {"raw": base64.b64encode(Path(result["evidence_record"]).read_bytes()).decode()}
        path = queue.root / spool.REFUSALS_SUBDIR / ("c" * 64) / "compat.json"
        path.parent.mkdir(parents=True)
        path.write_bytes(base64.b64decode(request["raw"]))
        request["_consumer_boundary"]()
        result = spool.refusal_records(queue, owner="c" * 64)
        assert len(result) == 1 and result[0]["code"] == "identity_changed"
        return result[0]
    else:
        records = queue.root / spool.RETIREMENTS_SUBDIR
        if writing:
            if variant == "empty":
                monotonic = time.monotonic
                time.monotonic = lambda: 100.
                try:
                    spool.retirement_tick(queue, store / "cas", host="compat-host", roots=[])
                finally:
                    time.monotonic = monotonic
            else:
                spool.record_failed_tick(queue, "compat-host", OSError("fixture failure"))
            return {"raw": base64.b64encode((records / "compat-host.tick.json").read_bytes()).decode()}
        records.mkdir(parents=True)
        (records / "compat-host.tick.json").write_bytes(base64.b64decode(request["raw"]))
        request["_consumer_boundary"]()
        result = spool.retirement_records(queue)["ticks"]["compat-host"]
        assert (result.get("namespaces") == 0 if variant == "empty" else result["failed"] == "OSError: fixture failure")
        return result
    return {"raw": base64.b64encode(json.dumps(value, sort_keys=True).encode()).decode()}
