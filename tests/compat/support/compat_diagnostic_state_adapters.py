"""Actual host offers, durable census diagnostics and pricing-log row consumers."""
from __future__ import annotations

import base64

M = "a" * 64


def operate(request, store, load):
    family, variant = request["case"].split("/", 1)
    pool = load("prismabuild.pool")
    queue = pool.PoolQueue(store / "queue")
    queue.ensure_layout()
    writing = request["op"] == "write"
    if family == "host-spool-offer":
        scratch = load("prismabuild.local_scratch")
        scratch.OFFER_ROOT = store / "offers"
        base = queue.ledger().base
        path = scratch.spool_offer_path(base)
        expected = 0 if variant == "zero" else 2
        if writing:
            scratch.write_spool_offer(base, expected, "supplied private capacity observation")
            if variant == "replay":
                scratch.write_spool_offer(base, expected, "supplied private capacity observation")
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(base64.b64decode(request["raw"]))
            request["_consumer_boundary"]()
            value = scratch.read_spool_offer(base)
            assert value == expected
            return {"spool_gb": value}
    elif family == "q-census-report":
        ledger = queue.ledger()
        path = ledger.census_report_path
        facts = dict(unreadable=[{"holder": M, "errno": 5}], asked=["cpu"], retired={"cpu": 1})
        count = 2 if variant == "repeat" else 1
        if writing:
            for _ in range(count):
                ledger.report_census(**facts)
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(base64.b64decode(request["raw"]))
            request["_consumer_boundary"]()
            value = ledger.report_census(**facts)
            assert value["cycles"] == count + 1 and value["first_unix"] == 100.0
            return value
    else:
        path = queue.move_pricing_log_path()
        if writing:
            queue.record_move(M, {"bytes_staged": 8, "seconds": 1.0, "complete": True, "unix": 100.0})
            if variant == "read-repair":
                path.unlink()
                assert len(queue.move_records()) == 1
            raw = path.read_bytes().splitlines(keepends=True)[-1]
            return {"raw": base64.b64encode(raw).decode()}
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(base64.b64decode(request["raw"]))
        queue.move_path(M).write_bytes(b"unreadable private fallback receipt")
        request["_consumer_boundary"]()
        values = queue.move_records()
        assert len(values) == 1 and values[0]["bytes_staged"] == 8
        return values
    return {"raw": base64.b64encode(path.read_bytes()).decode(),
            "fixture_facts": ["private diagnostic files and supplied capacity/census facts; no physical capacity or unreadability claim"]}
