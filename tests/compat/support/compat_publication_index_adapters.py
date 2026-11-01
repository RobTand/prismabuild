"""Exact advisory index bytes with supplied directory and clock observations."""
from __future__ import annotations

import base64
import json


def operate(request, store, load):
    index = load("prismabuild.publication_index")
    records = load("prismabuild.record_store").RecordStore(store / "queue")
    record = {"action_key": "c" * 64, "published_unix": 100.0,
              "checkout_root": "/compat/checkout", "published_by": "compat-host",
              "checkout_snapshot": {"parent": "a" * 40, "commit": "b" * 40}}
    path = records.item_path("ready", record["action_key"])
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(record))

    # Directory identity and trust are supplied observations, not durability proof.
    index._publication_stamp = lambda path: [17, 23, 4096, 50, 60, 16877] if path.is_dir() else None
    index.kept_reads._filesystem_type = lambda _device: "ext4"
    index._clock_fence = lambda: 1000
    assert records.index_publication(record)
    with records.begin_publication_reconciliation() as reconciliation:
        for _ in range(16):
            progress = reconciliation.step(max_entries=2)
            if progress["finished"]:
                break
        assert progress["finished"] and progress["complete"], progress

    candidate = index._publication_candidate(record)
    bucket = index._buckets(records.root, candidate)[0]
    variant = request["case"].split("/", 1)[1]
    path = {"leaf": bucket / "entries" / index._leaf(candidate),
            "namespace-coverage": index._root(records.root) / "coverage.json",
            "bucket-coverage": bucket / "coverage.json"}[variant]
    if request["op"] == "write":
        return {"raw": base64.b64encode(path.read_bytes()).decode()}

    path.write_bytes(base64.b64decode(request["raw"]))
    path.chmod(0o444)
    request["_consumer_boundary"]()
    result = records.publication_candidates(facets={"checkout_root": "/compat/checkout"})
    assert result.complete, result.reasons
    assert [(row.action_key, row.published_unix, row.facets) for row in result.candidates] == [
        ("c" * 64, 100.0, {"checkout_root": "/compat/checkout", "published_by": "compat-host",
                           "snapshot_parent": "a" * 40, "snapshot_commit": "b" * 40})]
    return {"action_key": result.candidates[0].action_key, "complete": result.complete}
