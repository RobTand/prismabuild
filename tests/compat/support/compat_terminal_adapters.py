"""Real private queue corruption and lost-claim terminal writers."""
from __future__ import annotations

import base64
import json
import uuid


def operate(request, store, load):
    variant = request["case"].split("/", 1)[1]
    pool = load("prismabuild.pool")
    queue = pool.PoolQueue(store / "queue")
    queue.ensure_layout()
    key = "c" * 64
    status = {"unreadable-writer": "unreadable_record", "orphan-writer": "orphaned_stub",
              "lost-claim-writer": "finish_lost_race", "slurm-withdrawal-writer": "withdrawn"}[variant]
    path = queue.item_path(pool.WITHDRAWN if variant == "slurm-withdrawal-writer" else pool.FAILED, key)
    if request["op"] == "read":
        path.write_bytes(base64.b64decode(request["raw"]))
        request["_consumer_boundary"]()
        result = load("pbrun").terminal_record(path, 100.0, report_unreadable=True)
        assert result and result["status"] == status and result["action_key"] == key
        return result
    uuid.uuid4 = lambda: uuid.UUID("7" * 32)
    if variant == "unreadable-writer":
        ready = queue.item_path(pool.READY, key)
        ready.write_bytes(b"{unparseable fixture")
        assert queue._file_unreadable(ready, reason="private syntax fixture") == key
    elif variant == "orphan-writer":
        queue.item_path(pool.READY, key).write_text(json.dumps({"action_key": key}))
        assert queue.quarantine_orphans() == [key]
    elif variant == "lost-claim-writer":
        assert queue.finish(key, status="failed", detail={"returncode": 1}) == path
    else:
        actual, _ = load("prismabuild.slurm_lane").publish_withdrawal(
            queue_root=queue.root, action_key=key, reason="private operator fixture", by="compat")
        assert actual == path
    return {"raw": base64.b64encode(path.read_bytes()).decode()}
