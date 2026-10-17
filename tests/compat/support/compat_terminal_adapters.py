"""Real private queue corruption and lost-claim terminal writers."""
from __future__ import annotations

import base64
import json
import uuid
from pathlib import Path
import runpy
import time


def terminal_reader(load):
    frontend = load("pbrun")
    if hasattr(frontend, "terminal_record"):
        return frontend.terminal_record
    return load("prismabuild.action_wait").terminal_record


def operate(request, store, load):
    bl = load("prismabuild.batch_ledger")
    variant = request["case"].split("/", 1)[1]
    pool = load("prismabuild.pool")
    queue = pool.PoolQueue(store / "queue")
    queue.ensure_layout()
    key = "d" * 64 if variant == "origin-release-writer" else "a" * 64 if variant == "dead-input-writer" else "c" * 64
    status = {"unreadable-writer": "unreadable_record", "orphan-writer": "orphaned_stub",
              "lost-claim-writer": "finish_lost_race", "slurm-withdrawal-writer": "withdrawn",
              "origin-release-writer": "origin_consumer_released", "dead-input-writer": "failed"}[variant]
    path = queue.item_path(pool.WITHDRAWN if variant == "slurm-withdrawal-writer" else pool.FAILED, key)
    if request["op"] == "read":
        path.write_bytes(base64.b64decode(request["raw"]))
        request["_consumer_boundary"]()
        result = terminal_reader(load)(path, 101.0 if variant == "origin-release-writer" else 100.0, report_unreadable=True)
        assert result and result["status"] == status and result["action_key"] == key
        return result
    uuid.uuid4 = lambda: uuid.UUID("7" * 32)
    if variant == "origin-release-writer":
        fixture = runpy.run_path(str(Path(__file__).with_name("compat_consumer_adapters.py")))["operate"]
        fixture({"case": "out-release-index/withdrawn", "op": "write"}, store, load)
        time.time = lambda: 101.0
        published = queue.publish(action_key=key, cas_root=store / "cas", checkout_root="/compat/checkout",
            worker_script="/compat/worker.py", resources={"cpu": 1, "mem_gb": 1})
        assert queue._refuse_released_origin_consumer(json.loads(published.read_bytes()), key)
    elif variant == "dead-input-writer":
        fixture = runpy.run_path(str(Path(__file__).with_name("compat_batch_adapters.py")))["committed"]
        po, _, _, queue, template, instance, descriptor, _ = fixture(store, load, "staged")
        owner = str(instance["owner_action_key"])
        claim_path = queue.item_path(pool.CLAIMED, owner)
        claim = json.loads(claim_path.read_bytes())
        # The setup's scope was a supplied identity, never a broker scope.
        # Exercise the supported legacy unscoped terminal API for this fixture.
        claim.pop("resource_scope")
        claim.update(max_attempts=1, retry_safe=False)
        queue.records.replace_record_at(claim_path, claim)
        ended = queue.finish(owner, status="failed", detail={"returncode": 1})
        assert not claim_path.exists(), {"ended": str(ended), "producer_state": bl.producer_attempt_state(queue, instance)}
        ref = queue.build_produced_output_batch_ref(instance=instance, template=template,
            batch_id="batch", descriptors=[descriptor], tier_id="prismabuild-stage:compat")
        published = queue.publish(action_key=key, cas_root=store / "cas", checkout_root="/compat/checkout",
            worker_script="/compat/worker.py", resources={"cpu": 1, "mem_gb": 1})
        item = {**json.loads(published.read_bytes()), "produced_output_batch": ref}
        Path(descriptor["path"]).unlink()
        assert po.dead_input_dependency(queue, item) is not None, {
            "producer_state": bl.producer_attempt_state(queue, instance), "batch": bl.BatchLedger.for_instance(queue.root, instance).capture(),
            "reference": ref, "origin_exists": Path(descriptor["path"]).exists()}
        assert queue.fail_dead_input_dependency(item, key)
    elif variant == "unreadable-writer":
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
    return {"raw": base64.b64encode(path.read_bytes()).decode(),
            "fixture_facts": ["private supplied legacy owner states and actual records/files; no physical producer death, broker drain or five-role proof"]}
