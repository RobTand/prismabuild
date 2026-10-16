"""Consumed-batch declarations and releases through private queue transitions."""
from __future__ import annotations

import base64
import json
from pathlib import Path
import runpy


def operate(request, store, load):
    family, variant = request["case"].split("/", 1)
    committed = runpy.run_path(str(Path(__file__).with_name("compat_batch_adapters.py")))["committed"]
    po, pool, core, queue, template, instance, descriptor, _ = committed(store, load, "origin-consumed")
    ref = po.origin_batch_ref(instance, batch_id="batch", manifest_digest=po.output_manifest_sha256([descriptor]))
    consumer, release_args = "d" * 64, {}
    if variant == "unpublished":
        body = runpy.run_path(str(Path(__file__).with_name("compat_core_adapters.py")))["action_body"](core, store)
        body["task"]["definition_id"] = "fleet/pbrun"
        body["environment"]["variables"]["PATH"] = "/compat/retired/tools:/usr/bin"
        action = core.seal_action(body)
        cas = core.PrismaBuildCAS(store / "cas")
        cas.publish_action_request(action)
        consumer = action["action_key"]
        release_args = {"cas_root": cas.root, "live_runtime": "/compat/live"}
    assert po.declare_origin_consumer(queue, ref, consumer_action_key=consumer)["declared"]
    declaration = po._consumers_dir(queue.root, instance, "batch") / (consumer + ".json")
    if family == "out-consumer":
        if variant == "repeated":
            assert po.declare_origin_consumer(queue, ref, consumer_action_key=consumer)["declared"]
        path = declaration
    else:
        if variant != "unpublished":
            published = queue.publish(action_key=consumer, cas_root=store / "cas", checkout_root="/compat/checkout",
                worker_script="/compat/worker.py", resources={"cpu": 1, "mem_gb": 1}, max_attempts=1, retry_safe=False)
            if variant == "withdrawn":
                queue.withdraw(consumer, by="compat", reason="ended", signal_child=False)
            else:
                record = json.loads(published.read_bytes())
                record.update(claimed_by="compat-consumer", claimed_host="compat-host", claimed_unix=100.0)
                published.unlink()
                pool._write_json_atomic(queue.item_path(pool.CLAIMED, consumer), record)
                queue.finish(consumer, status="failed")
        assert po.release_origin_consumer(queue, ref, consumer_action_key=consumer, by="compat",
                                          reason="ended", **release_args)["released"]
        if family == "out-release-index":
            path = po._release_index_path(queue, consumer, ref)
            if variant == "repaired":
                path.unlink()
                assert not po.release_origin_consumer(queue, ref, consumer_action_key=consumer, by="compat",
                                                      reason="ended")["released"]
        else:
            path = po._released_consumers_dir(queue.root, instance, "batch") / (consumer + ".json")
    if request["op"] == "write":
        return {"raw": base64.b64encode(path.read_bytes()).decode(),
                "fixture_facts": ["private supplied admitted claim", "writer-only deterministic portable stat identity"]}
    path.chmod(0o600)
    path.write_bytes(base64.b64decode(request["raw"]))
    request["_consumer_boundary"]()
    if family == "out-consumer":
        keys = po._declared_consumers(queue.root, instance, "batch", ref)
        assert keys == [consumer]
        return {"consumers": keys}
    if family == "out-release":
        result = po._consumer_release(queue.root, instance, "batch", consumer, ref)
    else:
        result = po.origin_consumer_release(queue, consumer)
    assert result and result["consumer_action_key"] == consumer
    return result
