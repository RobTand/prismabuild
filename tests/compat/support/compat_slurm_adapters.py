"""Retained transport writers with supplied scheduler answers, never a controller."""
from __future__ import annotations

import base64
import json
import subprocess

C = "c" * 64


def operate(request, store, load):
    family, variant = request["case"].split("/", 1)
    sl = load("prismabuild.slurm_lane")
    writing = request["op"] == "write"
    if family == "slurm-cache":
        path = store / "cache.json"
        if writing:
            sl.write_cache_hit(path, action_key=C, job_id="42", result_digest="a" * 64 if variant == "digest" else None)
        else:
            path.write_bytes(base64.b64decode(request["raw"]))
            request["_consumer_boundary"]()
            result = sl.read_cache_hit(path)
            assert result.get("action_key") == C and result.get("job_id") == "42"
            return result
    elif family == "slurm-submission":
        directory = sl.lane_directory(C, root=store / "lane")
        path = directory / "latest.json"
        if writing:
            core = load("prismabuild.core")
            cas = core.PrismaBuildCAS(store / "cas")
            action = {"action_key": C}
            nonce = sl.secrets.token_hex
            sl.secrets.token_hex = lambda count: "7" * (count * 2)
            try:
                sl.submit(action, cas=cas, request_path=store / "request.json", root=store / "lane",
                    resources=sl.LaneResources(cpus=1, memory_mib=1024),
                    timeout_s=30.0 if variant == "bounded" else None,
                    worker_script="/compat/worker.py", job_entry="/compat/slurm_job.py",
                    placement=["cpu"] if variant == "placed" else [], priority=-10 if variant == "placed" else 0,
                    published_unix=99.0, published_by="compat", retry_safe=False, max_attempts=1,
                    sbatch=lambda argv, **kwargs: subprocess.CompletedProcess(argv, 0, "42\n", ""))
            finally:
                sl.secrets.token_hex = nonce
        else:
            directory.mkdir(parents=True, exist_ok=True)
            path.write_bytes(base64.b64decode(request["raw"]))
            request["_consumer_boundary"]()
            record = sl.recorded_submission(C, root=store / "lane")
            assert record is not None
            job = sl.submitted_job(record)
            assert job.job_id == "42" and job.action_key == C and job.published_unix == 99.0
            return {"job_id": job.job_id, "attempt": job.attempt, "published_unix": job.published_unix}
    else:
        queue = load("prismabuild.pool").PoolQueue(store / "queue")
        if writing:
            path = sl.publish_outcome(queue_root=queue.root, action_key=C, published_unix=99.0,
                published_by="compat", status=variant, attempts=1, max_attempts=1, retry_safe=False,
                resources={"cpu": 1, "mem_gb": 1}, receipt={"result_digest": "a" * 64} if variant == "executed" else None)
            assert path is not None
        else:
            record = json.loads(base64.b64decode(request["raw"]))
            status = load("prismabuild.census")
            request["_consumer_boundary"]()
            transport = status._transport(record)
            assert transport == "slurm", "actual status consumer did not identify the ending as SLURM"
            return {"transport": transport, "status": record.get("status")}
    return {"raw": base64.b64encode(path.read_bytes()).decode(),
            "fixture_facts": ["supplied scheduler response; no scheduler process, submission or GPU probe"]}
