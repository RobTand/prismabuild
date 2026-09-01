"""Seal one action on this box and enqueue it for whichever box takes it."""
import sys, socket, json
from pathlib import Path
SH = Path("/mnt/shared/prismabuild-fleet")
sys.path.insert(0, str(SH / "repo" / "src"))
from prismabuild import core as pb, pool

checkout = SH / "checkout"
checkout.mkdir(parents=True, exist_ok=True)
# The code closure member must exist and be identical on both boxes.
(checkout / "task_code.py").write_text("# closure member\n", encoding="utf-8")

result_path = "fleet_result.txt"
body = {
    "schema": pb.ACTION_SCHEMA_V2,
    "task": {
        "definition_id": "fleet/first-dispatch",
        "definition_version": "v1",
        "task_class": "generation",
        "determinism": "deterministic",
        "artifact_family": "generic",
        "artifact_kind": "generic",
        # Deterministic output: the bytes must not depend on which box ran it,
        # or the CAS receipt would differ per host and the action key would be
        # a lie.  The executing hostname goes to stdout, never into the result.
        "argv": [
            "/usr/bin/python3", "-c",
            "import pathlib,socket,sys;"
            "sys.stdout.write('executed on '+socket.gethostname()+'\\n');"
            f"pathlib.Path({result_path!r}).write_text('prismabuild-fleet-v1\\n')",
        ],
        "working_directory": ".",
        "result_path": result_path,
    },
    "inputs": [],
    "code_closure": pb.build_code_closure(checkout, ["task_code.py"]),
    "params": {"smoke": True},
    "environment": {"variables": {}, "toolchain": {}},
    "execution_scope": {
        "portability": "portable", "platform_key": None, "host_class": None,
    },
}
action = pb.seal_action(body)
key = str(action["action_key"])

cas = pb.PrismaBuildCAS(SH / "cas")
request = cas.publish_action_request(action)

q = pool.PoolQueue(SH / "pb-queue")
q.publish(
    action_key=key,
    cas_root=str(SH / "cas"),
    checkout_root=str(checkout),
    worker_script=str(SH / "repo" / "tools" / "prismabuild_worker.py"),
    tags=["gb10"],
)
print(json.dumps({
    "sealed_on": socket.gethostname(),
    "action_key": key,
    "request": str(request),
    "queued": str(q.item_path(pool.READY, key)),
}, indent=1))
