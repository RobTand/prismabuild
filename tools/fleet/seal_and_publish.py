"""Seal one action on this box and enqueue it for whichever box takes it.

The smoke test for a dispatcher, so it goes out on the dispatcher under test:
``--transport`` picks the pull queue or the SLURM lane, and both report where
the action actually went -- a ``ready/`` item or a submission record beside the
job id.  Printing a ``ready/`` path for work the lane carried would be a smoke
test reporting a queue that had nothing to do with it.
"""
import argparse
import sys
import socket
import json
from pathlib import Path
SH = Path("/mnt/shared/prismabuild-fleet")
sys.path.insert(0, str(Path(__file__).resolve(strict=True).parent))
from runtime_paths import generation_root
RUNTIME_ROOT = generation_root(__file__)
sys.path.insert(0, str(RUNTIME_ROOT / "src"))
from prismabuild import core as pb
import fleet_submit

result_path = "fleet_result.txt"


def build_action(checkout: Path) -> dict:
    body = {
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {
            "definition_id": "fleet/first-dispatch",
            "definition_version": "v1",
            "task_class": "generation",
            "determinism": "deterministic",
            "artifact_family": "generic",
            "artifact_kind": "generic",
            # Deterministic output: the bytes must not depend on which box ran
            # it, or the CAS receipt would differ per host and the action key
            # would be a lie.  The executing hostname goes to stdout, never
            # into the result.
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
    return pb.seal_action(body)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    fleet_submit.add_transport_argument(ap)
    args = ap.parse_args(argv)

    checkout = SH / "checkout"
    checkout.mkdir(parents=True, exist_ok=True)
    # The code closure member must exist and be identical on both boxes.
    (checkout / "task_code.py").write_text("# closure member\n", encoding="utf-8")

    action = build_action(checkout)
    key = str(action["action_key"])

    cas = pb.PrismaBuildCAS(SH / "cas")
    request = cas.publish_action_request(action)

    submission = fleet_submit.submit(
        action,
        cas=cas,
        request_path=request,
        transport=args.transport,
        checkout_root=str(checkout) if args.transport == "pool" else None,
        worker_script=str(RUNTIME_ROOT / "tools" / "prismabuild_worker.py"),
        tags=["gb10"],
    )
    print(json.dumps({
        "sealed_on": socket.gethostname(),
        "action_key": key,
        "request": str(request),
        "transport": submission.transport,
        "submitted": submission.describe(),
        # Where the submission actually is: the queue item under the pool, the
        # sealed submission record under the lane.
        "where": str(submission.where),
    }, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
