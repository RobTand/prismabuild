#!/usr/bin/env python3
"""Seal a candidate D38 proof, or verify its fleet-produced CAS receipt.

Publish the sealed key using the published pbrun.py --publish-sealed entrypoint.
This tool never executes a payload or publishes runnable work.
"""
from __future__ import annotations

import argparse
import datetime
import json
from pathlib import Path

from prismabuild import core as pb, d38_gate, d38_producer, digest_primitives, fleet_roster, immutable_files, sealed_submission
from prismabuild.submission_types import FleetStore, SubmissionRequest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--seal", action="store_true",
                      help="Seal a candidate CPU preflight and save its key without publishing runnable work.")
    mode.add_argument("--verify", metavar="KEY",
                      help="Verify the fleet CAS receipt for preflight action KEY against its frozen target plan.")
    parser.add_argument("--image", help="Digest-pinned production canary image reference, required with --seal.")
    parser.add_argument("--out", type=Path, required=True,
                        help="Directory outside the checkout for the plan, sealed key and verification evidence.")
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)
    runtime = Path("/mnt/shared/prismabuild-fleet/repo").resolve(strict=True)
    store = FleetStore(Path("/mnt/shared/prismabuild-fleet"), runtime,
                       fleet_roster.Roster.beside(runtime / "tools/pbrun.py"))
    cas = pb.PrismaBuildCAS(store.cas_root)
    if args.seal:
        if not args.image:
            parser.error("--seal requires --image")
        input_digest = digest_primitives.raw_sha256(Path("AGENTS.md").read_bytes())
        command = ["/usr/bin/python3", "-I", d38_producer.SOURCE, "--image", args.image,
            "--device", "cuda", "--checks-path", d38_producer.CHECKS_PATH,
            "--input", f"instructions=AGENTS.md={input_digest}"]
        namespace = args.out / "namespace.json"
        namespace.write_text(json.dumps(d38_producer.namespace_for(command)))
        plan_path = args.out / "plan.json"
        request = SubmissionRequest(cwd=str(Path.cwd()), command=command, tag=["gb10"],
            demand="gpu=1,mem_gb=8", container_image=[args.image], d38_namespace=str(namespace),
            timeout_s=120, priority=-10,
            env=["OMP_NUM_THREADS=1", "MKL_NUM_THREADS=1", "OPENBLAS_NUM_THREADS=1"])
        d38_producer.write_d38_plan(request, plan_path, store=store)
        action, template, effective = d38_producer.prepare_preflight(
            plan_path, SubmissionRequest(priority=-10), store=store)
        cas.publish_action_request(action)
        context = dict(schema=sealed_submission.CONTEXT_SCHEMA, action_key=action["action_key"],
            request_sha256=digest_primitives.raw_sha256(pb._canonical_file_bytes(action)),
            options={name: getattr(effective, name) for name in sealed_submission.PUBLICATION_OPTIONS},
            tested_repository=template["tested_repository"], checkout_identity=template["checkout_identity"])
        immutable_files.publish_immutable(sealed_submission.sealed_context_path(store, action["action_key"]),
                                         pb._canonical_file_bytes(context))
        record = {"action_key": action["action_key"], "cpu_demand": action["params"]["demand"],
                  "publication": "none", "plan": str(plan_path)}
        (args.out / "seal.json").write_text(json.dumps(record, indent=2) + "\n")
        print(json.dumps(record), flush=True)
        return 0
    action = cas.read_action_request(args.verify)
    if action is None:
        raise ValueError("no sealed preflight request exists")
    plan = d38_gate.load_plan(cas, action)
    proof = d38_gate.verify_receipt(cas, args.verify,
        job=plan["target"]["job_identity_hash"], images=plan["target"]["image_digest"],
        namespace=plan["target"]["namespace"], target_command=plan["target_command"],
        at=datetime.datetime.now(datetime.timezone.utc))
    receipt = cas.lookup(action)
    payload = cas.blob_path(receipt["result"]["sha256"]).read_bytes()
    for name, value in (("verified-plan.json", plan), ("preflight-action.json", action),
                        ("cas-receipt.json", receipt)):
        (args.out / name).write_text(json.dumps(value, indent=2) + "\n")
    (args.out / "d38-receipt.json").write_bytes(payload)
    report = {"preflight_action_key": args.verify, "target_action_key": plan["target"]["job_identity_hash"],
        "authorization": proof, "receipt_sha256": receipt["receipt_sha256"],
        "payload_sha256": receipt["result"]["sha256"], "cpu_demand": action["params"]["demand"],
        "image": plan["target"]["image_digest"]}
    (args.out / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
