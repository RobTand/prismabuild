"""Observe the published window and prove refusal against its full designated generation."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import runpy
import subprocess
import sys
import time
from types import SimpleNamespace


def qualify_current_rollback(args):
    if not os.environ.get("PRISMABUILD_ACTION_SCOPE"):
        raise SystemExit("run through published PrismaBuild")
    root = Path(__file__).resolve().parents[2]
    sys.path.insert(0, str(root / "src"))
    from prismabuild.compatibility_window import WindowError, require_generation, require_predeploy, require_reachable
    from prismabuild.digest_primitives import raw_sha256

    args.out.mkdir(parents=True, exist_ok=False)
    designation_path = root / "docs/compatibility_rollback_designation.json"
    designation = json.loads(designation_path.read_bytes())
    rollback = designation["rollback"]
    target = Path(rollback["directory"])
    receipt_path = target / "RUNTIME_VERSION.json"
    receipt_raw = receipt_path.read_bytes()
    actual_receipt = require_generation(target, rollback)
    (args.out / "actual-rollback-receipt.json").write_bytes(receipt_raw)
    members = [{"path": str(target / name), "sha256": digest,
                "bytes": (target / name).stat().st_size}
               for name, digest in sorted(actual_receipt["files"].items())]
    entry = Path("/mnt/shared/prismabuild-fleet/repo/tools/fleet/pbmcp.py")
    source = entry.resolve(strict=True)
    published_root = next(parent for parent in source.parents if (parent / "RUNTIME_VERSION.json").is_file())
    producer_receipt = (published_root / "RUNTIME_VERSION.json").read_bytes()
    producer_raw = source.read_bytes()
    assert json.loads(producer_receipt)["files"][str(source.relative_to(published_root))] == raw_sha256(producer_raw)
    requests = [
        {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {
            "protocolVersion": "2024-11-05", "capabilities": {},
            "clientInfo": {"name": "c31-window-qualification", "version": "1"}}},
        {"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}},
        {"jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": {"name": "pb_status", "arguments": {"recent": 0}}},
        {"jsonrpc": "2.0", "id": 3, "method": "tools/call", "params": {"name": "pb_runtime", "arguments": {}}},
    ]
    started = time.time()
    process = subprocess.run(["/usr/bin/python3", str(source), "--deadline-s", "30"],
        input=("\n".join(json.dumps(row) for row in requests) + "\n").encode(),
        capture_output=True, timeout=90)
    (args.out / "mcp-response.jsonl").write_bytes(process.stdout)
    (args.out / "mcp-stderr.log").write_bytes(process.stderr)
    assert process.returncode == 0, process.stderr
    responses = [json.loads(line) for line in process.stdout.splitlines()]
    def result(identity):
        response = next(row for row in responses if row.get("id") == identity)
        value = json.loads(response["result"]["content"][0]["text"])
        assert value["complete"] is True and not value["timed_out_sections"] and not value["unavailable_sections"]
        assert value.get("generation_stale") is False
        return value
    status, runtime = result(2), result(3)
    assert entry.resolve(strict=True) == source and source.read_bytes() == producer_raw
    assert (published_root / "RUNTIME_VERSION.json").read_bytes() == producer_receipt
    status_raw = (json.dumps(status, indent=2) + "\n").encode()
    (args.out / "status.json").write_bytes(status_raw)
    (args.out / "runtime.json").write_text(json.dumps(runtime, indent=2) + "\n")
    envelope = {"captured_unix": started, "finished_unix": time.time(),
        "status_file": "status.json", "status_sha256": raw_sha256(status_raw), "status_exit": process.returncode,
        "producer": {"entry": str(entry), "source": str(source), "source_sha256": raw_sha256(producer_raw),
            "receipt_sha256": raw_sha256(producer_receipt)}, "complete": True}
    census_path = args.out / "capture.json"
    census_path.write_text(json.dumps(envelope, indent=2) + "\n")
    window_path = root / "tests/compat/generations/window.json"
    hosts = require_predeploy(json.loads(window_path.read_bytes()), status, captured_unix=started)
    assert {row["runtime_commit"] for row in runtime["loops"] if row["state"] == "live"} == {
        row["runtime_commit"] for row in status["nodes"] if row["state"] == "live"}
    refusals = {}
    try:
        require_reachable(root, target)
    except WindowError as exc:
        assert "receipt-member recovery protocol 1" in str(exc), str(exc)
        refusals["window"] = str(exc)
    else:
        raise AssertionError("actual designated rollback was incorrectly reachable")
    predeploy = runpy.run_path(str(root / "tests/compat/support/compat_predeploy.py"))
    approval = args.out / "approval.json"
    try:
        predeploy["validate"](SimpleNamespace(census=census_path, window=window_path,
            designation=designation_path, generations=target.parent, candidate=root, out=approval))
    except WindowError as exc:
        assert "receipt-member recovery protocol 1" in str(exc), str(exc)
        refusals["predeploy"] = str(exc)
    else:
        raise AssertionError("predeploy issued unsupported approval")
    assert not approval.exists()
    sys.path.insert(0, str(root / "tools/fleet"))
    publisher = runpy.run_path(str(root / "tools/fleet/publish_runtime.py"))
    guard = publisher["_require_reader_direction"]
    guard.__globals__["MIRROR"] = target
    for mode in ("rolling", "barrier"):
        try:
            guard(root, rollout=mode)
        except SystemExit as exc:
            assert "receipt-member recovery protocol 1" in str(exc), str(exc)
            refusals["activation_" + mode] = str(exc)
        else:
            raise AssertionError("activation source boundary accepted actual old/final direction")
    require_generation(target, rollback)
    assert receipt_path.read_bytes() == receipt_raw
    executable = Path(sys.executable).resolve()
    report = {"source_parent": subprocess.check_output(["git", "rev-parse", "HEAD^"]).decode().strip(),
        "host_input_reason": args.live_store_reason, "rollback": rollback,
        "receipt_sha256": raw_sha256(receipt_raw), "members": members,
        "hosts": hosts, "refusals": refusals, "published_mcp": envelope["producer"],
        "runtime": {"version": sys.version, "executable": sys.executable,
            "resolved_executable": str(executable), "sha256": raw_sha256(executable.read_bytes()),
            "affinity": sorted(os.sched_getaffinity(0)),
            "native_threads": {name: os.environ.get(name) for name in ("OMP_NUM_THREADS", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS")}},
        "scope": "read-only actual support window; no activation, publication, designation or installed-reader bootstrap"}
    (args.out / "qualification.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"out": str(args.out), "hosts": hosts, "actual_members": len(members), "refusals": refusals}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--live-store-reason", required=True)
    qualify_current_rollback(parser.parse_args())
