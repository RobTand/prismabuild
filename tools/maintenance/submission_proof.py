"""Verify named PB actions, native coverage and settled scope evidence."""
from __future__ import annotations
import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile

REPO = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(REPO / "src"), str(REPO / "tools" / "fleet")]
from prismabuild import core as pb
import pbtest_outcomes


def proof_blob(path, fact):
    raw = Path(path).read_bytes()
    if len(raw) != fact["bytes"] or hashlib.sha256(raw).hexdigest() != fact["sha256"]:
        raise ValueError(f"content differs: {path}")
    return raw


def snapshot_proof(blob, snapshot, parent):
    with tempfile.TemporaryDirectory(prefix="c14-proof-") as temp:
        repo = Path(temp) / "source.git"
        subprocess.run(["git", "clone", "--bare", "--quiet", str(blob), str(repo)], check=True)
        def git(*args):
            return subprocess.check_output(["git", "--git-dir", str(repo), *args], text=True).strip()
        actual = git("rev-parse", snapshot["commit"] + "^{tree}")
        expected = git("rev-parse", parent + "^{tree}")
        if actual != expected:
            raise ValueError(f"snapshot tree differs from frozen parent {parent}")
        return {"commit": snapshot["commit"], "parent": parent, "tree": actual,
                "input": snapshot["input"]}


def action_proof(root, key, *, parent, snapshots):
    queue, cas_root = root / "pb-queue", root / "cas"
    paths = [queue / state / (key + ".json") for state in ("done", "failed", "withdrawn")]
    terminal_paths = [path for path in paths if path.exists()]
    if len(terminal_paths) != 1:
        raise ValueError(f"{key}: expected one terminal record, got {terminal_paths}")
    path = terminal_paths[0]
    raw_terminal = path.read_bytes()
    terminal = json.loads(raw_terminal)
    if terminal["action_key"] != key or terminal["priority"] != -10:
        raise ValueError(f"{key}: identity or priority differs")
    snapshot = terminal["checkout_snapshot"]
    if snapshot["parent"] != parent:
        raise ValueError(f"{key}: source parent differs")
    request_path = cas_root / "requests" / key[:2] / (key + ".json")
    raw_action = request_path.read_bytes()
    action = pb.validate_action(json.loads(raw_action))
    if action["action_key"] != key:
        raise ValueError(f"{key}: request identity differs")
    variables = action["environment"]["variables"]
    for name in ("OMP_NUM_THREADS", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS"):
        if variables.get(name) != "1":
            raise ValueError(f"{key}: {name} is unbounded")
    if variables.get("TMPDIR") != "/home/rob/pbt17":
        raise ValueError(f"{key}: scratch differs")
    inputs = []
    for fact in action["inputs"]:
        blob = cas_root / "blobs" / fact["sha256"][:2] / fact["sha256"]
        proof_blob(blob, fact)
        inputs.append(fact)
        if fact == snapshot["input"]:
            if fact["sha256"] not in snapshots:
                snapshots[fact["sha256"]] = snapshot_proof(blob, snapshot, parent)
    if snapshot["input"]["sha256"] not in snapshots:
        raise ValueError(f"{key}: source snapshot is absent from sealed inputs")
    cleanup = terminal["resource_scope_cleanup"]
    exported = cleanup["export"]
    if not (cleanup["complete"] and exported["ok"] and exported["empty"]
            and exported["released"] and exported["settled"] and exported["stopped"]
            and not exported["tickets_pending"] and cleanup["released"]["ok"]):
        raise ValueError(f"{key}: scope is not empty, released and settled")
    logs, streams = {}, {}
    for name in ("stdout", "stderr"):
        fact = terminal["detail"][name + "_log"]
        raw = proof_blob(queue / fact["path"], fact)
        logs[name] = fact
        streams[name] = raw.decode(errors="replace")
    receipt = pb.PrismaBuildCAS(cas_root).lookup(action)
    runtime = executable = None
    if path.parent.name == "done":
        if receipt is None or terminal["detail"]["returncode"] != 0:
            raise ValueError(f"{key}: successful terminal lacks successful CAS evidence")
        producer = receipt["producer"]
        runtime, executable = producer["runtime"], producer["executable"]
        for name in ("core", "digest_primitives", "launcher"):
            fact = runtime[name]
            proof_blob(fact["resolved_path"], fact)
        result = receipt["result"]
        result_raw = proof_blob(cas_root / "blobs" / result["sha256"][:2] / result["sha256"], result)
        if result_raw != streams["stdout"].encode():
            raise ValueError(f"{key}: CAS result and complete stdout log differ")
    outcome = pbtest_outcomes.parse(streams["stdout"])
    completion = pbtest_outcomes.completion(streams["stdout"])
    native = None
    if outcome is not None:
        summary = pbtest_outcomes.pytest_summary(streams["stdout"].splitlines())
        counts, count = pbtest_outcomes.summary_outcomes(summary)
        reconciled = pbtest_outcomes.reconcile(outcome, counts, count)
        if reconciled["problems"] or completion["status"] != "complete":
            raise ValueError(f"{key}: native results are not complete: {reconciled['problems']}, {completion['problems']}")
        native = {"collected": outcome["collected"], "counts": counts,
            "skips": pbtest_outcomes.recorded_skips(outcome),
            "reconciled": reconciled, "completion": completion}
    return {"key": key, "state": path.parent.name, "status": terminal["status"],
        "host": terminal["finished_host"], "generation": terminal["published_unix"],
        "terminal_sha256": hashlib.sha256(raw_terminal).hexdigest(),
        "request_sha256": hashlib.sha256(raw_action).hexdigest(), "inputs": inputs,
        "resources": terminal["resources"], "cpu_allocation": terminal["cpu_allocation"],
        "interpreter": terminal["interpreter"], "worker_script": terminal["worker_script"],
        "logs": logs, "receipt": receipt, "runtime": runtime, "executable": executable,
        "scope": {"complete": cleanup["complete"], **{name: exported[name] for name in (
            "empty", "released", "settled", "stopped", "tickets_pending", "stop_reason")}},
        "native": native}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fleet", type=Path, default=Path("/mnt/shared/prismabuild-fleet"))
    parser.add_argument("--parent", required=True)
    parser.add_argument("--key", action="append", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--allow-failed", action="store_true")
    args = parser.parse_args()
    actions, snapshots = [], {}
    for key in args.key:
        actions.append(action_proof(args.fleet, key, parent=args.parent, snapshots=snapshots))
    nodes = [node for action in actions if action["native"] for node in action["native"]["collected"]]
    duplicate = sorted(node for node, count in Counter(nodes).items() if count > 1)
    failed = [action["key"] for action in actions if action["state"] != "done"]
    result = {"parent": args.parent, "actions": actions, "snapshots": snapshots,
        "native_count": len(nodes), "whole_files": len({node.split("::")[0] for node in nodes}),
        "duplicate_native_nodes": duplicate, "failed_actions": failed}
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({key: result[key] for key in ("parent", "native_count", "whole_files",
        "duplicate_native_nodes", "failed_actions")}))
    return bool(duplicate or (failed and not args.allow_failed))


if __name__ == "__main__":
    raise SystemExit(main())
