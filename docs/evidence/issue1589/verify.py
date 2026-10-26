"""Verify fleet endings, exact attempt logs, receipts and sealed scheduler bytes.

Read-only inspection; this never launches an action. Use --historical for red
evidence or superseded source. Keep --output outside the checkout.
"""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("ending", type=Path, help="pbwait --endings JSON")
parser.add_argument("--checkout", type=Path, required=True)
parser.add_argument("--output", type=Path, required=True)
parser.add_argument("--historical", action="store_true")
args = parser.parse_args()
runtime = Path("/mnt/shared/prismabuild-fleet/repo")
sys.path.insert(0, str(runtime / "src"))
from prismabuild import core as pb

ending = json.loads(args.ending.read_text())
if not ending["complete"]:
    raise ValueError("Fleet ending observation is incomplete")
args.output.mkdir(parents=True, exist_ok=True)
cas = pb.PrismaBuildCAS("/mnt/shared/prismabuild-fleet/cas")
git = args.output / "snapshots.git"
subprocess.run(["git", "init", "--bare", "--quiet", str(git)], check=True)
rows = []
for end in ending["endings"]:
    key, result = end["action_key"], end["result"]
    if not (end.get("diagnostics") or {}).get("cleanup", {}).get("complete"):
        raise ValueError(f"{key}: scope cleanup is unverified")
    logs, summaries = {}, []
    for stream in ("stdout", "stderr"):
        info = result[stream]
        raw = Path(info["path"]).read_bytes()
        if len(raw) != info["bytes"] or hashlib.sha256(raw).hexdigest() != info["sha256"]:
            raise ValueError(f"{key}: {stream} differs from the filed attempt")
        (args.output / f"{key}.{stream}.log").write_bytes(raw)
        logs[stream] = info
        if stream == "stdout":
            summaries = [line for line in raw.decode(errors="replace").splitlines()
                         if " passed" in line or " failed" in line or line.startswith("FAILED ")]
    action = cas.read_action_request(key)
    snapshot = action["params"]["checkout_snapshot"]["commit"]
    source = action["inputs"][0]
    bundle = cas.root / "blobs" / source["sha256"][:2] / source["sha256"]
    raw = bundle.read_bytes()
    if len(raw) != source["bytes"] or hashlib.sha256(raw).hexdigest() != source["sha256"]:
        raise ValueError(f"{key}: sealed source bundle differs from the request")
    subprocess.run(["git", f"--git-dir={git}", "fetch", "--quiet", str(bundle),
                    "refs/heads/prismabuild-snapshot"], check=True)
    hashes = {}
    for module in ("adaptive_cpu", "candidate_policy", "pool"):
        path = f"src/prismabuild/{module}.py"
        sealed = subprocess.check_output(["git", f"--git-dir={git}", "show", f"{snapshot}:{path}"])
        hashes[path] = {"sealed": hashlib.sha256(sealed).hexdigest(),
                        "current": hashlib.sha256((args.checkout / path).read_bytes()).hexdigest()}
        if not args.historical and hashes[path]["sealed"] != hashes[path]["current"]:
            raise ValueError(f"{key}: {path} differs from the current checkout")
    receipt = None
    if end["succeeded"]:
        digest = result["receipt_sha256"]
        cas.lookup_execution(action, digest)
        receipt = digest
    elif not args.historical:
        raise ValueError(f"{key}: action did not succeed")
    row = dict(action_key=key, host=end["host"], status=end["status"],
               exit_code=end["exit_code"], snapshot=snapshot, source=hashes,
               logs=logs, receipt_sha256=receipt, pytest_summary=summaries)
    rows.append(row)
    print(key, end["host"], end["status"], end["exit_code"], summaries,
          "receipt_verified", receipt is not None)
(args.output / "verified.json").write_text(json.dumps(rows, indent=2) + "\n")
