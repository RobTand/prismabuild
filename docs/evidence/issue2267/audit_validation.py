"""Read back #2267 validation records, source bindings, logs and CAS receipts."""
import argparse
import datetime
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
import tempfile

sys.path.insert(0, "/mnt/shared/prismabuild-fleet/repo/tools/fleet")
sys.path.insert(0, "/mnt/shared/prismabuild-fleet/repo/src")
import pbmcp
from prismabuild import core

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("manifest", type=Path)
parser.add_argument("log", type=Path)
parser.add_argument("output", type=Path)
args = parser.parse_args()
plans = json.loads(args.manifest.read_text())
published = dict((int(index), key) for index, key in re.findall(
    r"pbcampaign: row (\d+) submitted ([0-9a-f]{64})", args.log.read_text()))
if set(published) != set(range(len(plans))):
    raise RuntimeError(f"incomplete publication: {sorted(published)}")
session = pbmcp.session_from(pbmcp.build_parser().parse_args(["--deadline-s", "10"]))
cas = core.PrismaBuildCAS("/mnt/shared/prismabuild-fleet/cas")
snapshot_store = tempfile.TemporaryDirectory(prefix="issue2267-audit-")
subprocess.run(["git", "init", "--bare", "--quiet", snapshot_store.name],
               check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
rows = []
for index, plan in enumerate(plans):
    key = published[index]
    action = session.call("pb_action", {"key_prefix": key, "tail_lines": 1})
    if not action["complete"] or action["state"] != "done":
        raise RuntimeError(f"no complete success for row {index}: {key}")
    outcome = action["outcome"]
    if outcome["status"] != "executed" or outcome["returncode"] != 0:
        raise RuntimeError(f"not executed successfully: {key}")
    if outcome.get("resource_scope_cleanup", {}).get("complete") is not True:
        raise RuntimeError(f"scope cleanup not proven: {key}")
    finished = outcome["finished_unix"]
    if outcome["finished_host"] == "dl380g10" and 1792964700 <= finished <= 1792965120:
        raise RuntimeError(f"suspect crash-window result: {key}")
    log = Path(action["log_tail"]["path"])
    data = log.read_bytes()
    digest = hashlib.sha256(data).hexdigest()
    if digest != action["log_tail"]["recorded_sha256"]:
        raise RuntimeError(f"log digest mismatch: {key}")
    lines = data.decode().splitlines()
    runtimes = [json.loads(line.split(": ", 1)[1]) for line in lines
                if line.startswith("prismabuild-test-runtime: ")]
    architecture = "x86_64" if "x86" in plan["tags"] else "aarch64"
    if not runtimes:
        raise RuntimeError(f"missing runtime provenance: {key}")
    declared_files = set()
    for runtime in runtimes:
        if (runtime["kernel"]["machine"] != architecture or runtime["exitstatus"] != 0
                or runtime.get("complete") is not True):
            raise RuntimeError(f"wrong runtime or result: {key}")
        declared_files.update(runtime["files"])
        for path, expected in runtime["selected_source_sha256"].items():
            if hashlib.sha256((Path(plan["cwd"]) / path).read_bytes()).hexdigest() != expected:
                raise RuntimeError(f"test source differs: {path}")
    if not {path for path in plan["argv"] if path.startswith("tests/")} <= declared_files:
        raise RuntimeError(f"planned test selection not reported: {key}")
    if "-n" in plan["argv"] and "bringing up nodes..." not in data.decode():
        raise RuntimeError(f"xdist execution not reported: {key}")
    snapshot = action["sealed"]["checkout_snapshot"]
    blob = cas.root / "blobs" / snapshot["input"]["sha256"][:2] / snapshot["input"]["sha256"]
    with blob.open("rb") as handle:
        if hashlib.file_digest(handle, "sha256").hexdigest() != snapshot["input"]["sha256"]:
            raise RuntimeError(f"snapshot bundle digest differs: {key}")
    subprocess.run(["git", "-C", snapshot_store.name, "fetch", "--quiet", "--no-tags",
                    str(blob), snapshot["commit"]], check=True,
                   stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    def tree(ref, path, cwd):
        return subprocess.check_output(
            ["git", "rev-parse", f"{ref}:{path}"], cwd=cwd, text=True,
            stderr=subprocess.PIPE).strip()
    source_trees = {path: tree(snapshot["commit"], path, snapshot_store.name)
                    for path in ("src", "tests", "tools")}
    if any(digest != tree("HEAD", path, plan["cwd"]) for path, digest in source_trees.items()):
        raise RuntimeError(f"integrated execution trees differ: {key}")
    receipt_lines = [json.loads(line) for line in lines
                     if line.startswith("{") and '"receipt"' in line]
    receipt = receipt_lines[-1]["receipt"]
    sealed = json.loads((cas.root / "requests" / key[:2] / (key + ".json")).read_text())
    if cas.lookup_execution(sealed, receipt["receipt_sha256"]) is None:
        raise RuntimeError(f"unverified execution receipt: {key}")
    payload = cas.result_path(receipt, sealed)
    if not payload.is_file():
        raise RuntimeError(f"missing CAS payload: {key}")
    summaries = [line for line in lines if re.search(r"\d+ (passed|failed).* in [0-9.]+s", line)]
    expected_count = 103 if index in (0, 5) else 29
    if len(summaries) != 1 or not re.search(rf"\b{expected_count} passed\b", summaries[0]):
        raise RuntimeError(f"unexpected test count: {key}")
    if re.search(r"\b(failed|skipped|xfailed|xpassed|error)\b", summaries[0]):
        raise RuntimeError(f"non-green tests: {key}")
    rows.append({"row": index, "key": key, "status": outcome["status"],
                 "returncode": outcome["returncode"], "host": outcome["finished_host"],
                 "finished_utc": datetime.datetime.fromtimestamp(finished, datetime.timezone.utc).isoformat(),
                 "attempts": outcome["attempts"], "architecture": architecture,
                 "xdist_workers": int(plan["argv"][plan["argv"].index("-n") + 1]) if "-n" in plan["argv"] else 0,
                 "generation": action["generation"], "snapshot": snapshot,
                 "source_trees": source_trees, "runtimes": runtimes,
                 "summary": summaries, "log_path": str(log), "log_sha256": digest,
                 "receipt_sha256": receipt["receipt_sha256"], "result": receipt["result"],
                 "log_digest_verified": True, "execution_receipt_verified": True,
                 "payload_verified": True, "scope_cleanup_complete": True})
    print(index, key, architecture, summaries)
for architecture in ("x86_64", "aarch64"):
    selected = [row for row in rows if row["architecture"] == architecture]
    if len(selected) != 5 or sum(row["xdist_workers"] > 0 for row in selected) != 2:
        raise RuntimeError(f"incomplete architecture gate: {architecture}")
args.output.write_text(json.dumps({"schema": "prismabuild.issue2267.validation.v1",
    "issue": "https://github.com/RobTand/prismabuild/issues/2267", "matrix": rows}, indent=2) + "\n")
snapshot_store.cleanup()
print("Verified five executed runs per architecture, two xdist each; source trees, logs, receipts and CAS payloads match.")
