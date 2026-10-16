"""Read own PB terminal/request/log/CAS evidence without importing product code."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def check_hash(value, field):
    assert sha(canonical({k: v for k, v in value.items() if k != field})) == value[field], field


def file_hash(path, expected, size=None):
    actual = hashlib.sha256()
    count = 0
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            actual.update(chunk)
            count += len(chunk)
    assert actual.hexdigest() == expected, str(path)
    assert size is None or count == size, str(path)
    return count


def audit(key, fleet):
    assert re.fullmatch("[0-9a-f]{64}", key)
    queue, cas = fleet / "pb-queue", fleet / "cas"
    paths = [queue / state / (key + ".json") for state in ("done", "failed")]
    present = [path for path in paths if path.is_file()]
    assert len(present) == 1, "action must have exactly one terminal record"
    ending = json.loads(present[0].read_bytes())
    detail = ending["detail"]
    assert ending["action_key"] == key
    assert ending["resource_scope_cleanup"]["complete"] is True
    request_raw = (cas / "requests" / key[:2] / (key + ".json")).read_bytes()
    request = json.loads(request_raw)
    check_hash(request, "action_key")
    assert request["action_key"] == key
    logs, counts = {}, []
    for name in ("stdout", "stderr"):
        row = detail[name + "_log"]
        path = queue / row["path"]
        assert path.resolve().is_relative_to(queue.resolve())
        file_hash(path, row["sha256"], row["bytes"])
        logs[name] = {k: row[k] for k in ("path", "bytes", "sha256")}
        counts.extend(re.findall(r"(\d+) (passed|failed|skipped)", path.read_text(errors="replace")))
    inputs = []
    for row in request["inputs"]:
        path = cas / "blobs" / row["sha256"][:2] / row["sha256"]
        file_hash(path, row["sha256"], row["bytes"])
        inputs.append({key: row[key] for key in ("id", "sha256", "bytes")})
    snapshot = request["params"]["checkout_snapshot"]
    source = snapshot["input"]
    bundle = cas / "blobs" / source["sha256"][:2] / source["sha256"]
    heads = subprocess.check_output(["git", "bundle", "list-heads", str(bundle)], text=True)
    assert snapshot["commit"] in heads
    receipt_path = cas / "actions/v3" / key[:2] / (key + ".json")
    receipt = None
    if receipt_path.exists():
        receipt = json.loads(receipt_path.read_bytes())
        check_hash(receipt, "receipt_sha256")
        assert receipt["action_key"] == key and receipt["action_manifest_sha256"] == sha(canonical(request))
        producer = receipt["producer"]
        check_hash(producer, "attestation_sha256")
        assert producer["action_key"] == key
        runtime = producer["runtime"]
        check_hash(runtime, "runtime_sha256")
        for name in ("core", "launcher", "digest_primitives"):
            row = runtime.get(name)
            if row:
                file_hash(Path(row["resolved_path"]), row["sha256"], row["bytes"])
        row = receipt["result"]
        file_hash(cas / "blobs" / row["sha256"][:2] / row["sha256"], row["sha256"], row["bytes"])
        assert detail["returncode"] == 0
    else:
        assert detail["returncode"] != 0
    return {"action_key": key, "terminal": str(present[0]), "returncode": detail["returncode"],
            "worker": ending["finished_host"], "source_snapshot": snapshot,
            "request_sha256": sha(request_raw), "logs": logs, "native_counts": counts,
            "inputs": inputs, "source_bundle_heads_verified": True, "cleanup_complete": True,
            "receipt_sha256": receipt["receipt_sha256"] if receipt else None,
            "runtime_sha256": receipt["producer"]["runtime"]["runtime_sha256"] if receipt else None}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--action", action="append", required=True)
    parser.add_argument("--fleet", type=Path, default=Path("/mnt/shared/prismabuild-fleet"))
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    rows = [audit(key, args.fleet) for key in args.action]
    with args.out.open("x") as stream:
        stream.write(json.dumps(rows, sort_keys=True, indent=2) + "\n")
    args.out.chmod(0o444)
    for row in rows:
        print(row["action_key"], "exit", row["returncode"], "receipt", bool(row["receipt_sha256"]), "cleanup complete")


if __name__ == "__main__":
    main()
