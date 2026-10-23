"""Read own PB terminal/request/log/CAS evidence without importing product code."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess


def source_git(snapshot, bundle, repository, *, expect_head=None, require_clean=False):
    if not repository.exists():
        subprocess.run(["git", "init", "--bare", "--quiet", str(repository)], check=True,
                       capture_output=True)
    command = ["git", "--git-dir", str(repository)]
    # pb-history-ok: Audits objects supplied by the sealed snapshot bundle in a fresh repository.
    subprocess.run([*command, "bundle", "unbundle", str(bundle)], check=True, capture_output=True)
    commit = snapshot["commit"]
    # pb-history-ok: Audits objects supplied by the sealed snapshot bundle in a fresh repository.
    parents, tree = subprocess.check_output(
        [*command, "show", "-s", "--format=%P%n%T", commit], text=True).splitlines()
    assert parents.split() == [snapshot["parent"]], "snapshot parent differs from actual Git object"
    if expect_head is not None:
        assert parents == expect_head, "action did not run the declared source pin"
    # pb-history-ok: Audits objects supplied by the sealed snapshot bundle in a fresh repository.
    changed = subprocess.check_output(
        [*command, "diff-tree", "--no-commit-id", "--name-only", "-r", commit], text=True).splitlines()
    dirty = [name for name in changed if not re.fullmatch(r"\.pbrun-closure\.[0-9a-f]{16}\.json", name)]
    if require_clean:
        assert not dirty, "action source contains uncommitted changes: " + repr(dirty)
    return {"commit": commit, "parent": parents, "tree": tree, "dirty_paths": dirty,
            "bundle_sha256": snapshot["input"]["sha256"], "git_objects_verified": True}


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


def audit(key, fleet, repository=None, *, expect_head=None, require_clean=False):
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
    logs, counts, phases, completion, outcomes = {}, [], [], None, None
    for name in ("stdout", "stderr"):
        row = detail[name + "_log"]
        path = queue / row["path"]
        assert path.resolve().is_relative_to(queue.resolve())
        file_hash(path, row["sha256"], row["bytes"])
        logs[name] = {k: row[k] for k in ("path", "bytes", "sha256")}
        text = path.read_text(errors="replace")
        counts.extend(re.findall(r"(\d+) (passed|failed|skipped)", text))
        for line in text.splitlines():
            if line.startswith("pbtest-trace: "):
                row = json.loads(line.removeprefix("pbtest-trace: "))
                if row.get("event") == "phase":
                    phases.append(row)
            elif line.startswith("pbtest-completion: "):
                assert completion is None, "duplicate native completion"
                completion = json.loads(line.removeprefix("pbtest-completion: "))
            elif line.startswith("pbtest-outcomes: "):
                assert outcomes is None, "duplicate native outcomes"
                outcomes = json.loads(line.removeprefix("pbtest-outcomes: "))
    native = None
    incomplete_failed_population = (completion is not None and detail["returncode"] != 0 and
        (not completion["collection_complete"] or completion["collection_errors"] or not completion["collected"]))
    if incomplete_failed_population:
        native = {"selected": len(completion["collected"]), "qualifying_population": False,
                  "reason": "failed before complete nonempty native collection; no test credit"}
    elif completion is not None:
        assert completion["collection_complete"] and not completion["collect_only"]
        assert not completion["collection_errors"] and not completion["duplicate_teardown"]
        selected = set(completion["collected"])
        assert selected and len(selected) == len(completion["collected"])
        assert selected == set(completion["teardown_finished"]) == set(completion["outcome_nodeids"])
        assert outcomes is not None and not outcomes["collect_only"] and not outcomes["uncounted"]
        assert set(outcomes["collected"]) == selected
        assert {report[0] for report in outcomes["reports"]} == selected
        by_phase = {when: [row["nodeid"] for row in phases if row.get("when") == when]
                    for when in ("setup", "call", "teardown")}
        if phases:
            assert set(by_phase["setup"]) == set(by_phase["teardown"]) == selected
            assert all(len(values) == len(set(values)) for values in by_phase.values())
        native = {"selected": len(selected), "teardown_finished": len(completion["teardown_finished"]),
                  "outcome_nodeids": len(completion["outcome_nodeids"]),
                  "phase_counts": {when: len(values) for when, values in by_phase.items()},
                  "no_call": sorted(selected - set(by_phase["call"])) if phases else None}
    inputs = []
    for row in request["inputs"]:
        path = cas / "blobs" / row["sha256"][:2] / row["sha256"]
        file_hash(path, row["sha256"], row["bytes"])
        inputs.append({key: row[key] for key in ("id", "sha256", "bytes")})
    snapshot = request["params"]["checkout_snapshot"]
    source = snapshot["input"]
    bundle = cas / "blobs" / source["sha256"][:2] / source["sha256"]
    # pb-history-ok: Audits objects supplied by the sealed snapshot bundle in a fresh repository.
    heads = subprocess.check_output(["git", "bundle", "list-heads", str(bundle)], text=True)
    assert snapshot["commit"] in heads
    provenance = (source_git(snapshot, bundle, repository, expect_head=expect_head, require_clean=require_clean)
                  if repository is not None else None)
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
            "native_completion": native,
            "completion": completion, "outcomes": outcomes,
            "source_git": provenance, "source_parent": provenance["parent"] if provenance else None,
            "inputs": inputs, "source_bundle_heads_verified": True, "cleanup_complete": True,
            "receipt_sha256": receipt["receipt_sha256"] if receipt else None,
            "runtime_sha256": receipt["producer"]["runtime"]["runtime_sha256"] if receipt else None}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--action", action="append", required=True)
    parser.add_argument("--fleet", type=Path, default=Path("/mnt/shared/prismabuild-fleet"))
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--git-store", type=Path, required=True,
                        help="task-owned bare Git repository on private evidence storage")
    parser.add_argument("--expect-head")
    parser.add_argument("--require-clean", action="store_true")
    args = parser.parse_args()
    rows = [audit(key, args.fleet, args.git_store, expect_head=args.expect_head,
                  require_clean=args.require_clean) for key in args.action]
    with args.out.open("x") as stream:
        stream.write(json.dumps(rows, sort_keys=True, indent=2) + "\n")
    args.out.chmod(0o444)
    for row in rows:
        print(row["action_key"], "exit", row["returncode"], "receipt", bool(row["receipt_sha256"]), "cleanup complete")


if __name__ == "__main__":
    main()
