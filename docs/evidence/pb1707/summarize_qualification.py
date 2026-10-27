"""Summarize previously audited #1707 actions and check repeated-run coverage."""
import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--audit", type=Path, required=True)
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--fleet", type=Path, default=Path("/mnt/shared/prismabuild-fleet"))
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    raw = args.audit.read_bytes()
    audit = json.loads(raw)
    plan = json.loads(args.plan.read_bytes())
    assert audit["source_head"] == plan["source_head"]
    expected_files = set(plan["prelaunch"]["prelaunch"]["submission"]["files"])
    seen = set()
    counts = Counter()
    rounds = Counter()
    parallel = Counter()
    groups = []
    for group, declared in zip(audit["rows"], plan["rows"], strict=True):
        assert group["id"] == declared["id"]
        repeat = group["id"] != "main-integration"
        architecture = group["id"].rsplit("-", 1)[0] if repeat else None
        workers = int(declared["command"][declared["command"].index("--workers-per-shard") + 1])
        selected_files = set()
        actions = []
        group_counts = Counter()
        for row in group["actions"]:
            key = row["action_key"]
            assert key not in seen
            seen.add(key)
            assert row["report_ran"] and row["cleanup_complete"]
            assert row["source_parent"] == plan["source_head"]
            assert row["source_git"]["git_objects_verified"] and not row["source_git"]["dirty_paths"]
            native = row["native_completion"]
            assert native["selected"] == native["teardown_finished"] == native["outcome_nodeids"]
            assert bool(row["receipt_sha256"]) == (row["returncode"] == 0)
            request = json.loads((args.fleet / "cas/requests" / key[:2] / (key + ".json")).read_bytes())
            params = request["params"]
            assert params["demand"] == {"cpu": 2, "mem_gb": 4}
            assert params["execution_timeout_s"] == 1200
            env = request["environment"]["variables"]
            assert env["CUDA_VISIBLE_DEVICES"] == ""
            assert all(env[name] == "1" for name in ("OMP_NUM_THREADS", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS", "TORCH_NUM_THREADS"))
            command = params["command"][3:]
            actual_workers = int(command[command.index("-n") + 1]) if "-n" in command else 1
            assert actual_workers == workers
            if repeat:
                expected_tag = {"x86_64": "x86", "aarch64": "gb10"}[architecture]
                assert params["placement"]["required_tags"] == [expected_tag]
                assert row["worker"] in ({"dl380g10", "celestia"} if architecture == "x86_64" else {"sparky", "sparklina"})
                assert row["returncode"] == 0
            selected_files.update(row["files"])
            for count, outcome in row["native_counts"]:
                group_counts[outcome] += int(count)
            actions.append({"key": key, "receipt": row["receipt_sha256"], "worker": row["worker"], "selected": native["selected"], "returncode": row["returncode"]})
        if repeat:
            assert selected_files == expected_files
            assert not group_counts["failed"] and not group_counts["skipped"]
            rounds[architecture] += 1
            parallel[architecture] += workers == 2
        counts.update(group_counts)
        groups.append({"id": group["id"], "workers_per_shard": workers, "counts": dict(group_counts), "actions": actions})
    assert dict(rounds) == {"x86_64": 5, "aarch64": 5}
    assert dict(parallel) == {"x86_64": 2, "aarch64": 2}
    assert len(seen) == audit["distinct_actions"] == 95
    assert dict(counts) == audit["counts"] == {"passed": 2211, "failed": 1}
    summary = {"schema": "pb.1707_repeated_qualification.v1", "source_commit": plan["source_head"], "audit_path": str(args.audit), "audit_sha256": hashlib.sha256(raw).hexdigest(), "plan_path": str(args.plan), "plan_sha256": hashlib.sha256(args.plan.read_bytes()).hexdigest(), "distinct_executed_actions": len(seen), "counts": dict(counts), "rounds_per_architecture": dict(rounds), "xdist_rounds_per_architecture": dict(parallel), "checks": ["terminal request and source Git objects", "both log sizes and hashes", "complete native populations", "scope cleanup", "success CAS receipts and payload hashes", "unique keys and executed reports", "architecture placement, resources and native thread limits"], "known_baseline_failure": {"nodeid": "tests/test_wire_inventory.py::test_closed_field_sets_remain_pinned", "baseline_action": "e20895da5295c81770c8d1cca04dd19227a4e37441da191cd6e99c1e2f31d17c", "candidate_action": "04e0dd88fd4ebbfc0eb24c820f7a9da4bfdd86032dcaea391aa45b59be6ff3b3", "reason": "Pinned field-set inventory lacks the unchanged maintenance observe_inventory_process entry. Assertion and fixture remain unchanged."}, "scope": "Source/component qualification only. No deployment, staged-read campaign, real GPU, Docker daemon-load or NFS-fault proof. Eight crash-era actions stay excluded. CAS receipts do not prove power-loss durability.", "groups": groups}
    args.out.write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps({"actions": len(seen), "counts": dict(counts), "rounds": dict(rounds), "xdist_rounds": dict(parallel)}))


if __name__ == "__main__":
    main()
