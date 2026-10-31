"""Summarize retained raw audits for the #1707 review correction."""
import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import subprocess


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def git(*args):
    return subprocess.check_output(["git", *args], text=True).strip()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--delivered-head", required=True)
    parser.add_argument("--main", required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    specs = [
        ("counter-plan-0f914f44.json", "counter-audit-0f914f44.json", True),
        ("counter-followup-plan-687714c7.json", "counter-followup-audit-687714c7.json", False),
        ("counter-parity-plan-5482fb5e.json", "counter-parity-audit-5482fb5e.json", False),
    ]
    seen, groups, totals = set(), [], Counter()
    for plan_name, audit_name, first_attempt in specs:
        plan_path, audit_path = args.evidence / plan_name, args.evidence / audit_name
        plan, audit = json.loads(plan_path.read_bytes()), json.loads(audit_path.read_bytes())
        assert audit["source_head"] == plan["source_head"]
        if not first_attempt:
            assert audit["plan_sha256"] == digest(plan_path)
        rounds, parallel, rows = Counter(), Counter(), []
        declared_rows, audited_rows = plan["rows"], audit["rows"]
        if first_attempt:
            assert audit["counts"] == {"passed": 548, "failed": 2}
            declared_rows = [row for row in declared_rows if not row["id"].endswith("-integration")]
            audited_rows = [row for row in audited_rows if not row["id"].endswith("-integration")]
        for declared, group in zip(declared_rows, audited_rows, strict=True):
            assert declared["id"] == group["id"]
            integration = declared["id"].endswith("-integration")
            arch = declared["architecture"]
            files, counts, actions = set(), Counter(), []
            for action in group["actions"]:
                key = action["action_key"]
                assert key not in seen
                seen.add(key)
                assert action["report_ran"] and action["cleanup_complete"]
                assert action["returncode"] == 0 and action["receipt_sha256"]
                assert action["source_parent"] == plan["source_head"]
                assert action["source_git"]["git_objects_verified"]
                assert not action["source_git"]["dirty_paths"]
                native = action["native_completion"]
                assert native["selected"] == native["teardown_finished"] == native["outcome_nodeids"]
                assert action["worker"] in ({"dl380g10", "celestia"} if arch == "x86_64" else {"sparky", "sparklina"})
                for count, outcome in action["native_counts"]:
                    counts[outcome] += int(count)
                files.update(action["files"])
                for path in action["files"]:
                    assert git("rev-parse", f'{plan["source_head"]}:{path}') == git("rev-parse", f"{args.delivered_head}:{path}"), path
                actions.append({"key": key, "receipt": action["receipt_sha256"],
                                "worker": action["worker"], "selected": native["selected"],
                                "files": action["files"]})
            assert files == set(declared["files"])
            assert counts == {"passed": sum(row["selected"] for row in actions)}
            totals.update(counts)
            if not integration:
                rounds[arch] += 1
                parallel[arch] += declared["workers"] == 2
            rows.append({"id": group["id"], "counts": dict(counts), "actions": actions})
        assert rounds == {"x86_64": 5, "aarch64": 5}
        assert parallel == {"x86_64": 2, "aarch64": 2}
        groups.append({"source": plan["source_head"], "plan": str(plan_path),
                       "plan_sha256": digest(plan_path), "audit": str(audit_path),
                       "audit_sha256": digest(audit_path), "rows": rows})
    assert len(seen) == 54 and totals == {"passed": 1500}
    comparison = json.loads((args.evidence / "counter-followup-source-comparison.json").read_bytes())
    final_source = comparison["revision_source"]
    assert git("rev-parse", f"{final_source}^{{tree}}") == git("rev-parse", f"{args.delivered_head}^{{tree}}")
    for row in comparison["previous_paths"]:
        assert row["revision_blob"] == git("rev-parse", f'{args.delivered_head}:{row["path"]}')
        assert row["qualified_blob"] == git("rev-parse", f'{comparison["qualified_source"]}:{row["path"]}')
        assert row["equal"] == (row["qualified_blob"] == row["revision_blob"])
    comparison["qualification_pending"] = False
    comparison["main_observed"] = args.main
    comparison["main_integration_policy"] = "No additional rebase after first review; merge train owns current-main composition (merge-via-batch-20261026)."
    base = git("merge-base", args.delivered_head, args.main)
    changed = set(git("diff", "--name-only", base, args.main).splitlines())
    owned = {row["path"] for row in comparison["previous_paths"]}
    comparison["main_changed_qualified_paths"] = sorted(changed & owned)
    comparison["prior_evidence_scope"] = "20 of 25 retained blobs equal b530faad. Changed census, pool, worker_loop and both revised expectation files have fresh component integration evidence. No current-main composition is claimed."
    result = {"schema": "pb.1707_counter_qualification.v1", "counts": dict(totals),
              "distinct_executed_actions": len(seen), "successful_receipts": len(seen),
              "scope": "Source/component only; no deployment or workload proof. Earlier failures and eight crash-era exclusions remain recorded.",
              "source_comparison": comparison, "groups": groups,
              "resolved_failure_audit": {"path": str(args.evidence / "counter-audit-0f914f44.json"),
                                         "sha256": digest(args.evidence / "counter-audit-0f914f44.json"),
                                         "counts": {"passed": 548, "failed": 2}}}
    args.out.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({"actions": len(seen), "counts": dict(totals),
                      "newer_main_overlap": comparison["main_changed_qualified_paths"]}))


if __name__ == "__main__":
    main()
