"""Re-audit every completed row without submitting test work."""
import collections
import hashlib
import json
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parent
PLAN_SHA = "24a6313c5e516d0c80ebd5c14b63707e0d10645076a7dc28cb72f240a841e9de"

def main():
    raw = (ROOT / "qualification-plan.json").read_bytes()
    assert hashlib.sha256(raw).hexdigest() == PLAN_SHA
    plan = json.loads(raw)
    state = json.loads((ROOT / "qualification-state.json").read_text())
    assert state["state"] == "finished" and state["committed_rows"] == len(plan["rows"]) == 14
    assert [row["id"] for row in plan["rows"]] == [row["id"] for row in state["rows"]]
    out = ROOT / "final-audit"
    out.mkdir(exist_ok=True)
    entries = []
    rows = []
    for row, prior in zip(plan["rows"], state["rows"]):
        assert prior["returncode"] == prior["audit_returncode"] == 0
        path = out / (row["id"] + "-audit.json")
        subprocess.run([sys.executable, str(ROOT / "audit_reports.py"), "--report", row["report"],
                        "--head", plan["source_head"], "--output", str(path)], check=True)
        audited = json.loads(path.read_text())["entries"]
        assert all(e["status"] == "executed" and e["exit_code"] == 0 and e["receipt_sha256"]
                   and not e["failed_nodeids"] for e in audited)
        counts = collections.Counter()
        for e in audited:
            counts.update(e["outcome_counts"])
        assert set(counts) == {"call:passed"}
        rows.append({"id": row["id"], "actions": len(audited), "passed": counts["call:passed"],
                     "audit_sha256": hashlib.sha256(path.read_bytes()).hexdigest()})
        entries.extend(audited)
    keys = [e["action_key"] for e in entries]
    assert len(keys) == len(set(keys)) == 123
    result = {"source_head": plan["source_head"], "source_tree": plan["source_tree"],
              "plan_sha256": PLAN_SHA, "rows": rows, "actions": len(entries),
              "passed": sum(r["passed"] for r in rows), "failed": 0, "skipped": 0,
              "receipts": len(entries), "entries": entries,
              "scope": "Source/component validation only. Native archival compatibility is x86-only; portable repeats and integration cover both architectures.",
              "attestation_limit": "No worker signing keys checked; no cryptographic attestation or power-loss durability claim."}
    assert result["passed"] == 2419
    (ROOT / "final-summary.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({k:v for k,v in result.items() if k not in ("entries", "rows")}))

if __name__ == "__main__":
    main()
