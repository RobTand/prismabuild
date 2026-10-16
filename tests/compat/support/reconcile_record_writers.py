"""Join source constructors to attested real executions; report every uncovered boundary."""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import hashlib
import json
from pathlib import Path


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def reconcile(root, inventory, evidence):
    observed, functions, inputs = defaultdict(set), defaultdict(set), []
    for directory in evidence:
        for path in sorted(directory.rglob("*.json")):
            try:
                record = json.loads(path.read_bytes())
            except (ValueError, UnicodeError):
                continue
            if not isinstance(record, dict) or record.get("returncode") != 0 or not record.get("isolated"):
                continue
            if not record.get("compiled_source_code_verified") or not record.get("catalogued_entry_verified"):
                continue
            actual = {}
            for member in record.get("imports", {}).values():
                name = member["relative"]
                source = root / name
                if source.is_file() and digest(source) == member["sha256"]:
                    actual[name] = member["sha256"]
            phase = record.get("catalogued_operation")
            if phase != "write":
                continue
            inputs.append({"path": str(path), "sha256": digest(path), "case": record["request"]})
            for name, lines in record.get("executed_lines", {}).items():
                if name in actual:
                    for line in lines:
                        observed[name, line].add(record["request"])
            for name, _, _, owner in record["calls"]:
                if name in actual:
                    functions[name, owner].add(record["request"])
    rows = []
    for line in inventory.read_text().splitlines():
        wire = json.loads(line)
        for constructor in wire["schema_builders"]:
            if constructor["scope"] != "production":
                continue
            path, owner = constructor["path"], constructor["owner"]
            row = {"wire_id": wire["wire_id"], **constructor}
            row["owner_cases"] = sorted(functions[path, owner])
            row["constructor_cases"] = sorted(observed[path, constructor["line"]])
            if wire["disposition"] not in ("catalogued_adapter", "adapter_requires_capture"):
                row["disposition"] = wire["disposition"]
                row["reason"] = wire.get("classification", {}).get("reason", wire.get("disposition_reason"))
            elif row["constructor_cases"]:
                row["disposition"] = "executed_production_constructor"
            elif owner.rsplit(".", 1)[-1].startswith(("validate_", "_validate_")):
                row["disposition"] = "decoder_normalization"
                row["reason"] = "reader returns a normalized internal value; publication owner is separately catalogued"
            elif Path(path).name.startswith(("bench_", "qualify_", "verify_")) or "/pbcanary_legs/" in path or path.endswith("/shape_gate.py"):
                row["disposition"] = "qualification_fixture_constructor"
                row["reason"] = "source control fixture; not a fleet generation participant or runtime producer"
            else:
                row["disposition"] = "uncovered_constructor"
            rows.append(row)
    return {"constructors": rows, "counts": dict(Counter(row["disposition"] for row in rows)),
            "evidence": inputs, "inventory_sha256": digest(inventory),
            "uncovered": [row for row in rows if row["disposition"] == "uncovered_constructor"]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--inventory", type=Path, required=True)
    parser.add_argument("--attestations", type=Path, action="append", required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--require-complete", action="store_true")
    args = parser.parse_args()
    report = reconcile(args.root, args.inventory, args.attestations)
    with args.out.open("x") as stream:
        stream.write(json.dumps(report, sort_keys=True, indent=2) + "\n")
    args.out.chmod(0o444)
    print(json.dumps({"counts": report["counts"], "uncovered": len(report["uncovered"])}, sort_keys=True))
    if args.require_complete and report["uncovered"]:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
