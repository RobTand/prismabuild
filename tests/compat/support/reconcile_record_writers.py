"""Join source constructors to attested real executions; report every uncovered boundary."""
from __future__ import annotations

import argparse
import base64
import gzip
from collections import Counter, defaultdict
import hashlib
import json
from pathlib import Path

from wire_inventory import catalogue


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def constructor_dispositions(root):
    path = root / "tests/compat/constructor_dispositions.jsonl"
    result = {}
    for line in path.read_text().splitlines() if path.exists() else ():
        row = json.loads(line)
        assert digest(root / row["path"]) == row["source_sha256"], row
        assert row["reason"] and row["disposition"] in (
            "decoder_normalization", "in_memory_state", "reader_projection", "qualification_fixture_constructor")
        key = row["wire_id"], row["path"], row["owner"], row["line"]
        assert key not in result
        result[key] = row
    return result


def primary_records(audits, fleet, source_pin):
    for audit in audits:
        for action in json.loads(audit.read_bytes()):
            assert action["source_parent"] == source_pin and action["source_git"]["git_objects_verified"]
            assert action["returncode"] == 0 and action["receipt_sha256"] and action["cleanup_complete"]
            assert action["native_completion"] and not action["outcomes"]["deselected"]
            for stream in ("stdout", "stderr"):
                reference = action["logs"][stream]
                path = fleet / "pb-queue" / reference["path"]
                assert digest(path) == reference["sha256"] and path.stat().st_size == reference["bytes"]
                for number, line in enumerate(path.read_text().splitlines(), 1):
                    if line.startswith("compat-writer-proof: "):
                        raw = gzip.decompress(base64.b64decode(line.removeprefix("compat-writer-proof: "), validate=True))
                        yield f"{action['action_key']}:{stream}:{number}", hashlib.sha256(raw).hexdigest(), json.loads(raw)


def captured_records(evidence):
    for directory in evidence:
        for path in sorted(directory.rglob("*.json")):
            try:
                record = json.loads(path.read_bytes())
            except (ValueError, UnicodeError):
                continue
            yield str(path), digest(path), record


def reconcile(root, inventory, records):
    observed, functions, inputs = defaultdict(set), defaultdict(set), []
    case_records = defaultdict(list)
    reviewed, used = constructor_dispositions(root), set()
    for identity, data_hash, record in records:
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
        case_records[record["request"]].append((identity, phase, record, actual))
        if phase != "write":
            continue
        inputs.append({"path": identity, "sha256": data_hash, "case": record["request"]})
        for name, lines in record.get("executed_lines", {}).items():
            if name in actual:
                for line in lines:
                    observed[name, line].add(record["request"])
        for name, _, _, owner in record.get("calls", []):
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
            review_key = wire["wire_id"], path, owner, constructor["line"]
            if review_key in reviewed:
                review = reviewed[review_key]
                assert review["fields"] == constructor["fields"], review
                row.update(disposition=review["disposition"], reason=review["reason"], source_sha256=review["source_sha256"])
                used.add(review_key)
            elif wire["disposition"] not in ("catalogued_adapter", "adapter_requires_capture"):
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
    assert used == set(reviewed), "stale constructor dispositions require an explicit source review"
    case_writers = []
    for kind in catalogue(root).values():
        for variant in kind["variants"]:
            case = kind["name"] + "/" + variant
            mode = "input-reader" if kind.get("caller_input") else "source-reader" if kind.get("fixture_source") else "write"
            writer = dict(kind.get("variant_writers", [])).get(variant)
            if mode != "write":
                references = [dict(kind.get("variant_readers", [])).get(variant, kind["reader"]), *kind.get("legacy_readers", [])]
            else:
                if writer is None:
                    assert len(kind["writers"]) == 1
                    writer = kind["writers"][0]
                references = [writer, *kind.get("legacy_writers", [])]
            evidence = []
            for identity, phase, record, actual in case_records[case]:
                if phase != mode:
                    continue
                for reference in references:
                    module, qualified = reference.split(":", 1)
                    paths = (["src/" + module.replace(".", "/") + ".py"] if module.startswith("prismabuild.") else
                             ["tools/fleet/" + module + ".py", "tools/" + module + ".py"])
                    for name, _, _, owner in record.get("calls", []):
                        if name in paths and name in actual and owner == qualified:
                            evidence.append({"record": identity, "reference": reference, "source_sha256": actual[name]})
            case_writers.append({"case": case, "wire_id": kind["wire_id"], "operation": mode,
                                 "expected_entries": references, "executed": evidence})
    return {"constructors": rows, "counts": dict(Counter(row["disposition"] for row in rows)),
            "case_writers": case_writers, "uncovered_case_writers": [row for row in case_writers if not row["executed"]],
            "evidence": inputs, "inventory_sha256": digest(inventory),
            "uncovered": [row for row in rows if row["disposition"] == "uncovered_constructor"]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--inventory", type=Path, required=True)
    parser.add_argument("--attestations", type=Path, action="append", default=[])
    parser.add_argument("--primary-audit", type=Path, action="append", default=[])
    parser.add_argument("--source-pin")
    parser.add_argument("--fleet", type=Path, default=Path("/mnt/shared/prismabuild-fleet"))
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--require-complete", action="store_true")
    args = parser.parse_args()
    if args.primary_audit and not args.source_pin:
        parser.error("primary native evidence requires its explicit source pin")
    from itertools import chain
    records = chain(captured_records(args.attestations), primary_records(args.primary_audit, args.fleet, args.source_pin))
    report = reconcile(args.root, args.inventory, records)
    with args.out.open("x") as stream:
        stream.write(json.dumps(report, sort_keys=True, indent=2) + "\n")
    args.out.chmod(0o444)
    print(json.dumps({"counts": report["counts"], "uncovered": len(report["uncovered"]),
                      "uncovered_case_writers": len(report["uncovered_case_writers"])}, sort_keys=True))
    if args.require_complete and (report["uncovered"] or report["uncovered_case_writers"]):
        raise SystemExit(2)


if __name__ == "__main__":
    main()
