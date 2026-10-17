"""Adopt only fully observed cases from an immutable PB capture; preserve every RED."""
from __future__ import annotations

import argparse
import base64
import hashlib
import json
from pathlib import Path


def adopt(capture, destination, suffix, exclude, primary_audit, fleet):
    report_path = capture / "qualification.json"
    report = json.loads(report_path.read_bytes())
    if not (report.get('independent_writer_reproduction_required') is True):
        raise ValueError('rerun the generator with independent byte reproduction')
    producers = []
    for action in json.loads(primary_audit.read_bytes()):
        if action["source_snapshot"]["commit"] != report["source_git"]["snapshot"]:
            continue
        request_path = fleet / "cas/requests" / action["action_key"][:2] / (action["action_key"] + ".json")
        raw = request_path.read_bytes()
        if not (hashlib.sha256(raw).hexdigest() == action['request_sha256']):
            raise ValueError("capture integrity check failed: hashlib.sha256(raw).hexdigest() == action['request_sha256']")
        command = json.loads(raw)["params"].get("command", [])
        if "tests/compat/support/record_compat.py" not in command or "--generate" not in command:
            continue
        if Path(command[command.index("--generate") + 1]).resolve() != capture.resolve():
            continue
        if not (action['returncode'] == 0 and action['receipt_sha256'] and action['cleanup_complete']):
            raise ValueError("capture integrity check failed: action['returncode'] == 0 and action['receipt_sha256'] and action['cleanup_complete']")
        if not (action['source_git']['git_objects_verified']):
            raise ValueError("capture integrity check failed: action['source_git']['git_objects_verified']")
        producers.append({key: action[key] for key in ("action_key", "request_sha256", "receipt_sha256", "source_parent")})
    if not (len(producers) == 1):
        raise ValueError('one successful, fully verified producer must bind this capture output and source')
    excluded = {case for case in report["selected_cases"] if case.split("/", 1)[0] in exclude}
    selected = set(report["selected_cases"]) - excluded
    if not (selected):
        raise ValueError('capture adoption must select actual cases')
    if not (not any((row['case'] in selected for row in report['failures']))):
        raise ValueError('selected cases retain a failed direction')
    rows_by_name = {}
    for name in ("goldens", "historical", "absent-participants"):
        path = capture / (name + ".jsonl")
        rows = [json.loads(line) for line in path.read_text().splitlines()]
        rows = [row for row in rows if row["case"] in selected]
        for row in rows:
            if "raw" in row:
                if not (hashlib.sha256(base64.b64decode(row['raw'], validate=True)).hexdigest() == row['sha256']):
                    raise ValueError("capture integrity check failed: hashlib.sha256(base64.b64decode(row['raw'], validate=True)).hexdigest() == row['sha256']")
        rows_by_name[name] = rows
    if not ({row['case'] for row in rows_by_name['goldens']} == selected):
        raise ValueError("capture integrity check failed: {row['case'] for row in rows_by_name['goldens']} == selected")
    generations = {row["generation"] for key in ("historical", "absent-participants") for row in rows_by_name[key]}
    if not (generations):
        raise ValueError('no historical observations captured')
    for case in selected:
        for generation in generations:
            observations = sum(row["case"] == case and row["generation"] == generation
                               for key in ("historical", "absent-participants") for row in rows_by_name[key])
            if not (observations == 1):
                raise ValueError('each case requires exactly one codec or explicit absence observation per generation')
    provenance = {"capture_report_sha256": hashlib.sha256(report_path.read_bytes()).hexdigest(),
                  "verified_producer": producers[0], "independent_writer_reproduction_required": True,
                  "source_git": report["source_git"], "started_at": report["started_at"], "finished_at": report["finished_at"],
                  "selected_cases": sorted(selected), "excluded_cases": sorted(excluded),
                  "retained_failures": report["failures"], "generations": sorted(generations),
                  "capture_name": capture.name, "files": {}}
    for name, rows in rows_by_name.items():
        if not rows:
            continue
        output = destination / f"{name}-{suffix}.jsonl"
        with output.open("x") as stream:
            stream.write("".join(json.dumps(row, sort_keys=True) + "\n" for row in rows))
        output.chmod(0o444)
        provenance["files"][output.name] = hashlib.sha256(output.read_bytes()).hexdigest()
    output = destination / f"capture-{suffix}.json"
    with output.open("x") as stream:
        stream.write(json.dumps(provenance, sort_keys=True, indent=2) + "\n")
    output.chmod(0o444)
    print(len(selected), "cases adopted;", len(excluded), "explicit exclusions; RED evidence retained")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--capture", type=Path, required=True)
    parser.add_argument("--destination", type=Path, required=True)
    parser.add_argument("--suffix", required=True)
    parser.add_argument("--exclude-kind", action="append", default=[])
    parser.add_argument("--primary-audit", type=Path, required=True)
    parser.add_argument("--fleet", type=Path, default=Path("/mnt/shared/prismabuild-fleet"))
    args = parser.parse_args()
    adopt(args.capture, args.destination, args.suffix, set(args.exclude_kind), args.primary_audit, args.fleet)


if __name__ == "__main__":
    main()
