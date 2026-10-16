"""Adopt only fully observed cases from an immutable PB capture; preserve every RED."""
from __future__ import annotations

import argparse
import base64
import hashlib
import json
from pathlib import Path


def adopt(capture, destination, suffix, exclude):
    report_path = capture / "qualification.json"
    report = json.loads(report_path.read_bytes())
    excluded = {case for case in report["selected_cases"] if case.split("/", 1)[0] in exclude}
    selected = set(report["selected_cases"]) - excluded
    assert selected, "capture adoption must select actual cases"
    assert not any(row["case"] in selected for row in report["failures"]), "selected cases retain a failed direction"
    rows_by_name = {}
    for name in ("goldens", "historical", "absent-participants"):
        path = capture / (name + ".jsonl")
        rows = [json.loads(line) for line in path.read_text().splitlines()]
        rows = [row for row in rows if row["case"] in selected]
        for row in rows:
            if "raw" in row:
                assert hashlib.sha256(base64.b64decode(row["raw"], validate=True)).hexdigest() == row["sha256"]
        rows_by_name[name] = rows
    assert {row["case"] for row in rows_by_name["goldens"]} == selected
    generations = {row["generation"] for key in ("historical", "absent-participants") for row in rows_by_name[key]}
    assert generations, "no historical observations captured"
    for case in selected:
        for generation in generations:
            observations = sum(row["case"] == case and row["generation"] == generation
                               for key in ("historical", "absent-participants") for row in rows_by_name[key])
            assert observations == 1, "each case requires exactly one codec or explicit absence observation per generation"
    provenance = {"capture_report_sha256": hashlib.sha256(report_path.read_bytes()).hexdigest(),
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
    args = parser.parse_args()
    adopt(args.capture, args.destination, args.suffix, set(args.exclude_kind))


if __name__ == "__main__":
    main()
