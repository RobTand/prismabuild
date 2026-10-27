"""Derive observed validation spans from retained PB ending records."""
import argparse
import csv
import json
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("controller_endings", type=Path)
    parser.add_argument("baseline_ending", type=Path)
    args = parser.parse_args()
    manifest = Path(__file__).with_name("record_controller_2253_actions.tsv")
    rows = list(csv.DictReader(manifest.open(), delimiter="\t"))
    controller = [row for row in rows if row["kind"] == "controller"]
    records = json.loads(args.controller_endings.read_text())
    assert records["complete"]
    endings = {row["action_key"]: row for row in records["endings"]}
    assert set(endings) == {row["action_key"] for row in controller}
    baseline = json.loads(args.baseline_ending.read_text())
    assert baseline["complete"] and len(baseline["endings"]) == 1
    groups = {"baseline": baseline["endings"]}
    for row in controller:
        groups.setdefault("split-" + row["repeat"], []).append(endings[row["action_key"]])
    result = {}
    for name, group in groups.items():
        for ending in group:
            assert ending["succeeded"] and ending["result"]["verified"]
            assert ending["diagnostics"]["cleanup"]["complete"]
        published = min(row["publication"]["published_unix"] for row in group)
        started = min(row["started_unix"] for row in group)
        ended = max(row["ended_unix"] for row in group)
        result[name] = {
            "actions": len(group), "first_publication_unix": published,
            "first_execution_unix": started, "last_ending_unix": ended,
            "publication_to_ending_s": round(ended - published, 2),
            "execution_to_ending_s": round(ended - started, 2),
        }
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
