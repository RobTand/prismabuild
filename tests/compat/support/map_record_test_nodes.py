"""Map original executed corpus nodes to unique passing nodes in actual final PB audits."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path


def passing_nodes(audits):
    rows = []
    for path in audits:
        for action in json.loads(path.read_bytes()):
            assert action["returncode"] == 0 and action["receipt_sha256"] and action["cleanup_complete"]
            assert action["source_git"]["git_objects_verified"]
            if action["completion"] is None:
                continue
            outcomes = action["outcomes"]
            assert not outcomes["deselected"]
            for report in outcomes["reports"]:
                if report[2] == "passed":
                    rows.append((report[0], action["action_key"], action["source_parent"]))
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--original", type=Path, required=True)
    parser.add_argument("--final", type=Path, action="append", required=True)
    parser.add_argument("--source-pin", required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    original = json.loads(args.original.read_bytes())
    original = [row for row in original if row["action_key"] == "023a1bccedfee59488f93ef27110acf3db5bab1c00282a9c2f3df7422b07f65d"]
    assert len(original) == 1 and original[0]["native_completion"]["selected"] == 514
    current = passing_nodes(args.final)
    assert all(pin == args.source_pin for _, _, pin in current)
    mapping = []
    for node in original[0]["completion"]["collected"]:
        suffix = node.split("::", 1)[1].replace("test_invalid_record_reaches_and_fails_both_real_consumers", "test_invalid_record_reaches_both_real_consumer_boundaries")
        found = [(name, key) for name, key, _ in current
                 if name == node or name.endswith("::TestRecords::" + suffix)]
        assert len(found) == 1, (node, found)
        mapping.append({"original": node, "current": found[0][0], "action_key": found[0][1]})
    report = {"source_pin": args.source_pin, "original_nodes": 514, "mapped_passing_nodes": len(mapping),
              "original_audit_sha256": hashlib.sha256(args.original.read_bytes()).hexdigest(), "mapping": mapping}
    with args.out.open("x") as stream:
        stream.write(json.dumps(report, sort_keys=True, indent=2) + "\n")
    args.out.chmod(0o444)
    print(len(mapping), "original actual-consumer nodes have unique passing current owners")


if __name__ == "__main__":
    main()
