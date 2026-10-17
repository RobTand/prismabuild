#!/usr/bin/env python3
"""Index native failure details from retained PB results without product imports."""
import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import re


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("results", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    rows = json.loads(args.results.read_bytes())
    errors, details, native, scopes = Counter(), [], set(), []
    for row in rows:
        completion = row["test_completion"]
        native.update(completion["collected"])
        text = row["output"]
        if not isinstance(text, str):
            raise ValueError(type(text))
        blocks = re.split(r"\n(_{8,}[^\n]+_{8,})\n", text)
        for index in range(1, len(blocks), 2):
            body = blocks[index + 1]
            faults = [line for line in body.splitlines() if line.startswith("E ")]
            if not faults:
                continue
            references = [line for line in body.splitlines()
                          if re.search(r"\.py:[0-9]+:", line)]
            named = [line.strip() for line in faults
                     if re.match(r"E\s+[A-Za-z]+(?:Error|Exception):", line)]
            errors.update(named)
            details.append(dict(action_key=row["action_key"],
                                case=blocks[index].strip("_ "),
                                references=references, faults=faults))
        scopes.append(dict(action_key=row["action_key"],
                           native=len(completion["collected"]),
                           lifecycle=completion["status"], summary=row["summary"]))
    report = dict(results_sha256=hashlib.sha256(args.results.read_bytes()).hexdigest(),
                  native=len(native), actions=scopes, errors=dict(errors), details=details)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(dict(native=len(native), actions=len(rows),
                          errors=errors.most_common(15))))


if __name__ == "__main__":
    main()
