"""Recheck the raw runtime-field RED against the unchanged retained consumers."""
from __future__ import annotations

import argparse
import json
from pathlib import Path


def compare(before, after):
    failures = json.loads((before / "qualification.json").read_bytes())["failures"]
    assert len(failures) == 12
    assert all(row["phase"] == "reverse" and row["returncode"] == 1
               and "immutable_files" in row["error"]["message"] for row in failures)
    passed = json.loads((after / "qualification.json").read_bytes())
    assert passed["passed"] and passed["historical_captures"] == 12
    pairs = []
    for failed in failures:
        label = failed["case"].replace("/", "-")
        relative = Path("attestations") / failed["generation"] / (label + "-reverse.json")
        red = json.loads((before / relative).read_bytes())
        green = json.loads((after / relative).read_bytes())
        assert red["returncode"] == 1 and green["returncode"] == 0
        for field in ("driver_sha256", "adapters_sha256", "imports"):
            assert red[field] == green[field], (relative, field)
        for observed in (red, green):
            assert observed["isolated"] and observed["no_site"] and observed["catalogued_entry_verified"]
            assert any(call[0] == "src/prismabuild/core.py" and call[3] == "_validate_worker_runtime"
                       for call in observed["calls"])
        pairs.append({"case": failed["case"], "generation": failed["generation"],
                      "old_core_sha256": red["imports"]["prismabuild.core"]["sha256"],
                      "unchanged_driver_sha256": red["driver_sha256"],
                      "unchanged_adapters_sha256": red["adapters_sha256"]})
    rows = lambda root: {(row["generation"], row["case"]): row for row in
                         map(json.loads, (root / "historical.jsonl").read_text().splitlines())}
    historical = rows(before)
    assert len(historical) == 12 and historical == rows(after), "historical bytes were changed to get green"
    return {"unchanged_old_consumer_pairs": pairs, "historical_goldens_unchanged": len(historical),
            "red_reverse_reads": 12, "green_reverse_reads": 12}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--before", type=Path, required=True)
    parser.add_argument("--after", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    result = compare(args.before, args.after)
    with args.out.open("x") as stream:
        stream.write(json.dumps(result, indent=2) + "\n")
    args.out.chmod(0o444)
    print("12 unchanged retained consumers: actual production break RED, compatible source pin GREEN")
