"""Adopt one reviewed roster addition from a source-bound PB compatibility capture."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

sys.path.insert(0, str(Path(__file__).resolve().parent))
from refresh_compat_runtime import rows, sha, sorted_json, wire


def refresh_fleet_addition(args):
    root, capture = args.root.resolve(), args.capture.resolve()
    data = root / "tests/compat"
    report = json.loads((capture / "qualification.json").read_bytes())
    expected = {"config-fleet/source"}
    if (report.get("passed") is not True or report.get("failures") != []
            or report.get("independent_writer_reproduction_required") is not True
            or set(report["selected_cases"]) != expected):
        raise ValueError("requires one fully reproduced and qualified fleet-config capture")
    parents = report["source_git"]["parents"]
    if len(parents) != 1:
        raise ValueError("capture must bind one frozen source parent")
    producer = parents[0]
    drift = subprocess.check_output(
        ["git", "-C", str(root), "diff", "--name-only", producer, "--",
         "src", "tools", "tests/compat/support"], text=True).splitlines()
    if drift:
        raise ValueError(("capture source differs", drift))
    prior, historical = data / "goldens-config.jsonl", data / "historical-config.jsonl"
    old_raw = prior.read_bytes()
    before = [row for row in rows(prior) if row["case"] in expected]
    after = rows(capture / "goldens.jsonl")
    if len(before) != 1 or len(after) != 1 or after[0]["case"] not in expected:
        raise ValueError("requires exactly one old and new current fleet record")
    review = json.loads(args.review.read_bytes())
    if set(review) != {"add_box", "entry"}:
        raise ValueError("review must name the one added box and its complete entry")
    old, new = json.loads(wire(before[0])), json.loads(wire(after[0]))
    key = review["add_box"]
    if key in old["boxes"] or new != {**old, "boxes": {**old["boxes"], key: review["entry"]}}:
        raise ValueError("fleet config changed outside the reviewed box addition")
    source = (root / "tools/fleet/fleet_boxes.json").read_bytes()
    if wire(after[0]) != source:
        raise ValueError("capture must contain the actual current roster bytes")
    selected_history = b"".join(line for line in historical.read_bytes().splitlines(keepends=True)
                                 if json.loads(line)["case"] in expected)
    if (capture / "historical.jsonl").read_bytes() != selected_history:
        raise ValueError("retained historical records changed")
    if (capture / "absent-participants.jsonl").read_bytes():
        raise ValueError("fleet config requires both actual retained writers and readers")
    archive = data / "history-controls" / ("superseded-fleet-config-" + sha(old_raw)[:16])
    if archive.exists() or args.report.exists():
        raise ValueError("adoption already exists; inspect partial results")
    manifest = data / "capture-config.json"
    with tempfile.TemporaryDirectory(prefix="adopt-fleet-") as temporary:
        staged = Path(temporary)
        subprocess.run([sys.executable, str(root / "tests/compat/support/adopt_compat_capture.py"),
                        "--capture", str(capture), "--destination", str(staged),
                        "--suffix", "config", "--primary-audit", str(args.primary_audit)], check=True)
        adopted = (staged / prior.name).read_bytes()
        if (staged / historical.name).read_bytes() != selected_history:
            raise ValueError("adopter changed historical bytes")
        replacement = b"".join(adopted if json.loads(line)["case"] in expected else line
                                for line in old_raw.splitlines(keepends=True))
        metadata = json.loads((staged / manifest.name).read_bytes())
        metadata["retained_current_cases"] = [row["case"] for row in rows(prior)
                                               if row["case"] not in expected]
        metadata["superseded_current_archive"] = str(archive.relative_to(root))
        metadata["files"] = {prior.name: sha(replacement), historical.name: sha(historical.read_bytes())}
        archive.mkdir(parents=True)
        shutil.copyfile(prior, archive / prior.name)
        shutil.copyfile(manifest, archive / manifest.name)
        provenance = {"capture_source_parent": producer, "review": review,
                      "review_sha256": sha(args.review.read_bytes()),
                      "previous_current_sha256": sha(old_raw),
                      "historical_sha256": sha(historical.read_bytes()),
                      "credit": "superseded current fixture; no historical generation credit"}
        (archive / "provenance.json").write_text(sorted_json(provenance) + "\n")
        for path in archive.iterdir():
            path.chmod(0o444)
        for path, raw in ((prior, replacement), (manifest, (sorted_json(metadata) + "\n").encode())):
            path.chmod(0o644)
            path.write_bytes(raw)
            path.chmod(0o444)
    result = {"capture_source_parent": producer, "case": "config-fleet/source",
              "added_box": key, "archive": str(archive.relative_to(root)),
              "current_goldens_sha256": sha(prior.read_bytes()),
              "historical_sha256": sha(historical.read_bytes())}
    with args.report.open("x") as stream:
        stream.write(sorted_json(result) + "\n")
    print(sorted_json(result))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--capture", type=Path, required=True)
    parser.add_argument("--review", type=Path, required=True)
    parser.add_argument("--primary-audit", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    refresh_fleet_addition(parser.parse_args())
