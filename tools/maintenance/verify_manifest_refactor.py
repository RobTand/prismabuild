"""Compare manifest operations with a retained Git revision inside an admitted action."""
from __future__ import annotations

import argparse
from copy import deepcopy
from io import BytesIO
import json
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile

SCHEMAS = ("prismaquant.prismabuild.data_manifest.v1",
           "prismaquant.prismabuild.data_manifest.v2")


def corpus() -> list[dict]:
    cases = []
    for sizes in ([1], [0, 3, 5, 7], [3, 5, 7, 11]):
        entries = [{"path": f"/input/{i}", "offset": 0, "bytes": size}
                   for i, size in enumerate(sizes)]
        for version in (1, 2):
            order = list(range(len(sizes)))
            if version == 2:
                order.reverse()
            phases = []
            cumulative = 0
            for i, index in enumerate(order):
                cumulative += sizes[index]
                phase = {"name": f"p{i}", "bytes": sizes[index],
                         "cumulative_bytes": cumulative}
                if version == 2:
                    phase["entry_indices"] = [index]
                phases.append(phase)
            base = {"schema": SCHEMAS[version - 1],
                    "entries": entries, "total_bytes": sum(sizes),
                    "annotations": {"phases": phases} if version == 1 else {}}
            if version == 2:
                base["read_plan"] = {"phases": phases, "read_bytes": sum(sizes)}
            cases.append(deepcopy(base))
            if version == 1:
                unphased = deepcopy(base)
                unphased["annotations"] = {}
                cases.append(unphased)
            for change in ("duplicate_name", "bad_boundary", "boolean_boundary",
                           "wrong_total", "prelaunch_prefix", "prelaunch_gap"):
                changed = deepcopy(base)
                table = (changed["annotations"]["phases"] if version == 1
                         else changed["read_plan"]["phases"])
                if change == "duplicate_name":
                    table[-1]["name"] = table[0]["name"]
                elif change == "bad_boundary":
                    table[0]["cumulative_bytes"] = 2
                elif change == "boolean_boundary":
                    table[0]["cumulative_bytes"] = True
                elif change == "wrong_total":
                    changed["total_bytes"] += 1
                elif change == "prelaunch_prefix":
                    table[0]["resident_before_launch"] = True
                else:
                    table[-1]["resident_before_launch"] = True
                cases.append(changed)
    return cases


def observed(function, *args, **kwargs):
    try:
        return function(*args, **kwargs)
    except Exception as error:
        return {"error": type(error).__name__, "message": str(error)}


def capture(root: Path) -> list[dict]:
    sys.path[:0] = [str(root / "src"), str(root / "tools/fleet")]
    if (root / "src/prismabuild/manifest_layout.py").exists():
        from prismabuild import manifest_layout as shared
        phased = shared
        timeline = shared
        read_entries = shared.read_entries
    else:
        from prismabuild import storage_tiers as phased
        import prewarm_loop as timeline
        read_entries = timeline.manifest_read_entries
    for function in (phased.manifest_read_entries, timeline.manifest_phases, read_entries):
        if not Path(function.__code__.co_filename).resolve().is_relative_to(root.resolve()):
            raise RuntimeError(f"comparison imported outside its checkout: {function}")
    results = []
    for manifest in corpus():
        phases = timeline.manifest_phases(manifest)
        entries = read_entries(manifest)
        results.append({
            "entries": entries,
            "phased_entries": observed(phased.manifest_read_entries, manifest),
            "phases": phases,
            "ranges": observed(phased.manifest_phase_ranges, manifest),
            "prelaunch": observed(phased.manifest_prelaunch_phases, manifest),
            "bytes": observed(timeline.manifest_read_bytes, manifest),
            "consumed": [observed(timeline.consumed_through, phases, name)
                         for name in ("p0", "p1", "p3", "missing")],
            "windows": [observed(timeline.window_target, phases, entries,
                                 consumed=start, budget=budget)
                        for start in (0, 3, 8) for budget in (0, 1, 7, 100)],
            "spans": [observed(timeline.entries_between, entries, start, end)
                      for start, end in ((0, 1), (1, 3), (3, 8), (8, 100))],
            "chunks": [observed(phased.split_range_into_chunks,
                                0, sum(int(e["bytes"]) for e in entries), size,
                                entry_bytes=[e["bytes"] for e in entries],
                                window_bytes=window)
                       for size in (1, 7, 100) for window in (None, 4, 100)],
        })
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", default="6073c9d36555")
    parser.add_argument("--checkout", type=Path,
                        default=Path(__file__).resolve().parents[2])
    parser.add_argument("--capture", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.capture:
        print(json.dumps(capture(args.checkout)))
        return
    archive = subprocess.run(["git", "archive", args.base], cwd=args.checkout,
                             check=True, stdout=subprocess.PIPE).stdout
    with tempfile.TemporaryDirectory(prefix="pb-manifest-before-") as directory:
        baseline = Path(directory)
        with tarfile.open(fileobj=BytesIO(archive)) as tree:
            tree.extractall(baseline, filter="data")
        results = []
        for root in (baseline, args.checkout):
            completed = subprocess.run(
                [sys.executable, str(Path(__file__).resolve()), "--capture",
                 "--checkout", str(root)], cwd=root, check=True,
                stdout=subprocess.PIPE, text=True)
            results.append(json.loads(completed.stdout))
    if results[0] != results[1]:
        differences = [(i, key) for i, (before, after) in
                       enumerate(zip(*results, strict=True))
                       for key in before if before[key] != after[key]]
        raise SystemExit(f"manifest behavior changed: {differences}")
    operations = len(results[0]) * (6 + 4 + 12 + 4 + 9)
    print(json.dumps({"base": args.base, "manifests": len(results[0]),
                      "operation_results": operations, "equivalent": True}))


if __name__ == "__main__":
    main()
