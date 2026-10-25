"""Measure test cost through published pbtest and rank its retained reports."""
from __future__ import annotations

import argparse
import ast
from collections import Counter, defaultdict
import json
from pathlib import Path
import re
import statistics
import subprocess
import sys
import uuid

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools/fleet"))
import pbtest_outcomes
from prismabuild.digest_primitives import raw_sha256

PUBLISHED = Path("/mnt/shared/prismabuild-fleet/repo/tools")
DURATION = re.compile(r"^\s*(\d+\.\d+)s\s+(setup|call|teardown)\s+(.+)$")


def execution_receipt(shard: dict) -> str | None:
    for line in reversed(shard.get("output", "").splitlines()):
        if not line.startswith("{"):
            continue
        try:
            publication = json.loads(line)
        except ValueError:
            continue
        if not isinstance(publication, dict):
            continue
        receipt = publication.get("receipt") or {}
        digest = receipt.get("receipt_sha256")
        if (publication.get("status") == "published"
                and receipt.get("action_key") == shard.get("action_key")
                and isinstance(digest, str) and re.fullmatch(r"[0-9a-f]{64}", digest)):
            return digest
    return None


def source_group(root: Path, test_file: str) -> dict:
    tree = ast.parse((root / test_file).read_text(), filename=test_file)
    aliases = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for name in node.names:
                aliases[name.asname or name.name.split(".")[0]] = name.name
        elif isinstance(node, ast.ImportFrom):
            for name in node.names:
                module = node.module or ""
                if module in {"prismabuild", "tools.fleet"}:
                    module += "." + name.name
                aliases[name.asname or name.name] = module
    candidates = {}
    for alias, module in aliases.items():
        path = ("src/" if module.startswith("prismabuild.") else "") + module.replace(".", "/") + ".py"
        if (root / path).is_file():
            candidates[alias] = path
    scores = Counter(candidates.values())
    for node in ast.walk(tree):
        if isinstance(node, ast.Name) and node.id in candidates:
            scores[candidates[node.id]] += 1
    if not scores:
        for node in ast.walk(tree):
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                name = Path(node.value).name
                path = "tools/fleet/" + name
                if name.endswith(".py") and (root / path).is_file():
                    scores[path] += 1
    ordered = sorted(scores, key=lambda path: (-scores[path], path))
    return {"primary": ordered[0] if ordered else "unattributed",
            "candidates": ordered, "method": "static-import-references-or-tool-filename"}


def rank_test_runs(root: Path, runs: list[dict]) -> dict:
    if not runs or len({run.get("source_digest") for run in runs}) != 1 or not runs[0].get("source_digest"):
        raise ValueError("runs need one unchanged source identity")
    if len({run["repeat"] for run in runs}) != len(runs):
        raise ValueError("repeat identities must be distinct")
    problems, evidence, sessions, exclusions = [], [], [], []
    tests = defaultdict(list)
    actions = set()
    expected = set()
    populations = []
    for run in runs:
        for name in run.get("paths", ["tests"]):
            path = root / name
            expected.update(p.relative_to(root).as_posix() for p in
                            (path.rglob("test_*.py") if path.is_dir() else [path]))
        assigned, population = set(), set()
        for shard in run["results"]:
            key = shard.get("action_key")
            label = f"repeat {run['repeat']} shard {shard.get('shard', '?')}"
            assigned.update(shard["files"])
            evidence.append({"repeat": run["repeat"], "action_key": key,
                             "receipt_path": shard.get("receipt_path"),
                             "receipt_sha256": execution_receipt(shard),
                             "returncode": shard.get("returncode")})
            if (not key or key in actions or not shard.get("returncode") and not (
                    shard.get("receipt_path") or execution_receipt(shard))):
                problems.append(label + ": missing or reused action/receipt identity")
            actions.add(key)
            sessions.append({"repeat": run["repeat"], "action_key": key,
                             "pytest_s": shard.get("pytest_s")})
            record = pbtest_outcomes.parse(shard.get("output", ""),
                                          record_binding=shard.get("record_binding"))
            verdict = shard.get("verdict") or {}
            if (record is None or verdict.get("coverage") not in {"executed", "excluded"}
                    or shard.get("test_completion", {}).get("status") != "complete"):
                problems.append(label + ": incomplete collection/outcome evidence")
                continue
            if verdict.get("coverage") == "excluded":
                exclusions.extend(shard["files"])
            if shard.get("returncode") and not (
                    shard["returncode"] == 1 and verdict.get("failing_nodes")):
                problems.append(label + ": process did not return a verified test result")
            for problem in verdict.get("problems", []):
                if problem.get("code") not in {"test_failure", "client_exit"}:
                    problems.append(label + ": " + problem["message"])
            phases = defaultdict(dict)
            for line in shard.get("output", "").splitlines():
                match = DURATION.fullmatch(line)
                if match:
                    seconds, phase, node = match.groups()
                    if phase in phases[node]:
                        problems.append(label + ": duplicate duration for " + node)
                    phases[node][phase] = float(seconds)
            reports = defaultdict(list)
            for node, when, category, *_ in record.get("reports", []) + record.get("uncounted", []):
                reports[node].append((when, category))
            for node in record.get("collected", []):
                if node in population:
                    problems.append(label + ": duplicate selected node " + node)
                population.add(node)
                outcomes = reports[node]
                expected_phases = ("setup", "teardown")
                if any(when == "call" for when, _ in outcomes):
                    expected_phases += ("call",)
                if not outcomes or any(phase not in phases[node] for phase in expected_phases):
                    problems.append(label + ": missing outcome/duration for " + node)
                    continue
                categories = {category for _, category in outcomes}
                outcome = next((c for c in ("error", "failed", "xpassed", "xfailed", "skipped", "passed")
                                if c in categories), "unknown")
                tests[node].append({"repeat": run["repeat"], "action_key": key,
                                    "seconds": sum(phases[node].values()),
                                    "phases": phases[node], "outcome": outcome,
                                    "bound_failures": [f for f in record.get("bound_failures", [])
                                                       if f["nodeid"] == node]})
        if expected - assigned:
            problems.append(f"repeat {run['repeat']}: {len(expected - assigned)} test files not assigned")
        populations.append(population)
    if any(population != populations[0] for population in populations):
        problems.append("selected populations differ between repeats")
    if len(runs) < 2:
        problems.append("fewer than two repeats; outcome stability unmeasured")
    rows, grouped, file_groups = [], defaultdict(list), {}
    for node, samples in tests.items():
        if len(samples) != len(runs):
            problems.append(node + ": not measured in every repeat")
        filename = node.split("::", 1)[0]
        if filename not in file_groups:
            file_groups[filename] = source_group(root, filename)
        group = file_groups[filename]
        counts = dict(Counter(sample["outcome"] for sample in samples))
        row = {"nodeid": node, "group": group["primary"], "grouping": group,
               "median_s": statistics.median(s["seconds"] for s in samples),
               "max_s": max(s["seconds"] for s in samples), "outcomes": counts,
               "outcome_changed": len(counts) > 1, "samples": samples}
        rows.append(row)
        grouped[group["primary"]].append(row)
    groups = [{"module": name, "tests": len(items),
               "total_median_s": sum(item["median_s"] for item in items),
               "changed_outcomes": sum(item["outcome_changed"] for item in items)}
              for name, items in grouped.items()]
    return {"schema": "prismabuild.test_runtime_inventory.v1",
            "source_digest": runs[0]["source_digest"], "repeats": len(runs),
            "duration_resolution_s": 0.01,
            "coverage": {"complete": not problems, "problems": sorted(set(problems)),
                         "expected_files": len(expected), "measured_nodes": len(rows),
                         "excluded_files": sorted(set(exclusions))},
            "groups": sorted(groups, key=lambda row: (-row["total_median_s"], row["module"])),
            "tests": sorted(rows, key=lambda row: (-row["median_s"], row["nodeid"])),
            "sessions": sessions, "evidence": evidence}


def render_test_inventory(report: dict) -> str:
    lines = ["# Test runtime inventory", "",
             f"Source: `{report['source_digest']}`. Repeats: {report['repeats']}. "
             f"Measured nodes: {report['coverage']['measured_nodes']}. "
             f"Complete coverage: {report['coverage']['complete']}.", "",
             "Loaded diagnostic durations, rounded to 0.01 s by pytest. "
             "Source groups are static ownership leads. A finite sample cannot prove absence of flakes.", "",
             "| Source group | Tests | Summed median phase seconds | Changed outcomes |",
             "|---|---:|---:|---:|"]
    lines.extend(f"| `{g['module']}` | {g['tests']} | {g['total_median_s']:.2f} | {g['changed_outcomes']} |"
                 for g in report["groups"])
    lines += ["", "## Slowest tests", "", "| Node | Median seconds | Max seconds | Outcomes |",
              "|---|---:|---:|---|"]
    lines.extend(f"| `{r['nodeid'].replace('|', '&#124;')}` | {r['median_s']:.2f} | {r['max_s']:.2f} | {r['outcomes']} |"
                 for r in report["tests"][:100])
    lines += ["", "## Changed outcomes", ""]
    lines.extend(f"- `{r['nodeid']}`: {r['outcomes']}" for r in report["tests"] if r["outcome_changed"])
    lines += ["", "## Coverage gaps", ""]
    lines.extend("- " + p for p in report["coverage"]["problems"])
    lines += ["", "## PB actions", ""]
    lines.extend(f"- Repeat {r['repeat']}: `{r['action_key']}`, receipt `{r['receipt_sha256'] or r['receipt_path']}`, exit {r['returncode']}."
                 for r in report["evidence"])
    return "\n".join(lines) + "\n"


def inventory_checkout_revision(root: Path) -> str:
    if subprocess.check_output(["git", "status", "--porcelain"], cwd=root).strip():
        raise ValueError("collect from a clean committed checkout; keep output outside it")
    return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()


def collect_test_repeats(args) -> None:
    source = inventory_checkout_revision(args.checkout)
    manifest_path = args.output / "runs.json"
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text())
        if (manifest["source_digest"] != source or manifest["paths"] != args.paths
                or manifest["python"] != args.python or manifest["repeats"] != args.repeats):
            raise ValueError("existing collection has different source, selection or options")
    else:
        manifest = {"source_digest": source, "paths": args.paths, "python": args.python,
                    "repeats": args.repeats, "collection_id": uuid.uuid4().hex, "runs": []}
        with manifest_path.open("x") as stream:
            stream.write(json.dumps(manifest, indent=2) + "\n")
    for repeat in range(1, args.repeats + 1):
        target = args.output / f"repeat-{repeat}.json"
        previous = next((r for r in manifest["runs"] if r["repeat"] == repeat), None)
        if previous:
            if raw_sha256(target.read_bytes()) != previous["report_sha256"]:
                raise ValueError("retained report digest changed")
            continue
        log = args.output / f"repeat-{repeat}.log"
        if log.exists():
            raise ValueError("unfinished repeat; inspect its PB actions before collecting again")
        command = [sys.executable, str(PUBLISHED / "pbtest.py"), "--checkout", str(args.checkout),
                   "--python", args.python, "--workers-per-shard", "1", "--threads-per-shard", "1",
                   "--mem-gb", "2", "--max-clients", "8", "--timeout-s", "3600", "--wait-s", "7200",
                   "--env", f"PRISMABUILD_INVENTORY_REPEAT={manifest['collection_id']}:{repeat}",
                   "--pytest-args", json.dumps(["--durations", "0", "--durations-min", "0", "--tb", "short"]),
                   "--json", str(target), *args.paths]
        if inventory_checkout_revision(args.checkout) != source:
            raise ValueError("source changed during collection")
        for moment in ("before", "after"):
            with (args.output / f"repeat-{repeat}-fleet-{moment}.json").open("w") as status:
                subprocess.run([sys.executable, str(PUBLISHED / "pbstatus.py"), "--json", "--recent", "0"],
                               stdout=status, stderr=subprocess.STDOUT, check=False)
            if moment == "before":
                with log.open("x") as output:
                    result = subprocess.run(command, stdout=output, stderr=subprocess.STDOUT, check=False)
        if inventory_checkout_revision(args.checkout) != source:
            raise ValueError("source changed during collection")
        if not target.exists():
            raise ValueError(f"repeat {repeat} produced no report; inspect {log}")
        manifest["runs"].append({"repeat": repeat, "command": command, "returncode": result.returncode,
                                 "report_sha256": raw_sha256(target.read_bytes())})
        temporary = manifest_path.with_suffix(".tmp")
        temporary.write_text(json.dumps(manifest, indent=2) + "\n")
        temporary.replace(manifest_path)
        print(f"committed repeat {repeat}: controller exit {result.returncode}", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("collect", "summarize"), help="submit repeats or rebuild retained rankings")
    parser.add_argument("--checkout", type=Path, default=ROOT, help="clean source checkout")
    parser.add_argument("--output", type=Path, required=True, help="evidence directory outside the checkout")
    parser.add_argument("--python", help="absolute Python interpreter on eligible workers, required to collect")
    parser.add_argument("--repeats", type=int, default=3, help="fresh suite invocations, at least two")
    parser.add_argument("paths", nargs="*", default=["tests"], help="test selection relative to checkout")
    args = parser.parse_args()
    args.checkout = args.checkout.resolve()
    args.output = args.output.resolve()
    if args.output.is_relative_to(args.checkout):
        parser.error("output must be outside the checkout")
    if args.command == "collect" and (not args.python or args.repeats < 2):
        parser.error("collect requires --python and at least two repeats")
    args.output.mkdir(parents=True, exist_ok=True)
    try:
        if args.command == "collect":
            collect_test_repeats(args)
        manifest = json.loads((args.output / "runs.json").read_text())
        runs = []
        for entry in manifest["runs"]:
            path = args.output / f"repeat-{entry['repeat']}.json"
            if raw_sha256(path.read_bytes()) != entry["report_sha256"]:
                raise ValueError("retained report digest changed")
            runs.append({"repeat": entry["repeat"], "source_digest": manifest["source_digest"],
                         "paths": manifest["paths"], "results": json.loads(path.read_text())})
        if inventory_checkout_revision(args.checkout) != manifest["source_digest"]:
            raise ValueError("grouping checkout differs from the measured source")
        report = rank_test_runs(args.checkout, runs)
        (args.output / "inventory.json").write_text(json.dumps(report, indent=2) + "\n")
        (args.output / "inventory.md").write_text(render_test_inventory(report))
        print(json.dumps(report["coverage"]))
    except (OSError, ValueError) as exc:
        parser.exit(1, f"inventory: {exc}\n")


if __name__ == "__main__":
    main()
