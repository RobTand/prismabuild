"""Measure test cost through published pbtest and rank its retained reports."""
from __future__ import annotations

import argparse
import ast
from collections import Counter, defaultdict
import fcntl
import json
import os
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
    abandoned = report.get("abandoned_attempts", [])
    if abandoned:
        lines += ["", "## Abandoned attempts", ""]
        lines.extend(f"- Repeat {a['repeat']}, attempt `{a['attempt_id']}`: {a['reason']}; "
                     f"log `{a['log']}`, SHA-256 `{a['log_sha256']}`; "
                     f"actions {', '.join('`' + k + '`' for k in a['action_keys']) or 'none published'}."
                     for a in abandoned)
    return "\n".join(lines) + "\n"


def inventory_checkout_revision(root: Path) -> str:
    if subprocess.check_output(["git", "status", "--porcelain"], cwd=root).strip():
        raise ValueError("collect from a clean committed checkout; keep output outside it")
    return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()


def collection_manifest(args, source: str) -> dict:
    manifest_path = args.output / "runs.json"
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text())
        if (manifest["source_digest"] != source or manifest["paths"] != args.paths
                or manifest["python"] != args.python or manifest["repeats"] != args.repeats
                or manifest.get("priority", 0) != getattr(args, "priority", 0)):
            raise ValueError("existing collection has different source, selection or options")
    else:
        manifest = {"source_digest": source, "paths": args.paths, "python": args.python,
                    "repeats": args.repeats, "priority": getattr(args, "priority", 0),
                    "collection_id": uuid.uuid4().hex, "runs": []}
        save_collection(args.output, manifest)
    return manifest


def save_collection(output: Path, manifest: dict) -> None:
    temporary = output / "runs.tmp"
    temporary.write_text(json.dumps(manifest, indent=2) + "\n")
    temporary.replace(output / "runs.json")


def observe_inventory_process(pid: int) -> dict | None:
    try:
        fields = Path(f"/proc/{pid}/stat").read_text().rpartition(")")[2].split()
    except FileNotFoundError:
        return None
    if fields[0] == "Z":
        return None
    return {"pid": pid, "start_ticks": fields[19],
            "boot_id": Path("/proc/sys/kernel/random/boot_id").read_text().strip()}


def require_dead_producer(output: Path, attempt: dict) -> dict:
    progress_path = output / (attempt["report"] + ".progress.json")
    progress = json.loads(progress_path.read_text()) if progress_path.exists() else {}
    producer = attempt.get("producer") or {"pid": progress.get("producer_pid")}
    if not producer.get("pid"):
        raise ValueError("producer identity unavailable; cannot abandon attempt")
    current = observe_inventory_process(producer["pid"])
    if current and ("start_ticks" not in producer or current == producer):
        raise ValueError(f"producer {producer['pid']} is still alive; no replacement submitted")
    attempt["producer"] = producer
    attempt["producer_observation"] = current
    return progress


def reconcile_attempt(output: Path, manifest: dict, attempt: dict) -> None:
    report = output / attempt["report"]
    if attempt["status"] == "launching":
        log = output / attempt["log"]
        attempt.update(status="abandoned", reason="controller_died_before_producer_release",
                       action_keys=[], log_sha256=raw_sha256(log.read_bytes()) if log.exists() else None)
        save_collection(output, manifest)
        return
    progress = require_dead_producer(output, attempt)
    if report.exists():
        if not isinstance(json.loads(report.read_text()), list):
            raise ValueError("retained report must be a pbtest result list")
        attempt.update(status="completed", report_sha256=raw_sha256(report.read_bytes()),
                       recovered_after_controller_exit=True)
        manifest["runs"].append(dict(attempt))
    else:
        log = output / attempt["log"]
        content = log.read_bytes() if log.exists() else b""
        keys = {shard["action_key"] for shard in progress.get("shards", []) if shard.get("action_key")}
        keys.update(k.decode() for k in re.findall(rb"(?:action |queued )([0-9a-f]{64})\b", content))
        attempt.update(status="abandoned", reason="producer_dead_without_report",
                       log_sha256=raw_sha256(content) if log.exists() else None,
                       action_keys=sorted(keys))
        progress_path = output / (attempt["report"] + ".progress.json")
        if progress_path.exists():
            attempt["progress_sha256"] = raw_sha256(progress_path.read_bytes())
    save_collection(output, manifest)


def collect_test_repeats(args) -> None:
    with (args.output / "collector.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise ValueError("a collector already owns this output directory") from None
        source = inventory_checkout_revision(args.checkout)
        manifest = collection_manifest(args, source)
        manifest["controller"] = {"unit": getattr(args, "unit", None),
                                  "process": observe_inventory_process(os.getpid()), "status": "running"}
        attempts = manifest.setdefault("attempts", [])
        save_collection(args.output, manifest)
        for repeat in range(1, args.repeats + 1):
            previous = next((r for r in manifest["runs"] if r["repeat"] == repeat), None)
            if previous:
                target = args.output / previous.get("report", f"repeat-{repeat}.json")
                if raw_sha256(target.read_bytes()) != previous["report_sha256"]:
                    raise ValueError("retained report digest changed")
                continue
            legacy_log = args.output / f"repeat-{repeat}.log"
            if legacy_log.exists() and not any(a["repeat"] == repeat for a in attempts):
                attempts.append({"repeat": repeat, "attempt_id": f"legacy-{repeat}",
                                 "report": f"repeat-{repeat}.json", "log": legacy_log.name,
                                 "status": "running"})
                save_collection(args.output, manifest)
            for attempt in attempts:
                if attempt["repeat"] == repeat and attempt["status"] in {"launching", "running"}:
                    reconcile_attempt(args.output, manifest, attempt)
            if any(r["repeat"] == repeat for r in manifest["runs"]):
                continue
            attempt_id = uuid.uuid4().hex
            stem = f"repeat-{repeat}-{attempt_id}"
            target = args.output / (stem + ".json")
            command = [sys.executable, str(PUBLISHED / "pbtest.py"), "--checkout", str(args.checkout),
                       "--python", args.python, "--workers-per-shard", "1", "--threads-per-shard", "1",
                       "--mem-gb", "2", "--max-clients", "8", "--timeout-s", "3600", "--wait-s", "7200",
                       "--priority", str(getattr(args, "priority", 0)),
                       "--env", f"PRISMABUILD_INVENTORY_REPEAT={manifest['collection_id']}:{repeat}:{attempt_id}",
                       "--pytest-args", json.dumps(["--durations", "0", "--durations-min", "0", "--tb", "short"]),
                       "--json", str(target), *args.paths]
            attempt = {"repeat": repeat, "attempt_id": attempt_id, "report": target.name,
                       "log": stem + ".log", "command": command, "status": "launching"}
            attempts.append(attempt)
            save_collection(args.output, manifest)
            if inventory_checkout_revision(args.checkout) != source:
                raise ValueError("source changed during collection")
            for moment in ("before", "after"):
                with (args.output / f"{stem}-fleet-{moment}.json").open("w") as status:
                    subprocess.run([sys.executable, str(PUBLISHED / "pbstatus.py"), "--json", "--recent", "0"],
                                   stdout=status, stderr=subprocess.STDOUT, check=False)
                if moment == "before":
                    with (args.output / attempt["log"]).open("x") as output:
                        read_fd, write_fd = os.pipe()
                        try:
                            gate = ("import os,sys; fd=int(sys.argv[1]); token=os.read(fd,1); os.close(fd); "
                                    "sys.exit(1) if token != b'1' else os.execvp(sys.argv[2],sys.argv[2:])")
                            producer = subprocess.Popen([sys.executable, "-c", gate, str(read_fd), *command],
                                                        pass_fds=(read_fd,), stdin=subprocess.DEVNULL,
                                                        stdout=output, stderr=subprocess.STDOUT)
                            attempt.update(producer=observe_inventory_process(producer.pid) or {"pid": producer.pid},
                                           status="running")
                            save_collection(args.output, manifest)
                            os.write(write_fd, b"1")
                        finally:
                            os.close(read_fd)
                            os.close(write_fd)
                        returncode = producer.wait()
            if inventory_checkout_revision(args.checkout) != source:
                raise ValueError("source changed during collection")
            if not target.exists():
                raise ValueError(f"repeat {repeat} produced no report; inspect {attempt['log']}")
            attempt.update(status="completed", returncode=returncode,
                           report_sha256=raw_sha256(target.read_bytes()))
            manifest["runs"].append(dict(attempt))
            save_collection(args.output, manifest)
            print(f"committed repeat {repeat}: controller exit {returncode}", flush=True)
        summarize_test_repeats(args)
        manifest["controller"]["status"] = "completed"
        save_collection(args.output, manifest)


def launch_collector(args) -> None:
    inventory_checkout_revision(args.checkout)
    unit = "pb-test-inventory-" + raw_sha256(str(args.output).encode())[:20] + ".service"
    runtime = Path(os.environ.get("XDG_RUNTIME_DIR", f"/run/user/{os.getuid()}"))
    environment = dict(os.environ, XDG_RUNTIME_DIR=str(runtime),
                       DBUS_SESSION_BUS_ADDRESS=os.environ.get("DBUS_SESSION_BUS_ADDRESS", f"unix:path={runtime}/bus"))
    command = [sys.executable, str(Path(__file__).resolve()), "_collect",
               "--checkout", str(args.checkout), "--output", str(args.output),
               "--python", args.python, "--repeats", str(args.repeats),
               "--priority", str(args.priority), "--unit", unit, *args.paths]
    result = subprocess.run(["systemd-run", "--user", "--collect", "--unit", unit,
                             "--property", "Type=exec", "--property", f"WorkingDirectory={args.checkout}",
                             "--property", f"StandardOutput=append:{args.output / 'controller.log'}",
                             "--property", f"StandardError=append:{args.output / 'controller.log'}",
                             "--", *command], env=environment, check=False)
    if result.returncode:
        raise ValueError("could not start collector user service; inspect systemd-run diagnostic")
    print(json.dumps({"unit": unit, "manifest": str(args.output / "runs.json"),
                      "log": str(args.output / "controller.log")}))


def summarize_test_repeats(args) -> None:
    manifest = json.loads((args.output / "runs.json").read_text())
    runs = []
    for entry in manifest["runs"]:
        path = args.output / entry.get("report", f"repeat-{entry['repeat']}.json")
        if raw_sha256(path.read_bytes()) != entry["report_sha256"]:
            raise ValueError("retained report digest changed")
        runs.append({"repeat": entry["repeat"], "source_digest": manifest["source_digest"],
                     "paths": manifest["paths"], "results": json.loads(path.read_text())})
    if inventory_checkout_revision(args.checkout) != manifest["source_digest"]:
        raise ValueError("grouping checkout differs from the measured source")
    report = rank_test_runs(args.checkout, runs)
    if len(runs) != manifest["repeats"]:
        report["coverage"]["complete"] = False
        report["coverage"]["problems"].append(
            f"requested {manifest['repeats']} repeats; only {len(runs)} reports committed")
    report["abandoned_attempts"] = [a for a in manifest.get("attempts", []) if a["status"] == "abandoned"]
    (args.output / "inventory.json").write_text(json.dumps(report, indent=2) + "\n")
    (args.output / "inventory.md").write_text(render_test_inventory(report))
    print(json.dumps(report["coverage"]))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("collect", "summarize", "_collect"), help="submit repeats or rebuild retained rankings")
    parser.add_argument("--checkout", type=Path, default=ROOT, help="clean source checkout")
    parser.add_argument("--output", type=Path, required=True, help="evidence directory outside the checkout")
    parser.add_argument("--python", help="absolute Python interpreter on eligible workers, required to collect")
    parser.add_argument("--repeats", type=int, default=3, help="fresh suite invocations, at least two")
    parser.add_argument("--priority", type=int, default=0, help="PB priority; use -10 for collector qualification")
    parser.add_argument("--unit", help=argparse.SUPPRESS)
    parser.add_argument("paths", nargs="*", default=["tests"], help="test selection relative to checkout")
    args = parser.parse_args()
    args.checkout = args.checkout.resolve()
    args.output = args.output.resolve()
    if args.output.is_relative_to(args.checkout):
        parser.error("output must be outside the checkout")
    if args.command in {"collect", "_collect"} and (not args.python or args.repeats < 2):
        parser.error("collect requires --python and at least two repeats")
    args.output.mkdir(parents=True, exist_ok=True)
    try:
        if args.command == "collect":
            launch_collector(args)
        elif args.command == "_collect":
            collect_test_repeats(args)
        else:
            summarize_test_repeats(args)
    except (OSError, ValueError) as exc:
        parser.exit(1, f"inventory: {exc}\n")


if __name__ == "__main__":
    main()
