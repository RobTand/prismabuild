"""Submit one fixed frozen cohort with a complete retained controller record."""
from datetime import datetime, timezone
import hashlib, json, os, subprocess, sys
from pathlib import Path

plan_path = Path(sys.argv[1])
plan = json.loads(plan_path.read_bytes())
out, checkout = Path(plan["out"]), Path(plan["checkout"])
tmpdir = plan.get("tmpdir", "/home/rob/tmp")
cpus = plan.get("cpus_per_shard", 2)
mem_gb = plan.get("mem_gb", 4)
assert type(cpus) is int and cpus > 0 and type(mem_gb) is int and mem_gb > 0
roster_path = Path(plan.get("expected_nodes_file", plan_path.with_name(
    plan_path.stem.removesuffix("-plan") + "-expected-nodes.txt")))
roster_raw = roster_path.read_bytes()
expected_nodes = roster_raw.decode().splitlines()
assert len(expected_nodes) == len(set(expected_nodes)) == plan["expected_nodes"]
assert {node.split("::", 1)[0] for node in expected_nodes} == set(plan["files"])
assert ("tests/test_duplication_baseline.py" in plan["files"]
        or plan.get("architecture_exemption_reason")), "software gates require the complete architecture file"
out.mkdir(parents=True, exist_ok=True)
assert not (out / "request.json").exists(), "do not duplicate an existing controller"
def git(*args):
    return subprocess.check_output(["git", "-C", str(checkout), *args], text=True).strip()
assert git("rev-parse", "HEAD") == plan["pin"] and not git("status", "--porcelain")
argv = [sys.executable, "/mnt/shared/prismabuild-fleet/repo/tools/pbtest.py",
        "--checkout", str(checkout), "--python", plan.get("python", "/home/rob/venvs/pb-cpu/bin/python"),
        "--tmpdir", tmpdir, "--shards", str(plan.get("shards", 1)),
        "--max-clients", str(plan.get("max_clients", 1)),
        "--workers-per-shard", "1", "--threads-per-shard", "1",
        "--cpus-per-shard", str(cpus), "--mem-gb", str(mem_gb), "--priority", "-10",
        "--timeout-s", str(plan.get("timeout_s", 600)), "--test-timeout-s", "180",
        "--wait-s", "10800", "--json", str(out / "results.json")]
for tag in plan.get("tags", []):
    argv.extend(["--tag", tag])
if plan.get("pytest_args"):
    argv.extend(["--pytest-args", json.dumps(plan["pytest_args"])])
argv.extend(plan["files"])
space = os.statvfs(tmpdir)
request = dict(plan, started=datetime.now(timezone.utc).isoformat(), argv=argv,
               free_bytes=space.f_bavail * space.f_frsize,
               controller_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
               population_roster={"path": str(roster_path), "bytes": len(roster_raw),
                                  "sha256": hashlib.sha256(roster_raw).hexdigest()},
               aggregate_maximum={"cpu": cpus * plan.get("max_clients", 1),
                                  "mem_gb": mem_gb * plan.get("max_clients", 1), "gpu": 0})
(out / "request.json").write_text(json.dumps(request, indent=2) + "\n")
print(json.dumps({k: request[k] for k in ("pin", "free_bytes", "aggregate_maximum")}), flush=True)
with (out / "controller.log").open("wb") as log:
    run = subprocess.run(argv, env=dict(os.environ, TMPDIR=tmpdir), stdout=log, stderr=subprocess.STDOUT)
assert git("rev-parse", "HEAD") == plan["pin"] and not git("status", "--porcelain")
rows = json.loads((out / "results.json").read_bytes()) if (out / "results.json").exists() else []
actual_nodes = [node for row in rows for node in (row.get("test_completion") or {}).get("collected", [])]
population_ok = (len(actual_nodes) == len(set(actual_nodes))
                 and set(actual_nodes) == set(expected_nodes))
(out / "population.json").write_text(json.dumps(dict(
    verified=population_ok, expected=len(expected_nodes), actual=len(actual_nodes),
    missing=sorted(set(expected_nodes) - set(actual_nodes)),
    extra=sorted(set(actual_nodes) - set(expected_nodes))), indent=2) + "\n")
result = dict(returncode=run.returncode, population_verified=population_ok,
              ended=datetime.now(timezone.utc).isoformat(),
              log_sha256=hashlib.sha256((out / "controller.log").read_bytes()).hexdigest())
(out / "exit.json").write_text(json.dumps(result, indent=2) + "\n")
print(json.dumps(result), flush=True)
raise SystemExit(run.returncode or (0 if population_ok else 1))
