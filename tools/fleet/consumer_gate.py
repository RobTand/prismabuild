"""Generation-bound consumer execution, separate from read-only activation checks.

The operator-selected config is trusted policy, never a test receipt. Queue/CAS
records are the evidence authority. Nothing in this module mutates a generation.
"""
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import hashlib
import json
import math
import os
from pathlib import Path
import re
import select
import subprocess
import sys
import tempfile
import threading

from prismabuild import core as pb, pool, action_result, client_channel, immutable_files, action_client as client


def decode(raw):
    return pb.digest_primitives.decode_strict_json(raw, where="consumer gate evidence")


def requirement():
    cfg = config()
    return {} if cfg is None else {"consumer_gate": policy(cfg)}


def selected(generation):
    cfg = config()
    receipt = decode((Path(generation) / "RUNTIME_VERSION.json").read_bytes())
    required = receipt.get("consumer_gate")
    if required is not None and (cfg is None or policy(cfg) != required):
        refuse("sealed candidate requires its selected consumer configuration")
    return cfg

FILES = ("bin/test_pq_release_gate.py", "bin/test_pbtest_gate.py",
         "bin/test_computation_runner.py", "bin/test_tessera_release_gate.py")
PREFIX = "consumer-gate-outcomes: "
SCHEMA = "prismabuild.consumer_gate.config.v1"


def refuse(message):
    raise SystemExit("consumer gate: " + message)


def config():
    path = os.environ.get("PRISMABUILD_CONSUMER_GATE_CONFIG")
    if path is None:
        return None
    try:
        value = decode(Path(path).read_bytes())
        if not isinstance(value, dict) or value.get("schema") != SCHEMA:
            raise ValueError("invalid config schema")
        if set(value) != {"schema", "repository", "fleet_root", "pbrun", "runtimes", "accepted_breaks"}:
            raise ValueError("unknown or missing config fields")
        for name in ("repository", "fleet_root", "pbrun"):
            if not isinstance(value[name], str) or not Path(value[name]).is_absolute():
                raise ValueError(name + " must be an absolute path")
        routes = value["runtimes"]
        if not isinstance(routes, list) or len(routes) != 2:
            raise ValueError("exactly two runtimes required")
        for route in routes:
            if set(route) != {"id", "python", "python_version", "tags", "cpus", "mem_gb"}:
                raise ValueError("unknown or missing runtime fields")
            if not isinstance(route["id"], str) or not re.fullmatch(r"[a-zA-Z0-9_-]+", route["id"]):
                raise ValueError("invalid runtime id")
            if not isinstance(route["python"], str) or not Path(route["python"]).is_absolute():
                raise ValueError("python must be absolute")
            if not isinstance(route["tags"], list) or not route["tags"] or not all(isinstance(t, str) and t for t in route["tags"]):
                raise ValueError("runtime tags required")
            route["tags"] = pool.normalize_placement_tags(route["tags"])
            if type(route["cpus"]) is not int or route["cpus"] < 1:
                raise ValueError("cpus must be a positive integer")
            if type(route["mem_gb"]) not in (int, float) or not math.isfinite(route["mem_gb"]) or route["mem_gb"] <= 0:
                raise ValueError("mem_gb must be positive and finite")
        if len({r["id"] for r in routes}) != 2 or len({r["python"] for r in routes}) != 2:
            raise ValueError("runtime routes must be distinct")
        if sorted((r["python_version"], "x86" in r["tags"], "gb10" in r["tags"]) for r in routes) != [("3.12", False, True), ("3.14", True, False)]:
            raise ValueError("require x86 Python 3.14 and gb10 Python 3.12 routes")
        if not isinstance(value["accepted_breaks"], list):
            raise ValueError("accepted_breaks must be a list")
        return value
    except (OSError, ValueError, TypeError, KeyError, pb.PrismaBuildError) as exc:
        refuse("invalid configuration: " + str(exc))


def git(repository, *args):
    result = subprocess.run(["git", "-C", str(repository), *args],
                            capture_output=True, text=True, check=False)
    if result.returncode:
        raise ValueError("consumer git " + " ".join(args[:2]) + ": " + result.stderr)
    return result.stdout.strip()


def clean_pin(cfg):
    repository = cfg["repository"]
    # Include all untracked files, not only Git's abbreviated directory names.
    if git(repository, "status", "--porcelain", "--untracked-files=all"):
        refuse("dirty consumer checkout; tracked and untracked changes are forbidden")
    if git(repository, "rev-parse", "--show-prefix"):
        refuse("repository must name the consumer Git root")
    commit = git(repository, "rev-parse", "HEAD")
    if not re.fullmatch(r"[0-9a-f]{40,64}", commit):
        refuse("consumer HEAD is not a commit")
    for name in FILES:
        git(repository, "cat-file", "-e", commit + ":" + name)
    return commit


def recipe(cfg, generation, route, commit):
    tools = Path(cfg["fleet_root"]) / "runtime-generations" / generation.name / "tools"
    command = [route["python"], "-m", "pytest", "-q", "-o", "addopts=", "-p", "consumer_gate_capture", *FILES]
    env = {"PB_TOOLS_ROOT": str(tools), "PYTHONPATH": str(tools),
           "PB_CONSUMER_COMMIT": commit, "PB_CONSUMER_RUNTIME": route["id"],
           "PYTEST_ADDOPTS": "", "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "",
           "OMP_NUM_THREADS": "1", "MKL_NUM_THREADS": "1", "OPENBLAS_NUM_THREADS": "1"}
    return command, env


def record_path(generation):
    return generation.parent / (generation.name + ".consumer-gate.json")


def policy(cfg):
    return {k: v for k, v in cfg.items() if k != "accepted_breaks"}


def write_record(path, value):
    # First writer wins; interrupted execution retains a refusing record.
    raw = (json.dumps(value, sort_keys=True) + "\n").encode()
    with open(path, "xb") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    os.chmod(path, 0o444)
    descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


@contextmanager
def client_events(root):
    """Own a write-only event pipe and drain it independently of client logs."""
    decoder = client_channel.ClientEventDecoder(queue_root=root / "pb-queue", cas_root=root / "cas")
    read_fd, write_fd = os.pipe2(os.O_CLOEXEC | os.O_NONBLOCK)
    stopped = threading.Event()

    def drain():
        try:
            while decoder.problem is None:
                # At client exit, drain buffered bytes even if an earlier
                # select timed out just before the final events arrived.
                finishing = stopped.is_set()
                if not finishing and not select.select([read_fd], [], [], 0.05)[0]:
                    continue
                allowance = client_channel.MAX_CLIENT_EVENTS * client_channel.MAX_EVENT_BYTES + 1 - decoder.bytes_seen
                try:
                    raw = os.read(read_fd, allowance)
                except BlockingIOError:
                    if finishing:
                        break
                    continue
                if not raw:
                    break
                decoder.feed(raw)
        except OSError as exc:
            decoder.problem = f"client event pipe could not be read: {exc}"
        finally:
            os.close(read_fd)

    reader = threading.Thread(target=drain, name="consumer-gate-client-events", daemon=True)
    try:
        reader.start()
    except BaseException:
        os.close(read_fd)
        os.close(write_fd)
        raise
    try:
        yield write_fd, decoder
    finally:
        os.close(write_fd)
        stopped.set()
        reader.join()


def execute(generation):
    """Run both actions while admission is open; do not interpret submitter success."""
    cfg = selected(generation)
    if cfg is None:
        return
    generation = Path(generation)
    try:
        commit = clean_pin(cfg)
        if generation != Path(cfg["fleet_root"]) / "runtime-generations" / generation.name:
            refuse("configured fleet root differs from generation store")
        path = record_path(generation)
        if path.exists():
            refuse("execution record already exists; retain candidate and inspect its evidence")
        def submit(route):
            if clean_pin(cfg) != commit:
                refuse("consumer HEAD moved before sealing")
            command, env = recipe(cfg, generation, route, commit)
            argv = [sys.executable, cfg["pbrun"], "--cwd", cfg["repository"],
                    "--transport", "pool", "--cpus", str(route["cpus"]),
                    "--demand", "mem_gb=" + str(route["mem_gb"])]
            for tag in route["tags"]:
                argv += ["--tag", tag]
            for key, value in env.items():
                argv += ["--env", key + "=" + value]
            # Private events identify the exact publication, never test stdout.
            root = Path(cfg["fleet_root"])
            with client_events(root) as (event_fd, decoder):
                result = subprocess.run([*argv, "--client-events-fd", str(event_fd), "--", *command],
                                        text=True, capture_output=True, check=False,
                                        pass_fds=(event_fd,),
                                        env={**os.environ, "PRISMABUILD_SUBMISSION_FLEET_ROOT": cfg["fleet_root"]})
            channel = decoder.finish(result.returncode)
            identity = channel.identity
            valid = channel.problem is None and isinstance(identity, client.PoolClientIdentity)
            return {"runtime": route["id"],
                    "action_key": identity.action_key if valid else None,
                    "published_unix": identity.published_unix if valid else None,
                    "channel_problem": channel.problem,
                    "submitter_returncode": result.returncode,
                    "stdout": result.stdout, "stderr": result.stderr}
        # Both legs must execute even if the first fails. Concurrent attached
        # clients also avoid serializing the two independent fleet placements.
        with ThreadPoolExecutor(max_workers=2) as executor:
            actions = list(executor.map(submit, cfg["runtimes"]))
        if clean_pin(cfg) != commit:
            refuse("consumer HEAD moved during execution")
        write_record(path, {"schema": "prismabuild.consumer_gate.execution.v1",
                     "generation": generation.name, "consumer_commit": commit,
                     "policy": policy(cfg), "receipt_sha256": digest(generation / "RUNTIME_VERSION.json"),
                     "actions": actions})
    except (OSError, ValueError, TypeError, KeyError, pb.PrismaBuildError) as exc:
        refuse("execution refused: " + str(exc))


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def judge(log, *, cfg, generation, route, commit, returncode, host, required_nodes=()):
    rows = [decode(line[len(PREFIX):].encode()) for line in log.splitlines() if line.startswith(PREFIX)]
    if len(rows) != 1:
        raise ValueError("missing or duplicate complete consumer outcomes")
    row = rows[0]
    if not isinstance(row, dict):
        raise ValueError("outcomes must be an object")
    command, _ = recipe(cfg, generation, route, commit)
    expected = {"schema": "prismabuild.consumer_gate.outcomes.v1", "generation": generation.name,
                "receipt_sha256": digest(generation / "RUNTIME_VERSION.json"),
                "consumer_commit": commit, "runtime": route["id"], "python": route["python"],
                "args": command[3:], "exitstatus": returncode, "host": host,
                "executable_path_matches_running": True,
                "capture_path": str(generation / "tools/consumer_gate_capture.py"),
                "capture_sha256": digest(generation / "tools/consumer_gate_capture.py")}
    if any(row.get(k) != v for k, v in expected.items()):
        raise ValueError("outcomes identity/argv/exit/host mismatch")
    if not row.get("python_version", "").startswith(route["python_version"] + "."):
        raise ValueError("actual Python version differs from configured runtime")
    if not re.fullmatch(r"[a-f0-9]{64}", row.get("python_sha256", "")) or not Path(row.get("python_realpath", "")).is_absolute():
        raise ValueError("missing actual interpreter identity")
    machines = {"x86": {"x86_64", "AMD64"}, "gb10": {"aarch64", "arm64"}}
    if not any(row.get("machine") in machines[t] for t in route["tags"] if t in machines):
        raise ValueError("actual runtime architecture differs")
    nodes = row.get("collected")
    if not isinstance(nodes, list) or not nodes or not all(isinstance(n, str) for n in nodes) or len(set(nodes)) != len(nodes):
        raise ValueError("empty or duplicate collection")
    if not set(required_nodes).issubset(nodes):
        raise ValueError("available port omits observed failing nodes")
    if row.get("deselected") != [] or row.get("collection_errors") != []:
        raise ValueError("partial collection/deselection is not an acceptable break")
    if {n.split("::")[0] for n in nodes} != set(FILES):
        raise ValueError("collection does not cover exactly the requested files")
    reports = row.get("reports")
    if not isinstance(reports, list) or any(not isinstance(r, dict) or r.get("nodeid") not in nodes for r in reports):
        raise ValueError("outcomes not bound to collection")
    failures = []
    for node in nodes:
        phases = [r for r in reports if r["nodeid"] == node]
        if (len({r.get("when") for r in phases}) != len(phases)
                or any(r.get("wasxfail") or r.get("outcome") not in ("passed", "failed") for r in phases)):
            raise ValueError("duplicate/skip/xfail/invalid outcome")
        by_phase = {r["when"]: r["outcome"] for r in phases}
        if set(by_phase) not in ({"setup", "call", "teardown"}, {"setup", "teardown"}):
            raise ValueError("incomplete outcomes")
        if "call" not in by_phase and by_phase["setup"] != "failed":
            raise ValueError("missing test call")
        if "failed" in by_phase.values():
            failures.append(node)
    if returncode != (1 if failures else 0):
        raise ValueError("terminal return code disagrees with complete outcomes")
    return sorted(failures)


def verify_action(reference, *, cfg, generation, route, commit, required_nodes=()):
    """Independently bind request, snapshot bytes, terminal, and durable logs."""
    root = Path(cfg["fleet_root"])
    if not isinstance(reference, dict):
        raise ValueError("action key and publication identity are required")
    key = reference.get("action_key")
    published = reference.get("published_unix")
    if not isinstance(key, str) or not re.fullmatch(r"[a-f0-9]{64}", key):
        raise ValueError("a full action key is required")
    if type(published) not in (int, float) or not math.isfinite(published) or published <= 0:
        raise ValueError("exact queue publication identity required")
    q = pool.PoolQueue(root / "pb-queue")
    terminals = q.archived_generation_outcomes(key, generation=published)
    if len(terminals) != 1:
        raise ValueError("missing or ambiguous terminal action")
    _, done = terminals[0]
    cas = pb.PrismaBuildCAS(root / "cas")
    action = pb.validate_action(decode((root / "cas" / "requests" / key[:2] / (key + ".json")).read_bytes()))
    if action["action_key"] != key:
        raise ValueError("substituted request")
    action_result.bind_standard_capture_command(action)
    if action["task"]["working_directory"] not in ("", "."):
        raise ValueError("substituted execution directory")
    command, env = recipe(cfg, generation, route, commit)
    params = action["params"]
    if params["command"] != command or params["cwd"] not in (".", ""):
        raise ValueError("substituted command or cwd")
    variables = action["environment"]["variables"]
    if any(variables.get(k) != v for k, v in env.items()):
        raise ValueError("substituted generation/runtime environment")
    if (params["placement"]["required_tags"] != route["tags"]
            or params["demand"].get("cpu") != route["cpus"]
            or params["demand"].get("mem_gb") != route["mem_gb"]):
        raise ValueError("substituted placement/demand")
    snapshot = pb.validate_pbrun_checkout_snapshot(params["checkout_snapshot"])
    if snapshot.get("subdirectory") not in (None, "", "."):
        raise ValueError("consumer snapshot must cover repository root")
    if snapshot["schema"] != pb.PBRUN_CHECKOUT_SNAPSHOT_SCHEMA_V2 or snapshot["parent"] != commit:
        raise ValueError("snapshot parent differs from retained consumer HEAD")
    bundle = cas.input_path(snapshot["input"])
    # Bundles include ancestry; inspect in isolation, never import objects into
    # the configured live checkout during activation-time verification.
    with tempfile.TemporaryDirectory(prefix="consumer-gate-verify.") as temporary:
        git(temporary, "init", "--bare", "-q")
        git(temporary, "bundle", "unbundle", str(bundle))
        if git(temporary, "show", "-s", "--format=%P", snapshot["commit"]) != commit:
            raise ValueError("actual snapshot ancestry differs from pin")
        closure = action["code_closure"]["files"]
        if len(closure) != 1 or not re.fullmatch(r"\.pbrun-closure\.[0-9a-f]+\.json", closure[0]["path"]):
            raise ValueError("missing unique pbrun closure stamp")
        stamp = closure[0]["path"]
        changed = git(temporary, "diff-tree", "--no-commit-id", "-r", "--no-renames", "--name-only", commit, snapshot["commit"]).splitlines()
        if changed != [stamp]:
            raise ValueError("sealed consumer tree differs from pinned HEAD")
        if git(temporary, "ls-tree", commit, "--", stamp):
            raise ValueError("closure stamp overwrites consumer content")
        raw = subprocess.run(["git", "-C", temporary, "show", snapshot["commit"] + ":" + stamp],
                             capture_output=True, check=True).stdout
        if hashlib.sha256(raw).hexdigest() != closure[0]["sha256"] or len(raw) != closure[0]["bytes"]:
            raise ValueError("snapshot closure metadata differs from sealed code closure")
    attempts = q.attempt_outcomes(done)
    if not attempts:
        raise ValueError("no immutable terminal attempt/log evidence")
    attempt = attempts[-1]
    detail = attempt["detail"]
    rc = detail.get("returncode")
    if type(rc) is not int or rc not in (0, 1) or attempt.get("disposition") not in ("done", "failed"):
        raise ValueError("not a completed pytest test action")
    if rc == 1 and (attempt.get("disposition") != "failed" or done.get("status") != "failed"):
        raise ValueError("failed test action lacks a terminal failed attempt")
    if rc == 0:
        if attempt.get("disposition") != "done" or done.get("status") != "executed":
            raise ValueError("passing action did not finish executed")
        announcement = action_result.decode_result_announcement(attempt["stdout"].encode())
        receipt = cas.lookup_execution(action, announcement["receipt"]["receipt_sha256"])
        log = cas.result_path(receipt, action).read_text()
    else:
        # Failed actions have no successful CAS result. attempt_outcomes verifies
        # canonical links, byte counts and SHA-256 of each immutable log.
        log = attempt["stdout"]
    return judge(log, cfg=cfg, generation=generation, route=route, commit=commit,
                 returncode=rc, host=attempt.get("finished_host", done.get("finished_host")), required_nodes=required_nodes)


def verify(generation):
    """No submission here: safe for activation, bootstrap and barrier rechecks."""
    cfg = selected(generation)
    if cfg is None:
        return
    generation = Path(generation)
    try:
        record = decode(immutable_files.read_immutable_file(record_path(generation), where="consumer gate execution"))
        if not isinstance(record, dict):
            raise ValueError("execution record must be an object")
        if (record.get("schema") != "prismabuild.consumer_gate.execution.v1"
                or record.get("generation") != generation.name or record.get("policy") != policy(cfg)
                or record.get("receipt_sha256") != digest(generation / "RUNTIME_VERSION.json")):
            raise ValueError("stale or substituted execution record")
        commit = record["consumer_commit"]
        if not isinstance(commit, str) or not re.fullmatch(r"[a-f0-9]{40,64}", commit):
            raise ValueError("invalid retained consumer pin")
        actions = record["actions"]
        if not isinstance(actions, list) or len(actions) != 2 or any(not isinstance(a, dict) for a in actions) or {a["runtime"] for a in actions} != {r["id"] for r in cfg["runtimes"]}:
            raise ValueError("missing runtime executions")
        failures = {}
        for route in cfg["runtimes"]:
            reference = next(a for a in actions if a["runtime"] == route["id"])
            failures[route["id"]] = verify_action(reference, cfg=cfg, generation=generation, route=route, commit=commit)
        if not any(failures.values()):
            return
        for acceptance in cfg["accepted_breaks"]:
            if (not isinstance(acceptance, dict) or set(acceptance) != {"accepted_by", "generation", "consumer_commit", "failures", "port_commit", "port_actions"}
                    or acceptance["accepted_by"] not in ("Rob", "TrustLadder lead")
                    or acceptance["generation"] != generation.name or acceptance["consumer_commit"] != commit
                    or acceptance["failures"] != failures):
                continue
            port = acceptance["port_commit"]
            if not isinstance(port, str) or not re.fullmatch(r"[a-f0-9]{40,64}", port) or port == commit:
                continue
            # Already available locally, not a promise or remote branch label.
            git(cfg["repository"], "cat-file", "-e", port + "^{commit}")
            git(cfg["repository"], "merge-base", "--is-ancestor", commit, port)
            git(cfg["repository"], "merge-base", "--is-ancestor", port, "refs/heads/trustladder-deploy")
            if set(acceptance["port_actions"]) != set(failures):
                continue
            for route in cfg["runtimes"]:
                if verify_action(acceptance["port_actions"][route["id"]], cfg=cfg, generation=generation, route=route, commit=port, required_nodes=failures[route["id"]]):
                    raise ValueError("available port does not pass against this generation")
            return
        raise ValueError("observed consumer failures have no exact, proven accepted-break record: " + json.dumps(failures))
    except (OSError, ValueError, TypeError, KeyError, StopIteration, subprocess.SubprocessError, pb.PrismaBuildError, pool.PoolContractError) as exc:
        refuse("verified evidence required before activation: " + str(exc))
