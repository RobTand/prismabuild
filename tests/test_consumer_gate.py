"""Private consumer Git/queue/CAS evidence; no external adapters or live release."""
import copy
import json
import os
from pathlib import Path
import socket
import shutil
import subprocess
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools/fleet"))
sys.path.insert(0, str(ROOT / "src"))
import consumer_gate as gate
from prismabuild import core as pb, pool, submission, movement_actions
import pbmcp_fixture as fx


@pytest.fixture
def case(tmp_path, monkeypatch):
    for name in ("PRISMABUILD_ACTION_NONCE", "PRISMABUILD_ACTION_SCOPE", "PRISMABUILD_READER_HELPER_ROOT"):
        monkeypatch.delenv(name, raising=False)
    repository = tmp_path / "consumer"
    repository.mkdir()
    for name in gate.FILES:
        path = repository / name
        path.parent.mkdir(exist_ok=True)
        path.write_text("def test_adapter():\n    assert True\n")
    def git(*args):
        return gate.git(repository, *args)
    git("init", "-q")
    git("add", ".")
    git("-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit", "-qm", "consumer")
    generation = tmp_path / "runtime-generations/G"
    generation.mkdir(parents=True)
    (generation / "tools").mkdir()
    shutil.copy2(ROOT / "tools/fleet/consumer_gate_capture.py", generation / "tools/consumer_gate_capture.py")
    (generation / "RUNTIME_VERSION.json").write_text('{"generation":"G"}')
    cfg = {"schema": gate.SCHEMA, "repository": str(repository), "fleet_root": str(tmp_path),
           "pbrun": str(tmp_path / "tools/pbrun.py"), "accepted_breaks": [],
           "runtimes": [{"id": "x86", "python": "/fixture/x86/python", "python_version": "3.14", "tags": ["x86"], "cpus": 1, "mem_gb": 2},
                        {"id": "sparks", "python": "/fixture/arm/python", "python_version": "3.12", "tags": ["gb10"], "cpus": 1, "mem_gb": 2}]}
    config_path = tmp_path / "config.json"
    monkeypatch.setenv("PRISMABUILD_CONSUMER_GATE_CONFIG", str(config_path))
    def save():
        config_path.write_text(json.dumps(cfg))
    save()
    return cfg, generation, git, save


def outcomes(cfg, generation, route, commit, *, failed=False):
    nodes = [name + "::test_adapter" for name in gate.FILES]
    command, _ = gate.recipe(cfg, generation, route, commit)
    return {"schema": "prismabuild.consumer_gate.outcomes.v1", "generation": generation.name,
            "receipt_sha256": gate.digest(generation / "RUNTIME_VERSION.json"),
            "consumer_commit": commit, "runtime": route["id"], "python": route["python"],
            "python_realpath": route["python"], "python_sha256": "a" * 64,
            "executable_path_matches_running": True,
            "capture_path": str(generation / "tools/consumer_gate_capture.py"),
            "capture_sha256": gate.digest(generation / "tools/consumer_gate_capture.py"),
            "python_version": route["python_version"] + ".0", "machine": "x86_64" if route["id"] == "x86" else "aarch64",
            "host": socket.gethostname(), "args": command[3:], "collected": nodes,
            "collection_errors": [], "deselected": [], "exitstatus": int(failed),
            "reports": [{"nodeid": node, "when": phase, "outcome": "failed" if failed and node == nodes[0] and phase == "call" else "passed", "wasxfail": False}
                        for node in nodes for phase in ("setup", "call", "teardown")]}


def evidence(cfg, generation, route, commit, *, failed=False, alter=None):
    """Real synthetic pbrun snapshot, sealed request, CAS and immutable attempts."""
    root = Path(cfg["fleet_root"])
    repo = Path(cfg["repository"])
    cas = pb.PrismaBuildCAS(root / "cas")
    snapshot = submission.snapshot_checkout(repo, cas=cas,
        stamp_name=".pbrun-closure.abc123.json", stamp_payload="{}")
    command, env = gate.recipe(cfg, generation, route, commit)
    env["PATH"] = "/usr/bin:/bin"
    body = {"schema": pb.ACTION_SCHEMA_V2,
        "task": {"definition_id": "tests/consumer-gate", "definition_version": "v1",
                 "task_class": "generation", "determinism": "deterministic", "artifact_family": "generic", "artifact_kind": "generic",
                 "argv": movement_actions.standard_capture_argv(command, "result.txt", path_prefix="/usr/bin"),
                 "working_directory": ".", "result_path": "result.txt"},
        "inputs": [snapshot["input"]], "code_closure": submission.build_stamp_closure(".pbrun-closure.abc123.json", "{}"),
        "params": {"command": command, "cwd": ".", "demand": {"cpu": 1, "mem_gb": 2},
                   "placement": {"required_tags": route["tags"]}, "checkout_snapshot": snapshot},
        "environment": {"variables": env, "toolchain": {}},
        "execution_scope": {"portability": "portable", "platform_key": None, "host_class": None}}
    if alter:
        alter(body)
    action = pb.seal_action(body)
    key = action["action_key"]
    cas.publish_action_request(action)
    log = gate.PREFIX + json.dumps(outcomes(cfg, generation, route, commit, failed=failed)) + "\n"
    if not failed:
        materialized = root / ("materialized-" + key)
        subprocess.run(["git", "clone", "-q", str(cas.input_path(snapshot["input"])), str(materialized)], check=True)
        gate.git(materialized, "checkout", "-q", snapshot["commit"])
        attestation = pb.preflight_action(action, cas_root=cas.root, checkout_root=materialized)
        output = root / (route["id"] + ".txt")
        output.write_text(log)
        receipt, _ = cas.publish_result(action, output, attestation=attestation, return_execution_receipt=True)
        log = json.dumps({"receipt": receipt}) + "\n"
    queue = pool.PoolQueue(root / "pb-queue")
    queue.records.ensure_layout()
    queue.publish(action_key=key, cas_root=root / "cas", checkout_root=repo,
                  worker_script=root / "worker.py", tags=route["tags"], resources={"cpu": 1, "mem_gb": 2}, max_attempts=1)
    claimed = queue.claim(tags=route["tags"], capacity={"cpu": 1, "mem_gb": 2})
    queue.finish(key, status="failed" if failed else "executed",
                 detail={"returncode": int(failed), "status": "failed" if failed else "executed", "stdout": log, "stderr": ""})
    fx.settle_publishers()
    return {"action_key": key, "published_unix": claimed["published_unix"]}


def execution_record(cfg, generation, commit, keys):
    gate.write_record(gate.record_path(generation), {
        "schema": "prismabuild.consumer_gate.execution.v1", "generation": generation.name,
        "consumer_commit": commit, "policy": gate.policy(cfg),
        "receipt_sha256": gate.digest(generation / "RUNTIME_VERSION.json"),
        "actions": [{"runtime": route["id"], **key} for route, key in zip(cfg["runtimes"], keys)]})


def test_valid_evidence_for_both_runtimes_allows_activation(case):
    cfg, generation, git, _ = case
    commit = git("rev-parse", "HEAD")
    keys = [evidence(cfg, generation, r, commit) for r in cfg["runtimes"]]
    execution_record(cfg, generation, commit, keys)
    gate.verify(generation)


@pytest.mark.parametrize("field,value", [("generation", "other"), ("python_version", "3.11.0"), ("machine", "wrong"), ("args", []), ("receipt_sha256", "b" * 64), ("collected", []), ("reports", []), ("deselected", ["hidden"]), ("collection_errors", [{"nodeid": "hidden"}])])
def test_outcomes_tampering_refuses(case, field, value):
    cfg, generation, git, _ = case
    route = cfg["runtimes"][0]
    commit = git("rev-parse", "HEAD")
    record = outcomes(cfg, generation, route, commit)
    record[field] = value
    with pytest.raises(ValueError):
        gate.judge(gate.PREFIX + json.dumps(record), cfg=cfg, generation=generation,
                   route=route, commit=commit, returncode=0, host=socket.gethostname())


def test_missing_cas_receipt_is_not_passing_evidence(case):
    cfg, generation, git, _ = case
    commit = git("rev-parse", "HEAD")
    key = evidence(cfg, generation, cfg["runtimes"][0], commit)
    cas = pb.PrismaBuildCAS(Path(cfg["fleet_root"]) / "cas")
    # Fail closed even with a successful terminal and immutable attempt logs.
    from unittest.mock import patch
    with patch.object(pb.PrismaBuildCAS, "lookup_execution", side_effect=ValueError("no CAS receipt")):
        with pytest.raises(ValueError, match="CAS receipt"):
            gate.verify_action(key, cfg=cfg, generation=generation, route=cfg["runtimes"][0], commit=commit)


def test_failed_attempt_and_available_port_acceptance(case):
    cfg, generation, git, save = case
    commit = git("rev-parse", "HEAD")
    keys = [evidence(cfg, generation, r, commit, failed=True) for r in cfg["runtimes"]]
    execution_record(cfg, generation, commit, keys)
    with pytest.raises(SystemExit, match="no exact, proven"):
        gate.verify(generation)
    path = Path(cfg["repository"]) / gate.FILES[0]
    path.write_text(path.read_text() + "# available adapter port\n")
    git("add", ".")
    git("-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit", "-qm", "port")
    port = git("rev-parse", "HEAD")
    git("branch", "trustladder-deploy", port)
    port_keys = {r["id"]: evidence(cfg, generation, r, port) for r in cfg["runtimes"]}
    cfg["accepted_breaks"] = [{"accepted_by": "Rob", "generation": generation.name, "consumer_commit": commit,
        "failures": {r["id"]: [gate.FILES[0] + "::test_adapter"] for r in cfg["runtimes"]},
        "port_commit": port, "port_actions": port_keys}]
    save()
    gate.verify(generation)
    cfg["accepted_breaks"][0]["failures"]["x86"] = []
    save()
    with pytest.raises(SystemExit, match="no exact, proven"):
        gate.verify(generation)


def test_substituted_wrapper_refuses(case):
    cfg, generation, git, _ = case
    commit = git("rev-parse", "HEAD")
    route = cfg["runtimes"][0]
    key = evidence(cfg, generation, route, commit,
                   alter=lambda body: body["task"].update(argv=["/bin/true"]))
    with pytest.raises(Exception, match="standard captured-log"):
        gate.verify_action(key, cfg=cfg, generation=generation, route=route, commit=commit)


def test_sealed_policy_cannot_be_disabled_or_substituted(case, monkeypatch):
    cfg, generation, _, save = case
    receipt = generation / "RUNTIME_VERSION.json"
    receipt.write_text(json.dumps({"generation": "G", **gate.requirement()}))
    monkeypatch.delenv("PRISMABUILD_CONSUMER_GATE_CONFIG")
    with pytest.raises(SystemExit, match="sealed candidate requires"):
        gate.verify(generation)


def test_stale_publication_refuses(case):
    cfg, generation, git, _ = case
    commit = git("rev-parse", "HEAD")
    ref = evidence(cfg, generation, cfg["runtimes"][0], commit)
    ref["published_unix"] += 1
    with pytest.raises(ValueError, match="terminal action"):
        gate.verify_action(ref, cfg=cfg, generation=generation, route=cfg["runtimes"][0], commit=commit)


def test_snapshot_dirt_hidden_by_commit_label_refuses(case):
    cfg, generation, git, _ = case
    commit = git("rev-parse", "HEAD")
    (Path(cfg["repository"]) / gate.FILES[0]).write_text("# replaced source\n")
    route = cfg["runtimes"][0]
    ref = evidence(cfg, generation, route, commit, failed=True)
    with pytest.raises(ValueError, match="tree differs"):
        gate.verify_action(ref, cfg=cfg, generation=generation, route=route, commit=commit)


def test_snapshot_false_parent_refuses(case):
    cfg, generation, git, _ = case
    commit = git("rev-parse", "HEAD")
    route = cfg["runtimes"][0]
    ref = evidence(cfg, generation, route, commit, failed=True,
                   alter=lambda body: body["params"]["checkout_snapshot"].update(parent="a" * 40))
    with pytest.raises(ValueError, match="parent differs"):
        gate.verify_action(ref, cfg=cfg, generation=generation, route=route, commit=commit)


def test_corrupt_failed_attempt_log_refuses(case):
    cfg, generation, git, _ = case
    commit = git("rev-parse", "HEAD")
    route = cfg["runtimes"][0]
    ref = evidence(cfg, generation, route, commit, failed=True)
    queue = pool.PoolQueue(Path(cfg["fleet_root"]) / "pb-queue")
    _, terminal = queue.archived_generation_outcomes(ref["action_key"], generation=ref["published_unix"])[0]
    outcome = json.loads(queue.records.attempt_path(terminal, 1).read_text())
    log = queue.root / outcome["logs"]["stdout"]["path"]
    log.chmod(0o644)
    log.write_text("substituted output")
    log.chmod(0o444)
    with pytest.raises((pool.PoolContractError, pb.PrismaBuildError)):
        gate.verify_action(ref, cfg=cfg, generation=generation, route=route, commit=commit)


def test_port_cannot_delete_the_observed_failure(case):
    cfg, generation, git, _ = case
    route = cfg["runtimes"][0]
    commit = git("rev-parse", "HEAD")
    row = outcomes(cfg, generation, route, commit)
    with pytest.raises(ValueError, match="omits observed"):
        gate.judge(gate.PREFIX + json.dumps(row), cfg=cfg, generation=generation,
                   route=route, commit=commit, returncode=0, host=socket.gethostname(),
                   required_nodes=[gate.FILES[0] + "::test_deleted_failure"])


@pytest.mark.parametrize("prune", [False, True])
def test_external_checkout_capture_preserves_original_population(case, prune):
    cfg, generation, git, _ = case
    repo = Path(cfg["repository"])
    if prune:
        (repo / "conftest.py").write_text("def pytest_collection_modifyitems(items):\n    items.pop()\n")
    commit = git("rev-parse", "HEAD")
    route = {**cfg["runtimes"][0], "python": sys.executable,
             "python_version": ".".join(map(str, sys.version_info[:2]))}
    command, env = gate.recipe(cfg, generation, route, commit)
    # This subprocess is an already-admitted action child, not a coordinator
    # test run or recursive fleet submission.
    result = subprocess.run(command, cwd=repo, env={**os.environ, **env},
                            text=True, capture_output=True, check=False)
    if prune:
        with pytest.raises(ValueError, match="incomplete outcomes"):
            gate.judge(result.stdout, cfg=cfg, generation=generation, route=route,
                       commit=commit, returncode=result.returncode, host=socket.gethostname())
    else:
        assert gate.judge(result.stdout, cfg=cfg, generation=generation, route=route,
                          commit=commit, returncode=result.returncode, host=socket.gethostname()) == []
