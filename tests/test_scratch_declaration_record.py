"""Real pool launch/evidence boundaries for declaration-only scratch records.

Fake broker/process endpoints do not run payloads, Docker or GPU work. Recording
naming evidence neither designates required cleanup nor authorizes deletion.
"""
from __future__ import annotations

import copy
import hashlib
import json
import sys

import pytest

from prismabuild import adaptive_snapshot, client, core as pb, pool, resource_scope

OPT_IN = "PRISMABUILD_EPHEMERAL_SCRATCH_DECLARATIONS"
SELECTED = [{"root_env": "TEMP_ROOT", "name": "row-temp"}]
FIELD = "scratch_declaration_record"


@pytest.fixture
def runtime(tmp_path, monkeypatch):
    calls = []
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    (checkout / "task.py").write_text("# fixture payload, never executed\n")

    def request(scope, op, **extra):
        calls.append(op)
        if op == "create":
            unit = "prismabuild-job" + hashlib.sha256(
                (scope.action_key + scope.nonce).encode()).hexdigest()[:32] + ".slice"
            return {"ok": True, "scope_id": unit, "token": "b" * 64,
                    "cgroup_path": "/sys/fs/cgroup/prismabuild.slice/" + unit}
        return {"ok": True}

    def sample(scope):
        value = {"action_key": scope.action_key, "nonce": scope.nonce,
                 "sampled_unix": pool._now(), "wall_seconds": 1.,
                 "cpu_seconds": .25, "memory_peak_bytes": 1024,
                 "memory_current_bytes": 0, "oom_kill": 0, "complete": True}
        resource_scope._atomic_json(scope.telemetry_path, value)
        return value

    monkeypatch.setattr(resource_scope.ResourceScope, "_request", request)
    monkeypatch.setattr(resource_scope.ResourceScope, "sample", sample)
    monkeypatch.setattr(pool.cpu_admission, "record_completion", lambda *args: None)

    def create(selection=json.dumps(SELECTED)):
        variables = {"PRISMABUILD_LOCAL_SCRATCH_PAIRS": "TEMP_ROOT:TEMP_MAX,CACHE_ROOT:CACHE_MAX",
                     "TEMP_ROOT": str(tmp_path / "temporary"), "TEMP_MAX": "1024",
                     "CACHE_ROOT": str(tmp_path / "persistent"), "CACHE_MAX": "2048"}
        if selection is not None:
            variables[OPT_IN] = selection
        demand = {"cpu": 1, "mem_gb": 2, "spool_gb": 2}
        action = pb.seal_action({
            "schema": pb.ACTION_SCHEMA_V2,
            "task": {"definition_id": "tests/scratch-record", "definition_version": "v1",
                     "task_class": "generation", "determinism": "deterministic",
                     "artifact_family": "generic", "artifact_kind": "generic",
                     "argv": [sys.executable, "task.py"], "working_directory": ".",
                     "result_path": "result"},
            "inputs": [], "code_closure": pb.build_code_closure(checkout, ["task.py"]),
            "params": {"demand": demand},
            "environment": {"variables": variables, "toolchain": {}},
            "execution_scope": {"portability": "portable", "platform_key": None,
                                "host_class": None},
        })
        cas = pb.PrismaBuildCAS(tmp_path / "cas")
        cas.publish_action_request(action)
        queue = pool.PoolQueue(tmp_path / "queue")
        queue.publish(action_key=str(action["action_key"]), cas_root=cas.root,
                      checkout_root=checkout, worker_script="/worker.py",
                      resources=demand, max_attempts=2, retry_safe=True)
        item = queue.claim(capacity=demand)
        assert item is not None
        return queue, item, variables

    return create, calls


def is_snapshot_helper(argv):
    """True when argv launches the adaptive-snapshot diagnostic copy (#1539).

    The helper is a diagnostic, not the payload: it may spawn before scratch
    registration, so the payload spy must pass it through uninspected.
    """
    return (isinstance(argv, (list, tuple)) and len(argv) >= 2
            and argv[1] == str(adaptive_snapshot._ENTRY))


def process(monkeypatch, inspect=None, *, returncode=0):
    class Process:
        pid = 999999999
        def __init__(self, argv, **kwargs):
            self.returncode = returncode
            if inspect is not None and not is_snapshot_helper(argv):
                inspect(argv, kwargs)
        def communicate(self, *, timeout):
            return "fixture output", ""
        def poll(self):
            return returncode
    monkeypatch.setattr(pool.subprocess, "Popen", Process)


def context(item):
    scope = item["resource_scope"]
    return {pb.ACTION_KEY_ENV: item["action_key"],
            pb.ACTION_NONCE_ENV: scope["nonce"], pb.ACTION_SCOPE_ENV: scope["scope_id"]}


def live_record(queue, item):
    return json.loads(queue.item_path(pool.CLAIMED, item["action_key"]).read_text())


def record(queue, item):
    return client.record_ephemeral_scratch_declarations(
        queue, claim_snapshot=item, env=context(item))


def test_executor_commits_selected_record_and_lease_before_launch(runtime, monkeypatch):
    create, calls = runtime
    queue, item, variables = create()
    launched = []

    def inspect(argv, kwargs):
        observed = live_record(queue, item)
        evidence = observed[FIELD]
        assert evidence["purpose"] == "declaration-only"
        assert evidence["cleanup_required"] is False
        assert len(evidence["declarations"]) == 1
        declaration = evidence["declarations"][0]
        assert declaration["root"] == variables["TEMP_ROOT"]
        assert declaration["owner_attempt"] == {
            key: observed["resource_scope"][key] for key in ("nonce", "scope_id")}
        assert declaration["name"] == "row-temp"
        lease = json.loads(queue.lease_path(item["action_key"]).read_text())
        assert lease[FIELD] == evidence
        assert "--token" in argv
        launched.append(True)

    process(monkeypatch, inspect)
    outcome = queue.execute(item, containment=True)
    assert outcome["status"] == "executed" and launched == [True]
    assert item[FIELD] == live_record(queue, item)[FIELD]
    assert queue.ledger().held() == {"cpu": 1, "mem_gb": 2, "spool_gb": 2}
    assert "create" in calls


@pytest.mark.parametrize("selection", [None, "", "[]"])
def test_off_keeps_ordinary_launch_and_no_declaration_record(runtime, monkeypatch, selection):
    create, calls = runtime
    queue, item, variables = create(selection)
    process(monkeypatch, lambda *_: None)
    assert queue.execute(item, containment=True)["status"] == "executed"
    assert FIELD not in live_record(queue, item)
    assert FIELD not in json.loads(queue.lease_path(item["action_key"]).read_text())


@pytest.mark.parametrize("selection", ["{", "null", "{}", "[{}]",
    '[{"root_env":"TEMP_ROOT","name":"../escape"}]',
    '[{"root_env":"UNKNOWN","name":"temp"}]',
    '[{"root_env":"TEMP_ROOT","name":"temp","cleanup_required":true}]',
    json.dumps(SELECTED * 2)])
def test_bad_sealed_selection_refuses_before_payload(runtime, monkeypatch, selection):
    create, calls = runtime
    queue, item, variables = create(selection)
    launched = []
    process(monkeypatch, lambda *_: launched.append(True))
    with pytest.raises((client.LocalScratchError, pool.PoolContractError, ValueError)):
        queue.execute(item, containment=True)
    assert not launched
    assert FIELD not in live_record(queue, item)
    assert queue.ledger().held() == {"cpu": 1, "mem_gb": 2, "spool_gb": 2}


def test_opt_in_without_containment_refuses_before_launch(runtime, monkeypatch):
    create, calls = runtime
    queue, item, variables = create()
    launched = []
    process(monkeypatch, lambda *_: launched.append(True))
    with pytest.raises((client.LocalScratchError, pool.PoolContractError, ValueError)):
        queue.execute(item, containment=False)
    assert not launched and "create" not in calls


def test_restart_and_idempotent_public_recording_preserve_charges(runtime, monkeypatch):
    create, calls = runtime
    queue, item, variables = create()
    process(monkeypatch)
    queue.execute(item, containment=True)
    before = live_record(queue, item)
    held = queue.ledger().held()
    restarted = pool.PoolQueue(queue.root)
    assert record(restarted, item) == before[FIELD]
    assert record(restarted, item) == before[FIELD]
    assert live_record(queue, item) == before
    assert queue.ledger().held() == held


@pytest.mark.parametrize("change", ["publication", "nonce", "resources"])
def test_stale_snapshot_cannot_write_successor(runtime, monkeypatch, change):
    create, calls = runtime
    queue, item, variables = create()
    process(monkeypatch)
    queue.execute(item, containment=True)
    stale = copy.deepcopy(item)
    if change == "publication":
        stale["published_unix"] += 1
    elif change == "resources":
        stale["resources"]["spool_gb"] += 1
    else:
        nonce = "2" * 32
        stale["resource_scope"]["nonce"] = nonce
        stale["resource_scope"]["scope_id"] = "prismabuild-job" + hashlib.sha256(
            (stale["action_key"] + nonce).encode()).hexdigest()[:32] + ".slice"
    before = queue.item_path(pool.CLAIMED, item["action_key"]).read_bytes()
    lease = queue.lease_path(item["action_key"]).read_bytes()
    with pytest.raises((client.LocalScratchError, pool.PoolContractError)):
        record(queue, stale)
    assert queue.item_path(pool.CLAIMED, item["action_key"]).read_bytes() == before
    assert queue.lease_path(item["action_key"]).read_bytes() == lease


@pytest.mark.parametrize("returncode", [0, 1])
def test_ending_archives_declarations_but_does_not_delete_or_carry_to_retry(
        runtime, monkeypatch, returncode):
    create, calls = runtime
    queue, item, variables = create()
    process(monkeypatch, returncode=returncode)
    queue.execute(item, containment=True)
    evidence = live_record(queue, item)[FIELD]
    root = client.ephemeral_scratch_path(evidence["declarations"][0])
    root.mkdir(parents=True)
    sentinel = root / "payload-temp"
    sentinel.write_bytes(b"audit-only, not cleanup ownership")
    outcome = {"status": "executed" if returncode == 0 else "failed", "returncode": returncode}
    queue.finish(item["action_key"], status=outcome["status"], detail=outcome, claim_snapshot=item)
    ending = pool._read_json(queue.item_path(pool.DONE if returncode == 0 else pool.READY,
                                           item["action_key"]))
    assert ending is not None
    history = ending["attempt_history"]
    assert isinstance(history, list) and isinstance(history[-1], dict)
    attempt = json.loads((queue.root / history[-1]["outcome"]).read_text())
    assert attempt["detail"][FIELD] == evidence
    assert queue.ledger().held() == {} and sentinel.read_bytes().startswith(b"audit-only")
    if returncode:
        assert FIELD not in ending


def test_mirror_failure_prevents_launch_and_replay_repairs_existing_claim(runtime, monkeypatch):
    create, calls = runtime
    queue, item, variables = create()
    launched = []
    process(monkeypatch, lambda *_: launched.append(True))
    original = pool._write_json_atomic

    def fail_mirror(path, value):
        if path == queue.lease_path(item["action_key"]) and FIELD in value:
            raise OSError("fixture lease mirror unavailable")
        return original(path, value)

    monkeypatch.setattr(pool, "_write_json_atomic", fail_mirror)
    with pytest.raises(OSError, match="fixture lease mirror unavailable"):
        queue.execute(item, containment=True)
    evidence = live_record(queue, item)[FIELD]
    assert FIELD not in json.loads(queue.lease_path(item["action_key"]).read_text())
    assert not launched and "release" not in calls
    held = queue.ledger().held()
    monkeypatch.setattr(pool, "_write_json_atomic", original)
    assert record(pool.PoolQueue(queue.root), item) == evidence
    assert json.loads(queue.lease_path(item["action_key"]).read_text())[FIELD] == evidence
    assert queue.ledger().held() == held


def test_conflicting_stored_declaration_refuses_without_overwrite(runtime, monkeypatch):
    create, calls = runtime
    queue, item, variables = create()
    process(monkeypatch)
    queue.execute(item, containment=True)
    live = live_record(queue, item)
    live[FIELD]["declarations"][0]["name"] = "other-valid-name"
    pool._write_json_atomic(queue.item_path(pool.CLAIMED, item["action_key"]), live)
    before = queue.item_path(pool.CLAIMED, item["action_key"]).read_bytes()
    lease = queue.lease_path(item["action_key"]).read_bytes()
    with pytest.raises(client.LocalScratchError, match="conflicting"):
        record(queue, item)
    assert queue.item_path(pool.CLAIMED, item["action_key"]).read_bytes() == before
    assert queue.lease_path(item["action_key"]).read_bytes() == lease


def test_successor_lease_refuses_before_registration_write(runtime, monkeypatch):
    create, calls = runtime
    queue, item, variables = create()
    process(monkeypatch)
    queue.execute(item, containment=True)
    lease = json.loads(queue.lease_path(item["action_key"]).read_text())
    lease["published_unix"] += 1
    pool._write_json_atomic(queue.lease_path(item["action_key"]), lease)
    before = queue.item_path(pool.CLAIMED, item["action_key"]).read_bytes()
    after = queue.lease_path(item["action_key"]).read_bytes()
    with pytest.raises(pool.PoolContractError):
        record(queue, item)
    assert queue.item_path(pool.CLAIMED, item["action_key"]).read_bytes() == before
    assert queue.lease_path(item["action_key"]).read_bytes() == after


def test_payload_cleanup_failure_preserves_declarations_and_completed_result(runtime, monkeypatch):
    create, calls = runtime
    queue, item, variables = create()
    process(monkeypatch)
    queue.execute(item, containment=True)
    evidence = live_record(queue, item)[FIELD]
    original = resource_scope.ResourceScope.release
    monkeypatch.setattr(resource_scope.ResourceScope, "release",
                        lambda scope: (_ for _ in ()).throw(OSError("fixture broker down")))
    path = queue.finish(item["action_key"], status="executed", detail={"returncode": 0},
                        claim_snapshot=item)
    assert path == queue.item_path(pool.CLAIMED, item["action_key"])
    assert live_record(queue, item)[FIELD] == evidence
    pending = live_record(queue, item)["finish_pending"]
    assert isinstance(pending, dict) and pending["status"] == "executed"
    assert queue.ledger().held() == {"cpu": 1, "mem_gb": 2, "spool_gb": 2}
    monkeypatch.setattr(resource_scope.ResourceScope, "release", original)
    queue.finish(item["action_key"], status="executed", detail={"returncode": 0},
                 claim_snapshot=item)
    terminal = pool._read_json(queue.item_path(pool.DONE, item["action_key"]))
    assert terminal is not None
    details = terminal["detail"]
    assert isinstance(details, dict) and details[FIELD] == evidence
    assert queue.ledger().held() == {}


def test_stale_reap_archives_old_declarations_and_strips_retry(runtime, monkeypatch):
    create, calls = runtime
    queue, item, variables = create()
    process(monkeypatch)
    queue.execute(item, containment=True)
    evidence = live_record(queue, item)[FIELD]
    assert queue.reap_stale(timeout_s=-1) == [item["action_key"]]
    ready = pool._read_json(queue.item_path(pool.READY, item["action_key"]))
    assert ready is not None and FIELD not in ready
    details = ready["detail"]
    assert isinstance(details, dict) and details[FIELD] == evidence
    assert queue.ledger().held() == {}


@pytest.mark.parametrize("selection", [
    '[{"root_env":"TEMP_ROOT","name":"one","name":"two"}]',
    '[{"root_env":"TEMP_ROOT","name":NaN}]',
    json.dumps([{"root_env": "TEMP_ROOT", "name": f"temp-{i}"} for i in range(65)]),
    " " * (16 * 1024 + 1),
], ids=["duplicate-key", "nonfinite", "too-many", "too-large"])
def test_bounded_strict_parser_cannot_launch_on_ambiguous_input(runtime, monkeypatch, selection):
    create, calls = runtime
    queue, item, variables = create(selection)
    launched = []
    process(monkeypatch, lambda *_: launched.append(True))
    with pytest.raises((client.LocalScratchError, pool.PoolContractError, ValueError)):
        queue.execute(item, containment=True)
    assert not launched and "create" not in calls
