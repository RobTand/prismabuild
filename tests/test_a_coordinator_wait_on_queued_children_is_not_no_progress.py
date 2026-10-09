"""A coordinator waiting on queued children is not stuck (#1666).

A coordinator that awaits a decomposed batch waits on admission while its
native children sit behind priority queues. That wait has no finite bound,
but the worker clamps each phase's stall allowance at the loop ceiling. The
coordinator declares the batch it awaits as a sealed value (parent and plan
keys) beside its progress phases. The worker's stall watch credits quiet only
when it verifies the link: every counted child's sealed request names that
parent and plan, and at least one such child is ready or claimed.

A reporter runs beside the coordinator. It reports one unit per distinct
child whose native CAS lookup verifies the receipt, the result blob digest
and manifest membership. Children already durable at start are a verified
baseline and count zero. A child with several tasks still counts one.
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from prismabuild import core as pb, decomposition as dc, pool  # noqa: E402
from prismabuild.durable_child_reporter import DurableChildReporter  # noqa: E402

from admitted_queue_fixture import AdmittedQueueFixture  # noqa: E402
from test_progress_keeps_a_working_action_alive import _policy  # noqa: E402

EVIDENCE = "cas:sha256:" + "0" * 64

#: A producer that reads its batch envelope and writes its result manifest.
PRODUCER = """
import hashlib, json, sys
batch = json.load(open(sys.argv[1]))
open(batch["result_manifest_path"], "w").write(json.dumps({
    "schema": "prismabuild.child_result_manifest.v1",
    "parent_key": batch["parent_key"],
    "plan_key": batch["plan_key"],
    "child_ordinal": batch["child_ordinal"],
    "results": [
        {"task_id": task["id"], "output_id": task["output_id"],
         "value_sha256": hashlib.sha256(
             json.dumps(task["payload"], sort_keys=True).encode()).hexdigest()}
        for task in batch["tasks"]
    ],
}))
"""

#: A coordinator that waits on its children and never reports itself.
COORDINATOR = """
import time
time.sleep(float(__SECONDS__))
open("result", "w").write("ok")
"""


def _request() -> dict:
    return {
        "schema": dc.LOGICAL_REQUEST_SCHEMA_V1,
        "common": {
            "argv": [sys.executable, "-c", PRODUCER, dc.TASK_BATCH_PLACEHOLDER],
            "cwd": ".",
            "demand": {"cpu": 1, "mem_gb": 1},
            "gpu_memory_gb": None,
            "data_manifest": None,
            "env": {},
        },
        "roster": {
            "schema": dc.LOGICAL_TASK_ROSTER_SCHEMA_V1,
            "tasks": [
                {
                    "id": f"t{index}",
                    "payload": {"v": index},
                    "residency_key": "r",
                    "estimated_seconds": 8.2,
                    "estimate_evidence": EVIDENCE,
                    "output_id": f"o{index}",
                }
                for index in range(3)
            ],
        },
        "batch_policy": {
            "schema": dc.ROSTER_BATCH_POLICY_SCHEMA_V1,
            "residencies": [
                {"key": "r", "setup_seconds": 1.0, "setup_evidence": EVIDENCE}
            ],
            "max_setup_fraction": 0.4,
            "max_estimated_wall_seconds": 300.0,
        },
    }


def _frozen() -> dict:
    return {
        "schema": dc.FROZEN_COMMON_SCHEMA_V1,
        "argv": [sys.executable, "-c", PRODUCER, dc.TASK_BATCH_PLACEHOLDER],
        "action_common": {
            "task": {
                "definition_id": "t/a", "definition_version": "v1",
                "task_class": "generation", "determinism": "deterministic",
                "artifact_family": "g", "artifact_kind": "g"},
            "params": {
                "cwd": ".", "demand": {"cpu": 1, "mem_gb": 1},
                "placement": {"required_tags": []}, "retry_policy": {},
                "checkout_snapshot": "s"},
            "inputs": [],
            "code_closure": {
                "schema": "x", "files": [],
                "closure_sha256": "0" * 64},
            "environment": {"variables": {}, "toolchain": {}},
            "execution_scope": {
                "portability": "portable", "platform_key": None,
                "host_class": None},
        },
    }


def _batch(tmp_path: Path):
    """One validated plan with sealed children in an isolated CAS."""

    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    request = dc.validate_logical_request(_request())
    plan = dc.build_plan(request, _frozen())
    prepared = dc.PreparedBatches(request, plan)
    checkout = tmp_path / "child-src"
    checkout.mkdir()
    (checkout / "t.py").write_text("x")
    roster_input, _ = cas.ingest_bytes(
        dc.document_bytes(request["roster"]), input_id=dc.TASK_ROSTER_INPUT_ID)
    children = []
    for ordinal in range(len(plan["partitions"])):
        envelope = prepared.envelope(ordinal)
        batch_input, _ = cas.ingest_bytes(
            dc.document_bytes(envelope), input_id=dc.TASK_BATCH_INPUT_ID)
        command = dc.resolve_task_batch(
            [sys.executable, "-c", PRODUCER, dc.TASK_BATCH_PLACEHOLDER],
            batch_path=str(cas.blob_path(str(batch_input["sha256"]))))
        child = pb.seal_action({
            "schema": pb.ACTION_SCHEMA_V2,
            "task": {
                "definition_id": "t/a", "definition_version": "v1",
                "task_class": "generation", "determinism": "deterministic",
                "artifact_family": "g", "artifact_kind": "g",
                "argv": command, "working_directory": ".",
                "result_path": dc.child_result_manifest_path(ordinal)},
            "inputs": [roster_input, batch_input],
            "code_closure": pb.build_code_closure(checkout, ["t.py"]),
            "params": {"logical_batch": prepared.membership(ordinal)},
            "environment": {"variables": {}, "toolchain": {}},
            "execution_scope": {
                "portability": "portable", "platform_key": None,
                "host_class": None},
        })
        cas.publish_action_request(child)
        children.append(child)
    return cas, plan, children


def _queue(tmp_path: Path, cas) -> AdmittedQueueFixture:
    return AdmittedQueueFixture(
        pool.PoolQueue(tmp_path / "queue"),
        capacity={"cpu": 8, "mem_gb": 16},
        default_demand={"cpu": 1, "mem_gb": 1})


def _coordinator(tmp_path: Path, cas, plan, *, seconds: float):
    """One claimed coordinator that declares the awaited batch."""

    checkout = tmp_path / "coordinator-src"
    checkout.mkdir()
    (checkout / "task.py").write_text(
        COORDINATOR.replace("__SECONDS__", repr(seconds)))
    policy = _policy(0.4, 0.4, 0.4)
    action = pb.seal_action({
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {
            "definition_id": "tests/coordinator",
            "definition_version": "v1",
            "task_class": "generation", "determinism": "deterministic",
            "artifact_family": "generic", "artifact_kind": "generic",
            "argv": [sys.executable, "task.py"], "working_directory": ".",
            "result_path": "result"},
        "inputs": [],
        "code_closure": pb.build_code_closure(checkout, ["task.py"]),
        "params": {
            pb.PROGRESS_PARAM: policy,
            pb.AWAITED_BATCH_PARAM: {
                "schema": pb.AWAITED_BATCH_SCHEMA_V1,
                "parent_key": plan["parent_key"],
                "plan_key": plan["plan_key"]}},
        "environment": {"variables": {}, "toolchain": {}},
        "execution_scope": {
            "portability": "portable", "platform_key": None,
            "host_class": None},
    })
    cas.publish_action_request(action)
    queue = _queue(tmp_path, cas)
    queue.publish(
        action_key=action["action_key"], cas_root=cas.root,
        checkout_root=checkout,
        worker_script=Path(__file__).resolve().parents[1]
        / "tools" / "prismabuild_worker.py")
    claimed = queue.claim()
    assert claimed is not None, "the coordinator must claim before its child publishes"
    assert claimed["action_key"] == action["action_key"], claimed
    return queue, claimed


def test_a_child_with_other_keys_earns_no_credit(tmp_path: Path) -> None:
    """A sealed request naming another batch is foreign, not awaited."""

    cas, plan, children = _batch(tmp_path)
    queue = _queue(tmp_path, cas)
    checkout = tmp_path / "foreign-src"
    checkout.mkdir()
    (checkout / "t.py").write_text("x")
    foreign = pb.seal_action({
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {
            "definition_id": "t/a", "definition_version": "v1",
            "task_class": "generation", "determinism": "deterministic",
            "artifact_family": "g", "artifact_kind": "g",
            "argv": ["/bin/false"], "working_directory": ".",
            "result_path": "r"},
        "inputs": [],
        "code_closure": pb.build_code_closure(checkout, ["t.py"]),
        "params": {"logical_batch": {
            "schema": dc.LOGICAL_BATCH_SCHEMA_V1,
            "parent_key": "0" * 64, "plan_key": plan["plan_key"],
            "roster_sha256": "0" * 64, "batch_policy_sha256": "0" * 64,
            "child_ordinal": 0, "ordered_task_ids": ["t0"]}},
        "environment": {"variables": {}, "toolchain": {}},
        "execution_scope": {
            "portability": "portable", "platform_key": None,
            "host_class": None},
    })
    cas.publish_action_request(foreign)
    queue.publish(
        action_key=foreign["action_key"], cas_root=cas.root,
        checkout_root=checkout, worker_script="/bin/true")
    item = {"action_key": "c" * 64, "cas_root": str(cas.root)}
    awaited = {"schema": pb.AWAITED_BATCH_SCHEMA_V1,
               "parent_key": plan["parent_key"], "plan_key": plan["plan_key"]}
    admitted = queue._awaited_child_submissions(item, awaited=awaited)
    assert admitted == {}
    verdict = queue.queued_child_wait_verdict(
        "c" * 64, cas_root=str(cas.root), awaited=awaited,
        admitted={foreign["action_key"]: {}})
    assert verdict["exempt"] is False
    assert verdict["children"][0]["state"] == "foreign"


def test_a_child_with_no_logical_batch_earns_no_credit(tmp_path: Path) -> None:
    """An ordinary action without membership is not an awaited child."""

    cas, plan, children = _batch(tmp_path)
    queue = _queue(tmp_path, cas)
    checkout = tmp_path / "plain-src"
    checkout.mkdir()
    (checkout / "t.py").write_text("x")
    plain = pb.seal_action({
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {
            "definition_id": "t/a", "definition_version": "v1",
            "task_class": "generation", "determinism": "deterministic",
            "artifact_family": "g", "artifact_kind": "g",
            "argv": ["/bin/false"], "working_directory": ".",
            "result_path": "r"},
        "inputs": [],
        "code_closure": pb.build_code_closure(checkout, ["t.py"]),
        "params": {},
        "environment": {"variables": {}, "toolchain": {}},
        "execution_scope": {
            "portability": "portable", "platform_key": None,
            "host_class": None},
    })
    cas.publish_action_request(plain)
    queue.publish(
        action_key=plain["action_key"], cas_root=cas.root,
        checkout_root=checkout, worker_script="/bin/true")
    awaited = {"schema": pb.AWAITED_BATCH_SCHEMA_V1,
               "parent_key": plan["parent_key"], "plan_key": plan["plan_key"]}
    verdict = queue.queued_child_wait_verdict(
        "c" * 64, cas_root=str(cas.root), awaited=awaited,
        admitted={plain["action_key"]: {}})
    assert verdict["exempt"] is False
    assert verdict["children"][0]["state"] == "foreign"


def test_an_unreadable_child_request_earns_no_credit(tmp_path: Path) -> None:
    """A child with no sealed request in the CAS earns no credit."""

    cas, plan, children = _batch(tmp_path)
    queue = _queue(tmp_path, cas)
    missing = "d" * 64
    awaited = {"schema": pb.AWAITED_BATCH_SCHEMA_V1,
               "parent_key": plan["parent_key"], "plan_key": plan["plan_key"]}
    verdict = queue.queued_child_wait_verdict(
        "c" * 64, cas_root=str(cas.root), awaited=awaited,
        admitted={missing: {}})
    assert verdict["exempt"] is False
    assert verdict["children"][0]["state"] == "foreign"


def test_a_coordinator_survives_a_queue_wait_past_twice_its_allowance(
        tmp_path: Path) -> None:
    """Quiet past twice the allowance, with a ready child, is not ended."""

    cas, plan, children = _batch(tmp_path)
    queue, item = _coordinator(tmp_path, cas, plan, seconds=2.5)
    checkout = tmp_path / "held-src"
    checkout.mkdir()
    (checkout / "t.py").write_text("x")
    queue.publish(
        action_key=children[0]["action_key"], cas_root=cas.root,
        checkout_root=checkout, worker_script="/bin/true")
    outcome = queue.execute(item, timeout_s=30.0, heartbeat_s=0.05,
                            timeout_grace_s=0.2)
    assert outcome["status"] == "executed", outcome
    # Quiet for 2.5 s against a 0.4 s phase allowance: over six allowances.
    assert outcome["elapsed_s"] > 2.0
    observed = outcome["progress_observation"]
    assert observed["queued_child_wait_exempt_s"] > 1.0
    assert observed["queued_child_wait"]["exempt"] is True


def test_a_coordinator_ends_no_progress_with_no_awaited_child_ready(
        tmp_path: Path) -> None:
    """With no awaited child ready or claimed, the allowance still ends it."""

    cas, plan, children = _batch(tmp_path)
    queue, item = _coordinator(tmp_path, cas, plan, seconds=30.0)
    outcome = queue.execute(item, timeout_s=30.0, heartbeat_s=0.05,
                            timeout_grace_s=0.2)
    assert outcome["status"] == "timeout", outcome
    assert outcome["termination_reason"] == "no_progress", outcome
    assert outcome["progress_observation"]["queued_child_wait_exempt_s"] == 0.0


def test_a_coordinator_ends_no_progress_once_the_child_is_terminal(
        tmp_path: Path) -> None:
    """Survival during the wait, then expiry once nothing is queued."""

    cas, plan, children = _batch(tmp_path)
    queue, item = _coordinator(tmp_path, cas, plan, seconds=30.0)
    checkout = tmp_path / "held-src"
    checkout.mkdir()
    (checkout / "t.py").write_text("x")
    queue.publish(
        action_key=children[0]["action_key"], cas_root=cas.root,
        checkout_root=checkout, worker_script="/bin/true")
    # Withdraw the child mid-wait: the coordinator survives the ready wait
    # first (credit accrues), then expires once nothing is queued.
    import threading
    stopped = threading.Event()

    def withdraw_soon() -> None:
        deadline = time.monotonic() + 15.0
        while time.monotonic() < deadline and not stopped.is_set():
            if queue.item_path(pool.READY, children[0]["action_key"]).exists():
                time.sleep(1.0)
                queue.withdraw(children[0]["action_key"], by="test-terminal")
                return
            time.sleep(0.05)

    worker = threading.Thread(target=withdraw_soon, daemon=True)
    worker.start()
    try:
        outcome = queue.execute(item, timeout_s=30.0, heartbeat_s=0.05,
                                timeout_grace_s=0.2)
    finally:
        stopped.set()
        worker.join()
    assert outcome["status"] == "timeout", outcome
    assert outcome["termination_reason"] == "no_progress", outcome
    observed = outcome["progress_observation"]
    assert observed["queued_child_wait_exempt_s"] > 0.0


def test_a_missing_child_with_a_verified_result_is_durable(tmp_path: Path) -> None:
    """A queue miss with a CAS receipt is durable, not missing."""

    cas, plan, children = _batch(tmp_path)
    queue = _queue(tmp_path, cas)
    worker = (Path(__file__).resolve().parents[1]
              / "tools" / "prismabuild_worker.py")
    checkout = tmp_path / "run-src"
    checkout.mkdir()
    (checkout / "t.py").write_text("x")
    queue.publish(
        action_key=children[0]["action_key"], cas_root=cas.root,
        checkout_root=checkout, worker_script=worker)
    outcome = queue.execute(queue.claim(), timeout_s=60.0, heartbeat_s=0.05,
                            timeout_grace_s=0.2)
    assert outcome["status"] == "executed", outcome
    queue.finish(children[0]["action_key"], status="executed", detail=outcome)
    # Remove the terminal record: the queue no longer names the child, but
    # the CAS still verifies its durable result.
    queue.item_path(pool.DONE, children[0]["action_key"]).unlink()
    awaited = {"schema": pb.AWAITED_BATCH_SCHEMA_V1,
               "parent_key": plan["parent_key"], "plan_key": plan["plan_key"]}
    verdict = queue.queued_child_wait_verdict(
        "c" * 64, cas_root=str(cas.root), awaited=awaited,
        admitted={children[0]["action_key"]: {}})
    (entry,) = verdict["children"]
    assert entry["state"] == "durable", verdict
    assert verdict["exempt"] is False


def test_the_reporter_counts_one_unit_per_newly_durable_child(
        tmp_path: Path) -> None:
    """Receipt, blob digest, schema, parent and plan match: one unit."""

    cas, plan, children = _batch(tmp_path)
    queue = _queue(tmp_path, cas)
    worker = (Path(__file__).resolve().parents[1]
              / "tools" / "prismabuild_worker.py")
    reporter = DurableChildReporter(
        cas=cas, parent_key=plan["parent_key"], plan_key=plan["plan_key"],
        child_keys=[child["action_key"] for child in children],
        child_requests={child["action_key"]: child for child in children},
        phase="run")
    assert reporter.establish_baseline() == set()
    commits: list = []
    for ordinal, child in enumerate(children):
        checkout = tmp_path / f"run-{ordinal}"
        checkout.mkdir()
        (checkout / "t.py").write_text("x")
        queue.publish(
            action_key=child["action_key"], cas_root=cas.root,
            checkout_root=checkout, worker_script=worker)
        outcome = queue.execute(queue.claim(), timeout_s=60.0,
                                heartbeat_s=0.05, timeout_grace_s=0.2)
        assert outcome["status"] == "executed", outcome
        fresh = reporter.newly_durable()
        assert [child["action_key"]] == fresh or fresh == [child["action_key"]]
        reporter.commit(
            commit=lambda units, phase, unit=None: commits.append(units))
    assert reporter.units == len(children)
    assert commits[-1] == float(len(children))


def test_children_durable_at_start_count_zero(tmp_path: Path) -> None:
    """The baseline is verified and counts zero, not new progress."""

    cas, plan, children = _batch(tmp_path)
    queue = _queue(tmp_path, cas)
    worker = (Path(__file__).resolve().parents[1]
              / "tools" / "prismabuild_worker.py")
    checkout = tmp_path / "run-src"
    checkout.mkdir()
    (checkout / "t.py").write_text("x")
    queue.publish(
        action_key=children[0]["action_key"], cas_root=cas.root,
        checkout_root=checkout, worker_script=worker)
    outcome = queue.execute(queue.claim(), timeout_s=60.0, heartbeat_s=0.05,
                            timeout_grace_s=0.2)
    assert outcome["status"] == "executed", outcome
    reporter = DurableChildReporter(
        cas=cas, parent_key=plan["parent_key"], plan_key=plan["plan_key"],
        child_keys=[child["action_key"] for child in children],
        child_requests={child["action_key"]: child for child in children},
        phase="run")
    assert reporter.establish_baseline() == {children[0]["action_key"]}
    assert reporter.newly_durable() == []
    assert reporter.units == 0


def test_a_non_verifying_child_counts_zero(tmp_path: Path) -> None:
    """Queue waits, logs and heartbeats are not progress; neither is this."""

    cas, plan, children = _batch(tmp_path)
    reporter = DurableChildReporter(
        cas=cas, parent_key=plan["parent_key"], plan_key=plan["plan_key"],
        child_keys=["e" * 64], child_requests={}, phase="run")
    assert reporter.establish_baseline() == set()
    assert reporter.newly_durable() == []
    assert reporter.units == 0


def test_awaited_batch_needs_progress_phases(tmp_path: Path) -> None:
    """The declaration is valid only with progress phases on pool transport."""

    checkout = tmp_path / "gate-src"
    checkout.mkdir()
    (checkout / "t.py").write_text("x")
    with pytest.raises(pb.ActionContractError, match="needs action.params.progress"):
        pb.seal_action({
            "schema": pb.ACTION_SCHEMA_V2,
            "task": {
                "definition_id": "t/a", "definition_version": "v1",
                "task_class": "generation", "determinism": "deterministic",
                "artifact_family": "g", "artifact_kind": "g",
                "argv": ["/bin/false"], "working_directory": ".",
                "result_path": "r"},
            "inputs": [],
            "code_closure": pb.build_code_closure(checkout, ["t.py"]),
            "params": {"progress_awaited_batch": {
                "schema": pb.AWAITED_BATCH_SCHEMA_V1,
                "parent_key": "a" * 64, "plan_key": "b" * 64}},
            "environment": {"variables": {}, "toolchain": {}},
            "execution_scope": {
                "portability": "portable", "platform_key": None,
                "host_class": None},
        })
