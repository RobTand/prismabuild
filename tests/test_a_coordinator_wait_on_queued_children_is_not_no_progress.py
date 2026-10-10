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
    stored = cas.root / "decompositions" / plan["parent_key"][:2] / plan["parent_key"]
    stored.mkdir(parents=True, exist_ok=True)
    (stored / "plan.json").write_bytes(dc.document_bytes(plan))
    (stored / "publication.json").write_bytes(dc.document_bytes(
        dc.publication_index(
            plan, batch_input_digests=["0" * 64] * len(children),
            child_action_keys=[child["action_key"] for child in children])))
    return cas, plan, children


def _queue(tmp_path: Path, cas) -> AdmittedQueueFixture:
    return AdmittedQueueFixture(
        pool.PoolQueue(tmp_path / "queue"),
        capacity={"cpu": 8, "mem_gb": 16},
        default_demand={"cpu": 1, "mem_gb": 1})


def _controller_state(tmp_path: Path, children, *, name: str = "controller-state"):
    """An isolated controller-state directory naming these children."""

    state = tmp_path / name
    state.mkdir(parents=True, exist_ok=True)
    waves = {"waves": [{
        "wave": 1, "closed": False,
        "members": [
            {"batch": f"child-{ordinal:05d}",
             "key": child["action_key"]}
            for ordinal, child in enumerate(children)]}]}
    (state / "wave-state.json").write_text(json.dumps(waves))
    (state / "sub-keys.txt").write_text("".join(
        f"child-{ordinal:05d} {child['action_key']}\n"
        for ordinal, child in enumerate(children)))
    return state


def _awaited(plan, state: Path) -> dict:
    return {"schema": pb.AWAITED_BATCH_SCHEMA_V1,
            "parent_key": plan["parent_key"], "plan_key": plan["plan_key"],
            "controller_state": str(state)}


def _admitted(queue, cas, plan, state: Path, *, retained=None):
    item = {"action_key": "c" * 64, "cas_root": str(cas.root)}
    return queue._awaited_child_submissions(
        item, awaited=_awaited(plan, state), retained=retained)


def _coordinator(tmp_path: Path, cas, plan, children, *, seconds: float):
    """One claimed coordinator that declares the awaited batch."""

    state = _controller_state(tmp_path, children)
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
            pb.AWAITED_BATCH_PARAM: _awaited(plan, state)},
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
    return queue, claimed, state


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
    state = _controller_state(tmp_path, [children[0]], name="controller-foreign")
    awaited = _awaited(plan, state)
    admitted = _admitted(queue, cas, plan, state)
    assert set(admitted) == {children[0]["action_key"]}, admitted
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
    state = _controller_state(tmp_path, [children[0]], name="controller-plain")
    awaited = _awaited(plan, state)
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
    state = _controller_state(tmp_path, [children[0]], name="controller-missing")
    awaited = _awaited(plan, state)
    verdict = queue.queued_child_wait_verdict(
        "c" * 64, cas_root=str(cas.root), awaited=awaited,
        admitted={missing: {}})
    assert verdict["exempt"] is False
    assert verdict["children"][0]["state"] == "foreign"


def test_a_child_with_a_foreign_ordinal_earns_no_credit(tmp_path: Path) -> None:
    """A sealed ordinal outside its publication slot is foreign."""

    cas, plan, children = _batch(tmp_path)
    queue = _queue(tmp_path, cas)
    other = children[1] if len(children) > 1 else children[0]
    batch = dict(other["params"]["logical_batch"])
    checkout = tmp_path / "ordinal-src"
    checkout.mkdir()
    (checkout / "t.py").write_text("x")
    wrong = dict(other)
    wrong["params"] = {"logical_batch": {**batch, "child_ordinal": 0}}
    wrong.pop("action_key", None)
    sealed = pb.seal_action({k: v for k, v in wrong.items() if k != "action_key"})
    cas.publish_action_request(sealed)
    state = _controller_state(tmp_path, [children[0]], name="controller-ordinal")
    awaited = _awaited(plan, state)
    verdict = queue.queued_child_wait_verdict(
        "c" * 64, cas_root=str(cas.root), awaited=awaited,
        admitted={sealed["action_key"]: {}})
    assert verdict["exempt"] is False
    assert verdict["children"][0]["state"] == "foreign"


def test_a_child_with_foreign_tasks_earns_no_credit(tmp_path: Path) -> None:
    """A sealed task set outside its plan partition is foreign."""

    cas, plan, children = _batch(tmp_path)
    queue = _queue(tmp_path, cas)
    child = children[0]
    batch = dict(child["params"]["logical_batch"])
    wrong = dict(child)
    wrong["params"] = {"logical_batch": {
        **batch, "ordered_task_ids": ["no-such-task"]}}
    wrong.pop("action_key", None)
    sealed = pb.seal_action({k: v for k, v in wrong.items() if k != "action_key"})
    cas.publish_action_request(sealed)
    state = _controller_state(tmp_path, [children[0]], name="controller-tasks")
    awaited = _awaited(plan, state)
    verdict = queue.queued_child_wait_verdict(
        "c" * 64, cas_root=str(cas.root), awaited=awaited,
        admitted={sealed["action_key"]: {}})
    assert verdict["exempt"] is False
    assert verdict["children"][0]["state"] == "foreign"


def test_a_coordinator_survives_a_queue_wait_past_twice_its_allowance(
        tmp_path: Path) -> None:
    """Quiet past twice the allowance, with a ready child, is not ended."""

    cas, plan, children = _batch(tmp_path)
    queue, item, _state = _coordinator(
        tmp_path, cas, plan, [children[0]], seconds=2.5)
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
    queue, item, _state = _coordinator(
        tmp_path, cas, plan, [children[0]], seconds=30.0)
    outcome = queue.execute(item, timeout_s=30.0, heartbeat_s=0.05,
                            timeout_grace_s=0.2)
    assert outcome["status"] == "timeout", outcome
    assert outcome["termination_reason"] == "no_progress", outcome
    assert outcome["progress_observation"]["queued_child_wait_exempt_s"] == 0.0


def test_a_coordinator_ends_no_progress_once_the_child_is_terminal(
        tmp_path: Path) -> None:
    """Survival during the wait, then expiry once nothing is queued."""

    cas, plan, children = _batch(tmp_path)
    queue, item, _state = _coordinator(
        tmp_path, cas, plan, [children[0]], seconds=30.0)
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

def test_a_first_sighting_is_a_baseline_and_earns_no_credit(
        tmp_path: Path) -> None:
    """One live sample without a prior is a baseline, not a credit."""

    cas, plan, children = _batch(tmp_path)
    queue = _queue(tmp_path, cas)
    checkout = tmp_path / "held-src"
    checkout.mkdir()
    (checkout / "t.py").write_text("x")
    queue.publish(
        action_key=children[0]["action_key"], cas_root=cas.root,
        checkout_root=checkout, worker_script="/bin/true")
    state = _controller_state(tmp_path, [children[0]], name="controller-baseline")
    awaited = _awaited(plan, state)
    admitted = _admitted(queue, cas, plan, state)
    assert set(admitted) == {children[0]["action_key"]}, admitted
    first = queue.queued_child_wait_verdict(
        "c" * 64, cas_root=str(cas.root), awaited=awaited,
        admitted=admitted, prior=None)
    assert first["exempt"] is False
    assert first["children"][0]["evidence"] == "baseline"
    first["sample_monotonic"] = 100.0
    second = queue.queued_child_wait_verdict(
        "c" * 64, cas_root=str(cas.root), awaited=awaited,
        admitted=_admitted(queue, cas, plan, state, retained={
            key: {} for key in admitted}),
        prior=first)
    assert second["exempt"] is True
    assert second["children"][0]["evidence"] == "carried"
    assert second["since_monotonic"] == 100.0


def test_a_replacement_child_is_a_new_baseline(tmp_path: Path) -> None:
    """A live child that the prior never saw earns no credit yet."""

    cas, plan, children = _batch(tmp_path)
    queue = _queue(tmp_path, cas)
    assert len(children) >= 2, "the batch needs two children to replace one"
    checkout = tmp_path / "held-src"
    checkout.mkdir()
    (checkout / "t.py").write_text("x")
    queue.publish(
        action_key=children[0]["action_key"], cas_root=cas.root,
        checkout_root=checkout, worker_script="/bin/true")
    state = _controller_state(
        tmp_path, [children[0], children[1]], name="controller-replace")
    awaited = _awaited(plan, state)
    first = queue.queued_child_wait_verdict(
        "c" * 64, cas_root=str(cas.root), awaited=awaited,
        admitted=_admitted(queue, cas, plan, state),
        prior=None)
    assert first["exempt"] is False
    queue.withdraw(children[0]["action_key"], by="test-replace")
    queue.publish(
        action_key=children[1]["action_key"], cas_root=cas.root,
        checkout_root=checkout, worker_script="/bin/true")
    replacement = queue.queued_child_wait_verdict(
        "c" * 64, cas_root=str(cas.root), awaited=awaited,
        admitted=_admitted(queue, cas, plan, state),
        prior=first)
    assert replacement["exempt"] is False
    live = [entry for entry in replacement["children"]
            if entry["key"] == children[1]["action_key"]]
    assert live and live[0]["evidence"] == "baseline", replacement
    replacement["sample_monotonic"] = 200.0
    carried = queue.queued_child_wait_verdict(
        "c" * 64, cas_root=str(cas.root), awaited=awaited,
        admitted=_admitted(queue, cas, plan, state,
                           retained={children[1]["action_key"]: {}}),
        prior=replacement)
    assert carried["exempt"] is True
    assert carried["since_monotonic"] == 200.0


def test_a_missing_admitted_child_blocks_a_live_sibling(tmp_path: Path) -> None:
    """A retained member with no row stays in the verdict and earns none."""

    cas, plan, children = _batch(tmp_path)
    queue = _queue(tmp_path, cas)
    assert len(children) >= 2, "the batch needs two children for this"
    checkout = tmp_path / "held-src"
    checkout.mkdir()
    (checkout / "t.py").write_text("x")
    queue.publish(
        action_key=children[0]["action_key"], cas_root=cas.root,
        checkout_root=checkout, worker_script="/bin/true")
    queue.publish(
        action_key=children[1]["action_key"], cas_root=cas.root,
        checkout_root=checkout, worker_script="/bin/true")
    state = _controller_state(
        tmp_path, [children[0], children[1]], name="controller-missing-sibling")
    awaited = _awaited(plan, state)
    admitted = _admitted(queue, cas, plan, state)
    assert len(admitted) == 2, admitted
    queue.item_path(pool.READY, children[1]["action_key"]).unlink()
    retained = _admitted(queue, cas, plan, state, retained={
        key: {} for key in admitted})
    assert children[1]["action_key"] in retained, retained
    verdict = queue.queued_child_wait_verdict(
        "c" * 64, cas_root=str(cas.root), awaited=awaited,
        admitted=retained,
        prior={"children": [
            {"key": children[0]["action_key"], "state": pool.READY,
             "evidence": "baseline", "ordinal": 0,
             "parent_key": plan["parent_key"], "plan_key": plan["plan_key"]}]})
    assert verdict["exempt"] is False
    assert verdict["missing"] == [children[1]["action_key"]], verdict


def test_an_unreadable_ready_row_blocks_the_credit(tmp_path: Path) -> None:
    """Telemetry loss on a live child earns no credit (#1723).

    The child is ready at the baseline look, but its ready row stops
    parsing before the next one. The verdict reads unknown, keeps the
    child in missing, and refuses the credit: a carried prior never
    covers lost telemetry.
    """
    cas, plan, children = _batch(tmp_path)
    queue = _queue(tmp_path, cas)
    checkout = tmp_path / "held-src"
    checkout.mkdir()
    (checkout / "t.py").write_text("x")
    queue.publish(
        action_key=children[0]["action_key"], cas_root=cas.root,
        checkout_root=checkout, worker_script="/bin/true")
    state = _controller_state(
        tmp_path, [children[0]], name="controller-telemetry-loss")
    awaited = _awaited(plan, state)
    admitted = _admitted(queue, cas, plan, state)
    assert set(admitted) == {children[0]["action_key"]}, admitted
    first = queue.queued_child_wait_verdict(
        "c" * 64, cas_root=str(cas.root), awaited=awaited,
        admitted=admitted, prior=None)
    assert first["exempt"] is False
    assert first["children"][0]["evidence"] == "baseline"
    first["sample_monotonic"] = 100.0
    queue.item_path(pool.READY, children[0]["action_key"]).write_text("not a record")
    retained = _admitted(queue, cas, plan, state, retained={
        key: {} for key in admitted})
    assert children[0]["action_key"] in retained, retained
    verdict = queue.queued_child_wait_verdict(
        "c" * 64, cas_root=str(cas.root), awaited=awaited,
        admitted=retained, prior=first)
    assert verdict["exempt"] is False
    assert verdict["missing"] == [children[0]["action_key"]], verdict
    (entry,) = verdict["children"]
    assert entry["state"] == "unknown", verdict
    assert entry["evidence"] == "none", verdict


def test_an_older_worker_cannot_claim_an_awaited_coordinator(
        tmp_path: Path) -> None:
    """The declaration requires the queued-child capability tag."""

    cas, plan, children = _batch(tmp_path)
    queue = _queue(tmp_path, cas)
    checkout = tmp_path / "coord-src"
    checkout.mkdir()
    (checkout / "task.py").write_text("print('ok')\n")
    policy = {"schema": pb.PROGRESS_POLICY_SCHEMA_V1,
              "phases": [{"name": "run", "grace_s": 60.0}]}
    state = _controller_state(tmp_path, [children[0]], name="controller-capable")
    awaited = _awaited(plan, state)
    action = pb.seal_action({
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {
            "definition_id": "tests/coordinator", "definition_version": "v1",
            "task_class": "generation", "determinism": "deterministic",
            "artifact_family": "generic", "artifact_kind": "generic",
            "argv": [sys.executable, "task.py"], "working_directory": ".",
            "result_path": "result"},
        "inputs": [],
        "code_closure": pb.build_code_closure(checkout, ["task.py"]),
        "params": {pb.PROGRESS_PARAM: policy, pb.AWAITED_BATCH_PARAM: awaited},
        "environment": {"variables": {}, "toolchain": {}},
        "execution_scope": {"portability": "portable", "platform_key": None,
                            "host_class": None},
    })
    cas.publish_action_request(action)
    tags = [pb.PROGRESS_TAG, pb.PROGRESS_HELPER_TAG, pb.QUEUED_CHILD_TAG]
    import pbrun as pbrun_mod  # noqa: PLC0415 -- tools/fleet is on sys.path in PB
    required = pbrun_mod.progress_required_tags(policy, awaited)
    assert pb.QUEUED_CHILD_TAG in required
    assert pb.QUEUED_CHILD_TAG not in pbrun_mod.progress_required_tags(policy)
    queue.publish(
        action_key=action["action_key"], cas_root=cas.root,
        checkout_root=checkout, worker_script="/bin/true",
        tags=[*tags])
    assert queue.claim(tags=["x86", "old", pb.PROGRESS_TAG,
                             pb.PROGRESS_HELPER_TAG]) is None
    claimed = queue.claim(tags=["x86", "old", *tags])
    assert claimed is not None
    assert claimed["action_key"] == action["action_key"]


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
    state = _controller_state(tmp_path, [children[0]], name="controller-durable")
    awaited = _awaited(plan, state)
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
        controller_state=_controller_state(tmp_path, children),
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
        controller_state=_controller_state(tmp_path, children),
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
        controller_state=_controller_state(tmp_path, children),
        child_keys=["e" * 64], child_requests={}, phase="run")
    assert reporter.establish_baseline() == set()
    assert reporter.newly_durable() == []
    assert reporter.units == 0

def test_a_reporter_rejects_a_manifest_for_another_ordinal(
        tmp_path: Path) -> None:
    """A valid receipt with a valid digest but a foreign ordinal counts zero."""

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
    outcome = queue.execute(queue.claim(), timeout_s=60.0,
                            heartbeat_s=0.05, timeout_grace_s=0.2)
    assert outcome["status"] == "executed", outcome
    other = children[1] if len(children) > 1 else children[0]
    reporter = DurableChildReporter(
        cas=cas, parent_key=plan["parent_key"], plan_key=plan["plan_key"],
        child_keys=[other["action_key"]],
        controller_state=_controller_state(tmp_path, children),
        child_requests={other["action_key"]: children[0]}, phase="run")
    assert reporter.establish_baseline() == set()
    assert reporter.newly_durable() == []
    assert reporter.units == 0

def test_a_reporter_rejects_a_partial_task_set(tmp_path: Path) -> None:
    """A manifest that answers one task of a three-task batch counts zero."""

    checkout = tmp_path / "child-src"
    checkout.mkdir()
    (checkout / "t.py").write_text("x")
    request = dc.validate_logical_request({
        "schema": dc.LOGICAL_REQUEST_SCHEMA_V1,
        "common": {
            "argv": [sys.executable, "-c", PRODUCER,
                     dc.TASK_BATCH_PLACEHOLDER],
            "cwd": ".", "demand": {"cpu": 1, "mem_gb": 1},
            "gpu_memory_gb": None, "data_manifest": None, "env": {}},
        "roster": {
            "schema": dc.LOGICAL_TASK_ROSTER_SCHEMA_V1,
            "tasks": [
                {"id": f"q{index}", "payload": {"v": index},
                 "residency_key": "r", "estimated_seconds": 8.2,
                 "estimate_evidence": EVIDENCE, "output_id": f"z{index}"}
                for index in range(3)]},
        "batch_policy": {
            "schema": dc.ROSTER_BATCH_POLICY_SCHEMA_V1,
            "residencies": [{"key": "r", "setup_seconds": 20.0,
                             "setup_evidence": EVIDENCE}],
            "max_setup_fraction": 0.5,
            "max_estimated_wall_seconds": 300.0},
    })
    frozen = _frozen()
    frozen["argv"] = [sys.executable, "-c", PRODUCER,
                      dc.TASK_BATCH_PLACEHOLDER]
    plan = dc.build_plan(request, frozen)
    assert len(plan["partitions"]) == 1, plan["partitions"]
    assert len(plan["partitions"][0]) == 3, plan["partitions"]
    batch = dc.PreparedBatches(request, plan).membership(0)
    manifest = {
        "schema": dc.CHILD_RESULT_MANIFEST_SCHEMA_V1,
        "parent_key": plan["parent_key"], "plan_key": plan["plan_key"],
        "child_ordinal": 0,
        "results": [{
            "task_id": batch["ordered_task_ids"][0],
            "output_id": f"z0", "value_sha256": "0" * 64}]}
    checked = dc.validate_child_result_manifest(manifest)
    assert sorted(entry["task_id"] for entry in checked["results"]) != sorted(
        batch["ordered_task_ids"])


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
                "parent_key": "a" * 64, "plan_key": "b" * 64,
                "controller_state": str(tmp_path / "controller-state")}},
            "environment": {"variables": {}, "toolchain": {}},
            "execution_scope": {
                "portability": "portable", "platform_key": None,
                "host_class": None},
        })


def test_a_cleanup_tombstone_is_neither_credited_nor_released(
        tmp_path: Path) -> None:
    """A finish mark holds owner custody; the credit leaves it alone."""

    cas, plan, children = _batch(tmp_path)
    queue = _queue(tmp_path, cas)
    checkout = tmp_path / "held-src"
    checkout.mkdir()
    (checkout / "t.py").write_text("x")
    queue.publish(
        action_key=children[0]["action_key"], cas_root=cas.root,
        checkout_root=checkout, worker_script="/bin/true")
    claimed = queue.claim()
    assert claimed is not None
    tombstone, mine = queue._entomb_claim(
        children[0]["action_key"], expect=claimed)
    assert mine and tombstone is not None
    try:
        state = _controller_state(
            tmp_path, [children[0]], name="controller-tombstone")
        awaited = _awaited(plan, state)
        verdict = queue.queued_child_wait_verdict(
            "c" * 64, cas_root=str(cas.root), awaited=awaited,
            admitted={children[0]["action_key"]: dict(claimed)})
        (entry,) = verdict["children"]
        assert entry["state"] == "held", verdict
        assert verdict["exempt"] is False
        assert queue.item_path(
            pool.CLAIMED, children[0]["action_key"]).exists() is False
    finally:
        tombstone.unlink(missing_ok=True)


def test_a_child_with_three_tasks_counts_one_unit(tmp_path: Path) -> None:
    """Three L40 encodes in one child are one unit, not three."""

    cas = pb.PrismaBuildCAS(tmp_path / "cas3")
    checkout = tmp_path / "child-src"
    checkout.mkdir()
    (checkout / "t.py").write_text("x")
    request = dc.validate_logical_request({
        "schema": dc.LOGICAL_REQUEST_SCHEMA_V1,
        "common": {
            "argv": [sys.executable, "-c", PRODUCER,
                     dc.TASK_BATCH_PLACEHOLDER],
            "cwd": ".", "demand": {"cpu": 1, "mem_gb": 1},
            "gpu_memory_gb": None, "data_manifest": None, "env": {}},
        "roster": {
            "schema": dc.LOGICAL_TASK_ROSTER_SCHEMA_V1,
            "tasks": [
                {"id": f"u{index}", "payload": {"v": index},
                 "residency_key": "r", "estimated_seconds": 8.2,
                 "estimate_evidence": EVIDENCE, "output_id": f"w{index}"}
                for index in range(3)]},
        "batch_policy": {
            "schema": dc.ROSTER_BATCH_POLICY_SCHEMA_V1,
            "residencies": [{"key": "r", "setup_seconds": 20.0,
                             "setup_evidence": EVIDENCE}],
            "max_setup_fraction": 0.5,
            "max_estimated_wall_seconds": 300.0},
    })
    frozen = _frozen()
    frozen["argv"] = [sys.executable, "-c", PRODUCER,
                      dc.TASK_BATCH_PLACEHOLDER]
    plan = dc.build_plan(request, frozen)
    assert len(plan["partitions"]) == 1, plan["partitions"]
    assert len(plan["partitions"][0]) == 3, plan["partitions"]
    prepared = dc.PreparedBatches(request, plan)
    roster_input, _ = cas.ingest_bytes(
        dc.document_bytes(request["roster"]),
        input_id=dc.TASK_ROSTER_INPUT_ID)
    envelope = prepared.envelope(0)
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
            "result_path": dc.child_result_manifest_path(0)},
        "inputs": [roster_input, batch_input],
        "code_closure": pb.build_code_closure(checkout, ["t.py"]),
        "params": {"logical_batch": prepared.membership(0)},
        "environment": {"variables": {}, "toolchain": {}},
        "execution_scope": {"portability": "portable",
                            "platform_key": None, "host_class": None},
    })
    cas.publish_action_request(child)
    stored = cas.root / "decompositions" / plan["parent_key"][:2] / plan["parent_key"]
    stored.mkdir(parents=True, exist_ok=True)
    (stored / "plan.json").write_bytes(dc.document_bytes(plan))
    (stored / "publication.json").write_bytes(dc.document_bytes(
        dc.publication_index(
            plan, batch_input_digests=["0" * 64],
            child_action_keys=[child["action_key"]])))
    queue = _queue(tmp_path, cas)
    worker = (Path(__file__).resolve().parents[1]
              / "tools" / "prismabuild_worker.py")
    run = tmp_path / "run-multi"
    run.mkdir()
    (run / "t.py").write_text("x")
    queue.publish(action_key=child["action_key"], cas_root=cas.root,
                  checkout_root=run, worker_script=worker)
    outcome = queue.execute(queue.claim(), timeout_s=60.0,
                            heartbeat_s=0.05, timeout_grace_s=0.2)
    assert outcome["status"] == "executed", outcome
    reporter = DurableChildReporter(
        cas=cas, parent_key=plan["parent_key"], plan_key=plan["plan_key"],
        child_keys=[child["action_key"]],
        controller_state=_controller_state(tmp_path, [child]),
        child_requests={child["action_key"]: child}, phase="run")
    assert reporter.establish_baseline() == {child["action_key"]}
    assert reporter.units == 0
    manifest = reporter.verified[child["action_key"]]


def test_a_pending_only_child_earns_no_credit(tmp_path: Path) -> None:
    """Intent without controller acceptance is not admission and earns nothing."""

    cas, plan, children = _batch(tmp_path)
    queue = _queue(tmp_path, cas)
    state = _controller_state(tmp_path, [], name="controller-pending")
    (state / "wave-state.json").write_text(json.dumps({
        "waves": [], "pending_submission": {
            "batch": "child-00000", "key": children[0]["action_key"]}}))
    awaited = _awaited(plan, state)
    # The routed writer saves intent at the state level, not as a member.
    assert _admitted(queue, cas, plan, state) == {}
    verdict = queue.queued_child_wait_verdict(
        "c" * 64, cas_root=str(cas.root), awaited=awaited, admitted={})
    assert verdict["exempt"] is False
    assert verdict["children"] == []

def test_an_unreadable_wave_state_refuses_the_credit(tmp_path: Path) -> None:
    """A missing custody file carries custody and credits nothing."""

    cas, plan, children = _batch(tmp_path)
    queue = _queue(tmp_path, cas)
    checkout = tmp_path / "held-src"
    checkout.mkdir()
    (checkout / "t.py").write_text("x")
    queue.publish(
        action_key=children[0]["action_key"], cas_root=cas.root,
        checkout_root=checkout, worker_script="/bin/true")
    state = _controller_state(tmp_path, [children[0]], name="controller-unreadable")
    awaited = _awaited(plan, state)
    admitted = _admitted(queue, cas, plan, state)
    assert set(admitted) == {children[0]["action_key"]}, admitted
    (state / "wave-state.json").unlink()
    custody, refusal = queue._awaited_controller_custody(
        str(state), parent_key=plan["parent_key"], plan_key=plan["plan_key"])
    assert custody is None and "missing" in refusal
    retained = _admitted(queue, cas, plan, state, retained={
        key: {} for key in admitted})
    assert set(retained) == {children[0]["action_key"]}, retained
    verdict = queue.queued_child_wait_verdict(
        "c" * 64, cas_root=str(cas.root), awaited=awaited,
        admitted=retained,
        prior={"sample_monotonic": 100.0, "children": [{
            "key": children[0]["action_key"], "state": pool.READY,
            "evidence": "baseline", "ordinal": 0,
            "parent_key": plan["parent_key"],
            "plan_key": plan["plan_key"]}]})
    assert verdict["exempt"] is False
    assert "since_monotonic" not in verdict


def test_an_unknown_custody_shape_refuses_the_credit(tmp_path: Path) -> None:
    """An extra key, field, type or non-hex key refuses the credit."""

    cas, plan, children = _batch(tmp_path)
    queue = _queue(tmp_path, cas)
    state = _controller_state(tmp_path, [children[0]], name="controller-shape")
    shapes = [
        {"waves": [], "schema": "extra"},
        {"waves": [{"wave": 1, "closed": False, "members": [],
                    "extra": 1}]},
        {"waves": [{"wave": 1, "closed": False, "members": [
            {"batch": "child-00000", "key": children[0]["action_key"],
             "extra": 1}]}]},
        {"waves": [{"wave": "1", "closed": False, "members": []}]},
        {"waves": [{"wave": 1, "closed": False, "members": [
            {"batch": "child-00000", "key": "not-hex"}]}]},
    ]
    for shape in shapes:
        (state / "wave-state.json").write_text(json.dumps(shape))
        custody, refusal = queue._awaited_controller_custody(
            str(state), parent_key=plan["parent_key"],
            plan_key=plan["plan_key"])
        assert custody is None and refusal, shape
    (state / "wave-state.json").write_text(json.dumps({"waves": [{
        "wave": 1, "closed": False, "members": [
            {"batch": "child-00000",
             "key": children[0]["action_key"]}]}]}))
    (state / "sub-keys.txt").write_text("child-00000 not-hex\n")
    custody, refusal = queue._awaited_controller_custody(
        str(state), parent_key=plan["parent_key"], plan_key=plan["plan_key"])
    assert custody is None and "64-hex" in refusal


def test_a_key_in_one_custody_file_earns_no_credit(tmp_path: Path) -> None:
    """A member the journal does not confirm is not awaited."""

    cas, plan, children = _batch(tmp_path)
    queue = _queue(tmp_path, cas)
    assert len(children) >= 2, "the batch needs two children for this"
    state = _controller_state(
        tmp_path, [children[0], children[1]], name="controller-split")
    (state / "sub-keys.txt").write_text(
        f"child-00000 {children[0]['action_key']}\n")
    custody, refusal = queue._awaited_controller_custody(
        str(state), parent_key=plan["parent_key"], plan_key=plan["plan_key"])
    assert refusal == ""
    assert custody == {
        children[0]["action_key"]: {"batch": "child-00000", "confirmed": True},
        children[1]["action_key"]: {"batch": "child-00001", "confirmed": False}}, custody
    assert set(_admitted(queue, cas, plan, state)) == {
        children[0]["action_key"]}


def test_a_member_outside_the_plan_earns_no_credit(tmp_path: Path) -> None:
    """A custody key the sealed batch cannot bind blocks the credit."""

    cas, plan, children = _batch(tmp_path)
    queue = _queue(tmp_path, cas)
    checkout = tmp_path / "held-src"
    checkout.mkdir()
    (checkout / "t.py").write_text("x")
    queue.publish(
        action_key=children[0]["action_key"], cas_root=cas.root,
        checkout_root=checkout, worker_script="/bin/true")
    outsider = "f" * 64
    state = _controller_state(tmp_path, [children[0]], name="controller-outside")
    (state / "wave-state.json").write_text(json.dumps({"waves": [{
        "wave": 1, "closed": False, "members": [
            {"batch": "child-00000", "key": children[0]["action_key"]},
            {"batch": "child-00999", "key": outsider}]}]}))
    with open(state / "sub-keys.txt", "a") as handle:
        handle.write(f"child-00999 {outsider}\n")
    awaited = _awaited(plan, state)
    admitted = _admitted(queue, cas, plan, state)
    assert set(admitted) == {children[0]["action_key"], outsider}, admitted
    verdict = queue.queued_child_wait_verdict(
        "c" * 64, cas_root=str(cas.root), awaited=awaited,
        admitted=admitted,
        prior={"sample_monotonic": 100.0, "children": [{
            "key": children[0]["action_key"], "state": pool.READY,
            "evidence": "baseline", "ordinal": 0,
            "parent_key": plan["parent_key"],
            "plan_key": plan["plan_key"]}]})
    assert verdict["exempt"] is False
    assert verdict["missing"] == [outsider], verdict


def test_an_initially_absent_member_blocks_a_live_sibling(tmp_path: Path) -> None:
    """Custody names the absent child before any queue row does."""

    cas, plan, children = _batch(tmp_path)
    queue = _queue(tmp_path, cas)
    assert len(children) >= 2, "the batch needs two children for this"
    checkout = tmp_path / "held-src"
    checkout.mkdir()
    (checkout / "t.py").write_text("x")
    queue.publish(
        action_key=children[0]["action_key"], cas_root=cas.root,
        checkout_root=checkout, worker_script="/bin/true")
    state = _controller_state(
        tmp_path, [children[0], children[1]], name="controller-absent")
    awaited = _awaited(plan, state)
    admitted = _admitted(queue, cas, plan, state)
    assert set(admitted) == {
        children[0]["action_key"], children[1]["action_key"]}, admitted
    verdict = queue.queued_child_wait_verdict(
        "c" * 64, cas_root=str(cas.root), awaited=awaited,
        admitted=admitted,
        prior={"sample_monotonic": 100.0, "children": [{
            "key": children[0]["action_key"], "state": pool.READY,
            "evidence": "baseline", "ordinal": 0,
            "parent_key": plan["parent_key"],
            "plan_key": plan["plan_key"]}]})
    assert verdict["exempt"] is False
    assert verdict["missing"] == [children[1]["action_key"]], verdict


def test_an_unreadable_retained_request_stays_missing(tmp_path: Path) -> None:
    """A request that stops reading stays in the verdict, not foreign-covered."""

    cas, plan, children = _batch(tmp_path)
    queue = _queue(tmp_path, cas)
    assert len(children) >= 2, "the batch needs two children for this"
    checkout = tmp_path / "held-src"
    checkout.mkdir()
    (checkout / "t.py").write_text("x")
    queue.publish(
        action_key=children[0]["action_key"], cas_root=cas.root,
        checkout_root=checkout, worker_script="/bin/true")
    queue.publish(
        action_key=children[1]["action_key"], cas_root=cas.root,
        checkout_root=checkout, worker_script="/bin/true")
    state = _controller_state(
        tmp_path, [children[0], children[1]], name="controller-request")
    awaited = _awaited(plan, state)
    admitted = _admitted(queue, cas, plan, state)
    assert set(admitted) == {
        children[0]["action_key"], children[1]["action_key"]}, admitted
    request_path = (cas.root / "requests" / children[1]["action_key"][:2]
                    / f"{children[1]['action_key']}.json")
    request_path.unlink()
    retained = _admitted(queue, cas, plan, state, retained={
        key: {} for key in admitted})
    assert set(retained) == {
        children[0]["action_key"], children[1]["action_key"]}, retained
    verdict = queue.queued_child_wait_verdict(
        "c" * 64, cas_root=str(cas.root), awaited=awaited,
        admitted=retained,
        prior={"sample_monotonic": 100.0, "children": [{
            "key": children[0]["action_key"], "state": pool.READY,
            "evidence": "baseline", "ordinal": 0,
            "parent_key": plan["parent_key"],
            "plan_key": plan["plan_key"]}]})
    assert verdict["exempt"] is False
    assert verdict["missing"] == [children[1]["action_key"]], verdict


def test_a_terminal_transition_keeps_its_earned_credit(tmp_path: Path) -> None:
    """Intervals credited before the child ends stay credited."""

    cas, plan, children = _batch(tmp_path)
    queue = _queue(tmp_path, cas)
    checkout = tmp_path / "held-src"
    checkout.mkdir()
    (checkout / "t.py").write_text("x")
    queue.publish(
        action_key=children[0]["action_key"], cas_root=cas.root,
        checkout_root=checkout, worker_script="/bin/true")
    state = _controller_state(
        tmp_path, [children[0]], name="controller-terminal")
    awaited = _awaited(plan, state)
    admitted = _admitted(queue, cas, plan, state)
    first = queue.queued_child_wait_verdict(
        "c" * 64, cas_root=str(cas.root), awaited=awaited,
        admitted=admitted, prior=None)
    assert first["exempt"] is False
    first["sample_monotonic"] = 100.0
    watch = pool.ProgressWatch(
        tmp_path / "progress.json", "t" * 32,
        pool.ProgressPolicy(
            phases=(pool.ProgressPhase("run", 60.0, None),),
            ceiling_s=None, awaited_batch=awaited),
        started=90.0)
    second = queue.queued_child_wait_verdict(
        "c" * 64, cas_root=str(cas.root), awaited=awaited,
        admitted=_admitted(queue, cas, plan, state), prior=first)
    assert second["exempt"] is True
    credit = watch.exempt_queued_child_wait(
        second, now=110.0, since_monotonic=second["since_monotonic"])
    assert credit == 10.0
    assert watch.queued_child_wait_exempt_s == 10.0
    queue.withdraw(children[0]["action_key"], by="test-terminal-credit")
    second["sample_monotonic"] = 110.0
    third = queue.queued_child_wait_verdict(
        "c" * 64, cas_root=str(cas.root), awaited=awaited,
        admitted=_admitted(queue, cas, plan, state), prior=second)
    assert third["exempt"] is False
    watch.queued_child_wait = dict(third)
    assert watch.queued_child_wait_exempt_s == 10.0


def test_a_blocked_interval_never_refunds(tmp_path: Path) -> None:
    """A gap a missing member blocked starts no credit before it clears."""

    cas, plan, children = _batch(tmp_path)
    queue = _queue(tmp_path, cas)
    assert len(children) >= 2, "the batch needs two children for this"
    checkout = tmp_path / "held-src"
    checkout.mkdir()
    (checkout / "t.py").write_text("x")
    queue.publish(
        action_key=children[0]["action_key"], cas_root=cas.root,
        checkout_root=checkout, worker_script="/bin/true")
    state = _controller_state(
        tmp_path, [children[0], children[1]], name="controller-blocked")
    awaited = _awaited(plan, state)
    watch = pool.ProgressWatch(
        tmp_path / "progress.json", "t" * 32,
        pool.ProgressPolicy(
            phases=(pool.ProgressPhase("run", 60.0, None),),
            ceiling_s=None, awaited_batch=awaited),
        started=90.0)
    item = {"action_key": "c" * 64, "cas_root": str(cas.root)}
    queue._sample_queued_child_wait(
        item, item["action_key"], watch, watch.policy, now=100.0)
    first = watch.queued_child_wait
    assert first["exempt"] is False
    assert first["missing"] == [children[1]["action_key"]], first
    assert "since_monotonic" not in first
    queue.publish(
        action_key=children[1]["action_key"], cas_root=cas.root,
        checkout_root=checkout, worker_script="/bin/true")
    queue._sample_queued_child_wait(
        item, item["action_key"], watch, watch.policy, now=110.0)
    second = watch.queued_child_wait
    assert second["exempt"] is False
    assert "since_monotonic" not in second
    entries = {entry["key"]: entry for entry in second["children"]}
    assert entries[children[1]["action_key"]]["evidence"] == "baseline"
    assert watch.queued_child_wait_exempt_s == 0.0
    assert watch.stall_deadline() == 150.0
    queue._sample_queued_child_wait(
        item, item["action_key"], watch, watch.policy, now=120.0)
    assert watch.queued_child_wait["exempt"] is True
    assert watch.queued_child_wait_exempt_s == 10.0
    assert watch.stall_deadline() == 160.0


@pytest.mark.parametrize("refusal", [
    "missing-row", "unreadable-row", "unreadable-request",
    "unreadable-custody", "unreadable-plan",
])
def test_recovery_requires_two_valid_endpoints(
        tmp_path: Path, refusal: str) -> None:
    """Recovery grants no credit until both samples have valid custody."""

    cas, plan, children = _batch(tmp_path)
    queue = _queue(tmp_path, cas)
    checkout = tmp_path / "held-src"
    checkout.mkdir()
    (checkout / "t.py").write_text("x")
    for child in children[:2]:
        queue.publish(
            action_key=child["action_key"], cas_root=cas.root,
            checkout_root=checkout, worker_script="/bin/true")
    state = _controller_state(tmp_path, children[:2])
    awaited = _awaited(plan, state)
    policy = pool.ProgressPolicy(
        phases=(pool.ProgressPhase("run", 60.0, None),),
        ceiling_s=None, awaited_batch=awaited)
    watch = pool.ProgressWatch(
        tmp_path / "progress.json", "t" * 32, policy, started=90.0)
    item = {"action_key": "c" * 64, "cas_root": str(cas.root)}
    queue._sample_queued_child_wait(item, item["action_key"], watch, policy, now=100.0)
    assert watch.queued_child_wait["exempt"] is False
    queue._sample_queued_child_wait(item, item["action_key"], watch, policy, now=110.0)
    assert watch.queued_child_wait_exempt_s == 10.0
    assert watch.stall_deadline() == 160.0

    key = children[1]["action_key"]
    paths = {
        "missing-row": queue.item_path(pool.READY, key),
        "unreadable-row": queue.item_path(pool.READY, key),
        "unreadable-request": cas.root / "requests" / key[:2] / f"{key}.json",
        "unreadable-custody": state / "wave-state.json",
        "unreadable-plan": (
            cas.root / "decompositions" / plan["parent_key"][:2]
            / plan["parent_key"] / "plan.json"),
    }
    path = paths[refusal]
    original = path.read_bytes()
    path.unlink()
    if refusal != "missing-row":
        path.write_bytes(b"not JSON")
    queue._sample_queued_child_wait(item, item["action_key"], watch, policy, now=120.0)
    assert watch.queued_child_wait["exempt"] is False
    assert watch.queued_child_wait_exempt_s == 10.0
    assert watch.stall_deadline() == 160.0

    path.write_bytes(original)
    queue._sample_queued_child_wait(item, item["action_key"], watch, policy, now=130.0)
    assert watch.queued_child_wait["exempt"] is False
    assert "since_monotonic" not in watch.queued_child_wait
    assert watch.queued_child_wait_exempt_s == 10.0
    assert watch.stall_deadline() == 160.0
    queue._sample_queued_child_wait(item, item["action_key"], watch, policy, now=140.0)
    assert watch.queued_child_wait["exempt"] is True
    assert watch.queued_child_wait_exempt_s == 20.0
    assert watch.stall_deadline() == 170.0


def test_the_fixed_writer_optional_fields_do_not_grant_credit(tmp_path: Path) -> None:
    """The fa37751 optional fields are typed diagnostics, not child progress."""

    cas, plan, children = _batch(tmp_path)
    queue = _queue(tmp_path, cas)
    state = _controller_state(tmp_path, [children[0]])
    value = json.loads((state / "wave-state.json").read_text())
    value.update({
        "pending_submission": {"batch": "child-00001", "key": children[1]["action_key"]},
        "last_completion": {"status": "done", "action_key": children[0]["action_key"]},
        "wait_reason": "ship",
        "last_disk_check": {"action_key": children[1]["action_key"], "evidence": {"pass": True}},
        "disk_checks": [{"action_key": children[1]["action_key"], "evidence": {"pass": True}}],
    })
    value["waves"][0]["members"][0]["published_unix"] = 123.0
    (state / "wave-state.json").write_text(json.dumps(value))
    admitted = _admitted(queue, cas, plan, state)
    assert set(admitted) == {children[0]["action_key"]}
    verdict = queue.queued_child_wait_verdict(
        "c" * 64, cas_root=str(cas.root), awaited=_awaited(plan, state),
        admitted=admitted)
    assert verdict["exempt"] is False
    assert verdict["missing"] == [children[0]["action_key"]]


@pytest.mark.parametrize(("name", "bad"), [
    ("pending_submission", []), ("last_completion", "done"),
    ("wait_reason", 1), ("last_disk_check", []), ("disk_checks", {}),
    ("published_unix", True), ("writer_drift", 1),
])
def test_bad_optional_fields_refuse_and_name_the_field(
        tmp_path: Path, name: str, bad: object) -> None:
    """A field error refuses the whole read and appears in the observation."""

    cas, plan, children = _batch(tmp_path)
    queue = _queue(tmp_path, cas)
    state = _controller_state(tmp_path, children[:1])
    value = json.loads((state / "wave-state.json").read_text())
    if name == "published_unix":
        value["waves"][0]["members"][0][name] = bad
    else:
        value[name] = bad
    (state / "wave-state.json").write_text(json.dumps(value))
    policy = pool.ProgressPolicy(
        phases=(pool.ProgressPhase("run", 60.0, None),),
        ceiling_s=None, awaited_batch=_awaited(plan, state))
    watch = pool.ProgressWatch(tmp_path / "progress.json", "t" * 32, policy, started=0.0)
    item = {"action_key": "c" * 64, "cas_root": str(cas.root)}
    queue._sample_queued_child_wait(item, item["action_key"], watch, policy, now=100.0)
    assert watch.queued_child_wait["exempt"] is False
    assert name in watch.queued_child_wait["reason"]
    assert watch.queued_child_wait_exempt_s == 0.0


@pytest.mark.parametrize("conflict", ["same-key", "same-batch"])
def test_conflicting_custody_mappings_refuse_the_whole_read(
        tmp_path: Path, conflict: str) -> None:
    """Neither file can hide a conflicting candidate in the other file."""

    cas, plan, children = _batch(tmp_path)
    queue = _queue(tmp_path, cas)
    state = _controller_state(tmp_path, children[:1])
    if conflict == "same-key":
        line = f"child-00001 {children[0]['action_key']}\n"
    else:
        line = f"child-00000 {children[1]['action_key']}\n"
    (state / "sub-keys.txt").write_text(line)
    custody, refusal = queue._awaited_controller_custody(
        str(state), parent_key=plan["parent_key"], plan_key=plan["plan_key"])
    assert custody is None
    assert "two" in refusal
    assert _admitted(queue, cas, plan, state) == {}


def test_the_reporter_requires_confirmed_custody_even_with_a_durable_result(
        tmp_path: Path) -> None:
    """Prepared work and intent cannot produce progress from a CAS receipt."""

    cas, plan, children = _batch(tmp_path)
    queue = _queue(tmp_path, cas)
    state = _controller_state(tmp_path, [])
    key = children[0]["action_key"]
    reporter = DurableChildReporter(
        cas=cas, parent_key=plan["parent_key"], plan_key=plan["plan_key"],
        child_keys=[key, key], controller_state=state, phase="run")
    assert reporter.establish_baseline() == set()
    checkout = tmp_path / "run-src"
    checkout.mkdir()
    (checkout / "t.py").write_text("x")
    worker = Path(__file__).resolve().parents[1] / "tools" / "prismabuild_worker.py"
    queue.publish(action_key=key, cas_root=cas.root, checkout_root=checkout, worker_script=worker)
    outcome = queue.execute(queue.claim(), timeout_s=60.0, heartbeat_s=0.05, timeout_grace_s=0.2)
    assert outcome["status"] == "executed", outcome
    queue.finish(key, status="executed", detail=outcome)
    queue.item_path(pool.DONE, key).unlink()
    assert reporter.newly_durable() == []
    (state / "wave-state.json").write_text(json.dumps({
        "waves": [], "pending_submission": {"batch": "child-00000", "key": key}}))
    assert reporter.newly_durable() == []
    (state / "wave-state.json").write_text(json.dumps({"waves": [{
        "wave": 1, "closed": False, "members": [{"batch": "child-00000", "key": key}]}]}))
    assert reporter.newly_durable() == []
    (state / "sub-keys.txt").write_text(f"child-00000 {key}\n")
    assert reporter.newly_durable() == [key]
    assert reporter.units == 1
    assert reporter.newly_durable() == []
