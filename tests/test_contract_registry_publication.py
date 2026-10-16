"""A publisher cannot expose sealed work to a worker that ignores its contract."""
from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest

from prismabuild import contracts, core as pb, movement_actions, pool


DEMAND = {"cpu": 1, "mem_gb": 1}
PROGRESS = {"schema": pb.PROGRESS_POLICY_SCHEMA_V1,
            "phases": [{"name": "work", "grace_s": 60}]}
CONTENTION = {"schema": pb.POOL_CONTENTION_SCHEMA_V1, "members": ["sda"],
              "max_read_await_ms": 10, "max_backlog_ms": 20,
              "priced_bytes_per_s": 1024, "stage_root": "/stage/test"}
AWAITED = {"schema": pb.AWAITED_BATCH_SCHEMA_V1,
           "parent_key": "a" * 64, "plan_key": "b" * 64,
           "controller_state": "/controller"}
PROGRESS_TAGS = (contracts.PROGRESS_TAG, contracts.PROGRESS_HELPER_TAG,
                 contracts.PROGRESS_CYCLE_TAG, contracts.POOL_CONTENTION_TAG,
                 contracts.EGRESS_PROGRESS_TAG, contracts.QUEUED_CHILD_TAG)


@pytest.mark.parametrize("gang", [False, True])
@pytest.mark.parametrize("generation", ["", "g1"])
def test_registry_enablement_and_generation_companions_are_pure_facts(tmp_path, gang, generation):
    tags = frozenset(contracts.offered_contract_tags(contracts.WorkerFacts(
        gang_admission=gang, loaded_generation=generation)))
    queue = pool.PoolQueue(tmp_path / "queue")
    assert contracts.CONTAINER_IMAGE_TAG in tags
    assert contracts.SCRATCH_IO_TAG in tags
    assert contracts.SCRATCH_LIFETIME_TAG in tags
    assert (contracts.GANG_TAG in tags) is gang
    assert (contracts.PUBLICATION_CANARY_TAG in tags) is bool(generation)
    assert {tag for tag in tags if tag.startswith("runtime-generation:")} == (
        {f"runtime-generation:{generation}"} if generation else set())
    assert queue._placement_matches(
        {"tags": [contracts.GANG_TAG]}, tags=tags, has_gpu=False) is gang
    assert queue._placement_matches(
        {"tags": [contracts.PUBLICATION_CANARY_TAG]}, tags=tags,
        has_gpu=False) is bool(generation)


def _sealed_request(tmp_path, params, *, movement_tool=None):
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    (checkout / "task.py").write_text("pass\n")
    action = pb.seal_action({
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {"definition_id": "tests/alternate-publisher", "definition_version": "v1",
                 "task_class": "generation", "determinism": "deterministic",
                 "artifact_family": "generic", "artifact_kind": "generic",
                 "argv": [sys.executable, "task.py"], "working_directory": ".",
                 "result_path": "result"},
        "inputs": [], "code_closure": pb.build_code_closure(checkout, ["task.py"]),
        "params": {"demand": dict(DEMAND), "placement": {"required_tags": []},
                   "cwd": str(checkout), **params},
        "environment": {"variables": {}, "toolchain": {}},
        "execution_scope": {"portability": "portable", "platform_key": None,
                            "host_class": None}})
    if movement_tool is not None:
        action = movement_actions.seal_movement_action(
            {**action, "marker_root": tmp_path / "markers",
             "checkout_identity": {"commit": "a" * 40}},
            command=[movement_actions.MOVEMENT_PYTHON, str(movement_tool)],
            demand=DEMAND, tags=[], log_name="movement.log", extra_params=params)
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    cas.publish_action_request(action)
    return pool.PoolQueue(tmp_path / "queue"), action, cas, checkout


def _publish(queue, action, cas, checkout, *, tags=()):
    return queue.publish(action_key=action["action_key"], cas_root=cas.root,
                         checkout_root=checkout, worker_script="/worker.py",
                         resources=DEMAND, tags=tags)


@pytest.mark.parametrize("tag", PROGRESS_TAGS)
def test_a_progress_tag_without_its_sealed_declaration_never_reaches_ready(tmp_path, tag):
    queue, action, cas, checkout = _sealed_request(tmp_path, {})
    with pytest.raises(pool.PoolContractError, match=tag + ".*requires"):
        _publish(queue, action, cas, checkout, tags=[tag])
    assert not queue.records.item_path(pool.record_store.READY, action["action_key"]).exists()


@pytest.mark.parametrize("extra,required", [
    ({}, {contracts.PROGRESS_TAG, contracts.PROGRESS_HELPER_TAG}),
    ({pb.PROGRESS_PARAM: {**PROGRESS, "cycle": True}},
     {contracts.PROGRESS_TAG, contracts.PROGRESS_HELPER_TAG, contracts.PROGRESS_CYCLE_TAG}),
    ({pb.POOL_CONTENTION_PARAM: CONTENTION},
     {contracts.PROGRESS_TAG, contracts.PROGRESS_HELPER_TAG, contracts.POOL_CONTENTION_TAG}),
    ({pb.AWAITED_BATCH_PARAM: AWAITED},
     {contracts.PROGRESS_TAG, contracts.PROGRESS_HELPER_TAG, contracts.QUEUED_CHILD_TAG}),
])
def test_an_alternate_publisher_derives_the_fence_from_the_sealed_request(
        tmp_path, extra, required):
    queue, action, cas, checkout = _sealed_request(
        tmp_path, {pb.PROGRESS_PARAM: PROGRESS, **extra})
    path = _publish(queue, action, cas, checkout)
    row = json.loads(path.read_text())
    assert set(row["tags"]) == required
    assert action["params"]["placement"]["required_tags"] == []
    assert queue.claim(tags=[], capacity={"cpu": 4, "mem_gb": 8}) is None
    assert queue.records.item_path(pool.record_store.READY, action["action_key"]).exists()


@pytest.mark.parametrize("script,egress", [("stage_release.py", True),
                                         ("produced_export.py", False)])
def test_egress_support_follows_the_executed_movement_identity(
        tmp_path, movement_authority, script, egress):
    queue, action, cas, checkout = _sealed_request(
        tmp_path, {pb.PROGRESS_PARAM: PROGRESS, pb.POOL_CONTENTION_PARAM: CONTENTION},
        movement_tool=movement_authority / "tools/fleet" / script)
    role = movement_actions.capacity_role(action, DEMAND, residency=None)
    assert role is not None and role.role == "returns_capacity"
    path = _publish(queue, action, cas, checkout)
    tags = set(json.loads(path.read_text())["tags"])
    assert {contracts.PROGRESS_TAG, contracts.PROGRESS_HELPER_TAG,
            contracts.POOL_CONTENTION_TAG} <= tags
    assert (contracts.EGRESS_PROGRESS_TAG in tags) is egress


def _replace_request_with_corrupt_json(request: Path) -> None:
    """Violate JSON structure while keeping a readonly regular-file request."""
    damaged = request.with_suffix(".damaged")
    damaged.write_bytes(b"{")
    damaged.chmod(0o444)
    damaged.replace(request)


@pytest.mark.parametrize("damage", ["missing", "corrupt"])
def test_unknown_sealed_declaration_never_authorizes_a_progress_tag(tmp_path, damage):
    queue, action, cas, checkout = _sealed_request(tmp_path, {pb.PROGRESS_PARAM: PROGRESS})
    request = cas.root / "requests" / action["action_key"][:2] / (action["action_key"] + ".json")
    # Named damage after the real writer: the immutable publication is absent
    # or structurally unreadable, so its declaration cannot authorize a tag.
    if damage == "missing":
        request.unlink()
    else:
        _replace_request_with_corrupt_json(request)
    with pytest.raises(pool.PoolContractError):
        _publish(queue, action, cas, checkout, tags=[contracts.PROGRESS_TAG])
    assert not queue.records.item_path(pool.record_store.READY, action["action_key"]).exists()


def test_contract_remedy_reaches_the_real_submission_refusal(tmp_path, monkeypatch):
    import pbrun

    queue, action, _cas, checkout = _sealed_request(tmp_path, {
        "gang": {"group": "c" * 32, "size": 2, "index": 0},
        "placement": {"required_tags": [contracts.GANG_TAG]}})
    queue.announce(host="ordinary", tags=["ordinary"], has_gpu=False,
                   capacity={"cpu": 4, "mem_gb": 8})
    remedy = "Enable the reviewed gang worker configuration."
    monkeypatch.setattr(contracts, "CONTRACTS", tuple(
        replace(row, remedy=remedy) if row.tag == contracts.GANG_TAG else row
        for row in contracts.CONTRACTS))
    args = SimpleNamespace(progress_policy=None, here=False, awaited_batch=None,
                           timeout_s=None, wait_s=0.01)
    with pytest.raises(SystemExit) as exc:
        pbrun.announce_placement(queue, action, args=args, cwd=checkout,
                                 portable_checkout=True)
    assert remedy in str(exc.value)
    assert queue.ready_items() == []
