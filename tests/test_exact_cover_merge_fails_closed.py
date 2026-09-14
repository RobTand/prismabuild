"""A group receipt appears only when the children answered the roster exactly.

The merge is where a decomposition can lie most quietly.  Every child can exit
zero, every log can look right, and the campaign can still be missing a task,
or hold two answers for one task from two children that both believed they
owned it.  A verifier that trusts exit status would publish a group receipt
over either.

So the check is over the manifests, not the logs: one result per roster task,
each from the child whose batch contains it, under the output id the roster
declared, from this plan and no other.  Anything else fails closed.
"""
from __future__ import annotations

from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import conftest  # noqa: E402
from prismabuild import decomposition as dc  # noqa: E402


#: A stand-in for what pbrun's Stage A resolves: the parent is keyed on the
#: tree's snapshot digest and on the rest of what was sealed against it, never
#: on the path the submitter typed.
SNAPSHOT = conftest.DECOMPOSITION_SNAPSHOT
MANIFEST = conftest.DECOMPOSITION_MANIFEST


def _frozen(request: dict, *, snapshot: str = SNAPSHOT, cwd: str = ".",
            variables: dict | None = None, **sealed: object) -> dict:
    """What pbrun's Stage A would have sealed for this request.

    The declaration reaches the sealed half the way a real template carries it
    -- the manifest ingested, the GPU budget attached to a GPU demand, the
    environment merged into the resolved variables -- because that agreement is
    exactly what ``freeze_common`` refuses to take on trust.  Keywords override
    it, so a test can say "the same request, sealed differently".
    """

    common = request["common"]
    if common["gpu_memory_gb"] is not None:
        sealed.setdefault("gpu_memory_gb", common["gpu_memory_gb"])
    return dc.freeze_common(
        common,
        action_common=conftest.action_common(
            snapshot=snapshot,
            cwd=cwd,
            manifest=None if common["data_manifest"] is None else MANIFEST,
            variables={**common["env"], **(variables or {})},
            **sealed,
        ),
    )

EVIDENCE = "cas:sha256:" + "0" * 64


def _request(count: int = 66) -> dict:
    return dc.validate_logical_request({
        "schema": dc.LOGICAL_REQUEST_SCHEMA_V1,
        "common": {
            "argv": ["python", "collect.py", dc.TASK_BATCH_PLACEHOLDER],
            "cwd": "/checkout",
            "demand": {"gpu": 1, "cpu": 4, "mem_gb": 72},
            "gpu_memory_gb": 72,
            "data_manifest": None,
            "env": {},
        },
        "roster": {
            "schema": dc.LOGICAL_TASK_ROSTER_SCHEMA_V1,
            "tasks": [
                {
                    "id": f"t{index:04d}",
                    "payload": {"rate": 832 + index},
                    "residency_key": "r",
                    "estimated_seconds": 8.2,
                    "estimate_evidence": EVIDENCE,
                    "output_id": f"t{index:04d}",
                }
                for index in range(count)
            ],
        },
        "batch_policy": {
            "schema": dc.ROSTER_BATCH_POLICY_SCHEMA_V1,
            "residencies": [
                {"key": "r", "setup_seconds": 45.0, "setup_evidence": EVIDENCE}
            ],
            "max_setup_fraction": 0.20,
            "max_estimated_wall_seconds": 300.0,
        },
    })


def _value(task_id: str) -> str:
    return dc.canonical_sha256({"measured": task_id})


def _manifests(request: dict, plan: dict) -> list[dict]:
    return [
        {
            "schema": dc.CHILD_RESULT_MANIFEST_SCHEMA_V1,
            "parent_key": plan["parent_key"],
            "plan_key": plan["plan_key"],
            "child_ordinal": ordinal,
            "results": [
                {"task_id": task_id, "output_id": task_id,
                 "value_sha256": _value(task_id)}
                for task_id in batch
            ],
        }
        for ordinal, batch in enumerate(plan["partitions"])
    ]


@pytest.fixture
def planned() -> tuple[dict, dict, list[dict]]:
    request = _request()
    plan = dc.build_plan(request, _frozen(request))
    assert len(plan["partitions"]) >= 3, "fixture needs several children"
    return request, plan, _manifests(request, plan)


def test_a_complete_set_publishes_a_group_receipt(planned) -> None:
    """And the merged digest is a function of the results, in plan order."""

    request, plan, manifests = planned
    receipt = dc.verify_exact_cover(request, plan, manifests)
    assert receipt["schema"] == dc.GROUP_RECEIPT_SCHEMA_V1
    assert receipt["parent_key"] == plan["parent_key"]
    assert receipt["plan_key"] == plan["plan_key"]
    assert receipt["task_count"] == len(request["roster"]["tasks"])
    assert receipt["child_count"] == len(plan["partitions"])
    shuffled = [manifests[-1], *manifests[:-1]]
    assert dc.verify_exact_cover(request, plan, shuffled) == receipt, (
        "arrival order is not part of what the children measured"
    )


def test_a_missing_child_is_not_a_group_success(planned) -> None:
    request, plan, manifests = planned
    with pytest.raises(dc.ActionContractError, match="one result manifest per batch"):
        dc.verify_exact_cover(request, plan, manifests[:-1])


def test_a_child_short_one_task_is_not_a_group_success(planned) -> None:
    """The child exited fine; it just did not answer everything it owned."""

    request, plan, manifests = planned
    manifests[1] = {**manifests[1], "results": manifests[1]["results"][:-1]}
    with pytest.raises(dc.ActionContractError, match="unanswered"):
        dc.verify_exact_cover(request, plan, manifests)


def test_two_children_answering_one_task_is_refused(planned) -> None:
    """The case a re-partition on retry would produce, caught at the merge."""

    request, plan, manifests = planned
    stolen = manifests[0]["results"][0]
    manifests[1] = {**manifests[1], "results": [*manifests[1]["results"], stolen]}
    with pytest.raises(dc.ActionContractError, match="not in\n?\\s*its own batch"):
        dc.verify_exact_cover(request, plan, manifests)


def test_a_duplicate_ordinal_is_refused(planned) -> None:
    request, plan, manifests = planned
    manifests[1] = {**manifests[1], "child_ordinal": 0}
    with pytest.raises(dc.ActionContractError, match="claim child ordinal 0"):
        dc.verify_exact_cover(request, plan, manifests)


def test_a_foreign_task_is_refused(planned) -> None:
    request, plan, manifests = planned
    manifests[0] = {**manifests[0], "results": [
        {"task_id": "t9999", "output_id": "t9999", "value_sha256": _value("t9999")},
        *manifests[0]["results"][1:],
    ]}
    with pytest.raises(dc.ActionContractError, match="not in\n?\\s*its own batch"):
        dc.verify_exact_cover(request, plan, manifests)


def test_a_result_under_the_wrong_output_id_is_refused(planned) -> None:
    """The output id is where the answer lands, so a wrong one loses it."""

    request, plan, manifests = planned
    first = manifests[0]["results"][0]
    manifests[0] = {**manifests[0], "results": [
        {**first, "output_id": "somewhere-else"}, *manifests[0]["results"][1:],
    ]}
    with pytest.raises(dc.ActionContractError, match="not the roster's"):
        dc.verify_exact_cover(request, plan, manifests)


def test_a_manifest_from_another_plan_is_refused(planned) -> None:
    """Same command, same box, different frozen partition."""

    request, plan, manifests = planned
    request_88 = _request(count=88)
    other = dc.build_plan(request_88, _frozen(request_88))
    manifests[0] = {**manifests[0], "plan_key": other["plan_key"]}
    with pytest.raises(dc.ActionContractError, match="reports plan"):
        dc.verify_exact_cover(request, plan, manifests)


def test_a_manifest_from_another_parent_is_refused(planned) -> None:
    request, plan, manifests = planned
    request_88 = _request(count=88)
    other = dc.build_plan(request_88, _frozen(request_88))
    manifests[0] = {**manifests[0], "parent_key": other["parent_key"]}
    with pytest.raises(dc.ActionContractError, match="reports parent"):
        dc.verify_exact_cover(request, plan, manifests)


def test_the_batch_envelope_and_the_sealed_param_name_the_same_tasks(planned) -> None:
    """One says what the child reads; the other is what its key binds."""

    request, plan, _ = planned
    for ordinal, batch in enumerate(plan["partitions"]):
        envelope = dc.batch_envelope(request, plan, child_ordinal=ordinal)
        param = dc.logical_batch_param(request, plan, child_ordinal=ordinal)
        assert [task["id"] for task in envelope["tasks"]] == batch
        assert param["ordered_task_ids"] == batch
        assert envelope["roster_sha256"] == param["roster_sha256"]
        assert envelope["batch_policy_sha256"] == param["batch_policy_sha256"]
        assert envelope["parent_key"] == param["parent_key"] == plan["parent_key"]
        assert envelope["plan_key"] == param["plan_key"] == plan["plan_key"]


def test_the_publication_index_must_cover_every_planned_child(planned) -> None:
    """It is frozen before the first publish, so it cannot be short."""

    _, plan, _ = planned
    count = len(plan["partitions"])
    digests = [dc.canonical_sha256({"batch": index}) for index in range(count)]
    keys = [dc.canonical_sha256({"child": index}) for index in range(count)]
    index = dc.publication_index(
        plan, batch_input_digests=digests, child_action_keys=keys,
    )
    assert index["child_action_keys"] == keys
    with pytest.raises(dc.ActionContractError, match="one batch digest and one"):
        dc.publication_index(
            plan, batch_input_digests=digests[:-1], child_action_keys=keys,
        )
