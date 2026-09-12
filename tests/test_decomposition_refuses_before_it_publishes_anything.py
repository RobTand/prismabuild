"""A request PrismaBuild cannot cut is refused, never approximated.

The alternative is the failure this whole mechanism replaces: one opaque
oversized child, or a limit quietly relaxed to make the arithmetic work.  Both
produce a plan that runs, and neither produces the measurement the producer
asked for -- so every refusal here happens before a parent record exists, and
says which run, which tasks, and which of the two declared limits it could not
satisfy.

Unknown fields are refused for the same reason.  A decomposition's identity is
the hash of exactly the declared fields, so a field PrismaBuild does not
understand is one the producer believes is binding and PrismaBuild would drop.
"""
from __future__ import annotations

from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import decomposition as dc  # noqa: E402


#: A stand-in for what pbrun's Stage A resolves: the parent is keyed on the
#: tree's snapshot digest, never on the path the submitter typed.
SNAPSHOT = "a" * 64
MANIFEST = "b" * 64


def _frozen(request: dict, *, snapshot: str = SNAPSHOT, cwd: str = ".") -> dict:
    return dc.freeze_common(
        request["common"],
        logical_cwd=cwd,
        checkout_snapshot_sha256=snapshot,
        data_manifest_sha256=(
            None if request["common"]["data_manifest"] is None else MANIFEST
        ),
    )

EVIDENCE = "cas:sha256:" + "0" * 64


def _task(index: int, **overrides: object) -> dict:
    task = {
        "id": f"t{index:04d}",
        "payload": {"rate": 832 + index},
        "residency_key": "r",
        "estimated_seconds": 8.2,
        "estimate_evidence": EVIDENCE,
        "output_id": f"t{index:04d}",
    }
    task.update(overrides)
    return task


def _roster(*tasks: dict) -> dict:
    return {"schema": dc.LOGICAL_TASK_ROSTER_SCHEMA_V1, "tasks": list(tasks)}


def _policy(**overrides: object) -> dict:
    policy = {
        "schema": dc.ROSTER_BATCH_POLICY_SCHEMA_V1,
        "residencies": [
            {"key": "r", "setup_seconds": 45.0, "setup_evidence": EVIDENCE}
        ],
        "max_setup_fraction": 0.20,
        "max_estimated_wall_seconds": 300.0,
    }
    policy.update(overrides)
    return policy


def _common(**overrides: object) -> dict:
    common = {
        "argv": ["python", "collect.py", "--pb-task-batch",
                 dc.TASK_BATCH_PLACEHOLDER],
        "cwd": "/checkout",
        "demand": {"gpu": 1, "cpu": 4, "mem_gb": 72},
        "gpu_memory_gb": 72,
        "data_manifest": "/inputs/data-manifest.json",
        "env": {"OMP_NUM_THREADS": "1"},
    }
    common.update(overrides)
    return common


def _plan(request: dict) -> dict:
    return dc.build_plan(request, _frozen(request))


def _request(**overrides: object) -> dict:
    request = {
        "schema": dc.LOGICAL_REQUEST_SCHEMA_V1,
        "common": _common(),
        "roster": _roster(*[_task(index) for index in range(66)]),
        "batch_policy": _policy(),
    }
    request.update(overrides)
    return request


# --------------------------------------------------------------------------
# Limits the roster cannot meet
# --------------------------------------------------------------------------


def test_one_task_past_the_wall_ceiling_is_named_in_the_refusal() -> None:
    """The operator needs the task, not "infeasible"."""

    with pytest.raises(dc.PartitionRefused) as refused:
        _plan(_request(roster=_roster(
            _task(0), _task(1, estimated_seconds=900.0), _task(2),
        )))
    message = str(refused.value)
    assert "'t0001'" in message and "900" in message
    assert "residency 'r'" in message and "[0, 3)" in message


def test_a_roster_too_short_to_amortize_its_own_setup_is_refused() -> None:
    """Two tasks cannot carry a 45s setup at a 20% setup fraction."""

    with pytest.raises(dc.PartitionRefused) as refused:
        _plan(_request(roster=_roster(_task(0), _task(1))))
    message = str(refused.value)
    assert "180" in message, "the refusal states the useful-work floor"
    assert "300" in message, "and the wall ceiling"
    assert "does not relax a declared limit" in message


def test_setup_alone_past_the_wall_ceiling_is_refused() -> None:
    """No batch of this residency can run at all, whatever the roster."""

    with pytest.raises(dc.PartitionRefused, match="meets or exceeds the wall"):
        _plan(_request(
            batch_policy=_policy(max_estimated_wall_seconds=40.0)
        ))


def test_a_floor_above_the_ceiling_is_refused_as_a_pair() -> None:
    """The two limits contradict each other; neither alone is the fault."""

    with pytest.raises(dc.PartitionRefused, match="admit no batch at all"):
        _plan(_request(batch_policy=_policy(
            max_setup_fraction=0.05, max_estimated_wall_seconds=200.0,
        )))


# --------------------------------------------------------------------------
# Malformed declarations
# --------------------------------------------------------------------------


@pytest.mark.parametrize("roster,expected", [
    (_roster(), "at least one task"),
    (_roster(_task(0), _task(0)), "repeats an earlier task id"),
    (_roster(_task(0), _task(1, output_id="t0000")), "repeats an earlier output id"),
    (_roster(_task(0, estimated_seconds=0.0)), "greater than zero"),
    (_roster(_task(0, estimated_seconds=-1.0)), "greater than zero"),
    (_roster(_task(0, estimated_seconds="8.2")), "must be a number"),
    (_roster(_task(0, estimated_seconds=float("inf"))), "must be finite"),
    (_roster(_task(0, estimate_evidence="")), "non-empty string"),
])
def test_a_malformed_roster_is_refused(roster, expected) -> None:
    with pytest.raises(dc.ActionContractError, match=expected):
        dc.validate_roster(roster)


def test_an_unknown_roster_field_is_refused() -> None:
    """Silently dropping it would make the producer's belief unhashed."""

    with pytest.raises(dc.ActionContractError, match="extra=\\['weight'\\]"):
        dc.validate_roster(_roster({**_task(0), "weight": 3}))


@pytest.mark.parametrize("policy,expected", [
    (_policy(residencies=[]), "at least one residency"),
    (_policy(max_setup_fraction=0.0), r"must be in \(0, 1\]"),
    (_policy(max_setup_fraction=1.5), r"must be in \(0, 1\]"),
    (_policy(max_estimated_wall_seconds=0.0), "greater than zero"),
    (_policy(residencies=[
        {"key": "r", "setup_seconds": -1.0, "setup_evidence": EVIDENCE}
    ]), "must not be negative"),
    (_policy(residencies=[
        {"key": "r", "setup_seconds": 1.0, "setup_evidence": EVIDENCE},
        {"key": "r", "setup_seconds": 2.0, "setup_evidence": EVIDENCE},
    ]), "repeats an earlier residency"),
])
def test_a_malformed_policy_is_refused(policy, expected) -> None:
    with pytest.raises(dc.ActionContractError, match=expected):
        dc.validate_batch_policy(policy)


def test_a_task_naming_an_unpriced_residency_is_refused() -> None:
    """Unpriced setup is no setup, and no setup is no reason to batch."""

    with pytest.raises(dc.ActionContractError, match="is not priced by the batch"):
        dc.validate_logical_request(_request(
            roster=_roster(_task(0, residency_key="cold"))
        ))


# --------------------------------------------------------------------------
# The batch-input protocol
# --------------------------------------------------------------------------


def test_a_command_without_the_placeholder_stays_an_ordinary_action() -> None:
    """PB does not infer a decomposition from command text."""

    with pytest.raises(dc.ActionContractError, match="exactly once"):
        dc.validate_common_spec(_common(argv=["python", "collect.py"]))


def test_two_placeholders_would_give_one_child_two_batches() -> None:
    with pytest.raises(dc.ActionContractError, match="exactly once"):
        dc.validate_common_spec(_common(argv=[
            "python", dc.TASK_BATCH_PLACEHOLDER, dc.TASK_BATCH_PLACEHOLDER,
        ]))


def test_the_placeholder_is_not_string_interpolation() -> None:
    """Embedding it in a larger argument is the shell-expansion habit refused."""

    with pytest.raises(dc.ActionContractError, match="whole-argument placeholder"):
        dc.validate_common_spec(_common(argv=[
            "python", "collect.py", f"--batch={dc.TASK_BATCH_PLACEHOLDER}",
        ]))
