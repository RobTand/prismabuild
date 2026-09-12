"""The partition is a function of the sealed bytes and of nothing else.

Everything downstream of #517 rests on this.  The plan is frozen before the
first child is published and reused verbatim by every recovery, so a batcher
that answered differently on a second box -- or on the same box with a
different roster order in memory, or under a different live offer count --
would hand two workers overlapping claims on the same task.

The optimality tie-breaks are tested against a brute force rather than against
a recorded answer, because the property the design states is *the most batches,
then the smallest maximum estimated wall, then the earliest cuts*, and a golden
partition only shows that today's code agrees with itself.  The brute force is
exponential, so it runs on small rosters; the dynamic program runs on both, and
the large case is what shows the windowed recursion did not quietly change the
answer it produces on the small one.
"""
from __future__ import annotations

from itertools import combinations
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import decomposition as dc  # noqa: E402


def _roster(*specs: tuple[str, float]) -> dict:
    return dc.validate_roster({
        "schema": dc.LOGICAL_TASK_ROSTER_SCHEMA_V1,
        "tasks": [
            {
                "id": f"t{index:04d}",
                "payload": {"rate": 832 + index},
                "residency_key": residency,
                "estimated_seconds": seconds,
                "estimate_evidence": "cas:sha256:" + "0" * 64,
                "output_id": f"t{index:04d}",
            }
            for index, (residency, seconds) in enumerate(specs)
        ],
    })


def _policy(
    *residencies: tuple[str, float],
    fraction: float = 0.20,
    wall: float = 300.0,
) -> dict:
    return dc.validate_batch_policy({
        "schema": dc.ROSTER_BATCH_POLICY_SCHEMA_V1,
        "residencies": [
            {"key": key, "setup_seconds": setup,
             "setup_evidence": "cas:sha256:" + "1" * 64}
            for key, setup in residencies
        ],
        "max_setup_fraction": fraction,
        "max_estimated_wall_seconds": wall,
    })


def _brute_force(
    seconds: list[float], *, setup: float, fraction: float, wall: float,
) -> list[list[int]] | None:
    """Every contiguous exact cover, ranked by the design's three keys."""

    count = len(seconds)
    best = None
    for cuts in range(count):
        for interior in combinations(range(1, count), cuts):
            bounds = [0, *interior, count]
            batches = [
                list(range(bounds[index], bounds[index + 1]))
                for index in range(len(bounds) - 1)
            ]
            walls = [setup + sum(seconds[i] for i in batch) for batch in batches]
            if any(w > wall for w in walls):
                continue
            if any(setup / w > fraction for w in walls):
                continue
            key = (-len(batches), max(walls), list(interior))
            if best is None or key < best[0]:
                best = (key, batches)
    return None if best is None else best[1]


@pytest.mark.parametrize("seconds,setup,fraction,wall", [
    # Even work: the count is bounded by the setup-fraction floor, which
    # here admits three tasks per batch and so three batches of nine.
    ([8.2] * 9, 5.0, 0.20, 300.0),
    # A greedy packer's trap: filling to the wall leaves a remainder too small
    # to amortize its own setup, while a rebalanced cover exists.
    ([60.0, 60.0, 60.0, 60.0, 10.0], 20.0, 0.25, 200.0),
    # Wildly uneven: the wall bound binds on one task and the floor on others.
    ([5.0, 90.0, 5.0, 5.0, 80.0, 5.0, 5.0], 10.0, 0.15, 120.0),
    # A single batch is the only cover.
    ([30.0, 30.0], 40.0, 0.40, 110.0),
])
def test_the_dynamic_program_agrees_with_an_exhaustive_search(
    seconds, setup, fraction, wall,
) -> None:
    """Same optimum, same tie-breaks, on rosters small enough to enumerate."""

    roster = _roster(*[("r", value) for value in seconds])
    policy = _policy(("r", setup), fraction=fraction, wall=wall)
    expected = _brute_force(seconds, setup=setup, fraction=fraction, wall=wall)
    assert expected is not None, "fixture is meant to be feasible"
    observed = dc.partition_roster(roster, policy)
    assert observed == [
        [f"t{index:04d}" for index in batch] for batch in expected
    ]


def test_a_residency_boundary_is_never_crossed() -> None:
    """Two residencies in one process pay both setups, which is the whole cost.

    The run boundary also resets the batcher: the second run's first batch is
    cut from its own first task, not from wherever the first run ended.
    """

    roster = _roster(
        ("warm", 30.0), ("warm", 30.0), ("warm", 30.0),
        ("cold", 30.0), ("cold", 30.0), ("cold", 30.0),
    )
    policy = _policy(("warm", 20.0), ("cold", 20.0), fraction=0.30, wall=200.0)
    partitions = dc.partition_roster(roster, policy)
    residency = {task["id"]: task["residency_key"] for task in roster["tasks"]}
    for batch in partitions:
        assert len({residency[task_id] for task_id in batch}) == 1
    assert [task_id for batch in partitions for task_id in batch] == [
        task["id"] for task in roster["tasks"]
    ]


def test_the_cover_is_exact_and_in_roster_order() -> None:
    """Every task once, no task twice, and the order the producer declared."""

    roster = _roster(*[("r", 3.0 + (index % 7)) for index in range(400)])
    policy = _policy(("r", 45.0), fraction=0.20, wall=300.0)
    partitions = dc.partition_roster(roster, policy)
    flattened = [task_id for batch in partitions for task_id in batch]
    assert flattened == [task["id"] for task in roster["tasks"]]
    assert all(batch for batch in partitions)


def test_the_same_bytes_give_the_same_plan_key_and_a_changed_task_does_not() -> None:
    """Identity moves with the roster, the policy and the algorithm; not with time."""

    request = {
        "schema": dc.LOGICAL_REQUEST_SCHEMA_V1,
        "common": {
            "argv": ["python", "collect.py", "--pb-task-batch",
                     dc.TASK_BATCH_PLACEHOLDER],
            "cwd": "/checkout",
            "demand": {"gpu": 1, "cpu": 4, "mem_gb": 72},
            "gpu_memory_gb": 72,
            "data_manifest": "/inputs/data-manifest.json",
            "env": {"OMP_NUM_THREADS": "1"},
        },
        "roster": _roster(*[("r", 8.2) for _ in range(66)]),
        "batch_policy": _policy(("r", 45.0)),
    }
    first = dc.build_plan(request)
    assert dc.build_plan(request) == first
    assert dc.validate_plan(first) == first

    moved = dict(request)
    moved["batch_policy"] = _policy(("r", 45.0), fraction=0.25)
    replanned = dc.build_plan(moved)
    assert replanned["parent_key"] != first["parent_key"], (
        "the policy is part of what the parent is, not only of how it is cut"
    )
    assert replanned["plan_key"] != first["plan_key"]


def test_a_plan_key_that_does_not_match_its_blueprint_is_refused() -> None:
    """Recovery reuses published plan bytes, so it may not take them on trust."""

    plan = dc.build_plan({
        "schema": dc.LOGICAL_REQUEST_SCHEMA_V1,
        "common": {
            "argv": ["python", "collect.py", dc.TASK_BATCH_PLACEHOLDER],
            "cwd": "/checkout",
            "demand": {"cpu": 1},
            "gpu_memory_gb": None,
            "data_manifest": None,
            "env": {},
        },
        "roster": _roster(*[("r", 8.2) for _ in range(66)]),
        "batch_policy": _policy(("r", 45.0)),
    })
    tampered = {**plan, "partitions": [
        [*plan["partitions"][0], *plan["partitions"][1]],
        *plan["partitions"][2:],
    ]}
    with pytest.raises(dc.ActionContractError, match="does not match its blueprint"):
        dc.validate_plan(tampered)
