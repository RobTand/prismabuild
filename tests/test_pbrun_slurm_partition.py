"""``pbrun --transport slurm`` names the partition the lane's rule derives.

The rule lives in ``slurm_lane.partition_for``; this checks that ``pbrun``
actually hands its answer to ``slurm_lane.run`` for each of the three shapes
an agent submits: GPU work, untagged CPU work, and CPU work pinned to a box.
"""
from __future__ import annotations

from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
from prismabuild import slurm_lane as sl  # noqa: E402
import pbrun  # noqa: E402


class _Captured(Exception):
    def __init__(self, kwargs: dict) -> None:
        super().__init__("captured")
        self.kwargs = kwargs


def _lane_kwargs(monkeypatch: pytest.MonkeyPatch, *, tags, demand,
                 exclusive: bool = False, **extra):
    def record(action, **kwargs):
        raise _Captured(kwargs)

    monkeypatch.setattr(pbrun.slurm_lane, "run", record)
    with pytest.raises(_Captured) as raised:
        pbrun.slurm_outcome(
            {"action_key": "0" * 64}, cas=None, request_path="request.json",
            tags=list(tags), demand=dict(demand), exclusive=exclusive,
            timeout_s=60.0, wait_s=60.0, retry_safe=False, max_attempts=1,
            **extra,
        )
    return raised.value.kwargs


def _partition_pbrun_sends(monkeypatch: pytest.MonkeyPatch, *, tags, demand):
    return _lane_kwargs(monkeypatch, tags=tags, demand=demand)["partition"]


def test_gpu_work_is_sent_to_the_gpu_partition(monkeypatch) -> None:
    assert _partition_pbrun_sends(
        monkeypatch, tags=["gb10"], demand={"gpu": 1, "mem_gb": 16}
    ) == sl.GPU_PARTITION


def test_untagged_cpu_work_is_sent_to_the_cpu_partition(monkeypatch) -> None:
    assert _partition_pbrun_sends(
        monkeypatch, tags=[], demand={"cpu": 8, "mem_gb": 32}
    ) == sl.CPU_PARTITION


def test_pinned_cpu_work_is_left_to_its_constraint(monkeypatch) -> None:
    assert _partition_pbrun_sends(
        monkeypatch, tags=["sparky"], demand={"cpu": 4, "mem_gb": 8}
    ) is None


def test_the_priority_pbrun_accepted_reaches_the_lane(monkeypatch) -> None:
    """``--priority`` was parsed, recorded in the pool's publication, and
    dropped on the way to SLURM.  ``pool_reset`` submits its bulk resets at
    ``--priority -10``, so dropping it put a reset alongside interactive work
    rather than behind it."""

    kwargs = _lane_kwargs(
        monkeypatch, tags=[], demand={"cpu": 1}, priority=-10)
    assert kwargs["priority"] == -10


def test_an_exclusive_action_asking_for_more_than_one_device_is_refused(
    monkeypatch,
) -> None:
    """``--gpu-capacity`` above one is read and then discarded under SLURM:
    ``LaneResources.gres()`` answers ``gpu:1`` for any exclusive action.  It is
    harmless while every box has one device and silently wrong the day a
    two-GPU box joins, so it is refused where the caller can still read it."""

    with pytest.raises(SystemExit) as refused:
        pbrun.slurm_outcome(
            {"action_key": "0" * 64}, cas=None, request_path="request.json",
            tags=[], demand={"gpu": 2, "mem_gb": 16}, exclusive=True,
            timeout_s=60.0, wait_s=60.0, retry_safe=False, max_attempts=1,
        )
    message = str(refused.value)
    assert "--exclusive --gpu-capacity 2" in message
    assert "--gres=gpu:1" in message


def test_one_exclusive_device_is_still_accepted(monkeypatch) -> None:
    """The shape ``--exclusive`` alone produces, which the refusal must not
    catch."""

    kwargs = _lane_kwargs(
        monkeypatch, tags=["gb10"], demand={"gpu": 1, "mem_gb": 16},
        exclusive=True)
    assert kwargs["resources"].gres() == "gpu:1"
