"""Logical-request common policies are ordinary sealed child policies.

The decomposition owns task membership, not a second abbreviated submission
language. A policy shared by every child belongs in ``common`` and must reach
the same pbrun row vocabulary as an ordinary campaign row. Submission-level
priority and a single-action reseal handle deliberately remain outside it.
"""
from __future__ import annotations

from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import decomposition as dc  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import pbcampaign  # noqa: E402


def _common(**overrides: object) -> dict:
    common = {
        "argv": ["python", "collect.py", dc.TASK_BATCH_PLACEHOLDER],
        "cwd": "/checkout",
        "demand": {"gpu": 1, "cpu": 1, "mem_gb": 4},
        "gpu_memory_gb": 2,
        "data_manifest": None,
        "env": {"OMP_NUM_THREADS": "1"},
    }
    common.update(overrides)
    return common


def test_common_accepts_every_shared_child_policy_and_maps_it_to_pbrun() -> None:
    """The pre-fix schema rejected these as unknown common fields."""

    policies = {
        "timeout_s": 600.0,
        "progress_phases": ["prepare=60", "measure=120"],
        "progress_cycle": True,
        "profile": "sample",
        "retry_safe": True,
        "max_attempts": 1,
        "tags": ["x86"],
        "snapshot_ref": ["main"],
        "deterministic": True,
        "no_default_env": True,
        "exclusive": True,
        "gpu_capacity": 1,
        "measurement": True,
        "host_class": "x86",
    }
    common = dc.validate_common_spec(_common(**policies))

    assert {name: common[name] for name in policies} == policies
    argv = pbcampaign.pbrun_argv(common)
    for flag in (
        "--timeout-s", "--progress-phase", "--progress-cycle", "--profile",
        "--retry-safe", "--max-attempts", "--tag", "--snapshot-ref",
        "--deterministic", "--no-default-env", "--exclusive",
        "--gpu-capacity", "--measurement", "--host-class",
    ):
        assert flag in argv


@pytest.mark.parametrize("field", ["anywhere", "here"])
def test_common_accepts_each_portability_policy(field: str) -> None:
    """Their mutual-exclusion rule stays at the pbrun submission boundary."""

    common = dc.validate_common_spec(_common(**{field: True}))
    assert common[field] is True
    assert {"--", "python", "collect.py", dc.TASK_BATCH_PLACEHOLDER}.issubset(
        pbcampaign.pbrun_argv(common)
    )


def test_absent_common_policies_keep_the_legacy_canonical_shape() -> None:
    """Old logical requests must retain their parent identities unchanged."""

    common = dc.validate_common_spec(_common())
    assert set(common) == {
        "argv", "cwd", "demand", "env", "gpu_memory_gb", "data_manifest",
    }


@pytest.mark.parametrize("field", ["priority", "as_sealed_by"])
def test_common_refuses_submitter_only_policy_fields(field: str) -> None:
    """One parent has many children, so neither field has one sound meaning."""

    value = 1 if field == "priority" else "a" * 64
    with pytest.raises(dc.ActionContractError, match="extra"):
        dc.validate_common_spec(_common(**{field: value}))
