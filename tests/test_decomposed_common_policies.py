"""Logical-request common policies are ordinary sealed child policies.

The decomposition owns task membership, not a second abbreviated submission
language. A policy shared by every child belongs in ``common`` and must reach
the same pbrun row vocabulary as an ordinary campaign row. Submission-level
priority and a single-action reseal handle deliberately remain outside it.
"""
from __future__ import annotations

import json
from pathlib import Path
import socket
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import core as pb, decomposition as dc, pool  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import pbcampaign, pbrun  # noqa: E402

from test_a_decomposed_campaign_closes_on_an_exact_cover import _request  # noqa: E402
from test_pbrun_detach import _checkout  # noqa: E402


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


def _progress_queue(tmp_path: Path) -> pool.PoolQueue:
    """The existing cover queue, with the declared progress contract."""

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.announce(
        host="sparky",
        tags=["sparky", "x86", pb.PROGRESS_TAG, pb.PROGRESS_HELPER_TAG],
        has_gpu=False,
        capacity={"cpu": 4, "mem_gb": 16, "gpu": 0},
        progress_contracts=[pb.PROGRESS_RECORD_SCHEMA_V1],
    )
    return queue


def test_decomposition_seals_shared_policies_on_every_child(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The logical path must seal policies, not merely turn them into flags."""

    monkeypatch.setattr(pbrun, "SH", tmp_path)
    monkeypatch.setattr(socket, "gethostname", lambda: "sparky")
    work = _checkout(tmp_path)
    _progress_queue(tmp_path)

    request = _request(work)
    request["common"].update({
        "timeout_s": 600.0,
        "progress_phases": ["prepare=60", "measure=120"],
        "profile": "sample",
        "retry_safe": True,
        "max_attempts": 1,
        "tags": ["x86"],
    })
    records, group = pbcampaign.decompose(
        dc.validate_logical_request(request), transport="pool", priority=0,
    )
    assert {record["status"] for record in records} == {"submitted"}
    progress = pbrun.parse_progress_phases(request["common"]["progress_phases"])
    for child in group["children"]:
        params = child["params"]
        assert params["execution_timeout_s"] == 600.0
        assert params[pb.PROGRESS_PARAM] == progress
        assert params[pb.PROFILE_PARAM] == "sample"
        assert params["retry_policy"] == {"max_attempts": 1, "retry_safe": True}
        assert "x86" in params["placement"]["required_tags"]
        assert pb.PROGRESS_TAG in params["placement"]["required_tags"]
        assert pb.PROGRESS_HELPER_TAG in params["placement"]["required_tags"]

    _, baseline = pbcampaign.decompose(
        dc.validate_logical_request(_request(work)), transport="pool", priority=0,
    )
    assert group["plan"]["parent_key"] != baseline["plan"]["parent_key"]
    assert [child["action_key"] for child in group["children"]] != [
        child["action_key"] for child in baseline["children"]
    ]


def test_logical_load_refuses_a_malformed_progress_policy_before_publish(
    tmp_path: Path,
) -> None:
    """The row-policy boundary rejects a bad common contract before a plan exists."""

    work = _checkout(tmp_path)
    request = _request(work)
    request["common"]["progress_cycle"] = True
    manifest = tmp_path / "logical-request.json"
    manifest.write_text(json.dumps(request), encoding="utf-8")

    with pytest.raises(pbcampaign.ManifestError, match="requires --progress-phase"):
        pbcampaign.load_manifest(manifest, transport="pool")
    assert not (tmp_path / "cas").exists()
