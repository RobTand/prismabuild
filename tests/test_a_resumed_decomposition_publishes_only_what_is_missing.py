"""A campaign interrupted halfway resumes into the batches it already cut.

The plan is the thing that must not be re-derived.  A second run that asked
the batcher for a fresh opinion would be correct only if the batcher agreed
with itself across versions -- and nothing but the published plan can prove
that it did.  So the published plan is read back and reused, the publication
index says which children the plan authorized, and the re-run fills the gaps.

What is checked here is that ordering under the two things that actually
happen: a run that died between publishing some children and publishing the
rest, and a source tree that moved on between two runs.
"""
from __future__ import annotations

import json
from pathlib import Path
import socket
import subprocess
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import core as pb, decomposition as dc, pool  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import pbrun  # noqa: E402
import pbcampaign  # noqa: E402

from test_pbrun_detach import _checkout, _queue  # noqa: E402

EVIDENCE = "cas:sha256:" + "0" * 64


@pytest.fixture()
def fleet(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(pbrun, "SH", tmp_path)
    monkeypatch.setattr(socket, "gethostname", lambda: "sparky")
    return _checkout(tmp_path), _queue(tmp_path)


def _request(work: Path, count: int = 66) -> dict:
    return {
        "schema": dc.LOGICAL_REQUEST_SCHEMA_V1,
        "common": {
            "argv": ["/bin/bash", "-lc", "printf ok", "measure",
                     dc.TASK_BATCH_PLACEHOLDER],
            "cwd": str(work),
            "demand": {"cpu": 1, "mem_gb": 4},
            "gpu_memory_gb": None,
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
    }


def _decompose(request) -> list[dict]:
    return pbcampaign.decompose(
        dc.validate_logical_request(request), transport="pool", priority=0)


def _refuse_to_partition(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make asking the batcher a second time an error rather than a guess."""

    def refuse(*args, **kwargs):
        raise AssertionError(
            "the batcher was asked again; a resumed campaign must read the "
            "published plan instead"
        )

    monkeypatch.setattr(dc, "partition_roster", refuse)


def _plan_path(tmp_path: Path, parent_key: str) -> Path:
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    return pbcampaign.decomposition_dir(cas, parent_key) / "plan.json"


def _index_path(tmp_path: Path, parent_key: str) -> Path:
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    return pbcampaign.decomposition_dir(cas, parent_key) / "publication.json"


def _published_plan(tmp_path: Path) -> dict:
    root = tmp_path / "cas" / pbcampaign.DECOMPOSITIONS
    plans = sorted(root.glob("*/*/plan.json"))
    assert len(plans) == 1, f"expected one plan, found {plans!r}"
    return dc.validate_plan(json.loads(plans[0].read_text(encoding="utf-8")))


# --------------------------------------------------------------------------
# Resuming
# --------------------------------------------------------------------------

def test_a_resumed_campaign_publishes_only_the_children_that_are_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fleet,
) -> None:
    """One child lost, one child republished, and the batcher never consulted."""

    work, queue = fleet
    request = _request(work)
    first = _decompose(request)
    assert {record["status"] for record in first} == {"submitted"}
    plan = _published_plan(tmp_path)
    assert len(first) == len(plan["partitions"]) >= 3

    # The state a campaign dies in: the index and most of the children are
    # published, one is not.
    missing = str(first[0]["action_key"])
    (queue.root / pool.READY / f"{missing}.json").unlink()

    _refuse_to_partition(monkeypatch)
    second = _decompose(request)

    assert [str(record["action_key"]) for record in second] == [
        str(record["action_key"]) for record in first
    ], "the resumed run must publish the same children, in the same order"
    assert second[0]["status"] == "submitted"
    assert {record["status"] for record in second[1:]} == {"attached"}, (
        "a child still in the queue is attached to, not published twice"
    )


def test_a_second_run_of_an_untouched_request_reuses_the_published_plan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fleet,
) -> None:
    """Nothing changed, so nothing is re-derived and nothing is re-published."""

    work, _ = fleet
    request = _request(work)
    first = _decompose(request)
    before = _plan_path(tmp_path, _published_plan(tmp_path)["parent_key"])
    plan_bytes = before.read_bytes()

    _refuse_to_partition(monkeypatch)
    second = _decompose(request)

    assert before.read_bytes() == plan_bytes, "the plan is immutable"
    assert [str(record["action_key"]) for record in second] == [
        str(record["action_key"]) for record in first
    ]
    assert {record["status"] for record in second} == {"attached"}


def test_a_tree_that_moved_on_is_a_different_parent_and_leaves_the_old_plan(
    tmp_path: Path, fleet,
) -> None:
    """A parent answers for the tree it was frozen against, and only that one."""

    work, _ = fleet
    request = _request(work)
    _decompose(request)
    old = _published_plan(tmp_path)
    old_path = _plan_path(tmp_path, old["parent_key"])
    old_bytes = old_path.read_bytes()

    (work / "seed.txt").write_text("moved on\n", encoding="utf-8")
    for args in (("add", "seed.txt"), ("commit", "-qm", "moved on")):
        done = subprocess.run(["git", "-C", str(work), *args],
                              capture_output=True, text=True)
        assert done.returncode == 0, done.stderr
    _decompose(request)

    plans = sorted((tmp_path / "cas" / pbcampaign.DECOMPOSITIONS)
                   .glob("*/*/plan.json"))
    assert len(plans) == 2, "a changed tree is a second parent, not an edit"
    assert old_path.read_bytes() == old_bytes, (
        "the first parent's plan still describes the tree it was cut for"
    )


def test_a_publication_index_that_names_other_children_is_refused(
    tmp_path: Path, fleet,
) -> None:
    """Every input to the index is fixed, so a differing one is damage."""

    work, _ = fleet
    request = _request(work)
    _decompose(request)
    parent = _published_plan(tmp_path)["parent_key"]
    path = _index_path(tmp_path, parent)
    index = json.loads(path.read_text(encoding="utf-8"))
    index["child_action_keys"][0] = "f" * 64
    path.unlink()
    path.write_text(json.dumps(index), encoding="utf-8")

    with pytest.raises(pbcampaign.ManifestError) as refusal:
        _decompose(request)
    assert "different children" in str(refusal.value)


def test_the_index_names_exactly_the_children_that_were_published(
    tmp_path: Path, fleet,
) -> None:
    """The index is written before the first child, and it is the roster of them."""

    work, _ = fleet
    records = _decompose(_request(work))
    parent = _published_plan(tmp_path)["parent_key"]
    index = json.loads(
        _index_path(tmp_path, parent).read_text(encoding="utf-8"))
    assert index["schema"] == dc.PUBLICATION_INDEX_SCHEMA_V1
    assert index["child_action_keys"] == [
        str(record["action_key"]) for record in records
    ]


# --------------------------------------------------------------------------
# What the tool will not do
# --------------------------------------------------------------------------

def test_a_logical_request_is_refused_on_the_slurm_lane(
    tmp_path: Path, fleet,
) -> None:
    """The SLURM lane submits one job per action; a plan has no path through it."""

    work, _ = fleet
    with pytest.raises(pbcampaign.ManifestError) as refusal:
        pbcampaign.decompose(
            dc.validate_logical_request(_request(work)),
            transport="slurm", priority=0,
        )
    assert "--transport pool" in str(refusal.value)


def test_a_manifest_that_is_neither_a_list_nor_a_request_says_so(
    tmp_path: Path,
) -> None:
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps({"schema": "something.else.v1"}),
                    encoding="utf-8")
    with pytest.raises(pbcampaign.ManifestError) as refusal:
        pbcampaign.load_manifest(str(path))
    assert "something.else.v1" in str(refusal.value)


def test_a_logical_request_reaches_the_campaign_through_load_manifest(
    tmp_path: Path, fleet,
) -> None:
    """The object comes back validated, so ``main`` branches on shape alone."""

    work, _ = fleet
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(_request(work)), encoding="utf-8")
    loaded = pbcampaign.load_manifest(str(path), transport="pool")
    assert isinstance(loaded, dict)
    assert loaded["schema"] == dc.LOGICAL_REQUEST_SCHEMA_V1
    assert loaded["common"]["argv"][-1] == dc.TASK_BATCH_PLACEHOLDER
