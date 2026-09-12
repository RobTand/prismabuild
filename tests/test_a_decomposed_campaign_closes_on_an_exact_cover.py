"""Every child passing is not yet the request being answered.

A decomposed campaign's claim is the cover: that between them the children
answered each roster task once, under the plan that fixed their membership.
The children here really run -- a small producer reads the batch it was handed
and writes the manifest its action declared -- so what is checked is the whole
chain the merge rests on: the declared result is the manifest, the manifest is
read back through the receipt that fixed its digest, and the group receipt
appears only when the cover closes.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import socket
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import core as pb, decomposition as dc, pool  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import pbrun  # noqa: E402
import pbcampaign  # noqa: E402

from test_pbrun_detach import _checkout, _queue  # noqa: E402

EVIDENCE = "cas:sha256:" + "0" * 64

#: A producer, in the only sense this test needs one: it reads the envelope it
#: was handed, answers for exactly the tasks in it, and writes the manifest at
#: the name the envelope names -- which is the name its action already sealed
#: as its result path.  ``OUTPUT`` is what a real adapter would take from the
#: roster; spelling it wrong is how the cover is made to fail.
PRODUCER = """
import hashlib, json, sys
batch = json.load(open(sys.argv[1]))
open(batch["result_manifest_path"], "w").write(json.dumps({
    "schema": "prismabuild.child_result_manifest.v1",
    "parent_key": batch["parent_key"],
    "plan_key": batch["plan_key"],
    "child_ordinal": batch["child_ordinal"],
    "results": [
        {
            "task_id": task["id"],
            "output_id": OUTPUT,
            "value_sha256": hashlib.sha256(
                json.dumps(task["payload"], sort_keys=True).encode()
            ).hexdigest(),
        }
        for task in batch["tasks"]
    ],
}))
"""


@pytest.fixture()
def fleet(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(pbrun, "SH", tmp_path)
    monkeypatch.setattr(socket, "gethostname", lambda: "sparky")
    return _checkout(tmp_path), _queue(tmp_path)


def _request(work: Path, *, output: str = 'task["output_id"]') -> dict:
    return {
        "schema": dc.LOGICAL_REQUEST_SCHEMA_V1,
        "common": {
            "argv": [sys.executable, "-c", PRODUCER.replace("OUTPUT", output),
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
                    "output_id": f"out{index:04d}",
                }
                for index in range(66)
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


def _decompose(request) -> tuple[list[dict], dict]:
    return pbcampaign.decompose(
        dc.validate_logical_request(request), transport="pool", priority=0)


def _serve(queue: pool.PoolQueue, *, limit: int | None = None) -> int:
    """Run the queued children here, and say how many actually executed."""

    served = 0
    while limit is None or served < limit:
        one = queue.serve_once(
            tags=["sparky"], python=sys.executable, timeout_s=120.0,
            capacity={"cpu": 4, "mem_gb": 16, "gpu": 1},
        )
        if one is None:
            break
        assert one["status"] == "executed", one
        served += 1
    return served


def _group_path(tmp_path: Path, group: dict) -> Path:
    return (tmp_path / "cas" / pbcampaign.DECOMPOSITIONS
            / group["plan"]["parent_key"][:2] / group["plan"]["parent_key"]
            / "group.json")


# --------------------------------------------------------------------------
# The cover closes
# --------------------------------------------------------------------------

def test_children_that_answered_their_own_batches_earn_one_group_receipt(
    tmp_path: Path, fleet, capsys,
) -> None:
    work, queue = fleet
    records, group = _decompose(_request(work))
    assert len(group["children"]) >= 2, "one child is not a decomposition"
    assert {one["status"] for one in records} == {"submitted"}

    assert _serve(queue) == len(group["children"])

    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    assert pbcampaign.close_group(group, cas=cas) == 0

    receipt = json.loads(_group_path(tmp_path, group).read_text())
    assert receipt["schema"] == dc.GROUP_RECEIPT_SCHEMA_V1
    assert receipt["parent_key"] == group["plan"]["parent_key"]
    assert receipt["plan_key"] == group["plan"]["plan_key"]
    assert receipt["task_count"] == 66
    assert receipt["child_count"] == len(group["children"])
    # The merge is over every answer in plan order, so it is a statement about
    # the whole roster and not about whichever child finished last.
    assert receipt["merged_result_sha256"] == pb.canonical_sha256([
        [f"t{index:04d}", f"out{index:04d}",
         hashlib.sha256(
             json.dumps({"rate": 832 + index}, sort_keys=True).encode()
         ).hexdigest()]
        for index in range(66)
    ])
    assert "group receipt" in capsys.readouterr().err


def test_closing_the_same_group_twice_publishes_the_same_bytes(
    tmp_path: Path, fleet,
) -> None:
    """Nothing in the receipt can vary, so a second close is a re-read."""

    work, queue = fleet
    _, group = _decompose(_request(work))
    _serve(queue)
    cas = pb.PrismaBuildCAS(tmp_path / "cas")

    assert pbcampaign.close_group(group, cas=cas) == 0
    first = _group_path(tmp_path, group).read_bytes()
    assert pbcampaign.close_group(group, cas=cas) == 0
    assert _group_path(tmp_path, group).read_bytes() == first


# --------------------------------------------------------------------------
# The cover does not close
# --------------------------------------------------------------------------

def test_a_child_without_a_receipt_is_never_a_group_success(
    tmp_path: Path, fleet, capsys,
) -> None:
    """An incomplete set stops here even though every child that ran passed."""

    work, queue = fleet
    _, group = _decompose(_request(work))
    assert _serve(queue, limit=len(group["children"]) - 1) \
        == len(group["children"]) - 1

    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    assert pbcampaign.close_group(group, cas=cas) == 1
    assert "no group receipt" in capsys.readouterr().err
    assert not _group_path(tmp_path, group).exists()


def test_a_child_answering_under_the_wrong_output_id_fails_the_cover(
    tmp_path: Path, fleet, capsys,
) -> None:
    """Every child exits zero; the roster is still unanswered.

    This is the whole reason the group receipt is not just "did they all
    pass".  A producer that answers for its own tasks under a name the roster
    never asked for has produced something, and nothing that was wanted.
    """

    work, queue = fleet
    _, group = _decompose(_request(work, output='"elsewhere"'))
    assert _serve(queue) == len(group["children"])

    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    assert pbcampaign.close_group(group, cas=cas) == 1
    assert "not the roster's" in capsys.readouterr().err
    assert not _group_path(tmp_path, group).exists()


def test_a_group_receipt_is_refused_rather_than_replaced(
    tmp_path: Path, fleet,
) -> None:
    """Two runs cannot honestly disagree here, so a difference is the finding."""

    work, queue = fleet
    _, group = _decompose(_request(work))
    _serve(queue)
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    assert pbcampaign.close_group(group, cas=cas) == 0

    stored = json.loads(_group_path(tmp_path, group).read_text())
    with pytest.raises(pbcampaign.ManifestError) as refusal:
        pbcampaign.publish_group_receipt(
            {**stored, "task_count": stored["task_count"] + 1},
            cas=cas, parent_key=group["plan"]["parent_key"],
        )
    assert "not the one this run verified" in str(refusal.value)
