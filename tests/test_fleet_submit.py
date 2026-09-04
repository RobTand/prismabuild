"""The one submit path the fleet's producers share, on both transports.

Three tools seal their own actions and enqueued them by calling
``PoolQueue.publish`` directly.  That is a bypass once there are two
dispatchers: after the cutover a direct publish puts 120 export shards in a
queue no worker drains, and it *succeeds*, so nothing anywhere says so.

What is asserted here is that the transport actually decides -- an ``sbatch``
and no pull-queue item under SLURM, a pull-queue item and no ``sbatch`` under
the pool -- and that the one action shape these producers currently build is
refused before anything is submitted rather than after the scheduler has
placed it.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))
sys.path.insert(0, str(REPOSITORY / "tools" / "fleet"))

from prismabuild import core as pb  # noqa: E402
from prismabuild import pool  # noqa: E402
from prismabuild import slurm_lane as sl  # noqa: E402

import fleet_submit  # noqa: E402

from test_slurm_lane import (  # noqa: E402
    _paper_action,
    _runnable_action,
    _submissions,
    fleet,
)

__all__ = ["fleet"]


def _cas(tmp_path: Path) -> pb.PrismaBuildCAS:
    return pb.PrismaBuildCAS(tmp_path / "cas")


def _ready(queue_root: Path) -> list[str]:
    directory = queue_root / pool.READY
    if not directory.is_dir():
        return []
    return sorted(path.stem for path in directory.glob("*.json"))


def test_the_slurm_transport_sbatches_and_never_touches_the_queue(
    tmp_path: Path, fleet: Path,
) -> None:
    """The producer's demand vocabulary reaches the scheduler as scheduler flags."""

    cas = _cas(tmp_path)
    action = _runnable_action(tmp_path, cas)
    request = cas.publish_action_request(action)
    queue_root = tmp_path / "pb-queue"

    submission = fleet_submit.submit(
        action, cas=cas, request_path=request, transport="slurm",
        tags=["gb10"], needs_gpu=True, resources={"gpu": 1, "mem_gb": 16},
        queue_root=queue_root, timeout_s=600.0,
    )

    assert submission.transport == "slurm"
    assert submission.job_id
    assert submission.describe() == f"slurm job {submission.job_id}"
    assert Path(submission.where).exists()

    rows = _submissions(fleet)
    assert len(rows) == 1
    argv = rows[0]["argv"]
    assert "--gres=shard:1" in argv
    assert "--constraint=gb10" in argv
    assert "--mem=16384M" in argv
    assert _ready(queue_root) == []


def test_the_pool_transport_publishes_and_never_sbatches(
    tmp_path: Path, fleet: Path,
) -> None:
    """The pull queue stays live until the cutover, byte for byte as before."""

    cas = _cas(tmp_path)
    action = _paper_action(tmp_path, "pool")
    request = cas.publish_action_request(action)
    queue_root = tmp_path / "pb-queue"
    checkout = tmp_path / "checkout"
    checkout.mkdir()

    submission = fleet_submit.submit(
        action, cas=cas, request_path=request, transport="pool",
        checkout_root=checkout, tags=["gb10"], needs_gpu=True,
        resources={"gpu": 1, "mem_gb": 16}, queue_root=queue_root,
    )

    assert submission.transport == "pool"
    assert submission.describe() == "queued"
    key = str(action["action_key"])
    assert _ready(queue_root) == [key]
    item = json.loads((queue_root / pool.READY / f"{key}.json").read_text())
    assert item["checkout_root"] == str(checkout)
    assert item["resources"] == {"gpu": 1, "mem_gb": 16}
    assert _submissions(fleet) == []


def test_an_action_with_no_sealed_snapshot_is_refused_before_it_is_submitted(
    tmp_path: Path, fleet: Path,
) -> None:
    """The shape all three producers build today, and the lane's own rule.

    ``slurm_job`` materializes ``params.checkout_snapshot`` on the node that
    won the allocation and refuses an action carrying none.  Refusing here
    costs one message; refusing there costs one message per placed job, after
    the scheduler has queued every one of them.
    """

    cas = _cas(tmp_path)
    action = _paper_action(tmp_path, "unsnapshotted")
    request = cas.publish_action_request(action)
    checkout = tmp_path / "checkout"
    checkout.mkdir()

    with pytest.raises(fleet_submit.SubmitRefused) as refusal:
        fleet_submit.submit(
            action, cas=cas, request_path=request, transport="slurm",
            checkout_root=checkout, tags=["gb10"],
            queue_root=tmp_path / "pb-queue",
        )
    assert "snapshot-addressed" in str(refusal.value)
    assert _submissions(fleet) == []


def test_a_submission_retires_a_live_withdrawal_on_the_lane_too(
    tmp_path: Path, fleet: Path,
) -> None:
    """``publish`` does this and says why; the lane path has to do it itself.

    A marker left in place makes the re-submitted action unrunnable and the
    only remedy a hand edit of the live queue.  The decision is kept, not
    deleted.
    """

    cas = _cas(tmp_path)
    action = _runnable_action(tmp_path, cas)
    request = cas.publish_action_request(action)
    key = str(action["action_key"])
    queue_root = tmp_path / "pb-queue"
    marker = queue_root / pool.WITHDRAWN / f"{key}.json"
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.write_text(json.dumps({
        "action_key": key, "status": "withdrawn", "published_unix": 5.0,
        "withdrawn_by": "rob@sparky",
    }), encoding="utf-8")

    fleet_submit.submit(
        action, cas=cas, request_path=request, transport="slurm",
        tags=["gb10"], resources={"mem_gb": 4}, queue_root=queue_root,
        timeout_s=600.0,
    )

    assert not marker.exists()
    kept = sorted((queue_root / pool.WITHDRAWN / "superseded").glob("*.json"))
    assert len(kept) == 1
    assert json.loads(kept[0].read_text())["withdrawn_by"] == "rob@sparky"


def test_the_job_entry_is_a_sibling_of_this_module(tmp_path: Path) -> None:
    """Both deployed layouts, one path.

    ``publish_runtime`` writes every fleet script twice -- ``tools/<name>`` and
    ``tools/fleet/<name>`` -- so a sibling resolves wherever the caller was
    published, while a path assembled from the runtime root has to guess.
    """

    assert fleet_submit.JOB_ENTRY.is_file()
    assert fleet_submit.JOB_ENTRY.name == "slurm_job.py"
    assert fleet_submit.JOB_ENTRY.parent == Path(
        fleet_submit.__file__).resolve().parent


# --------------------------------------------------------------------------
# The producers themselves: no tool may keep its own publish
# --------------------------------------------------------------------------

def assert_routed_through_the_shared_submit(name: str) -> None:
    """No producer may keep a publish of its own.

    Read from the source because that is the property: a tool that still calls
    ``PoolQueue.publish`` bypasses the lane no matter what its own tests do,
    and its ``main`` cannot be exercised here (the ladder copies a wrapper onto
    the shared mount at import of its work, and none of the three has a fleet
    to talk to).
    """

    text = (REPOSITORY / "tools" / "fleet" / name).read_text(encoding="utf-8")
    assert ".publish(" not in text.replace("publish_action_request(", ""), name
    assert "fleet_submit.submit(" in text, name
    assert "add_transport_argument" in text, name


def test_the_ladder_dispatcher_routes_through_the_shared_submit() -> None:
    assert_routed_through_the_shared_submit("dispatch_tessera_ladder.py")
