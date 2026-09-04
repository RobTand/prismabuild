"""``pool_reset`` after the cutover: which transport a re-submission rides.

The tool recovers work out of ``failed/`` and submits it again through
``pbrun``.  Once an ending in that directory can have been filed by the SLURM
lane, "submit it again" is ambiguous: the pull queue has no worker draining it
under SLURM, so a record the lane wrote must go back through the lane or the
reset is a re-submission into a queue nobody reads.

Two further things are asserted here because both are silent when wrong.  A
``TIMEOUT`` is a failure like any other -- the lane files it under ``failed/``
with ``detail.slurm.state`` saying so -- and it must be resettable, or the one
kind of failure a retry most often fixes is the one this tool skips.  And a
withdrawal is a decision: it stops a re-submission of *its own generation* and
of no other, because an action key is a content hash and asking for the same
work again is not an attempt to defeat somebody's cancellation.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))
sys.path.insert(0, str(REPOSITORY / "tools" / "fleet"))

from prismabuild import pool  # noqa: E402

import pool_reset  # noqa: E402


KEY_SLURM = "a" * 64
KEY_POOL = "b" * 64


def _cas_request(cas_root: Path, key: str, argv: list[str]) -> None:
    path = cas_root / "requests" / key[:2] / f"{key}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({
        "action_key": key,
        "params": {"command": argv},
        "task": {"working_directory": "."},
    }), encoding="utf-8")


def _failed(
    queue_root: Path, key: str, *, checkout: Path, transport: str | None,
    state: str = "FAILED", published_unix: float = 1000.0,
) -> Path:
    record: dict[str, object] = {
        "action_key": key,
        "published_unix": published_unix,
        "status": "failed",
        "attempts": 1,
        "checkout_root": str(checkout),
        "resources": {"cpu": 2, "mem_gb": 4},
        "tags": ["x86"],
        "detail": {"status": "failed", "returncode": 1},
    }
    if transport is not None:
        record["transport"] = transport
        record["schema"] = "prismaquant.prismabuild.slurm_outcome.v1"
        record["detail"]["slurm"] = {"job_id": "1001", "state": state}
    path = queue_root / pool.FAILED / f"{key}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(record), encoding="utf-8")
    return path


@pytest.fixture()
def fleet(tmp_path: Path) -> dict:
    """A queue with one lane-filed failure and one pull-queue failure."""

    queue_root = tmp_path / "pb-queue"
    cas_root = tmp_path / "cas"
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    for key in (KEY_SLURM, KEY_POOL):
        _cas_request(cas_root, key, ["/usr/bin/python3", "-c", f"pass  # {key[:4]}"])
    _failed(queue_root, KEY_SLURM, checkout=checkout,
            transport="slurm", state="TIMEOUT")
    _failed(queue_root, KEY_POOL, checkout=checkout, transport=None)
    return {"queue": pool.PoolQueue(queue_root), "queue_root": queue_root,
            "cas_root": cas_root, "checkout": checkout}


def _plan_for(plans: list[dict], key: str) -> dict:
    found = [plan for plan in plans if plan["key"] == key]
    assert found, f"{key[:12]} was not planned"
    return found[0]


def test_a_lane_filed_timeout_is_resubmitted_through_the_lane(fleet) -> None:
    """The record says which dispatcher carried it, so the reset can too.

    A ``TIMEOUT`` is the case that matters most: it is a failure the pull queue
    would have retried, it lands in ``failed/`` like every other ending, and
    re-submitting it into a queue with no workers wastes the operator's whole
    reset.
    """

    plans, skipped = pool_reset.plan_resets(
        fleet["queue"], cas_root=fleet["cas_root"])
    assert not skipped, skipped
    plan = _plan_for(plans, KEY_SLURM)
    assert plan["transport"] == "slurm"
    command = pool_reset.submit_command(plan, transport=plan["transport"])
    assert "--transport" in command
    assert command[command.index("--transport") + 1] == "slurm"


def test_a_pull_queue_failure_stays_on_the_pull_queue(fleet, monkeypatch) -> None:
    """Even when the operator's shell has already been cut over.

    The transport is a property of the record, not of the environment: an
    ambient ``PRISMABUILD_TRANSPORT`` must not silently re-route work whose
    ending the pull queue filed, so the child's transport is always stated.
    """

    monkeypatch.setenv("PRISMABUILD_TRANSPORT", "")
    plans, _ = pool_reset.plan_resets(fleet["queue"], cas_root=fleet["cas_root"])
    plan = _plan_for(plans, KEY_POOL)
    assert plan["transport"] == "pool"
    command = pool_reset.submit_command(plan, transport=plan["transport"])
    assert command[command.index("--transport") + 1] == "pool"


def test_asking_for_slurm_carries_every_reset_onto_the_lane(fleet) -> None:
    """``--transport slurm`` is the cutover switch for a bulk reset."""

    plans, _ = pool_reset.plan_resets(
        fleet["queue"], cas_root=fleet["cas_root"], transport="slurm")
    assert {plan["transport"] for plan in plans} == {"slurm"}


def test_a_withdrawal_of_this_generation_is_never_resubmitted(fleet) -> None:
    """The marker, not just the stamp on the record.

    A cancellation reaches ``failed/`` two ways -- the lane files the ending
    and ``--withdraw`` files the marker -- and only the marker is guaranteed to
    be there.  Re-submitting past it undoes an operator's decision.
    """

    marker = fleet["queue_root"] / pool.WITHDRAWN / f"{KEY_SLURM}.json"
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.write_text(json.dumps({
        "action_key": KEY_SLURM, "status": "withdrawn",
        "published_unix": 1000.0, "withdrawn_unix": 1001.0,
        "withdrawn_by": "rob@sparky", "reason": "wrong shard",
    }), encoding="utf-8")

    plans, skipped = pool_reset.plan_resets(
        fleet["queue"], cas_root=fleet["cas_root"])
    assert [plan["key"] for plan in plans] == [KEY_POOL]
    assert any("withdrawn by rob@sparky" in why for _, why in skipped)


def test_a_withdrawal_of_another_generation_does_not_blacklist_the_key(
    fleet,
) -> None:
    """An action key is a content hash; the same key is how work is re-asked for.

    ``PoolQueue.withdrawal_covers`` scopes the guard to ``published_unix`` for
    exactly this reason, and reading the directory by filename alone -- which
    is what this tool did -- turns one cancellation into a permanent ban.
    """

    marker = fleet["queue_root"] / pool.WITHDRAWN / f"{KEY_SLURM}.json"
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.write_text(json.dumps({
        "action_key": KEY_SLURM, "status": "withdrawn",
        "published_unix": 12.0, "withdrawn_unix": 13.0,
        "withdrawn_by": "rob@sparky",
    }), encoding="utf-8")

    plans, _ = pool_reset.plan_resets(
        fleet["queue"], cas_root=fleet["cas_root"])
    assert KEY_SLURM in {plan["key"] for plan in plans}
