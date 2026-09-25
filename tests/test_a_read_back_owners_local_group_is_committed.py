"""A read-back owner's group read back only on its own box is committed (#1034).

Since PQ #1118 a Stage A owner reads the groups it wrote from its own local
spool and never publishes them, so `publish_prepaid_batch` never ran
`commit_batch` for them, and `commit_origin_batch` refused a read-back
template (`template-reads-back`).  Each such group's prewrite stayed
outstanding for the owner's whole life, charged at its ceiling in
`_outstanding_sums`, and at the owner's end nothing retired it.

The fix: a read-back template's group commits at its origin, with no stage
copy, once its spool export is acknowledged -- the only path that carries
the landed identities (`ProducedSpool.commit_origin_group`).  Driven through
the real spool export action:

*   the commit consumes the prewrite, and the instance is charged the
    group's actual bytes, not its ceiling;
*   a direct `commit_origin_batch` of a read-back template without the
    landed identities still refuses, so a read-back owner cannot skip its
    export's acknowledgement;
*   a ``consumed`` read-back batch is retired once its owner attempt has
    ended, success included, because no other action can declare it;
*   a ``retain`` one is left to its owner and `reclaim_origin`, as #912's;
*   `publish_prepaid_batch` and `ensure_batch_materialized` refuse such a
    batch by name (``batch-committed-at-origin``) rather than try to stage it.
"""
from __future__ import annotations

from pathlib import Path

import pytest

import test_prepaid_writer_integration as fx
from test_produced_spool import claim_export, prepare, world
from prismabuild import produced_output as po

_isolated_synthetic_launch_context = fx._isolated_synthetic_launch_context

CEILING = 64
PAYLOAD = b"boundary group bytes"


def _acknowledged(spool):
    """One group written locally, exported through the real action, acknowledged.

    The owner reads it back from its local spool; nothing publishes it.
    """

    source, destination, entries = prepare(spool, payload=PAYLOAD,
                                           ceiling=CEILING)
    handle = spool.submit_group("b1", entries)
    assert handle["ok"], handle
    outcome = spool.queue.execute(claim_export(spool, handle), timeout_s=120)
    assert outcome.get("returncode") == 0, outcome
    spool.queue.finish(handle["export_key"], status="executed")
    assert spool.poll_group("b1")["complete"]
    assert source.read_bytes() == PAYLOAD, "the owner's local copy stays readable"
    descriptor = po.validate_descriptor({
        "schema": po.DESCRIPTOR_SCHEMA_V2, "slot": "s0",
        "artifact_class": "payload", "path": str(destination),
        "bytes": len(PAYLOAD), "sha256": entries[0]["sha256"],
        "producer_generation": po.mint_generation(),
        "owner_action_key": spool.owner,
        "owner_attempt": spool.instance["owner_attempt"]},
        spool.template, spool.instance)
    return destination, descriptor


def _prewrites(spool) -> list[Path]:
    return sorted(po._prewrites_dir(spool.queue.root, spool.instance)
                  .glob("*.prewrite.json"))


def _charged(spool) -> dict[str, int]:
    return po._outstanding_sums(spool.queue.root, spool.instance,
                                exclude_batch_id="")


def test_the_acknowledged_group_is_committed_and_charged_its_actual_bytes(
        tmp_path: Path) -> None:
    spool = world(tmp_path)
    assert not po.is_write_only(spool.template)
    destination, descriptor = _acknowledged(spool)
    assert _charged(spool)["payload"] == CEILING, "the prewrite charges the ceiling"

    committed = spool.commit_origin_group("b1", [descriptor])

    assert committed["ok"] is True, committed
    assert committed["origin_only"] is True
    assert committed["class_bytes"] == {
        "payload": len(PAYLOAD), "checkpoint": 0, "temp": 0}
    assert not _prewrites(spool), "the commit consumes the prewrite"
    assert _charged(spool) == {"payload": len(PAYLOAD), "checkpoint": 0,
                               "temp": 0}
    assert destination.read_bytes() == PAYLOAD
    again = spool.commit_origin_group("b1", [descriptor])
    assert again["ok"] is True and again["duplicate"] is True
    # The batch is not a handoff: no consumer can build a manifest over it.
    with pytest.raises(po.ProducedOutputError, match="not-write-only"):
        po.load_origin_batch(spool.queue.root, committed["ref"])
    with pytest.raises(po.ProducedOutputError, match="not-write-only"):
        po.declare_origin_consumer(spool.queue, committed["ref"],
                                   consumer_action_key=fx._hexkey("reader"))


def test_a_read_back_origin_commit_needs_the_landed_identities(
        tmp_path: Path) -> None:
    spool = world(tmp_path)
    _destination, descriptor = _acknowledged(spool)

    refused = po.commit_origin_batch(spool.queue, spool.instance,
                                     spool.template, [descriptor],
                                     batch_id="b1")

    assert refused == {"ok": False, "refusal": "template-reads-back"}
    assert _prewrites(spool), "nothing was consumed"


def test_a_consumed_read_back_batch_is_retired_when_its_owner_succeeds(
        tmp_path: Path) -> None:
    spool = world(tmp_path)
    destination, descriptor = _acknowledged(spool)
    committed = spool.commit_origin_group("b1", [descriptor],
                                          lifetime=po.ORIGIN_LIFETIME_CONSUMED)
    assert committed["ok"] is True, committed
    assert spool.release_group("b1")["ok"]

    assert po.origin_retirement_tick(spool.queue) == [], (
        "a live owner may still read it")
    assert destination.exists()

    spool.queue.finish(spool.owner, status="executed")
    events = po.origin_retirement_tick(spool.queue)

    assert [(event["event"], event["reason"]) for event in events] == [
        (po.ORIGIN_RETIRED_EVENT, "orphan")]
    assert not destination.exists()
    assert _charged(spool) == {"payload": 0, "checkpoint": 0, "temp": 0}


def test_a_retained_read_back_batch_is_left_to_its_owner(tmp_path: Path) -> None:
    spool = world(tmp_path)
    destination, descriptor = _acknowledged(spool)
    assert spool.commit_origin_group("b1", [descriptor])["ok"]
    spool.queue.finish(spool.owner, status="executed")

    assert po.origin_retirement_tick(spool.queue) == []
    assert destination.exists()
    assert _charged(spool)["payload"] == len(PAYLOAD)
    assert po.reclaim_origin(spool.queue, spool.instance, spool.template,
                             batch_id="b1")["refusal"] == "origin-present-retain"
    destination.unlink()
    assert po.reclaim_origin(spool.queue, spool.instance, spool.template,
                             batch_id="b1")["reclaimed"] is True
    assert _charged(spool)["payload"] == 0


def test_the_staged_paths_answer_an_origin_committed_read_back_batch(
        tmp_path: Path) -> None:
    spool = world(tmp_path)
    _destination, descriptor = _acknowledged(spool)
    assert spool.commit_origin_group("b1", [descriptor])["ok"]

    published = po.publish_prepaid_batch(
        spool.queue, spool.instance, spool.template, [descriptor],
        batch_id="b1", tier=fx.TIER, cas_root=spool.cas_root,
        producer_action_key=spool.owner, command_extra=["--unpaced"])
    assert published["ok"] is False
    assert published["refusal"] == "batch-committed-at-origin"
    materialized = po.ensure_batch_materialized(
        spool.queue, spool.instance, spool.template, batch_id="b1",
        cas_root=spool.cas_root, producer_action_key=spool.owner)
    assert materialized == {"ok": False, "step": "validate",
                            "refusal": "batch-committed-at-origin"}
