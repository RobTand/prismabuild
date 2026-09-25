"""A RAM promotion settles a divergent name by its owners' states (#1004 item 1).

#966 taught the stage mover to arbitrate a staged name whose recorded owner
holds different bytes: an owner that has provably ended is replaced, a live
owner is the terminal ``staged_destination_conflict``, and only an unproven
ending stays retryable.  ``ram_promote`` built its ``_StagedPublisher`` with no
consumer, so ``_arbitrates`` was false and a divergent ram name kept the old
retryable refusal: a promotion whose destination held a dead owner's
different bytes exited incomplete and was republished into the same refusal
every window cycle, exactly as the stage mover did before #1003.

The promoting consumer is now the owner the promotion arbitrates by.  The
fixtures drive the real ``stage_move.move`` and ``ram_promote.promote`` /
``ram_promote.main`` over tiny real files, with another consumer's promotion
records dating the very inode that is there under another digest.
"""
from __future__ import annotations

import os
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

import test_a_restarted_promotion_adopts_its_own_copies as ram  # noqa: E402
import test_dead_owner_fragment_blocks_then_retires as base  # noqa: E402
from prismabuild import pool, reader_lease, residency_map  # noqa: E402
import ram_promote  # noqa: E402
import stage_move  # noqa: E402


def _foreign_owner(tmp_path: Path, state: str):
    """Promote once, forget it, and let another consumer's promotion own it.

    ``state`` is the other consumer's: ``failed`` (ended, with its promotion
    ``executed``) or ``claimed`` (live).  Its records date the current inode
    of the first ram name under a digest that is not the manifest's.
    """

    queue, args, _body, paths, epoch, manifest_sha = ram._promoted(tmp_path)
    residence = queue.root / pool.RESIDENCY
    fragment = residency_map.read_fragments(residence, ram.CONSUMER)
    [fragment] = [row for row in fragment
                  if row.get("mover_action_key") == ram.RAM_MOVER]
    material = reader_lease.read_material(residence, ram.CONSUMER,
                                          ram.RAM_MOVER)
    assert isinstance(material, dict), material
    ram._forget(queue)
    consumer, mover = base._key(), base._key()
    if state == "failed":
        base._publish(queue, consumer, max_attempts=1)
        queue.finish(consumer, status="failed", detail={"returncode": 1})
        base._publish(queue, mover, max_attempts=1)
        queue.finish(mover, status="executed", detail={"returncode": 0})
    else:
        assert state == "claimed", state
        base._publish(queue, consumer, max_attempts=1)
    norm = os.path.normpath(str(paths[0]))
    entries = {key: dict(entry) for key, entry in material["entries"].items()}
    for entry in entries.values():
        if os.path.normpath(str(entry["stage_path"])) == norm:
            entry["sha256"] = "0" * 64
    residency_map.write_fragment(residence, dict(
        fragment, consumer_action_key=consumer, mover_action_key=mover))
    reader_lease.write_material(
        residence, consumer_action_key=consumer, mover_action_key=mover,
        tier_id=ram.RAM_TIER, stage_root=str(tmp_path / "ram"),
        manifest_sha256=manifest_sha,
        generation=reader_lease.mint_generation(), entries=entries,
        epoch=epoch)
    return queue, args, paths, consumer, mover


def test_an_ended_owners_ram_name_is_replaced_and_the_promotion_completes(
        tmp_path: Path, monkeypatch) -> None:
    """RED on the base source: a retryable refusal that never ends."""

    queue, args, paths, consumer, mover = _foreign_owner(tmp_path, "failed")
    before = ram._identities(paths)
    monkeypatch.setattr(stage_move, "_PUBLISH_GRACE_S", ram.GRACE)

    receipt = ram_promote.promote(args)

    assert receipt["complete"] is True, (
        f"#1004 item 1: the promotion refused a name whose every owner has "
        f"ended: refusal={receipt.get('refusal')!r} "
        f"errors={receipt.get('errors')}")
    assert "refusal" not in receipt, receipt.get("refusal")
    for index, path in enumerate(paths):
        assert path.read_bytes() == ram._payload(index)
    after = ram._identities(paths)
    assert after[0] != before[0], "the ended owner's name was not replaced"
    assert after[1:] == before[1:], "an undisputed name was replaced"
    assert receipt["entries_invalidated"] == 1
    [row] = receipt["invalidated"]
    assert row["stage_path"] == os.path.normpath(str(paths[0]))
    assert row["owners"] == [{"consumer_action_key": consumer,
                              "mover_action_key": mover, "state": "ended"}]
    outcomes = receipt["phase_timings"]["outcomes"]
    assert outcomes.get("replaced_ended_owner") == 1, outcomes


def test_a_live_owners_ram_name_is_a_terminal_conflict(
        tmp_path: Path, monkeypatch) -> None:
    """RED on the base source: rc 0, no refusal, and the window loops."""

    queue, args, paths, consumer, mover = _foreign_owner(tmp_path, "claimed")
    before = ram._identities(paths)
    monkeypatch.setattr(stage_move, "_PUBLISH_GRACE_S", ram.GRACE)
    argv = ["--pool-root", str(queue.root), "--cas-root", str(args.cas_root),
            "--action-key", ram.RAM_MOVER,
            "--consumer-action-key", ram.CONSUMER,
            "--tier-id", ram.RAM_TIER, "--ram-root", str(args.ram_root),
            "--source-stage-root", str(args.source_stage_root),
            "--manifest-sha256", str(args.manifest_sha256),
            "--range-start-bytes", "0",
            "--range-end-bytes", str(ram.TOTAL),
            "--manifest", str(args.manifest),
            "--residency-root", str(args.residency_root),
            "--block", "4096", "--readers", "1", "--max-readers", "1"]

    rc = ram_promote.main(argv)
    receipt = queue.move_record(ram.RAM_MOVER)

    assert isinstance(receipt, dict)
    assert rc == 1, (rc, receipt.get("refusal"), receipt.get("errors"))
    assert receipt["refusal"] == stage_move.STAGED_DESTINATION_CONFLICT
    assert receipt["complete"] is False
    conflict = receipt["conflict"]
    assert conflict["stage_path"] == os.path.normpath(str(paths[0]))
    assert conflict["consumer_action_key"] == ram.CONSUMER
    assert conflict["mover_action_key"] == ram.RAM_MOVER
    assert conflict["owners"] == [{"consumer_action_key": consumer,
                                   "mover_action_key": mover,
                                   "state": "live"}]
    # No plan names this promotion here, so nothing is retired; the stage
    # mover's tests cover the retirement of a filed window.
    assert receipt["plan_superseded"] is False
    assert ram._identities(paths)[0] == before[0], "a live copy was replaced"
    assert paths[0].read_bytes() == ram._payload(0)
    assert not receipt.get("invalidated")


@pytest.mark.parametrize("which", ["mover-queued", "no-outcome"])
def test_an_unproven_ending_keeps_the_retryable_refusal(
        tmp_path: Path, monkeypatch, which: str) -> None:
    """Uncertainty never replaces, and never reads as the terminal conflict."""

    queue, args, paths, consumer, mover = _foreign_owner(tmp_path, "failed")
    if which == "mover-queued":
        queue.publish(action_key=mover, cas_root="/cas", checkout_root="/co",
                      worker_script="/w.py", resources={"cpu": 1},
                      max_attempts=1, recompute=True)
    else:
        # The consumer's outcome record is gone: #798's legacy shape.
        queue.item_path(pool.FAILED, consumer).unlink()
    before = ram._identities(paths)
    monkeypatch.setattr(stage_move, "_PUBLISH_GRACE_S", ram.GRACE)

    receipt = ram_promote.promote(args)

    assert receipt["complete"] is False, receipt
    assert receipt.get("refusal") in (None, "residency_moved_nothing"), (
        receipt.get("refusal"), receipt.get("conflict"))
    assert "conflict" not in receipt
    assert not receipt.get("invalidated")
    assert ram._identities(paths)[0] == before[0]
    assert any("different bytes" in str(error)
               for error in receipt["errors"]), receipt["errors"]
