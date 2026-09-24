"""A retry is never superseded by its failed predecessor's cleanup (#1114).

On 2026-09-24 the GLM Stage B row 37 consumer failed with a shared range's
mover still queued (#1026 shares one mover per staged range, so every
consumer of that range names the same key).  Its retry, a new consumer key
over the same manifest, sealed a plan naming that queued mover.  The tier
loop's dead-consumer pass then withdrew the mover for the failed consumer,
the retry's window read the withdrawal as a cancellation of its own plan,
marked the plan superseded, and the retry failed in its first staged read.

The contract this file pins: a retry either seals a plan free of the dead
attempt, or refuses by name at submission.  It never seals and is then
superseded by the predecessor's cleanup.  The failed attempt's queued mover
is the same work the retry asks for, so the pass must leave it to the retry.

Two ways the pass's per-interest rule failed to see the retry, both driven
through the real submission path (``pbrun.main``) and the real pass:

* The pass runs between the retry's ``freeze`` and the publication of its
  consumer row.  The pass read interest off live queue rows only, and a
  sealed consumer whose row is not visible yet has none.
* The shared-mover index cannot be listed when the pass runs.  The pass read
  that as "no mover is shared" and withdrew every one.

The same-key half of the contract rides the reused-plan path: a failed
consumer resubmitted under its own key before the pass reaped its plan
reuses the frozen plan, and a child another dead consumer's pass withdrew in
the meantime must not supersede it.

Nothing here touches the live queue or a real device: the queue, the tier
and the CAS live under ``tmp_path``.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import stat
import sys
import threading

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

from prismabuild import pool, residency_plan  # noqa: E402
import pbrun  # noqa: E402
import tier_loop  # noqa: E402

import test_pbrun_residency_stage_submission as submission  # noqa: E402

#: How long the hooked submission waits for the pass it started.
PASS_JOIN_S = 30.0


def _fail(queue: pool.PoolQueue, consumer: str) -> None:
    """End a queued consumer the way a failed attempt does."""

    source = queue.item_path(pool.READY, consumer)
    record = json.loads(source.read_text())
    record["status"] = "failed"
    queue.item_path(pool.FAILED, consumer).write_text(json.dumps(record))
    source.unlink()


def _retry_argv(monkeypatch: pytest.MonkeyPatch) -> None:
    """The same row resubmitted from a changed tree: a new consumer key.

    Only the command changes, so the manifest, the tier and every range are
    the ones the failed attempt sealed, and the shared ranges' registrations
    hand the retry the failed attempt's movers.
    """

    argv = list(sys.argv)
    assert argv[-1] == "printf staged"
    monkeypatch.setattr(sys, "argv", [*argv[:-1], "printf retry"])


def _failed_attempt(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys,
                    ) -> tuple[pool.PoolQueue, str, str]:
    """A staged consumer whose lead was published and which then failed.

    Returns ``(queue, failed_consumer, queued_lead)``.
    """

    prepared = submission._prepare(tmp_path, monkeypatch)
    queue = prepared["queue"]
    queue.mint_tier_capacity(submission.TIER, {"stage_gib": 8})
    assert pbrun.main() == 0
    failed = submission._detach_key(capsys)
    plan = residency_plan.read(queue, failed)
    assert plan is not None
    lead = str(plan["phases"][0]["mover_row"]["action_key"])
    assert residency_plan.share_namespace_of(queue, lead) is not None, (
        "precondition: the lead is a shared range's mover (#1026)")
    submission._tier_cycle(queue, tmp_path / "stage")
    assert queue.item_path(pool.READY, lead).exists(), (
        "precondition: the window published the failed attempt's lead")
    _fail(queue, failed)
    return queue, failed, lead


def _assert_retry_survives(queue: pool.PoolQueue, tmp_path: Path, retry: str,
                           lead: str) -> None:
    """The retry's plan stands and its lead is the queued mover, unwithdrawn."""

    plan = residency_plan.read(queue, retry)
    assert plan is not None, "the retry left no plan filed"
    assert str(plan["phases"][0]["mover_row"]["action_key"]) == lead
    withdrawn = queue.live_withdrawal(lead)
    # The cycle the fleet runs next: its window reads the plan's children.
    submission._tier_cycle(queue, tmp_path / "stage")
    assert residency_plan.superseded(queue, plan) is None, (
        "the retry's plan was superseded by its predecessor's cleanup "
        f"(the lead's withdrawal: {withdrawn!r})")
    assert withdrawn is None, (
        "the failed attempt's cleanup withdrew a mover the retry's plan names")
    assert queue.item_path(pool.READY, retry).exists(), "the retry is not queued"
    assert (queue.item_path(pool.READY, lead).exists()
            or queue.item_path(pool.CLAIMED, lead).exists()), (
        "the retry's lead is no longer queued")


def test_a_pass_between_the_retrys_seal_and_its_row_does_not_supersede_it(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    """RED before #1114: the pass saw no live row naming the lead and withdrew it.

    The retry's plan is frozen and its consumer row not yet published when
    the dead-consumer pass runs, from another thread the way the tier loop
    runs from another process.  The failed attempt's queued lead is the
    retry's lead too, so the pass must leave it.
    """

    queue, failed, lead = _failed_attempt(tmp_path, monkeypatch, capsys)
    _retry_argv(monkeypatch)

    real_publish = pbrun.publish_or_refuse
    ran: list[list[dict[str, object]]] = []

    def pass_then_publish(q, publication):
        # Between ``freeze`` and the consumer's own row: the plan is filed
        # and the consumer is in no queue state yet.
        assert residency_plan.read(q, str(publication["action_key"])) is not None
        worker = threading.Thread(
            target=lambda: ran.append(tier_loop.withdraw_dead_consumer_movers(q)),
            daemon=True)
        worker.start()
        worker.join(PASS_JOIN_S)
        assert not worker.is_alive(), "the pass never finished"
        return real_publish(q, publication)

    monkeypatch.setattr(pbrun, "publish_or_refuse", pass_then_publish)
    assert pbrun.main() == 0
    retry = submission._detach_key(capsys)
    assert retry != failed, "precondition: the retry is a new consumer key"
    assert ran, "the pass never ran inside the submission"

    _assert_retry_survives(queue, tmp_path, retry, lead)


def test_an_unreadable_shared_mover_index_does_not_withdraw_the_retrys_lead(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    """RED before #1114: an index the pass could not list read as "none shared".

    The retry is published and its plan names the lead, exactly the literal
    #1114 timeline.  The shared-mover index cannot be listed when the pass
    runs; unknown is not absence, so the pass must not withdraw the lead.
    """

    queue, _failed, lead = _failed_attempt(tmp_path, monkeypatch, capsys)
    _retry_argv(monkeypatch)
    assert pbrun.main() == 0
    retry = submission._detach_key(capsys)

    index = residency_plan.shared_mover_path(queue, lead).parent
    mode = stat.S_IMODE(os.stat(index).st_mode)
    os.chmod(index, 0)
    try:
        if os.access(index, os.R_OK):
            pytest.skip("running as a user the directory mode cannot refuse")
        tier_loop.withdraw_dead_consumer_movers(queue)
    finally:
        os.chmod(index, mode)

    _assert_retry_survives(queue, tmp_path, retry, lead)


def test_the_literal_timeline_keeps_the_lead_for_the_published_retry(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    """The #1114 order with every record readable: seal, publish, then the pass."""

    queue, _failed, lead = _failed_attempt(tmp_path, monkeypatch, capsys)
    _retry_argv(monkeypatch)
    assert pbrun.main() == 0
    retry = submission._detach_key(capsys)

    events = tier_loop.withdraw_dead_consumer_movers(queue)

    assert not any(event.get("event") == "dead-consumer-mover-withdrawn"
                   and event.get("mover") == lead for event in events), events
    _assert_retry_survives(queue, tmp_path, retry, lead)


def test_a_same_key_retry_reusing_its_plan_is_not_superseded_by_a_stale_cancellation(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    """RED before #1114: the reused plan skipped the renewal the fresh seal runs.

    The failed consumer is resubmitted under its own key before any pass
    reaped its plan, so the submission reuses the frozen plan.  Meanwhile the
    lead was withdrawn by a dead-consumer pass (for another consumer that
    shared it).  The resubmission is how a person asks for the work again;
    it must retire that cancellation, or refuse by name, never seal a window
    its first cycle supersedes.
    """

    queue, failed, lead = _failed_attempt(tmp_path, monkeypatch, capsys)
    # A pass for another sharer that died too: the same marker the tier loop
    # files, with no live interest left at that moment.
    queue.withdraw(lead, reason="consumer-failed", by="tier-loop")
    assert not queue.item_path(pool.READY, lead).exists()

    try:
        code = pbrun.main()
    except SystemExit as exc:
        # The other half of the contract: a refusal at submission, by name.
        assert "still owns live work" in str(exc) or "cannot" in str(exc), exc
        assert not queue.item_path(pool.READY, failed).exists()
        return
    assert code == 0
    assert submission._detach_key(capsys) == failed
    plan = residency_plan.read(queue, failed)
    assert plan is not None
    assert queue.live_withdrawal(lead) is None, (
        "the resubmission reused a plan whose lead carries a live cancellation")
    submission._tier_cycle(queue, tmp_path / "stage")
    assert residency_plan.superseded(queue, plan) is None, (
        "the resubmitted plan was superseded by a predecessor's cancellation")
    assert queue.item_path(pool.READY, lead).exists(), (
        "the resubmitted window did not publish its lead again")
