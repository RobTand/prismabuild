"""A dead consumer's cleanup owns the generation it sweeps (#708).

The caller-boundary fix gave ``reap`` a locked identity recheck and left the
withdrawals before it exposed.  ``withdraw_dead_consumer_movers`` scans the
terminal directories, checks the consumer is not live, reads the plan, and
only then withdraws each queued or claimed child -- all outside the
consumer's transition lock.  A resubmission of the same consumer key publishes
its own consumer row and its own lead under that lock, so a stale pass that
observed the old terminal and no live parent can then see the NEW ready lead
and cancel the new generation's work.  ``reap``'s locked recheck is too late:
the cancellation has already happened.

The rule this file pins is that the terminal re-read, the live-parent
recheck, the plan attribution, every child withdrawal and the reap happen in
one consumer-lock transaction, so a concurrent resubmission either runs wholly
before the pass or is excluded by it.  The regression reaches the race
deterministically: the pass is paused at its plan attribution -- the moment
after its no-live-parent observation -- while a real ``pbrun.main``
resubmission runs in another thread.

Nothing here touches the live queue or a real device.
"""
from __future__ import annotations

from pathlib import Path
import sys
import threading

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

from prismabuild import pool, residency_plan  # noqa: E402
import pbrun  # noqa: E402
import tier_loop  # noqa: E402

import test_pbrun_residency_stage_submission as submission  # noqa: E402

#: How long the paused pass waits for the concurrent resubmission to publish.
#: The corrected pass holds the consumer's lock here, so the resubmission
#: cannot publish at all and the wait expires; the buggy pass holds nothing,
#: and the resubmission completes in well under this bound.
RESUBMISSION_WINDOW_S = 5.0


def test_a_stale_cleanup_cannot_cancel_the_resubmitted_generation(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    """RED before the fix: the new consumer was published, its lead withdrawn."""

    # The submitter publishes the consumer alone; the loop publishes the lead
    # and its protected run-ahead, so a test that needs a queued lead runs the
    # cycle the fleet runs rather than expecting the submitter to queue one.
    submitted = submission._submit_staged(tmp_path, monkeypatch)
    queue = submitted["queue"]
    key = submission._detach_key(capsys)
    first = residency_plan.read(queue, key)
    assert first is not None
    lead = str(first["phases"][0]["mover_row"]["action_key"])

    # The generation ends the way the race finds it: a withdrawn terminal, a
    # live marker on the plan, and the old generation's queued movers already
    # concluded.
    withdrawal = queue.withdraw(key, reason="stale price", by="operator")
    assert withdrawal["residency_plan_superseded"] is True
    for mover_key in residency_plan.mover_keys(first):
        queue.item_path(pool.READY, mover_key).unlink()

    real_read_filed = residency_plan.read_filed
    published = threading.Event()
    results: dict[str, object] = {}
    started: list[threading.Thread] = []

    def resubmit() -> None:
        results["returncode"] = pbrun.main()
        # The submitter publishes the consumer alone; the loop publishes the
        # resubmitted generation's lead, the same cycle the fleet runs.
        submission._tier_cycle(queue, tmp_path / "stage")
        published.set()

    def observe_then_resubmit(q, consumer, **kwargs):
        plan, filing = real_read_filed(q, consumer, **kwargs)
        if consumer == key and plan is not None and not started:
            # The pass has just looked for a live consumer and found none.
            # A resubmission of the same key now runs for real; whether it
            # can publish before this pass reaches its child withdrawals is
            # exactly what the consumer's lock decides.
            thread = threading.Thread(target=resubmit, daemon=True)
            started.append(thread)
            thread.start()
            published.wait(RESUBMISSION_WINDOW_S)
        return plan, filing

    monkeypatch.setattr(residency_plan, "read_filed", observe_then_resubmit)

    events = tier_loop.withdraw_dead_consumer_movers(queue)

    for thread in started:
        thread.join(10)
        assert not thread.is_alive(), "the resubmission never returned"
    assert started, "the pass never reached its plan attribution"
    assert results.get("returncode") == 0, results

    final = residency_plan.read(queue, key)
    assert final is not None, "the live generation left no plan filed"
    new_lead = str(final["phases"][0]["mover_row"]["action_key"])
    assert queue.item_path(pool.READY, key).exists(), (
        "the stale cleanup withdrew the resubmitted consumer")
    assert queue.item_path(pool.READY, new_lead).exists(), (
        "the stale cleanup cancelled the resubmitted generation's lead")
    assert lead not in queue.withdrawn_keys(), (
        "the new lead was filed as withdrawn by the stale pass")
    assert not any(event.get("event") == "dead-consumer-mover-withdrawn"
                   and event.get("mover") == new_lead for event in events), events
