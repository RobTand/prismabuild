"""A done consumer's plan is read only when a marker can cover it (#992).

On 2026-09-26 the live queue held 452 filed residency plans (141 MB), 450 of
them terminal in ``done/`` and none superseded.  Every 5 s cycle
``withdraw_dead_consumer_movers`` read and re-validated every plan body and
re-computed its canonical SHA-256 in :func:`residency_plan.superseded` to
learn that no marker covered it: 2.48 s of ``superseded`` plus 1.46 s of
``read_filed`` in the profile.

The precheck here builds one names-only snapshot of the supersession marker
directory per sweep.  A ``done/`` consumer no marker names is refused before
its plan is read at all, which can defer a marker written after the snapshot
by one cycle.  When a marker exists -- or the listing cannot be read, or an
entry cannot be classified -- the sweep runs the existing locked checks
unchanged: the plan is read, the marker's identity and filing are compared,
and a plan for another filing is never treated as superseded.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"
                       / "fleet"))
import test_a_failed_consumer_stops_staging as base  # noqa: E402
from test_a_failed_consumer_stops_staging import queue  # noqa: E402


def _finish_consumer(queue, consumer: str) -> None:
    """File the consumer's terminal the way a done ending does."""

    source = queue.item_path(base.pool.READY, consumer)
    record = json.loads(source.read_text())
    record["status"] = "executed"
    queue.item_path(base.pool.DONE, consumer).write_text(json.dumps(record))
    source.unlink()


def _plan_reads(monkeypatch) -> list[str]:
    reads: list[str] = []
    real = base.residency_plan.read_filed

    def counted(queue_, key, **kwargs):
        reads.append(key)
        return real(queue_, key, **kwargs)

    monkeypatch.setattr(base.residency_plan, "read_filed", counted)
    return reads


def _done_consumer(queue, planner=base._plan, label: str = "done") -> None:
    plan = planner(queue, base.FIRST, label=label)
    base._publish_consumer(queue, base.FIRST, plan)
    _finish_consumer(queue, base.FIRST)


def _reaped(events) -> bool:
    return any(event.get("event") == "residency-plan-reaped"
               for event in events)


def test_a_done_consumer_without_a_marker_reads_no_plan(queue, monkeypatch):
    _done_consumer(queue)
    reads = _plan_reads(monkeypatch)

    events = base.tier_loop.withdraw_dead_consumer_movers(queue)

    assert events == []
    assert reads == [], "no plan body is read to learn a marker is absent"


def test_a_marker_written_after_the_precheck_is_seen_on_the_next_pass(
        queue, monkeypatch):
    _done_consumer(queue)
    reads = _plan_reads(monkeypatch)
    assert base.tier_loop.withdraw_dead_consumer_movers(queue) == []
    assert reads == [], "the first pass reads no plan"

    marker = base.residency_plan.mark_superseded(queue, base.FIRST,
                                                 reason="test")
    assert marker is not None
    events = base.tier_loop.withdraw_dead_consumer_movers(queue)

    assert _reaped(events), events
    assert not queue.residency_plan_path(base.FIRST).exists()
    # The sweep reads the plan, and reap re-reads it under the lock before
    # it archives the exact filing it decided against.
    assert reads and set(reads) == {base.FIRST}, (
        "the marker's plan is read and reaped")


def test_an_unreadable_marker_listing_keeps_the_locked_checks(
        queue, monkeypatch):
    _done_consumer(queue)
    reads = _plan_reads(monkeypatch)
    superseded = (queue.residency_plan_path(base.FIRST).parent
                  / base.residency_plan.SUPERSEDED)
    real_listdir = base.tier_loop.os.listdir

    def unreadable(path="."):
        if Path(path) == superseded:
            raise OSError("the marker directory cannot be listed")
        return real_listdir(path)

    monkeypatch.setattr(base.tier_loop.os, "listdir", unreadable)
    events = base.tier_loop.withdraw_dead_consumer_movers(queue)

    assert events == []
    assert reads == [base.FIRST], (
        "unknown is never read as no marker: the full check runs")


def test_a_marker_for_another_filing_does_not_supersede(queue, monkeypatch):
    _done_consumer(queue)
    reads = _plan_reads(monkeypatch)
    superseded = (queue.residency_plan_path(base.FIRST).parent
                  / base.residency_plan.SUPERSEDED)
    superseded.mkdir(parents=True, exist_ok=True)
    # A well-formed address for this key, but not this plan's body: the
    # digest belongs to nothing filed.
    (superseded / f"{base.FIRST}.{'f' * 64}.superseded.json").write_text(
        json.dumps({"schema": base.residency_plan
                    .RESIDENCY_PLAN_SUPERSEDED_SCHEMA_V1,
                    "consumer_action_key": base.FIRST,
                    "plan_sha256": "f" * 64,
                    "plan_incarnation": [1, 2, 3]}))

    events = base.tier_loop.withdraw_dead_consumer_movers(queue)

    assert events == []
    assert reads == [base.FIRST], (
        "the marker address exists, so the locked identity check runs")
    assert queue.residency_plan_path(base.FIRST).exists()


def test_a_resubmitted_filing_is_not_covered_by_its_predecessors_marker(
        queue, monkeypatch):
    """The old filing's retired marker is evidence, not authority (#708)."""

    _done_consumer(queue)
    assert base.residency_plan.mark_superseded(queue, base.FIRST,
                                               reason="old") is not None
    first = base.tier_loop.withdraw_dead_consumer_movers(queue)
    assert _reaped(first), first
    assert not queue.residency_plan_path(base.FIRST).exists()

    # A new filing under the same key: the archived window's marker was
    # retired beside its body, so the fresh window starts clean.
    plan = base._plan(queue, base.FIRST, label="resubmitted")
    base._publish_consumer(queue, base.FIRST, plan)
    _finish_consumer(queue, base.FIRST)
    reads = _plan_reads(monkeypatch)

    events = base.tier_loop.withdraw_dead_consumer_movers(queue)

    assert not _reaped(events), events
    assert reads == [], "no active marker names the new filing"
    assert queue.residency_plan_path(base.FIRST).exists()
    assert base.residency_plan.superseded(queue, plan) is None


def test_a_live_resubmission_defers_before_the_precheck(queue, monkeypatch):
    _done_consumer(queue)
    reads = _plan_reads(monkeypatch)
    # A resubmission publishes a live row under the same key.
    queue.item_path(base.pool.READY, base.FIRST).write_text(
        json.dumps({"action_key": base.FIRST}))

    events = base.tier_loop.withdraw_dead_consumer_movers(queue)

    assert events == []
    assert reads == [], "a live consumer defers before any plan read"
    assert queue.item_path(base.pool.READY, base.FIRST).exists()


def test_an_unrecognized_marker_entry_makes_the_hint_unknown(queue):
    _done_consumer(queue)
    superseded = (queue.residency_plan_path(base.FIRST).parent
                  / base.residency_plan.SUPERSEDED)
    superseded.mkdir(parents=True, exist_ok=True)

    assert base.tier_loop._supersession_marker_keys(queue) == frozenset()
    (superseded / "not-a-marker.json").write_text("{}")
    assert base.tier_loop._supersession_marker_keys(queue) is None


def test_the_hint_parses_only_the_marker_address(queue):
    superseded = (queue.residency_plan_path(base.FIRST).parent
                  / base.residency_plan.SUPERSEDED)
    superseded.mkdir(parents=True, exist_ok=True)
    key, digest = base.FIRST, "a" * 64
    (superseded / f"{key}.{digest}.superseded.json").write_text("{}")
    # A retired marker and a reaped body in the same directory are archive
    # names, not active markers for their key.
    (superseded / f"{key}.{digest}.1790059841670832536.1790064475.880532"
                  ".marker.json").write_text("{}")
    (superseded / f"{key}.1790064475.417246.consumer-withdrawn.json"
                  ).write_text("{}")
    (superseded / f".{key}.deadbeef.tmp").write_text("{}")

    assert base.tier_loop._supersession_marker_keys(queue) == frozenset({key})
