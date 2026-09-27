"""A transition keeps the decision that produced it (#1239, request 1).

The reason ring (#991) records *what* was refused and *when*; the #1239
incident showed why that is not enough to diagnose a starved row after the
fact.  A canary waited through 1072 claim passes while the boundary it
needed went to an ordinary row, and when the ring was opened afterwards it
held only the verdict word ``host_pressure`` -- not the controller's
decision snapshot: the foreign/held CPU split, the eligible set, the
pressure that was measured.  That snapshot was already in the latest-only
record, which overwrites itself every pass, so by the time anyone looked,
the evidence of *why* the first refusal happened was gone.

These tests pin the fix: every *new* transition entry stores the bounded
decision snapshot that produced it, damped repeats keep the first snapshot
(the transition that matters is where the starvation began, the same
philosophy as #1006's damping), and the snapshot obeys the ring's own
evidence bounds so a key's ring stays the size of the records it already
keeps.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
from prismabuild import pool  # noqa: E402
import test_a_kill_names_what_it_waited_on as fx  # noqa: E402


HOST_PRESSURE_DECISION = {
    "reason": "host_pressure",
    "cpus": [3],
    "held_cpus": [],
    "foreign_cpus": [3, 4, 5],
    "foreign_busy_cpus": 8.6,
    "eligible_cpus": [],
    "sampled_unix": 2000000.0,
}


def _refuse_once(queue, item, decision, clock):
    queue.record_denial(item, "adaptive_cpu_refused",
                        {"decision": dict(decision)})
    clock[0] += 1.0


def test_a_new_transition_carries_the_decision_snapshot(tmp_path, monkeypatch):
    """The first refusal of a starved row keeps its evidence."""

    queue = pool.PoolQueue(tmp_path / "queue")
    item = fx._publish_export(queue, fx._hexkey("owner"), "starved")
    clock = [1000.0]
    monkeypatch.setattr(pool, "_now", lambda: clock[0])

    _refuse_once(queue, item, HOST_PRESSURE_DECISION, clock)

    history = queue.denial_transitions(str(item["action_key"]))
    assert len(history) == 1
    entry = history[0]
    # RED pre-fix: the entry carries no decision at all, so the incident's
    # question -- which CPUs were foreign, what did the controller measure --
    # cannot be answered from the ring.
    assert entry["decision"]["reason"] == "host_pressure"
    assert entry["decision"]["foreign_cpus"] == [3, 4, 5]
    assert entry["decision"]["cpus"] == [3]
    assert entry["decision"]["foreign_busy_cpus"] == 8.6


def test_a_damped_repeat_keeps_the_first_snapshot(tmp_path, monkeypatch):
    """Damping does not overwrite where the starvation began."""

    queue = pool.PoolQueue(tmp_path / "queue")
    item = fx._publish_export(queue, fx._hexkey("owner"), "damped")
    key = str(item["action_key"])
    clock = [1000.0]
    monkeypatch.setattr(pool, "_now", lambda: clock[0])

    _refuse_once(queue, item, HOST_PRESSURE_DECISION, clock)
    # A same-verdict second pass in ONE process answers from the in-process
    # memo with no I/O at all (the #991 design: a starved row's reason is
    # the same for hours).  A damped repeat -- count on an existing entry --
    # arises across loops or after a restart, so retire the memo the way
    # _retire_denial_memo would before the second pass sees the file.
    pool._DENIAL_SEEN.clear()
    # A later pass sees a different foreign set but the same verdict word.
    later = dict(HOST_PRESSURE_DECISION, foreign_cpus=[17], cpus=[17])
    _refuse_once(queue, item, later, clock)

    history = queue.denial_transitions(key)
    assert len(history) == 1, history
    repeat = history[0]
    assert repeat["count"] == 2
    assert repeat["last_unix"] == clock[0] - 1.0
    # The snapshot is the first one: the entry is the record of how the
    # starvation began, not a rolling latest-only view (that record exists
    # separately and overwrites every pass).
    assert repeat["decision"]["foreign_cpus"] == [3, 4, 5]


def test_the_snapshot_uses_the_ring_evidence_bounds(tmp_path, monkeypatch):
    """A decision is bounded exactly like the latest-only evidence is.

    The ring's own records on a live box run ~3.9 KiB each (256 entries,
    p50 3881 bytes); a snapshot passed through the same
    ``_bounded_denial_value`` cannot exceed that magnitude, so a key's ring
    of at most 16 transitions stays within the cost the ring already had.
    """

    queue = pool.PoolQueue(tmp_path / "queue")
    item = fx._publish_export(queue, fx._hexkey("owner"), "bounded")
    clock = [1000.0]
    monkeypatch.setattr(pool, "_now", lambda: clock[0])

    noisy = dict(HOST_PRESSURE_DECISION, note="x" * 100_000,
                 many={str(i): i for i in range(500)})
    queue.record_denial(item, "adaptive_cpu_refused", {"decision": noisy})

    entry = queue.denial_transitions(str(item["action_key"]))[0]
    decision = entry["decision"]
    assert len(decision["note"]) == pool.MAX_DENIAL_VALUE_TEXT
    assert len(decision["many"]) == pool.MAX_DENIAL_VALUE_ITEMS
    serialized = len(json.dumps(entry))
    assert serialized <= 4096, serialized


def test_a_reason_without_a_decision_records_none(tmp_path, monkeypatch):
    """No decision present is recorded as an explicit ``None``.

    A reader of the ring can then tell a refusal that carried no decision
    snapshot apart from a ring written before the field existed only by
    schema, not by ambiguity inside one schema.
    """

    queue = pool.PoolQueue(tmp_path / "queue")
    item = fx._publish_export(queue, fx._hexkey("owner"), "no-decision")
    clock = [1000.0]
    monkeypatch.setattr(pool, "_now", lambda: clock[0])

    queue.record_denial(item, "residency_plan_unreadable", {"why": "test"})

    entry = queue.denial_transitions(str(item["action_key"]))[0]
    assert entry["decision"] is None
