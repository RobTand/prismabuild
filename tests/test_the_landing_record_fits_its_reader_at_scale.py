"""The landing record's size and the tier cycle's reads at scale (#1018).

#1018 lists every leg a consumer has still to read, not only the legs inside
its refill horizon.  Two costs grow with that, and this file measures both at
the largest shape the fleet runs: eight 47-phase R12 consumers on one stage
tier, as ``test_eight_claimed_consumers_drain_the_tier_in_claim_order`` builds
them, and the same eight with every phase sealed as stage chunks (#675).

* **The record's size.**  PQ's reader drops a landing record larger than its
  ``MAX_MAP_BYTES`` (256 MiB) whole (``residency_map._read_landing``).  The
  test prints each record's bytes, rows and bytes per row, and the leg count
  at which one record would reach the reader's bound.
* **What one cycle reads.**  The tier loop runs every 5 s.  The test counts
  one cycle's directory listings, ``stat`` calls and file opens, and the
  cycle's own read counters (``tier_loop.LAST_CYCLE["reads"]``, #992), on the
  first cycle and on a steady cycle that changes nothing.

Every number is printed on one ``MEASURE`` line and raised as a
:class:`Measurement` warning, which pytest's warnings summary shows for a
passing test (``pbtest`` forwards no ``-rP`` or ``-s``).  The only
assertion is the reader's: every record parses under PQ's rules and fits its
bound.  Run on main, the file measures the tree before #1018; run on the fix,
after it.

Everything runs on ``tmp_path`` queues and stage roots (#628).
"""
from __future__ import annotations

import io
import json
import os
from pathlib import Path
import sys
import time
import warnings

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from prismabuild import residency_map  # noqa: E402
import tier_loop  # noqa: E402
from test_a_consumer_stages_only_to_its_refill_horizon import (  # noqa: E402
    _cycle, _fixture_queue)
from test_a_leg_past_the_horizon_is_waited_for_not_clocked import (  # noqa: E402
    READER_MAX_BYTES, reader_landing)
from test_claimed_consumers_drain_a_tier_in_admission_order import (  # noqa: E402
    EIGHT_HOLD, READING, _blocked, _chunk_of, _chunked_blocked,
    _chunked_cycle, _key, _manifest)
from test_r12_and_the_capture_replay_under_the_refill_horizon import (  # noqa: E402
    DATA, SAMPLE_UNIX)

COUNT = 8


class Measurement(UserWarning):
    """One shape's numbers, shown in the warnings summary of a passing run."""


def _report(payload: dict[str, object]) -> None:
    line = "MEASURE " + json.dumps(payload, sort_keys=True)
    print(line)
    warnings.warn(Measurement(line), stacklevel=2)


def _counted(monkeypatch: pytest.MonkeyPatch, run) -> dict[str, dict[str, int]]:
    """``run()``'s directory listings, ``stat`` calls and file opens.

    ``cycle`` is the whole cycle; ``landing`` is the share taken inside
    ``publish_landing_expectations``, the writer #1018 changes.
    """

    names = ("listdir", "scandir", "stat", "lstat", "open", "os_open")
    counts = dict.fromkeys(names, 0)
    landing = dict.fromkeys(names, 0)
    with monkeypatch.context() as patch:
        for name, module, attribute in (
                ("listdir", os, "listdir"), ("scandir", os, "scandir"),
                ("stat", os, "stat"), ("lstat", os, "lstat"),
                ("open", io, "open"), ("os_open", os, "open")):
            original = getattr(module, attribute)

            def wrapped(*args, _original=original, _name=name, **kwargs):
                counts[_name] += 1
                return _original(*args, **kwargs)

            patch.setattr(module, attribute, wrapped)
        # The builtin ``open`` is the same object as ``io.open`` but a
        # separate name: point it at the counting one too.
        patch.setattr("builtins.open", io.open)
        writer = tier_loop.publish_landing_expectations

        def measured_writer(*args, **kwargs):
            before = dict(counts)
            try:
                return writer(*args, **kwargs)
            finally:
                for name in names:
                    landing[name] += counts[name] - before[name]

        patch.setattr(tier_loop, "publish_landing_expectations", measured_writer)
        run()
    return {"cycle": counts, "landing": landing}


def _records(queue, keys: list[str]) -> list[dict[str, object]]:
    """Each consumer's record: its bytes, rows, rows by state, bytes per row."""

    measured = []
    for key in keys:
        path = residency_map.landing_path(queue.residency_fragment_root(), key)
        raw = path.read_bytes()
        payload = json.loads(raw)
        rows = list(payload["ranges"])
        header = len(json.dumps({**payload, "ranges": []}, sort_keys=True,
                                separators=(",", ":")).encode())
        states: dict[str, int] = {}
        for row in rows:
            label = str(row["state"]) + (
                f"/{row['deferred_by']}" if row.get("deferred_by") else "")
            states[label] = states.get(label, 0) + 1
        per_row = max((len(json.dumps(row, sort_keys=True,
                                      separators=(",", ":")).encode()) + 1
                       for row in rows), default=0)
        measured.append({"consumer": key[:12], "bytes": len(raw),
                         "rows": len(rows), "states": states,
                         "max_row_bytes": per_row, "header_bytes": header})
    return measured


def _summary(records: list[dict[str, object]]) -> dict[str, object]:
    widest = max(int(record["max_row_bytes"]) for record in records)
    header = max(int(record["header_bytes"]) for record in records)
    return {
        "records": records,
        "largest_bytes": max(int(record["bytes"]) for record in records),
        "total_bytes": sum(int(record["bytes"]) for record in records),
        "reader_max_bytes": READER_MAX_BYTES,
        # Compact rows at the widest row measured: where one record would
        # reach the reader's bound.  The writer indents, so the real count
        # is lower by the indent's share; the printed bytes are the real
        # file sizes.
        "legs_at_reader_bound_compact": (
            (READER_MAX_BYTES - header) // widest if widest else None)}


def test_eight_r12_records_fit_their_reader_and_the_cycle_is_counted(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    capacity = int(DATA["tier"]["capacity_gib"])
    queue, stage = _fixture_queue(tmp_path, capacity)
    shift = time.time() - SAMPLE_UNIX
    for n in range(COUNT):
        _blocked(queue, stage, n, shift=shift, holding=EIGHT_HOLD)
    keys = [_key(n) for n in range(COUNT)]

    cycles = []
    for label in ("first", "second", "steady"):
        counts = _counted(monkeypatch,
                          lambda: _cycle(queue, stage, gib=capacity))
        cycles.append({"cycle": label, "calls": counts,
                       "reads": dict(tier_loop.LAST_CYCLE.get("reads", {})),
                       "cycle_seconds": tier_loop.LAST_CYCLE.get("cycle_seconds"),
                       "phases": tier_loop.LAST_CYCLE.get("phases")})

    records = _records(queue, keys)
    for n, key in enumerate(keys):
        assert reader_landing(queue, key, _manifest(n)) is not None, key
    summary = _summary(records)
    assert summary["largest_bytes"] <= READER_MAX_BYTES
    _report({"shape": "eight-r12", "cycles": cycles, **summary})


def test_eight_chunked_r12_records_fit_their_reader(tmp_path: Path) -> None:
    capacity = int(DATA["tier"]["capacity_gib"])
    queue, stage = _fixture_queue(tmp_path, capacity)
    shift = time.time() - SAMPLE_UNIX
    plans = [_chunked_blocked(queue, stage, n, shift=shift,
                              holding=((READING, 0),))
             for n in range(COUNT)]
    chunk_gib = int(_chunk_of(plans[0], READING, 1)["stage_gib"])  # type: ignore[arg-type]
    for _ in range(2):
        _chunked_cycle(queue, stage, gib=capacity, chunk_gib=chunk_gib)
    keys = [_key(n) for n in range(COUNT)]
    records = _records(queue, keys)
    for n, key in enumerate(keys):
        assert reader_landing(queue, key, _manifest(n)) is not None, key
    summary = _summary(records)
    assert summary["largest_bytes"] <= READER_MAX_BYTES
    _report({"shape": "eight-r12-chunked", **summary})
