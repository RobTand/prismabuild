"""Commitment reporting: the three gaps #930 left (#960).

1. ``file_tier_commitment`` ran ``mkdir(parents=True)`` on every call -- and
   so did the atomic writer under it -- the per-poll directory walk #595
   removed from ``ensure_layout``.  ``announce_tier`` had the same pattern.
   The directory is now made only when a write finds it missing.
2. A retired stage tier kept its last commitment record, holders, waits and
   claim order included, aging only through ``commitment_age_s``.  The
   retirement now marks it retired in the same cycle.
3. The ``tier-cycle`` summary line carried no commitment totals; only the
   events and the record did.  Each stage tier's entry now carries
   ``committed_gib``, ``waiting`` and ``waiting_need_gib``.

Everything runs on ``tmp_path`` queues and stage roots (#628).
"""
from __future__ import annotations

from pathlib import Path
import sys
import time

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from prismabuild import pool  # noqa: E402
import tier_loop  # noqa: E402
from test_a_resident_range_is_adopted_rather_than_recopied import (  # noqa: E402
    TIER, _hexkey, _tier_record)
from test_a_consumer_stages_only_to_its_refill_horizon import (  # noqa: E402
    _fixture_queue)
from test_admission_charges_refill_horizons_jointly import (  # noqa: E402
    READER_FOOTPRINT, Consumer, _reader, _refused, _tiers)

HOST = "dl380g10"
STATIC = _hexkey("static")


def _count_mkdirs(monkeypatch: pytest.MonkeyPatch) -> list[Path]:
    made: list[Path] = []
    real = Path.mkdir

    def mkdir(self, *args, **kwargs):
        made.append(self)
        return real(self, *args, **kwargs)

    monkeypatch.setattr(Path, "mkdir", mkdir)
    return made


def _cycle(queue: pool.PoolQueue, discovered: dict) -> list[dict[str, object]]:
    return tier_loop.cycle(queue, host=HOST, source_pool="storage_pool",
                           receipts=tier_loop.ReceiptCache(),
                           discover=lambda **_kwargs: discovered)


# ----------------------------------------------------- 1. the directory, once


def test_the_commitment_directory_is_made_once_not_per_call(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    made = _count_mkdirs(monkeypatch)

    queue.file_tier_commitment({"tier_id": TIER, "waiting": []})
    assert made == [queue.tier_commitment_path(TIER).parent]

    made.clear()
    for _ in range(3):
        queue.file_tier_commitment({"tier_id": TIER, "waiting": []})
    assert made == []
    assert queue.tier_commitment(TIER)["waiting"] == []

    # A directory an operator deletes comes back on the next write.
    queue.tier_commitment_path(TIER).unlink()
    queue.tier_commitment_path(TIER).parent.rmdir()
    queue.file_tier_commitment({"tier_id": TIER, "waiting": []})
    assert queue.tier_commitment(TIER) is not None


def test_announcing_a_tier_makes_no_directory_per_call(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    made = _count_mkdirs(monkeypatch)

    for _ in range(3):
        queue.announce_tier({"tier_id": TIER, "tier": "stage"})

    assert made == []
    assert [record["tier_id"] for record in queue.tiers()] == [TIER]


# ---------------------------------------------------- 2. a retired tier's record


def test_a_retired_tier_marks_its_commitment_record_in_the_same_cycle(
        tmp_path: Path) -> None:
    queue, stage = _fixture_queue(tmp_path, 20)
    assert queue.tier_ledger(TIER).acquire(STATIC, {"stage_gib": 10})
    _reader(queue, stage, landed=(0,))
    _cycle(queue, {TIER: _tier_record(stage, gib=20)})
    live = queue.tier_commitment(TIER)
    assert live["over_committed_gib"] > 0 and live.get("retired") is None

    _cycle(queue, {})

    retired = queue.tier_commitment(TIER)
    assert retired["retired"] is True
    assert retired["waiting"] == []
    # Nothing a staged wait could read as a live (over-)commitment.
    for field in ("over_committed_gib", "committed_gib", "claim_order",
                  "holders", "terms"):
        assert field not in retired, field
    assert queue._tier_commitment_standing(
        TIER, _hexkey("anyone"), now=time.time()) == (None, None)
    assert tier_loop.LAST_CYCLE["commitments"][TIER]["retired"] is True

    # Already retired: a later cycle reads it and does not rewrite it.
    filed = retired["filed_unix"]
    _cycle(queue, {})
    assert queue.tier_commitment(TIER)["filed_unix"] == filed


# ---------------------------------------------------- 3. totals on the cycle line


def test_the_tier_cycle_line_carries_each_tiers_commitment_totals(
        tmp_path: Path) -> None:
    queue, stage = _fixture_queue(tmp_path, 30)
    _reader(queue, stage)

    records = _cycle(queue, {TIER: _tier_record(stage, gib=30)})
    line = tier_loop.tier_cycle_line(HOST, records, tier_loop.LAST_CYCLE)

    [entry] = [tier for tier in line["tiers"] if tier["tier_id"] == TIER]
    assert entry["committed_gib"] == READER_FOOTPRINT
    assert entry["waiting"] == 0 and entry["waiting_need_gib"] == 0
    assert "commitments" not in line
    assert line["event"] == "tier-cycle" and "cycle_seconds" in line


def test_a_waiting_newcomer_is_counted_on_the_line(tmp_path: Path) -> None:
    queue, stage = _fixture_queue(tmp_path, 30)
    assert queue.tier_ledger(TIER).acquire(STATIC, {"stage_gib": 10})
    _reader(queue, stage)
    newcomer = Consumer(queue, stage, "n")
    tier_loop._CYCLE_COMMITMENTS.clear()

    events = tier_loop.residency_window(queue, tiers=_tiers(stage))
    _refused(events, newcomer.key)
    record = queue.tier_commitment(TIER)
    line = tier_loop.tier_cycle_line(
        HOST, [{"tier_id": TIER, "tier": "stage"}],
        {"commitments": dict(tier_loop._CYCLE_COMMITMENTS)})

    [entry] = line["tiers"]
    assert entry["committed_gib"] == record["committed_gib"] == 10 + READER_FOOTPRINT
    assert entry["waiting"] == len(record["waiting"]) == 1
    assert entry["waiting_need_gib"] == record["waiting"][0]["need_gib"]
    assert entry["waiting_need_gib"] > 0


def test_a_tier_without_a_commitment_record_carries_no_totals() -> None:
    line = tier_loop.tier_cycle_line(
        HOST, [{"tier_id": "prismabuild-ram:dl380g10", "tier": "ram"}],
        {"commitments": {}})

    [entry] = line["tiers"]
    assert "committed_gib" not in entry and "waiting" not in entry
