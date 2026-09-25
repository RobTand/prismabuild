"""A host-level tier event has a durable home (#1006).

#1002 (#990/#991) gave every event that names a consumer a per-consumer
file, ``residency-events/<consumer>/<host>.jsonl``, read by
``queue.consumer_events`` and by a kill's ending record.  An event that
names neither a consumer nor a tier with any planned consumer on it -- the
stage's ARC ``primarycache`` refusal among them -- still had nowhere to go
but that host's own stdout.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
from prismabuild import pool, storage_tiers  # noqa: E402
import tier_loop  # noqa: E402

HOST = "dl380g10"
TIER = "prismabuild-stage:dl380g10"


def _metadata_only_cycle(tmp_path: Path) -> pool.PoolQueue:
    """One tier cycle over a stage dataset that forbids the ARC to cache it."""

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()

    def discover(**_kwargs):
        return {TIER: {
            "schema": storage_tiers.TIER_RECORD_SCHEMA_V1,
            "tier_id": TIER, "host": HOST, "tier": "stage",
            "mountpoint": str(tmp_path / "stage"),
            "capacity_bytes": 4 * storage_tiers.GIB,
            "primarycache": "metadata",
        }}

    tier_loop.cycle(queue, host=HOST, source_pool="storage_pool",
                    receipts=tier_loop.ReceiptCache(), discover=discover)
    return queue


def test_the_arc_refusal_lands_in_the_host_sink(tmp_path):
    queue = _metadata_only_cycle(tmp_path)

    events = queue.host_events(HOST)

    refusals = [e for e in events if e.get("event") == "stage-primarycache-refused"]
    assert refusals, events
    assert refusals[0]["tier_id"] == TIER
    assert refusals[0]["primarycache"] == "metadata"
    assert refusals[0]["host"] == HOST


def test_the_arc_refusal_still_prints_to_stdout(tmp_path, capsys):
    """Filing it durably does not take away the operator's live tail."""

    _metadata_only_cycle(tmp_path)

    lines = [json.loads(line) for line in capsys.readouterr().out.splitlines()
             if line.startswith("{")]

    assert any(line.get("event") == "stage-primarycache-refused" for line in lines)


def test_pbstatus_starvation_shows_the_newest_host_event(tmp_path):
    """``pbstatus --starvation`` reads the host sink pool.py just gained."""

    import pbstatus

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    tier_loop._emit(queue, HOST, {"event": "ram-admission-refused", "tier_id": TIER})
    tier_loop._emit(queue, HOST, {"event": "ram-epoch-changed", "tier_id": TIER})

    blob = pbstatus.read_starvation(tmp_path / "pb-queue")

    [entry] = [row for row in blob["host_events"] if row["host"] == HOST]
    assert entry["newest"]["event"] == "ram-epoch-changed"
    assert entry["events_total"] == 2


def test_a_tier_verdict_for_a_tier_planning_no_consumer_falls_back_to_the_host_sink(
        tmp_path):
    """A tier-level verdict whose tier plans nobody this cycle (#1006)."""

    queue = pool.PoolQueue(tmp_path / "queue")

    tier_loop._emit(queue, HOST,
                    {"event": "beyond-horizon-eviction-futile", "tier_id": "t",
                     "needed_gib": 9, "free_gib": 1, "beyond_horizon_gib": 2},
                    tier_consumers={"t": []})

    events = queue.host_events(HOST)
    assert events and events[0]["event"] == "beyond-horizon-eviction-futile"


def test_the_host_sink_is_bounded_and_keeps_the_newest(tmp_path, monkeypatch):
    queue = pool.PoolQueue(tmp_path / "queue")
    monkeypatch.setattr(tier_loop, "_EVENT_LINES", {})
    total = 2 * pool.MAX_CONSUMER_EVENT_LINES + 3
    for index in range(total):
        tier_loop._emit(queue, HOST, {"event": "ram-admission-refused",
                                      "ordinal": index})

    events = queue.host_events(HOST)

    assert len(events) <= 2 * pool.MAX_CONSUMER_EVENT_LINES
    assert events[-1]["ordinal"] == total - 1
    assert [event["ordinal"] for event in events] == sorted(
        event["ordinal"] for event in events)


def test_host_events_can_be_read_across_every_host(tmp_path):
    """``pbstatus`` merges every writer, the same way ``consumer_events`` does."""

    queue = pool.PoolQueue(tmp_path / "queue")
    tier_loop._emit(queue, "sparky", {"event": "ram-admission-refused"})
    tier_loop._emit(queue, "dl380g10", {"event": "ram-epoch-changed"})

    merged = {event["host"] for event in queue.host_events()}

    assert merged == {"sparky", "dl380g10"}
