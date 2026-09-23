"""An orphan sweep that cannot read its tier ledger deletes nothing, and says so (#1007).

``stage_release.sweep`` reads a tier's held keys twice: once for the held-key
pass, and again, after that pass, to join what is still held into the movers
whose fragments count as attribution for ``reconcile``.  Before #1007 a
failure of that second read made the held set empty, and the sweep still ran
``reconcile``, a pass that deletes.  A held mover that no live plan names is
attribution only through that held set, so its fragment stopped counting and
its staged bytes read as unowned.

Unknown ownership must never delete.  When the ledger cannot be read the
sweep skips the reconciliation for that cycle and records the skip with its
reason.  A failure of the first read already skipped the whole tier, but
silently; it now records the skip too.
"""
from __future__ import annotations

from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
from prismabuild import pool, residency_map  # noqa: E402
import stage_release  # noqa: E402

CONSUMER = "c" * 64
MOVER = "4" * 64
TIER = "prismabuild-stage:dl380g10"
#: The record a skipped pass leaves, spelled here rather than read from the
#: module, so that the test fails on the missing record and not on a name.
SKIPPED_EVENT = "stage-sweep-ledger-unreadable"


@pytest.fixture()
def held_mover(tmp_path: Path):
    """A complete mover still holding its tokens, whose plan no longer names it."""

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    stage = tmp_path / "stage"
    stage.mkdir()
    stage_release.register_stage_root(queue, tier_id=TIER, stage_root=stage)
    shard = stage / "model-00007-of-00120.safetensors"
    shard.write_bytes(b"\0" * 8192)
    residency_map.write_fragment(queue.root / pool.RESIDENCY, {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": CONSUMER, "mover_action_key": MOVER,
        "tier_id": TIER, "stage_root": str(stage),
        "manifest_sha256": "a" * 64,
        "entries": {residency_map.residency_map_key("/mnt/shared/shard", 0): {
            "stage_path": str(shard), "bytes": 8192,
            "sha256": "b" * 64, "offset": 0}},
    })
    queue.record_move(MOVER, {
        "tier_id": TIER, "consumer_action_key": CONSUMER, "complete": True,
        "bytes_staged": 8192, "range_start_bytes": 0, "range_end_bytes": 8192,
        "stage_root": str(stage), "unix": 10.0})
    ledger = queue.tier_ledger(TIER)
    ledger.ensure_capacity({"stage_gib": 4})
    assert ledger.acquire(MOVER, {"stage_gib": 1})
    return queue, stage, shard


def _unreadable_ledger(monkeypatch, *, armed: bool) -> dict[str, bool]:
    """Make every tier-ledger ``held_keys`` read fail once ``armed`` is set."""

    state = {"armed": armed}
    original = pool.ResourceLedger.held_keys

    def held_keys(self):
        if state["armed"]:
            raise OSError("tier ledger unreadable (test)")
        return original(self)

    monkeypatch.setattr(pool.ResourceLedger, "held_keys", held_keys)
    return state


def _sweep(queue, stage) -> list[dict[str, object]]:
    # The tier is under no pressure, so the held-key pass keeps the held
    # mover and the reconciliation is what would reach its bytes.
    return stage_release.sweep(queue, stage_roots={TIER: str(stage)},
                               pressure={TIER: 0})


def test_the_reconciliation_is_skipped_when_the_held_set_cannot_be_read(
        held_mover, monkeypatch) -> None:
    """The read after the held-key pass fails: nothing is deleted, the skip is recorded."""

    queue, stage, shard = held_mover
    state = _unreadable_ledger(monkeypatch, armed=False)
    original = stage_release._unresolved_reports

    def then_the_ledger_fails(*args, **kwargs):
        # The held-key pass has run; the next ledger read is the one that
        # decides what the reconciliation may attribute.
        reports = original(*args, **kwargs)
        state["armed"] = True
        return reports

    monkeypatch.setattr(stage_release, "_unresolved_reports",
                        then_the_ledger_fails)
    events = _sweep(queue, stage)

    assert shard.exists(), f"a held mover's bytes were reconciled away: {events}"
    assert not [event for event in events
                if event.get("event") == stage_release.UNATTRIBUTED_EVENT
                and event.get("entries_deleted")], events
    skipped = [event for event in events if event.get("event") == SKIPPED_EVENT]
    assert len(skipped) == 1, events
    record = skipped[0]
    assert record["tier_id"] == TIER
    assert record["stage_root"] == str(stage)
    assert record["skipped"] == "reconciliation"
    assert "tier ledger unreadable (test)" in record["reason"], record
    assert record["entries_deleted"] == 0
    assert record["complete"] is False

    state["armed"] = False
    assert MOVER in queue.tier_ledger(TIER).held_keys()


def test_a_tier_whose_ledger_cannot_be_read_at_all_is_skipped_on_the_record(
        held_mover, monkeypatch) -> None:
    """The first read fails: the whole tier is skipped, and the skip is recorded."""

    queue, stage, shard = held_mover
    state = _unreadable_ledger(monkeypatch, armed=True)

    events = _sweep(queue, stage)

    assert shard.exists(), events
    skipped = [event for event in events if event.get("event") == SKIPPED_EVENT]
    assert len(skipped) == 1, events
    record = skipped[0]
    assert record["tier_id"] == TIER
    assert record["skipped"] == "held-key pass and reconciliation"
    assert "tier ledger unreadable (test)" in record["reason"], record
    assert record["complete"] is False

    state["armed"] = False
    assert MOVER in queue.tier_ledger(TIER).held_keys()
