"""Evidence nobody can read is unknown readiness, never "not staged".

``staged: false`` asserts a fact -- these bytes are not on the tier -- and a
window acts on it.  A fragment that is corrupt, truncated or unreadable
supports no such assertion, and reporting one as ``false`` is the same class
of error as #759 itself: an unproven state published as a known one.

``residency_map.read_fragments`` deliberately *skips* a file it cannot read or
validate, because a consumer composing its map must still find the copies its
other movers really did make.  That tolerance is right there and wrong here:
skipping is what flattens "I could not tell" into "not staged".  So the
readiness predicate reads this plan's own movers strictly and raises
``residency_plan.ResidencyEvidenceUnreadable``; the tier window gates closed
*and* emits ``ram-window-unknown``, and the census reports ``staged: null``.

A file beside them that no leg of this plan names is still skipped: it cannot
change this plan's answer, so refusing on it would be a stall with no reason.

No payload byte is read here or in the predicate under test.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
from prismabuild import pool, residency_map, residency_plan  # noqa: E402

import tier_loop  # noqa: E402

import test_a_reserved_range_is_not_resident_until_published as base  # noqa: E402

CONSUMER = base.CONSUMER
MOVER = base.MOVER
RAM_MOVER = base.RAM_MOVER
STAGE_TIER = base.STAGE_TIER
N_BYTES = base.N_BYTES


def _landed(tmp_path: Path) -> tuple[pool.PoolQueue, Path]:
    """A real, complete, published stage copy -- residency by every measure."""

    queue, _manifest, digest, manifest_path, _paths = base._world(tmp_path)
    base._reserve(queue, digest, N_BYTES)
    receipt = base._move(queue, digest, manifest_path, N_BYTES)
    assert receipt["complete"] is True, receipt.get("errors")
    queue.record_move(MOVER, receipt)
    plan = base._filed(queue)
    assert residency_plan.resident_movers(queue, plan, STAGE_TIER) == {MOVER}
    return queue, residency_map.fragment_path(
        queue.residency_fragment_root(), CONSUMER, MOVER)


def _unknown_now(queue: pool.PoolQueue) -> None:
    """Every reader must say unknown, and none of them may say unstaged."""

    plan = base._filed(queue)
    with pytest.raises(residency_plan.ResidencyEvidenceUnreadable):
        residency_plan.resident_movers(queue, plan, STAGE_TIER)

    events = tier_loop.ram_residency_window(queue, tiers=base._tiers(queue))
    assert base._promotions(events) == [], "unknown is not a source"
    assert not queue.item_path(pool.READY, RAM_MOVER).exists()
    assert [event for event in events
            if event.get("event") == "ram-window-unknown"], events

    entry = base._cursor(queue)
    phase = entry["phases"][0]
    assert phase["stage"]["staged"] is None, "unknown, not a clean false"
    assert phase["ram"]["staged"] is None
    assert phase["stage"]["reserved"] is True, "the booking is still reported"

    # The aggregate, not just the leg.  ``cursor_gap`` is what ``pb_cursors``
    # serves and what ``pbmetrics`` turns into the backlog series, and a leg
    # that reports ``None`` honestly is undone if the sum counts it as bytes
    # known to be missing.  An unknown belongs to neither side of that sum.
    for leg in ("stage", "ram"):
        gap = entry["cursor_gap"][leg]
        assert gap["unstaged_phases"] == [], (leg, gap)
        assert gap["unstaged_bytes"] == 0, (leg, gap)
        assert gap["unknown_phases"] == ["head"], (leg, gap)
        assert gap["unknown_bytes"] == base.N_BYTES, (leg, gap)
        assert gap["staged_phases"] == 0, (leg, gap)
        assert (gap["staged_phases"] + len(gap["unstaged_phases"])
                + len(gap["unknown_phases"]) == gap["remaining_phases"]), gap


def test_a_malformed_fragment_is_unknown_not_unstaged(tmp_path: Path) -> None:
    """A fragment that no longer validates is unknown readiness."""

    queue, fragment = _landed(tmp_path)
    # Truncated on the way to disk, or half of somebody else's document: the
    # file is there and its name is this mover's, and it says nothing.
    fragment.write_text('{"schema": "not-a-fragment", "entries": ')
    _unknown_now(queue)


def test_an_unreadable_fragment_is_unknown_not_unstaged(tmp_path: Path) -> None:
    """A fragment the process may not open is unknown readiness."""

    queue, fragment = _landed(tmp_path)
    os.chmod(fragment, 0)
    try:
        try:
            fragment.read_text()
        except OSError:
            pass
        else:
            pytest.skip("this process can read a mode-0 file; no denial to test")
        _unknown_now(queue)
    finally:
        os.chmod(fragment, 0o644)


def test_a_file_this_plan_does_not_name_is_still_skipped(tmp_path: Path) -> None:
    """Strictness covers this plan's movers, not every file in the directory.

    The tolerance ``read_fragments`` was written for survives: a foreign or
    half-written document beside the fragments cannot change what this plan's
    movers published, so refusing on it would be a stall with no reason.
    """

    queue, fragment = _landed(tmp_path)
    foreign = fragment.parent / f"{'a' * 64}.json"
    foreign.write_text("{oh dear")
    plan = base._filed(queue)
    assert residency_plan.resident_movers(queue, plan, STAGE_TIER) == {MOVER}
    assert [event["phase"] for event in base._promotions(
        tier_loop.ram_residency_window(queue, tiers=base._tiers(queue)))] == ["head"]
    entry = base._cursor(queue)
    assert entry["phases"][0]["stage"]["staged"] is True
    assert entry["cursor_gap"]["stage"]["staged_phases"] == 1
    assert entry["cursor_gap"]["stage"]["unknown_phases"] == []


def test_the_map_the_consumer_composes_is_untouched(tmp_path: Path) -> None:
    """The strict path is readiness only; the consumer's own reader is not.

    ``read_fragments`` keeps skipping, because a consumer that can still find
    three of its four movers' copies must read those three.  Readiness and
    composition ask different questions and keep different answers.
    """

    queue, fragment = _landed(tmp_path)
    fragment.write_text('{"schema": "not-a-fragment"}')
    assert residency_map.read_fragments(
        queue.residency_fragment_root(), CONSUMER) == []
    assert json.loads(fragment.read_text())["schema"] == "not-a-fragment"
