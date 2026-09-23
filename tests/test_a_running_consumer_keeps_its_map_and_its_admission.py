"""A running consumer keeps its residency map and its admission (#908).

Two defects broke a running consumer's continuity on 2026-09-22.  Capture
``a92f62783e8f`` was claimed at 23:03:10Z, read ``head`` through ``layer-2``
off the stage and reported ``layer-3``.  At 1790118227 the tier loop
egressed those four ranges, which was correct.  Then:

* ``compose_map`` removed the capture's map, because no staged range was
  left, and the capture's reader logged ``residency map is unreadable: No
  such file or directory``.  A claimed consumer's map is now kept, empty,
  until its next range lands, and removed only once the consumer is no
  longer running.
* ``advance_needs`` names the plan's first range as the lead.  Once ``head``
  was egressed, the lead read as unpublished, and the joint gate re-checked
  the running capture as a newcomer: 60 cycles of ``window-gated
  joint-fit-stall``.  A claimed consumer passed the claim's residency gate,
  so its window is admitted for the rest of its run and is never a newcomer
  again.  Its unpublished current counts against a newcomer only on a pass
  that permits the window, so a running window that cannot publish holds
  no newcomer out (#881).

Everything runs on a ``tmp_path`` queue and stage root; nothing touches a live
mountpoint, the live queue or a tier file (#628).
"""

from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from prismabuild import pool, residency_map  # noqa: E402
import stage_release  # noqa: E402
import tier_loop  # noqa: E402
from test_a_consumer_stages_only_to_its_refill_horizon import (  # noqa: E402
    CAPTURE, CAPTURE_MANIFEST, CAPTURE_NAMES, NEWCOMER, NEWCOMER_MANIFEST,
    _capture, _claim_shortage, _cycle, _fixture_queue, _mover, _plan,
    _publish_consumer)
from test_a_resident_range_is_adopted_rather_than_recopied import (  # noqa: E402
    assert_ledger_matches_the_stage)


def _egress(queue: pool.PoolQueue, stage: Path, ordinals: range) -> None:
    """Run the capture's egresses: every passed range given back."""

    for ordinal in ordinals:
        receipt = stage_release.evict(
            queue, _mover("capture", ordinal), consumer_action_key=CAPTURE,
            stage_root=str(stage))
        assert receipt["complete"], receipt


def _events(capsys: pytest.CaptureFixture[str]) -> list[dict[str, object]]:
    out = []
    for line in capsys.readouterr().out.splitlines():
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(event, dict):
            out.append(event)
    return out


# ------------------------------------------------------------------ the map


def test_a_running_consumers_map_outlives_the_gap_before_its_next_range(
        tmp_path: Path) -> None:
    """Between the last egress and the next landing the map stays, empty.

    The capture's first cycle composes its map from the four ranges it has
    read and publishes their egresses and its ``layer-3`` mover.  The
    egresses run before ``layer-3`` lands.  The next cycle keeps the map the
    reader adopted, with the same tier, stage root and manifest, naming no
    range and no mover, rather than removing it.
    """

    capacity = 30
    queue, stage = _fixture_queue(tmp_path, capacity)
    _capture(queue, stage, landed=(0, 1, 2, 3))
    _cycle(queue, stage, gib=capacity)
    path = queue.residency_map_path(CAPTURE)
    before = residency_map.read_map(path)
    assert len(before["entries"]) == 4                     # type: ignore[arg-type]

    _egress(queue, stage, range(4))
    _cycle(queue, stage, gib=capacity)

    after = residency_map.read_map(path)
    assert after["entries"] == {}
    assert after["leads"] == []
    for field in ("tier_id", "stage_root", "manifest_sha256"):
        assert after[field] == before[field], field
    assert after["manifest_sha256"] == CAPTURE_MANIFEST
    assert_ledger_matches_the_stage(queue)


def test_the_kept_map_names_the_next_range_once_it_lands(
        tmp_path: Path) -> None:
    """The empty map is an ordinary map: the next landing recomposes it."""

    capacity = 30
    queue, stage = _fixture_queue(tmp_path, capacity)
    plan = _capture(queue, stage, landed=(0, 1, 2, 3))
    _cycle(queue, stage, gib=capacity)
    _egress(queue, stage, range(4))
    _cycle(queue, stage, gib=capacity)
    path = queue.residency_map_path(CAPTURE)
    assert residency_map.read_map(path)["entries"] == {}

    from test_a_consumer_stages_only_to_its_refill_horizon import _land
    layer_3 = plan["phases"][4]                            # type: ignore[index]
    queue.item_path(pool.READY, _mover("capture", 4)).unlink()
    _land(queue, stage, consumer=CAPTURE, manifest=CAPTURE_MANIFEST,
          mover=_mover("capture", 4), name=CAPTURE_NAMES[4],
          start=int(layer_3["start_bytes"]), end=int(layer_3["end_bytes"]),
          seconds=36.0)
    _cycle(queue, stage, gib=capacity)

    landed = residency_map.read_map(path)
    assert len(landed["entries"]) == 1                     # type: ignore[arg-type]
    assert landed["leads"] == [_mover("capture", 4)]


def test_a_consumer_that_is_no_longer_running_loses_its_empty_map(
        tmp_path: Path) -> None:
    """Back in ``ready`` with nothing staged, the map goes, as before #908.

    A consumer returned to ``ready`` (a membership requeue) must pass the
    claim's residency gate again, and that gate reads a missing map as
    ``map_not_composed``.  A kept empty map would read as ``map_stale``
    instead; either refuses, but the kept map is only for a running
    consumer, so it is removed.
    """

    capacity = 30
    queue, stage = _fixture_queue(tmp_path, capacity)
    _capture(queue, stage, landed=(0, 1, 2, 3))
    _cycle(queue, stage, gib=capacity)
    _egress(queue, stage, range(4))
    _cycle(queue, stage, gib=capacity)
    path = queue.residency_map_path(CAPTURE)
    assert path.exists()

    claimed = queue.item_path(pool.CLAIMED, CAPTURE)
    claimed.rename(queue.item_path(pool.READY, CAPTURE))
    _cycle(queue, stage, gib=capacity)

    assert not path.exists()


def test_a_ready_consumer_with_nothing_staged_still_has_no_map(
        tmp_path: Path) -> None:
    """The claim gate's ``map_not_composed`` is untouched for a newcomer."""

    capacity = 30
    queue, stage = _fixture_queue(tmp_path, capacity)
    plan = _plan(queue, NEWCOMER, label="newcomer", manifest=NEWCOMER_MANIFEST,
                 sizes=[3, 1])
    _publish_consumer(queue, NEWCOMER, plan, manifest=NEWCOMER_MANIFEST)
    _cycle(queue, stage, gib=capacity)

    assert not queue.residency_map_path(NEWCOMER).exists()


# ---------------------------------------------------------- the admission


def _big_newcomer(queue: pool.PoolQueue, gib: int) -> str:
    """A ready consumer whose queued lead holds no tokens yet.

    The joint gate counts a queued row's demand in full, so this row is what
    turns the capture's re-check as a newcomer into ``joint-fit-stall`` while
    the tier's free room is untouched.
    """

    plan = _plan(queue, NEWCOMER, label="newcomer", manifest=NEWCOMER_MANIFEST,
                 sizes=[gib])
    _publish_consumer(queue, NEWCOMER, plan, manifest=NEWCOMER_MANIFEST)
    lead = dict(plan["phases"][0]["mover_row"])            # type: ignore[index]
    queue.publish(**lead)
    return str(lead["action_key"])


def test_a_running_consumer_is_not_regated_after_its_first_range_is_egressed(
        tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """The capture from 23:03:47 on, with room: ``layer-3`` publishes.

    The capture's four ranges are egressed and ``layer-3`` is not yet
    published.  The tier is empty, so 40 GiB are free, but another consumer's
    30 GiB lead is queued.  As a newcomer the capture would ask the joint
    gate for held + queued + current = 0 + 30 + 14 = 44 GiB of 40, and be
    gated.  It is running, so its window is admitted: ``layer-3`` publishes
    from free room and claims.
    """

    capacity = 40
    queue, stage = _fixture_queue(tmp_path, capacity)
    _capture(queue, stage, landed=(0, 1, 2, 3))
    _egress(queue, stage, range(4))
    _big_newcomer(queue, 30)
    layer_3 = _mover("capture", 4)
    capsys.readouterr()

    _cycle(queue, stage, gib=capacity)

    gated = [event for event in _events(capsys)
             if event.get("event") == "window-gated"
             and event.get("consumer") == CAPTURE]
    assert gated == []
    assert queue.item_path(pool.READY, layer_3).exists()
    assert _claim_shortage(queue, layer_3, 14) is None


def test_a_newcomer_cannot_take_the_room_a_running_consumers_current_needs(
        tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """The running capture's unpublished ``layer-3`` counts for a newcomer.

    40 GiB free and nothing queued.  The capture is running with its 14 GiB
    ``layer-3`` unpublished; a ready newcomer asks for a 20 GiB lead and a
    10 GiB next.  Either fits the free room alone, but not beside the other:
    14 + 20 + 10 = 44 of 40.  The gate permits the capture this pass, so it
    counts the capture's current for the newcomer, which waits.  An
    admitted window's current is counted only when the window is permitted;
    a window gated on its own fence reserves nothing (#881, the
    ``test_claimed_window_does_not_set_newcomer_priority_barrier`` guard).
    """

    capacity = 40
    queue, stage = _fixture_queue(tmp_path, capacity)
    _capture(queue, stage, landed=(0, 1, 2, 3))
    _egress(queue, stage, range(4))
    plan = _plan(queue, NEWCOMER, label="newcomer", manifest=NEWCOMER_MANIFEST,
                 sizes=[20, 10])
    _publish_consumer(queue, NEWCOMER, plan, manifest=NEWCOMER_MANIFEST)
    lead = str(plan["phases"][0]["mover_row"]["action_key"])  # type: ignore[index]
    capsys.readouterr()

    _cycle(queue, stage, gib=capacity)

    assert queue.item_path(pool.READY, _mover("capture", 4)).exists()
    assert not queue.item_path(pool.READY, lead).exists()
    gated = [event for event in _events(capsys)
             if event.get("event") == "window-gated"
             and event.get("consumer") == NEWCOMER]
    assert gated, "the newcomer was not gated"
    assert {event.get("reason") for event in gated} == {"joint-fit-stall"}


def test_a_running_consumer_is_never_an_admission_newcomer(
        tmp_path: Path) -> None:
    """``_advance_wants`` marks a claimed window admitted, lead or no lead."""

    capacity = 40
    queue, stage = _fixture_queue(tmp_path, capacity)
    _capture(queue, stage, landed=(0, 1, 2, 3))
    _egress(queue, stage, range(4))
    tiers = _announced(stage, capacity)

    wants, unknown = tier_loop._advance_wants(
        queue, tiers, mover_role="mover_row",
        tier_of=lambda plan: plan.get("tier_id"),
        state_of=tier_loop._mover_state)

    assert unknown == []
    capture = [want for want in wants if want["key"] == CAPTURE]
    assert len(capture) == 1
    assert capture[0]["newcomer"] is False
    assert capture[0]["needs"]["lead_mover_action_key"] == _mover("capture", 0)


def _announced(stage: Path, capacity: int) -> dict[str, dict[str, object]]:
    from test_a_resident_range_is_adopted_rather_than_recopied import (
        TIER, _tier_record)
    return {TIER: _tier_record(stage, gib=capacity)}
