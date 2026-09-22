"""A consumer stages only as far ahead as its refill horizon (#903).

On 2026-09-22 GLM Stage A R12 (``683cb3caa5ea``) was reading ``chain-043``
while 24 of its movers held 528 GiB of the 565 GiB stage.  The furthest landed
range was ``chain-019``, about 24 phases and roughly 20 hours of reading ahead
of the consumer.  Two of its later movers and the native capture's 3 GiB lead
waited on ``tier_reservation_unavailable``.  The run-ahead bound (#632) had
done its job, which is to keep the stage from filling to 0 B; nothing bounded
the window by what the consumer could read before a later copy landed.

Rob: *"The time a spark spends processing should be used to refill the ramdisk
and ssds."*  The fix bounds a window by its **refill horizon**: the phase being
read, the bytes the consumer can hold ahead of it, and enough further ranges to
cover the time a newly published copy takes to land.  Ranges past the horizon
are published as the consumer advances.  Ranges already landed past it -- a
consumer claimed before the fix, or a horizon that shrank -- stay resident as a
cache until another window needs the room, and are then evicted farthest-needed
first.

The numbers every case here uses, so each assertion can be checked by hand:

* phases of 2 GiB, 8 of them; the reader is inside ``phase-0``;
* each landed copy took ``MOVER_SECONDS`` = 200 s, so a copy lands at
  2 GiB / 200 s = 10.7 MB/s;
* the reader was claimed ``CLAIMED_AGO_S`` = 1000 s ago and entered
  ``phase-0`` 10 s ago: it has read at most 2 GiB in 990 s, 2.2 MB/s;
* it reserves ``mem_gb`` 1, so it can hold 1 GiB ahead of what it reads.

The reader can be reading anything that starts before 2 GiB (the end of
``phase-0``) + 1 GiB (read-ahead) = 3 GiB, which is ``phase-0`` and
``phase-1``.  A copy published now lands within 30 s (heartbeat) + 60 s
(cycle) + 200 s (the slowest measured copy) = 290 s, in which the reader reads
0.6 GB -- less than one phase -- so one phase past the read-ahead, ``phase-2``,
completes the horizon.  ``phase-3`` is the advance the window publishes next;
``phase-4`` to ``phase-7`` are past the horizon.

Everything runs on a ``tmp_path`` queue and stage root; nothing touches a live
queue or a real stage mountpoint (#628).
"""
from __future__ import annotations

import json
from pathlib import Path
import sys
import time

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from prismabuild import pool, residency_plan  # noqa: E402
import stage_release  # noqa: E402
import tier_loop  # noqa: E402
from test_a_resident_range_is_adopted_rather_than_recopied import (  # noqa: E402
    GIB, PHASE_GIB, STAGE_KIND, TIER, _hexkey, _row, _stage_range,
    _tier_record, assert_ledger_matches_the_stage)

READER = "3" * 64       # R12's shape: claimed, reading phase-0, staged ahead
NEWCOMER = "4" * 64     # the capture's shape: ready, its lead short
READER_MANIFEST = "5" * 64
NEWCOMER_MANIFEST = "6" * 64
PHASES = 8
MOVER_SECONDS = 200.0
CLAIMED_AGO_S = 1000.0
REPORTED_AGO_S = 10.0
#: Phase names inside the horizon, the advance, and past the horizon, by the
#: arithmetic in the module docstring.
IN_HORIZON = (0, 1, 2)
ADVANCE = 3
BEYOND = (4, 5, 6, 7)


def _mover(label: str, ordinal: int) -> str:
    return _hexkey(f"{label}mover{ordinal}")


def _plan(queue: pool.PoolQueue, consumer: str, *, label: str,
          manifest: str, phases: int = PHASES) -> dict[str, object]:
    """One consumer's frozen plan: ``phases`` phases of ``PHASE_GIB`` each."""

    built = []
    for ordinal in range(phases):
        start, end = ordinal * PHASE_GIB * GIB, (ordinal + 1) * PHASE_GIB * GIB
        built.append({
            "name": f"phase-{ordinal}",
            "start_bytes": start, "end_bytes": end, "stage_gib": PHASE_GIB,
            "mover_row": {
                **_row(queue, _mover(label, ordinal),
                       {STAGE_KIND: PHASE_GIB, "cpu": 1, "mem_gb": 1}),
                "residency": {
                    "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                    "manifest_sha256": manifest,
                    "manifest_bytes": phases * PHASE_GIB * GIB,
                    "range_start_bytes": start, "range_end_bytes": end},
            },
            "egress_row": _row(queue, _hexkey(f"{label}egress{ordinal}"),
                               {"mem_gb": 1}),
        })
    return residency_plan.build_plan(
        consumer_action_key=consumer, tier_id=TIER, stage_root="/stage/prewarm",
        manifest_sha256=manifest, manifest_bytes=phases * PHASE_GIB * GIB,
        phases=built)


def _publish_consumer(queue: pool.PoolQueue, consumer: str,
                      plan: dict[str, object], *, manifest: str) -> None:
    residency_plan.freeze(queue, plan)
    queue.publish(
        action_key=consumer, cas_root=queue.root / "cas",
        checkout_root=queue.root / "co", worker_script=queue.root / "worker.py",
        resources={"cpu": 1, "mem_gb": 1},
        residency={"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                   "manifest_sha256": manifest,
                   "manifest_bytes": int(plan["manifest_bytes"]),
                   "leads": residency_plan.leads_for(plan)})


def _claim(queue: pool.PoolQueue, consumer: str, *, phase: str,
           claimed_unix: float, reported_unix: float) -> None:
    """Claim ``consumer`` and file its accepted progress, at stated times.

    ``tests/test_a_resident_range_is_adopted_rather_than_recopied.py`` files
    the claim and the report at the same instant; a consumption rate needs
    the two apart, so this helper takes both.
    """

    source = queue.item_path(pool.READY, consumer)
    item = json.loads(source.read_text())
    source.unlink()
    item.update({"action_key": consumer, "claimed_unix": claimed_unix,
                 "claimed_by": "horizon-fixture", "claimed_host": "dl380g10"})
    queue.item_path(pool.CLAIMED, consumer).write_text(json.dumps(item))
    queue.write_lease(
        consumer, owner="horizon-fixture", claim_snapshot=item,
        progress_observation={
            "source": "action-progress",
            "last_accepted": {"phase": phase, "units_completed": 1,
                              "reported_unix": reported_unix}})


def _reader(queue: pool.PoolQueue, stage: Path, *,
            landed: tuple[int, ...]) -> dict[int, list[Path]]:
    """The reader, claimed inside ``phase-0``, with ``landed`` phases staged."""

    plan = _plan(queue, READER, label="reader", manifest=READER_MANIFEST)
    _publish_consumer(queue, READER, plan, manifest=READER_MANIFEST)
    files = {ordinal: _stage_range(
        queue, mover=_mover("reader", ordinal), consumer=READER, stage=stage,
        ordinal=ordinal, manifest=READER_MANIFEST, seconds=MOVER_SECONDS)
        for ordinal in landed}
    now = time.time()
    _claim(queue, READER, phase="phase-0", claimed_unix=now - CLAIMED_AGO_S,
           reported_unix=now - REPORTED_AGO_S)
    return files


def _newcomer(queue: pool.PoolQueue, *, phases: int = 1,
              queued: bool = True) -> str:
    """A ready consumer of another manifest, and its lead mover's key.

    ``queued`` publishes the lead mover, the shape of a lead whose claim is
    short; without it the lead waits on the window's joint-fit gate, the
    shape of the native capture's lead on 2026-09-22.
    """

    plan = _plan(queue, NEWCOMER, label="newcomer", manifest=NEWCOMER_MANIFEST,
                 phases=phases)
    _publish_consumer(queue, NEWCOMER, plan, manifest=NEWCOMER_MANIFEST)
    lead = dict(plan["phases"][0]["mover_row"])  # type: ignore[index]
    if queued:
        queue.publish(**lead)
    return str(lead["action_key"])


def _claim_shortage(queue: pool.PoolQueue, key: str,
                    gib: int) -> dict[str, object] | None:
    """A claim's own tier gate, run and rolled back: ``None`` means it fits."""

    handles: dict[str, str] = {}
    funded: dict[str, dict[str, object]] = {}
    shortage = queue._begin_tier_acquire(
        key, {TIER: {"stage_gib": gib}}, handles, funded)
    queue._abandon_tier_acquire(handles)
    return shortage


def _held(queue: pool.PoolQueue, key: str) -> bool:
    return bool(queue.tier_ledger(TIER).holder_tokens(key))


def _cycle(queue: pool.PoolQueue, stage: Path, *, gib: int) -> None:
    tier_loop.cycle(queue, host="dl380g10", source_pool="storage_pool",
                    receipts=tier_loop.ReceiptCache(),
                    discover=lambda **_kwargs: {TIER: _tier_record(stage, gib=gib)})


def _fixture_queue(tmp_path: Path, gib: int) -> tuple[pool.PoolQueue, Path]:
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    queue.mint_tier_capacity(TIER, {"stage_gib": gib})
    stage = tmp_path / "stage"
    stage.mkdir()
    stage_release.register_stage_root(queue, tier_id=TIER, stage_root=stage)
    return queue, stage


# ------------------------------------------------------------- the incident


def test_a_range_past_the_horizon_makes_room_for_another_consumers_lead(
        tmp_path: Path) -> None:
    """The #903 incident, one cycle: the farthest range goes, the lead fits.

    The reader holds all 8 phases (16 GiB) of a 17 GiB stage, as R12 held
    24 phases of 565 GiB.  The newcomer's 2 GiB lead is queued and its claim
    is short.  Before the fix the cycle evicted nothing -- every range belongs
    to a live plan, so none is an orphan -- and the lead waited for as long
    as the reader took to read 16 GiB.  After it, the one range needed
    farthest in the future goes and the lead fits.
    """

    capacity = PHASE_GIB * PHASES + 1
    queue, stage = _fixture_queue(tmp_path, capacity)
    files = _reader(queue, stage, landed=tuple(range(PHASES)))
    lead = _newcomer(queue)
    shortage = _claim_shortage(queue, lead, PHASE_GIB)
    assert shortage is not None
    assert shortage["reason"] == "tier_reservation_unavailable"

    _cycle(queue, stage, gib=capacity)

    assert _claim_shortage(queue, lead, PHASE_GIB) is None
    # Just enough: the farthest range and nothing else.
    assert not _held(queue, _mover("reader", 7))
    assert not any(path.exists() for path in files[7])
    for ordinal in range(PHASES - 1):
        assert _held(queue, _mover("reader", ordinal)), ordinal
        assert all(path.exists() for path in files[ordinal]), ordinal
    assert_ledger_matches_the_stage(queue)


def test_a_window_publishes_only_to_its_refill_horizon(tmp_path: Path) -> None:
    """With room for every phase, the window still stops at the horizon.

    Before the fix the run-ahead bound was the tier less one phase, so a
    40 GiB stage published all seven remaining phases on the first cycle
    after the reader's first report -- R12 published 22 movers in one cycle
    69 s after its claim.  After it, only the phases inside the horizon are
    published; the rest follow as the reader advances.
    """

    capacity = 40
    queue, stage = _fixture_queue(tmp_path, capacity)
    _reader(queue, stage, landed=(0,))

    _cycle(queue, stage, gib=capacity)

    published = {ordinal for ordinal in range(PHASES)
                 if queue.item_path(pool.READY, _mover("reader", ordinal)).exists()}
    assert published == {1, 2}, published


def test_a_gated_newcomer_is_relieved_by_ranges_past_the_horizon(
        tmp_path: Path) -> None:
    """The capture's shape: an unpublished lead gated on current plus next.

    The newcomer has two phases, so the window's joint-fit gate admits it
    only when the tier holds its lead and its next beside everything already
    held: 16 + 2 + 2 against 17, three short.  The relief that gate asks for
    was bounded by the tier's orphans (#orphan-pressure, #901), and a live
    plan's ranges are never orphans, so before the fix nothing was asked for
    and the lead was never published.  After it, ranges past the reader's
    horizon count toward that bound: the two farthest go, and the lead
    publishes the same cycle.
    """

    capacity = PHASE_GIB * PHASES + 1
    queue, stage = _fixture_queue(tmp_path, capacity)
    files = _reader(queue, stage, landed=tuple(range(PHASES)))
    lead = _newcomer(queue, phases=2, queued=False)

    _cycle(queue, stage, gib=capacity)

    assert queue.item_path(pool.READY, lead).exists()
    for ordinal in (6, 7):
        assert not _held(queue, _mover("reader", ordinal)), ordinal
        assert not any(path.exists() for path in files[ordinal]), ordinal
    for ordinal in range(6):
        assert _held(queue, _mover("reader", ordinal)), ordinal
        assert all(path.exists() for path in files[ordinal]), ordinal
    assert_ledger_matches_the_stage(queue)
