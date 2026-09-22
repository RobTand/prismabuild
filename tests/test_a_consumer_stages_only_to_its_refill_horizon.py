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
from prismabuild import pool, reader_lease, residency_map, residency_plan  # noqa: E402
from prismabuild import storage_tiers  # noqa: E402
import stage_release  # noqa: E402
import tier_loop  # noqa: E402
from test_a_resident_range_is_adopted_rather_than_recopied import (  # noqa: E402
    DIGEST, GIB, PHASE_GIB, STAGE_KIND, TIER, _hexkey, _row, _stage_range,
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
          manifest: str, phases: int = PHASES,
          sizes: list[int] | None = None,
          names: list[str] | None = None) -> dict[str, object]:
    """One consumer's frozen plan: ``phases`` phases of ``PHASE_GIB`` each.

    ``sizes`` (GiB per phase) and ``names`` shape a plan like the capture's
    instead; the phases stay contiguous in read order, as ``validate_plan``
    requires.
    """

    sizes = list(sizes) if sizes is not None else [PHASE_GIB] * phases
    names = (list(names) if names is not None
             else [f"phase-{ordinal}" for ordinal in range(len(sizes))])
    total = sum(sizes) * GIB
    built = []
    start = 0
    for ordinal, gib in enumerate(sizes):
        end = start + gib * GIB
        built.append({
            "name": names[ordinal],
            "start_bytes": start, "end_bytes": end, "stage_gib": gib,
            "mover_row": {
                **_row(queue, _mover(label, ordinal),
                       {STAGE_KIND: gib, "cpu": 1, "mem_gb": 1}),
                "residency": {
                    "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                    "manifest_sha256": manifest,
                    "manifest_bytes": total,
                    "range_start_bytes": start, "range_end_bytes": end},
            },
            "egress_row": _row(queue, _hexkey(f"{label}egress{ordinal}"),
                               {"mem_gb": 1}),
        })
        start = end
    return residency_plan.build_plan(
        consumer_action_key=consumer, tier_id=TIER, stage_root="/stage/prewarm",
        manifest_sha256=manifest, manifest_bytes=total, phases=built)


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
            landed: tuple[int, ...],
            phases: int = PHASES) -> dict[int, list[Path]]:
    """The reader, claimed inside ``phase-0``, with ``landed`` phases staged."""

    plan = _plan(queue, READER, label="reader", manifest=READER_MANIFEST,
                 phases=phases)
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


# ------------------------------------------------- the capture, 2026-09-22

CAPTURE = "7" * 64
CAPTURE_MANIFEST = "8" * 64
#: The native capture's plan (``a92f62783e8f``): a 3 GiB head, three 1 GiB
#: layers, and a 14 GiB fourth layer.
CAPTURE_NAMES = ["head", "layer-0", "layer-1", "layer-2", "layer-3"]
CAPTURE_SIZES = [3, 1, 1, 1, 14]
#: A reader shaped like R12 on a larger stage: twenty 2 GiB phases, all
#: landed.  By the module docstring's arithmetic its horizon ends where
#: ``phase-3`` starts, ``phase-3`` is its advance, and ``phase-4`` to
#: ``phase-19`` (32 GiB) are past the horizon.
WIDE = 20


def _land(queue: pool.PoolQueue, stage: Path, *, consumer: str,
          manifest: str, mover: str, name: str, start: int, end: int,
          seconds: float = MOVER_SECONDS) -> list[Path]:
    """``_stage_range`` for a range of any size: tokens, files, fragment,
    material and a complete receipt, filed the way ``stage_move`` files them."""

    size = end - start
    path = stage / manifest[:8] / name / "part-0.bin"
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as stream:
        stream.truncate(size)
    entries = {residency_map.residency_map_key(
        f"/pool/{manifest[:8]}/{name}/part-0.bin", 0): {
            "stage_path": str(path), "bytes": size, "offset": 0,
            "sha256": DIGEST}}
    assert queue.tier_ledger(TIER).acquire(
        mover, {"stage_gib": storage_tiers.stage_tokens_for_bytes(size)})
    residency_map.write_fragment(queue.residency_fragment_root(), {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": consumer, "mover_action_key": mover,
        "tier_id": TIER, "stage_root": str(stage), "manifest_sha256": manifest,
        "entries": entries})
    reader_lease.write_material(
        queue.residency_fragment_root(), consumer_action_key=consumer,
        mover_action_key=mover, tier_id=TIER, stage_root=str(stage),
        manifest_sha256=manifest, generation="b" * 32,
        entries={key: {**dict(mention),  # type: ignore[dict-item]
                       "file_id": reader_lease.stat_identity(
                           str(mention["stage_path"]))}  # type: ignore[index]
                 for key, mention in entries.items()})
    queue.record_move(mover, {
        "consumer_action_key": consumer, "tier_id": TIER,
        "stage_root": str(stage), "manifest_sha256": manifest,
        "range_start_bytes": start, "range_end_bytes": end,
        "range_bytes": size, "bytes_staged": size,
        "entries_declared": 1, "entries_staged": 1,
        "complete": True, "seconds": float(seconds), "unix": 2000.0})
    return [path]


def _capture(queue: pool.PoolQueue, stage: Path, *,
             landed: tuple[int, ...]) -> dict[str, object]:
    """The capture, claimed and reporting ``layer-3``, ``landed`` phases staged."""

    plan = _plan(queue, CAPTURE, label="capture", manifest=CAPTURE_MANIFEST,
                 sizes=CAPTURE_SIZES, names=CAPTURE_NAMES)
    _publish_consumer(queue, CAPTURE, plan, manifest=CAPTURE_MANIFEST)
    for ordinal in landed:
        phase = plan["phases"][ordinal]                      # type: ignore[index]
        _land(queue, stage, consumer=CAPTURE, manifest=CAPTURE_MANIFEST,
              mover=_mover("capture", ordinal), name=CAPTURE_NAMES[ordinal],
              start=int(phase["start_bytes"]), end=int(phase["end_bytes"]),
              seconds=10.0)
    now = time.time()
    # Claimed at 23:03:10Z and reporting ``layer-3`` 27 s later.
    _claim(queue, CAPTURE, phase="layer-3", claimed_unix=now - 37.0,
           reported_unix=now - 10.0)
    return plan


def _assert_farthest_evicted(files: dict[int, list[Path]], queue: pool.PoolQueue,
                             *, kept: range, evicted: range) -> None:
    for ordinal in kept:
        assert _held(queue, _mover("reader", ordinal)), ordinal
        assert all(path.exists() for path in files[ordinal]), ordinal
    for ordinal in evicted:
        assert not _held(queue, _mover("reader", ordinal)), ordinal
        assert not any(path.exists() for path in files[ordinal]), ordinal


def test_a_running_consumers_next_range_preempts_ranges_past_another_horizon(
        tmp_path: Path) -> None:
    """The 23:03 capture, on the first cycle after it reported ``layer-3``.

    The capture was admitted on its lead and protected next, read ``head``
    through ``layer-2`` off the stage and reported ``layer-3``.  Its 14 GiB
    ``layer-3`` range was never published: the stage had 0 to 5 GiB free, R12
    held 564 of 566 GiB, every one of R12's ranges belonged to a live plan
    and so none was an orphan, and after 300 s without progress PB stopped
    the capture for it.  Here the tier has 1 GiB free.  After the fix the
    same cycle that publishes the capture's egresses gives back the seven
    ranges the reader needs last -- 14 GiB -- and publishes ``layer-3``.
    """

    capacity = PHASE_GIB * WIDE + sum(CAPTURE_SIZES[:4]) + 1
    queue, stage = _fixture_queue(tmp_path, capacity)
    files = _reader(queue, stage, landed=tuple(range(WIDE)), phases=WIDE)
    plan = _capture(queue, stage, landed=(0, 1, 2, 3))
    layer_3 = _mover("capture", 4)

    _cycle(queue, stage, gib=capacity)

    assert queue.item_path(pool.READY, layer_3).exists()
    assert _claim_shortage(queue, layer_3, 14) is None
    for ordinal in range(4):
        egress = plan["phases"][ordinal]["egress_row"]["action_key"]  # type: ignore[index]
        assert queue.item_path(pool.READY, egress).exists(), CAPTURE_NAMES[ordinal]
    _assert_farthest_evicted(files, queue, kept=range(13), evicted=range(13, WIDE))
    assert_ledger_matches_the_stage(queue)


def test_a_running_consumer_regated_after_its_lead_retires_still_preempts(
        tmp_path: Path) -> None:
    """The capture from 23:03:47 on: its egresses ran, ``layer-3`` still waits.

    Once ``head`` was given back the joint gate read the capture as a
    newcomer again (its original lead holds nothing), and 60 cycles logged
    ``window-gated joint-fit-stall`` until PB stopped it.  The newcomer's
    relief was bounded by the tier's orphans, and there were none.  After
    the fix the ranges past the reader's horizon count toward that bound:
    the same seven go, the gate admits and ``layer-3`` publishes.
    """

    capacity = PHASE_GIB * WIDE + 1
    queue, stage = _fixture_queue(tmp_path, capacity)
    files = _reader(queue, stage, landed=tuple(range(WIDE)), phases=WIDE)
    _capture(queue, stage, landed=())
    layer_3 = _mover("capture", 4)

    _cycle(queue, stage, gib=capacity)

    assert queue.item_path(pool.READY, layer_3).exists()
    assert _claim_shortage(queue, layer_3, 14) is None
    _assert_farthest_evicted(files, queue, kept=range(13), evicted=range(13, WIDE))
    assert_ledger_matches_the_stage(queue)


# ------------------------------------------------------------ the invariants


def test_no_range_inside_a_horizon_is_evicted_when_room_cannot_be_made(
        tmp_path: Path) -> None:
    """A demand the ranges past the horizon cannot cover evicts nothing.

    The newcomer's lead needs 10 GiB; the tier has 1 free and the reader's
    ranges past its horizon hold 8.  Taking all 8 would still leave the lead
    short and would cost the reader ranges it will read, so nothing goes --
    and the ranges inside the horizon, and the advance, are never offered.
    """

    capacity = PHASE_GIB * PHASES + 1
    queue, stage = _fixture_queue(tmp_path, capacity)
    files = _reader(queue, stage, landed=tuple(range(PHASES)))
    plan = _plan(queue, NEWCOMER, label="newcomer", manifest=NEWCOMER_MANIFEST,
                 sizes=[10])
    _publish_consumer(queue, NEWCOMER, plan, manifest=NEWCOMER_MANIFEST)
    queue.publish(**dict(plan["phases"][0]["mover_row"]))  # type: ignore[index]

    _cycle(queue, stage, gib=capacity)

    _assert_farthest_evicted(files, queue, kept=range(PHASES), evicted=range(0))
    assert_ledger_matches_the_stage(queue)


def test_a_pinned_range_past_the_horizon_is_declined_whole(
        tmp_path: Path) -> None:
    """A reader's pin keeps a range whole; the next farthest goes instead.

    ``phase-7`` is past the horizon but pinned.  An egress would defer it and
    file a retiring mark; an eviction for room may do neither, because the
    consumer will still read that range.  So the eviction declines it --
    nothing unlinked, tokens and fragment held, no retiring mark -- and
    ``phase-6`` makes the room.
    """

    capacity = PHASE_GIB * PHASES + 1
    queue, stage = _fixture_queue(tmp_path, capacity)
    files = _reader(queue, stage, landed=tuple(range(PHASES)))
    pinned = reader_lease.acquire(
        queue, consumer_action_key=READER,
        attempt={"nonce": "n1", "scope_id": "s1"}, tier_id=TIER, epoch="",
        span={"start_bytes": 7 * PHASE_GIB * GIB,
              "end_bytes": 8 * PHASE_GIB * GIB},
        holder={"host": "test-host", "pid": 4242}, acquire_token="t1",
        covers=[{"mover_action_key": _mover("reader", 7),
                 "manifest_sha256": READER_MANIFEST}])
    assert pinned.get("pin_id"), pinned
    lead = _newcomer(queue)

    _cycle(queue, stage, gib=capacity)

    assert _claim_shortage(queue, lead, PHASE_GIB) is None
    _assert_farthest_evicted(files, queue, kept=range(6), evicted=range(6, 7))
    assert _held(queue, _mover("reader", 7))
    assert all(path.exists() for path in files[7])
    marks, tainted = reader_lease.retiring_for(
        reader_lease.leases_root(queue, queue.residency_fragment_root()),
        _mover("reader", 7))
    assert (marks, tainted) == ([], [])
    assert_ledger_matches_the_stage(queue)


def test_an_evicted_range_is_published_again_when_its_reader_nears_it(
        tmp_path: Path) -> None:
    """The farthest range goes for the newcomer, then comes back in time.

    After the incident cycle ``phase-7`` holds nothing, so it reads as
    unpublished.  When the reader reports ``phase-5``, ``phase-7`` is inside
    its horizon again and the window publishes its mover, whole, from the
    plan -- while ``phase-0`` to ``phase-4`` are given back as read.
    """

    capacity = PHASE_GIB * PHASES + 1
    queue, stage = _fixture_queue(tmp_path, capacity)
    _reader(queue, stage, landed=tuple(range(PHASES)))
    _newcomer(queue)
    _cycle(queue, stage, gib=capacity)
    assert not _held(queue, _mover("reader", 7))
    assert not queue.item_path(pool.READY, _mover("reader", 7)).exists()

    # The worker's next heartbeat, filed into the lease the claim wrote.
    # ``write_lease`` would refuse it here: the fixture claims as
    # ``dl380g10`` from whichever box runs the test, and a second write
    # compares the two (``AmbiguousClaimHolder``).
    lease_path = queue.lease_path(READER)
    lease = json.loads(lease_path.read_text())
    lease["progress_observation"] = {
        "source": "action-progress",
        "last_accepted": {"phase": "phase-5", "units_completed": 2,
                          "reported_unix": time.time()}}
    lease_path.write_text(json.dumps(lease))

    _cycle(queue, stage, gib=capacity)

    assert queue.item_path(pool.READY, _mover("reader", 7)).exists()
    for ordinal in range(5):
        egress = _hexkey(f"readeregress{ordinal}")
        assert queue.item_path(pool.READY, egress).exists(), ordinal


def test_a_window_claimed_before_the_horizon_keeps_its_ranges(
        tmp_path: Path) -> None:
    """R12's shape: rows published past the horizon before it existed.

    The reader holds ``phase-0`` to ``phase-3`` and two rows past its horizon
    wait in ``ready/`` on a full tier, as R12's ``chain-021`` and
    ``chain-018`` did.  Those rows are not in-horizon demand, so nothing is
    evicted for them -- not the reader's own landed ranges, and nothing
    else -- they stay queued rather than withdrawn (a withdrawal would retire
    the whole plan, #708), and nothing further past the horizon publishes.
    """

    capacity = PHASE_GIB * 4 + 1
    queue, stage = _fixture_queue(tmp_path, capacity)
    files = _reader(queue, stage, landed=(0, 1, 2, 3))
    plan = residency_plan.read(queue, READER)
    assert plan is not None
    for ordinal in (5, 6):
        queue.publish(**dict(plan["phases"][ordinal]["mover_row"]))  # type: ignore[index]

    _cycle(queue, stage, gib=capacity)

    _assert_farthest_evicted(files, queue, kept=range(4), evicted=range(0))
    for ordinal in (5, 6):
        assert queue.item_path(pool.READY, _mover("reader", ordinal)).exists()
    for ordinal in (4, 7):
        assert not queue.item_path(pool.READY, _mover("reader", ordinal)).exists()
    assert_ledger_matches_the_stage(queue)


# ------------------------------------------------------ the horizon itself


def _horizon(queue: pool.PoolQueue, **overrides) -> dict[str, object] | None:
    plan = _plan(queue, READER, label="reader", manifest=READER_MANIFEST)
    phase = overrides.pop("phase", "phase-0")
    arguments: dict[str, object] = {
        "claimed_unix": 1000.0, "reported_unix": 1990.0,
        "readahead_bytes": 1 * GIB,
        "landing_bytes_per_s": PHASE_GIB * GIB / MOVER_SECONDS,
        "report_latency_s": 90.0, "fill_supply_mb_s": None}
    arguments.update(overrides)
    return residency_plan.refill_horizon(plan, phase,        # type: ignore[arg-type]
                                         **arguments)  # type: ignore[arg-type]


def test_the_horizon_is_the_docstring_arithmetic(tmp_path: Path) -> None:
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    horizon = _horizon(queue)
    assert horizon is not None
    assert horizon["consumption_basis"] == "measured"
    assert horizon["consumption_bytes_per_s"] == pytest.approx(2 * GIB / 990.0)
    assert horizon["landing_s"] == pytest.approx(MOVER_SECONDS)
    assert horizon["latency_s"] == pytest.approx(90.0 + MOVER_SECONDS)
    assert horizon["reach_end_bytes"] == 3 * GIB
    assert horizon["horizon_end_bytes"] == ADVANCE * PHASE_GIB * GIB
    assert horizon["advance"] == _mover("reader", ADVANCE)
    assert [leg["phase"] for leg in horizon["beyond"]] == [   # type: ignore[union-attr]
        f"phase-{ordinal}" for ordinal in BEYOND]


def test_before_a_rate_is_measured_the_fill_supply_prices_consumption(
        tmp_path: Path) -> None:
    """No report after the claim: the tier's fill supply stands in, and a
    faster consumer's refill covers more ranges, never fewer."""

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    horizon = _horizon(queue, reported_unix=1000.0, fill_supply_mb_s=144.0)
    assert horizon is not None
    assert horizon["consumption_basis"] == "fill-supply"
    # 144 MB/s over 290 s is 41.8 GB: every remaining phase is inside.
    assert horizon["horizon_end_bytes"] is None
    assert horizon["beyond"] == []


@pytest.mark.parametrize("override", [
    {"phase": None}, {"readahead_bytes": None}, {"landing_bytes_per_s": None},
    {"landing_bytes_per_s": 0.0},
    {"reported_unix": 1000.0},          # no measured rate and no fill supply
])
def test_an_undefined_horizon_leaves_the_window_as_it_was(
        tmp_path: Path, override: dict[str, object]) -> None:
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    assert _horizon(queue, **override) is None
