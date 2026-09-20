"""A later artifact takes over bytes that are already on the stage (#598).

The campaign is one probe and many artifacts of one model, so every artifact
reads the same shards.  Measured on 2026-09-18: a run stage filled 731.5 GB
across 11 movers, its consumer failed on an application defect at 13:50Z, the
orphan sweep had deleted all of it by 14:17Z, and the resubmission of the same
manifest had to copy every byte again.  Rob: *"We should not be rerunning
anything in bulk if avoidable."*

Two changes, one rule.  A resident range a live consumer's window names is
**adopted** -- the tokens move from the finished mover's key to the
successor's, nothing is copied -- and a range nobody names stays resident until
a window cannot be placed without the room.  Both say the same thing: an
orphan is evicted when the tier needs its tokens, never because a clock said
so.

What must not change is the accounting, and it is what every case here
asserts after every operation: **held tier tokens equal the ranges the stage's
fragments account for, at every instant.**  Retained-but-unpinned bytes were
refused for this reason and adoption does not reintroduce them -- the tokens
are held throughout, by one key or the other, and at no point by neither.
"""
from __future__ import annotations

from contextlib import contextmanager
import json
from pathlib import Path
import sys
import threading
import time

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
from prismabuild import pool, residency_map, residency_plan, storage_tiers  # noqa: E402
import stage_release  # noqa: E402
import tier_loop  # noqa: E402

TIER = "prismabuild-stage:dl380g10"
STAGE_KIND = f"stage_gib@{TIER}"
MANIFEST = "9" * 64
DIGEST = "a" * 64
GIB = storage_tiers.GIB
#: Two GiB a phase against a five GiB stage: two phases fit and a third does
#: not, which is what makes "the tier needs the tokens" a state a test can
#: reach without inventing a threshold.
PHASE_GIB = 2
STAGE_GIB = 5

FIRST = "1" * 64        # the consumer that staged the range and then failed
SECOND = "2" * 64       # the consumer that wants the same bytes


def _hexkey(seed: str) -> str:
    """A distinct 64-hex action key per name, without hashing a real body."""

    return (seed.encode().hex() * 64)[:64]


def _row(queue: pool.PoolQueue, key: str,
         resources: dict[str, int]) -> dict[str, object]:
    return {"action_key": key, "cas_root": str(queue.root / "cas"),
            "checkout_root": str(queue.root / "co"),
            "worker_script": str(queue.root / "worker.py"),
            "tags": ["dl380g10"], "resources": resources}


def _plan(queue: pool.PoolQueue, consumer: str, *, phases: int = 2,
          label: str = "") -> dict[str, object]:
    """One consumer's frozen plan over the shared manifest.

    ``label`` is what makes two consumers' mover keys differ, which is what the
    real seal does: a mover's argv carries ``--consumer-action-key``, so the
    same range under two consumers is two action keys and the same residency
    descriptor.  That gap is the whole reason adoption is a transfer rather
    than a no-op.
    """

    built = []
    for ordinal in range(phases):
        start, end = ordinal * PHASE_GIB * GIB, (ordinal + 1) * PHASE_GIB * GIB
        built.append({
            "name": f"phase-{ordinal}",
            "start_bytes": start, "end_bytes": end, "stage_gib": PHASE_GIB,
            "mover_row": {
                **_row(queue, _hexkey(f"{label}mover{ordinal}"),
                       {STAGE_KIND: PHASE_GIB, "cpu": 1, "mem_gb": 1}),
                "residency": {
                    "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                    "manifest_sha256": MANIFEST, "manifest_bytes": 1 << 30,
                    "range_start_bytes": start, "range_end_bytes": end},
            },
            "egress_row": _row(queue, _hexkey(f"{label}egress{ordinal}"),
                               {"mem_gb": 1}),
        })
    return residency_plan.build_plan(
        consumer_action_key=consumer, tier_id=TIER, stage_root="/stage/prewarm",
        manifest_sha256=MANIFEST, manifest_bytes=1 << 30, phases=built)


def _publish_consumer(queue: pool.PoolQueue, consumer: str,
                      plan: dict[str, object]) -> None:
    residency_plan.freeze(queue, plan)
    queue.publish(
        action_key=consumer, cas_root=queue.root / "cas",
        checkout_root=queue.root / "co", worker_script=queue.root / "worker.py",
        resources={"cpu": 1, "mem_gb": 1},
        residency={"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                   "manifest_sha256": MANIFEST, "manifest_bytes": 1 << 30,
                   "leads": residency_plan.leads_for(plan)})


def _stage_range(queue: pool.PoolQueue, *, mover: str, consumer: str,
                 stage: Path, ordinal: int = 0, files: int = 2,
                 manifest: str = MANIFEST) -> list[Path]:
    """Drive the ledger and the stage into the state a finished mover leaves.

    Tokens held, files on the device, a fragment naming them and a receipt
    saying the copy completed -- the four things every reader downstream reads,
    filed the way ``stage_move`` files them.
    """

    start, end = ordinal * PHASE_GIB * GIB, (ordinal + 1) * PHASE_GIB * GIB
    entries: dict[str, object] = {}
    written: list[Path] = []
    for index in range(files):
        path = stage / manifest[:8] / f"phase-{ordinal}" / f"part-{index}.bin"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"x" * 32)
        written.append(path)
        entries[residency_map.residency_map_key(
            f"/pool/{manifest[:8]}/phase-{ordinal}/part-{index}.bin", 0)] = {
                "stage_path": str(path), "bytes": 32, "offset": 0,
                "sha256": DIGEST}
    assert queue.tier_ledger(TIER).acquire(mover, {"stage_gib": PHASE_GIB})
    residency_map.write_fragment(queue.residency_fragment_root(), {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": consumer, "mover_action_key": mover,
        "tier_id": TIER, "stage_root": str(stage), "manifest_sha256": manifest,
        "entries": entries})
    queue.record_move(mover, {
        "consumer_action_key": consumer, "tier_id": TIER,
        "stage_root": str(stage), "manifest_sha256": manifest,
        "range_start_bytes": start, "range_end_bytes": end,
        "range_bytes": end - start, "bytes_staged": end - start,
        "entries_declared": files, "entries_staged": files,
        "complete": True, "seconds": 1.0, "unix": 1000.0 + ordinal})
    return written


def _claim_with_progress(queue: pool.PoolQueue, consumer: str, *,
                         phase: str) -> None:
    """Move a consumer into ``claimed`` and give it an accepted phase.

    The lease, not the action-writable channel: the storage role reads the
    worker's already authenticated observation, which is the only one it can
    trust, and ``prewarm_loop.progress_phase`` checks the claim stamp against
    it.  Written the way ``tests/prewarm_fixture.py`` writes it.
    """

    source = queue.item_path(pool.READY, consumer)
    item = json.loads(source.read_text())
    source.unlink()
    item.update({"action_key": consumer, "claimed_unix": time.time(),
                 "claimed_by": "adoption-fixture", "claimed_host": "dl380g10"})
    queue.item_path(pool.CLAIMED, consumer).write_text(json.dumps(item))
    queue.write_lease(
        consumer, owner="adoption-fixture", claim_snapshot=item,
        progress_observation={
            "source": "action-progress",
            "last_accepted": {"phase": phase, "units_completed": 1,
                              "reported_unix": time.time()}})


def _claim_row(queue: pool.PoolQueue, key: str) -> None:
    """Move one published mover row into ``claimed``: a live reader.

    The live half of a landed range -- the state a claimed mover that
    finished its copy keeps until its consumer reads past it.
    """

    source = queue.item_path(pool.READY, key)
    item = json.loads(source.read_text())
    source.unlink()
    item.update({"action_key": key, "claimed_unix": time.time(),
                 "claimed_by": "adoption-fixture", "claimed_host": "dl380g10"})
    queue.item_path(pool.CLAIMED, key).write_text(json.dumps(item))


@contextmanager
def _lock_held_elsewhere(queue: pool.PoolQueue, mover: str):
    """Hold one mover's transition lock from another thread, as an egress does.

    ``posix_lock.held`` nests within a thread on purpose -- one descriptor per
    inode per process -- so a ``with`` in the test's own thread would grant the
    adoption the very lock it is supposed to be excluded by.  The exclusion is
    between two parties, and this is the cheapest honest second party.
    """

    taken, release = threading.Event(), threading.Event()

    def hold() -> None:
        with queue.mover_transition_lock(mover):
            taken.set()
            release.wait(30)

    thread = threading.Thread(target=hold, daemon=True)
    thread.start()
    assert taken.wait(30), "the holder never took the lock"
    try:
        yield
    finally:
        release.set()
        thread.join(30)


def _tier_record(stage: Path) -> dict[str, object]:
    return {"schema": storage_tiers.TIER_RECORD_SCHEMA_V1, "tier_id": TIER,
            "host": "dl380g10", "tier": "stage", "mountpoint": str(stage),
            "capacity_bytes": STAGE_GIB * GIB}


def _cycle(queue: pool.PoolQueue, stage: Path) -> None:
    """One whole tier cycle, the way the loop on the storage box runs it."""

    tier_loop.cycle(queue, host="dl380g10", source_pool="storage_pool",
                    receipts=tier_loop.ReceiptCache(),
                    discover=lambda **_kwargs: {TIER: _tier_record(stage)})


# ------------------------------------------------------------- the invariant


def assert_ledger_matches_the_stage(queue: pool.PoolQueue) -> None:
    """Held tier tokens equal the ranges the stage's fragments account for.

    Both directions, because each one alone is half the accounting.  Tokens
    held for bytes that are gone are capacity nobody can use; bytes with no
    tokens behind them are the overfill the reservation exists to prevent, and
    they are what a "retain the prefix unpinned" design would have left.

    The token count is compared against the *range* a mover's receipt declares
    rather than against the bytes of the fixture's files, because that is what
    a token stands for: ``residency_demand`` derives it from the manifest's
    read order and a fragment is what says those bytes arrived.  Read straight
    off the receipt rather than through ``PoolQueue.staged_range_of``, so this
    check does not depend on the code it is checking.
    """

    ledger = queue.tier_ledger(TIER)
    held = {key: ledger.holder_tokens(key).get("stage_gib", 0)
            for key in ledger.held_keys()}
    held = {key: gib for key, gib in held.items() if gib}
    # An advance fence holds tokens with no bytes yet, by design (the
    # window lane's protected next): it is reserved-but-unwritten credit,
    # not a ledger/stage disagreement.  Fence holders are exactly holders
    # with a live funding record and no move receipt -- a landed mover
    # always has its receipt and stays compared.
    fenced = {key for key in held
              if (record := queue.read_funding(key, TIER)) is not None
              and record.get("state") in ("reserved", "transferring")
              and queue.move_record(key) is None}
    held = {key: gib for key, gib in held.items() if key not in fenced}
    root = queue.residency_fragment_root()
    accounted: dict[str, int] = {}
    consumers = sorted(entry.name for entry in root.iterdir() if entry.is_dir())
    for consumer in consumers:
        for fragment in residency_map.read_fragments(root, consumer):
            mover = str(fragment["mover_action_key"])
            for entry in dict(fragment["entries"]).values():
                assert Path(str(entry["stage_path"])).exists(), (
                    f"{mover[:8]} vouches for {entry['stage_path']}, which is gone")
            receipt = queue.move_record(mover)
            assert receipt is not None, f"{mover[:8]} has a fragment and no receipt"
            accounted[mover] = storage_tiers.stage_tokens_for_bytes(
                int(receipt["range_end_bytes"]) - int(receipt["range_start_bytes"]))
    assert held == accounted, (
        "held tier tokens and the bytes on the stage disagree: "
        f"held={held} accounted={accounted}")


@pytest.fixture()
def queue(tmp_path: Path) -> pool.PoolQueue:
    q = pool.PoolQueue(tmp_path / "pb-queue")
    q.ensure_layout()
    q.mint_tier_capacity(TIER, {"stage_gib": STAGE_GIB})
    return q


@pytest.fixture()
def stage(tmp_path: Path, queue: pool.PoolQueue) -> Path:
    path = tmp_path / "stage"
    path.mkdir()
    # The fleet's loop registers its own stage on every cycle; a test that
    # sweeps without a cycle first is standing in for that loop (#628).
    stage_release.register_stage_root(queue, tier_id=TIER, stage_root=path)
    return path


# --------------------------------------------------- the ledger's own half


def test_a_transfer_never_lets_the_tier_read_a_token_as_free(queue) -> None:
    """The reason adoption is not release-then-reacquire.

    Between a release and the matching acquire the ledger reads capacity it
    does not have, and whatever is admitted in that window lands on a stage
    that is already full.  A transfer has no such window: every token is under
    ``held/`` at every instant, so the three numbers a claimant reads are the
    same before and after.
    """

    ledger = queue.tier_ledger(TIER)
    assert ledger.acquire("a" * 64, {"stage_gib": 2})
    capacity, available = ledger.capacity(), ledger.available()

    moved = ledger.transfer("a" * 64, "b" * 64)

    assert moved == 2
    assert ledger.holder_tokens("a" * 64) == {}
    assert ledger.holder_tokens("b" * 64) == {"stage_gib": 2}
    assert ledger.capacity() == capacity and ledger.available() == available


def test_a_transfer_of_a_holder_that_holds_nothing_moves_nothing(queue) -> None:
    """The idempotent half: a second attempt after a completed one is a no-op."""

    ledger = queue.tier_ledger(TIER)
    assert ledger.acquire("a" * 64, {"stage_gib": 2})
    assert ledger.transfer("a" * 64, "b" * 64) == 2

    assert ledger.transfer("a" * 64, "b" * 64) == 0
    assert ledger.holder_tokens("b" * 64) == {"stage_gib": 2}


def test_a_transfer_refuses_a_claimant_private_acquisition(queue) -> None:
    """Those are named for a claimant, whose action is not decided yet."""

    ledger = queue.tier_ledger(TIER)
    handle = ledger.begin_acquire("a" * 64, {"stage_gib": 1})
    assert handle is not None

    with pytest.raises(pool.PoolContractError, match="never a claimant-private"):
        ledger.transfer(handle, "b" * 64)


# ------------------------------------------------ (a) and (b): the adoption


def test_a_successor_adopts_the_resident_range_instead_of_copying_it(
        queue, stage) -> None:
    """The measured case: a consumer fails, its retry must not re-copy 731 GB.

    RED before #598: the orphan sweep deletes the first consumer's range on the
    cycle after it goes terminal, and the window then publishes the successor's
    own mover to copy the same bytes again.
    """

    first_mover = _hexkey("firstmover0")
    staged_files = _stage_range(queue, mover=first_mover, consumer=FIRST,
                               stage=stage)
    assert_ledger_matches_the_stage(queue)
    # The first consumer is terminal: nothing in ready or claimed names it.
    second = _plan(queue, SECOND, label="second")
    _publish_consumer(queue, SECOND, second)
    second_mover = _hexkey("secondmover0")
    assert first_mover != second_mover, "two consumers seal two mover keys"

    _cycle(queue, stage)

    assert_ledger_matches_the_stage(queue)
    ledger = queue.tier_ledger(TIER)
    assert ledger.holder_tokens(second_mover) == {"stage_gib": PHASE_GIB}
    assert ledger.holder_tokens(first_mover) == {}
    # No copy was queued, and no byte moved.
    assert not queue.item_path(pool.READY, second_mover).exists()
    assert all(path.exists() for path in staged_files)
    receipt = queue.move_record(second_mover)
    assert receipt is not None
    assert receipt[pool.MOVE_ADOPTED_FROM_FIELD] == first_mover
    assert receipt["bytes_copied"] == 0
    assert receipt["bytes_staged"] == PHASE_GIB * GIB


def test_the_adopted_range_admits_the_consumer_that_took_it_over(
        queue, stage) -> None:
    """An adopted mover never runs, so the gate cannot read a terminal record.

    What it reads instead is the receipt that says the range was taken over and
    the ledger that says it is still held -- the same two facts the
    ``executed`` branch is really checking.
    """

    _stage_range(queue, mover=_hexkey("firstmover0"), consumer=FIRST, stage=stage)
    _publish_consumer(queue, SECOND, _plan(queue, SECOND, label="second"))

    _cycle(queue, stage)

    assert_ledger_matches_the_stage(queue)
    second_mover = _hexkey("secondmover0")
    assert not queue.item_path(pool.DONE, second_mover).exists()
    claimed = queue.claim(capacity={"cpu": 4, "mem_gb": 8}, tags=["dl380g10"])
    assert claimed is not None and claimed["action_key"] == SECOND
    assert claimed["residency_verdict"]["state"] == "resident"
    # And the map it reads names the files the first consumer's copy verified.
    composed = residency_map.read_map(queue.residency_map_path(SECOND))
    assert composed["leads"] == [second_mover]
    assert set(composed["entries"]) == {
        residency_map.residency_map_key(f"/pool/{MANIFEST[:8]}/phase-0/part-0.bin", 0),
        residency_map.residency_map_key(f"/pool/{MANIFEST[:8]}/phase-0/part-1.bin", 0)}


def test_an_adopted_lead_whose_bytes_went_is_refused_again(queue, stage) -> None:
    """Residency can be lost as well as gained, adopted or copied.

    The denial is ``absent`` rather than ``unpinned``, and that is the useful
    one: an adopted mover files no terminal record, so once its tokens go it is
    neither pinned nor queued -- which is exactly what ``_mover_state`` reads as
    unpublished, so the window stages it again.  ``unpinned`` would say the
    opposite, that nothing will happen without a republication.
    """

    _stage_range(queue, mover=_hexkey("firstmover0"), consumer=FIRST, stage=stage)
    _publish_consumer(queue, SECOND, _plan(queue, SECOND, label="second"))
    _cycle(queue, stage)

    queue.release_tier_reservations(_hexkey("secondmover0"))

    verdict = queue.residency_verdict(
        pool._read_json(queue.item_path(pool.READY, SECOND)))
    assert verdict["state"] == "lead_not_resident"
    assert verdict["pending"] == [
        {"lead": _hexkey("secondmover0"), "status": "absent"}]
    # ...and the window does stage it again rather than waiting for ever.
    decision = residency_plan.window(
        residency_plan.read(queue, SECOND), accepted_phase=None,
        free_gib=STAGE_GIB, published=[], staged=[])
    assert decision["publish"][0]["phase"] == "phase-0"


def test_a_range_the_successor_has_already_read_past_is_not_adopted(
        queue, stage) -> None:
    """Taking it over would pin bytes the consumer will never open again.

    Its accepted phase is what says so, and it is the same reading the window
    evicts by: the phase a consumer names is the one it is *inside*, so
    everything before it is finished with.
    """

    _stage_range(queue, mover=_hexkey("firstmover0"), consumer=FIRST, stage=stage)
    _publish_consumer(queue, SECOND, _plan(queue, SECOND, label="second"))
    _claim_with_progress(queue, SECOND, phase="phase-1")

    events = tier_loop.adopt_resident_ranges(
        queue, tiers={TIER: _tier_record(stage)})

    assert events == []
    assert queue.tier_ledger(TIER).holder_tokens(
        _hexkey("firstmover0")) == {"stage_gib": PHASE_GIB}
    assert queue.tier_ledger(TIER).holder_tokens(_hexkey("secondmover0")) == {}
    assert_ledger_matches_the_stage(queue)


# ----------------------------------------- (c): pressure still frees a range


def test_a_range_nobody_adopted_stays_resident_while_nothing_needs_the_room(
        queue, stage) -> None:
    """The deferral, and why it is not the retained-unpinned design #598 refused.

    The bytes stay and so do the tokens: the ledger counts every resident byte
    at every instant, so nothing can be admitted onto capacity that is not
    there.  What is deferred is the deletion, and only until a window cannot be
    placed without the room.
    """

    files = _stage_range(queue, mover=_hexkey("firstmover0"), consumer=FIRST,
                         stage=stage)

    _cycle(queue, stage)

    assert_ledger_matches_the_stage(queue)
    assert queue.tier_ledger(TIER).holder_tokens(
        _hexkey("firstmover0")) == {"stage_gib": PHASE_GIB}
    assert all(path.exists() for path in files)


def test_an_orphan_is_evicted_when_a_window_cannot_be_placed_without_it(
        queue, stage) -> None:
    """"The tier needs the tokens" is measured off the window, not off a clock.

    Four of the five GiB are orphaned under two finished movers of another
    manifest, and the new consumer's window is current-plus-next (2+2
    GiB): it fits only once the orphans are taken back, and the joint gate
    that protects the next step is also what the relief answers to -- the
    sweep reclaims what ADMISSION needs, not just the first phase, and
    the window publishes in the same cycle.
    """

    other = "8" * 64
    for ordinal in (0, 1):
        _stage_range(queue, mover=_hexkey(f"stalemover{ordinal}"),
                     consumer=other, stage=stage, ordinal=ordinal,
                     manifest="e" * 64)
    assert queue.tier_ledger(TIER).available()["stage_gib"] == 1
    # Another model's shards: the descriptor differs, so none of it is this
    # consumer's to adopt and the room has to be taken back.
    _publish_consumer(queue, SECOND, _plan(queue, SECOND, label="second"))

    _cycle(queue, stage)

    assert_ledger_matches_the_stage(queue)
    # Admission under the cur+next contract needs 4 GiB against 1 free, so
    # BOTH orphan ranges go -- the oldest first, and the stage is not
    # emptied on principle: what stays held afterwards is exactly the
    # window's protected next (its advance fence), nothing else.
    assert queue.tier_ledger(TIER).holder_tokens(_hexkey("stalemover0")) == {}
    assert queue.tier_ledger(TIER).holder_tokens(
        _hexkey("stalemover1")) == {}
    free = queue.tier_ledger(TIER).available()["stage_gib"]
    assert free == STAGE_GIB - PHASE_GIB, free
    # Useful progress, not just reclamation: the admitted window publishes
    # its first mover and protects its next.
    assert queue.item_path(pool.READY, _hexkey("secondmover0")).exists()
    record = queue.read_funding(_hexkey("secondmover1"), TIER)
    assert record is not None and record["state"] in (
        "reserved", "transferring"), record


def test_an_unfittable_newcomer_adds_no_admission_pressure(
        queue, stage) -> None:
    """No futile relief: a window the orphans cannot fit asks for none.

    A live reader's protected bytes sit beside the orphan, so even taking
    every orphan back leaves the joint gate refusing (2 live + 4 window
    against 5).  The admission probe must contribute NOTHING beyond the
    ordinary next-phase term -- evicting for it would empty room nobody
    can use -- and the live reader's bytes and tokens survive the cycle
    regardless.
    """

    live_files = _stage_range(queue, mover=_hexkey("firstmover0"),
                              consumer=FIRST, stage=stage, ordinal=0)
    _stage_range(queue, mover=_hexkey("stalemover0"),
                 consumer="8" * 64, stage=stage, ordinal=1,
                 manifest="e" * 64)
    # One phase: FIRST is final once its single range lands, so its own
    # protected next is empty and SECOND's admission turns on the orphan.
    _publish_consumer(queue, FIRST, _plan(queue, FIRST, label="first",
                                          phases=1))
    _claim_with_progress(queue, FIRST, phase="phase-0")
    _publish_consumer(queue, SECOND, _plan(queue, SECOND, label="second"))
    assert queue.tier_ledger(TIER).available()["stage_gib"] == 1

    # The probe itself, on the actual function: only the ordinary
    # next-phase term (one phase's GiB), never free+shortfall (1+3=4).
    pressure = tier_loop.window_pressure(
        queue, tiers={TIER: _tier_record(stage)})
    assert pressure.get(TIER) == PHASE_GIB, pressure

    _cycle(queue, stage)

    # The live reader is never the relief: its bytes and tokens stay.
    assert queue.tier_ledger(TIER).holder_tokens(_hexkey("firstmover0")) == {
        "stage_gib": PHASE_GIB}
    assert all(path.exists() for path in live_files)
    # And a window that still cannot be placed publishes nothing.
    assert not queue.item_path(pool.READY, _hexkey("secondmover0")).exists()
    assert_ledger_matches_the_stage(queue)


def test_a_feasible_newcomers_relief_takes_the_orphan_never_the_live_reader(
        tmp_path: Path) -> None:
    """Relief is bounded to orphans: the live reader's bytes survive it.

    Capacity 6: a claimed consumer's landed range (2, protected) beside an
    orphan (2), free 2, and a newcomer's 2+2 window.  The admission
    shortfall is exactly the orphan, the sweep takes it alone, and the
    newcomer publishes in the same cycle.
    """

    q = pool.PoolQueue(tmp_path / "pb-queue")
    q.ensure_layout()
    q.mint_tier_capacity(TIER, {"stage_gib": 6})
    stage = tmp_path / "stage"
    stage.mkdir()
    stage_release.register_stage_root(q, tier_id=TIER, stage_root=stage)
    live_files = _stage_range(q, mover=_hexkey("firstmover0"),
                              consumer=FIRST, stage=stage, ordinal=0)
    orphan_files = _stage_range(q, mover=_hexkey("stalemover0"),
                                consumer="8" * 64, stage=stage, ordinal=1,
                                manifest="e" * 64)
    # FIRST's own window is live and landed: its lead's row is published
    # and claimed (a ready row would double-count in the gate's queued
    # term; a claimed one is simply a live reader holding its bytes), and
    # its plan is final so no protected next of its own is in play.
    first_plan = _plan(q, FIRST, label="first", phases=1)
    residency_plan.freeze(q, first_plan)
    lead_row = first_plan["phases"][0]["mover_row"]  # type: ignore[index]
    q.publish(action_key=_hexkey("firstmover0"),
              cas_root=lead_row["cas_root"],  # type: ignore[arg-type]
              checkout_root=lead_row["checkout_root"],  # type: ignore[arg-type]
              worker_script=lead_row["worker_script"],  # type: ignore[arg-type]
              tags=["dl380g10"], resources=lead_row["resources"],  # type: ignore[arg-type]
              residency=lead_row["residency"])  # type: ignore[arg-type]
    _claim_row(q, _hexkey("firstmover0"))
    _publish_consumer(q, SECOND, _plan(q, SECOND, label="second"))
    assert q.tier_ledger(TIER).available()["stage_gib"] == 2

    # The actual probe: free (2) + the admission shortfall (2).
    pressure = tier_loop.window_pressure(
        q, tiers={TIER: _tier_record(stage)})
    assert pressure.get(TIER) == 4, pressure

    _cycle(q, stage)

    assert q.tier_ledger(TIER).holder_tokens(_hexkey("firstmover0")) == {
        "stage_gib": PHASE_GIB}
    assert all(path.exists() for path in live_files)
    assert q.tier_ledger(TIER).holder_tokens(_hexkey("stalemover0")) == {}
    assert not any(path.exists() for path in orphan_files)
    assert q.item_path(pool.READY, _hexkey("secondmover0")).exists()
    record = q.read_funding(_hexkey("secondmover1"), TIER)
    assert record is not None and record["state"] in (
        "reserved", "transferring"), record
    assert_ledger_matches_the_stage(q)


def test_a_direct_sweep_with_no_pressure_named_still_takes_every_orphan(
        queue, stage) -> None:
    """An operator, or a caller that is not the loop, means all of them."""

    _stage_range(queue, mover=_hexkey("firstmover0"), consumer=FIRST, stage=stage)

    swept = stage_release.sweep(queue, stage_roots={TIER: str(stage)})

    assert [record["reason"] for record in swept] == ["orphan-sweep"]
    assert queue.tier_ledger(TIER).holder_tokens(_hexkey("firstmover0")) == {}
    assert_ledger_matches_the_stage(queue)


# --------------------------------- (d): the old key can no longer take it back


def test_an_egress_of_the_old_key_never_deletes_an_adopted_range(
        queue, stage) -> None:
    """The first consumer's egress row may be claimed long after the adoption.

    It must find nothing of its own: the fragment that named these files moved
    to the successor with the tokens, so the egress deletes no byte and
    releases no token.  The other outcome -- an egress that deletes bytes a
    live consumer now holds tokens for -- is the one this whole ordering
    exists to make unreachable.
    """

    first_mover = _hexkey("firstmover0")
    files = _stage_range(queue, mover=first_mover, consumer=FIRST, stage=stage)
    _publish_consumer(queue, SECOND, _plan(queue, SECOND, label="second"))
    _cycle(queue, stage)
    second_mover = _hexkey("secondmover0")
    assert queue.tier_ledger(TIER).holder_tokens(second_mover)

    receipt = stage_release.evict(queue, first_mover,
                                  consumer_action_key=FIRST,
                                  stage_root=str(stage))

    assert receipt["complete"] is True
    assert receipt["entries_deleted"] == 0 and receipt["tokens_released"] == 0
    assert all(path.exists() for path in files)
    assert queue.tier_ledger(TIER).holder_tokens(
        second_mover) == {"stage_gib": PHASE_GIB}
    assert_ledger_matches_the_stage(queue)


def test_an_adoption_declines_while_the_range_is_being_evicted(
        queue, stage) -> None:
    """Exclusion, not ordering: neither order of the two is safe on its own.

    Holding the mover's transition lock is what an egress mid-delete looks
    like from here, and declining costs the successor a copy rather than its
    bytes.
    """

    first_mover = _hexkey("firstmover0")
    _stage_range(queue, mover=first_mover, consumer=FIRST, stage=stage)
    _publish_consumer(queue, SECOND, _plan(queue, SECOND, label="second"))

    # Another thread, because the lock nests within one: an egress is another
    # process, and a same-thread ``with`` would have the adoption walk straight
    # through the exclusion it is being tested for.
    with _lock_held_elsewhere(queue, first_mover):
        events = tier_loop.adopt_resident_ranges(
            queue, tiers={TIER: _tier_record(stage)})

    assert [event["reason"] for event in events] == ["range_busy"]
    assert queue.tier_ledger(TIER).holder_tokens(
        first_mover) == {"stage_gib": PHASE_GIB}
    assert queue.tier_ledger(TIER).holder_tokens(_hexkey("secondmover0")) == {}
    assert_ledger_matches_the_stage(queue)


def test_a_range_a_running_consumer_still_names_is_never_adopted(
        queue, stage) -> None:
    """Only a finished consumer's range is anybody else's to take.

    That is the distinction #598 said was missing, and it is read off the
    queue's live state: a plan in ``ready`` or ``claimed`` names its movers,
    and a mover a live plan names is out of the adoptable set whatever its
    phase.
    """

    first = _plan(queue, FIRST, label="first")
    _publish_consumer(queue, FIRST, first)
    first_mover = _hexkey("firstmover0")
    _stage_range(queue, mover=first_mover, consumer=FIRST, stage=stage)
    _publish_consumer(queue, SECOND, _plan(queue, SECOND, label="second"))

    events = tier_loop.adopt_resident_ranges(
        queue, tiers={TIER: _tier_record(stage)})

    assert events == []
    assert queue.tier_ledger(TIER).holder_tokens(
        first_mover) == {"stage_gib": PHASE_GIB}
    assert_ledger_matches_the_stage(queue)


def test_a_range_of_another_manifest_is_not_adopted(queue, stage) -> None:
    """The descriptor is the identity, and the manifest digest is half of it."""

    first_mover = _hexkey("firstmover0")
    _stage_range(queue, mover=first_mover, consumer=FIRST, stage=stage)
    # Drive the receipt, not the plan: the range is the same bytes of a
    # different list, which is a different range.
    record = dict(queue.move_record(first_mover) or {})
    record["manifest_sha256"] = "b" * 64
    queue.record_move(first_mover, record)
    _publish_consumer(queue, SECOND, _plan(queue, SECOND, label="second"))

    assert tier_loop.adopt_resident_ranges(
        queue, tiers={TIER: _tier_record(stage)}) == []
    assert queue.tier_ledger(TIER).holder_tokens(
        first_mover) == {"stage_gib": PHASE_GIB}


def test_a_range_whose_fragment_cannot_be_read_is_not_adopted(
        queue, stage) -> None:
    """A range nobody can name is not one to take over; the same refusal ``evict`` makes."""

    first_mover = _hexkey("firstmover0")
    _stage_range(queue, mover=first_mover, consumer=FIRST, stage=stage)
    residency_map.fragment_path(queue.residency_fragment_root(), FIRST,
                                first_mover).write_text("{not json")
    _publish_consumer(queue, SECOND, _plan(queue, SECOND, label="second"))

    events = tier_loop.adopt_resident_ranges(
        queue, tiers={TIER: _tier_record(stage)})

    assert [event["reason"] for event in events] == ["range_not_named"]
    assert queue.tier_ledger(TIER).holder_tokens(
        first_mover) == {"stage_gib": PHASE_GIB}
    assert queue.tier_ledger(TIER).holder_tokens(_hexkey("secondmover0")) == {}
