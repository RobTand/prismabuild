"""The ram window promotes only to its consumer's refill horizon (#906).

#903 bounded the stage window by the consumer's refill horizon: the phase it
is reading, the bytes it can hold ahead of it, and enough further ranges to
cover the time a copy published now takes to land.  The ram window kept its
own bounds only: the #633 run-ahead (capacity minus one step) and the ram
policy's optional ``prefill_depth``.  A consumer with a small reservation
could therefore promote as far ahead as the tmpfs had room, and no other
consumer's promotion could take any of it back, because every range it held
belonged to a live plan.

This applies the same horizon to the ram window, priced from the plan's own
promotions: the slowest complete promotion receipt is the landing rate.
Ranges past the horizon are given back farthest-needed first when another
window needs the room.  A stage range given back past its horizon takes its
ram copy with it, ram first, because a ram range whose stage source is gone
is one the consumer's map can no longer read (#640).

A ram horizon that comes out short costs less than a short stage horizon: the
consumer reads the range from the stage while its ram copy is not there yet.
That is slower, but it is not a stall.  So the ram horizon uses the ram
landing rate alone, and with no promotion measured yet it is undefined and
the window keeps its #633 bound.

The numbers match ``test_a_consumer_stages_only_to_its_refill_horizon.py``:

* 8 phases of 2 GiB.  Each phase has a stage leg and a ram leg, and the
  reader is inside ``phase-0``;
* each landed copy took 200 s, so a copy lands at 2 GiB / 200 s = 10.7 MB/s;
* the reader was claimed 1000 s ago and reported ``phase-0`` 10 s ago, so it
  reads at most 2 GiB in 990 s, or 2.2 MB/s;
* it reserves ``mem_gb`` 1, so it can hold 1 GiB ahead of what it reads.

Its horizon is ``phase-0`` to ``phase-2``.  ``phase-3`` is the advance the
window publishes next, and ``phase-4`` to ``phase-7`` are past the horizon.

Everything runs on ``tmp_path`` queues, stage roots and ram roots.  Nothing
touches a live queue, ``/stage/prewarm`` or ``/ram/prewarm`` (#628).
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
    _tier_record)
from test_a_consumer_stages_only_to_its_refill_horizon import (  # noqa: E402
    CLAIMED_AGO_S, MOVER_SECONDS, REPORTED_AGO_S, _claim, _claim_shortage,
    _newcomer)

RAM_TIER = "ram:dl380g10"
RAM_KIND = f"ram_gib@{RAM_TIER}"
READER = "7" * 64
PROMOTER = "8" * 64     # a second consumer that wants the tmpfs
READER_MANIFEST = "a" * 64
PROMOTER_MANIFEST = "b" * 64
PHASES = 8
#: Phases inside the horizon, the advance, and past the horizon, by the
#: arithmetic in the module docstring.
IN_HORIZON = (0, 1, 2)
ADVANCE = 3
BEYOND = (4, 5, 6, 7)


def _mover(label: str, ordinal: int) -> str:
    return _hexkey(f"{label}mover{ordinal}")


def _promotion(label: str, ordinal: int) -> str:
    return _hexkey(f"{label}promote{ordinal}")


def _plan(queue: pool.PoolQueue, consumer: str, *, label: str,
          manifest: str, stage: Path,
          phases: int = PHASES) -> dict[str, object]:
    """A frozen plan whose every phase has a stage leg and a ram leg.

    Its stage root is the fixture's own: a promotion publishes only behind a
    stage range whose fragment names the plan's stage root.
    """

    total = phases * PHASE_GIB * GIB
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
                    "manifest_sha256": manifest, "manifest_bytes": total,
                    "range_start_bytes": start, "range_end_bytes": end}},
            "egress_row": _row(queue, _hexkey(f"{label}egress{ordinal}"),
                               {"mem_gb": 1}),
            "ram_mover_row": {
                **_row(queue, _promotion(label, ordinal),
                       {RAM_KIND: PHASE_GIB, "cpu": 1, "mem_gb": 1}),
                "residency": {
                    "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": RAM_TIER,
                    "manifest_sha256": manifest, "manifest_bytes": total,
                    "range_start_bytes": start, "range_end_bytes": end}},
            "ram_egress_row": _row(queue, _hexkey(f"{label}ramrelease{ordinal}"),
                                   {"mem_gb": 1}),
        })
    return residency_plan.build_plan(
        consumer_action_key=consumer, tier_id=TIER, stage_root=str(stage),
        manifest_sha256=manifest, manifest_bytes=total, phases=built,
        ram_tier_id=RAM_TIER)


def _publish_consumer(queue: pool.PoolQueue, consumer: str,
                      plan: dict[str, object]) -> None:
    residency_plan.freeze(queue, plan)
    queue.publish(
        action_key=consumer, cas_root=queue.root / "cas",
        checkout_root=queue.root / "co", worker_script=queue.root / "worker.py",
        resources={"cpu": 1, "mem_gb": 1},
        residency={"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                   "manifest_sha256": str(plan["manifest_sha256"]),
                   "manifest_bytes": int(plan["manifest_bytes"]),
                   "leads": residency_plan.leads_for(plan)})


def _ram_range(queue: pool.PoolQueue, *, promotion: str, consumer: str,
               ram: Path, epoch: str, ordinal: int, manifest: str,
               files: int = 2, seconds: float = MOVER_SECONDS) -> list[Path]:
    """The state a finished promotion leaves, filed the way ``ram_promote`` does.

    Tokens held on the ram ledger, sparse files in the ram root under the
    staged names, a fragment and a sidecar dated with the ram epoch, and a
    complete receipt whose ``seconds`` is what the ram horizon prices.
    """

    start, end = ordinal * PHASE_GIB * GIB, (ordinal + 1) * PHASE_GIB * GIB
    entries: dict[str, dict[str, object]] = {}
    written: list[Path] = []
    share, remainder = divmod(end - start, files)
    for index in range(files):
        size = share + (remainder if index == files - 1 else 0)
        path = ram / manifest[:8] / f"phase-{ordinal}" / f"part-{index}.bin"
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "wb") as stream:
            stream.truncate(size)
        written.append(path)
        entries[residency_map.residency_map_key(
            f"/pool/{manifest[:8]}/phase-{ordinal}/part-{index}.bin", 0)] = {
                "stage_path": str(path), "bytes": size, "offset": 0,
                "sha256": DIGEST}
    assert queue.tier_ledger(RAM_TIER).acquire(promotion, {"ram_gib": PHASE_GIB})
    residency_map.write_fragment(queue.residency_fragment_root(), {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": consumer, "mover_action_key": promotion,
        "tier_id": RAM_TIER, "stage_root": str(ram), "manifest_sha256": manifest,
        "epoch": epoch, "entries": entries})
    reader_lease.write_material(
        queue.residency_fragment_root(), consumer_action_key=consumer,
        mover_action_key=promotion, tier_id=RAM_TIER, stage_root=str(ram),
        manifest_sha256=manifest, generation="b" * 32, epoch=epoch,
        entries={key: {**mention,
                       "file_id": reader_lease.stat_identity(
                           str(mention["stage_path"]))}
                 for key, mention in entries.items()})
    queue.record_move(promotion, {
        "consumer_action_key": consumer, "tier_id": RAM_TIER,
        "ram_root": str(ram), "manifest_sha256": manifest,
        "range_start_bytes": start, "range_end_bytes": end,
        "range_bytes": end - start, "bytes_staged": end - start,
        "entries_declared": files, "entries_staged": files,
        "complete": True, "epoch": epoch, "seconds": float(seconds),
        "unix": 1000.0 + ordinal})
    return written


class Box:
    """One ``tmp_path`` storage box: a queue, a stage root and a ram root."""

    def __init__(self, tmp_path: Path, *, stage_gib: int, ram_gib: int) -> None:
        self.queue = pool.PoolQueue(tmp_path / "pb-queue")
        self.queue.ensure_layout()
        self.stage = tmp_path / "stage"
        self.ram = tmp_path / "ram"
        self.stage.mkdir()
        self.ram.mkdir()
        stamped = storage_tiers.ensure_ram_epoch(self.ram, host="dl380g10")
        assert stamped is not None
        self.epoch = str(stamped["epoch"])
        self.stage_gib, self.ram_gib = stage_gib, ram_gib
        # The loop registers both roots on every cycle; a test that evicts
        # before its first cycle stands in for that loop (#628).
        for tier_id, root in ((TIER, self.stage), (RAM_TIER, self.ram)):
            assert stage_release.register_stage_root(
                self.queue, tier_id=tier_id, stage_root=root) == "registered"
        self.queue.mint_tier_capacity(TIER, {"stage_gib": stage_gib})
        self.queue.mint_tier_capacity(RAM_TIER, {"ram_gib": ram_gib})
        self.queue.announce_tier(self.ram_record())

    def ram_record(self) -> dict[str, object]:
        """The tmpfs as the loop announces it: writable plus landed is the tier."""

        held = int(self.queue.tier_ledger(RAM_TIER).held().get("ram_gib", 0))
        return {"schema": storage_tiers.TIER_RECORD_SCHEMA_V1, "tier": "ram",
                "tier_id": RAM_TIER, "host": "dl380g10",
                "mountpoint": str(self.ram), "epoch": self.epoch,
                "capacity_bytes": (self.ram_gib - held) * GIB,
                "window_gib": self.ram_gib,
                "ram_admission": {"admissible": True, "reason": None}}

    def tiers(self) -> dict[str, dict[str, object]]:
        return {TIER: _tier_record(self.stage, gib=self.stage_gib),
                RAM_TIER: self.ram_record()}

    def cycle(self) -> None:
        records = self.tiers()
        tier_loop.cycle(self.queue, host="dl380g10",
                        source_pool="storage_pool",
                        receipts=tier_loop.ReceiptCache(),
                        discover=lambda **_kwargs: records)

    def reader(self, *, staged, promoted) -> tuple[dict, dict]:
        """The reader, claimed inside ``phase-0``, with ranges on both tiers."""

        plan = _plan(self.queue, READER, label="reader", manifest=READER_MANIFEST,
                     stage=self.stage)
        _publish_consumer(self.queue, READER, plan)
        stage_files = {ordinal: _stage_range(
            self.queue, mover=_mover("reader", ordinal), consumer=READER,
            stage=self.stage, ordinal=ordinal, manifest=READER_MANIFEST,
            seconds=MOVER_SECONDS) for ordinal in staged}
        ram_files = {ordinal: _ram_range(
            self.queue, promotion=_promotion("reader", ordinal),
            consumer=READER, ram=self.ram, epoch=self.epoch, ordinal=ordinal,
            manifest=READER_MANIFEST) for ordinal in promoted}
        now = time.time()
        _claim(self.queue, READER, phase="phase-0",
               claimed_unix=now - CLAIMED_AGO_S,
               reported_unix=now - REPORTED_AGO_S)
        return stage_files, ram_files

    def promoter(self) -> str:
        """A ready consumer of another manifest, its stage range landed.

        What it asks the tmpfs for next is its head promotion, and the key of
        that promotion is returned.
        """

        plan = _plan(self.queue, PROMOTER, label="promoter",
                     manifest=PROMOTER_MANIFEST, stage=self.stage, phases=1)
        _publish_consumer(self.queue, PROMOTER, plan)
        _stage_range(self.queue, mover=_mover("promoter", 0),
                     consumer=PROMOTER, stage=self.stage, ordinal=0,
                     manifest=PROMOTER_MANIFEST, seconds=MOVER_SECONDS)
        return _promotion("promoter", 0)

    def held(self, tier_id: str, key: str) -> bool:
        return bool(self.queue.tier_ledger(tier_id).holder_tokens(key))


def _promoted(events: list[dict[str, object]]) -> list[str]:
    return [str(event["phase"]) for event in events
            if event.get("event") == "ram-mover-published"
            and event.get("consumer") == READER]


def assert_ledgers_match_the_roots(box: Box) -> None:
    """Held tokens equal the ranges each tier's fragments vouch for, per tier.

    ``test_a_resident_range_is_adopted_rather_than_recopied``'s invariant,
    asked of the stage and the tmpfs separately: a fragment names the tier it
    vouches on, and a ram fragment's tokens are ``ram_gib`` on the ram
    ledger.
    """

    root = box.queue.residency_fragment_root()
    accounted: dict[str, dict[str, int]] = {TIER: {}, RAM_TIER: {}}
    consumers = sorted(entry.name for entry in root.iterdir()
                       if entry.is_dir()
                       and entry.name not in (reader_lease.MATERIAL_SUBDIR,
                                              reader_lease.LEASES_SUBDIR))
    for consumer in consumers:
        for fragment in residency_map.read_fragments(root, consumer):
            mover = str(fragment["mover_action_key"])
            for entry in dict(fragment["entries"]).values():
                assert Path(str(entry["stage_path"])).exists(), (
                    f"{mover[:8]} vouches for {entry['stage_path']}, which is gone")
            receipt = box.queue.move_record(mover)
            assert receipt is not None, f"{mover[:8]} has a fragment and no receipt"
            accounted[str(fragment["tier_id"])][mover] = (
                storage_tiers.stage_tokens_for_bytes(
                    int(receipt["range_end_bytes"])
                    - int(receipt["range_start_bytes"])))
    for tier_id, kind in ((TIER, "stage_gib"), (RAM_TIER, "ram_gib")):
        ledger = box.queue.tier_ledger(tier_id)
        held = {key: int(ledger.holder_tokens(key).get(kind, 0))
                for key in ledger.held_keys()}
        held = {key: gib for key, gib in held.items() if gib
                and box.queue.move_record(key) is not None}
        assert held == accounted[tier_id], (
            f"{tier_id}: held tokens and the bytes on the root disagree: "
            f"held={held} accounted={accounted[tier_id]}")


# ------------------------------------------------------------ the window


def test_the_ram_window_promotes_only_to_its_refill_horizon(
        tmp_path: Path) -> None:
    """With room for every phase, the ram window still stops at the horizon.

    Every stage range has landed and ``phase-0`` is promoted, so its receipt
    prices the ram landing rate.  The tmpfs has 64 GiB, so the #633 bound
    (64 - 2 = 62 GiB of run-ahead) would promote all seven later phases.
    The horizon promotes ``phase-1`` and ``phase-2`` and stops there, without
    a stall: waiting at the horizon is how a rolling window is meant to wait.
    """

    box = Box(tmp_path, stage_gib=64, ram_gib=64)
    box.reader(staged=range(PHASES), promoted=(0,))

    events = tier_loop.ram_residency_window(box.queue, tiers=box.tiers())

    assert _promoted(events) == ["phase-1", "phase-2"], events
    for ordinal in range(ADVANCE, PHASES):
        assert not box.queue.item_path(
            pool.READY, _promotion("reader", ordinal)).exists(), ordinal
    assert [event for event in events
            if event.get("event") == "ram-window-stalled"] == [], events


def test_with_no_promotion_measured_the_ram_window_keeps_its_bound(
        tmp_path: Path) -> None:
    """No promotion has landed: the ram horizon is undefined, not borrowed.

    The stage horizon is defined, because the stage copies have receipts.
    The ram window does not take it over and does not price a promotion at
    the stage's rate.  With no ram landing rate it keeps its #633 bound and
    promotes every phase the tmpfs has room for.
    """

    box = Box(tmp_path, stage_gib=64, ram_gib=64)
    box.reader(staged=range(PHASES), promoted=())
    plan = residency_plan.read(box.queue, READER)
    consumer = next(entry for entry in tier_loop.live_consumers(box.queue)
                    if entry["action_key"] == READER)
    assert tier_loop._stage_horizon(box.queue, consumer, plan, None) is not None

    events = tier_loop.ram_residency_window(box.queue, tiers=box.tiers())

    assert _promoted(events) == [f"phase-{ordinal}" for ordinal in range(PHASES)]


# ------------------------------------------------------------ the eviction


def test_a_ram_range_past_the_horizon_makes_room_for_another_promotion(
        tmp_path: Path) -> None:
    """The farthest ram range goes, and the other consumer's promotion fits.

    The reader holds all 8 promotions (16 GiB) of a 17 GiB tmpfs.  A second
    consumer's stage range has landed, and its head promotion needs 2 GiB.
    Every ram range belongs to a live plan, so before this change none of
    them was an orphan: nothing was evicted, and the promotion waited for as
    long as the reader took to read 16 GiB.  Now the one ram range needed
    farthest in the future goes, and nothing on the stage moves.
    """

    box = Box(tmp_path, stage_gib=64, ram_gib=PHASE_GIB * PHASES + 1)
    stage_files, ram_files = box.reader(staged=range(PHASES),
                                        promoted=range(PHASES))
    head = box.promoter()

    box.cycle()

    assert box.queue.item_path(pool.READY, head).exists()
    assert not box.held(RAM_TIER, _promotion("reader", 7))
    assert not any(path.exists() for path in ram_files[7])
    for ordinal in range(PHASES - 1):
        assert box.held(RAM_TIER, _promotion("reader", ordinal)), ordinal
        assert all(path.exists() for path in ram_files[ordinal]), ordinal
    for ordinal in range(PHASES):
        assert box.held(TIER, _mover("reader", ordinal)), ordinal
        assert all(path.exists() for path in stage_files[ordinal]), ordinal
    assert_ledgers_match_the_roots(box)


def test_an_evicted_promotion_is_published_again_when_its_reader_nears_it(
        tmp_path: Path) -> None:
    """The farthest promotion goes, then comes back when it is needed.

    After the eviction ``phase-7``'s promotion holds nothing, so it reads as
    unpublished.  When the reader reports ``phase-5``, ``phase-7`` is inside
    the horizon again, its stage source is still there, and the ram window
    promotes it again, whole.  ``phase-0`` to ``phase-4`` are given back as
    read.
    """

    box = Box(tmp_path, stage_gib=64, ram_gib=PHASE_GIB * PHASES + 1)
    box.reader(staged=range(PHASES), promoted=range(PHASES))
    box.promoter()
    box.cycle()
    assert not box.held(RAM_TIER, _promotion("reader", 7))
    assert not box.queue.item_path(
        pool.READY, _promotion("reader", 7)).exists()

    # The worker's next heartbeat, filed into the lease the claim wrote
    # (``write_lease`` refuses a second write from another box's claim).
    lease_path = box.queue.lease_path(READER)
    lease = json.loads(lease_path.read_text())
    lease["progress_observation"] = {
        "source": "action-progress",
        "last_accepted": {"phase": "phase-5", "units_completed": 2,
                          "reported_unix": time.time()}}
    lease_path.write_text(json.dumps(lease))

    box.cycle()

    assert box.queue.item_path(pool.READY, _promotion("reader", 7)).exists()
    for ordinal in range(5):
        release = _hexkey(f"readerramrelease{ordinal}")
        assert box.queue.item_path(pool.READY, release).exists(), ordinal


def test_a_stage_range_past_the_horizon_takes_its_ram_copy_first(
        tmp_path: Path) -> None:
    """A stage range given back past its horizon leaves no ram copy behind.

    The #903 incident, with the reader's ranges promoted: all 8 phases on a
    17 GiB stage, and another consumer's 2 GiB lead queued.  The stage
    eviction takes ``phase-7``.  Its ram copy is past the ram horizon too,
    and a ram range whose stage source is gone is one the consumer's map
    skips (``overlay_ram`` reads a ram entry only beside its stage entry).
    Left alone, it would hold 2 GiB of the tmpfs that nothing reads.  So it
    goes first, then the stage range (#640's order).
    """

    capacity = PHASE_GIB * PHASES + 1
    box = Box(tmp_path, stage_gib=capacity, ram_gib=64)
    stage_files, ram_files = box.reader(staged=range(PHASES),
                                        promoted=range(PHASES))
    lead = _newcomer(box.queue)

    box.cycle()

    assert _claim_shortage(box.queue, lead, PHASE_GIB) is None
    assert not box.held(TIER, _mover("reader", 7))
    assert not box.held(RAM_TIER, _promotion("reader", 7))
    assert not any(path.exists() for path in stage_files[7] + ram_files[7])
    for ordinal in range(PHASES - 1):
        assert box.held(TIER, _mover("reader", ordinal)), ordinal
        assert box.held(RAM_TIER, _promotion("reader", ordinal)), ordinal
    assert_ledgers_match_the_roots(box)


def test_a_pinned_ram_copy_keeps_its_stage_range(tmp_path: Path) -> None:
    """A ram copy the reader has pinned keeps its stage range too.

    ``phase-7``'s ram copy is pinned by a reader lease, so its eviction is
    declined whole.  Evicting the stage range under it would leave exactly
    the stranded copy the ram-first order exists to prevent, so the stage
    range stays as well, and ``phase-6``, ram copy then stage range, makes
    the room instead.
    """

    capacity = PHASE_GIB * PHASES + 1
    box = Box(tmp_path, stage_gib=capacity, ram_gib=64)
    stage_files, ram_files = box.reader(staged=range(PHASES),
                                        promoted=range(PHASES))
    pinned = reader_lease.acquire(
        box.queue, consumer_action_key=READER,
        attempt={"nonce": "n1", "scope_id": "s1"}, tier_id=RAM_TIER,
        epoch=box.epoch,
        span={"start_bytes": 7 * PHASE_GIB * GIB,
              "end_bytes": 8 * PHASE_GIB * GIB},
        holder={"host": "test-host", "pid": 4242}, acquire_token="t1",
        covers=[{"mover_action_key": _promotion("reader", 7),
                 "manifest_sha256": READER_MANIFEST}])
    assert pinned.get("pin_id"), pinned
    lead = _newcomer(box.queue)

    box.cycle()

    assert _claim_shortage(box.queue, lead, PHASE_GIB) is None
    assert box.held(TIER, _mover("reader", 7))
    assert box.held(RAM_TIER, _promotion("reader", 7))
    assert all(path.exists() for path in stage_files[7] + ram_files[7])
    assert not box.held(TIER, _mover("reader", 6))
    assert not box.held(RAM_TIER, _promotion("reader", 6))
    assert not any(path.exists() for path in stage_files[6] + ram_files[6])
    for ordinal in range(PHASES - 2):
        assert box.held(TIER, _mover("reader", ordinal)), ordinal
        assert box.held(RAM_TIER, _promotion("reader", ordinal)), ordinal
    assert_ledgers_match_the_roots(box)


def test_the_ram_horizon_is_the_docstring_arithmetic(tmp_path: Path) -> None:
    """The ram horizon, priced from the promotion's own receipt."""

    box = Box(tmp_path, stage_gib=64, ram_gib=64)
    box.reader(staged=range(PHASES), promoted=(0,))
    plan = residency_plan.read(box.queue, READER)
    consumer = next(entry for entry in tier_loop.live_consumers(box.queue)
                    if entry["action_key"] == READER)

    horizon = tier_loop._ram_horizon(box.queue, consumer, plan, box.tiers())

    assert horizon is not None
    assert horizon["consumption_basis"] == "measured"
    assert horizon["landing_s"] == pytest.approx(MOVER_SECONDS)
    assert horizon["reach_end_bytes"] == 3 * GIB
    assert horizon["horizon_end_bytes"] == ADVANCE * PHASE_GIB * GIB
    assert horizon["advance"] == _promotion("reader", ADVANCE)
    assert [leg["phase"] for leg in horizon["beyond"]] == [
        f"phase-{ordinal}" for ordinal in BEYOND]


# ------------------------------------------------------ R12's live numbers


R12_RAM = json.loads((Path(__file__).resolve().parent / "fixtures"
                      / "r12_ram_20260923.json").read_text())


@pytest.mark.parametrize("interval_s", [R12_RAM["ram"]["live_interval_s"],
                                        tier_loop.CYCLE_INTERVAL_S])
def test_r12s_ram_window_is_unchanged_by_its_ram_horizon(
        tmp_path: Path, interval_s: float) -> None:
    """R12's own ram leg, read from the live queue on 2026-09-23.

    47 phases of 9 to 22 GiB, six promotions held (``chain-029`` to
    ``chain-024``) on a 160 GiB tmpfs, and 23 complete promotion receipts,
    the slowest at 231 MB/s.  R12 reserves 100 GiB of memory and an 80 GiB
    GPU budget, which is more than the tmpfs holds, so its ram horizon ends
    past anything the #633 bound (160 - 22 = 138 GiB of run-ahead) would
    promote.  The horizon changes nothing for R12: with the tmpfs's live free
    room and with an empty one, the window promotes the same ranges and stops
    at the same run-ahead stall.
    """

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    built = []
    for ordinal, phase in enumerate(R12_RAM["phases"]):
        start, end = int(phase["start_bytes"]), int(phase["end_bytes"])
        gib = int(phase["stage_gib"])
        total = int(R12_RAM["phases"][-1]["end_bytes"])
        built.append({
            "name": phase["name"], "start_bytes": start, "end_bytes": end,
            "stage_gib": gib,
            "mover_row": {
                **_row(queue, _mover("r12", ordinal),
                       {STAGE_KIND: gib, "mem_gb": 1}),
                "residency": {
                    "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                    "manifest_sha256": READER_MANIFEST, "manifest_bytes": total,
                    "range_start_bytes": start, "range_end_bytes": end}},
            "egress_row": _row(queue, _hexkey(f"r12egress{ordinal}"),
                               {"mem_gb": 1}),
            "ram_mover_row": {
                **_row(queue, _promotion("r12", ordinal),
                       {RAM_KIND: gib, "mem_gb": 1}),
                "residency": {
                    "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": RAM_TIER,
                    "manifest_sha256": READER_MANIFEST, "manifest_bytes": total,
                    "range_start_bytes": start, "range_end_bytes": end}},
            "ram_egress_row": _row(queue, _hexkey(f"r12ramrelease{ordinal}"),
                                   {"mem_gb": 1}),
        })
    plan = residency_plan.build_plan(
        consumer_action_key=READER, tier_id=TIER, stage_root="/stage/prewarm",
        manifest_sha256=READER_MANIFEST,
        manifest_bytes=int(R12_RAM["phases"][-1]["end_bytes"]),
        phases=built, ram_tier_id=RAM_TIER)
    receipts = [phase["ram_receipt"] for phase in R12_RAM["phases"]
                if phase["ram_receipt"]]
    landing = min(receipt["bytes_staged"] / receipt["seconds"]
                  for receipt in receipts)
    assert len(receipts) == 23 and 230e6 < landing < 232e6

    horizon = residency_plan.refill_horizon(
        plan, R12_RAM["accepted_phase"],
        claimed_unix=R12_RAM["claimed_unix"],
        reported_unix=R12_RAM["reported_unix"],
        readahead_bytes=(int(R12_RAM["mem_gb"]) * GIB
                         + int(R12_RAM["gpu_memory_budget_bytes"])),
        landing_bytes_per_s=landing,
        report_latency_s=pool.HEARTBEAT_S + float(interval_s),
        mover_role="ram_mover_row")
    assert horizon is not None and horizon["horizon_end_bytes"] is not None

    held = [str(built[ordinal]["ram_mover_row"]["action_key"])
            for ordinal, phase in enumerate(R12_RAM["phases"])
            if phase["ram_held"]]
    assert len(held) == 6
    capacity = int(R12_RAM["ram"]["capacity_gib"])
    for free in (int(R12_RAM["ram"]["free_gib"]), capacity):
        arguments = dict(
            accepted_phase=R12_RAM["accepted_phase"], free_gib=free,
            capacity_gib=capacity, published=held, staged=held,
            runahead_cap_gib=R12_RAM["ram"]["prefill_depth"],
            mover_role="ram_mover_row")
        before = residency_plan.window(plan, **arguments)
        after = residency_plan.window(
            plan, **arguments, horizon_end_bytes=horizon["horizon_end_bytes"])
        assert after == before, free
        stall = before["stall"]
        assert isinstance(stall, dict) and stall["reason"] == "runahead_budget"
        blocked = next(phase for phase in R12_RAM["phases"]
                       if phase["name"] == stall["blocked_phase"])
        assert int(blocked["start_bytes"]) < int(horizon["horizon_end_bytes"])
