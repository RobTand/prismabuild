"""A reserved range is not resident until its completion is published.

Live Stage A 2026-09-20 (issue #759): the head stage mover held ledger
tokens from CLAIM while it was still copying, and ``tier_loop._mover_state``
read those holdings as resident bytes.  ``_stage_source_staged`` then let the
RAM promotion publish; its own proof found no per-consumer fragment and
refused ``source-coverage-gap`` -- three times at first, 80 retained failed
attempts across 27 publication generations by the time maintenance counted
them.  Reservations are accounting; only published completion (the consumer's
fragments, which every reader proof already checks) is residency.

The four steps the fix has to make true, all run here for real rather than
asserted:

1. reserve, paused before the copy -- no RAM row, and nothing reported staged;
2. half of a two-entry copy -- no full-range readiness;
3. publish complete -- the ordinary window offers RAM, the promotion runs,
   the bytes read back exactly, and the range releases cleanly;
4. an old complete receipt plus a new partial fragment -- still not resident.

Steps 1, 2 and 4 run the ordinary cycle repeatedly, because the live failure
was not one premature publication but a retained reservation that kept
re-publishing one (Rob, issue #759, 2026-09-20 17:04:55 UTC).
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
from prismabuild import (  # noqa: E402
    pool, reader_lease, residency_map, residency_plan, storage_tiers)

import pbstatus  # noqa: E402
import ram_promote  # noqa: E402
import stage_move  # noqa: E402
import stage_release  # noqa: E402
import tier_loop  # noqa: E402

CONSUMER = "c" * 64
MOVER = "d" * 64
RAM_MOVER = "e" * 64
EGRESS = "f" * 64
RAM_EGRESS = "0" * 64
STAGE_TIER = "prismabuild-stage:dl380g10"
RAM_TIER = "ram:dl380g10"
N_BYTES = 1 << 16

#: How many ordinary cycles a "must not publish" case runs.  One pass proves
#: the first publication is gone; the live defect kept publishing, so the
#: assertion is made over repeated cycles.
CYCLES = 3


def _row(key: str, resources: dict[str, int], queue: pool.PoolQueue) -> dict:
    """One queue row in the shape ``publish`` seals."""

    return {"action_key": key, "cas_root": str(queue.root / "cas"),
            "checkout_root": str(queue.root / "co"),
            "worker_script": str(queue.root / "worker.py"),
            "tags": ["dl380g10"], "resources": resources}


def _corpus(tmp_path: Path, entries: int) -> tuple[list[Path], dict, str]:
    """``entries`` tiny real files plus the manifest naming them, all hashed."""

    paths: list[Path] = []
    declared: list[dict[str, object]] = []
    for index in range(entries):
        raw = bytes((((i + 1) * (index + 1) * 2654435761) & 0xFF)
                    for i in range(N_BYTES))
        path = tmp_path / "mnt" / "model" / f"shard-{index}.bin"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)
        paths.append(path)
        declared.append({"path": str(path), "offset": 0, "bytes": N_BYTES,
                         "sha256": hashlib.sha256(raw).hexdigest()})
    total = entries * N_BYTES
    manifest = {
        "schema": "prismaquant.prismabuild.data_manifest.v1",
        "produced_by": {"tool": "published-readiness-harness"},
        "mount_prefix": str(tmp_path / "mnt"),
        "entries": declared,
        "entry_count": entries, "total_bytes": total,
        "annotations": {"phases": [
            {"name": "head", "bytes": total, "cumulative_bytes": total}]},
    }
    digest = hashlib.sha256(
        json.dumps(manifest, sort_keys=True).encode()).hexdigest()
    return paths, manifest, digest


def _plan(queue: pool.PoolQueue, digest: str, total: int) -> dict[str, object]:
    """One phase, one stage leg, one ram leg -- the live Stage A head shape."""

    pin = {"schema": pool.RESIDENCY_SCHEMA_V1, "manifest_sha256": digest,
           "manifest_bytes": total, "range_start_bytes": 0,
           "range_end_bytes": total}
    return residency_plan.build_plan(
        consumer_action_key=CONSUMER, tier_id=STAGE_TIER,
        stage_root=str(queue.root / "stage"), manifest_sha256=digest,
        manifest_bytes=total, phases=[{
            "name": "head", "start_bytes": 0, "end_bytes": total,
            "stage_gib": 1,
            "mover_row": {
                **_row(MOVER, {f"stage_gib@{STAGE_TIER}": 1, "mem_gb": 1},
                       queue),
                "residency": {**pin, "tier_id": STAGE_TIER}},
            "egress_row": _row(EGRESS, {"mem_gb": 1}, queue),
            "ram_mover_row": {
                **_row(RAM_MOVER, {f"ram_gib@{RAM_TIER}": 1, "mem_gb": 1},
                       queue),
                "residency": {**pin, "tier_id": RAM_TIER}},
            "ram_egress_row": _row(RAM_EGRESS, {"mem_gb": 1}, queue),
        }], ram_tier_id=RAM_TIER)


def _world(tmp_path: Path, *, entries: int = 1) -> tuple[
        pool.PoolQueue, dict, str, Path, list[Path]]:
    """Frozen plan, published consumer, minted capacity, announced tiers."""

    paths, manifest, digest = _corpus(tmp_path, entries)
    total = entries * N_BYTES
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    (queue.root / "worker.py").write_text("import sys\n")
    plan = _plan(queue, digest, total)
    residency_plan.freeze(queue, plan)
    queue.publish(**_row(CONSUMER, {"mem_gb": 1}, queue), residency={
        "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": STAGE_TIER,
        "manifest_sha256": digest, "manifest_bytes": total,
        "leads": residency_plan.leads_for(plan)})
    queue.mint_tier_capacity(STAGE_TIER, {"stage_gib": 64})
    queue.mint_tier_capacity(RAM_TIER, {"ram_gib": 8})
    (queue.root / "stage").mkdir(parents=True, exist_ok=True)
    ram = queue.root / "ram"
    ram.mkdir(parents=True, exist_ok=True)
    assert storage_tiers.ensure_ram_epoch(ram, host="dl380g10") is not None
    queue.announce_tier({
        "schema": storage_tiers.TIER_RECORD_SCHEMA_V1, "tier": "stage",
        "tier_id": STAGE_TIER, "host": "dl380g10",
        "mountpoint": str(queue.root / "stage"),
        "capacity_bytes": 64 * storage_tiers.GIB})
    queue.announce_tier({
        "schema": storage_tiers.TIER_RECORD_SCHEMA_V1, "tier": "ram",
        "tier_id": RAM_TIER, "host": "dl380g10", "mountpoint": str(ram),
        "epoch": str(storage_tiers.read_ram_epoch(ram)["epoch"]),
        "capacity_bytes": 8 * storage_tiers.GIB})
    # Both roots belong to this queue, exactly as the tier loop marks them
    # before it publishes a mover -- an egress refuses an unregistered root.
    for tier, root in ((STAGE_TIER, queue.root / "stage"), (RAM_TIER, ram)):
        assert stage_release.register_stage_root(
            queue, tier_id=tier, stage_root=root) == "registered"
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest))
    return queue, manifest, digest, manifest_path, paths


def _tiers(queue: pool.PoolQueue) -> dict[str, dict[str, object]]:
    """The announced records the cycle is handed, read back from the queue."""

    return {str(record["tier_id"]): record for record in queue.tiers()}


def _filed(queue: pool.PoolQueue) -> dict[str, object]:
    """The frozen plan, read the way every window reads it."""

    plan, _incarnation = residency_plan.read_filed(queue, CONSUMER)
    assert plan is not None
    return plan


def _reserve(queue: pool.PoolQueue, digest: str, total: int) -> None:
    """Reservation held, row queued, zero bytes copied or published.

    The live shape: a mover past admission (tokens held from CLAIM) but
    before its copy lands anything -- no fragment, no material, no receipt.
    """

    queue.publish(**_row(
        MOVER, {f"stage_gib@{STAGE_TIER}": 1, "mem_gb": 1}, queue),
        residency={
            "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": STAGE_TIER,
            "manifest_sha256": digest, "manifest_bytes": total,
            "range_start_bytes": 0, "range_end_bytes": total})
    assert queue.tier_ledger(STAGE_TIER).acquire(MOVER, {"stage_gib": 1})


def _cycles(queue: pool.PoolQueue, count: int = CYCLES) -> list[dict]:
    """``count`` ordinary ram tier cycles, with every event they emitted."""

    events: list[dict] = []
    for _pass in range(count):
        events.extend(tier_loop.ram_residency_window(
            queue, tiers=_tiers(queue)))
    return events


def _promotions(events: list[dict]) -> list[dict]:
    """Only the events that say a promotion row was published."""

    return [event for event in events
            if event.get("event") == "ram-mover-published"]


def _cursor(queue: pool.PoolQueue) -> dict:
    """The reported per-phase state, through the census the MCP cursor serves."""

    ready = {path.stem for path in queue.dir(pool.READY).glob("*.json")}
    claimed = {path.stem for path in queue.dir(pool.CLAIMED).glob("*.json")}
    notes: list[str] = []
    unreadable: list[str] = []
    entry = pbstatus._starvation_plan_entry(
        queue, CONSUMER, _filed(queue), ready=ready, claimed=claimed,
        notes=notes, unreadable=unreadable, now=1700000000.0)
    assert entry["valid"] is True, notes
    return entry


def _move(queue: pool.PoolQueue, digest: str, manifest_path: Path,
          total: int) -> dict[str, object]:
    """The real stage copy, run the way the fleet runs it."""

    return stage_move.move(stage_move.build_parser().parse_args([
        "--pool-root", str(queue.root),
        "--cas-root", str(queue.root / "cas"),
        "--action-key", MOVER,
        "--consumer-action-key", CONSUMER,
        "--tier-id", STAGE_TIER,
        "--stage-root", str(queue.root / "stage"),
        "--manifest-sha256", digest,
        "--range-start-bytes", "0", "--range-end-bytes", str(total),
        "--manifest", str(manifest_path),
        "--residency-root", str(queue.root / pool.RESIDENCY),
        "--block", str(1 << 12),
        "--readers", "1", "--max-readers", "1", "--unpaced"]))


def _prefix_fragment(queue: pool.PoolQueue, digest: str,
                     source: Path) -> None:
    """The prefix a running (or crashed) mover leaves: one entry published."""

    staged = queue.root / "stage" / "model" / source.name
    staged.parent.mkdir(parents=True, exist_ok=True)
    staged.write_bytes(source.read_bytes())
    residency_map.write_fragment(queue.root / pool.RESIDENCY, {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": CONSUMER, "mover_action_key": MOVER,
        "tier_id": STAGE_TIER, "stage_root": str(queue.root / "stage"),
        "manifest_sha256": digest,
        "entries": {residency_map.residency_map_key(str(source), 0): {
            "stage_path": str(staged), "bytes": N_BYTES, "offset": 0,
            "sha256": hashlib.sha256(source.read_bytes()).hexdigest()}}})


# ---------------------------------------------------------------------------
# Step 1: reserved, paused before the copy.


def test_a_reservation_before_any_copy_publishes_no_ram(tmp_path: Path) -> None:
    """Tokens held, nothing copied: the RAM window stays shut, every cycle."""

    queue, _manifest, digest, _manifest_path, _paths = _world(tmp_path)
    _reserve(queue, digest, N_BYTES)

    assert _promotions(_cycles(queue)) == [], "a booking is not bytes"
    assert not queue.item_path(pool.READY, RAM_MOVER).exists()
    assert queue.tier_ledger(RAM_TIER).held().get("ram_gib", 0) == 0

    plan = _filed(queue)
    assert residency_plan.resident_movers(queue, plan, STAGE_TIER) == set()
    published, held = tier_loop._mover_state(queue, plan, STAGE_TIER)
    assert MOVER in published, "the reservation is still accounted for"
    assert held == {MOVER}, "and the ledger still carries it"

    entry = _cursor(queue)
    phase = entry["phases"][0]
    assert phase["stage"]["staged"] is False
    assert phase["stage"]["reserved"] is True, "reservations stay visible"
    assert phase["ram"]["staged"] is False
    # Known-unstaged keeps counting as backlog: the unknown split must not
    # cost the census the bytes it really can prove are still to come.
    gap = entry["cursor_gap"]["stage"]
    assert gap["unstaged_phases"] == ["head"], gap
    assert gap["unstaged_bytes"] == N_BYTES, gap
    assert gap["unknown_phases"] == [] and gap["unknown_bytes"] == 0, gap


# ---------------------------------------------------------------------------
# Step 2: half of a two-entry copy.


def test_half_a_copy_is_not_full_range_readiness(tmp_path: Path) -> None:
    """One of two entries published mid-copy is not the range's residency."""

    queue, _manifest, digest, _manifest_path, paths = _world(
        tmp_path, entries=2)
    _reserve(queue, digest, 2 * N_BYTES)
    _prefix_fragment(queue, digest, paths[0])

    assert _promotions(_cycles(queue)) == [], "half a range is not the range"
    assert not queue.item_path(pool.READY, RAM_MOVER).exists()
    assert residency_plan.resident_movers(
        queue, _filed(queue), STAGE_TIER) == set()
    assert _cursor(queue)["phases"][0]["stage"]["staged"] is False


# ---------------------------------------------------------------------------
# Step 3: publish complete, promote, read, release.


def test_publication_completes_the_transition(tmp_path: Path) -> None:
    """Reserve, cycle, copy for real, promote, read the bytes, release."""

    queue, _manifest, digest, manifest_path, paths = _world(tmp_path)
    _reserve(queue, digest, N_BYTES)
    assert _promotions(_cycles(queue)) == [], "nothing is resident yet"

    receipt = _move(queue, digest, manifest_path, N_BYTES)
    assert receipt["complete"] is True, receipt.get("errors")
    queue.record_move(MOVER, receipt)

    assert residency_plan.resident_movers(
        queue, _filed(queue), STAGE_TIER) == {MOVER}, (
            "a published, complete, covering copy is resident")
    offered = _promotions(tier_loop.ram_residency_window(
        queue, tiers=_tiers(queue)))
    assert [event["phase"] for event in offered] == ["head"]
    assert queue.item_path(pool.READY, RAM_MOVER).exists()

    promotion = ram_promote.promote(ram_promote.build_parser().parse_args([
        "--pool-root", str(queue.root),
        "--action-key", RAM_MOVER,
        "--consumer-action-key", CONSUMER,
        "--tier-id", RAM_TIER,
        "--ram-root", str(queue.root / "ram"),
        "--source-stage-root", str(queue.root / "stage"),
        "--manifest-sha256", digest,
        "--range-start-bytes", "0", "--range-end-bytes", str(N_BYTES),
        "--manifest", str(manifest_path),
        "--residency-root", str(queue.root / pool.RESIDENCY)]))
    assert promotion.get("refusal") is None, promotion
    assert promotion["complete"] is True
    queue.record_move(RAM_MOVER, promotion)
    assert queue.tier_ledger(RAM_TIER).acquire(RAM_MOVER, {"ram_gib": 1})

    promoted = queue.root / "ram" / "model" / "shard-0.bin"
    assert promoted.read_bytes() == paths[0].read_bytes(), "exact bytes"

    assert residency_plan.resident_movers(
        queue, _filed(queue), RAM_TIER) == {RAM_MOVER}
    phase = _cursor(queue)["phases"][0]
    assert phase["stage"]["staged"] is True
    assert phase["ram"]["staged"] is True
    assert reader_lease.live_for(
        queue, None, residency_root=queue.root / pool.RESIDENCY) == ({}, [])

    # ...and the range releases cleanly: files gone, tokens back, and the
    # shared predicate stops calling it resident.
    released = stage_release.evict(
        queue, RAM_MOVER, consumer_action_key=CONSUMER,
        stage_root=str(queue.root / "ram"),
        residency_root=str(queue.root / pool.RESIDENCY))
    assert not released.get("errors"), released
    assert not promoted.exists()
    assert queue.tier_ledger(RAM_TIER).held().get("ram_gib", 0) == 0
    assert residency_plan.resident_movers(
        queue, _filed(queue), RAM_TIER) == set(), (
            "released bytes are not resident")


# ---------------------------------------------------------------------------
# Step 4: an old complete receipt plus new, partial publication.


def test_a_historical_receipt_alone_is_not_residency(tmp_path: Path) -> None:
    """An old complete receipt plus new holdings, and no fragment at all."""

    queue, _manifest, digest, _manifest_path, _paths = _world(tmp_path)
    queue.record_move(MOVER, {
        "consumer_action_key": CONSUMER, "tier_id": STAGE_TIER,
        "stage_root": str(queue.root / "stage"), "manifest_sha256": digest,
        "range_start_bytes": 0, "range_end_bytes": N_BYTES,
        "bytes_staged": N_BYTES, "entries_declared": 1, "entries_staged": 1,
        "complete": True, "seconds": 1.0, "unix": 1000.0})
    _reserve(queue, digest, N_BYTES)

    assert _promotions(_cycles(queue)) == [], "a receipt is not a publication"
    assert not queue.item_path(pool.READY, RAM_MOVER).exists()
    assert residency_plan.resident_movers(
        queue, _filed(queue), STAGE_TIER) == set()


def test_an_old_receipt_with_a_new_partial_fragment_is_not_resident(
        tmp_path: Path) -> None:
    """A crashed retry's prefix plus the predecessor's receipt is not staged.

    Two entries; only the first was ever republished.  The predecessor's
    complete receipt (two declared, two staged) still sits on disk and no row
    is CLAIMED -- ordinary recovery after a crash before filing.  Readiness
    ties to the CURRENT fragment's coverage, so the short count refuses even
    though every file present is old and honest.
    """

    queue, _manifest, digest, _manifest_path, paths = _world(
        tmp_path, entries=2)
    total = 2 * N_BYTES
    _prefix_fragment(queue, digest, paths[0])
    _reserve(queue, digest, total)
    queue.record_move(MOVER, {
        "consumer_action_key": CONSUMER, "tier_id": STAGE_TIER,
        "stage_root": str(queue.root / "stage"), "manifest_sha256": digest,
        "range_start_bytes": 0, "range_end_bytes": total,
        "bytes_staged": total, "entries_declared": 2, "entries_staged": 2,
        "complete": True, "seconds": 1.0, "unix": 1000.0})

    assert _promotions(_cycles(queue)) == [], (
        "the receipt describes a copy this fragment is not")
    assert not queue.item_path(pool.READY, RAM_MOVER).exists()
    assert residency_plan.resident_movers(
        queue, _filed(queue), STAGE_TIER) == set()
    assert _cursor(queue)["phases"][0]["stage"]["staged"] is False
