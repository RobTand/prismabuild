"""A ceiling no paced mover can refute earns a probe above it (#706).

Live on 2026-09-19: a wedge-era receipt measured the sick pool at 111.2 MB/s
while its mover fell short of the 166 it had reserved, and the fold sealed
that delivery as ``ceiling_mb_s``.  Every later mover was then priced and
admitted *at* the ceiling -- delivery <= reservation <= capacity == ceiling
-- so no receipt's pool delivery could ever exceed the ceiling, the only
clause that clears one, and the tier froze.  Worse, each paced-at-ceiling
mover that fell short of its own reservation set a new, lower ceiling (111
sank to 65 as slow movers re-poisoned it), wedging every queued mover whose
sealed price predates the sink; recovery needed a hand-archived receipt and
seven hand-withdrawn movers.

The escape is the growth rule applied to the ceiling case.  While a ceiling
stands, the receipts that set it still say what one reader is worth -- the
same ``min(file-side, pool delivery over sharers)`` bound that prices a next
mover -- so the fold offers ``ceiling + the median single-reader rate`` and
marks it ``probing`` with its basis.  A probing tier's tokens mint from the
probe offer, so the next mover admits above the ceiling: if the pool
delivers, that receipt refutes the ceiling and growth resumes; if it falls
short, the ceiling re-sets at the new delivery and probes again -- bounded
oscillation, never a one-way ratchet down.  When no receipt can price a
reader, nothing is invented: the ceiling stands unpinned, exactly as before.

The queued-work follow-up (#706, after the merged fold proved insufficient
live): the fold cannot see the queue, and its offer of 171 still sat under
six already-ready movers reserving 259.  `tier_loop.cycle` now selects the
larger of that historical offer and `int(ceiling_mb_s) + the oldest ready
mover's sealed demand`, announces the selected offer and basis, and mints it;
with no ready demand the historical offer stands, and with neither the offer
is exactly the ceiling.  A ready or republished row keeps the resources its
dispatch sealed -- rewriting them would admit a reservation the sealed
request and the copy's argv never named -- so the tier's offer is what moves.
"""
from __future__ import annotations

from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
from prismabuild import pool, storage_tiers  # noqa: E402
import tier_loop  # noqa: E402

TIER = "prismabuild-stage:dl380g10"
GIB = storage_tiers.GIB
FILL = storage_tiers.FILL_KIND
KIND = f"{FILL}@{TIER}"


def _receipt(*, unix: float, delivered: float | None, sealed: int = 0,
             achieved: float | None = None, seconds: float = 100.0,
             held: float = 0.0, sharers: int | None = None,
             key: str = "0") -> dict[str, object]:
    """One mover receipt: what it reserved, what it got, what the pool gave.

    ``sharers`` is ``movers_claimed_on_tier`` -- without it the receipt
    cannot price a single reader, which is the difference between a probe
    increment the fold can measure and one it must invent.
    """

    pacing: dict[str, object] = {"held_seconds": held}
    if delivered is not None:
        pacing[storage_tiers.POOL_FILL_FIELD] = delivered
    record: dict[str, object] = {
        "action_key": key * 64, "tier_id": TIER, "complete": True,
        "seconds": seconds, "unix": unix, "disk_pacing": pacing,
        storage_tiers.MOVER_FILL_DEMAND_FIELD: sealed,
    }
    if achieved is not None:
        record["bytes_staged"] = int(achieved * 1e6 * (seconds - held))
        record["mb_per_s_file_side"] = achieved
    if sharers is not None:
        record[storage_tiers.MOVER_CONCURRENCY_FIELD] = sharers
    return record


def _supply(records: list[dict[str, object]]) -> dict[str, object]:
    return storage_tiers.fill_supply_from_records(records)


# -- the freeze the live fleet hit, and the probe that re-opens it ----------


def test_a_ceiling_no_paced_mover_can_refute_still_gets_a_probe_above_it():
    """The 2026-09-19 freeze, exactly: a sick pool's shortfall seals a
    ceiling, a mover paced at that ceiling delivers it to the byte, and the
    strict refutation clause never fires.  The supply must say so honestly
    and offer one reader's worth above the ceiling anyway."""

    sick = _receipt(unix=100.0, delivered=120.0, sealed=166, achieved=120.0,
                    sharers=1, key="1")
    paced = _receipt(unix=200.0, delivered=120.0, sealed=120, achieved=120.0,
                     sharers=1, key="2")
    supply = _supply([sick, paced])

    # The freeze itself, unchanged: delivery == ceiling does not refute.
    assert supply["ceiling_mb_s"] == 120.0
    assert supply["may_grow"] is False
    assert supply["ceiling_receipt"] == "1" * 64

    # The escape: both receipts price one reader (min(file, pool/sharers) =
    # 120.0 each), so the median single-reader rate is 120.0 and the probe
    # offers ceiling + one reader's worth.
    assert supply["probing"] is True
    assert supply["probe_offer_mb_s"] == pytest.approx(240.0)
    assert supply["probe_basis"]["basis"] == "median-single-reader-share"
    assert supply["probe_basis"]["share_mb_s"] == pytest.approx(120.0)
    assert supply["probe_basis"]["receipts_priced"] == 2


def test_a_sinking_ceiling_keeps_its_probe_never_a_one_way_ratchet():
    """111 sinking to 65: each paced-at-ceiling mover that falls short re-sets
    the ceiling lower.  Every re-set supply must still carry a way up, so the
    ratchet is bounded by the pool's own delivery, not by the last sick
    receipt's."""

    sick = _receipt(unix=100.0, delivered=120.0, sealed=166, achieved=120.0,
                    sharers=1, key="1")
    slow = _receipt(unix=200.0, delivered=65.0, sealed=120, achieved=65.0,
                    sharers=1, key="2")
    supply = _supply([sick, slow])

    assert supply["ceiling_mb_s"] == 65.0
    assert supply["best_mb_s"] is None
    assert supply["may_grow"] is False
    # The median of one reader at 120.0 and one at 65.0 is 92.5, so the way
    # up is 65 + 92.5.
    assert supply["probing"] is True
    assert supply["probe_offer_mb_s"] == pytest.approx(157.5)


def test_a_probe_that_lands_refutes_the_ceiling_and_growth_resumes():
    """The probing mover admits above the ceiling and the recovered pool
    delivers through it: the receipt refutes the ceiling, ``best`` rebuilds,
    and the supply stops probing."""

    sick = _receipt(unix=100.0, delivered=120.0, sealed=166, achieved=120.0,
                    sharers=1, key="1")
    probing = _receipt(unix=200.0, delivered=240.0, sealed=235, achieved=240.0,
                       sharers=1, key="2")
    supply = _supply([sick, probing])

    assert supply["ceiling_mb_s"] is None
    assert supply["best_mb_s"] == 240.0
    assert supply["may_grow"] is True
    assert supply["probing"] is False
    assert supply["probe_offer_mb_s"] is None


def test_a_probe_that_falls_short_re_sets_the_ceiling_at_its_delivery():
    """The other arm of the oscillation: the pool is still sick, the probing
    mover falls short, and the ceiling re-sets at what the pool then gave --
    with a fresh probe above it."""

    sick = _receipt(unix=100.0, delivered=120.0, sealed=166, achieved=120.0,
                    sharers=1, key="1")
    probing = _receipt(unix=200.0, delivered=90.0, sealed=235, achieved=90.0,
                       sharers=1, key="2")
    supply = _supply([sick, probing])

    assert supply["ceiling_mb_s"] == 90.0
    assert supply["ceiling_receipt"] == "2" * 64
    assert supply["probing"] is True
    assert supply["probe_offer_mb_s"] == pytest.approx(90.0 + 105.0)


def test_no_priced_reader_means_no_probe_and_nothing_invented():
    """A ceiling set by receipts that cannot price a single reader (no
    concurrency count) stays as it was: sealed, honest, and unpinned -- the
    increment is measured or absent, never guessed."""

    sick = _receipt(unix=100.0, delivered=120.0, sealed=166, achieved=120.0,
                    key="1")
    paced = _receipt(unix=200.0, delivered=110.0, sealed=110, achieved=110.0,
                     key="2")
    supply = _supply([sick, paced])

    assert supply["ceiling_mb_s"] == 120.0
    assert supply["may_grow"] is False
    assert supply["probing"] is False
    assert supply["probe_offer_mb_s"] is None


def test_the_probe_fold_is_pure_and_order_free():
    records = [_receipt(unix=300.0, delivered=240.0, sealed=235, achieved=240.0,
                        sharers=1, key="3"),
               _receipt(unix=200.0, delivered=120.0, sealed=120, achieved=120.0,
                        sharers=1, key="2"),
               _receipt(unix=100.0, delivered=120.0, sealed=166, achieved=120.0,
                        sharers=1, key="1")]
    first = _supply(records)
    second = _supply(list(reversed(records)))
    assert first == second
    assert first["ceiling_mb_s"] is None and first["best_mb_s"] == 240.0


def test_no_ceiling_no_probe():
    """Probing is the ceiling's escape, not a second growth rule: with no
    ceiling the growth rule already offers best + one reader, and the fold
    says nothing about probes."""

    healthy = _receipt(unix=100.0, delivered=498.0, sealed=166, achieved=166.0,
                       sharers=3, key="1")
    supply = _supply([healthy])
    assert supply["ceiling_mb_s"] is None
    assert supply["probing"] is False
    assert supply["probe_offer_mb_s"] is None


# -- through the real cycle: the tokens mint from the probe offer -----------


def _cycle(queue):
    # A stage root beside this queue, never a live box's: the fixture must
    # not register, sweep or measure anything on a real stage.
    stage_root = queue.root.parent / "stage"

    def discover(**kwargs):
        record = {"schema": storage_tiers.TIER_RECORD_SCHEMA_V1,
                  "tier_id": TIER, "host": "dl380g10", "tier": "stage",
                  "mountpoint": str(stage_root), "capacity_bytes": 600 * GIB}
        record[storage_tiers.FILL_RECORD_FIELD] = storage_tiers.fill_rate_from_records(
            kwargs.get("fill_records") or ())
        return {TIER: record}

    return tier_loop.cycle(queue, host="dl380g10", source_pool="storage_pool",
                           receipts=tier_loop.ReceiptCache(),
                           discover=discover)[0]


def _ready_mover(queue, key: str, fill: int) -> None:
    queue.publish(action_key=key * 64, cas_root="/cas", worker_script="w.py",
                  checkout_root="/co", tags=["dl380g10"],
                  resources={"cpu": 4, "mem_gb": 2, KIND: fill,
                             f"stage_gib@{TIER}": 4},
                  residency={"schema": pool.RESIDENCY_SCHEMA_V1,
                             "manifest_sha256": "0" * 64,
                             "manifest_bytes": 4 * GIB, "tier_id": TIER,
                             "range_start_bytes": 0, "range_end_bytes": 4 * GIB})


def test_a_frozen_tier_mints_from_the_probe_offer(tmp_path):
    """THE cycle-level escape: the sick receipts freeze the fold, and the
    tier's fill tokens mint from the probe offer rather than the ceiling, so
    the next mover can admit, pace and deliver above it."""

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    queue.record_move("1" * 64, _receipt(unix=100.0, delivered=120.0, sealed=166,
                                         achieved=120.0, sharers=1, key="1"))
    queue.record_move("2" * 64, _receipt(unix=200.0, delivered=120.0, sealed=120,
                                         achieved=120.0, sharers=1, key="2"))
    _ready_mover(queue, "a", 110)
    record = _cycle(queue)

    assert record["fill_source"] == "measured-probing"
    # 120.0 ceiling + 120.0 median single-reader share.
    assert record["tokens"][FILL] == 240
    assert record["fill_probe_mb_s"] == 120
    assert record["fill_supply"]["probing"] is True
    assert record["fill_supply"]["probe_offer_mb_s"] == pytest.approx(240.0)
    assert record["fill_ceiling_receipt"] == "1" * 64
    # The ledger is the admission authority: it now holds the probe offer.
    assert queue.tier_ledger(TIER).capacity()[FILL] == 240


def test_the_escaped_probe_refutes_the_ceiling_and_growth_resumes(tmp_path):
    """The probing mover runs above the ceiling and the pool keeps up: its
    receipt refutes the ceiling, and the next cycle grows off the new best
    exactly as the growth rule always has."""

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    queue.record_move("1" * 64, _receipt(unix=100.0, delivered=120.0, sealed=166,
                                         achieved=120.0, sharers=1, key="1"))
    _ready_mover(queue, "a", 110)
    frozen = _cycle(queue)
    assert frozen["fill_source"] == "measured-probing"

    # The probing mover admits at 240, reserves 235 of it, and the recovered
    # pool delivers 240 through its window.
    queue.record_move("2" * 64, _receipt(unix=200.0, delivered=240.0, sealed=235,
                                         achieved=240.0, sharers=1, key="2"))
    record = _cycle(queue)

    assert record["fill_source"] == "measured-growing"
    assert record["tokens"][FILL] == 240 + 110
    assert record["fill_probe_mb_s"] == 110
    assert record["fill_supply"]["probing"] is False
    assert record["fill_supply"]["may_grow"] is True
    assert "fill_ceiling_receipt" not in record


def test_a_ceiling_without_a_priced_reader_mints_the_queued_floor(tmp_path):
    """The fold cannot price a reader here, so its only measured offer is the
    plain ceiling -- but the queue is not blind: the oldest ready mover's own
    sealed demand lifts the offer above the ceiling, and the announced supply
    names that selection rather than the absent fold."""

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    queue.record_move("1" * 64, _receipt(unix=100.0, delivered=498.0, sealed=166,
                                         achieved=130.0, key="1"))
    queue.record_move("2" * 64, _receipt(unix=200.0, delivered=522.0, sealed=166,
                                         achieved=130.0, key="2"))
    _ready_mover(queue, "a", 166)
    record = _cycle(queue)

    assert record["fill_source"] == "measured-probing"
    assert record["tokens"][FILL] == 522 + 166
    assert record["fill_probe_mb_s"] == 166
    assert record["fill_supply"]["probing"] is True
    assert record["fill_supply"]["probe_offer_mb_s"] == 522 + 166
    assert record["fill_supply"]["probe_basis"] == {
        "basis": "oldest-ready-sealed-demand", "demand_mb_s": 166}


def test_the_larger_of_the_fold_offer_and_the_queued_floor_is_selected(tmp_path):
    """#707's historical offer is the fallback, not a cap: when the oldest
    ready mover's sealed demand asks for more, that floor is the selected
    offer and the announcement carries the historical value beside it."""

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    queue.record_move("1" * 64, _receipt(unix=100.0, delivered=120.0, sealed=166,
                                         achieved=120.0, sharers=1, key="1"))
    _ready_mover(queue, "a", 145)
    record = _cycle(queue)

    # The fold prices one reader at 120 (120 + 120 historical offer); the
    # queued floor is 120 + 145 and is the larger, so it is selected.
    assert record["fill_supply"]["probe_offer_mb_s"] == 265
    assert record["fill_supply"]["probe_basis"]["basis"] == (
        "oldest-ready-sealed-demand")
    assert record["fill_supply"]["probe_basis"]["demand_mb_s"] == 145
    assert record["fill_supply"]["probe_basis"]["historical_offer_mb_s"] == 240
    assert record["tokens"][FILL] == 265
    assert record["fill_source"] == "measured-probing"


def test_the_fold_offer_stands_when_the_queued_floor_is_smaller(tmp_path):
    """With no ready demand, or ready demand smaller than the historical
    increment, the fold's own offer is the selected one and its basis stands:
    the queue can only add, never shrink, what the receipts measured."""

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    queue.record_move("1" * 64, _receipt(unix=100.0, delivered=120.0, sealed=166,
                                         achieved=120.0, sharers=1, key="1"))
    queue.record_move("2" * 64, _receipt(unix=200.0, delivered=120.0, sealed=120,
                                         achieved=120.0, sharers=1, key="2"))
    _ready_mover(queue, "a", 50)
    record = _cycle(queue)

    # 120.0 ceiling + 120.0 median single-reader share beats the queued
    # floor of 170.
    assert record["fill_source"] == "measured-probing"
    assert record["tokens"][FILL] == 240
    assert record["fill_probe_mb_s"] == 120
    assert record["fill_supply"]["probe_offer_mb_s"] == pytest.approx(240.0)
    assert record["fill_supply"]["probe_basis"]["basis"] == (
        "median-single-reader-share")


# -- the adoption path: a stale price waits for the cycle, it is not rewritten --


CONSUMER = "c" * 64
MANIFEST = "9" * 64
STAGE_KIND = f"stage_gib@{TIER}"


def _hexkey(seed: str) -> str:
    return (seed.encode().hex() * 64)[:64]


def _movement_row(key: str, resources: dict[str, int],
                  queue: pool.PoolQueue) -> dict[str, object]:
    return {"action_key": key, "cas_root": str(queue.root / "cas"),
            "checkout_root": str(queue.root / "co"),
            "worker_script": str(queue.root / "worker.py"),
            "tags": ["dl380g10"], "resources": resources}


def _stale_plan(queue: pool.PoolQueue, *, fills: tuple[int, int],
                gib_per_phase: int = 2) -> dict[str, object]:
    """Two whole-phase legs whose movers carry the fill prices ``fills``
    sealed -- the second priced under the tier, the first above it."""

    phases = []
    for ordinal, fill in enumerate(fills):
        start = ordinal * gib_per_phase * GIB
        end = start + gib_per_phase * GIB
        phases.append({
            "name": f"phase-{ordinal}", "start_bytes": start,
            "end_bytes": end, "stage_gib": gib_per_phase,
            "mover_row": {
                **_movement_row(_hexkey(f"mover{ordinal}"),
                                {STAGE_KIND: gib_per_phase, KIND: fill,
                                 "mem_gb": 1}, queue),
                "residency": {
                    "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                    "manifest_sha256": MANIFEST, "manifest_bytes": 1 << 30,
                    "range_start_bytes": start, "range_end_bytes": end}},
            "egress_row": _movement_row(_hexkey(f"egress{ordinal}"),
                                        {"mem_gb": 1}, queue),
        })
    from prismabuild import residency_plan
    return residency_plan.build_plan(
        consumer_action_key=CONSUMER, tier_id=TIER, stage_root="/stage/prewarm",
        manifest_sha256=MANIFEST, manifest_bytes=1 << 30, phases=phases)


def _claim_consumer(queue: pool.PoolQueue, plan: dict[str, object]) -> None:
    from prismabuild import residency_plan
    residency_plan.freeze(queue, plan)
    queue.publish(
        action_key=CONSUMER, cas_root=queue.root / "cas",
        checkout_root=queue.root / "co", worker_script=queue.root / "worker.py",
        resources={"cpu": 1, "mem_gb": 1},
        residency={"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                   "manifest_sha256": MANIFEST, "manifest_bytes": 1 << 30,
                   "leads": residency_plan.leads_for(plan)})


def test_a_republished_mover_priced_above_the_tier_keeps_its_sealed_resources(
        tmp_path):
    """A mover adopted across dispatches carries the fill price its dispatch
    sealed, and the tier's supply may have sunk under it since.  The window
    must publish that row byte for byte as sealed -- rewriting the resources
    here would admit a reservation the sealed request and the copy's argv
    never named -- and once the row is the oldest ready demand the next cycle
    raises the fill offer to that unchanged demand, with other admission
    gates still applying."""

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    # The tier the loop would have minted by now: 5 GiB of stage, 130 MB/s of
    # fill -- sunk far under the 400 each mover's dispatch sealed.
    queue.mint_tier_capacity(TIER, {"stage_gib": 5, FILL: 130})
    plan = _stale_plan(queue, fills=(400, 400))
    _claim_consumer(queue, plan)

    events = tier_loop.residency_window(
        queue, tiers={TIER: {"tier_id": TIER, "tier": "stage",
                             "mountpoint": str(tmp_path / "stage")}})

    assert not [event for event in events
                if event["event"] == "mover-repriced-to-tier-offer"]
    for ordinal in (0, 1):
        row = pool._read_json(queue.item_path(pool.READY,
                                              _hexkey(f"mover{ordinal}")))
        assert row["resources"][KIND] == 400
        assert row["resources"][STAGE_KIND] == 2
        assert row["resources"]["mem_gb"] == 1
        assert row["residency"]["range_end_bytes"] == (ordinal + 1) * 2 * GIB
    # The tier cannot seat a 400 reservation on the 130 it holds: the row
    # waits, unchanged, rather than claiming against a price it never sealed.
    assert queue.claim(capacity={"cpu": 32, "mem_gb": 48},
                       tags=["dl380g10"]) is None

    # The next cycle measures a 65.7 ceiling that no receipt can price a
    # reader for; the queued floor is 65 + 400, and the sealed row claims.
    queue.record_move("1" * 64, _receipt(unix=100.0, delivered=65.7,
                                         sealed=400, achieved=65.0, key="1"))
    record = _cycle(queue)
    assert record["fill_source"] == "measured-probing"
    assert record["tokens"][FILL] == int(65.7) + 400
    claimed = queue.claim(capacity={"cpu": 32, "mem_gb": 48},
                          tags=["dl380g10"])
    assert claimed is not None
    assert claimed["resources"][KIND] == 400


def test_a_mover_priced_under_the_tier_is_republished_verbatim(tmp_path):
    """The window never rewrites a mover's resources: a row the tier can
    already admit is published byte for byte as sealed."""

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    queue.mint_tier_capacity(TIER, {"stage_gib": 5, FILL: 130})
    plan = _stale_plan(queue, fills=(100, 100))
    _claim_consumer(queue, plan)

    events = tier_loop.residency_window(
        queue, tiers={TIER: {"tier_id": TIER, "tier": "stage",
                             "mountpoint": str(tmp_path / "stage")}})

    assert not [event for event in events
                if event["event"] == "mover-repriced-to-tier-offer"]
    for ordinal in (0, 1):
        row = pool._read_json(queue.item_path(pool.READY,
                                              _hexkey(f"mover{ordinal}")))
        assert row["resources"][KIND] == 100
        assert row["resources"][STAGE_KIND] == 2

