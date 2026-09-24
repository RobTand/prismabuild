"""The reader plan's parts, one at a time (#1091, #1090).

``tests/test_a_blocked_copy_goes_first_on_its_tier.py`` drives the four cases
end to end.  These pin the pieces those cases rest on:

* the mover cap fold (``storage_tiers.mover_cap_from_records``): the knee, a
  curve still rising, too little to measure, and the receipts it skips;
* the warm-copy test (``storage_tiers.outran_the_pool``) and a yielded copy's
  shortfall (``_fell_short``);
* the claim gate (``PoolQueue.reader_plan_verdict``): a waited copy's fill is
  waived and its occupancy is not, and a stale plan fails open;
* the mover's own landing report: priced only inside the current claim.

Everything runs on ``tmp_path`` queues (#628).
"""
from __future__ import annotations

import json
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools" / "fleet"))
from prismabuild import pool, residency_plan, storage_tiers  # noqa: E402
import tier_loop  # noqa: E402

TIER = "prismabuild-stage:dl380g10"
FILL_KIND = f"{storage_tiers.FILL_KIND}{storage_tiers.TIER_DEMAND_SEPARATOR}{TIER}"
STAGE_KIND = f"stage_gib{storage_tiers.TIER_DEMAND_SEPARATOR}{TIER}"
GIB = storage_tiers.GIB
IDENTITY = {"source": {"guid": "1", "pool": "storage_pool"}}


def _receipt(movers: int, delivered: float, *, seconds: float = 100.0,
             identity: object = IDENTITY, staged: int | None = None,
             pool_read: int | None = None) -> dict[str, object]:
    staged = int(delivered * 1e6 * seconds / movers) if staged is None else staged
    return {"schema": storage_tiers.MOVER_RECEIPT_SCHEMA, "tier_id": TIER,
            "action_key": f"{movers:02d}{delivered:.0f}".ljust(64, "0"),
            "seconds": seconds, "bytes_staged": staged, "complete": True,
            "pool_identity": identity,
            "mb_per_s_file_side": round(staged / 1e6 / seconds, 1),
            "disk_pacing": {"mean_pool_read_mb_s": delivered,
                            "pool_read_bytes": (int(delivered * 1e6 * seconds)
                                                if pool_read is None else pool_read)},
            storage_tiers.MOVER_CONCURRENCY_FIELD: movers}


# ------------------------------------------------------------- the cap fold


def test_the_knee_is_the_first_level_the_measurement_cannot_tell_from_the_best() -> None:
    records = [_receipt(m, r) for m, rates in {
        1: (200, 210), 2: (300, 310), 3: (400, 410), 4: (395, 405),
        6: (300, 310)}.items() for r in rates]

    cap = storage_tiers.mover_cap_from_records(records, tier_id=TIER,
                                               pool_identity=IDENTITY)

    assert cap["movers"] == 3 and cap["basis"] == "measured"
    assert cap["best"] == {"movers": 3, "mean_mb_s": 405.0, "se_mb_s": 5.0}
    assert cap["receipts"] == 10
    assert cap["method"] == storage_tiers.MOVER_CAP_METHOD
    assert [(row["movers"], row["mean_mb_s"], row["se_mb_s"])
            for row in cap["curve"]] == [(1, 205.0, 5.0), (2, 305.0, 5.0),
                                         (3, 405.0, 5.0), (4, 400.0, 5.0),
                                         (6, 305.0, 5.0)]


def test_a_level_within_its_error_of_the_best_is_the_knee() -> None:
    """The best level is 6, but 4 cannot be told from it: the cap is 4."""

    records = [_receipt(m, r) for m, rates in {
        2: (200, 210), 4: (398, 408), 6: (400, 410)}.items() for r in rates]

    cap = storage_tiers.mover_cap_from_records(records, tier_id=TIER)

    assert cap["best"]["movers"] == 6                    # type: ignore[index]
    assert cap["movers"] == 4 and cap["basis"] == "measured"


def test_a_curve_still_rising_at_its_last_level_probes_one_more() -> None:
    records = [_receipt(m, r) for m, rates in {
        1: (200, 210), 2: (300, 310)}.items() for r in rates]

    cap = storage_tiers.mover_cap_from_records(records, tier_id=TIER)

    assert cap["movers"] == 3 and cap["basis"] == "rising"


def test_single_receipts_measure_no_error_and_cap_nothing() -> None:
    records = [_receipt(1, 200), _receipt(2, 300), _receipt(3, 250)]

    cap = storage_tiers.mover_cap_from_records(records, tier_id=TIER)

    assert cap["movers"] is None and cap["basis"] == "unmeasured"
    assert [row["se_mb_s"] for row in cap["curve"]] == [None, None, None]


def test_the_fold_skips_what_did_not_measure_this_pool() -> None:
    other_pool = {"source": {"guid": "2", "pool": "storage_pool"}}
    records = [
        _receipt(1, 200), _receipt(1, 210),
        # Another pool's disks, before a rebuild (#611).
        _receipt(5, 900, identity=other_pool), _receipt(5, 910, identity=other_pool),
        # An adoption: staged from the stage, barely reading the pool (#654).
        _receipt(5, 900, staged=10 * GIB, pool_read=1), _receipt(5, 910, staged=10 * GIB,
                                                                 pool_read=1),
        # A receipt from another tier.
        {**_receipt(5, 900), "tier_id": "prismabuild-stage:elsewhere"},
    ]

    cap = storage_tiers.mover_cap_from_records(records, tier_id=TIER,
                                               pool_identity=IDENTITY)

    assert [row["movers"] for row in cap["curve"]] == [1]
    assert cap["receipts"] == 2


# ------------------------------------------------- warm copies, yielded time


def test_a_copy_faster_than_the_pools_whole_delivery_is_warm() -> None:
    warm = _receipt(1, 250, staged=int(2150e6 * 2), seconds=5.0)
    cold = _receipt(1, 250)

    assert storage_tiers.outran_the_pool(warm)
    assert not storage_tiers.outran_the_pool(cold)
    assert not storage_tiers.outran_the_pool({"mb_per_s_file_side": 900.0})


def test_seconds_a_copy_stood_aside_are_not_a_shortfall() -> None:
    """Sealed at 100 MB/s, 50 MB in 1 s, 0.5 s of it yielded to a waited copy."""

    record = {"bytes_staged": 50_000_000, "seconds": 1.0,
              storage_tiers.MOVER_FILL_DEMAND_FIELD: 100,
              "disk_pacing": {"held_seconds": 0.0,
                              storage_tiers.YIELDED_FIELD: 0.5}}

    assert not storage_tiers._fell_short(record)
    record["disk_pacing"] = {"held_seconds": 0.0}
    assert storage_tiers._fell_short(record)


# ------------------------------------------------------------ the claim gate


def _queue(tmp_path: Path, *, fill: int) -> pool.PoolQueue:
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    tokens = {"stage_gib": 16}
    if fill:
        tokens[storage_tiers.FILL_KIND] = fill
    queue.mint_tier_capacity(TIER, tokens)
    return queue


def _announce(queue: pool.PoolQueue, *, waits: list[str], cap: int | None,
              announced: float | None = None) -> None:
    queue.announce_tier({
        "schema": storage_tiers.TIER_RECORD_SCHEMA_V1, "tier_id": TIER,
        "host": "dl380g10", "tier": "stage", "capacity_bytes": 16 * GIB,
        pool.READER_PLAN_FIELD: {
            "declared_wait": [{"mover_action_key": key, "state": "ready",
                               "consumers": ["c" * 64], "since_unix": 1.0}
                              for key in waits],
            "cap": {"movers": cap, "basis": "measured"},
            "stale_after_s": pool.OFFER_TIMEOUT_S}},
        now=announced)


def _mover(queue: pool.PoolQueue, key: str, ordinal: int, *,
           fill: int = 0) -> None:
    resources = {"cpu": 1, "mem_gb": 1, STAGE_KIND: 2}
    if fill:
        resources[FILL_KIND] = fill
    queue.publish(action_key=key, cas_root=queue.root / "cas",
                  checkout_root=queue.root / "co",
                  worker_script=queue.root / "worker.py", tags=["dl380g10"],
                  resources=resources, max_attempts=1, retry_safe=False,
                  residency={"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                             "manifest_sha256": "9" * 64,
                             "manifest_bytes": 1 << 40,
                             "range_start_bytes": ordinal * 2 * GIB,
                             "range_end_bytes": (ordinal + 1) * 2 * GIB})
    time.sleep(0.01)


def _claim(queue: pool.PoolQueue) -> str | None:
    item = queue.claim(capacity={"cpu": 16, "mem_gb": 64}, tags=["dl380g10"])
    return None if item is None else str(item["action_key"])


def test_the_waited_copy_is_admitted_without_fill_and_holds_its_stage(
        tmp_path: Path) -> None:
    """The plan admits it, not the fill ledger: no fill is free here."""

    queue = _queue(tmp_path, fill=0)
    waited = "a" * 64
    _mover(queue, "b" * 64, 0, fill=50)
    _mover(queue, waited, 1, fill=50)
    _announce(queue, waits=[waited], cap=None)

    assert _claim(queue) == waited
    assert _claim(queue) is None
    claimed = json.loads(queue.item_path(pool.CLAIMED, waited).read_text())
    assert claimed["tier_fill_waived"]["waived"] == 50
    assert queue.tier_ledger(TIER).holder_tokens(waited) == {"stage_gib": 2}


def test_the_cap_admits_that_many_movers_and_no_more(tmp_path: Path) -> None:
    queue = _queue(tmp_path, fill=0)
    for ordinal in range(4):
        _mover(queue, f"{ordinal}".ljust(64, "e"), ordinal)
    _announce(queue, waits=[], cap=2)

    claimed = [_claim(queue) for _ in range(4)]

    assert [key is not None for key in claimed] == [True, True, False, False]
    assert len(queue.movers_claimed_on_tier(TIER)) == 2


def test_a_stale_plan_fails_open(tmp_path: Path) -> None:
    """A tier loop that stopped announcing cannot hold the tier on its last word."""

    queue = _queue(tmp_path, fill=0)
    for ordinal in range(3):
        _mover(queue, f"{ordinal}".ljust(64, "f"), ordinal)
    _announce(queue, waits=["a" * 64], cap=1,
              announced=time.time() - pool.OFFER_TIMEOUT_S - 1)

    assert all(_claim(queue) is not None for _ in range(3))


# ---------------------------------------------------- the own landing report


def test_a_report_from_before_the_claim_prices_nothing(tmp_path: Path) -> None:
    queue = _queue(tmp_path, fill=0)
    mover = "d" * 64
    path = queue.mover_landing_path(mover)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({
        "schema": pool.MOVER_LANDING_SCHEMA_V1, "mover_action_key": mover,
        "started_unix": 1000.0, "reported_unix": 1010.0,
        "copied_bytes": 100_000_000, "landed_bytes": 0,
        "range_bytes": 1_000_000_000}))

    assert tier_loop._mover_report(queue, mover, claimed_unix=1001.0) is None
    report = tier_loop._mover_report(queue, mover, claimed_unix=999.0)
    assert report == {"landed_bytes": 0, "copied_bytes": 100_000_000,
                      "started_unix": 1000.0, "reported_unix": 1010.0}

    priced = residency_plan.expected_landings(
        [{"mover_action_key": mover, "state": "claimed", "claimed_unix": 999.0,
          "range_bytes": 1_000_000_000, **report}],
        now=1010.0, landing_bytes_per_s=1e9)[mover]
    # 100 MB in the 10 s since the copy started: 10 MB/s, 900 MB to go.
    assert priced["basis"] == "reported"
    assert priced["live_bytes_per_s"] == 10_000_000.0
    assert priced["expected_landing_unix"] == 1100.0
