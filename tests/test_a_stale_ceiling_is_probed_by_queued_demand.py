"""A measured ceiling is probed above itself, and the queue sizes the probe (#706).

The supply fold seals ``ceiling_mb_s`` from the most recent receipt that fell
short of the fill it reserved, and clears it only when a later delivery exceeds
it.  But movers are admitted against the tier's fill tokens, so a tier that
minted exactly the ceiling admitted no mover priced above it: the pool was
never asked for more and the refutation clause could not fire from admitted
work.  Two separate live observations have that shape and must not be read as
one:

* the archived poisoning of 2026-09-19 -- receipt ``aa34e2a6`` measured the
  pool at 111.2 MB/s while the box churned
  (``superseded-poisoned-measurements-20260919/``); and
* the live freeze after #707 deployed, ceiling 65.7 MB/s (receipt
  ``5867d9f47655``, tokens 65) with six already-ready movers reserving
  259 MB/s each, denied ``never_fits_tier_capacity`` against the fold's
  171 MB/s offer.

#707 gives the fold a historical escape: ``ceiling_mb_s`` plus the median
single-reader share its own receipts price, announced as ``probe_offer_mb_s``
and minted with the ``measured-probing`` label.  That offer cannot see the
queue, so a mover sealed before a sink may still ask more than it.  This file
pins the follow-up: the loop selects the larger of the fold's historical offer
and the queued floor -- ``int(ceiling_mb_s)`` plus the oldest ready mover's own
sealed demand -- announces the selected offer and basis, and mints it.  With no
ready demand the fold's offer stands, and with no priceable history either the
offer is exactly the measured ceiling.  The ready row's sealed resources and
key are never rewritten; once such a row is the oldest ready demand, the next
cycle raises the fill offer to that unchanged demand, with other admission
gates still applying.

Nothing here touches the disk pacer or the client rate: ``fill_mb_s_pool_side``
is recorded on the receipt as the copy's own reservation, and the freeze is the
admission ledger's.  The tests drive the real tier cycle and the real
claim-time reservation, with host capacity well above what the movers ask so
the fill ledger -- never host CPU or memory -- is the admission bound.
"""
from __future__ import annotations

from pathlib import Path
import socket
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

#: The archived receipt's window, byte for byte: an 8 MiB copy held 2.257 s of
#: its 2.364 s window achieved 3.5 MB/s file-side while the pool delivered
#: 111.2, and it had reserved 259.
POISONED_KEY = "aa34e2a6e22f8d83b31ccea259349246e4f22898127fceda4c461343e393c4da"
ARCHIVED_CEILING = 111.2
#: The live ceiling after #707 deployed (receipt ``5867d9f47655``).
LIVE_CEILING = 65.7
MOVERS = "abcdef"
PROBE = 259
#: Six ready 259 movers fit this fixture's host box several times over, so a
#: denied claim can only be the fill ledger's answer.
HOST = {"cpu": 32, "mem_gb": 48}


def _poisoned_receipt(ceiling: float = ARCHIVED_CEILING, *,
                      sharers: int | None = 1) -> dict[str, object]:
    """The shortfall receipt whose delivery became the measured ceiling.

    ``sharers`` is the receipt's ``movers_claimed_on_tier``; ``None`` leaves
    it off, which is how a ceiling with no priceable single-reader share (no
    historical probe offer) is built.
    """

    record: dict[str, object] = {
        "schema": storage_tiers.MOVER_RECEIPT_SCHEMA,
        "action_key": POISONED_KEY,
        "tier_id": TIER,
        "complete": True,
        "refusal": None,
        "seconds": 2.364,
        "bytes_staged": 8388608,
        "mb_per_s_file_side": 3.5,
        storage_tiers.MOVER_FILL_DEMAND_FIELD: PROBE,
        "disk_pacing": {
            "mean_pool_read_mb_s": ceiling,
            "pool_read_bytes": 250929152,
            "held_seconds": 2.257,
        },
        "unix": 1789852859.8464358,
    }
    if sharers is not None:
        record[storage_tiers.MOVER_CONCURRENCY_FIELD] = sharers
    return record


def _receipt(*, unix: float, action_key: str, delivered: float,
             sealed: int = PROBE, achieved: float | None = None,
             seconds: float = 10.0, sharers: int = 1) -> dict[str, object]:
    """One mover receipt: what it reserved, what it achieved, what the pool gave."""

    if achieved is None:
        achieved = delivered
    return {
        "schema": storage_tiers.MOVER_RECEIPT_SCHEMA,
        "action_key": action_key,
        "tier_id": TIER,
        "complete": True,
        "seconds": seconds,
        "bytes_staged": int(achieved * 1e6 * seconds),
        "mb_per_s_file_side": achieved,
        storage_tiers.MOVER_CONCURRENCY_FIELD: sharers,
        storage_tiers.MOVER_FILL_DEMAND_FIELD: sealed,
        "disk_pacing": {"mean_pool_read_mb_s": delivered, "held_seconds": 0.0},
        "unix": unix,
    }


def _queue(tmp_path: Path) -> pool.PoolQueue:
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    return queue


def _cycle(queue: pool.PoolQueue, receipts=None) -> dict[str, object]:
    """One real tier cycle over this queue's receipts and ready work."""

    # A stage root beside this queue, never a live box's: the test must not
    # register, sweep or measure anything on a real stage.
    stage_root = queue.root.parent / "stage"

    def discover(**kwargs):
        record = {"schema": storage_tiers.TIER_RECORD_SCHEMA_V1,
                  "tier_id": TIER, "host": "dl380g10", "tier": "stage",
                  "mountpoint": str(stage_root),
                  "capacity_bytes": 600 * GIB}
        record[storage_tiers.FILL_RECORD_FIELD] = storage_tiers.fill_rate_from_records(
            kwargs.get("fill_records") or ())
        return {TIER: record}

    return tier_loop.cycle(queue, host="dl380g10", source_pool="storage_pool",
                           receipts=(receipts if receipts is not None
                                     else tier_loop.ReceiptCache()),
                           discover=discover)[0]


def _ready_mover(queue: pool.PoolQueue, key: str, fill: int) -> None:
    # Tier demand travels with its range block (#595); the tier loop only
    # reads the demand here, but the publish gate is the same one.
    queue.publish(action_key=key * 64, cas_root="/cas", worker_script="w.py",
                  checkout_root="/co", tags=["dl380g10"],
                  resources={"cpu": 4, "mem_gb": 2, KIND: fill,
                             f"stage_gib@{TIER}": 4},
                  residency={"schema": pool.RESIDENCY_SCHEMA_V1,
                             "manifest_sha256": "0" * 64,
                             "manifest_bytes": 4 * GIB, "tier_id": TIER,
                             "range_start_bytes": 0, "range_end_bytes": 4 * GIB})


def _frozen_queue(tmp_path: Path, ceiling: float = ARCHIVED_CEILING,
                  *, sharers: int | None = 1) -> pool.PoolQueue:
    """The freeze shape: a stale ceiling and six ready movers priced above it."""

    queue = _queue(tmp_path)
    queue.record_move(POISONED_KEY, _poisoned_receipt(ceiling, sharers=sharers))
    for key in MOVERS:
        _ready_mover(queue, key, PROBE)
    return queue


def _claim(queue: pool.PoolQueue) -> dict[str, object] | None:
    """The real claim-time admission path: host capacity, then tier tokens."""

    return queue.claim(capacity=dict(HOST), tags=["dl380g10"])


def _copy_ended(queue: pool.PoolQueue, key: str) -> None:
    """The reservation half of ``finish``: host capacity back, rate returned.

    Occupancy stays charged because the bytes are on the stage (#636), so the
    next claim is bounded by fill alone and not by the box this fixture ran on.
    """

    queue._release_reservation(key, host=socket.gethostname(), keep_tier=True)
    assert FILL not in queue.tier_ledger(TIER).holder_tokens(key)


@pytest.mark.parametrize("ceiling", [ARCHIVED_CEILING, LIVE_CEILING])
def test_a_stale_ceiling_no_longer_freezes_movers_priced_above_it(
        tmp_path, ceiling):
    """Six ready 259 movers under a stale ceiling: the live freeze shape."""

    queue = _frozen_queue(tmp_path, ceiling)
    record = _cycle(queue)

    # The behavioral fact first: the queued floor seats exactly one mover;
    # without it every one of the six is denied against an offer below its
    # demand.
    first = _claim(queue)
    assert first is not None
    assert first["action_key"] in {key * 64 for key in MOVERS}
    assert _claim(queue) is None
    assert queue.tier_ledger(TIER).available()[FILL] == int(ceiling)

    # The offer is the larger of the fold's historical increment (3.5 here)
    # and the queued floor, and the announced supply names the selection.
    assert record["fill_supply"]["ceiling_mb_s"] == ceiling
    assert record["fill_supply"]["may_grow"] is False
    assert record["fill_source"] == "measured-probing"
    assert record["fill_probe_mb_s"] == PROBE
    assert record["tokens"][FILL] == int(ceiling) + PROBE
    assert record["fill_supply"]["probing"] is True
    assert record["fill_supply"]["probe_offer_mb_s"] == int(ceiling) + PROBE
    assert record["fill_supply"]["probe_basis"] == {
        "basis": "oldest-ready-sealed-demand", "demand_mb_s": PROBE,
        "historical_offer_mb_s": int(ceiling + 3.5)}

    # The queued rows are untouched: same keys, same sealed demand.
    assert first["resources"][KIND] == PROBE
    assert first["resources"][f"stage_gib@{TIER}"] == 4
    remaining = [item for item in queue.ready_items()
                 if item.get("action_key") != first["action_key"]]
    assert remaining
    assert all(item["resources"][KIND] == PROBE for item in remaining)


@pytest.mark.parametrize("ceiling", [ARCHIVED_CEILING, LIVE_CEILING])
def test_no_priceable_history_and_no_ready_demand_offer_the_plain_ceiling(
        tmp_path, ceiling):
    """Nothing invents a size: the fold cannot price a reader and no mover is
    queued, so the offer is exactly the measured ceiling."""

    queue = _queue(tmp_path)
    queue.record_move(POISONED_KEY, _poisoned_receipt(ceiling, sharers=None))

    record = _cycle(queue)

    assert record["fill_supply"]["ceiling_mb_s"] == ceiling
    assert record["fill_supply"]["probing"] is False
    assert record["fill_source"] == "measured-ceiling"
    assert "fill_probe_mb_s" not in record
    assert record["tokens"][FILL] == int(ceiling)


@pytest.mark.parametrize("ceiling", [ARCHIVED_CEILING, LIVE_CEILING])
def test_no_priceable_history_still_admits_queued_demand(tmp_path, ceiling):
    """The live shape exactly: the fold can price no reader (171 MB/s came
    from the median before it sank), and the queued 259 movers still admit
    because the floor is the queue's own sealed demand."""

    queue = _frozen_queue(tmp_path, ceiling, sharers=None)
    record = _cycle(queue)

    assert record["fill_source"] == "measured-probing"
    assert record["tokens"][FILL] == int(ceiling) + PROBE
    assert record["fill_probe_mb_s"] == PROBE
    assert record["fill_supply"]["probe_basis"] == {
        "basis": "oldest-ready-sealed-demand", "demand_mb_s": PROBE}
    assert _claim(queue) is not None
    assert _claim(queue) is None


def test_no_ready_demand_keeps_the_fold_offer(tmp_path):
    """'No ready demand means exactly the measured ceiling' is superseded: the
    #707 fold offer (ceiling + median single-reader share) is the fallback."""

    queue = _queue(tmp_path)
    queue.record_move("1" * 64, _receipt(unix=100.0, action_key="1" * 64,
                                         delivered=120.0, sealed=166,
                                         achieved=120.0))
    queue.record_move("2" * 64, _receipt(unix=200.0, action_key="2" * 64,
                                         delivered=120.0, sealed=120,
                                         achieved=120.0))

    record = _cycle(queue)

    assert record["fill_source"] == "measured-probing"
    assert record["tokens"][FILL] == 240
    assert record["fill_probe_mb_s"] == 120
    assert record["fill_supply"]["probe_offer_mb_s"] == pytest.approx(240.0)
    assert record["fill_supply"]["probe_basis"]["basis"] == (
        "median-single-reader-share")


def test_a_larger_fold_offer_is_retained_over_a_smaller_queued_floor(tmp_path):
    queue = _queue(tmp_path)
    queue.record_move("1" * 64, _receipt(unix=100.0, action_key="1" * 64,
                                         delivered=120.0, sealed=166,
                                         achieved=120.0))
    queue.record_move("2" * 64, _receipt(unix=200.0, action_key="2" * 64,
                                         delivered=120.0, sealed=120,
                                         achieved=120.0))
    _ready_mover(queue, "a", 50)

    record = _cycle(queue)

    # The floor is 120 + 50 = 170; the historical offer 240 wins unchanged.
    assert record["fill_source"] == "measured-probing"
    assert record["tokens"][FILL] == 240
    assert record["fill_probe_mb_s"] == 120
    assert record["fill_supply"]["probe_offer_mb_s"] == pytest.approx(240.0)
    assert record["fill_supply"]["probe_basis"]["basis"] == (
        "median-single-reader-share")
    assert _claim(queue) is not None


def test_repeated_cycles_over_unchanged_evidence_mint_unchanged_capacity(tmp_path):
    queue = _frozen_queue(tmp_path)
    receipts = tier_loop.ReceiptCache()

    first = _cycle(queue, receipts)
    second = _cycle(queue, receipts)

    assert first["fill_source"] == second["fill_source"]
    assert first["fill_probe_mb_s"] == second["fill_probe_mb_s"] == PROBE
    assert first["tokens"] == second["tokens"]
    assert second["ledger"]["capacity"][FILL] == int(ARCHIVED_CEILING) + PROBE


def test_a_probe_receipt_that_keeps_up_refutes_the_ceiling(tmp_path):
    """The admitted probe delivers what it reserved: 259, above the 111.2."""

    queue = _frozen_queue(tmp_path)
    admitted = _cycle(queue)
    assert admitted["tokens"][FILL] == int(ARCHIVED_CEILING) + PROBE

    claimed = _claim(queue)
    assert claimed is not None
    key = str(claimed["action_key"])
    queue.record_move(key, _receipt(unix=1789852900.0, action_key=key,
                                    delivered=PROBE, achieved=float(PROBE)))
    _copy_ended(queue, key)

    record = _cycle(queue)

    assert record["fill_supply"]["ceiling_mb_s"] is None
    assert record["fill_supply"]["best_mb_s"] == float(PROBE)
    assert record["fill_source"] == "measured-growing"
    assert record["tokens"][FILL] == PROBE + PROBE
    # Two movers fit where one fit before the refutation.
    assert _claim(queue) is not None
    assert _claim(queue) is not None
    assert _claim(queue) is None


def test_two_readers_below_the_ceiling_refute_it_together(tmp_path):
    """Each demand is under the ceiling; their summed delivery is above it.

    The selected floor seats two 166 movers (332, inside 300 + 166) where the
    bare ceiling seated one, and their overlapping window delivers 332 -- the
    pool doing more than the ceiling claimed, measured from admitted work.
    """

    queue = _queue(tmp_path)
    # A priced seed: one reader's share is 150, so the historical offer is
    # 450 and the queued floor (466) is the larger and selected.
    queue.record_move("f" * 64, _receipt(unix=100.0, action_key="f" * 64,
                                         delivered=300.0, sealed=500,
                                         achieved=250.0, seconds=100.0,
                                         sharers=2))
    for key in "abc":
        _ready_mover(queue, key, 166)

    record = _cycle(queue)
    assert record["fill_supply"]["ceiling_mb_s"] == 300.0
    assert record["fill_source"] == "measured-probing"
    assert record["tokens"][FILL] == 300 + 166
    assert record["fill_supply"]["probe_basis"]["basis"] == (
        "oldest-ready-sealed-demand")

    first, second = _claim(queue), _claim(queue)
    assert first is not None and second is not None
    assert _claim(queue) is None
    for key in (first["action_key"], second["action_key"]):
        queue.record_move(key, _receipt(unix=1789852900.0, action_key=key,
                                        delivered=332.0, sealed=166,
                                        achieved=166.0, seconds=100.0,
                                        sharers=2))
        _copy_ended(queue, key)

    refuted = _cycle(queue)

    assert refuted["fill_supply"]["ceiling_mb_s"] is None
    assert refuted["fill_supply"]["best_mb_s"] == 332.0
    assert refuted["tokens"][FILL] == 332 + 166


def test_a_probe_shortfall_raises_the_stale_ceiling(tmp_path):
    """The pool gave 180: above the stale 111.2, below the 259 reserved."""

    queue = _frozen_queue(tmp_path)
    _cycle(queue)
    claimed = _claim(queue)
    assert claimed is not None
    key = str(claimed["action_key"])
    queue.record_move(key, _receipt(unix=1789852900.0, action_key=key,
                                    delivered=180.0, achieved=180.0))
    _copy_ended(queue, key)

    record = _cycle(queue)

    assert record["fill_supply"]["ceiling_mb_s"] == 180.0
    assert record["fill_ceiling_receipt"] == key
    assert record["fill_source"] == "measured-probing"
    assert record["tokens"][FILL] == 180 + PROBE


def test_continuing_shortfalls_track_the_latest_measurement_without_ratcheting(
        tmp_path):
    """Two failing probes: the offer follows the ceiling down, never up."""

    queue = _frozen_queue(tmp_path)
    seen: list[int] = []
    for unix, delivered in ((1789852900.0, 100.0), (1789853000.0, 90.0)):
        record = _cycle(queue)
        seen.append(record["tokens"][FILL])
        claimed = _claim(queue)
        assert claimed is not None
        key = str(claimed["action_key"])
        queue.record_move(key, _receipt(unix=unix, action_key=key,
                                        delivered=delivered, achieved=delivered))
        _copy_ended(queue, key)
    record = _cycle(queue)
    seen.append(record["tokens"][FILL])

    # Each mint is the latest measured ceiling plus the one queued probe, and
    # a continuing shortfall can only lower it -- not ratchet it upward.
    assert seen == [int(ARCHIVED_CEILING) + PROBE, 100 + PROBE, 90 + PROBE]
    assert seen == sorted(seen, reverse=True)
    assert record["fill_supply"]["ceiling_mb_s"] == 90.0


def test_a_best_after_the_ceiling_does_not_refreeze_the_probe(tmp_path):
    """A ceiling and a rebuilt ``best`` can coexist; the floor is not gated on it.

    Gating the queued floor on ``best_mb_s is None`` -- the narrow reading of
    "best is null after a ceiling" -- would leave the loop exactly as frozen
    as the ceiling branch it replaced.
    """

    queue = _queue(tmp_path)
    queue.record_move("1" * 64, _receipt(unix=100.0, action_key="1" * 64,
                                         delivered=522.0, sealed=166,
                                         achieved=130.0, seconds=100.0))
    queue.record_move("2" * 64, _receipt(unix=200.0, action_key="2" * 64,
                                         delivered=400.0, sealed=166,
                                         achieved=166.0, seconds=100.0))
    _ready_mover(queue, "a", 166)

    record = _cycle(queue)

    assert record["fill_supply"]["ceiling_mb_s"] == 522.0
    assert record["fill_supply"]["best_mb_s"] == 400.0
    assert record["fill_source"] == "measured-probing"
    assert record["fill_probe_mb_s"] == 166
    assert record["tokens"][FILL] == 522 + 166
    assert record["fill_supply"]["probe_basis"] == {
        "basis": "oldest-ready-sealed-demand", "demand_mb_s": 166,
        "historical_offer_mb_s": 670}
